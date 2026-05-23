//! Embedded portless-style reverse proxy.
//!
//! Ports the core of `../portless`: a Host-header reverse proxy that maps
//! `<name>.<tld>` hostnames to `127.0.0.1:<port>` backends, so Starling resources
//! get stable, named URLs (e.g. `http://web.localhost:1355`) instead of raw
//! `localhost:PORT`. Supports WebSocket upgrades and arbitrary streaming via
//! bidirectional byte copy, injects `X-Forwarded-*` headers, and detects
//! forwarding loops via `x-portless-hops`.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Max hops before a request is rejected as a forwarding loop (matches portless).
const MAX_PROXY_HOPS: u32 = 5;
/// Cap on the size of the buffered request head we parse (bytes).
const MAX_HEAD_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct Route {
    pub hostname: String,
    pub port: u16,
}

/// A live-updating registry of hostname → backend port mappings, shared
/// between the proxy and the engine.
#[derive(Clone, Default)]
pub struct ProxyRegistry {
    routes: Arc<RwLock<Vec<Route>>>,
}

impl ProxyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a route for `hostname`.
    pub fn register(&self, hostname: &str, port: u16) -> Option<u16> {
        let mut routes = self.routes.write().unwrap();
        let previous = routes
            .iter()
            .find(|r| r.hostname == hostname)
            .map(|r| r.port);
        routes.retain(|r| r.hostname != hostname);
        routes.push(Route {
            hostname: hostname.to_string(),
            port,
        });
        previous
    }

    pub fn remove(&self, hostname: &str) -> Option<u16> {
        let mut routes = self.routes.write().unwrap();
        let previous = routes
            .iter()
            .find(|r| r.hostname == hostname)
            .map(|r| r.port);
        routes.retain(|r| r.hostname != hostname);
        previous
    }

    pub fn snapshot(&self) -> Vec<Route> {
        self.routes.read().unwrap().clone()
    }

    /// Find the backend port for a host (exact match, then wildcard subdomain).
    fn lookup(&self, host: &str) -> Option<u16> {
        let routes = self.routes.read().unwrap();
        if let Some(r) = routes.iter().find(|r| r.hostname == host) {
            return Some(r.port);
        }
        // Non-strict fallback: tenant.myapp.localhost → myapp.localhost.
        routes
            .iter()
            .find(|r| host.ends_with(&format!(".{}", r.hostname)))
            .map(|r| r.port)
    }
}

/// Proxy settings for standalone (`--no-daemon`) mode, held by [`ProxyHandle::Local`].
#[derive(Clone)]
pub struct ProxyConfig {
    pub registry: ProxyRegistry,
    pub tld: String,
    pub proxy_port: u16,
    pub leased_ports: Arc<Mutex<HashSet<u16>>>,
    /// Reserved for future HTTPS support; currently always false.
    #[allow(dead_code)]
    pub tls: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortReservation {
    pub port: u16,
    pub preferred: Option<u16>,
    pub conflict: bool,
}

/// How the engine allocates ports and registers named routes: either through
/// the central daemon (shared proxy, cross-instance port leasing) or, in
/// standalone `--no-daemon` mode, an in-process proxy.
#[derive(Clone)]
pub enum ProxyHandle {
    Local(ProxyConfig),
    Daemon {
        client: crate::daemon::client::DaemonClient,
        instance: String,
        /// Project name, used to namespace hostnames across instances.
        project: String,
        tld: String,
        proxy_port: u16,
        tls: bool,
    },
}

impl ProxyHandle {
    pub fn proxy_port(&self) -> u16 {
        match self {
            ProxyHandle::Local(c) => c.proxy_port,
            ProxyHandle::Daemon { proxy_port, .. } => *proxy_port,
        }
    }
    fn tls(&self) -> bool {
        match self {
            ProxyHandle::Local(c) => c.tls,
            ProxyHandle::Daemon { tls, .. } => *tls,
        }
    }
    /// Hostname for a resource. In daemon mode it's namespaced by project
    /// (`<resource>.<project>.<tld>`, collapsing to `<project>.<tld>` when the
    /// resource and project names match) so multiple instances don't collide.
    pub fn hostname(&self, label: &str) -> String {
        let res = sanitize_label(label);
        match self {
            ProxyHandle::Local(c) => format!("{res}.{}", c.tld),
            ProxyHandle::Daemon { project, tld, .. } => {
                let proj = sanitize_label(project);
                // Single label under the TLD so one `*.<tld>` wildcard cert
                // covers every resource (multi-level wildcards aren't valid).
                if res == proj || res.is_empty() {
                    format!("{proj}.{tld}")
                } else {
                    format!("{res}-{proj}.{tld}")
                }
            }
        }
    }
    pub fn url_for(&self, label: &str) -> String {
        format_url(&self.hostname(label), self.proxy_port(), self.tls())
    }

    /// Reserve a backend port, preferring the requested port if it is free.
    pub async fn reserve_port(&self, preferred: Option<u16>) -> Option<PortReservation> {
        match self {
            ProxyHandle::Local(c) => reserve_local_port(c, preferred).await,
            ProxyHandle::Daemon {
                client, instance, ..
            } => {
                use crate::daemon::protocol::{Request, Response};
                match client
                    .call(&Request::ReservePort {
                        instance: instance.clone(),
                        preferred,
                    })
                    .await
                {
                    Ok(Response::ReservedPort {
                        port,
                        preferred,
                        conflict,
                    }) => Some(PortReservation {
                        port,
                        preferred,
                        conflict,
                    }),
                    Ok(Response::Error(_)) => match client
                        .call(&Request::AllocatePort {
                            instance: instance.clone(),
                        })
                        .await
                    {
                        Ok(Response::Port { port }) => Some(PortReservation {
                            port,
                            preferred,
                            conflict: preferred.is_some(),
                        }),
                        _ => None,
                    },
                    _ => None,
                }
            }
        }
    }

    /// Register a route for `label` → `port`.
    pub async fn register(&self, label: &str, port: u16) {
        let host = self.hostname(label);
        match self {
            ProxyHandle::Local(c) => {
                let previous = c.registry.register(&host, port);
                let mut leased = c.leased_ports.lock().await;
                leased.insert(port);
                if let Some(previous) = previous {
                    release_local_port_if_unused(c, &mut leased, previous);
                }
            }
            ProxyHandle::Daemon {
                client, instance, ..
            } => {
                use crate::daemon::protocol::Request;
                let _ = client
                    .call(&Request::RegisterRoute {
                        instance: instance.clone(),
                        hostname: host,
                        port,
                    })
                    .await;
            }
        }
    }

    pub async fn remove(&self, label: &str) {
        let host = self.hostname(label);
        match self {
            ProxyHandle::Local(c) => {
                let previous = c.registry.remove(&host);
                if let Some(previous) = previous {
                    let mut leased = c.leased_ports.lock().await;
                    release_local_port_if_unused(c, &mut leased, previous);
                }
            }
            ProxyHandle::Daemon { client, .. } => {
                use crate::daemon::protocol::Request;
                let _ = client.call(&Request::RemoveRoute { hostname: host }).await;
            }
        }
    }
}

async fn reserve_local_port(
    config: &ProxyConfig,
    preferred: Option<u16>,
) -> Option<PortReservation> {
    if let Some(port) = preferred {
        let mut leased = config.leased_ports.lock().await;
        if !leased.contains(&port) && port_available(port).await {
            leased.insert(port);
            return Some(PortReservation {
                port,
                preferred,
                conflict: false,
            });
        }
    }

    for _ in 0..50 {
        if let Ok(port) = find_free_port().await {
            let mut leased = config.leased_ports.lock().await;
            if leased.insert(port) {
                return Some(PortReservation {
                    port,
                    preferred,
                    conflict: preferred.is_some(),
                });
            }
        }
    }
    None
}

fn release_local_port_if_unused(config: &ProxyConfig, leased: &mut HashSet<u16>, port: u16) {
    if !config.registry.snapshot().iter().any(|r| r.port == port) {
        leased.remove(&port);
    }
}

/// Turn a resource name into a DNS-safe hostname label.
pub fn sanitize_label(name: &str) -> String {
    let mut s: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    while s.contains("--") {
        s = s.replace("--", "-");
    }
    s.trim_matches('-').to_string()
}

/// Build the user-facing URL for a route.
pub fn format_url(hostname: &str, proxy_port: u16, tls: bool) -> String {
    let scheme = if tls { "https" } else { "http" };
    let default = if tls { 443 } else { 80 };
    if proxy_port == default {
        format!("{scheme}://{hostname}")
    } else {
        format!("{scheme}://{hostname}:{proxy_port}")
    }
}

/// Allocate a free TCP port on 127.0.0.1 by binding to port 0.
pub async fn find_free_port() -> std::io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

/// Test whether a specific TCP port is bindable on 127.0.0.1.
pub async fn port_available(port: u16) -> bool {
    TcpListener::bind(("127.0.0.1", port)).await.is_ok()
}

/// Run the proxy server until the listener errors.
pub async fn serve(addr: String, registry: ProxyRegistry, proxy_port: u16) {
    accept_loop(addr, registry, proxy_port, None, false).await;
}

/// Serve the proxy over TLS (HTTPS), demuxing plain HTTP on the same port to a
/// redirect. `config` mints a matching cert per SNI hostname (see [`crate::certs`]).
pub async fn serve_tls(
    addr: String,
    registry: ProxyRegistry,
    proxy_port: u16,
    config: tokio_rustls::rustls::ServerConfig,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
    accept_loop(addr, registry, proxy_port, Some(acceptor), true).await;
}

async fn accept_loop(
    addr: String,
    registry: ProxyRegistry,
    proxy_port: u16,
    tls: Option<tokio_rustls::TlsAcceptor>,
    is_tls: bool,
) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("starling proxy: failed to bind {addr}: {e}");
            return;
        }
    };
    let scheme = if is_tls { "https" } else { "http" };
    println!("named-URL proxy listening on {scheme}://{addr}/");
    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let reg = registry.clone();
                let tls = tls.clone();
                tokio::spawn(async move {
                    if let Some(acceptor) = tls {
                        // Peek the first byte: 0x16 = TLS ClientHello.
                        let mut first = [0u8; 1];
                        match socket.peek(&mut first).await {
                            Ok(1) if first[0] == 0x16 => match acceptor.accept(socket).await {
                                Ok(stream) => {
                                    let _ = handle_conn(stream, reg, proxy_port, true).await;
                                }
                                Err(e) => tracing::debug!("tls handshake failed: {e}"),
                            },
                            // Plain HTTP on the TLS port → redirect to https.
                            _ => {
                                let _ = redirect_to_https(socket, proxy_port).await;
                            }
                        }
                    } else if let Err(e) = handle_conn(socket, reg, proxy_port, false).await {
                        tracing::debug!("proxy conn ended: {e}");
                    }
                });
            }
            Err(e) => {
                tracing::warn!("proxy accept error: {e}");
            }
        }
    }
}

/// Read a plain-HTTP request on the TLS port and 308-redirect it to https.
async fn redirect_to_https(mut socket: TcpStream, proxy_port: u16) -> std::io::Result<()> {
    let (head, _leftover) = read_head(&mut socket).await?;
    let port_suffix = if proxy_port == 443 {
        String::new()
    } else {
        format!(":{proxy_port}")
    };
    let location = format!("https://{}{}/", head.host, port_suffix);
    let resp = format!(
        "HTTP/1.1 308 Permanent Redirect\r\nLocation: {location}\r\n\
         Content-Length: 0\r\nX-Portless: 1\r\nConnection: close\r\n\r\n"
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.flush().await
}

/// The parsed head of an incoming HTTP/1.1 request.
struct Head {
    raw: Vec<u8>,
    host: String,
    hops: u32,
    is_websocket: bool,
}

/// Read and parse the request head (up to and including the blank line).
/// Returns the head plus any body bytes already read.
async fn read_head<S: AsyncRead + Unpin>(socket: &mut S) -> std::io::Result<(Head, Vec<u8>)> {
    let mut buf = Vec::with_capacity(2048);
    let mut tmp = [0u8; 2048];
    let split;
    loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            split = pos + 4;
            break;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "request head too large",
            ));
        }
        let n = socket.read(&mut tmp).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before request head",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let leftover = buf.split_off(split);
    let head_str = String::from_utf8_lossy(&buf);

    let mut host = String::new();
    let mut hops = 0u32;
    let mut is_websocket = false;
    for line in head_str.split("\r\n").skip(1) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim();
        match key.as_str() {
            "host" => host = val.split(':').next().unwrap_or(val).to_string(),
            "x-portless-hops" => hops = val.parse().unwrap_or(0),
            "upgrade" if val.eq_ignore_ascii_case("websocket") => is_websocket = true,
            _ => {}
        }
    }

    Ok((
        Head {
            raw: buf,
            host,
            hops,
            is_websocket,
        },
        leftover,
    ))
}

/// Inject X-Forwarded-* and incremented x-portless-hops into the head, just
/// before the terminating blank line.
fn rewrite_head(head: &Head, tls: bool) -> Vec<u8> {
    let proto = if tls { "https" } else { "http" };
    let extra = format!(
        "X-Forwarded-For: 127.0.0.1\r\n\
         X-Forwarded-Proto: {proto}\r\n\
         X-Forwarded-Host: {host}\r\n\
         x-portless-hops: {hops}\r\n",
        host = head.host,
        hops = head.hops + 1,
    );
    // head.raw ends with \r\n\r\n; insert before the final \r\n.
    let mut out = head.raw.clone();
    let insert_at = out.len() - 2; // before the closing CRLF
    out.splice(insert_at..insert_at, extra.into_bytes());
    out
}

async fn handle_conn<S: AsyncRead + AsyncWrite + Unpin>(
    mut socket: S,
    registry: ProxyRegistry,
    proxy_port: u16,
    tls: bool,
) -> std::io::Result<()> {
    let (head, leftover) = read_head(&mut socket).await?;

    if head.host.is_empty() {
        return write_simple(&mut socket, 400, "Missing Host header").await;
    }
    if head.hops >= MAX_PROXY_HOPS {
        return write_simple(
            &mut socket,
            508,
            "Loop Detected: request passed through the proxy too many times",
        )
        .await;
    }

    let Some(port) = registry.lookup(&head.host) else {
        let body = not_found_body(&head.host, &registry, proxy_port);
        return write_html(&mut socket, 404, &body).await;
    };

    let mut backend = match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(s) => s,
        Err(_) => {
            let body = format!(
                "<h1>502 Bad Gateway</h1><p>No app responding on 127.0.0.1:{port} \
                 for <strong>{}</strong>. It may not be running yet.</p>",
                html_escape(&head.host)
            );
            return write_html(&mut socket, 502, &body).await;
        }
    };

    // Forward the (rewritten) head + any buffered body, then splice both ways.
    let rewritten = rewrite_head(&head, tls);
    backend.write_all(&rewritten).await?;
    if !leftover.is_empty() {
        backend.write_all(&leftover).await?;
    }
    backend.flush().await?;

    // WebSocket upgrades and plain HTTP are both just byte streams from here.
    let _ = head.is_websocket;
    tokio::io::copy_bidirectional(&mut socket, &mut backend)
        .await
        .map(|_| ())
}

// -- response helpers --------------------------------------------------------

async fn write_simple<S: AsyncWrite + Unpin>(
    socket: &mut S,
    status: u16,
    msg: &str,
) -> std::io::Result<()> {
    let body = format!("{status} {msg}\n");
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/plain\r\n\
         Content-Length: {len}\r\nX-Portless: 1\r\nConnection: close\r\n\r\n{body}",
        reason = reason(status),
        len = body.len(),
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.flush().await
}

async fn write_html<S: AsyncWrite + Unpin>(
    socket: &mut S,
    status: u16,
    body: &str,
) -> std::io::Result<()> {
    let page = format!(
        "<!doctype html><html><head><meta charset=utf-8>\
         <title>{status} {reason}</title><style>body{{font-family:ui-monospace,monospace;\
         background:#1d1d1d;color:#eee;padding:3rem;max-width:42rem;margin:auto}}\
         a{{color:#7cd}}code{{color:#9c9}}</style></head><body>{body}</body></html>",
        reason = reason(status),
    );
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Length: {len}\r\nX-Portless: 1\r\nConnection: close\r\n\r\n{page}",
        reason = reason(status),
        len = page.len(),
    );
    socket.write_all(resp.as_bytes()).await?;
    socket.flush().await
}

fn not_found_body(host: &str, registry: &ProxyRegistry, proxy_port: u16) -> String {
    let routes = registry.snapshot();
    let list = if routes.is_empty() {
        "<p>No apps registered.</p>".to_string()
    } else {
        let items: String = routes
            .iter()
            .map(|r| {
                let url = format_url(&r.hostname, proxy_port, false);
                format!(
                    "<li><a href=\"{url}\">{name}</a> → <code>127.0.0.1:{port}</code></li>",
                    name = html_escape(&r.hostname),
                    port = r.port,
                )
            })
            .collect();
        format!("<p>Active apps:</p><ul>{items}</ul>")
    };
    format!(
        "<h1>404 Not Found</h1><p>No app registered for <strong>{}</strong></p>{list}",
        html_escape(host)
    )
}

fn reason(status: u16) -> &'static str {
    match status {
        400 => "Bad Request",
        404 => "Not Found",
        502 => "Bad Gateway",
        508 => "Loop Detected",
        _ => "OK",
    }
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_labels() {
        assert_eq!(sanitize_label("(Starlingfile)"), "starlingfile");
        assert_eq!(sanitize_label("My App!"), "my-app");
        assert_eq!(sanitize_label("api.web"), "api-web");
        assert_eq!(sanitize_label("frontend"), "frontend");
    }

    #[test]
    fn formats_urls() {
        assert_eq!(format_url("web.localhost", 1355, false), "http://web.localhost:1355");
        assert_eq!(format_url("web.localhost", 80, false), "http://web.localhost");
        assert_eq!(format_url("web.localhost", 443, true), "https://web.localhost");
    }

    #[test]
    fn registry_lookup_exact_and_wildcard() {
        let reg = ProxyRegistry::new();
        reg.register("myapp.localhost", 4001);
        assert_eq!(reg.lookup("myapp.localhost"), Some(4001));
        assert_eq!(reg.lookup("tenant.myapp.localhost"), Some(4001));
        assert_eq!(reg.lookup("other.localhost"), None);
        reg.remove("myapp.localhost");
        assert_eq!(reg.lookup("myapp.localhost"), None);
    }
}
