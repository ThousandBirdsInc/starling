//! The Starling daemon: one per machine.
//!
//! Owns the single shared named-URL proxy, allocates ports centrally so
//! multiple `starling up` instances never collide, and aggregates every
//! instance's resources for the shared TUI dashboard.

pub mod client;
pub mod protocol;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

use crate::proxy::{self, ProxyRegistry};
use protocol::*;

/// Max recent log lines retained per (instance, resource).
const LOG_RING: usize = 400;
/// Instances not seen within this window are pruned from the dashboard.
const INSTANCE_TTL: Duration = Duration::from_secs(10);

struct Instance {
    state: InstanceState,
    logs: HashMap<String, LogRing>,
    commands: Vec<Command>,
    last_seen: Instant,
}

/// A capped ring of recent log lines tagged with absolute sequence numbers, so
/// the dashboard can fetch only the lines newer than a cursor it already holds.
#[derive(Default)]
struct LogRing {
    lines: VecDeque<String>,
    /// Absolute sequence of `lines.front()`. `start + lines.len()` is the
    /// sequence the next appended line will get (also the "end" cursor).
    start: u64,
}

impl LogRing {
    /// Append new lines, dropping the oldest beyond `LOG_RING` (which advances
    /// `start`, so retained lines keep their original sequence numbers).
    fn append(&mut self, incoming: impl IntoIterator<Item = String>) {
        self.lines.extend(incoming);
        while self.lines.len() > LOG_RING {
            self.lines.pop_front();
            self.start += 1;
        }
    }

    fn end(&self) -> u64 {
        self.start + self.lines.len() as u64
    }

    /// Lines with sequence `>= since`, plus the new end cursor. A `since` older
    /// than what's retained yields everything still in the ring.
    fn since(&self, since: u64) -> (Vec<String>, u64) {
        let skip = since.saturating_sub(self.start) as usize;
        let lines = self.lines.iter().skip(skip).cloned().collect();
        (lines, self.end())
    }
}

#[derive(Default)]
struct Inner {
    instances: HashMap<String, Instance>,
    seq: u64,
    leased_ports: HashMap<u16, String>,
    routes: Vec<RouteInfo>,
    shutting_down: bool,
    /// mDNS advertiser processes (kept alive while routes are active).
    advertisers: Vec<std::process::Child>,
}

struct Daemon {
    inner: Mutex<Inner>,
    registry: ProxyRegistry,
    proxy_port: u16,
    tld: String,
    /// Advertise routes over mDNS for LAN access.
    lan: bool,
    lan_ip: String,
}

/// Run the daemon until the process is killed. If another daemon is already
/// listening, this returns immediately.
pub async fn run(proxy_port: u16, tld: String, host: String, tls: bool, lan: bool) {
    let dir = state_dir();
    std::fs::create_dir_all(&dir).ok();
    let sock = socket_path();

    // If a daemon is already up, don't start a second one.
    if client::DaemonClient::new().is_running().await {
        println!("starling daemon already running at {}", sock.display());
        return;
    }
    // Clear a stale socket file.
    if sock.exists() {
        std::fs::remove_file(&sock).ok();
    }

    let listener = match UnixListener::bind(&sock) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("daemon: failed to bind {}: {e}", sock.display());
            return;
        }
    };
    std::fs::write(pid_path(), std::process::id().to_string()).ok();

    let registry = ProxyRegistry::new();
    // Start the shared proxy (HTTPS if requested, else HTTP).
    {
        let registry = registry.clone();
        let addr = format!("{host}:{proxy_port}");
        if tls {
            match crate::certs::tls_server_config() {
                Ok(config) => {
                    tokio::spawn(proxy::serve_tls(addr, registry, proxy_port, config));
                }
                Err(e) => {
                    eprintln!("daemon: TLS setup failed ({e}); serving plain HTTP");
                    tokio::spawn(proxy::serve(addr, registry, proxy_port));
                }
            }
        } else {
            tokio::spawn(proxy::serve(addr, registry, proxy_port));
        }
    }

    let lan_ip = if lan {
        crate::netmodes::lan_ip()
    } else {
        String::new()
    };
    if lan {
        println!("LAN mode: advertising routes over mDNS as <name>.local at {lan_ip}");
    }
    let daemon = Arc::new(Daemon {
        inner: Mutex::new(Inner::default()),
        registry,
        proxy_port,
        tld,
        lan,
        lan_ip,
    });

    println!(
        "starling daemon listening on {} (shared proxy :{})",
        sock.display(),
        proxy_port
    );

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let daemon = daemon.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, daemon).await;
                });
            }
            Err(e) => {
                eprintln!("daemon accept error: {e}");
            }
        }
    }
}

async fn handle_conn(stream: tokio::net::UnixStream, daemon: Arc<Daemon>) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let resp = match serde_json::from_str::<Request>(&line) {
        Ok(req) => daemon.handle(req).await,
        Err(e) => Response::Error(format!("bad request: {e}")),
    };
    let mut out = serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into());
    out.push('\n');
    write_half.write_all(out.as_bytes()).await?;
    write_half.flush().await
}

impl Daemon {
    async fn handle(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Ok,

            Request::Register { name, dir, pid } => {
                let mut inner = self.inner.lock().await;
                inner.seq += 1;
                let id = format!("{}-{}", sanitize(&name), inner.seq);
                inner.instances.insert(
                    id.clone(),
                    Instance {
                        state: InstanceState {
                            id: id.clone(),
                            name,
                            dir,
                            pid,
                            resources: vec![],
                        },
                        logs: HashMap::new(),
                        commands: vec![],
                        last_seen: Instant::now(),
                    },
                );
                Response::Registered { instance: id }
            }

            Request::Deregister { instance } => {
                let mut inner = self.inner.lock().await;
                inner.instances.remove(&instance);
                inner.routes.retain(|r| r.instance != instance);
                inner.leased_ports.retain(|_, owner| owner != &instance);
                // Re-sync the proxy registry with the surviving routes.
                self.resync_registry(&inner);
                Response::Ok
            }

            Request::Update {
                instance,
                resources,
                logs,
            } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.state.resources = resources;
                    inst.last_seen = Instant::now();
                    // The reporter sends only lines appended since its last
                    // push, so append (don't replace) to preserve scrollback
                    // and keep each line's sequence stable across fetches.
                    for (res, lines) in logs {
                        inst.logs.entry(res).or_default().append(lines);
                    }
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::AllocatePort { instance } => {
                // Find a free port not already leased.
                match self.reserve_port(&instance, None).await {
                    Some(reservation) => Response::Port {
                        port: reservation.port,
                    },
                    None => Response::Error("could not allocate a free port".into()),
                }
            }

            Request::ReservePort {
                instance,
                preferred,
            } => match self.reserve_port(&instance, preferred).await {
                Some(reservation) => Response::ReservedPort {
                    port: reservation.port,
                    preferred: reservation.preferred,
                    conflict: reservation.conflict,
                },
                None => Response::Error("could not allocate a free port".into()),
            },

            Request::ReleasePort { instance, port } => {
                let mut inner = self.inner.lock().await;
                if inner
                    .leased_ports
                    .get(&port)
                    .map(|owner| owner == &instance)
                    .unwrap_or(false)
                {
                    inner.leased_ports.remove(&port);
                }
                Response::Ok
            }

            Request::RegisterRoute {
                instance,
                hostname,
                port,
            } => {
                self.registry.register(&hostname, port);
                if self.lan {
                    if let Some(child) = crate::netmodes::advertise_lan(&hostname, &self.lan_ip) {
                        self.inner.lock().await.advertisers.push(child);
                    }
                }
                let mut inner = self.inner.lock().await;
                let previous = inner
                    .routes
                    .iter()
                    .find(|r| r.hostname == hostname)
                    .map(|r| r.port);
                inner.routes.retain(|r| r.hostname != hostname);
                if let Some(previous) = previous {
                    release_port_if_unused(&mut inner, previous);
                }
                inner.leased_ports.insert(port, instance.clone());
                inner.routes.push(RouteInfo {
                    hostname,
                    port,
                    instance,
                });
                Response::Ok
            }

            Request::RemoveRoute { hostname } => {
                self.registry.remove(&hostname);
                let mut inner = self.inner.lock().await;
                let previous = inner
                    .routes
                    .iter()
                    .find(|r| r.hostname == hostname)
                    .map(|r| r.port);
                inner.routes.retain(|r| r.hostname != hostname);
                if let Some(previous) = previous {
                    release_port_if_unused(&mut inner, previous);
                }
                Response::Ok
            }

            Request::GetState => {
                let mut inner = self.inner.lock().await;
                self.prune(&mut inner);
                let instances = inner.instances.values().map(|i| i.state.clone()).collect();
                Response::State(DashboardState {
                    instances,
                    routes: inner.routes.clone(),
                    proxy_port: self.proxy_port,
                    tld: self.tld.clone(),
                })
            }

            Request::GetLogs {
                instance,
                resource,
                since,
            } => {
                let inner = self.inner.lock().await;
                let (lines, cursor) = inner
                    .instances
                    .get(&instance)
                    .and_then(|i| i.logs.get(&resource))
                    .map(|r| r.since(since))
                    .unwrap_or_default();
                Response::Logs { lines, cursor }
            }

            Request::PollCommands { instance } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.last_seen = Instant::now();
                    Response::Commands(std::mem::take(&mut inst.commands))
                } else {
                    Response::Commands(vec![])
                }
            }

            Request::Trigger { instance, resource } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.commands.push(Command::Trigger { resource });
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::Restart { instance, resource } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.commands.push(Command::Restart { resource });
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::SetPort {
                instance,
                resource,
                port,
            } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.commands.push(Command::SetPort { resource, port });
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::ShutdownProject { dir } => {
                let mut inner = self.inner.lock().await;
                let mut instances = Vec::new();
                for inst in inner.instances.values_mut() {
                    if inst.state.dir == dir {
                        inst.commands.push(Command::Shutdown);
                        instances.push(inst.state.clone());
                    }
                }
                Response::ShutdownQueued { instances }
            }

            Request::ShutdownDaemon => {
                let mut inner = self.inner.lock().await;
                let instances: Vec<InstanceState> = inner
                    .instances
                    .values_mut()
                    .map(|inst| {
                        inst.commands.push(Command::Shutdown);
                        inst.state.clone()
                    })
                    .collect();
                inner.routes.clear();
                inner.leased_ports.clear();
                self.resync_registry(&inner);
                if !inner.shutting_down {
                    inner.shutting_down = true;
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        std::fs::remove_file(socket_path()).ok();
                        std::fs::remove_file(pid_path()).ok();
                        std::process::exit(0);
                    });
                }
                Response::ShutdownQueued { instances }
            }
        }
    }

    /// Drop instances we haven't heard from recently and their routes.
    fn prune(&self, inner: &mut Inner) {
        let now = Instant::now();
        let dead: Vec<String> = inner
            .instances
            .iter()
            .filter(|(_, i)| now.duration_since(i.last_seen) > INSTANCE_TTL)
            .map(|(id, _)| id.clone())
            .collect();
        if dead.is_empty() {
            return;
        }
        for id in &dead {
            inner.instances.remove(id);
        }
        inner.routes.retain(|r| !dead.contains(&r.instance));
        inner
            .leased_ports
            .retain(|_, owner| !dead.iter().any(|id| id == owner));
        self.resync_registry(inner);
    }

    async fn reserve_port(
        &self,
        instance: &str,
        preferred: Option<u16>,
    ) -> Option<proxy::PortReservation> {
        if let Some(port) = preferred {
            let mut inner = self.inner.lock().await;
            if !inner.leased_ports.contains_key(&port) && proxy::port_available(port).await {
                inner.leased_ports.insert(port, instance.to_string());
                return Some(proxy::PortReservation {
                    port,
                    preferred,
                    conflict: false,
                });
            }
        }

        for _ in 0..50 {
            if let Ok(port) = proxy::find_free_port().await {
                let mut inner = self.inner.lock().await;
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    inner.leased_ports.entry(port)
                {
                    entry.insert(instance.to_string());
                    return Some(proxy::PortReservation {
                        port,
                        preferred,
                        conflict: preferred.is_some(),
                    });
                }
            }
        }
        None
    }

    /// Rebuild the proxy registry from the authoritative route list.
    fn resync_registry(&self, inner: &Inner) {
        for existing in self.registry.snapshot() {
            if !inner.routes.iter().any(|r| r.hostname == existing.hostname) {
                self.registry.remove(&existing.hostname);
            }
        }
        for r in &inner.routes {
            self.registry.register(&r.hostname, r.port);
        }
    }
}

fn release_port_if_unused(inner: &mut Inner, port: u16) {
    if !inner.routes.iter().any(|r| r.port == port) {
        inner.leased_ports.remove(&port);
    }
}

fn sanitize(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() {
        "app".into()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_daemon() -> Daemon {
        Daemon {
            inner: Mutex::new(Inner::default()),
            registry: ProxyRegistry::new(),
            proxy_port: 1360,
            tld: "localhost".to_string(),
            lan: false,
            lan_ip: String::new(),
        }
    }

    #[test]
    fn log_ring_serves_only_lines_newer_than_cursor() {
        let mut ring = LogRing::default();
        ring.append(["a".to_string(), "b".to_string()]);

        // since 0 returns everything plus the end cursor.
        let (lines, cursor) = ring.since(0);
        assert_eq!(lines, vec!["a", "b"]);
        assert_eq!(cursor, 2);

        // Nothing new since the last cursor.
        ring.append(["c".to_string()]);
        let (lines, cursor) = ring.since(cursor);
        assert_eq!(lines, vec!["c"]);
        assert_eq!(cursor, 3);
        assert_eq!(ring.since(cursor).0, Vec::<String>::new());
    }

    #[test]
    fn log_ring_drops_oldest_but_keeps_sequence_stable() {
        let mut ring = LogRing::default();
        for i in 0..(LOG_RING + 10) {
            ring.append([format!("line {i}")]);
        }
        // Capped to LOG_RING, end cursor counts every line ever appended, and
        // the oldest 10 sequences (0..10) have aged out.
        assert_eq!(ring.lines.len(), LOG_RING);
        assert_eq!(ring.end(), (LOG_RING + 10) as u64);
        assert_eq!(ring.start, 10);
        // A cursor pointing at dropped lines just gets what's still retained.
        let (lines, cursor) = ring.since(0);
        assert_eq!(lines.len(), LOG_RING);
        assert_eq!(lines[0], "line 10");
        assert_eq!(cursor, (LOG_RING + 10) as u64);
    }

    #[tokio::test]
    async fn reserves_preferred_port_when_available() {
        let daemon = test_daemon();
        let port = proxy::find_free_port().await.unwrap();

        let reservation = daemon.reserve_port("inst", Some(port)).await.unwrap();

        assert_eq!(reservation.port, port);
        assert_eq!(reservation.preferred, Some(port));
        assert!(!reservation.conflict);
    }

    #[tokio::test]
    async fn falls_back_when_preferred_port_is_leased() {
        let daemon = test_daemon();
        let port = proxy::find_free_port().await.unwrap();
        let first = daemon.reserve_port("inst-a", Some(port)).await.unwrap();

        let fallback = daemon
            .reserve_port("inst-b", Some(first.port))
            .await
            .unwrap();

        assert_ne!(fallback.port, first.port);
        assert_eq!(fallback.preferred, Some(first.port));
        assert!(fallback.conflict);
    }

    #[tokio::test]
    async fn falls_back_when_preferred_port_is_bound_elsewhere() {
        let daemon = test_daemon();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let busy_port = listener.local_addr().unwrap().port();

        let fallback = daemon.reserve_port("inst", Some(busy_port)).await.unwrap();

        assert_ne!(fallback.port, busy_port);
        assert_eq!(fallback.preferred, Some(busy_port));
        assert!(fallback.conflict);
    }

    #[tokio::test]
    async fn removing_route_releases_its_port() {
        let daemon = test_daemon();
        let port = daemon.reserve_port("inst", None).await.unwrap().port;
        let hostname = "web.localhost".to_string();

        assert!(matches!(
            daemon
                .handle(Request::RegisterRoute {
                    instance: "inst".to_string(),
                    hostname: hostname.clone(),
                    port,
                })
                .await,
            Response::Ok
        ));
        assert!(daemon.inner.lock().await.leased_ports.contains_key(&port));

        assert!(matches!(
            daemon.handle(Request::RemoveRoute { hostname }).await,
            Response::Ok
        ));
        assert!(!daemon.inner.lock().await.leased_ports.contains_key(&port));
    }

    #[tokio::test]
    async fn release_port_releases_matching_instance_lease() {
        let daemon = test_daemon();
        let port = daemon.reserve_port("inst", None).await.unwrap().port;

        assert!(matches!(
            daemon
                .handle(Request::ReleasePort {
                    instance: "inst".to_string(),
                    port,
                })
                .await,
            Response::Ok
        ));

        assert!(!daemon.inner.lock().await.leased_ports.contains_key(&port));
    }
}
