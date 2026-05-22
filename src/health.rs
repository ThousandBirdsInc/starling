use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX_RESPONSE_BYTES: usize = 512 * 1024;
const MAX_MODULES: usize = 24;

#[derive(Clone, Debug)]
pub struct ProxyHealth {
    pub ok: bool,
    pub status: Option<u16>,
    pub reason: String,
    pub message: String,
}

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

pub async fn check_proxy_route(hostname: &str, proxy_port: u16, user_agent: &str) -> ProxyHealth {
    let root = match proxy_http_get(hostname, proxy_port, "/", user_agent).await {
        Ok(resp) => resp,
        Err(e) => {
            return ProxyHealth {
                ok: false,
                status: None,
                reason: "ProxyProbeFailed".to_string(),
                message: format!(
                    "http://{hostname}:{proxy_port}/ is not reachable through the proxy: {e}"
                ),
            };
        }
    };

    if (502..=504).contains(&root.status) {
        return ProxyHealth {
            ok: false,
            status: Some(root.status),
            reason: format!("HTTP{}", root.status),
            message: format!(
                "http://{hostname}:{proxy_port}/ returned HTTP {} through the proxy",
                root.status
            ),
        };
    }

    if root.status < 200 || root.status >= 300 || !is_html(&root) {
        return ProxyHealth {
            ok: true,
            status: Some(root.status),
            reason: format!("HTTP{}", root.status),
            message: format!(
                "http://{hostname}:{proxy_port}/ returned HTTP {} through the proxy",
                root.status
            ),
        };
    }

    match check_module_graph(hostname, proxy_port, user_agent, &root.body).await {
        Ok(()) => ProxyHealth {
            ok: true,
            status: Some(root.status),
            reason: format!("HTTP{}", root.status),
            message: format!(
                "http://{hostname}:{proxy_port}/ returned HTTP {} and module assets loaded through the proxy",
                root.status
            ),
        },
        Err(message) => ProxyHealth {
            ok: false,
            status: Some(root.status),
            reason: "ModuleProbeFailed".to_string(),
            message,
        },
    }
}

async fn check_module_graph(
    hostname: &str,
    proxy_port: u16,
    user_agent: &str,
    html_body: &[u8],
) -> Result<(), String> {
    let html = String::from_utf8_lossy(html_body);
    let mut queue: VecDeque<String> = extract_module_scripts(&html).into();
    let mut seen = Vec::new();

    while let Some(path) = queue.pop_front() {
        if seen.iter().any(|p| p == &path) {
            continue;
        }
        if seen.len() >= MAX_MODULES {
            break;
        }
        seen.push(path.clone());

        let response = proxy_http_get(hostname, proxy_port, &path, user_agent)
            .await
            .map_err(|e| format!("module {path} is not reachable through the proxy: {e}"))?;
        if response.status < 200 || response.status >= 300 {
            return Err(format!(
                "module {path} returned HTTP {} through the proxy{}",
                response.status,
                content_type_suffix(&response)
            ));
        }
        if !is_javascript(&response) {
            return Err(format!(
                "module {path} returned disallowed MIME type {} through the proxy",
                response
                    .headers
                    .get("content-type")
                    .map(|v| format!("{v:?}"))
                    .unwrap_or_else(|| "\"\"".to_string())
            ));
        }

        let body = String::from_utf8_lossy(&response.body);
        for import in extract_local_imports(&body) {
            if !seen.iter().any(|p| p == &import) && !queue.iter().any(|p| p == &import) {
                queue.push_back(import);
            }
        }
    }

    Ok(())
}

async fn proxy_http_get(
    hostname: &str,
    proxy_port: u16,
    path: &str,
    user_agent: &str,
) -> Result<HttpResponse, String> {
    let mut stream = tokio::time::timeout(
        Duration::from_millis(750),
        tokio::net::TcpStream::connect(("127.0.0.1", proxy_port)),
    )
    .await
    .map_err(|_| "connection timed out".to_string())?
    .map_err(|e| e.to_string())?;

    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {hostname}:{proxy_port}\r\nUser-Agent: {user_agent}\r\nConnection: close\r\n\r\n"
    );
    tokio::time::timeout(
        Duration::from_millis(750),
        stream.write_all(request.as_bytes()),
    )
    .await
    .map_err(|_| "write timed out".to_string())?
    .map_err(|e| e.to_string())?;

    let mut raw = Vec::with_capacity(8192);
    let mut buf = [0u8; 8192];
    loop {
        let n = tokio::time::timeout(Duration::from_millis(750), stream.read(&mut buf))
            .await
            .map_err(|_| "read timed out".to_string())?
            .map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        raw.extend_from_slice(&buf[..n]);
        if raw.len() > MAX_RESPONSE_BYTES {
            break;
        }
    }

    parse_http_response(raw)
}

fn parse_http_response(raw: Vec<u8>) -> Result<HttpResponse, String> {
    let split = find_subslice(&raw, b"\r\n\r\n")
        .ok_or_else(|| "proxy returned a response without headers".to_string())?;
    let (head, body_with_sep) = raw.split_at(split);
    let body = body_with_sep[4..].to_vec();
    let head = String::from_utf8_lossy(head);
    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| "proxy returned an empty response".to_string())?;
    let status = parse_http_status(status_line).ok_or_else(|| {
        format!("proxy returned a response without an HTTP status: {status_line}")
    })?;

    let mut headers = HashMap::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

fn parse_http_status(status_line: &str) -> Option<u16> {
    let mut parts = status_line.split_whitespace();
    let version = parts.next()?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse::<u16>().ok()
}

fn is_html(response: &HttpResponse) -> bool {
    response
        .headers
        .get("content-type")
        .is_some_and(|ct| ct.to_ascii_lowercase().contains("text/html"))
}

fn is_javascript(response: &HttpResponse) -> bool {
    response.headers.get("content-type").is_some_and(|ct| {
        let ct = ct.to_ascii_lowercase();
        ct.contains("javascript") || ct.contains("ecmascript")
    })
}

fn content_type_suffix(response: &HttpResponse) -> String {
    response
        .headers
        .get("content-type")
        .map(|ct| format!(" with Content-Type {ct:?}"))
        .unwrap_or_else(|| " with missing Content-Type".to_string())
}

fn extract_module_scripts(html: &str) -> VecDeque<String> {
    let mut scripts = VecDeque::new();
    let mut rest = html;
    while let Some(start) = rest.find("<script") {
        rest = &rest[start + "<script".len()..];
        let Some(end) = rest.find('>') else {
            break;
        };
        let tag = &rest[..end];
        rest = &rest[end + 1..];
        if !has_module_type(tag) {
            continue;
        }
        if let Some(src) = extract_attr(tag, "src").and_then(normalize_local_path) {
            scripts.push_back(src);
        }
    }
    scripts
}

fn has_module_type(tag: &str) -> bool {
    extract_attr(tag, "type").is_some_and(|value| value.eq_ignore_ascii_case("module"))
}

fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let needle = format!("{attr}={quote}");
        let Some(start) = tag.find(&needle) else {
            continue;
        };
        let value_start = start + needle.len();
        let value_end = tag[value_start..].find(quote)? + value_start;
        return Some(tag[value_start..value_end].to_string());
    }
    None
}

fn extract_local_imports(module: &str) -> Vec<String> {
    let import_re = regex::Regex::new(
        r#"(?:import|export)\s+(?:[^'"]*?\s+from\s*)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)"#,
    )
    .unwrap();
    import_re
        .captures_iter(module)
        .filter_map(|captures| captures.get(1).or_else(|| captures.get(2)))
        .filter_map(|m| normalize_local_path(m.as_str().to_string()))
        .collect()
}

fn normalize_local_path(path: String) -> Option<String> {
    if !path.starts_with('/') || path.starts_with("//") {
        return None;
    }
    Some(path)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_http_status_line() {
        assert_eq!(parse_http_status("HTTP/1.1 502 Bad Gateway"), Some(502));
        assert_eq!(parse_http_status("not http"), None);
    }

    #[test]
    fn extracts_module_scripts() {
        let html = r#"<script type="module" src="/src/main.tsx"></script>"#;
        assert_eq!(
            extract_module_scripts(html).into_iter().collect::<Vec<_>>(),
            vec!["/src/main.tsx"]
        );
    }

    #[test]
    fn extracts_local_imports() {
        let js = r#"import React from "/node_modules/react.js?v=1"; import "/src/style.css";"#;
        assert_eq!(
            extract_local_imports(js),
            vec!["/node_modules/react.js?v=1", "/src/style.css"]
        );
    }
}
