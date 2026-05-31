//! Readiness-probe execution for local resources.
//!
//! Mirrors Tilt's probe actions: run a command (`exec_action`), open a TCP
//! connection (`tcp_socket_action`), or issue an HTTP GET (`http_get_action`).
//! The Starlingfile records the probe on the manifest; [`run_probe_action`]
//! evaluates a single attempt, and `spawn_serve` polls it to gate readiness.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::starlingfile::ProbeAction;

/// Run one probe attempt, returning `Ok(())` on success or a human-readable
/// reason on failure. Each attempt is bounded by `timeout`.
pub async fn run_probe_action(action: &ProbeAction, timeout: Duration) -> Result<(), String> {
    match action {
        ProbeAction::Exec { command } => exec_probe(command, timeout).await,
        ProbeAction::Tcp { host, port } => tcp_probe(host, *port, timeout).await,
        ProbeAction::Http {
            host,
            port,
            scheme,
            path,
        } => http_probe(host, *port, scheme, path, timeout).await,
    }
}

async fn exec_probe(command: &[String], timeout: Duration) -> Result<(), String> {
    if command.is_empty() {
        return Err("exec probe has no command".to_string());
    }
    let mut cmd = tokio::process::Command::new(&command[0]);
    cmd.args(&command[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // If the timeout fires, the future (and child) is dropped — kill it.
        .kill_on_drop(true);
    let child = cmd
        .spawn()
        .map_err(|e| format!("failed to start {}: {e}", command[0]))?;
    match tokio::time::timeout(timeout, child.wait_with_output()).await {
        Err(_) => Err(format!(
            "command timed out after {:.1}s",
            timeout.as_secs_f64()
        )),
        Ok(Err(e)) => Err(format!("command error: {e}")),
        Ok(Ok(out)) if out.status.success() => Ok(()),
        Ok(Ok(out)) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            let tail = stderr.lines().last().unwrap_or("").trim();
            if tail.is_empty() {
                Err(format!("command exited with {}", out.status))
            } else {
                Err(format!("command exited with {}: {tail}", out.status))
            }
        }
    }
}

async fn tcp_probe(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect((host, port))).await {
        Err(_) => Err(format!("connection to {host}:{port} timed out")),
        Ok(Err(e)) => Err(format!("connection to {host}:{port} failed: {e}")),
        Ok(Ok(_)) => Ok(()),
    }
}

async fn http_probe(
    host: &str,
    port: u16,
    scheme: &str,
    path: &str,
    timeout: Duration,
) -> Result<(), String> {
    if scheme.eq_ignore_ascii_case("https") {
        // TLS probing isn't supported; fall back to a plain TCP reachability
        // check so an https probe still gates on the port being open.
        return tcp_probe(host, port, timeout).await;
    }
    let path = if path.is_empty() { "/" } else { path };
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|e| format!("connection to {host}:{port} failed: {e}"))?;
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUser-Agent: starling-probe\r\nConnection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| format!("write failed: {e}"))?;
        let mut buf = Vec::with_capacity(1024);
        let mut chunk = [0u8; 1024];
        // We only need the status line; stop once we have the first line.
        loop {
            let n = stream
                .read(&mut chunk)
                .await
                .map_err(|e| format!("read failed: {e}"))?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(2).any(|w| w == b"\r\n") || buf.len() > 4096 {
                break;
            }
        }
        let head = String::from_utf8_lossy(&buf);
        let status_line = head.lines().next().unwrap_or("");
        let status = status_line
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse::<u16>().ok())
            .ok_or_else(|| format!("no HTTP status in response: {status_line:?}"))?;
        if status < 400 {
            Ok(())
        } else {
            Err(format!("GET {path} returned HTTP {status}"))
        }
    };
    match tokio::time::timeout(timeout, fut).await {
        Err(_) => Err(format!("HTTP GET {host}:{port}{path} timed out")),
        Ok(result) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn exec_probe_reflects_exit_status() {
        let ok = run_probe_action(
            &ProbeAction::Exec {
                command: vec!["sh".into(), "-c".into(), "exit 0".into()],
            },
            Duration::from_secs(2),
        )
        .await;
        assert!(ok.is_ok());

        let fail = run_probe_action(
            &ProbeAction::Exec {
                command: vec!["sh".into(), "-c".into(), "echo nope >&2; exit 3".into()],
            },
            Duration::from_secs(2),
        )
        .await;
        let err = fail.unwrap_err();
        assert!(err.contains("nope"), "stderr surfaced: {err}");
    }

    #[tokio::test]
    async fn exec_probe_times_out() {
        let result = run_probe_action(
            &ProbeAction::Exec {
                command: vec!["sh".into(), "-c".into(), "sleep 5".into()],
            },
            Duration::from_millis(150),
        )
        .await;
        assert!(result.unwrap_err().contains("timed out"));
    }

    #[tokio::test]
    async fn tcp_probe_succeeds_on_open_port() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let ok = run_probe_action(
            &ProbeAction::Tcp {
                host: "127.0.0.1".into(),
                port,
            },
            Duration::from_secs(1),
        )
        .await;
        assert!(ok.is_ok());
    }

    #[tokio::test]
    async fn tcp_probe_fails_on_closed_port() {
        // Port 1 is in the privileged range and effectively never has a
        // listener, so a connect is refused deterministically (no port-reuse
        // race with a just-dropped listener under concurrent tests).
        let fail = run_probe_action(
            &ProbeAction::Tcp {
                host: "127.0.0.1".into(),
                port: 1,
            },
            Duration::from_secs(1),
        )
        .await;
        assert!(fail.is_err());
    }
}
