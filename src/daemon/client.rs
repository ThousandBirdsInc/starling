//! Client for talking to the Starling daemon over its Unix socket.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::protocol::{socket_path, Request, Response};

#[derive(Clone)]
pub struct DaemonClient {
    // Read only on Unix (the socket path); Windows resolves the port from a file.
    #[cfg_attr(windows, allow(dead_code))]
    sock: PathBuf,
}

impl DaemonClient {
    pub fn new() -> Self {
        DaemonClient {
            sock: socket_path(),
        }
    }

    /// Send one request and read one response (connection-per-request). Connects
    /// over the Unix socket on Unix, or the daemon's recorded 127.0.0.1 TCP port
    /// on Windows.
    pub async fn call(&self, req: &Request) -> Result<Response> {
        #[cfg(unix)]
        let stream = tokio::net::UnixStream::connect(&self.sock)
            .await
            .map_err(|e| anyhow!("connecting to daemon at {}: {e}", self.sock.display()))?;
        #[cfg(windows)]
        let stream = {
            let port_file = super::protocol::port_file_path();
            let port: u16 = std::fs::read_to_string(&port_file)
                .map_err(|e| anyhow!("reading daemon port {}: {e}", port_file.display()))?
                .trim()
                .parse()
                .map_err(|e| anyhow!("bad daemon port: {e}"))?;
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .map_err(|e| anyhow!("connecting to daemon at 127.0.0.1:{port}: {e}"))?
        };
        let (read_half, mut write_half) = tokio::io::split(stream);
        let mut line = serde_json::to_string(req)?;
        line.push('\n');
        write_half.write_all(line.as_bytes()).await?;
        write_half.flush().await?;

        let mut reader = BufReader::new(read_half);
        let mut resp = String::new();
        reader.read_line(&mut resp).await?;
        if resp.trim().is_empty() {
            return Err(anyhow!("empty response from daemon"));
        }
        Ok(serde_json::from_str(&resp)?)
    }

    pub async fn is_running(&self) -> bool {
        matches!(self.call(&Request::Ping).await, Ok(Response::Ok))
    }

    /// Ensure a daemon is running, spawning `starling daemon` (detached) if not.
    pub async fn ensure_running(&self, proxy_port: u16, tld: &str, tls: bool) -> Result<()> {
        self.ensure_running_with(proxy_port, tld, "127.0.0.1", tls, false)
            .await
    }

    /// Ensure a daemon is running, spawning `starling daemon` (detached) if not.
    pub async fn ensure_running_with(
        &self,
        proxy_port: u16,
        tld: &str,
        host: &str,
        tls: bool,
        lan: bool,
    ) -> Result<()> {
        if self.is_running().await {
            return Ok(());
        }
        let exe = std::env::current_exe()?;
        let log = super::protocol::log_path();
        if let Some(dir) = log.parent() {
            std::fs::create_dir_all(dir).ok();
        }
        let out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .ok();
        let mut cmd = std::process::Command::new(exe);
        cmd.arg("daemon")
            .arg("--proxy-port")
            .arg(proxy_port.to_string())
            .arg("--tld")
            .arg(tld)
            .arg("--host")
            .arg(host)
            .stdin(Stdio::null());
        if tls {
            cmd.arg("--tls");
        }
        if lan {
            cmd.arg("--lan");
        }
        if let Some(f) = out {
            let f2 = f.try_clone().ok();
            cmd.stdout(Stdio::from(f));
            if let Some(f2) = f2 {
                cmd.stderr(Stdio::from(f2));
            }
        }
        cmd.spawn().map_err(|e| anyhow!("spawning daemon: {e}"))?;

        // Wait for the socket to come up.
        for _ in 0..40 {
            if self.is_running().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Err(anyhow!(
            "daemon did not start within 2s; see {}",
            log.display()
        ))
    }
}
