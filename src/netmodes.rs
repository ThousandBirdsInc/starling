//! Optional LAN (mDNS) and Tailscale sharing for named URLs, ported from
//! portless. These shell out to platform tools (`dns-sd`/`avahi-publish`,
//! `tailscale`) and are best-effort: if the tool is missing they log and
//! no-op rather than failing the whole run.
//!
//! NOTE: these paths require external daemons/networks and are not exercised
//! by the test suite.

use std::process::Stdio;

/// Best-effort detection of this machine's LAN IP (for mDNS advertising).
pub fn lan_ip() -> String {
    use std::net::UdpSocket;
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| {
            s.connect("8.8.8.8:80")?;
            Ok(s.local_addr()?.ip().to_string())
        })
        .unwrap_or_else(|_| "127.0.0.1".to_string())
}

/// Advertise `<host>` → `ip` over mDNS so other devices on the LAN can resolve
/// it (as `<host>` or `<host>.local`). Returns the spawned advertiser child so
/// the caller can keep it alive; dropping it stops the advertisement.
pub fn advertise_lan(host: &str, ip: &str) -> Option<std::process::Child> {
    // Strip any TLD; mDNS uses the `.local` domain.
    let label = host.split('.').next().unwrap_or(host);
    let local = format!("{label}.local");

    #[cfg(target_os = "macos")]
    let mut cmd = {
        // `dns-sd -P` registers a proxy host record mapping <local> -> <ip>.
        let mut c = std::process::Command::new("dns-sd");
        c.args(["-P", label, "_http._tcp", "local", "80", &local, ip]);
        c
    };
    #[cfg(not(target_os = "macos"))]
    let mut cmd = {
        // avahi-utils on Linux.
        let mut c = std::process::Command::new("avahi-publish-address");
        c.args([&local, ip]);
        c
    };

    match cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
        Ok(child) => {
            println!("LAN: advertising {local} -> {ip}");
            Some(child)
        }
        Err(e) => {
            eprintln!("LAN: could not advertise {local} (is the mDNS tool installed?): {e}");
            None
        }
    }
}

/// Expose the local proxy on the tailnet via `tailscale serve`. Returns true if
/// the command was issued successfully.
pub fn tailscale_serve(proxy_port: u16) -> bool {
    // Modern Tailscale: `tailscale serve --bg <port>` proxies tailnet :443 to
    // the given local port over HTTPS (requires Tailscale HTTPS certs enabled).
    let status = std::process::Command::new("tailscale")
        .args(["serve", "--bg", &proxy_port.to_string()])
        .status();
    match status {
        Ok(s) if s.success() => {
            println!(
                "Tailscale: serving the proxy on your tailnet (see `tailscale serve status`)."
            );
            true
        }
        Ok(s) => {
            eprintln!("tailscale serve failed ({s}); is Tailscale up with HTTPS enabled?");
            false
        }
        Err(e) => {
            eprintln!("tailscale not found ({e}); install the Tailscale CLI to use --tailscale.");
            false
        }
    }
}
