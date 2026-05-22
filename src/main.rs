//! Starling: a local dev orchestrator, ported from Tilt (tilt.dev) + portless.
//!
//! Architecture: a single background **daemon** owns the shared named-URL proxy
//! and allocates ports centrally, so multiple `starling up` instances never
//! collide. Each `starling up` runs an engine for one project and reports its
//! resources to the daemon. `starling` (or `starling dash`) opens a k9s-style
//! TUI showing every instance's resources.

mod api;
mod certs;
mod daemon;
mod engine;
mod k8s;
mod netmodes;
mod proxy;
mod seed;
mod server;
mod starlingfile;
mod store;
mod tui;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tokio::sync::mpsc;

use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{Command as DaemonCommand, Request, ResourceSnapshot, Response};
use crate::proxy::{ProxyConfig, ProxyHandle, ProxyRegistry};
use crate::server::AppState;
use crate::store::Store;

#[derive(Parser)]
#[command(name = "starling", version, about = "Starling: orchestrate your local dev services with named URLs")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start an instance for the current project (registers with the daemon).
    Up(UpArgs),
    /// Run the central daemon (auto-started by `up`/`dash` if not running).
    Daemon(DaemonArgs),
    /// Open the shared k9s-style TUI dashboard (default when run with no args).
    Dash(DashArgs),
    /// Install the local Starling CA into the system trust store (for HTTPS).
    Trust,
    /// Sync /etc/hosts with the proxy's route hostnames (for non-.localhost TLDs).
    Hosts,
}

#[derive(Parser)]
struct UpArgs {
    /// Web UI / HUD port (only used with --web).
    #[arg(long, default_value_t = 10360)]
    port: u16,
    /// Host/interface to bind.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// Directory of the built frontend assets (only used with --web).
    #[arg(long, default_value = "web/build")]
    web_dir: String,
    /// Path to the Starlingfile to load.
    #[arg(long, default_value = "Starlingfile")]
    file: String,
    /// Apply Kubernetes manifests with `--dry-run=client` (nothing mutated).
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// Shared named-URL proxy port (owned by the daemon).
    #[arg(long, default_value_t = 1360)]
    proxy_port: u16,
    /// TLD used for proxy hostnames.
    #[arg(long, default_value = "localhost")]
    tld: String,
    /// Disable named URLs entirely.
    #[arg(long, default_value_t = false)]
    no_proxy: bool,
    /// Run standalone with an in-process proxy instead of the shared daemon.
    #[arg(long, default_value_t = false)]
    no_daemon: bool,
    /// Also serve the legacy web UI for this instance.
    #[arg(long, default_value_t = false)]
    web: bool,
    /// Serve named URLs over HTTPS (the daemon generates a local CA; run
    /// `starling trust` once to avoid browser warnings).
    #[arg(long, default_value_t = false)]
    tls: bool,
    /// Share the proxy on your tailnet via `tailscale serve` (experimental).
    #[arg(long, default_value_t = false)]
    tailscale: bool,
}

#[derive(Parser)]
struct DaemonArgs {
    #[arg(long, default_value_t = 1360)]
    proxy_port: u16,
    #[arg(long, default_value = "localhost")]
    tld: String,
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = false)]
    tls: bool,
    /// Advertise routes over mDNS so other LAN devices can resolve them
    /// (experimental; requires dns-sd/avahi).
    #[arg(long, default_value_t = false)]
    lan: bool,
}

#[derive(Parser)]
struct DashArgs {
    /// Proxy port to advertise to the daemon if it needs auto-starting.
    #[arg(long, default_value_t = 1360)]
    proxy_port: u16,
    #[arg(long, default_value = "localhost")]
    tld: String,
    #[arg(long, default_value_t = false)]
    tls: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,tower_http=warn".into()),
        )
        .init();

    match Cli::parse().command {
        Some(Command::Daemon(a)) => daemon::run(a.proxy_port, a.tld, a.host, a.tls, a.lan).await,
        Some(Command::Up(a)) => up(a).await,
        Some(Command::Dash(a)) => tui::run(a.proxy_port, &a.tld, a.tls).await,
        Some(Command::Trust) => {
            if let Err(e) = certs::install_trust() {
                eprintln!("starling trust: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Hosts) => {
            if let Err(e) = sync_hosts().await {
                eprintln!("starling hosts: {e}");
                std::process::exit(1);
            }
        }
        // Bare `starling` opens the dashboard.
        None => tui::run(1360, "localhost", false).await,
    }
}

/// Sync /etc/hosts with the daemon's current route hostnames (skips
/// `.localhost`, which resolves automatically). Requires sudo to write.
async fn sync_hosts() -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    let hosts: Vec<String> = state
        .routes
        .iter()
        .map(|r| r.hostname.clone())
        .filter(|h| !h.ends_with(".localhost"))
        .collect();
    if hosts.is_empty() {
        println!("No non-.localhost hostnames to sync.");
        return Ok(());
    }
    let block: String = hosts
        .iter()
        .map(|h| format!("127.0.0.1 {h} # starling\n"))
        .collect();
    println!("Add these lines to /etc/hosts (needs sudo):\n{block}");
    // Best-effort append via sudo tee.
    let status = std::process::Command::new("sudo")
        .arg("sh")
        .arg("-c")
        .arg(format!("printf '%s' '{block}' >> /etc/hosts"))
        .status();
    match status {
        Ok(s) if s.success() => println!("Updated /etc/hosts."),
        _ => println!("(Could not write /etc/hosts automatically; add the lines above manually.)"),
    }
    Ok(())
}

/// Map a `UIResource` to the compact snapshot the dashboard shows.
fn snapshot(r: &api::v1alpha1::UIResource) -> ResourceSnapshot {
    let name = r.metadata.as_ref().map(|m| m.name.clone()).unwrap_or_default();
    let st = r.status.clone().unwrap_or_default();
    let kind = st
        .specs
        .first()
        .and_then(|s| s.target_type.clone())
        .unwrap_or_else(|| "local".into());
    let pod = st.k8s_resource_info.as_ref().and_then(|k| k.pod_name.clone());
    let url = st
        .endpoint_links
        .first()
        .and_then(|l| l.url.clone());
    ResourceSnapshot {
        name,
        kind,
        update_status: st.update_status.unwrap_or_default(),
        runtime_status: st.runtime_status.unwrap_or_default(),
        pod,
        url,
        build_count: st.build_history.len() as u32,
        last_deploy: st.last_deploy_time,
    }
}

/// Infer a project name from the Starlingfile's directory.
fn project_name(file: &PathBuf) -> String {
    let dir = std::fs::canonicalize(file)
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_default();
    dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("app")
        .to_string()
}

async fn up(args: UpArgs) {
    let (build_tx, build_rx) = mpsc::unbounded_channel::<String>();
    let (restart_tx, restart_rx) = mpsc::unbounded_channel::<String>();
    let store = Arc::new(Store::new(build_tx.clone()));
    store.set_restart_tx(restart_tx);

    // Set up the proxy handle: daemon (default), local (--no-daemon), or none.
    let mut daemon_instance: Option<(DaemonClient, String)> = None;
    let proxy_handle: Option<ProxyHandle> = if args.no_proxy {
        None
    } else if args.no_daemon {
        let registry = ProxyRegistry::new();
        let cfg = ProxyConfig {
            registry: registry.clone(),
            tld: args.tld.clone(),
            proxy_port: args.proxy_port,
            tls: args.tls,
        };
        registry.register(&format!("starling.{}", args.tld), args.port);
        tokio::spawn(proxy::serve(
            format!("{}:{}", args.host, args.proxy_port),
            registry,
            args.proxy_port,
        ));
        Some(ProxyHandle::Local(cfg))
    } else {
        let client = DaemonClient::new();
        if let Err(e) = client.ensure_running(args.proxy_port, &args.tld, args.tls).await {
            eprintln!("starling: {e}");
            std::process::exit(1);
        }
        let name = project_name(&PathBuf::from(&args.file));
        let dir = std::fs::canonicalize(&args.file)
            .ok()
            .and_then(|p| p.parent().map(|p| p.display().to_string()))
            .unwrap_or_default();
        let instance = match client
            .call(&Request::Register {
                name: name.clone(),
                dir,
                pid: std::process::id(),
            })
            .await
        {
            Ok(Response::Registered { instance }) => instance,
            other => {
                eprintln!("starling: daemon register failed: {other:?}");
                std::process::exit(1);
            }
        };
        println!("starling: registered '{name}' with daemon as {instance}");
        daemon_instance = Some((client.clone(), instance.clone()));
        Some(ProxyHandle::Daemon {
            client,
            instance,
            project: name,
            tld: args.tld.clone(),
            proxy_port: args.proxy_port,
            tls: args.tls,
        })
    };

    // Load the Starlingfile and start the engine.
    let config_path = PathBuf::from(&args.file);
    match starlingfile::load(&config_path) {
        Ok(result) => {
            store.append_log(
                Some("(Starlingfile)"),
                "INFO",
                &format!("Loaded Starlingfile ({} resources)\n", result.manifests.len()),
            );
            for line in result.log.lines() {
                store.append_log(Some("(Starlingfile)"), "INFO", &format!("{line}\n"));
            }
            if let Some(handle) = &proxy_handle {
                for (name, port) in &result.aliases {
                    handle.register(name, *port).await;
                    store.append_log(
                        Some("(Starlingfile)"),
                        "INFO",
                        &format!("alias: {} -> 127.0.0.1:{port}\n", handle.url_for(name)),
                    );
                }
            }
            let eng = engine::Engine::new(
                store.clone(),
                result.manifests,
                build_rx,
                build_tx.clone(),
                args.dry_run,
                config_path.clone(),
                result.config_files,
                restart_rx,
                proxy_handle.clone(),
            );
            tokio::spawn(eng.run());
        }
        Err(e) => {
            store.append_log(
                Some("(Starlingfile)"),
                "ERROR",
                &format!("Failed to load Starlingfile at {}: {e}\n", config_path.display()),
            );
        }
    }

    // Reporter: push state to the daemon and execute queued commands.
    if let Some((client, instance)) = daemon_instance.clone() {
        let store = store.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            loop {
                tick.tick().await;
                let view = store.full_view();
                let resources: Vec<ResourceSnapshot> =
                    view.ui_resources.iter().map(snapshot).collect();
                let logs: HashMap<String, Vec<String>> =
                    store.recent_logs_by_resource(120).into_iter().collect();
                let _ = client
                    .call(&Request::Update {
                        instance: instance.clone(),
                        resources,
                        logs,
                    })
                    .await;
                if let Ok(Response::Commands(cmds)) = client
                    .call(&Request::PollCommands {
                        instance: instance.clone(),
                    })
                    .await
                {
                    for c in cmds {
                        match c {
                            DaemonCommand::Trigger { resource } => {
                                let _ = store.trigger(&resource);
                            }
                            DaemonCommand::Restart { resource } => {
                                let _ = store.restart(&resource);
                            }
                        }
                    }
                }
            }
        });
    }

    // Optional legacy web UI for this instance.
    if args.web {
        let state = AppState {
            store: store.clone(),
            csrf_token: uuid::Uuid::new_v4().to_string(),
        };
        let app = server::router(state, &args.web_dir);
        let addr = format!("{}:{}", args.host, args.port);
        match tokio::net::TcpListener::bind(&addr).await {
            Ok(l) => {
                println!("web UI on http://{addr}/");
                tokio::spawn(async move {
                    let _ = axum::serve(l, app).await;
                });
            }
            Err(e) => eprintln!("starling: web UI bind {addr} failed: {e}"),
        }
    }

    if args.tailscale {
        netmodes::tailscale_serve(args.proxy_port);
    }

    println!("starling up running — open the dashboard with `starling`. Ctrl-C to stop.");

    let _ = tokio::signal::ctrl_c().await;
    if let Some((client, instance)) = daemon_instance {
        let _ = client.call(&Request::Deregister { instance }).await;
    }
}
