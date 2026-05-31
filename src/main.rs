//! Starling: a local dev orchestrator, ported from Tilt (tilt.dev) + portless.
//!
//! Architecture: a single background **daemon** owns the shared named-URL proxy
//! and allocates ports centrally, so multiple `starling up` instances never
//! collide. Each `starling up` runs an engine for one project and reports its
//! resources to the daemon. `starling` (or `starling dash`) opens a k9s-style
//! TUI showing every instance's resources.

mod api;
mod certs;
mod ci;
mod daemon;
mod engine;
mod health;
mod k8s;
mod kube_client;
mod netmodes;
mod probe;
mod proxy;
mod seed;
mod server;
mod starlingfile;
mod store;
mod tui;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};
use tokio::sync::{mpsc, Mutex};

use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{
    ApiObjectSnapshot, Command as DaemonCommand, InstanceState, Request, ResourceSnapshot, Response,
};
use crate::proxy::{ProxyConfig, ProxyHandle, ProxyRegistry};
use crate::server::AppState;
use crate::store::Store;

#[derive(Parser)]
#[command(
    name = "starling",
    version,
    about = "Starling: orchestrate your local dev services with named URLs"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start an instance for the current project (registers with the daemon).
    Up(UpArgs),
    /// Stop the running instance(s) for the current project.
    Down(DownArgs),
    /// Print daemon, resource, and named-route status.
    Status(StatusArgs),
    /// Fetch recent logs for one or more resources.
    Logs(LogsArgs),
    /// Install agent skills for Codex and Claude Code.
    Skills(SkillsArgs),
    /// Run the central daemon (auto-started by `up`/`dash` if not running).
    Daemon(DaemonArgs),
    /// Bring the project up once in batch mode and exit 0/non-zero based on
    /// whether everything came up (Tilt's `tilt ci`).
    Ci(CiArgs),
    /// List API objects from the running instance(s) (Tilt's `tilt get`).
    Get(GetArgs),
    /// Show a single API object in detail (Tilt's `tilt describe`).
    Describe(DescribeArgs),
    /// List the API object kinds available (Tilt's `tilt api-resources`).
    ApiResources,
    /// Print the Starling version (Tilt's `tilt version`).
    Version,
    /// Describe the spec fields of an API object kind (Tilt's `tilt explain`).
    Explain(ExplainArgs),
    /// Print a diagnostic bundle: version, environment, daemon + resource health
    /// (Tilt's `tilt doctor`).
    Doctor,
    /// Replace the running instance's Tiltfile args and reload (Tilt's `tilt args`).
    Args(ArgsArgs),
    /// Write a JSON snapshot of the current dashboard state (Tilt's `tilt snapshot`).
    Snapshot(SnapshotArgs),
    /// Dump internal state as JSON to stdout (Tilt's `tilt dump`).
    Dump(DumpArgs),
    /// Queue a build for a resource (Tilt's `tilt trigger`).
    Trigger(ResourceArgs),
    /// Enable (resume) a paused resource (Tilt's `tilt enable`).
    Enable(ResourceArgs),
    /// Disable (pause) a resource (Tilt's `tilt disable`).
    Disable(ResourceArgs),
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
    /// Config to load. Defaults to ./Starlingfile, falling back to ./Tiltfile
    /// (Starling runs existing Tiltfiles unchanged).
    #[arg(long)]
    file: Option<String>,
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
    /// Tiltfile args passed after `--`.
    #[arg(last = true)]
    tiltfile_args: Vec<String>,
}

#[derive(Parser)]
struct GetArgs {
    /// Object kind to list (e.g. KubernetesApply, Cmd, FileWatch). Omit to list
    /// the available kinds.
    kind: Option<String>,
    /// Optional object name to fetch a single object.
    name: Option<String>,
    /// Emit the raw object JSON instead of a summary table.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Parser)]
struct CiArgs {
    /// Config to load. Defaults to ./Starlingfile, falling back to ./Tiltfile.
    #[arg(long)]
    file: Option<String>,
    /// Apply Kubernetes manifests with `--dry-run=client` (nothing mutated).
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// Fail the run if it has not settled within this many seconds. Overrides
    /// `ci_settings(timeout=...)`; defaults to that, else 300s.
    #[arg(long)]
    timeout: Option<u64>,
    /// Tiltfile args passed after `--`.
    #[arg(last = true)]
    tiltfile_args: Vec<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DumpTarget {
    /// Aggregated dashboard state (instances, resources, routes).
    State,
    /// All API objects across instances.
    Objects,
    /// The generated OpenAPI document for the tilt.dev/v1alpha1 types.
    Openapi,
}

#[derive(Parser)]
struct DumpArgs {
    /// What to dump.
    #[arg(value_enum)]
    target: DumpTarget,
}

#[derive(Parser)]
struct SnapshotArgs {
    /// Output file path (defaults to ./starling-snapshot.json).
    #[arg(long, default_value = "starling-snapshot.json")]
    out: String,
}

#[derive(Parser)]
struct ArgsArgs {
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long)]
    file: Option<String>,
    /// New Tiltfile args (after `--`). Pass none to clear them.
    #[arg(last = true)]
    tiltfile_args: Vec<String>,
}

#[derive(Parser)]
struct ResourceArgs {
    /// Resource name.
    resource: String,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long)]
    file: Option<String>,
}

#[derive(Parser)]
struct ExplainArgs {
    /// Object kind to explain (e.g. KubernetesApply, Cmd). Omit to list kinds.
    kind: Option<String>,
}

#[derive(Parser)]
struct DescribeArgs {
    /// Object kind (e.g. KubernetesApply, Cmd, FileWatch).
    kind: String,
    /// Object name.
    name: String,
}

#[derive(Parser)]
struct DownArgs {
    /// Config to identify the project. Defaults to ./Starlingfile, falling back
    /// to ./Tiltfile, matching `starling up`.
    #[arg(long)]
    file: Option<String>,
}

#[derive(Parser)]
struct StatusArgs {
    /// Emit machine-readable JSON.
    #[arg(long, default_value_t = false)]
    json: bool,
    /// Skip probing route backend ports.
    #[arg(long, default_value_t = false)]
    no_check: bool,
}

#[derive(Parser)]
struct LogsArgs {
    /// Resource name to fetch. Omit to fetch logs for all resources.
    resource: Option<String>,
    /// Instance id or project name to narrow matches.
    #[arg(long)]
    instance: Option<String>,
    /// Number of recent log lines per resource.
    #[arg(long, default_value_t = 120)]
    tail: usize,
    /// Emit machine-readable JSON.
    #[arg(long, default_value_t = false)]
    json: bool,
}

#[derive(Parser)]
struct SkillsArgs {
    #[command(subcommand)]
    command: SkillsCommand,
}

#[derive(Subcommand)]
enum SkillsCommand {
    /// Install the Starling skill for Codex and/or Claude Code.
    Install(SkillsInstallArgs),
}

#[derive(Parser)]
struct SkillsInstallArgs {
    /// Agent to install for.
    #[arg(long, value_enum, default_value_t = SkillTarget::All)]
    target: SkillTarget,
    /// Install into user-level or project-local skill directories.
    #[arg(long, value_enum, default_value_t = SkillScope::User)]
    scope: SkillScope,
    /// Replace an existing installed skill.
    #[arg(long, default_value_t = false)]
    force: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SkillTarget {
    Codex,
    Claude,
    All,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SkillScope {
    User,
    Project,
}

#[derive(Parser)]
struct DaemonArgs {
    #[command(subcommand)]
    service: Option<DaemonServiceCommand>,
    /// Stop all running instances and terminate the daemon.
    #[arg(long, default_value_t = false)]
    shutdown: bool,
    /// Restart the daemon, or start it if it is not running.
    #[arg(long, default_value_t = false)]
    restart: bool,
    /// Alias for --restart.
    #[arg(long, default_value_t = false)]
    reload: bool,
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

#[derive(Subcommand)]
enum DaemonServiceCommand {
    /// macOS only: install and start a user LaunchAgent for the daemon.
    InstallService,
    /// macOS only: stop and remove the user LaunchAgent for the daemon.
    UninstallService,
    /// macOS only: start the installed user LaunchAgent for the daemon.
    StartService,
    /// macOS only: stop the installed user LaunchAgent for the daemon.
    StopService,
    /// macOS only: print user LaunchAgent status for the daemon.
    ServiceStatus,
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
        Some(Command::Daemon(a)) => {
            if let Some(service) = &a.service {
                if a.shutdown || a.restart || a.reload {
                    eprintln!(
                        "starling daemon: service commands cannot be combined with --shutdown/--restart/--reload"
                    );
                    std::process::exit(2);
                }
                if let Err(e) = daemon_service(service, &a).await {
                    eprintln!("starling daemon: {e}");
                    std::process::exit(1);
                }
            } else if a.shutdown && (a.restart || a.reload) {
                eprintln!("starling daemon: --shutdown cannot be combined with --restart/--reload");
                std::process::exit(2);
            } else if a.shutdown {
                if let Err(e) = shutdown_daemon().await {
                    eprintln!("starling daemon: {e}");
                    std::process::exit(1);
                }
            } else if a.restart || a.reload {
                if let Err(e) = restart_daemon(&a).await {
                    eprintln!("starling daemon: {e}");
                    std::process::exit(1);
                }
            } else {
                daemon::run(a.proxy_port, a.tld, a.host, a.tls, a.lan).await;
            }
        }
        Some(Command::Up(a)) => up(a).await,
        Some(Command::Down(a)) => {
            if let Err(e) = down(a).await {
                eprintln!("starling down: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Status(a)) => {
            if let Err(e) = status(a).await {
                eprintln!("starling status: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Logs(a)) => {
            if let Err(e) = logs(a).await {
                eprintln!("starling logs: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Skills(a)) => {
            if let Err(e) = skills(a) {
                eprintln!("starling skills: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Ci(a)) => {
            // ci() exits the process itself with the right code.
            ci(a).await;
        }
        Some(Command::Get(a)) => {
            if let Err(e) = get(a).await {
                eprintln!("starling get: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Describe(a)) => {
            if let Err(e) = describe(a).await {
                eprintln!("starling describe: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::ApiResources) => {
            if let Err(e) = get(GetArgs {
                kind: None,
                name: None,
                json: false,
            })
            .await
            {
                eprintln!("starling api-resources: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Version) => {
            println!("starling {}", env!("CARGO_PKG_VERSION"));
        }
        Some(Command::Explain(a)) => {
            if let Err(e) = explain(a) {
                eprintln!("starling explain: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Doctor) => {
            if let Err(e) = doctor().await {
                eprintln!("starling doctor: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Args(a)) => {
            if let Err(e) = set_args(a).await {
                eprintln!("starling args: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Snapshot(a)) => {
            if let Err(e) = write_snapshot(a).await {
                eprintln!("starling snapshot: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Dump(a)) => {
            if let Err(e) = dump(a).await {
                eprintln!("starling dump: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Trigger(a)) => {
            if let Err(e) = resource_command(a, ResourceAction::Trigger).await {
                eprintln!("starling trigger: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Enable(a)) => {
            if let Err(e) = resource_command(a, ResourceAction::Enable).await {
                eprintln!("starling enable: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Disable(a)) => {
            if let Err(e) = resource_command(a, ResourceAction::Disable).await {
                eprintln!("starling disable: {e}");
                std::process::exit(1);
            }
        }
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
    let name = r
        .metadata
        .as_ref()
        .map(|m| m.name.clone())
        .unwrap_or_default();
    let st = r.status.clone().unwrap_or_default();
    let kind = st
        .specs
        .first()
        .and_then(|s| s.target_type.clone())
        .unwrap_or_else(|| "local".into());
    let pod = st
        .k8s_resource_info
        .as_ref()
        .and_then(|k| k.pod_name.clone());
    let url = st.endpoint_links.first().and_then(|l| l.url.clone());
    let proxy_condition = st
        .conditions
        .iter()
        .find(|c| c.condition_type == "ProxyReachable");
    let local = st.local_resource_info.as_ref();
    let paused = st
        .disable_status
        .as_ref()
        .map(|d| d.state == "Disabled")
        .unwrap_or(false);
    ResourceSnapshot {
        name,
        kind,
        paused,
        update_status: st.update_status.unwrap_or_default(),
        runtime_status: st.runtime_status.unwrap_or_default(),
        pod,
        url,
        proxy_status: proxy_condition.map(|c| c.status.clone()),
        proxy_message: proxy_condition.and_then(|c| c.message.clone()),
        build_count: st.build_history.len() as u32,
        last_deploy: st.last_deploy_time,
        restart_count: local
            .and_then(|l| l.restart_count)
            .and_then(|count| u32::try_from(count).ok()),
        last_start: local.and_then(|l| l.last_start_time.clone()),
    }
}

fn config_error_resource(name: &str, message: &str) -> api::v1alpha1::UIResource {
    use api::v1alpha1::*;
    UIResource {
        metadata: Some(ObjectMeta {
            name: name.to_string(),
            uid: uuid::Uuid::new_v4().to_string(),
            ..Default::default()
        }),
        spec: Some(UIResourceSpec {}),
        status: Some(UIResourceStatus {
            runtime_status: Some("not_applicable".to_string()),
            update_status: Some("error".to_string()),
            conditions: vec![UIResourceCondition {
                condition_type: "ConfigLoaded".to_string(),
                status: "False".to_string(),
                last_transition_time: Some(chrono::Utc::now().to_rfc3339()),
                reason: Some("ConfigLoadFailed".to_string()),
                message: Some(message.to_string()),
            }],
            ..Default::default()
        }),
    }
}

/// Resolve which config file to load: an explicit `--file`, else `./Starlingfile`,
/// else `./Tiltfile` — so `starling up` runs an existing Tilt project unchanged.
fn resolve_config(explicit: Option<&str>) -> PathBuf {
    if let Some(f) = explicit {
        return PathBuf::from(f);
    }
    for candidate in ["Starlingfile", "Tiltfile"] {
        if std::path::Path::new(candidate).exists() {
            return PathBuf::from(candidate);
        }
    }
    PathBuf::from("Starlingfile")
}

/// Log-span label for the config file, e.g. `(Tiltfile)` or `(Starlingfile)`.
fn config_span(path: &std::path::Path) -> String {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("Starlingfile");
    format!("({name})")
}

/// Infer a project name from the config file's directory.
fn project_name(file: &PathBuf) -> String {
    let dir = project_dir_path(file);
    dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("app")
        .to_string()
}

/// Infer the canonical project directory from the config file path.
fn project_dir_path(file: &PathBuf) -> PathBuf {
    std::fs::canonicalize(file)
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()))
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_default()
}

fn project_dir(file: &PathBuf) -> String {
    project_dir_path(file).display().to_string()
}

async fn get(args: GetArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();

    // No kind given: list the available object kinds.
    let Some(kind) = args.kind.clone() else {
        let Response::Objects(objects) = client.call(&Request::GetObjects { kind: None }).await?
        else {
            anyhow::bail!("daemon returned unexpected response");
        };
        let mut kinds: Vec<String> = objects.into_iter().map(|o| o.kind).collect();
        kinds.sort();
        kinds.dedup();
        if kinds.is_empty() {
            println!("No API objects found (is an instance running?)");
        } else {
            println!("Available kinds:");
            for k in kinds {
                println!("  {k}");
            }
        }
        return Ok(());
    };

    let Response::Objects(mut objects) = client
        .call(&Request::GetObjects {
            kind: Some(kind.clone()),
        })
        .await?
    else {
        anyhow::bail!("daemon returned unexpected response");
    };
    if let Some(name) = &args.name {
        objects.retain(|o| &o.name == name);
        if objects.is_empty() {
            anyhow::bail!("{kind} \"{name}\" not found");
        }
    }
    if objects.is_empty() {
        println!("No {kind} objects found");
        return Ok(());
    }

    if args.json {
        let values: Vec<&serde_json::Value> = objects.iter().map(|o| &o.object).collect();
        // A single named object prints bare; a list prints as an array.
        if args.name.is_some() && values.len() == 1 {
            println!("{}", serde_json::to_string_pretty(values[0])?);
        } else {
            println!("{}", serde_json::to_string_pretty(&values)?);
        }
        return Ok(());
    }

    println!("{:<28} {}", "NAME", "KIND");
    for o in &objects {
        println!("{:<28} {}", o.name, o.kind);
    }
    Ok(())
}

async fn describe(args: DescribeArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let Response::Objects(objects) = client
        .call(&Request::GetObjects {
            kind: Some(args.kind.clone()),
        })
        .await?
    else {
        anyhow::bail!("daemon returned unexpected response");
    };
    let Some(obj) = objects.into_iter().find(|o| o.name == args.name) else {
        anyhow::bail!("{} \"{}\" not found", args.kind, args.name);
    };

    println!("Name:    {}", obj.name);
    println!("Kind:    {}", obj.kind);
    if let Some(meta) = obj.object.get("metadata") {
        if let Some(uid) = meta.get("uid").and_then(|v| v.as_str()) {
            println!("UID:     {uid}");
        }
        if let Some(rv) = meta.get("resourceVersion").and_then(|v| v.as_str()) {
            println!("Version: {rv}");
        }
    }
    if let Some(spec) = obj.object.get("spec") {
        println!("Spec:\n{}", serde_json::to_string_pretty(spec)?);
    }
    if let Some(status) = obj.object.get("status") {
        println!("Status:\n{}", serde_json::to_string_pretty(status)?);
    }
    Ok(())
}

/// `starling ci`: bring the project up headless (no daemon/proxy), wait for it
/// to settle, and exit 0 if everything came up, non-zero on failure/timeout.
async fn ci(args: CiArgs) -> ! {
    let config_path = resolve_config(args.file.as_deref());
    let result = match starlingfile::load_with_options(
        &config_path,
        starlingfile::LoadOptions {
            args: args.tiltfile_args.clone(),
            ..starlingfile::LoadOptions::default()
        },
    ) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("starling ci: failed to load {}: {e}", config_path.display());
            std::process::exit(1);
        }
    };
    let expected = result.manifests.len();
    let max_parallel_updates = result.max_parallel_updates;
    // --timeout overrides ci_settings(timeout=...), which defaults to 300s.
    let timeout_secs = args.timeout.or(result.ci_timeout_secs).unwrap_or(300);

    // Headless engine: no daemon, no proxy. Keep the control-channel senders
    // alive so the engine's select loop doesn't spin on closed receivers.
    let (build_tx, build_rx) = mpsc::unbounded_channel::<store::BuildRequest>();
    let (_restart_tx, restart_rx) = mpsc::unbounded_channel::<String>();
    let (_tiltfile_args_tx, tiltfile_args_rx) = mpsc::unbounded_channel::<Vec<String>>();
    let (_port_tx, port_rx) = mpsc::unbounded_channel::<(String, u16)>();
    let store = Arc::new(Store::new(build_tx.clone()));
    let api_objects = Arc::new(api::store::ApiObjectStore::new());
    let eng = engine::Engine::new(
        store.clone(),
        result.manifests,
        build_rx,
        build_tx.clone(),
        args.dry_run,
        config_path.clone(),
        args.tiltfile_args.clone(),
        result.config_files,
        result.port_leases,
        restart_rx,
        tiltfile_args_rx,
        port_rx,
        None,
        api_objects,
        max_parallel_updates,
    );
    tokio::spawn(eng.run());

    println!("starling ci: bringing up {} ...", config_path.display());
    let mut elapsed_ms = 0u64;
    let timeout_ms = timeout_secs.saturating_mul(1000);
    loop {
        tokio::time::sleep(Duration::from_millis(250)).await;
        elapsed_ms += 250;
        let view = store.full_view();
        // Wait until every expected resource has materialized before judging,
        // so an empty initial view isn't mistaken for "done".
        if view.ui_resources.len() >= expected {
            match ci::ci_outcome(&view.ui_resources) {
                ci::CiOutcome::Done => {
                    println!("starling ci: all {expected} resource(s) are up.");
                    std::process::exit(0);
                }
                ci::CiOutcome::Failed => {
                    eprintln!("starling ci: one or more resources failed:");
                    for r in &view.ui_resources {
                        let st = r.status.as_ref();
                        let update = st
                            .and_then(|s| s.update_status.as_deref())
                            .unwrap_or("none");
                        let runtime = st
                            .and_then(|s| s.runtime_status.as_deref())
                            .unwrap_or("none");
                        if update == "error" || runtime == "error" {
                            let name = r
                                .metadata
                                .as_ref()
                                .map(|m| m.name.as_str())
                                .unwrap_or("(unknown)");
                            eprintln!("  {name}: update={update} runtime={runtime}");
                        }
                    }
                    std::process::exit(1);
                }
                ci::CiOutcome::Pending => {}
            }
        }
        if elapsed_ms >= timeout_ms {
            eprintln!("starling ci: timed out after {timeout_secs}s before all resources settled");
            std::process::exit(1);
        }
    }
}

/// `starling dump <target>`: print internal state as JSON to stdout.
async fn dump(args: DumpArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    match args.target {
        DumpTarget::State => {
            let Response::State(state) = client.call(&Request::GetState).await? else {
                anyhow::bail!("could not query daemon state");
            };
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "proxyPort": state.proxy_port,
                    "tld": state.tld,
                    "instances": state.instances,
                    "routes": state.routes,
                }))?
            );
        }
        DumpTarget::Objects => {
            let Response::Objects(objects) =
                client.call(&Request::GetObjects { kind: None }).await?
            else {
                anyhow::bail!("could not query daemon objects");
            };
            let items: Vec<_> = objects.into_iter().map(|o| o.object).collect();
            println!("{}", serde_json::to_string_pretty(&items)?);
        }
        DumpTarget::Openapi => {
            // Generated locally; no daemon needed.
            println!(
                "{}",
                serde_json::to_string_pretty(&api::store::openapi_document())?
            );
        }
    }
    Ok(())
}

/// `starling snapshot`: write the current aggregated dashboard state (instances,
/// resources, routes) to a JSON file for sharing/inspection.
async fn write_snapshot(args: SnapshotArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    let json = serde_json::to_string_pretty(&serde_json::json!({
        "proxyPort": state.proxy_port,
        "tld": state.tld,
        "instances": state.instances,
        "routes": state.routes,
    }))?;
    std::fs::write(&args.out, json)
        .map_err(|e| anyhow::anyhow!("writing snapshot to {}: {e}", args.out))?;
    println!("Wrote snapshot to {}", args.out);
    Ok(())
}

/// `starling explain [kind]`: describe an API object kind's spec fields, or list
/// the available kinds when no kind is given.
fn explain(args: ExplainArgs) -> anyhow::Result<()> {
    let Some(kind) = args.kind else {
        println!("Object kinds (use `starling explain <kind>`):");
        for k in api::store::known_kinds() {
            println!("  {k}");
        }
        return Ok(());
    };
    // Case-insensitive match against the known kinds.
    let resolved = api::store::known_kinds()
        .into_iter()
        .find(|k| k.eq_ignore_ascii_case(&kind));
    let Some(resolved) = resolved else {
        anyhow::bail!("unknown kind {kind:?} (try `starling explain` to list kinds)");
    };
    let fields = api::store::spec_fields(resolved).unwrap_or(&[]);
    println!("{resolved} (tilt.dev/v1alpha1)");
    println!("spec fields:");
    for f in fields {
        println!("  {f}");
    }
    Ok(())
}

/// `starling doctor`: print a diagnostic bundle — version, environment, daemon
/// health, and any resources currently in an error state.
async fn doctor() -> anyhow::Result<()> {
    println!("starling {}", env!("CARGO_PKG_VERSION"));
    println!(
        "os/arch:  {}/{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    );
    println!(
        "kubectl:  {}",
        if which_ok("kubectl") {
            "found"
        } else {
            "not found"
        }
    );
    println!(
        "docker:   {}",
        if which_ok("docker") {
            "found"
        } else {
            "not found"
        }
    );

    let client = DaemonClient::new();
    if !client.is_running().await {
        println!("daemon:   not running");
        return Ok(());
    }
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    println!(
        "daemon:   running (proxy :{}  tld {})",
        state.proxy_port, state.tld
    );
    println!(
        "instances: {}  routes: {}",
        state.instances.len(),
        state.routes.len()
    );

    let mut problems = 0;
    for inst in &state.instances {
        for r in &inst.resources {
            if r.update_status == "error" || r.runtime_status == "error" {
                problems += 1;
                println!(
                    "  [error] {} / {} (update={}, runtime={})",
                    inst.name, r.name, r.update_status, r.runtime_status
                );
            }
        }
    }
    if problems == 0 {
        println!("health:   all resources healthy");
    } else {
        println!("health:   {problems} resource(s) in error");
    }
    Ok(())
}

/// Whether `bin` is found on `PATH` (best-effort, for diagnostics).
fn which_ok(bin: &str) -> bool {
    std::process::Command::new(bin)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// `starling args`: replace the running instance's Tiltfile args and reload.
async fn set_args(args: ArgsArgs) -> anyhow::Result<()> {
    let config_path = resolve_config(args.file.as_deref());
    let dir = project_dir(&config_path);
    let client = DaemonClient::new();

    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    let instances: Vec<String> = state
        .instances
        .iter()
        .filter(|i| i.dir == dir)
        .map(|i| i.id.clone())
        .collect();
    if instances.is_empty() {
        anyhow::bail!("no running Starling instance found for {dir}");
    }
    for instance in instances {
        match client
            .call(&Request::SetTiltfileArgs {
                instance,
                args: args.tiltfile_args.clone(),
            })
            .await?
        {
            Response::Ok => {}
            Response::Error(e) => anyhow::bail!("{e}"),
            other => anyhow::bail!("unexpected daemon response: {other:?}"),
        }
    }
    if args.tiltfile_args.is_empty() {
        println!("Cleared Tiltfile args; reloading.");
    } else {
        println!("Set Tiltfile args to {:?}; reloading.", args.tiltfile_args);
    }
    Ok(())
}

enum ResourceAction {
    Trigger,
    Enable,
    Disable,
}

/// Send a per-resource action (`trigger`/`enable`/`disable`) to every instance
/// running the current project, via the daemon's command queue.
async fn resource_command(args: ResourceArgs, action: ResourceAction) -> anyhow::Result<()> {
    let config_path = resolve_config(args.file.as_deref());
    let dir = project_dir(&config_path);
    let client = DaemonClient::new();

    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    let instances: Vec<String> = state
        .instances
        .iter()
        .filter(|i| i.dir == dir)
        .map(|i| i.id.clone())
        .collect();
    if instances.is_empty() {
        anyhow::bail!("no running Starling instance found for {dir}");
    }

    let verb = match action {
        ResourceAction::Trigger => "Triggered",
        ResourceAction::Enable => "Enabled",
        ResourceAction::Disable => "Disabled",
    };
    for instance in instances {
        let req = match action {
            ResourceAction::Trigger => Request::Trigger {
                instance,
                resource: args.resource.clone(),
            },
            ResourceAction::Enable => Request::SetPaused {
                instance,
                resource: args.resource.clone(),
                paused: false,
            },
            ResourceAction::Disable => Request::SetPaused {
                instance,
                resource: args.resource.clone(),
                paused: true,
            },
        };
        match client.call(&req).await? {
            Response::Ok => {}
            Response::Error(e) => anyhow::bail!("{e}"),
            other => anyhow::bail!("unexpected daemon response: {other:?}"),
        }
    }
    println!("{verb} {}", args.resource);
    Ok(())
}

async fn down(args: DownArgs) -> anyhow::Result<()> {
    let config_path = resolve_config(args.file.as_deref());
    let dir = project_dir(&config_path);
    let client = DaemonClient::new();
    let resp = client
        .call(&Request::ShutdownProject { dir: dir.clone() })
        .await?;
    let Response::ShutdownQueued { instances } = resp else {
        anyhow::bail!("daemon returned unexpected response: {resp:?}");
    };

    if instances.is_empty() {
        println!("No running Starling instance found for {dir}");
        return Ok(());
    }

    let ids: Vec<String> = instances.iter().map(|i| i.id.clone()).collect();
    println!(
        "Stopping {} Starling instance{} for {dir}...",
        ids.len(),
        if ids.len() == 1 { "" } else { "s" }
    );

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if instances.iter().all(|i| !pid_is_running(i.pid)) {
            println!("Stopped.");
            return Ok(());
        }
        match client.call(&Request::GetState).await {
            Ok(Response::State(state)) => {
                if ids
                    .iter()
                    .all(|id| !state.instances.iter().any(|i| &i.id == id))
                {
                    println!("Stopped.");
                    return Ok(());
                }
            }
            Ok(_) => {}
            Err(_) => {}
        }
    }

    anyhow::bail!(
        "shutdown was queued, but {} instance{} did not stop within 5s",
        ids.len(),
        if ids.len() == 1 { "" } else { "s" }
    )
}

async fn status(args: StatusArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };

    let mut route_checks = Vec::new();
    if !args.no_check {
        for route in &state.routes {
            let backend_open = route_open(route.port).await;
            let proxy_health =
                health::check_proxy_route(&route.hostname, state.proxy_port, "starling-status")
                    .await;
            route_checks.push((
                route.hostname.clone(),
                route.port,
                backend_open,
                proxy_health,
            ));
        }
    }

    if args.json {
        let route_status: Vec<_> = state
            .routes
            .iter()
            .map(|route| {
                let reachable = route_checks
                    .iter()
                    .find(|(host, _, _, _)| host == &route.hostname)
                    .map(|(_, _, ok, _)| *ok);
                let proxy_status = route_checks
                    .iter()
                    .find(|(host, _, _, _)| host == &route.hostname)
                    .and_then(|(_, _, _, health)| health.status);
                let proxy_reachable = route_checks
                    .iter()
                    .find(|(host, _, _, _)| host == &route.hostname)
                    .map(|(_, _, _, health)| health.ok);
                let proxy_message = route_checks
                    .iter()
                    .find(|(host, _, _, _)| host == &route.hostname)
                    .map(|(_, _, _, health)| health.message.clone());
                serde_json::json!({
                    "hostname": route.hostname,
                    "port": route.port,
                    "instance": route.instance,
                    "reachable": reachable,
                    "proxy_reachable": proxy_reachable,
                    "proxy_status": proxy_status,
                    "proxy_message": proxy_message,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "proxy_port": state.proxy_port,
                "tld": state.tld,
                "instances": state.instances,
                "routes": route_status,
            }))?
        );
        return Ok(());
    }

    println!("Daemon: proxy :{}  tld {}", state.proxy_port, state.tld);
    if state.instances.is_empty() {
        println!("No running instances.");
    }
    for inst in &state.instances {
        println!(
            "\nInstance {}  name={}  pid={}  dir={}",
            inst.id, inst.name, inst.pid, inst.dir
        );
        if inst.resources.is_empty() {
            println!("  No resources.");
            continue;
        }
        println!(
            "  {:<28} {:<10} {:<8} {:<12} {:<14} {:<8} {}",
            "RESOURCE", "KIND", "STATE", "UPDATE", "RUNTIME", "PROXY", "URL"
        );
        for res in &inst.resources {
            println!(
                "  {:<28} {:<10} {:<8} {:<12} {:<14} {:<8} {}",
                res.name,
                res.kind,
                if res.paused { "paused" } else { "active" },
                empty_dash(&res.update_status),
                empty_dash(&res.runtime_status),
                proxy_condition_label(res.proxy_status.as_deref()),
                res.url.as_deref().unwrap_or("-")
            );
        }
    }

    println!("\nRoutes:");
    if state.routes.is_empty() {
        println!("  No routes.");
    }
    for route in &state.routes {
        let reachable = route_checks
            .iter()
            .find(|(host, _, _, _)| host == &route.hostname)
            .map(|(_, _, ok, _)| if *ok { "open" } else { "closed" })
            .unwrap_or("not checked");
        let proxy = route_checks
            .iter()
            .find(|(host, _, _, _)| host == &route.hostname)
            .map(|(_, _, _, health)| proxy_route_label(health))
            .unwrap_or_else(|| "not checked".to_string());
        println!(
            "  {:<40} -> 127.0.0.1:{:<5} backend={:<11} proxy={:<9} instance={}",
            route.hostname, route.port, reachable, proxy, route.instance
        );
        if let Some((_, _, _, health)) = route_checks
            .iter()
            .find(|(host, _, _, _)| host == &route.hostname)
        {
            if !health.ok {
                println!("    proxy: {}", health.message);
            }
        }
    }

    Ok(())
}

async fn logs(args: LogsArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };

    let mut matches = Vec::new();
    for inst in &state.instances {
        if let Some(filter) = &args.instance {
            if &inst.id != filter && &inst.name != filter {
                continue;
            }
        }
        for res in &inst.resources {
            if let Some(filter) = &args.resource {
                if &res.name != filter {
                    continue;
                }
            }
            matches.push((inst.id.clone(), inst.name.clone(), res.name.clone()));
        }
    }

    if matches.is_empty() {
        anyhow::bail!("no matching resources; run `starling status` to list resources");
    }

    let mut outputs = Vec::new();
    for (instance_id, instance_name, resource) in matches {
        let lines = match client
            .call(&Request::GetLogs {
                instance: instance_id.clone(),
                resource: resource.clone(),
                since: 0,
            })
            .await?
        {
            Response::Logs { lines, .. } => tail_lines(lines, args.tail),
            other => anyhow::bail!("daemon returned unexpected response: {other:?}"),
        };
        outputs.push((instance_id, instance_name, resource, lines));
    }

    if args.json {
        let items: Vec<_> = outputs
            .into_iter()
            .map(|(instance_id, instance_name, resource, lines)| {
                serde_json::json!({
                    "instance": instance_id,
                    "instance_name": instance_name,
                    "resource": resource,
                    "lines": lines,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    let multiple = outputs.len() > 1;
    for (instance_id, instance_name, resource, lines) in outputs {
        if multiple {
            println!("== {instance_name}/{resource} ({instance_id}) ==");
        }
        for line in lines {
            println!("{line}");
        }
        if multiple {
            println!();
        }
    }

    Ok(())
}

async fn route_open(port: u16) -> bool {
    tokio::time::timeout(
        Duration::from_millis(250),
        tokio::net::TcpStream::connect(("127.0.0.1", port)),
    )
    .await
    .map(|r| r.is_ok())
    .unwrap_or(false)
}

fn proxy_route_label(health: &health::ProxyHealth) -> String {
    match (health.ok, health.status) {
        (true, Some(code)) => format!("ok({code})"),
        (false, Some(code)) => format!("bad({code})"),
        (false, None) => "failed".to_string(),
        (true, None) => "ok".to_string(),
    }
}

fn proxy_condition_label(status: Option<&str>) -> &str {
    match status {
        Some("True") => "ok",
        Some("False") => "failed",
        Some(other) => other,
        None => "-",
    }
}

fn tail_lines(mut lines: Vec<String>, tail: usize) -> Vec<String> {
    if lines.len() > tail {
        lines.drain(0..lines.len() - tail);
    }
    lines
}

fn empty_dash(s: &str) -> &str {
    if s.is_empty() {
        "-"
    } else {
        s
    }
}

fn skills(args: SkillsArgs) -> anyhow::Result<()> {
    match args.command {
        SkillsCommand::Install(args) => install_skills(args),
    }
}

fn install_skills(args: SkillsInstallArgs) -> anyhow::Result<()> {
    let mut targets = Vec::new();
    match args.target {
        SkillTarget::Codex => targets.push(SkillTarget::Codex),
        SkillTarget::Claude => targets.push(SkillTarget::Claude),
        SkillTarget::All => {
            targets.push(SkillTarget::Codex);
            targets.push(SkillTarget::Claude);
        }
    }

    for target in targets {
        let dir = skill_install_dir(target, args.scope)?;
        let skill_dir = dir.join("starling-devex");
        let skill_file = skill_dir.join("SKILL.md");
        if skill_file.exists() && !args.force {
            anyhow::bail!(
                "{} already exists; rerun with --force to replace it",
                skill_file.display()
            );
        }
        std::fs::create_dir_all(&skill_dir)?;
        std::fs::write(&skill_file, starling_skill_markdown())?;
        println!(
            "Installed {} skill at {}",
            skill_target_name(target),
            skill_file.display()
        );
    }

    Ok(())
}

fn skill_install_dir(target: SkillTarget, scope: SkillScope) -> anyhow::Result<PathBuf> {
    let cwd = std::env::current_dir()?;
    let home =
        || dirs::home_dir().ok_or_else(|| anyhow::anyhow!("could not determine home directory"));
    match (target, scope) {
        (SkillTarget::Codex, SkillScope::User) => {
            if let Ok(home) = std::env::var("CODEX_HOME") {
                Ok(PathBuf::from(home).join("skills"))
            } else {
                Ok(home()?.join(".codex").join("skills"))
            }
        }
        (SkillTarget::Codex, SkillScope::Project) => Ok(cwd.join(".codex").join("skills")),
        (SkillTarget::Claude, SkillScope::User) => Ok(home()?.join(".claude").join("skills")),
        (SkillTarget::Claude, SkillScope::Project) => Ok(cwd.join(".claude").join("skills")),
        (SkillTarget::All, _) => unreachable!("expanded before install"),
    }
}

fn skill_target_name(target: SkillTarget) -> &'static str {
    match target {
        SkillTarget::Codex => "Codex",
        SkillTarget::Claude => "Claude",
        SkillTarget::All => "all",
    }
}

fn starling_skill_markdown() -> &'static str {
    r#"---
name: starling-devex
description: Use when operating or diagnosing Starling local dev environments, including named URL proxy issues, resource health checks, recent service logs, project shutdown, and daemon lifecycle management.
---

# Starling Dev Environment

Use the `starling` CLI instead of scraping the TUI when checking local dev environment state for users or agents.

## Core Commands

- `starling status --json`: machine-readable daemon state, instances, resources, routes, and route backend reachability.
- `starling status`: human-readable summary of the same state.
- `starling logs <resource> --tail 120 --json`: recent logs for a resource.
- `starling logs --instance <id-or-name> --json`: recent logs for all resources in one instance.
- `starling down --file <Starlingfile-or-Tiltfile>`: stop the running project instance matching that config directory.
- `starling daemon --shutdown`: stop all running instances and terminate the daemon.
- `starling daemon --restart`: restart the daemon, or start it if absent.

## Diagnosing 502 Named URLs

If a Starling URL returns `502 Bad Gateway`, run:

```bash
starling status --json
starling logs <resource> --tail 80
```

Interpretation:

- A route with `"reachable": false` means the Starling proxy has a route, but `127.0.0.1:<port>` is not accepting connections.
- If logs show the app listening on a different port than the route, update the Starlingfile resource to either honor `$PORT` or set `serve_port=<actual-port>`.
- If the process is running but bound to another interface or container-only address, bind it to `127.0.0.1` or expose the selected port on the host.

## Preferred Agent Workflow

1. Run `starling status --json`.
2. Identify the instance/resource and route backend port.
3. Run `starling logs <resource> --tail 120 --json`.
4. Recommend a concrete Starlingfile or service-command fix based on the port and logs.
5. Use `starling down` or `starling daemon --shutdown` only when asked to stop services.
"#
}

fn pid_is_running(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

async fn shutdown_daemon() -> anyhow::Result<()> {
    let client = DaemonClient::new();
    if !client.is_running().await {
        println!("No Starling daemon is running.");
        return Ok(());
    }
    let instances = request_daemon_shutdown(&client).await?;
    println!(
        "Stopping Starling daemon and {} instance{}...",
        instances.len(),
        if instances.len() == 1 { "" } else { "s" }
    );
    if wait_for_daemon_stop(&client, &instances).await {
        println!("Stopped.");
        Ok(())
    } else {
        anyhow::bail!("shutdown was queued, but the daemon did not stop within 8s")
    }
}

async fn restart_daemon(args: &DaemonArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    if client.is_running().await {
        let instances = request_daemon_shutdown(&client).await?;
        println!(
            "Restarting Starling daemon; stopping {} instance{} first...",
            instances.len(),
            if instances.len() == 1 { "" } else { "s" }
        );
        if !wait_for_daemon_stop(&client, &instances).await {
            anyhow::bail!("shutdown was queued, but the daemon did not stop within 8s");
        }
    } else {
        println!("No Starling daemon is running; starting one.");
    }
    if daemon_service_installed() {
        println!("Starting installed Starling daemon service.");
        start_daemon_service().await?;
        return Ok(());
    }
    client
        .ensure_running_with(args.proxy_port, &args.tld, &args.host, args.tls, args.lan)
        .await?;
    println!("Starling daemon is running.");
    Ok(())
}

async fn daemon_service(command: &DaemonServiceCommand, args: &DaemonArgs) -> anyhow::Result<()> {
    match command {
        DaemonServiceCommand::InstallService => install_daemon_service(args).await,
        DaemonServiceCommand::UninstallService => uninstall_daemon_service().await,
        DaemonServiceCommand::StartService => start_daemon_service().await,
        DaemonServiceCommand::StopService => stop_daemon_service().await,
        DaemonServiceCommand::ServiceStatus => daemon_service_status().await,
    }
}

#[cfg(target_os = "macos")]
const DAEMON_SERVICE_LABEL: &str = "com.thousandbirds.starling.daemon";

#[cfg(target_os = "macos")]
fn launch_agent_path() -> anyhow::Result<PathBuf> {
    Ok(dirs::home_dir()
        .ok_or_else(|| anyhow::anyhow!("could not resolve home directory"))?
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{DAEMON_SERVICE_LABEL}.plist")))
}

#[cfg(target_os = "macos")]
fn daemon_service_installed() -> bool {
    launch_agent_path()
        .map(|path| path.exists())
        .unwrap_or(false)
}

#[cfg(not(target_os = "macos"))]
fn daemon_service_installed() -> bool {
    false
}

#[cfg(target_os = "macos")]
fn launchd_domain() -> String {
    format!("gui/{}", unsafe { libc::getuid() })
}

#[cfg(target_os = "macos")]
fn launchd_target() -> String {
    format!("{}/{}", launchd_domain(), DAEMON_SERVICE_LABEL)
}

#[cfg(target_os = "macos")]
fn run_launchctl(args: &[&str]) -> anyhow::Result<std::process::Output> {
    std::process::Command::new("launchctl")
        .args(args)
        .output()
        .map_err(|e| anyhow::anyhow!("running launchctl {}: {e}", args.join(" ")))
}

#[cfg(target_os = "macos")]
fn service_loaded() -> bool {
    run_launchctl(&["print", &launchd_target()])
        .map(|out| out.status.success())
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(target_os = "macos")]
fn daemon_service_plist(args: &DaemonArgs) -> anyhow::Result<String> {
    let exe = std::env::current_exe()?;
    let log = daemon::protocol::log_path();
    let mut program_args = vec![
        exe.display().to_string(),
        "daemon".to_string(),
        "--proxy-port".to_string(),
        args.proxy_port.to_string(),
        "--tld".to_string(),
        args.tld.clone(),
        "--host".to_string(),
        args.host.clone(),
    ];
    if args.tls {
        program_args.push("--tls".to_string());
    }
    if args.lan {
        program_args.push("--lan".to_string());
    }
    let program_args = program_args
        .iter()
        .map(|arg| format!("        <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    let log = xml_escape(&log.display().to_string());
    Ok(format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{}</string>
    <key>ProgramArguments</key>
    <array>
{}
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>{}</string>
    <key>StandardErrorPath</key>
    <string>{}</string>
</dict>
</plist>
"#,
        DAEMON_SERVICE_LABEL, program_args, log, log
    ))
}

#[cfg(target_os = "macos")]
async fn install_daemon_service(args: &DaemonArgs) -> anyhow::Result<()> {
    let path = launch_agent_path()?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    if let Some(dir) = daemon::protocol::log_path().parent() {
        std::fs::create_dir_all(dir)?;
    }

    if DaemonClient::new().is_running().await {
        let _ = shutdown_daemon().await;
    }
    if service_loaded() {
        let _ = run_launchctl(&["bootout", &launchd_target()]);
    }

    std::fs::write(&path, daemon_service_plist(args)?)?;
    let out = run_launchctl(&[
        "bootstrap",
        &launchd_domain(),
        path.to_string_lossy().as_ref(),
    ])?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl bootstrap failed: {}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let _ = run_launchctl(&["kickstart", "-k", &launchd_target()]);
    wait_for_service_daemon().await?;
    println!(
        "Installed and started Starling daemon service at {}",
        path.display()
    );
    Ok(())
}

#[cfg(not(target_os = "macos"))]
async fn install_daemon_service(_args: &DaemonArgs) -> anyhow::Result<()> {
    anyhow::bail!("daemon service management is currently implemented for macOS launchd only")
}

#[cfg(target_os = "macos")]
async fn uninstall_daemon_service() -> anyhow::Result<()> {
    if service_loaded() {
        let out = run_launchctl(&["bootout", &launchd_target()])?;
        if !out.status.success() {
            anyhow::bail!(
                "launchctl bootout failed: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    let path = launch_agent_path()?;
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    println!("Uninstalled Starling daemon service.");
    Ok(())
}

#[cfg(not(target_os = "macos"))]
async fn uninstall_daemon_service() -> anyhow::Result<()> {
    anyhow::bail!("daemon service management is currently implemented for macOS launchd only")
}

#[cfg(target_os = "macos")]
async fn start_daemon_service() -> anyhow::Result<()> {
    let path = launch_agent_path()?;
    if !path.exists() {
        anyhow::bail!(
            "Starling daemon service is not installed; run `starling daemon install-service` first"
        );
    }
    if !service_loaded() {
        let out = run_launchctl(&[
            "bootstrap",
            &launchd_domain(),
            path.to_string_lossy().as_ref(),
        ])?;
        if !out.status.success() {
            anyhow::bail!(
                "launchctl bootstrap failed: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    let _ = run_launchctl(&["kickstart", "-k", &launchd_target()]);
    wait_for_service_daemon().await?;
    println!("Started Starling daemon service.");
    Ok(())
}

#[cfg(not(target_os = "macos"))]
async fn start_daemon_service() -> anyhow::Result<()> {
    anyhow::bail!("daemon service management is currently implemented for macOS launchd only")
}

#[cfg(target_os = "macos")]
async fn stop_daemon_service() -> anyhow::Result<()> {
    if service_loaded() {
        let out = run_launchctl(&["bootout", &launchd_target()])?;
        if !out.status.success() {
            anyhow::bail!(
                "launchctl bootout failed: {}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    let client = DaemonClient::new();
    for _ in 0..40 {
        if !client.is_running().await {
            println!("Stopped Starling daemon service.");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("service was stopped, but the daemon socket is still responding")
}

#[cfg(not(target_os = "macos"))]
async fn stop_daemon_service() -> anyhow::Result<()> {
    anyhow::bail!("daemon service management is currently implemented for macOS launchd only")
}

#[cfg(target_os = "macos")]
async fn daemon_service_status() -> anyhow::Result<()> {
    let path = launch_agent_path()?;
    let loaded = service_loaded();
    let daemon = DaemonClient::new().is_running().await;
    println!("Service: {}", DAEMON_SERVICE_LABEL);
    println!("Plist: {}", path.display());
    println!("Installed: {}", if path.exists() { "yes" } else { "no" });
    println!("Loaded: {}", if loaded { "yes" } else { "no" });
    println!("Daemon responding: {}", if daemon { "yes" } else { "no" });
    Ok(())
}

#[cfg(not(target_os = "macos"))]
async fn daemon_service_status() -> anyhow::Result<()> {
    anyhow::bail!("daemon service management is currently implemented for macOS launchd only")
}

#[cfg(target_os = "macos")]
async fn wait_for_service_daemon() -> anyhow::Result<()> {
    let client = DaemonClient::new();
    for _ in 0..80 {
        if client.is_running().await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "service started, but the daemon did not respond within 8s; see {}",
        daemon::protocol::log_path().display()
    )
}

async fn request_daemon_shutdown(client: &DaemonClient) -> anyhow::Result<Vec<InstanceState>> {
    let resp = client.call(&Request::ShutdownDaemon).await?;
    let Response::ShutdownQueued { instances } = resp else {
        anyhow::bail!("daemon returned unexpected response: {resp:?}");
    };
    Ok(instances)
}

async fn wait_for_daemon_stop(client: &DaemonClient, instances: &[InstanceState]) -> bool {
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let instances_stopped = instances.iter().all(|i| !pid_is_running(i.pid));
        if instances_stopped && !client.is_running().await {
            return true;
        }
    }
    false
}

async fn up(args: UpArgs) {
    // Resolve the config: explicit --file, else ./Starlingfile, else ./Tiltfile.
    let config_path = resolve_config(args.file.as_deref());
    let span = config_span(&config_path);

    let (build_tx, build_rx) = mpsc::unbounded_channel::<store::BuildRequest>();
    let (restart_tx, restart_rx) = mpsc::unbounded_channel::<String>();
    let (tiltfile_args_tx, tiltfile_args_rx) = mpsc::unbounded_channel::<Vec<String>>();
    let (port_tx, port_rx) = mpsc::unbounded_channel::<(String, u16)>();
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();
    let store = Arc::new(Store::new(build_tx.clone()));
    store.set_restart_tx(restart_tx);
    store.set_tiltfile_args_tx(tiltfile_args_tx);

    // Set up the proxy handle: daemon (default), local (--no-daemon), or none.
    let mut daemon_instance: Option<(DaemonClient, String)> = None;
    let proxy_handle: Option<ProxyHandle> = if args.no_proxy {
        None
    } else if args.no_daemon {
        let registry = ProxyRegistry::new();
        let leased_ports = Arc::new(Mutex::new(HashSet::new()));
        leased_ports.lock().await.insert(args.port);
        let cfg = ProxyConfig {
            registry: registry.clone(),
            tld: args.tld.clone(),
            proxy_port: args.proxy_port,
            leased_ports,
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
        if let Err(e) = client
            .ensure_running(args.proxy_port, &args.tld, args.tls)
            .await
        {
            eprintln!("starling: {e}");
            std::process::exit(1);
        }
        let name = project_name(&config_path);
        let dir = project_dir(&config_path);
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

    // Custom-deploy delete commands to run on `starling down` (populated below,
    // before the manifests move into the engine).
    let mut k8s_down_specs = Vec::new();

    // API object store, shared between the engine (which populates it) and the
    // web server (which exposes read/watch routes over it).
    let api_objects = Arc::new(api::store::ApiObjectStore::new());

    // Load the config (Starlingfile or Tiltfile) and start the engine.
    match starlingfile::load_with_options(
        &config_path,
        starlingfile::LoadOptions {
            args: args.tiltfile_args.clone(),
            ..starlingfile::LoadOptions::default()
        },
    ) {
        Ok(result) => {
            store.append_log(
                Some(&span),
                "INFO",
                &format!(
                    "Loaded {} ({} resources)\n",
                    config_path.display(),
                    result.manifests.len()
                ),
            );
            for line in result.log.lines() {
                store.append_log(Some(&span), "INFO", &format!("{line}\n"));
            }
            if let Some(handle) = &proxy_handle {
                for (name, port) in &result.aliases {
                    handle.register(name, *port).await;
                    store.append_log(
                        Some(&span),
                        "INFO",
                        &format!("alias: {} -> 127.0.0.1:{port}\n", handle.url_for(name)),
                    );
                }
            }
            k8s_down_specs = engine::k8s_down_specs(&result.manifests);
            let eng = engine::Engine::new(
                store.clone(),
                result.manifests,
                build_rx,
                build_tx.clone(),
                args.dry_run,
                config_path.clone(),
                args.tiltfile_args.clone(),
                result.config_files,
                result.port_leases,
                restart_rx,
                tiltfile_args_rx,
                port_rx,
                proxy_handle.clone(),
                api_objects.clone(),
                result.max_parallel_updates,
            );
            store.set_scrub_secrets(result.secret_values.clone());
            tokio::spawn(eng.run());
            store.apply_session_settings(result.team_id, &result.feature_flags);
            for (name, path) in &result.extension_repos {
                api_objects.apply(
                    "ExtensionRepo",
                    "default",
                    name,
                    serde_json::json!({ "spec": { "url": format!("file://{path}") } }),
                );
            }
            for (reference, repo) in &result.extensions {
                api_objects.apply(
                    "Extension",
                    "default",
                    reference,
                    serde_json::json!({ "spec": { "repoName": repo, "repoPath": reference } }),
                );
            }
        }
        Err(e) => {
            let msg = format!("Failed to load {}: {e}", config_path.display());
            store.append_log(Some(&span), "ERROR", &format!("{msg}\n"));
            store.upsert_resource(config_error_resource(&span, &msg));
        }
    }

    // Reporter: push state to the daemon and execute queued commands.
    if let Some((client, instance)) = daemon_instance.clone() {
        let store = store.clone();
        let port_tx = port_tx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let api_objects = api_objects.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            // Segment count already pushed to the daemon; only newer lines go
            // out each tick so the daemon appends rather than re-sending tails.
            let mut log_checkpoint = 0usize;
            loop {
                tick.tick().await;
                let view = store.full_view();
                let resources: Vec<ResourceSnapshot> =
                    view.ui_resources.iter().map(snapshot).collect();
                let (logs_by_resource, checkpoint) = store.logs_since(log_checkpoint);
                log_checkpoint = checkpoint;
                let logs: HashMap<String, Vec<String>> = logs_by_resource.into_iter().collect();
                let objects = api_objects
                    .all()
                    .into_iter()
                    .map(|o| ApiObjectSnapshot {
                        kind: o.kind,
                        name: o.name,
                        object: o.object,
                    })
                    .collect();
                let _ = client
                    .call(&Request::Update {
                        instance: instance.clone(),
                        resources,
                        logs,
                        objects,
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
                            DaemonCommand::SetPort { resource, port } => {
                                let _ = port_tx.send((resource, port));
                            }
                            DaemonCommand::SetPaused { resource, paused } => {
                                if store.resource_exists(&resource) {
                                    store.set_resource_disabled(&resource, paused);
                                    store.append_log(
                                        Some(&resource),
                                        "INFO",
                                        &format!(
                                            "{} via dashboard\n",
                                            if paused { "Paused" } else { "Resumed" }
                                        ),
                                    );
                                }
                            }
                            DaemonCommand::SetTiltfileArgs { args } => {
                                let _ = store.set_tiltfile_args(args);
                            }
                            DaemonCommand::Shutdown => {
                                let _ = shutdown_tx.send(());
                                return;
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
            api_objects: api_objects.clone(),
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

    // A daemon-queued Shutdown (from `starling down` / `daemon --shutdown`) tears
    // down deployed resources; Ctrl-C leaves them running, matching Tilt.
    let down_requested = tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = shutdown_rx.recv() => true,
    };
    if down_requested && !k8s_down_specs.is_empty() {
        println!("Running k8s_custom_deploy delete commands...");
        engine::run_k8s_down(&k8s_down_specs, &store, args.dry_run).await;
    }
    if let Some((client, instance)) = daemon_instance {
        let _ = client.call(&Request::Deregister { instance }).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explain_knows_core_kinds() {
        // Every known kind has a (possibly empty) field list, and a couple of
        // representative kinds expose the expected spec fields.
        for k in api::store::known_kinds() {
            assert!(
                api::store::spec_fields(k).is_some(),
                "missing fields for {k}"
            );
        }
        assert!(api::store::spec_fields("KubernetesApply")
            .unwrap()
            .contains(&"yaml"));
        assert!(api::store::spec_fields("Cmd").unwrap().contains(&"args"));
        assert!(api::store::spec_fields("NotAKind").is_none());
    }
}
