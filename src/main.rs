//! Starling: a local dev orchestrator, ported from Tilt (tilt.dev) + portless.
//!
//! Architecture: a single background **daemon** owns the shared named-URL proxy
//! and allocates ports centrally, so multiple `starling up` instances never
//! collide. Each `starling up` runs an engine for one project and reports its
//! resources to the daemon. `starling` (or `starling dash`) opens a k9s-style
//! TUI showing every instance's resources.

mod api;
mod build_history;
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
    /// Stop everything Starling owns — all instances, the processes they spawned
    /// (port-forwards, log followers, serve commands), and the daemon — and reap
    /// orphans left by an unclean exit. Returns the machine to a clean slate.
    Clean,
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
    /// Create or update API objects from a file (Tilt's `tilt apply`).
    Apply(ApplyArgs),
    /// Create an API object from a file (Tilt's `tilt create`).
    Create(ApplyArgs),
    /// Merge-patch an API object (Tilt's `tilt patch`).
    Patch(PatchArgs),
    /// Delete an API object (Tilt's `tilt delete`).
    Delete(DeleteArgs),
    /// Wait until an API object reaches a condition (Tilt's `tilt wait`).
    Wait(WaitArgs),
    /// Edit an API object in $EDITOR and apply the result (Tilt's `tilt edit`).
    Edit(EditArgs),
    /// Language-server mode (Tilt's `tilt lsp`) — not supported by Starling.
    Lsp,
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
    /// Tiltfile args passed after `--` (accepted for `tilt down` parity).
    #[arg(last = true)]
    tiltfile_args: Vec<String>,
}

/// Shared by `apply` and `create`: a manifest file of one or more API objects.
#[derive(Parser)]
struct ApplyArgs {
    /// File of one or more API objects (YAML or JSON; `---`-separated docs).
    #[arg(short = 'f', long = "filename")]
    file: String,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long = "config")]
    config: Option<String>,
}

#[derive(Parser)]
struct PatchArgs {
    /// Object kind (e.g. KubernetesApply, Cmd, FileWatch).
    kind: String,
    /// Object name.
    name: String,
    /// RFC 7386 JSON merge patch, e.g. `{"spec":{"queue":["web"]}}`.
    #[arg(short = 'p', long = "patch")]
    patch: String,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long = "config")]
    config: Option<String>,
}

#[derive(Parser)]
struct DeleteArgs {
    /// Object kind (e.g. KubernetesApply, Cmd, FileWatch).
    kind: String,
    /// Object name.
    name: String,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long = "config")]
    config: Option<String>,
}

#[derive(Parser)]
struct WaitArgs {
    /// Object kind (e.g. KubernetesApply, KubernetesDiscovery).
    kind: String,
    /// Object name.
    name: String,
    /// Condition to wait for, as a `jsonpath=value` over the object, e.g.
    /// `--for=status.error=` (empty) or `--for=status.readyPods=1`.
    #[arg(long = "for", default_value = "")]
    condition: String,
    /// Give up after this many seconds.
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long = "config")]
    config: Option<String>,
}

#[derive(Parser)]
struct EditArgs {
    /// Object kind (e.g. KubernetesApply, Cmd).
    kind: String,
    /// Object name.
    name: String,
    /// Config to identify the project. Defaults to ./Starlingfile, then ./Tiltfile.
    #[arg(long = "config")]
    config: Option<String>,
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
        Some(Command::Clean) => {
            if let Err(e) = clean().await {
                eprintln!("starling clean: {e}");
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
        Some(Command::Apply(a)) => {
            if let Err(e) = apply_objects(a, false).await {
                eprintln!("starling apply: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Create(a)) => {
            if let Err(e) = apply_objects(a, true).await {
                eprintln!("starling create: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Patch(a)) => {
            if let Err(e) = patch_object(a).await {
                eprintln!("starling patch: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Delete(a)) => {
            if let Err(e) = delete_object(a).await {
                eprintln!("starling delete: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Wait(a)) => {
            if let Err(e) = wait_object(a).await {
                eprintln!("starling wait: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Edit(a)) => {
            if let Err(e) = edit_object(a).await {
                eprintln!("starling edit: {e}");
                std::process::exit(1);
            }
        }
        Some(Command::Lsp) => {
            eprintln!(
                "starling lsp: Starling does not provide a Tiltfile language server. \
                 Use your editor's Starlark/Python support for Starlingfiles."
            );
            std::process::exit(1);
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

/// Resolve the API-server `host:port` of every running instance for the current
/// project (those that have reported one). The CLI write verbs issue HTTP CRUD
/// directly against this authoritative object store.
async fn instance_api_addrs(
    client: &DaemonClient,
    config: Option<&str>,
) -> anyhow::Result<Vec<String>> {
    let config_path = resolve_config(config);
    let dir = project_dir(&config_path);
    let Response::State(state) = client.call(&Request::GetState).await? else {
        anyhow::bail!("could not query daemon state");
    };
    let addrs: Vec<String> = state
        .instances
        .iter()
        .filter(|i| i.dir == dir)
        .filter_map(|i| i.api_addr.clone())
        .collect();
    if addrs.is_empty() {
        anyhow::bail!("no running Starling instance with an API server found for {dir}");
    }
    Ok(addrs)
}

/// Minimal localhost HTTP/1.1 client for the engine's object-store API. Returns
/// `(status_code, body_json)`. Uses `Connection: close` and reads to EOF, so it
/// relies on the `Content-Length` bodies axum emits (no chunked decoding).
async fn api_http(
    addr: &str,
    method: &str,
    path: &str,
    body: Option<&serde_json::Value>,
) -> anyhow::Result<(u16, serde_json::Value)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|e| anyhow::anyhow!("connect {addr}: {e}"))?;
    let body_bytes = match body {
        Some(b) => serde_json::to_vec(b)?,
        None => Vec::new(),
    };
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    if body.is_some() {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", body_bytes.len()));
    }
    req.push_str("\r\n");
    stream.write_all(req.as_bytes()).await?;
    if !body_bytes.is_empty() {
        stream.write_all(&body_bytes).await?;
    }
    stream.flush().await?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    let text = String::from_utf8_lossy(&buf);
    let (head, rest) = text.split_once("\r\n\r\n").unwrap_or((text.as_ref(), ""));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response from {addr}"))?;
    let json = if rest.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(rest.trim()).unwrap_or(serde_json::Value::Null)
    };
    Ok((status, json))
}

/// Parse a manifest file (one or more YAML/JSON API objects, `---`-separated)
/// into JSON object values.
fn parse_api_documents(raw: &str) -> anyhow::Result<Vec<serde_json::Value>> {
    use serde::Deserialize;
    let mut out = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(raw) {
        let val = serde_yaml::Value::deserialize(doc).map_err(|e| anyhow::anyhow!("parse: {e}"))?;
        if val.is_null() {
            continue;
        }
        out.push(serde_json::to_value(&val).map_err(|e| anyhow::anyhow!("convert: {e}"))?);
    }
    Ok(out)
}

/// Extract `(kind, metadata.name)` from an API object value.
fn object_kind_name(obj: &serde_json::Value) -> anyhow::Result<(String, String)> {
    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("object missing 'kind'"))?
        .to_string();
    let name = obj
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("{kind} object missing metadata.name"))?
        .to_string();
    Ok((kind, name))
}

/// Turn an object-store HTTP status into a user-facing line or error, mirroring
/// the apiserver semantics (409 already-exists, 404 not-found).
fn report_write(status: u16, kind: &str, name: &str, verb: &str) -> anyhow::Result<()> {
    match status {
        200 | 201 => {
            println!("{kind}/{name} {verb}");
            Ok(())
        }
        409 => anyhow::bail!("{kind}/{name} already exists"),
        404 => anyhow::bail!("{kind}/{name} not found"),
        400 => anyhow::bail!("{kind}/{name}: bad request"),
        s => anyhow::bail!("{kind}/{name}: unexpected status {s}"),
    }
}

/// `starling apply -f` / `starling create -f`: create-or-update (or create-only)
/// API objects against the running instance(s)' object store.
async fn apply_objects(args: ApplyArgs, create_only: bool) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&args.file)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", args.file))?;
    let docs = parse_api_documents(&raw)?;
    if docs.is_empty() {
        anyhow::bail!("no API objects found in {}", args.file);
    }
    let client = DaemonClient::new();
    let addrs = instance_api_addrs(&client, args.config.as_deref()).await?;
    for addr in &addrs {
        for obj in &docs {
            let (kind, name) = object_kind_name(obj)?;
            let coll = format!("/api/v1alpha1/{kind}");
            let single = format!("/api/v1alpha1/{kind}/{name}");
            if create_only {
                let (status, _) = api_http(addr, "POST", &coll, Some(obj)).await?;
                report_write(status, &kind, &name, "created")?;
            } else {
                // apply = replace if it exists, else create.
                let (status, _) = api_http(addr, "PUT", &single, Some(obj)).await?;
                if status == 404 {
                    let (status, _) = api_http(addr, "POST", &coll, Some(obj)).await?;
                    report_write(status, &kind, &name, "created")?;
                } else {
                    report_write(status, &kind, &name, "configured")?;
                }
            }
        }
    }
    Ok(())
}

/// `starling patch <kind> <name> -p <json>`: RFC 7386 merge-patch.
async fn patch_object(args: PatchArgs) -> anyhow::Result<()> {
    let patch: serde_json::Value = serde_json::from_str(&args.patch)
        .map_err(|e| anyhow::anyhow!("invalid --patch JSON: {e}"))?;
    let client = DaemonClient::new();
    let addrs = instance_api_addrs(&client, args.config.as_deref()).await?;
    let path = format!("/api/v1alpha1/{}/{}", args.kind, args.name);
    for addr in &addrs {
        let (status, _) = api_http(addr, "PATCH", &path, Some(&patch)).await?;
        report_write(status, &args.kind, &args.name, "patched")?;
    }
    Ok(())
}

/// `starling delete <kind> <name>`. Engine-managed objects are re-materialized
/// on the next reload, so deleting one is transient; client-created objects
/// (create/apply) persist until reload.
async fn delete_object(args: DeleteArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let addrs = instance_api_addrs(&client, args.config.as_deref()).await?;
    let path = format!("/api/v1alpha1/{}/{}", args.kind, args.name);
    for addr in &addrs {
        let (status, _) = api_http(addr, "DELETE", &path, None).await?;
        report_write(status, &args.kind, &args.name, "deleted")?;
    }
    Ok(())
}

/// Navigate a dotted JSON path (e.g. `status.error`) to a string value.
fn json_path_str(obj: &serde_json::Value, path: &str) -> Option<String> {
    let mut cur = obj;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(match cur {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    })
}

/// `starling wait <kind> <name> --for=<path>=<value>`: poll the object until the
/// field at `path` equals `value` (or it exists, when no `--for` is given), or
/// the timeout elapses.
async fn wait_object(args: WaitArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let addr = instance_api_addrs(&client, args.config.as_deref())
        .await?
        .into_iter()
        .next()
        .unwrap();
    let path = format!("/api/v1alpha1/{}/{}", args.kind, args.name);
    let (field, expected) = match args.condition.split_once('=') {
        Some((f, v)) => (f.trim().to_string(), v.trim().to_string()),
        None => (args.condition.trim().to_string(), String::new()),
    };
    let start = std::time::Instant::now();
    loop {
        let (status, body) = api_http(&addr, "GET", &path, None).await?;
        if status == 200 {
            if field.is_empty() {
                println!("{}/{} exists", args.kind, args.name);
                return Ok(());
            }
            if json_path_str(&body, &field).as_deref().unwrap_or("") == expected {
                println!("{}/{} {field}={expected}", args.kind, args.name);
                return Ok(());
            }
        }
        if start.elapsed().as_secs() >= args.timeout {
            anyhow::bail!(
                "timed out after {}s waiting for {}/{} {}",
                args.timeout,
                args.kind,
                args.name,
                args.condition
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// `starling edit <kind> <name>`: fetch the object, open it in `$EDITOR`, then
/// replace it with the edited JSON.
async fn edit_object(args: EditArgs) -> anyhow::Result<()> {
    let client = DaemonClient::new();
    let addr = instance_api_addrs(&client, args.config.as_deref())
        .await?
        .into_iter()
        .next()
        .unwrap();
    let path = format!("/api/v1alpha1/{}/{}", args.kind, args.name);
    let (status, body) = api_http(&addr, "GET", &path, None).await?;
    if status != 200 {
        anyhow::bail!("{}/{} not found", args.kind, args.name);
    }
    let tmp = std::env::temp_dir().join(format!(
        "starling-edit-{}-{}-{}.json",
        args.kind,
        args.name,
        std::process::id()
    ));
    std::fs::write(&tmp, serde_json::to_string_pretty(&body)?)?;
    let editor = std::env::var("EDITOR")
        .or_else(|_| std::env::var("VISUAL"))
        .unwrap_or_else(|_| "vi".to_string());
    let ok = std::process::Command::new(&editor)
        .arg(&tmp)
        .status()
        .map_err(|e| anyhow::anyhow!("launching {editor}: {e}"))?
        .success();
    if !ok {
        let _ = std::fs::remove_file(&tmp);
        anyhow::bail!("editor exited non-zero; not applying");
    }
    let edited = std::fs::read_to_string(&tmp)?;
    let _ = std::fs::remove_file(&tmp);
    let obj: serde_json::Value =
        serde_json::from_str(&edited).map_err(|e| anyhow::anyhow!("invalid edited JSON: {e}"))?;
    let (status, _) = api_http(&addr, "PUT", &path, Some(&obj)).await?;
    report_write(status, &args.kind, &args.name, "edited")
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
    // CI never signals an explicit shutdown; hold the sender so the engine's
    // shutdown receiver pends forever and the run loop is driven by builds only.
    let (_ci_shutdown_tx, ci_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(eng.run(ci_shutdown_rx));

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

    // The instance bounds its own teardown (stop local serves, then run k8s
    // delete commands) at ~10s before exiting, so wait a bit past that for the
    // pid to clear rather than bailing while teardown is still in flight.
    for _ in 0..150 {
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
        "shutdown was queued, but {} instance{} did not stop within 15s; \
         run `starling clean` to force-kill leftover processes",
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

/// Stop everything Starling owns and return the machine to a clean slate.
///
/// `daemon --shutdown` relies on the daemon's own bookkeeping, so it can't help
/// when the daemon is unreachable (a split-brain after an unclean restart) or
/// when an instance died without reaping the `kubectl port-forward` / `kubectl
/// logs -f` children it spawned — the usual cause of "address already in use" on
/// the next `up`. `clean` instead works from the live process table:
///
/// 1. Ask the daemon to shut down gracefully (best-effort) so well-behaved
///    instances tear down their own children.
/// 2. Sweep the process table: kill every Starling process and the subtree it
///    owns, plus reparented `kubectl` orphans matching Starling's distinctive
///    port-forward / log-follow command shapes. Children are signalled first.
/// 3. Remove the stale socket / pidfile so the next `up` starts fresh.
///
/// Safe to run when nothing is up (it simply reports a clean slate).
async fn clean() -> anyhow::Result<()> {
    let self_pid = std::process::id();

    // 1. Graceful shutdown first, if a daemon is answering.
    let client = DaemonClient::new();
    if client.is_running().await {
        match request_daemon_shutdown(&client).await {
            Ok(instances) => {
                println!(
                    "Stopping daemon and {} instance{}...",
                    instances.len(),
                    if instances.len() == 1 { "" } else { "s" }
                );
                wait_for_daemon_stop(&client, &instances).await;
            }
            Err(e) => {
                eprintln!("starling clean: graceful shutdown failed ({e}); sweeping processes");
            }
        }
    }

    // 2. Process sweep: Starling roots + their descendants, plus reparented
    //    Starling-spawned kubectl orphans.
    let table = process_table();
    let roots = starling_root_pids(&table, self_pid);
    let mut targets = descendants_of(&roots, &table); // children before parents
    targets.extend(roots.iter().copied());
    let orphans = orphaned_starling_kubectl(&table);
    for (pid, cmd) in &orphans {
        if !targets.contains(pid) {
            println!("Reaping orphaned process {pid}: {}", short_cmd(cmd));
            targets.push(*pid);
        }
    }
    targets.retain(|&p| p != self_pid);

    let stopped = kill_pids(&targets).await;

    // 3. Clear stale daemon state so the next `up` starts clean.
    let _ = std::fs::remove_file(daemon::protocol::socket_path());
    let _ = std::fs::remove_file(daemon::protocol::pid_path());

    if stopped == 0 {
        println!("Clean: nothing left running.");
    } else {
        println!(
            "Clean: stopped {stopped} Starling process{}.",
            if stopped == 1 { "" } else { "es" }
        );
    }
    Ok(())
}

/// `(pid, ppid, command)` for every process on the host, via `ps`. Returns an
/// empty table if `ps` is unavailable (the sweep then no-ops).
fn process_table() -> Vec<(u32, u32, String)> {
    let Ok(out) = std::process::Command::new("ps")
        .args(["-axo", "pid=,ppid=,command="])
        .output()
    else {
        return vec![];
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .filter_map(parse_ps_row)
        .collect()
}

/// Parse one `pid ppid command...` row from `ps -axo pid=,ppid=,command=`.
fn parse_ps_row(line: &str) -> Option<(u32, u32, String)> {
    let line = line.trim_start();
    let (pid, rest) = line.split_once(char::is_whitespace)?;
    let rest = rest.trim_start();
    let (ppid, cmd) = rest.split_once(char::is_whitespace)?;
    Some((
        pid.parse().ok()?,
        ppid.parse().ok()?,
        cmd.trim().to_string(),
    ))
}

/// PIDs whose executable is the Starling binary (the daemon, its supervisor, and
/// every `up` instance), excluding `self_pid`. Matched on the program's file
/// name, so it works however the binary was invoked (`starling`,
/// `/abs/starling`, `~/.cargo/bin/starling`).
fn starling_root_pids(table: &[(u32, u32, String)], self_pid: u32) -> Vec<u32> {
    table
        .iter()
        .filter_map(|(pid, _ppid, cmd)| {
            if *pid == self_pid {
                return None;
            }
            let prog = cmd.split_whitespace().next()?;
            let base = prog.rsplit('/').next().unwrap_or(prog);
            (base == "starling").then_some(*pid)
        })
        .collect()
}

/// All descendant PIDs of `roots` (breadth-first, parents before children),
/// derived from the ppid links in the process table. Excludes the roots.
fn descendants_of(roots: &[u32], table: &[(u32, u32, String)]) -> Vec<u32> {
    use std::collections::{HashMap, HashSet, VecDeque};
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    for (pid, ppid, _) in table {
        children.entry(*ppid).or_default().push(*pid);
    }
    let mut seen: HashSet<u32> = HashSet::new();
    let mut out = vec![];
    let mut queue: VecDeque<u32> = roots.iter().copied().collect();
    while let Some(p) = queue.pop_front() {
        if let Some(kids) = children.get(&p) {
            for &k in kids {
                if seen.insert(k) {
                    out.push(k);
                    queue.push_back(k);
                }
            }
        }
    }
    out
}

/// Reparented (`ppid == 1`) `kubectl` processes whose command matches the
/// distinctive shapes Starling spawns — `kubectl port-forward … --address` and
/// `kubectl logs … -f --all-containers`. These are the children an instance
/// leaks when it dies uncleanly; the `--address` / `-f --all-containers` combos
/// make a false match against a hand-run kubectl unlikely.
fn orphaned_starling_kubectl(table: &[(u32, u32, String)]) -> Vec<(u32, String)> {
    table
        .iter()
        .filter_map(|(pid, ppid, cmd)| {
            if *ppid != 1 || !cmd.contains("kubectl") {
                return None;
            }
            let is_port_forward = cmd.contains("port-forward") && cmd.contains("--address");
            let is_log_follow =
                cmd.contains("logs") && cmd.contains("-f") && cmd.contains("--all-containers");
            (is_port_forward || is_log_follow).then(|| (*pid, cmd.clone()))
        })
        .collect()
}

/// SIGTERM every target, wait briefly for graceful exit, then SIGKILL any
/// survivor. Returns the number that are no longer running afterward.
async fn kill_pids(pids: &[u32]) -> usize {
    if pids.is_empty() {
        return 0;
    }
    for &pid in pids {
        signal_pid(pid, "TERM");
    }
    for _ in 0..20 {
        if pids.iter().all(|&p| !pid_is_running(p)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for &pid in pids {
        if pid_is_running(pid) {
            signal_pid(pid, "KILL");
        }
    }
    pids.iter().filter(|&&p| !pid_is_running(p)).count()
}

/// Send a signal to a pid via `kill`, ignoring errors (the process may already
/// be gone, or owned by another user).
fn signal_pid(pid: u32, sig: &str) {
    let _ = std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .stderr(std::process::Stdio::null())
        .status();
}

/// Truncate a command line for one-line reporting.
fn short_cmd(cmd: &str) -> String {
    const MAX: usize = 80;
    if cmd.len() <= MAX {
        cmd.to_string()
    } else {
        format!("{}…", &cmd[..MAX])
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

    // Handle + shutdown signal for the engine task. On `starling down` / Ctrl-C
    // we signal the engine to stop all local serves (killing their process
    // groups) and wait for it to finish before the instance exits, so services
    // don't leak.
    let mut engine_handle: Option<tokio::task::JoinHandle<()>> = None;
    let mut engine_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>> = None;

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
            let (eng_shutdown_tx, eng_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            engine_shutdown_tx = Some(eng_shutdown_tx);
            engine_handle = Some(tokio::spawn(eng.run(eng_shutdown_rx)));
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

    // API server: always bind the authoritative object-store HTTP surface so
    // the CLI write verbs (apply/create/patch/delete/wait) can reach it. With
    // --web it also serves the dashboard UI on the fixed --host:--port; without
    // --web it binds an ephemeral localhost port. The bound address is reported
    // to the daemon (below) so the CLI can discover it.
    let api_addr: Option<String> = {
        let state = AppState {
            store: store.clone(),
            csrf_token: uuid::Uuid::new_v4().to_string(),
            api_objects: api_objects.clone(),
        };
        let app = server::router(state, &args.web_dir);
        let bind = if args.web {
            format!("{}:{}", args.host, args.port)
        } else {
            "127.0.0.1:0".to_string()
        };
        match tokio::net::TcpListener::bind(&bind).await {
            Ok(l) => {
                let addr = l.local_addr().ok().map(|a| a.to_string());
                if args.web {
                    if let Some(a) = &addr {
                        println!("web UI on http://{a}/");
                    }
                }
                tokio::spawn(async move {
                    let _ = axum::serve(l, app).await;
                });
                addr
            }
            Err(e) => {
                eprintln!("starling: API server bind {bind} failed: {e}");
                None
            }
        }
    };

    // Reporter: push state to the daemon and execute queued commands.
    if let Some((client, instance)) = daemon_instance.clone() {
        let store = store.clone();
        let port_tx = port_tx.clone();
        let shutdown_tx = shutdown_tx.clone();
        let api_objects = api_objects.clone();
        let api_addr = api_addr.clone();
        tokio::spawn(async move {
            let mut changes = store.subscribe();
            let mut tick = tokio::time::interval(Duration::from_millis(1000));
            // Segment count already pushed to the daemon; only newer lines go
            // out each tick so the daemon appends rather than re-sending tails.
            let mut log_checkpoint = 0usize;
            loop {
                let woke_for_change = tokio::select! {
                    _ = tick.tick() => false,
                    _ = changes.recv() => true,
                };
                if woke_for_change {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                    while changes.try_recv().is_ok() {}
                }
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
                        api_addr: api_addr.clone(),
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

    if args.tailscale {
        netmodes::tailscale_serve(args.proxy_port);
    }

    println!("starling up running — open the dashboard with `starling`. Ctrl-C to stop.");

    // A daemon-queued Shutdown (from `starling down` / `daemon --shutdown`) tears
    // down deployed k8s resources; Ctrl-C leaves those running, matching Tilt.
    // Either way the local serves below are stopped — they're children of this
    // instance and must not outlive it.
    let down_requested = tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = shutdown_rx.recv() => true,
    };
    // Stop local serves first so each one's whole process group is killed (not
    // orphaned) before the instance exits — applies to both `starling down` and
    // Ctrl-C. Bounded by a timeout so a serve stuck ignoring SIGTERM/SIGKILL
    // can't hang shutdown indefinitely (the engine SIGKILLs after a per-serve
    // grace, so this should only ever bite if the engine itself is wedged).
    if let Some(tx) = engine_shutdown_tx.take() {
        let _ = tx.send(());
    }
    if let Some(handle) = engine_handle.take() {
        if tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .is_err()
        {
            eprintln!("starling: local services did not stop within 10s");
        }
    }
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
    fn clean_identifies_starling_roots_and_subtrees() {
        // pid, ppid, command — a daemon (200) + supervisor (201), an instance
        // `up` (300) with kubectl children (310/311), and an unrelated process.
        let table = vec![
            (1u32, 0u32, "/sbin/launchd".to_string()),
            (
                200,
                1,
                "/Users/me/.cargo/bin/starling daemon --proxy-port 1360".to_string(),
            ),
            (201, 200, "starling".to_string()),
            (300, 1, "starling up --no-proxy".to_string()),
            (
                310,
                300,
                "kubectl port-forward -n agent-builder pod/x 50051:50051 --address 127.0.0.1"
                    .to_string(),
            ),
            (
                311,
                300,
                "kubectl logs -n agent-builder -f --all-containers --tail 20 x".to_string(),
            ),
            (999, 1, "kubectl get pods".to_string()),
        ];
        // Roots are the three Starling processes; self (300 here is NOT self) is
        // only excluded when it equals self_pid.
        let mut roots = starling_root_pids(&table, /*self_pid*/ 0);
        roots.sort();
        assert_eq!(roots, vec![200, 201, 300]);
        // self exclusion drops only the running CLI's own pid.
        assert!(!starling_root_pids(&table, 200).contains(&200));
        // Descendants of the instance include both kubectl children.
        let mut desc = descendants_of(&[300], &table);
        desc.sort();
        assert_eq!(desc, vec![310, 311]);
        // An unrelated `kubectl get pods` is never a root or descendant.
        assert!(!roots.contains(&999));
        assert!(!descendants_of(&roots, &table).contains(&999));
    }

    #[test]
    fn clean_reaps_only_starling_shaped_kubectl_orphans() {
        let table = vec![
            // Reparented Starling port-forward + log-follow orphans (ppid 1).
            (
                310u32,
                1u32,
                "kubectl port-forward -n ns pod/x 8080:8080 --address 127.0.0.1".to_string(),
            ),
            (
                311,
                1,
                "kubectl logs -n ns -f --all-containers --tail 20 x".to_string(),
            ),
            // A hand-run kubectl reparented to init must NOT be reaped.
            (999, 1, "kubectl get pods -A".to_string()),
            // A Starling-shaped kubectl still owned by a live parent is left to
            // the subtree sweep, not the orphan pass.
            (
                320,
                300,
                "kubectl port-forward -n ns pod/y 9090:9090 --address 127.0.0.1".to_string(),
            ),
        ];
        let mut orphans: Vec<u32> = orphaned_starling_kubectl(&table)
            .into_iter()
            .map(|(pid, _)| pid)
            .collect();
        orphans.sort();
        assert_eq!(orphans, vec![310, 311]);
    }

    #[test]
    fn parses_ps_rows() {
        assert_eq!(
            parse_ps_row("  200   1 starling daemon --proxy-port 1360"),
            Some((200, 1, "starling daemon --proxy-port 1360".to_string()))
        );
        // No command column -> skipped.
        assert_eq!(parse_ps_row("123"), None);
    }

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

    #[test]
    fn parse_api_documents_handles_multidoc_and_kind_name() {
        let raw = "apiVersion: tilt.dev/v1alpha1\nkind: FileWatch\nmetadata:\n  name: web\nspec:\n  watchedPaths: [\"./src\"]\n---\nkind: Cmd\nmetadata:\n  name: build\n";
        let docs = parse_api_documents(raw).unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(
            object_kind_name(&docs[0]).unwrap(),
            ("FileWatch".into(), "web".into())
        );
        assert_eq!(
            object_kind_name(&docs[1]).unwrap(),
            ("Cmd".into(), "build".into())
        );
        // JSON parses too (it's a YAML subset).
        let json = parse_api_documents("{\"kind\":\"Cmd\",\"metadata\":{\"name\":\"x\"}}").unwrap();
        assert_eq!(
            object_kind_name(&json[0]).unwrap(),
            ("Cmd".into(), "x".into())
        );
        // Missing kind/name is an error.
        assert!(object_kind_name(&serde_json::json!({"metadata": {"name": "a"}})).is_err());
    }

    #[test]
    fn json_path_str_navigates_and_stringifies() {
        let obj = serde_json::json!({"status": {"error": "", "readyPods": 2}});
        assert_eq!(json_path_str(&obj, "status.error").as_deref(), Some(""));
        assert_eq!(
            json_path_str(&obj, "status.readyPods").as_deref(),
            Some("2")
        );
        assert_eq!(json_path_str(&obj, "status.missing"), None);
    }

    /// End-to-end of the CLI write path against the engine's real object-store
    /// HTTP surface (no daemon/cluster): bind `server::router`, then exercise
    /// the `api_http` client used by apply/create/patch/delete/wait, asserting
    /// the apiserver status codes (201/200/409/404) and store effects.
    #[tokio::test]
    async fn cli_write_verbs_round_trip_against_object_store() {
        let api_objects = std::sync::Arc::new(api::store::ApiObjectStore::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let store = std::sync::Arc::new(store::Store::new(tx));
        let state = AppState {
            store,
            csrf_token: "t".to_string(),
            api_objects: api_objects.clone(),
        };
        let app = server::router(state, "web/build");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let obj = serde_json::json!({
            "kind": "FileWatch", "metadata": {"name": "web"},
            "spec": {"watchedPaths": ["./src"]}
        });
        // create (POST) -> 201; second create -> 409.
        let (s, _) = api_http(&addr, "POST", "/api/v1alpha1/FileWatch", Some(&obj))
            .await
            .unwrap();
        assert_eq!(s, 201);
        let (s, _) = api_http(&addr, "POST", "/api/v1alpha1/FileWatch", Some(&obj))
            .await
            .unwrap();
        assert_eq!(s, 409);
        // get -> 200, reflects the spec.
        let (s, body) = api_http(&addr, "GET", "/api/v1alpha1/FileWatch/web", None)
            .await
            .unwrap();
        assert_eq!(s, 200);
        assert_eq!(body["spec"]["watchedPaths"][0], "./src");
        // patch (merge) -> 200; the store reflects it.
        let (s, _) = api_http(
            &addr,
            "PATCH",
            "/api/v1alpha1/FileWatch/web",
            Some(&serde_json::json!({"spec": {"manual": true}})),
        )
        .await
        .unwrap();
        assert_eq!(s, 200);
        assert_eq!(
            api_objects
                .get("FileWatch", "default", "web")
                .unwrap()
                .object["spec"]["manual"],
            serde_json::json!(true)
        );
        // patch a missing object -> 404.
        let (s, _) = api_http(
            &addr,
            "PATCH",
            "/api/v1alpha1/FileWatch/nope",
            Some(&serde_json::json!({"spec": {}})),
        )
        .await
        .unwrap();
        assert_eq!(s, 404);
        // delete -> 200, then it's gone.
        let (s, _) = api_http(&addr, "DELETE", "/api/v1alpha1/FileWatch/web", None)
            .await
            .unwrap();
        assert_eq!(s, 200);
        assert!(api_objects.get("FileWatch", "default", "web").is_none());
    }
}
