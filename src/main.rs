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
mod health;
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

use clap::{Parser, Subcommand, ValueEnum};
use tokio::sync::mpsc;

use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::{
    Command as DaemonCommand, InstanceState, Request, ResourceSnapshot, Response,
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
            if a.shutdown && (a.restart || a.reload) {
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
    ResourceSnapshot {
        name,
        kind,
        update_status: st.update_status.unwrap_or_default(),
        runtime_status: st.runtime_status.unwrap_or_default(),
        pod,
        url,
        proxy_status: proxy_condition.map(|c| c.status.clone()),
        proxy_message: proxy_condition.and_then(|c| c.message.clone()),
        build_count: st.build_history.len() as u32,
        last_deploy: st.last_deploy_time,
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
            "  {:<28} {:<10} {:<12} {:<14} {:<8} {}",
            "RESOURCE", "KIND", "UPDATE", "RUNTIME", "PROXY", "URL"
        );
        for res in &inst.resources {
            println!(
                "  {:<28} {:<10} {:<12} {:<14} {:<8} {}",
                res.name,
                res.kind,
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
            })
            .await?
        {
            Response::Logs(lines) => tail_lines(lines, args.tail),
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
    client
        .ensure_running_with(args.proxy_port, &args.tld, &args.host, args.tls, args.lan)
        .await?;
    println!("Starling daemon is running.");
    Ok(())
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

    let (build_tx, build_rx) = mpsc::unbounded_channel::<String>();
    let (restart_tx, restart_rx) = mpsc::unbounded_channel::<String>();
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel::<()>();
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

    // Load the config (Starlingfile or Tiltfile) and start the engine.
    match starlingfile::load(&config_path) {
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
                Some(&span),
                "ERROR",
                &format!("Failed to load {}: {e}\n", config_path.display()),
            );
        }
    }

    // Reporter: push state to the daemon and execute queued commands.
    if let Some((client, instance)) = daemon_instance.clone() {
        let store = store.clone();
        let shutdown_tx = shutdown_tx.clone();
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

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = shutdown_rx.recv() => {}
    }
    if let Some((client, instance)) = daemon_instance {
        let _ = client.call(&Request::Deregister { instance }).await;
    }
}
