//! Wire protocol for the Starling daemon control plane.
//!
//! A single local daemon owns the shared named-URL proxy, allocates ports
//! centrally (so multiple `starling up` instances never collide), and
//! aggregates every instance's resources for the shared TUI dashboard.
//!
//! Transport is newline-delimited JSON over a Unix socket, one request +
//! one response per connection.

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Directory holding the daemon socket / pid / log (`~/.starling`).
pub fn state_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".starling")
}

pub fn socket_path() -> PathBuf {
    state_dir().join("daemon.sock")
}
pub fn pid_path() -> PathBuf {
    state_dir().join("daemon.pid")
}
pub fn log_path() -> PathBuf {
    state_dir().join("daemon.log")
}

/// One resource as shown on the dashboard (derived from a `UIResource`).
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ResourceSnapshot {
    pub name: String,
    pub kind: String,
    pub update_status: String,
    pub runtime_status: String,
    pub pod: Option<String>,
    pub url: Option<String>,
    pub proxy_status: Option<String>,
    pub proxy_message: Option<String>,
    pub build_count: u32,
    pub last_deploy: Option<String>,
}

/// Everything the daemon knows about one connected instance.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct InstanceState {
    pub id: String,
    pub name: String,
    pub dir: String,
    pub pid: u32,
    pub resources: Vec<ResourceSnapshot>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RouteInfo {
    pub hostname: String,
    pub port: u16,
    pub instance: String,
}

/// Aggregated dashboard state returned by `GetState`.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct DashboardState {
    pub instances: Vec<InstanceState>,
    pub routes: Vec<RouteInfo>,
    pub proxy_port: u16,
    pub tld: String,
}

/// A command the dashboard wants an instance to run.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Command {
    Trigger {
        resource: String,
    },
    /// Restart a resource's serve_cmd (kill the process and start it again).
    Restart {
        resource: String,
    },
    /// Stop this `starling up` instance.
    Shutdown,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Liveness check (used by clients to detect a running daemon).
    Ping,
    /// Register a new instance; daemon assigns and returns an id.
    Register {
        name: String,
        dir: String,
        pid: u32,
    },
    Deregister {
        instance: String,
    },
    /// Push the instance's current resources + recent per-resource logs.
    Update {
        instance: String,
        resources: Vec<ResourceSnapshot>,
        logs: HashMap<String, Vec<String>>,
    },
    /// Lease a free port for a serve_cmd (avoids cross-instance conflicts).
    AllocatePort {
        instance: String,
    },
    RegisterRoute {
        instance: String,
        hostname: String,
        port: u16,
    },
    RemoveRoute {
        hostname: String,
    },
    /// Dashboard: fetch aggregated state.
    GetState,
    /// Dashboard: fetch the recent logs for a resource.
    GetLogs {
        instance: String,
        resource: String,
    },
    /// Instance: pull (and clear) pending commands queued by the dashboard.
    PollCommands {
        instance: String,
    },
    /// Dashboard: queue a trigger for a resource on an instance.
    Trigger {
        instance: String,
        resource: String,
    },
    /// Dashboard: queue a serve_cmd restart for a resource on an instance.
    Restart {
        instance: String,
        resource: String,
    },
    /// CLI: ask every instance registered for this project directory to stop.
    ShutdownProject {
        dir: String,
    },
    /// CLI: ask every instance to stop, then terminate the daemon.
    ShutdownDaemon,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok,
    Registered { instance: String },
    Port { port: u16 },
    State(DashboardState),
    Logs(Vec<String>),
    Commands(Vec<Command>),
    ShutdownQueued { instances: Vec<InstanceState> },
    Error(String),
}
