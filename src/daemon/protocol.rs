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
/// On Windows there are no Unix domain sockets, so the daemon listens on a
/// 127.0.0.1 TCP port and records the chosen port here for the client to read.
pub fn port_file_path() -> PathBuf {
    state_dir().join("daemon.port")
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
    pub paused: bool,
    pub update_status: String,
    pub runtime_status: String,
    pub pod: Option<String>,
    pub url: Option<String>,
    pub proxy_status: Option<String>,
    pub proxy_message: Option<String>,
    pub build_count: u32,
    pub last_deploy: Option<String>,
    pub restart_count: Option<u32>,
    pub last_start: Option<String>,
}

/// One API object as mirrored from an instance's API object store, for CLI
/// `get`/`describe`-style reads through the daemon.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ApiObjectSnapshot {
    pub kind: String,
    pub name: String,
    pub object: serde_json::Value,
}

/// Everything the daemon knows about one connected instance.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct InstanceState {
    pub id: String,
    pub name: String,
    pub dir: String,
    pub pid: u32,
    pub resources: Vec<ResourceSnapshot>,
    #[serde(default)]
    pub objects: Vec<ApiObjectSnapshot>,
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
    /// Change a resource's preferred serve_cmd backend port and restart it.
    SetPort {
        resource: String,
        port: u16,
    },
    /// Pause/resume a resource. Paused resources keep their state visible but
    /// ignore file-change rebuilds/live updates until resumed.
    SetPaused {
        resource: String,
        paused: bool,
    },
    /// Stop this `starling up` instance.
    Shutdown,
    /// Replace the Tiltfile args and reload (Tilt's `tilt args`).
    SetTiltfileArgs {
        args: Vec<String>,
    },
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
    /// Push the instance's current resources plus the log lines appended since
    /// the reporter's previous push. The daemon appends these to its
    /// per-resource ring (it does not replace), so each line is sent once.
    Update {
        instance: String,
        resources: Vec<ResourceSnapshot>,
        logs: HashMap<String, Vec<String>>,
        #[serde(default)]
        objects: Vec<ApiObjectSnapshot>,
    },
    /// Lease a free port for a serve_cmd (avoids cross-instance conflicts).
    AllocatePort {
        instance: String,
    },
    /// Reserve a backend port for a serve_cmd, optionally preferring a fixed
    /// port from local_resource(..., serve_port=N).
    ReservePort {
        instance: String,
        preferred: Option<u16>,
    },
    /// Release a previously leased host port that is no longer needed.
    ReleasePort {
        instance: String,
        port: u16,
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
    /// Dashboard: fetch log lines for a resource newer than `since` (a cursor
    /// from a prior `Logs` response). Pass `0` to fetch the full retained tail.
    GetLogs {
        instance: String,
        resource: String,
        since: u64,
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
    /// Dashboard: change a resource's preferred serve_cmd backend port.
    SetPort {
        instance: String,
        resource: String,
        port: u16,
    },
    /// Dashboard: pause/resume a resource on an instance.
    SetPaused {
        instance: String,
        resource: String,
        paused: bool,
    },
    /// CLI: ask every instance registered for this project directory to stop.
    ShutdownProject {
        dir: String,
    },
    /// CLI: ask every instance to stop, then terminate the daemon.
    ShutdownDaemon,
    /// CLI: fetch API objects across instances, optionally filtered by kind.
    GetObjects {
        kind: Option<String>,
    },
    /// CLI: replace the Tiltfile args on an instance and reload it.
    SetTiltfileArgs {
        instance: String,
        args: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok,
    Registered {
        instance: String,
    },
    Port {
        port: u16,
    },
    ReservedPort {
        port: u16,
        preferred: Option<u16>,
        conflict: bool,
    },
    State(DashboardState),
    /// Log lines for a resource, plus the cursor to pass as `since` next time.
    Logs {
        lines: Vec<String>,
        cursor: u64,
    },
    Commands(Vec<Command>),
    ShutdownQueued {
        instances: Vec<InstanceState>,
    },
    /// API objects aggregated across instances (response to `GetObjects`).
    Objects(Vec<ApiObjectSnapshot>),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_tiltfile_args_request_round_trips() {
        let req = Request::SetTiltfileArgs {
            instance: "i1".to_string(),
            args: vec!["--foo".to_string(), "bar".to_string()],
        };
        let wire = serde_json::to_string(&req).unwrap();
        match serde_json::from_str::<Request>(&wire).unwrap() {
            Request::SetTiltfileArgs { instance, args } => {
                assert_eq!(instance, "i1");
                assert_eq!(args, vec!["--foo", "bar"]);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn update_request_round_trips_objects() {
        let req = Request::Update {
            instance: "i1".to_string(),
            resources: vec![],
            logs: HashMap::new(),
            objects: vec![ApiObjectSnapshot {
                kind: "Cmd".to_string(),
                name: "web".to_string(),
                object: serde_json::json!({"spec": {"args": ["echo"]}}),
            }],
        };
        let wire = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&wire).unwrap();
        match back {
            Request::Update { objects, .. } => {
                assert_eq!(objects.len(), 1);
                assert_eq!(objects[0].kind, "Cmd");
                assert_eq!(
                    objects[0].object["spec"]["args"],
                    serde_json::json!(["echo"])
                );
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn instance_state_objects_default_for_old_peers() {
        // An older peer that omits `objects` must still deserialize (serde default).
        let json = r#"{"id":"i","name":"n","dir":"d","pid":1,"resources":[]}"#;
        let state: InstanceState = serde_json::from_str(json).unwrap();
        assert!(state.objects.is_empty());
    }
}
