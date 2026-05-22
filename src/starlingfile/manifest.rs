//! The manifest model produced by executing a Starlingfile.
//!
//! A `Manifest` is the unit of "a thing to build and run". This is a
//! pragmatic subset of Go's `pkg/model.Manifest`: enough to fully drive
//! `local_resource` execution, plus registration of k8s/docker resources so
//! they appear in the UI (deploying them to a cluster is a later phase).

use std::path::PathBuf;

/// How a command should be executed.
#[derive(Debug, Clone, Default)]
pub struct Cmd {
    /// argv to exec. A bare string command becomes `["sh", "-c", <string>]`.
    pub argv: Vec<String>,
    /// Working directory for the command (defaults to the Starlingfile dir).
    pub workdir: Option<PathBuf>,
    /// Extra environment variables to set on the child (e.g. `PORT`).
    pub env: Vec<(String, String)>,
}

impl Cmd {
    pub fn is_empty(&self) -> bool {
        self.argv.is_empty()
    }
    pub fn display(&self) -> String {
        self.argv.join(" ")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetKind {
    Local,
    Kubernetes,
    // Constructed once docker_compose() is implemented (later phase).
    #[allow(dead_code)]
    DockerCompose,
}

impl TargetKind {
    /// The string the UI expects in `UIResourceTargetSpec.type`.
    pub fn target_type(&self) -> &'static str {
        match self {
            TargetKind::Local => "local",
            TargetKind::Kubernetes => "k8s",
            TargetKind::DockerCompose => "docker-compose",
        }
    }
}

/// A single `live_update` step (Tilt's `sync()` / `run()` / etc.).
#[derive(Debug, Clone)]
pub enum LiveUpdateStep {
    /// Copy `local` (relative to the Starlingfile) into the container at `remote`.
    Sync { local: String, remote: String },
    /// Run a command inside the container after syncing.
    Run { cmd: String },
    /// Changes to these paths force a full rebuild instead of a live update.
    /// (Recorded; the path-level fallback discrimination is future work.)
    FallBackOn(#[allow(dead_code)] Vec<String>),
    /// Restart the container after syncing (deprecated for k8s in Tilt).
    RestartContainer,
    /// Sync the synced files once at container startup.
    InitialSync,
}

/// A docker image build registered by `docker_build(...)`.
#[derive(Debug, Clone)]
pub struct DockerBuild {
    pub image_ref: String,
    pub context: PathBuf,
    pub dockerfile: Option<PathBuf>,
    /// `--target` stage for multi-stage builds. Accepted for Tiltfile
    /// compatibility; bollard's classic builder doesn't expose it yet.
    #[allow(dead_code)]
    pub target: Option<String>,
    /// `--build-arg KEY=VALUE` pairs.
    pub build_args: Vec<(String, String)>,
    /// For `custom_build`: an arbitrary command that builds + tags the image
    /// (run with `EXPECTED_REF` set). When set, this replaces the bollard build.
    pub command: Option<Cmd>,
    /// For `custom_build`: file deps that trigger a rebuild.
    pub deps: Vec<PathBuf>,
    /// live_update steps: sync files into the running container instead of a
    /// full image rebuild + redeploy.
    pub live_update: Vec<LiveUpdateStep>,
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub name: String,
    pub kind: TargetKind,
    /// One-shot update command (`local_resource(cmd=...)`).
    pub update_cmd: Cmd,
    /// Long-running serve command (`local_resource(serve_cmd=...)`).
    pub serve_cmd: Cmd,
    /// Explicit port for the serve command's server. When unset and the proxy
    /// is enabled, a free port is allocated and passed via `PORT`.
    pub serve_port: Option<u16>,
    /// Files/dirs that trigger a rebuild when changed.
    pub deps: Vec<PathBuf>,
    /// Names of other resources that must build first.
    pub resource_deps: Vec<String>,
    /// Model trigger mode (matches Tilt's `pkg/model.TriggerMode`):
    /// 0 = Auto, 1 = ManualWithAutoInit, 2 = Manual, 3 = AutoWithManualInit.
    pub trigger_mode: i32,
    /// (url, name) endpoint links shown in the UI.
    pub links: Vec<(String, String)>,
    /// UI labels for grouping resources.
    pub labels: std::collections::BTreeMap<String, String>,
    /// Informational notes surfaced in the resource log.
    pub notes: Vec<String>,
    /// Auto-init: build on startup without a manual trigger.
    pub auto_init: bool,

    // -- Kubernetes-specific ----------------------------------------------
    /// Serialized YAML docs to `kubectl apply` for this resource.
    pub k8s_apply_docs: Vec<String>,
    /// Image builds to run before applying (docker_build matched to this
    /// workload's container images).
    pub docker_builds: Vec<DockerBuild>,
    /// `kind/name` of the primary workload (for status display).
    pub k8s_workload: Option<String>,
    /// Pod selector labels for watching pod status.
    pub pod_selector: std::collections::BTreeMap<String, String>,
    /// live_update steps inherited from a matched docker_build.
    pub live_update: Vec<LiveUpdateStep>,
}

impl Manifest {
    /// Whether file changes auto-trigger a build (Auto / AutoWithManualInit).
    /// Manual modes (ManualWithAutoInit / Manual) only mark pending changes.
    pub fn auto_on_change(&self) -> bool {
        matches!(self.trigger_mode, 0 | 3)
    }

    pub fn new(name: impl Into<String>, kind: TargetKind) -> Self {
        Manifest {
            name: name.into(),
            kind,
            update_cmd: Cmd::default(),
            serve_cmd: Cmd::default(),
            serve_port: None,
            deps: vec![],
            resource_deps: vec![],
            trigger_mode: 0,
            links: vec![],
            labels: std::collections::BTreeMap::new(),
            notes: vec![],
            auto_init: true,
            k8s_apply_docs: vec![],
            docker_builds: vec![],
            k8s_workload: None,
            pod_selector: std::collections::BTreeMap::new(),
            live_update: vec![],
        }
    }
}
