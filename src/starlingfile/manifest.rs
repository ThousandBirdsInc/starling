//! The manifest model produced by executing a Starlingfile.
//!
//! A `Manifest` is the unit of "a thing to build and run". This is a
//! pragmatic subset of Go's `pkg/model.Manifest`: enough to fully drive
//! `local_resource` execution, plus registration of k8s/docker resources so
//! they appear in the UI (deploying them to a cluster is a later phase).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// A readiness probe action (Tilt's `exec_action` / `tcp_socket_action` /
/// `http_get_action`). Serialized to JSON to flow through Starlark values.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeAction {
    /// Run a command in a subprocess; success = exit status 0.
    Exec { command: Vec<String> },
    /// Open a TCP connection; success = connection established.
    Tcp { host: String, port: ProbePort },
    /// Issue an HTTP GET; success = response status < 400.
    Http {
        host: String,
        port: ProbePort,
        scheme: String,
        path: String,
    },
}

/// A probe port can be a literal number or a deferred Starling env reference
/// such as `${STARLING_POSTGRES_PORT}` from `starling_port(...)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProbePort {
    Number(u16),
    Deferred(String),
}

impl ProbePort {
    pub fn as_u16(&self) -> Result<u16, String> {
        match self {
            ProbePort::Number(port) => Ok(*port),
            ProbePort::Deferred(port) => resolve_deferred_port(port),
        }
    }
}

fn resolve_deferred_port(port: &str) -> Result<u16, String> {
    if let Ok(port) = port.parse::<u16>() {
        return Ok(port);
    }
    let env_name = port
        .strip_prefix("${")
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(port);
    std::env::var(env_name)
        .map_err(|_| format!("could not resolve probe port {port}"))?
        .parse::<u16>()
        .map_err(|_| format!("probe port {port} did not resolve to a valid TCP port"))
}

/// A readiness probe (Tilt's `probe(...)`). Gates a `serve_cmd` resource to
/// "ready" only once the probe action succeeds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessProbe {
    #[serde(default)]
    pub initial_delay_secs: f64,
    #[serde(default = "default_period_secs")]
    pub period_secs: f64,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: f64,
    #[serde(default = "default_probe_threshold")]
    pub success_threshold: i32,
    #[serde(default = "default_probe_threshold")]
    pub failure_threshold: i32,
    pub action: ProbeAction,
}

fn default_period_secs() -> f64 {
    1.0
}
fn default_timeout_secs() -> f64 {
    1.0
}
fn default_probe_threshold() -> i32 {
    1
}

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
    Run {
        cmd: String,
        echo_off: bool,
        triggers: Vec<String>,
    },
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
    /// Inline Dockerfile text from `docker_build(dockerfile_contents=...)`.
    pub dockerfile_contents: Option<String>,
    /// `--target` stage for multi-stage builds.
    pub target: Option<String>,
    /// Docker platform in `os[/arch[/variant]]` form.
    pub platform: Option<String>,
    /// Additional image references to apply after a successful build.
    pub extra_tags: Vec<String>,
    /// Kubernetes command override for containers using this image.
    pub entrypoint: Vec<String>,
    /// Kubernetes args override for containers using this image.
    pub container_args: Option<Vec<String>>,
    /// Match this image ref in container env var values as well as image fields.
    pub match_in_env_vars: bool,
    /// `--build-arg KEY=VALUE` pairs.
    pub build_args: Vec<(String, String)>,
    /// Images used for build cache resolution.
    pub cache_from: Vec<String>,
    /// BuildKit SSH agent config strings from `docker_build(ssh=...)`.
    pub ssh: Vec<String>,
    /// BuildKit secret spec strings from `docker_build(secret=...)`.
    pub secrets: Vec<String>,
    /// Whether Docker should attempt to pull newer base images.
    pub pull: bool,
    /// Docker network mode for RUN steps during image build.
    pub network: Option<String>,
    /// Extra host mappings for image build containers.
    pub extra_hosts: Vec<String>,
    /// Dockerignore-style rules applied while tarring the build context.
    pub ignore_rules: Vec<IgnoreRule>,
    /// Tilt's `only=` context allowlist. Paths are relative to `context`.
    pub only: Vec<PathBuf>,
    /// For `custom_build`: an arbitrary command that builds + tags the image
    /// (run with `EXPECTED_REF` set). When set, this replaces the bollard build.
    pub command: Option<Cmd>,
    /// For `custom_build(tag=...)`: expected output tag/ref from the script.
    pub custom_tag: Option<String>,
    /// For `custom_build(outputs_image_ref_to=...)`: file containing the built ref.
    pub outputs_image_ref_to: Option<PathBuf>,
    /// For `custom_build(image_deps=...)`: image builds this custom build depends on.
    pub image_deps: Vec<String>,
    /// For `custom_build(disable_push=...)`.
    pub disable_push: bool,
    /// For `custom_build(skips_local_docker=...)`.
    pub skips_local_docker: bool,
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
    /// Whether this local resource's update command may run concurrently with
    /// other resource updates.
    pub allow_parallel: bool,
    /// Readiness probe (`local_resource(readiness_probe=probe(...))`): when set,
    /// the serve_cmd is held "pending" until the probe action first succeeds.
    pub readiness_probe: Option<ReadinessProbe>,
    /// Deprecated Tilt `test(...)` resources are modeled as local resources but
    /// marked for frontend compatibility.
    pub is_test: bool,
    /// Ignore rules that suppress file-change rebuilds for this resource.
    pub ignore_rules: Vec<IgnoreRule>,

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
    /// If true, pod readiness does not gate runtime status.
    pub pod_readiness_ignore: bool,
    /// Custom Kubernetes deploy command registered by `k8s_custom_deploy(...)`.
    pub k8s_custom_apply_cmd: Option<Cmd>,
    /// Custom Kubernetes delete command registered by `k8s_custom_deploy(...)`.
    pub k8s_custom_delete_cmd: Option<Cmd>,
    /// Image deps requested by `k8s_custom_deploy(image_deps=...)`.
    pub k8s_custom_image_deps: Vec<String>,
    /// live_update steps inherited from a matched docker_build.
    pub live_update: Vec<LiveUpdateStep>,
    /// Kubernetes port-forwards requested by `k8s_resource(port_forwards=...)`.
    pub k8s_port_forwards: Vec<PortForwardSpec>,

    // -- Docker Compose-specific ------------------------------------------
    /// Compose project name for disambiguating `dc_resource(project_name=...)`.
    pub docker_compose_project: Option<String>,
}

/// A named host TCP port requested by the Starlingfile.
///
/// These are not HTTP proxy routes. They are centrally leased ports for
/// services such as databases where other resources need a stable
/// `STARLING_<NAME>_PORT` value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedPortLease {
    pub name: String,
    pub preferred: Option<u16>,
}

/// A Dockerignore-style watch ignore rule. `base` is the directory patterns are
/// evaluated relative to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoreRule {
    pub base: PathBuf,
    pub pattern: String,
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
            allow_parallel: false,
            readiness_probe: None,
            is_test: false,
            ignore_rules: vec![],
            k8s_apply_docs: vec![],
            docker_builds: vec![],
            k8s_workload: None,
            pod_selector: std::collections::BTreeMap::new(),
            pod_readiness_ignore: false,
            k8s_custom_apply_cmd: None,
            k8s_custom_delete_cmd: None,
            k8s_custom_image_deps: vec![],
            live_update: vec![],
            k8s_port_forwards: vec![],
            docker_compose_project: None,
        }
    }
}

/// A Kubernetes pod port-forward requested by `k8s_resource(port_forwards=...)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortForwardSpec {
    pub host: String,
    pub local_port: u16,
    pub container_port: u16,
    pub name: String,
    pub link_path: String,
}
