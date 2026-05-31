//! Starlingfile execution: parse and run a Starlingfile with the Starlark interpreter,
//! producing a set of [`Manifest`]s.
//!
//! Mirrors the role of Go's `internal/tiltfile`. Implements a pragmatic subset
//! of Tilt's builtins: `local_resource` (fully executed), plus `docker_build` /
//! `k8s_yaml` / `k8s_resource` which drive real `docker build` + `kubectl apply`
//! via the engine.

pub mod manifest;

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use starlark::any::ProvidesStaticType;
use starlark::environment::{FrozenModule, Globals, GlobalsBuilder, LibraryExtension, Module};
use starlark::eval::{Evaluator, FileLoader};
use starlark::syntax::{AstModule, Dialect};
use starlark::values::list::AllocList;
use starlark::values::list::ListRef;
use starlark::values::none::NoneType;
use starlark::values::structs::AllocStruct;
use starlark::values::tuple::UnpackTuple;
use starlark::values::{
    starlark_value, AllocValue, Heap, NoSerialize, StarlarkValue, Value, ValueLike,
};

use allocative::Allocative;

/// A Tilt `Blob`: a string-like value carrying text, returned by content
/// producers (`blob`, `read_file`, `local`, `kustomize`, `helm`, `filter_yaml`).
/// It displays as its text, concatenates with strings (`+`), and is accepted
/// anywhere Starling coerces a value to a string (via [`coerce_str`]).
#[derive(Debug, Clone, PartialEq, Eq, ProvidesStaticType, NoSerialize, Allocative)]
struct Blob(String);

impl std::fmt::Display for Blob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[starlark_value(type = "blob")]
impl<'v> StarlarkValue<'v> for Blob {
    fn add(&self, rhs: Value<'v>, heap: &'v Heap) -> Option<starlark::Result<Value<'v>>> {
        let rhs = coerce_str(rhs)?;
        Some(Ok(heap.alloc(format!("{}{rhs}", self.0))))
    }
    fn radd(&self, lhs: Value<'v>, heap: &'v Heap) -> Option<starlark::Result<Value<'v>>> {
        let lhs = coerce_str(lhs)?;
        Some(Ok(heap.alloc(format!("{lhs}{}", self.0))))
    }
}

impl<'v> AllocValue<'v> for Blob {
    fn alloc_value(self, heap: &'v Heap) -> Value<'v> {
        heap.alloc_simple(self)
    }
}

/// Coerce a value to a string: a Starlark string, or a [`Blob`]'s text.
fn coerce_str(v: Value) -> Option<String> {
    v.unpack_str()
        .map(str::to_string)
        .or_else(|| v.downcast_ref::<Blob>().map(|b| b.0.clone()))
}

pub use manifest::{
    Cmd, DockerBuild, IgnoreRule, LiveUpdateStep, Manifest, NamedPortLease, PortForwardSpec,
    ProbeAction, ReadinessProbe, TargetKind,
};

/// Separator for encoding live_update steps as strings returned by sync()/run().
const LU_SEP: char = '\u{1}';

/// Separator for encoding `link(url, name)` results as strings (so they can flow
/// through `links=[...]` lists alongside bare URL strings).
const LINK_SEP: char = '\u{2}';
const PORT_FORWARD_SEP: char = '\u{3}';

/// Parse a Starlark dict of `build_args` into key/value pairs.
fn parse_build_args(v: Value) -> Vec<(String, String)> {
    use starlark::values::dict::DictRef;
    let mut out = vec![];
    if let Some(d) = DictRef::from_value(v) {
        for (k, val) in d.iter() {
            let key = k
                .unpack_str()
                .map(str::to_string)
                .unwrap_or_else(|| k.to_str());
            let value = val
                .unpack_str()
                .map(str::to_string)
                .unwrap_or_else(|| val.to_str());
            out.push((key, value));
        }
    }
    out
}

fn service_port_env_name(name: &str) -> String {
    let mut out = String::from("STARLING_");
    let mut previous_sep = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            previous_sep = false;
        } else if !previous_sep {
            out.push('_');
            previous_sep = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out == "STARLING" {
        out.push_str("_SERVICE");
    }
    out.push_str("_PORT");
    out
}

/// Parse `sync()`/`run()` encoded strings into steps, resolving sync locals.
fn parse_live_update(v: Value, dir: &Path) -> Result<Vec<LiveUpdateStep>> {
    let steps: Vec<LiveUpdateStep> = as_str_vec(v)
        .iter()
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(LU_SEP).collect();
            match parts.as_slice() {
                ["sync", local, remote] => Some(LiveUpdateStep::Sync {
                    local: resolve(dir, local).display().to_string(),
                    remote: remote.to_string(),
                }),
                ["run", cmd] => Some(LiveUpdateStep::Run {
                    cmd: cmd.to_string(),
                    echo_off: false,
                    triggers: vec![],
                }),
                ["run", cmd, echo_off, triggers @ ..] => Some(LiveUpdateStep::Run {
                    cmd: cmd.to_string(),
                    echo_off: *echo_off == "1",
                    triggers: triggers
                        .iter()
                        .map(|p| resolve(dir, p).display().to_string())
                        .collect(),
                }),
                ["restart"] => Some(LiveUpdateStep::RestartContainer),
                ["initialsync"] => Some(LiveUpdateStep::InitialSync),
                [first, rest @ ..] if *first == "fallback" => Some(LiveUpdateStep::FallBackOn(
                    rest.iter()
                        .map(|p| resolve(dir, p).display().to_string())
                        .collect(),
                )),
                _ => None,
            }
        })
        .collect();

    let initial_sync_count = steps
        .iter()
        .filter(|step| matches!(step, LiveUpdateStep::InitialSync))
        .count();
    let initial_sync_at_start = steps
        .first()
        .is_some_and(|step| matches!(step, LiveUpdateStep::InitialSync));
    if initial_sync_count > 1 || (initial_sync_count == 1 && !initial_sync_at_start) {
        return Err(anyhow!(
            "initial_sync must appear at most once, at the start of the list"
        ));
    }

    Ok(steps)
}

/// Parse a `links=[...]` value into `(url, label)` pairs. Accepts bare URL
/// strings (label defaults to the URL) and `link(url, name)` results (which
/// encode the name after a [`LINK_SEP`]); an empty name falls back to the URL.
fn parse_links(v: Value) -> Vec<(String, String)> {
    as_str_vec(v)
        .into_iter()
        .map(|s| match s.split_once(LINK_SEP) {
            Some((url, name)) if !name.is_empty() => (url.to_string(), name.to_string()),
            Some((url, _)) => (url.to_string(), url.to_string()),
            None => (s.clone(), s),
        })
        .collect()
}

/// Parse Tilt resource labels. Tilt accepts a string or list of strings for
/// resource grouping; Starling also accepts dicts for existing compatibility.
fn parse_resource_labels(v: Value) -> Vec<(String, String)> {
    let dict_labels = parse_build_args(v);
    if !dict_labels.is_empty() {
        return dict_labels;
    }
    as_str_vec(v)
        .into_iter()
        .map(|label| (label, String::new()))
        .collect()
}

fn looks_like_compose_blob(s: &str) -> bool {
    let trimmed = s.trim_start();
    s.contains('\n')
        || trimmed.starts_with("services:")
        || trimmed.starts_with("name:")
        || trimmed.starts_with("version:")
        || trimmed.starts_with('{')
}

fn compose_config_inputs(v: Value, st: &TfState, dir: &Path) -> Result<Vec<ComposeConfigInput>> {
    let values = if let Some(list) = ListRef::from_value(v) {
        list.iter().collect::<Vec<_>>()
    } else {
        vec![v]
    };
    if values.is_empty() {
        return Err(anyhow!("docker_compose: configPaths must not be empty"));
    }

    let mut inputs = vec![];
    for value in values {
        let Some(raw) = coerce_str(value) else {
            return Err(anyhow!(
                "docker_compose: got {}, want string, blob, or list",
                value.get_type()
            ));
        };
        let raw = raw.as_str();
        let resolved = resolve(dir, raw);
        if resolved.exists() && !looks_like_compose_blob(raw) {
            st.config_files.borrow_mut().push(resolved.clone());
            let content = std::fs::read_to_string(&resolved)
                .with_context(|| format!("docker_compose: reading {}", resolved.display()))?;
            let doc: serde_yaml::Value = serde_yaml::from_str(&content)
                .with_context(|| format!("docker_compose: parsing {}", resolved.display()))?;
            let project_name = doc.get("name").and_then(|n| n.as_str()).map(str::to_string);
            inputs.push(ComposeConfigInput {
                path: resolved,
                content,
                project_name,
            });
        } else if looks_like_compose_blob(raw) {
            let path =
                std::env::temp_dir().join(format!("starling-compose-{}.yml", uuid::Uuid::new_v4()));
            std::fs::write(&path, raw)
                .with_context(|| format!("docker_compose: writing {}", path.display()))?;
            let doc: serde_yaml::Value = serde_yaml::from_str(raw)
                .with_context(|| "docker_compose: parsing inline config")?;
            let project_name = doc.get("name").and_then(|n| n.as_str()).map(str::to_string);
            inputs.push(ComposeConfigInput {
                path,
                content: raw.to_string(),
                project_name,
            });
        } else {
            return Err(anyhow!(
                "docker_compose: reading {}: not found",
                resolved.display()
            ));
        }
    }

    Ok(inputs)
}

use crate::k8s::{self, K8sEntity};

const COMPAT_PRELUDE: &str = r#"
os = struct(
    name = "posix",
    environ = struct(get = _starling_getenv),
    getenv = _starling_getenv,
    getcwd = _starling_getcwd,
    putenv = _starling_putenv,
    unsetenv = _starling_unsetenv,
    path = struct(
        abspath = _starling_path_abspath,
        relpath = _starling_path_relpath,
        basename = _starling_path_basename,
        dirname = _starling_path_dirname,
        exists = _starling_path_exists,
        join = _starling_path_join,
        realpath = _starling_path_realpath,
    ),
)
sys = struct(executable = _starling_sys_executable())
shlex = struct(quote = _starling_shlex_quote)
config = struct(
    tilt_subcommand = "up",
    main_path = _starling_config_main_path(),
    main_dir = _starling_config_main_dir(),
    define_string = _starling_config_define_string,
    define_string_list = _starling_config_define_string_list,
    define_bool = _starling_config_define_bool,
    define_object = _starling_config_define_object,
    parse = _starling_config_parse,
    set_enabled_resources = _starling_config_set_enabled_resources,
    clear_enabled_resources = _starling_config_clear_enabled_resources,
)
v1alpha1 = struct(
    config_map = _starling_v1alpha1_config_map,
    cmd = _starling_v1alpha1_cmd,
    kubernetes_apply = _starling_v1alpha1_kubernetes_apply,
    kubernetes_discovery = _starling_v1alpha1_kubernetes_discovery,
    file_watch = _starling_v1alpha1_file_watch,
    ui_button = _starling_v1alpha1_ui_button,
    extension_repo = _starling_v1alpha1_extension_repo,
    extension = _starling_v1alpha1_extension,
)
"#;

/// The result of loading a Starlingfile.
pub struct LoadResult {
    pub manifests: Vec<Manifest>,
    /// Static proxy routes from `alias(name, port)`.
    pub aliases: Vec<(String, u16)>,
    /// Named TCP ports requested with `starling_port(...)`.
    pub port_leases: Vec<NamedPortLease>,
    /// Captured Starlingfile-execution log output (from `print`, `local`, notes).
    pub log: String,
    /// Directory containing the Starlingfile (base for relative paths).
    #[allow(dead_code)]
    pub config_dir: PathBuf,
    /// Every file read while loading (main file, `include()`d files, `load()`
    /// targets, `read_file()`/`watch_file()` paths) — the engine watches these
    /// and reloads on any change.
    pub config_files: Vec<PathBuf>,
    /// `ci_settings(timeout=...)` in seconds, if set. Consumed by `starling ci`.
    pub ci_timeout_secs: Option<u64>,
    /// `update_settings(max_parallel_updates=...)`, if set. Caps concurrent
    /// parallel local-resource updates in the engine.
    pub max_parallel_updates: Option<usize>,
    /// `set_team(team_id)`, surfaced on the UISession status.
    pub team_id: Option<String>,
    /// Feature flags from `enable_feature`/`disable_feature` (name -> enabled).
    pub feature_flags: Vec<(String, bool)>,
    /// Registered local extension repos: (name, resolved local path).
    pub extension_repos: Vec<(String, String)>,
    /// `ext://` extensions loaded: (ext ref, repo name).
    pub extensions: Vec<(String, String)>,
    /// Secret values to redact from logs, unless `secret_settings(disable_scrub=True)`.
    pub secret_values: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// Arguments after `--` on `starling up`, exposed to Tilt's `config.parse()`
    /// and, if config is not parsed, used as resource names to enable.
    pub args: Vec<String>,
    /// Optional Kubernetes context override for tests or embedders. Normal CLI
    /// use falls back to `kubectl config current-context`.
    pub kube_context: Option<String>,
    /// Optional Kubernetes namespace override for tests or embedders. Normal CLI
    /// use falls back to the current kubectl context namespace, or "default".
    pub kube_namespace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigKind {
    String,
    StringList,
    Bool,
    Object,
}

#[derive(Debug, Clone)]
struct ConfigSetting {
    kind: ConfigKind,
    positional: bool,
}

#[derive(Debug, Clone)]
struct ParsedConfig {
    values: BTreeMap<String, serde_json::Value>,
}

impl ParsedConfig {
    fn into_starlark<'v>(self, heap: &'v Heap) -> Value<'v> {
        heap.alloc(serde_json::Value::Object(
            self.values.into_iter().collect::<serde_json::Map<_, _>>(),
        ))
    }
}

/// The directory of a file path, or "." if it has no parent.
fn parent_or_dot(path: &Path) -> PathBuf {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

/// Lexically absolutize `path` against `cwd` (no filesystem access) and return
/// its normalized `Normal` components, with `.`/`..` resolved in place.
fn lexical_abs_components(path: &str, cwd: &Path) -> Vec<String> {
    use std::path::Component;
    let joined = if Path::new(path).is_absolute() {
        PathBuf::from(path)
    } else {
        cwd.join(path)
    };
    let mut comps: Vec<String> = Vec::new();
    for component in joined.components() {
        match component {
            Component::Normal(s) => comps.push(s.to_string_lossy().to_string()),
            Component::ParentDir => {
                comps.pop();
            }
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
        }
    }
    comps
}

/// Clone an extension repo (`https://`, `git@…`, `git://`, or a git repo path)
/// into a local cache (`$STARLING_EXT_CACHE` or `~/.starling/extension-repos`)
/// and return the checkout path. Reuses an existing checkout. Tilt's `ext://`
/// repos are git repos; this is the network-fetch path (local clones work too).
fn clone_extension_repo(name: &str, url: &str) -> Result<PathBuf> {
    let base = std::env::var("STARLING_EXT_CACHE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".starling")
                .join("extension-repos")
        });
    let slug: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let dest = base.join(slug);
    if dest.join(".git").exists() {
        return Ok(dest); // already cloned
    }
    std::fs::create_dir_all(&base)
        .with_context(|| format!("extension_repo: creating cache {}", base.display()))?;
    let status = Command::new("git")
        .args(["clone", "--depth", "1", url])
        .arg(&dest)
        .status()
        .with_context(|| format!("extension_repo: running git clone {url}"))?;
    if !status.success() {
        return Err(anyhow!(
            "extension_repo({name:?}): git clone {url:?} failed (status {status})"
        ));
    }
    Ok(dest)
}

/// Enforce a `version_settings(constraint=...)` semver range against the running
/// version, matching Tilt's behavior of failing the load when it isn't satisfied.
fn check_version_constraint(constraint: &str, version: &str) -> Result<()> {
    let req = semver::VersionReq::parse(constraint)
        .map_err(|e| anyhow!("version_settings: invalid constraint {constraint:?}: {e}"))?;
    let ver = semver::Version::parse(version)
        .map_err(|e| anyhow!("version_settings: invalid running version {version:?}: {e}"))?;
    if !req.matches(&ver) {
        return Err(anyhow!(
            "version_settings: running Starling {version} does not satisfy required constraint {constraint:?}"
        ));
    }
    Ok(())
}

/// POSIX-style `os.path.relpath`: the relative path from `base` to `target`,
/// emitting `..` segments for cross-directory cases (matching Python/Tilt).
/// Both paths are absolutized against `cwd` first; no filesystem access.
fn posix_relpath(target: &str, base: &str, cwd: &Path) -> String {
    let target_comps = lexical_abs_components(target, cwd);
    let base_comps = lexical_abs_components(base, cwd);
    let common = target_comps
        .iter()
        .zip(base_comps.iter())
        .take_while(|(a, b)| a == b)
        .count();
    let ups = base_comps.len() - common;
    let mut out: Vec<String> = vec!["..".to_string(); ups];
    out.extend(target_comps[common..].iter().cloned());
    if out.is_empty() {
        ".".to_string()
    } else {
        out.join("/")
    }
}

/// Build the Starlark globals (standard + the extensions Starlingfiles use).
fn build_globals() -> Globals {
    GlobalsBuilder::extended_by(&[
        LibraryExtension::Print,
        LibraryExtension::StructType,
        LibraryExtension::Json,
        LibraryExtension::Map,
        LibraryExtension::Filter,
        LibraryExtension::Debug,
        LibraryExtension::Typing,
    ])
    .with(starling_builtins)
    .build()
}

/// Routes `print(...)` output into the Starlingfile log buffer.
struct LogPrint<'a>(&'a RefCell<String>);
impl starlark::PrintHandler for LogPrint<'_> {
    fn println(&self, text: &str) -> anyhow::Result<()> {
        let mut log = self.0.borrow_mut();
        log.push_str(text);
        log.push('\n');
        Ok(())
    }
}

/// Resolves Starlark `load("path", "sym")` statements to other Starlingfiles,
/// evaluating each into the same shared [`TfState`] (so their resource
/// registrations take effect too) and recording the path for watching.
struct StarlingLoader<'a> {
    st: &'a TfState,
    globals: &'a Globals,
}

impl FileLoader for StarlingLoader<'_> {
    fn load(&self, path: &str) -> anyhow::Result<FrozenModule> {
        let target = if path.starts_with("ext://") {
            self.st.resolve_ext(path)?
        } else {
            resolve(&self.st.cur_dir(), path)
        };
        self.st.config_files.borrow_mut().push(target.clone());
        let src = std::fs::read_to_string(&target)
            .with_context(|| format!("load: reading {}", target.display()))?;
        let src = with_compat_prelude(src);
        let ast = AstModule::parse(&target.to_string_lossy(), src, &Dialect::Extended)
            .map_err(|e| anyhow!("parsing {}: {e}", target.display()))?;
        let module = Module::new();
        let printer = LogPrint(&self.st.log);
        self.st.dir_stack.borrow_mut().push(parent_or_dot(&target));
        let result = {
            let mut eval = Evaluator::new(&module);
            eval.extra = Some(self.st);
            eval.set_loader(self);
            eval.set_print_handler(&printer);
            eval.eval_module(ast, self.globals)
        };
        self.st.dir_stack.borrow_mut().pop();
        result.map_err(|e| anyhow!("evaluating {}: {e}", target.display()))?;
        module
            .freeze()
            .map_err(|e| anyhow!("freezing {}: {e}", target.display()))
    }
}

/// Config recorded by `k8s_resource(...)`, applied during assembly.
#[derive(Default)]
struct K8sResourceConfig {
    workload: String,
    new_name: Option<String>,
    port_forwards: Vec<PortForwardSpec>,
    links: Vec<(String, String)>,
    resource_deps: Vec<String>,
    trigger_mode: Option<i32>,
    auto_init: bool,
    labels: Vec<(String, String)>,
    extra_pod_selectors: Vec<(String, String)>,
    objects: Vec<String>,
    pod_readiness: Option<String>,
}

/// Config recorded by `dc_resource(...)`, applied after Compose resources are
/// loaded so declaration order matches Tilt.
#[derive(Default)]
struct DcResourceConfig {
    name: String,
    image: Option<String>,
    new_name: Option<String>,
    resource_deps: Vec<String>,
    trigger_mode: Option<i32>,
    auto_init: bool,
    links: Vec<(String, String)>,
    labels: Vec<(String, String)>,
    project_name: Option<String>,
}

struct ComposeConfigInput {
    path: PathBuf,
    content: String,
    project_name: Option<String>,
}

/// Mutable state accumulated while a Starlingfile executes. Threaded through the
/// Starlark `Evaluator` via its `extra` field.
#[derive(ProvidesStaticType, Default)]
struct TfState {
    /// Stack of "current directories"; the top is the dir of the file being
    /// evaluated (so relative paths in `include()`d/`load()`ed files resolve
    /// against their own location, like Tilt).
    dir_stack: RefCell<Vec<PathBuf>>,
    /// Every file read so far (for the engine's reload watcher).
    config_files: RefCell<Vec<PathBuf>>,
    /// `local_resource` manifests (k8s manifests are assembled post-eval).
    local_manifests: RefCell<Vec<Manifest>>,
    docker_builds: RefCell<Vec<DockerBuild>>,
    default_registry: RefCell<Option<DefaultRegistry>>,
    k8s_image_locators: RefCell<Vec<K8sImageLocator>>,
    k8s_kind_configs: RefCell<Vec<K8sKindConfig>>,
    k8s_entities: RefCell<Vec<K8sEntity>>,
    k8s_custom_manifests: RefCell<Vec<Manifest>>,
    k8s_configs: RefCell<Vec<K8sResourceConfig>>,
    dc_configs: RefCell<Vec<DcResourceConfig>>,
    /// Static proxy routes registered via `alias(name, port)`.
    aliases: RefCell<Vec<(String, u16)>>,
    /// Named host TCP ports requested via `starling_port(...)`.
    port_leases: RefCell<Vec<NamedPortLease>>,
    /// Tiltfile args passed after `starling up --`.
    args: RefCell<Vec<String>>,
    config_defs: RefCell<BTreeMap<String, ConfigSetting>>,
    config_parse_called: RefCell<bool>,
    enabled_resources: RefCell<Option<Vec<String>>>,
    disable_all_resources: RefCell<bool>,
    kube_context: RefCell<Option<String>>,
    kube_namespace: RefCell<Option<String>>,
    allowed_kube_contexts: RefCell<Option<Vec<String>>>,
    team_id: RefCell<Option<String>>,
    watch_ignores: RefCell<Vec<IgnoreRule>>,
    /// Resource names computed by `workload_to_resource_function`, keyed by the
    /// workload's object id string (`name:kind:namespace:group`, lowercased).
    /// Empty when no function was registered.
    workload_resource_names: RefCell<HashMap<String, String>>,
    /// `ci_settings(timeout=...)` in seconds, consumed by `starling ci`.
    ci_timeout_secs: RefCell<Option<u64>>,
    /// `update_settings(max_parallel_updates=...)`, caps concurrent parallel
    /// local-resource updates in the engine.
    max_parallel_updates: RefCell<Option<usize>>,
    /// Feature flags toggled via `enable_feature`/`disable_feature`, surfaced on
    /// the UISession status.
    feature_flags: RefCell<BTreeMap<String, bool>>,
    /// `extension_repo(name, url)` registrations: repo name -> local path.
    /// Only local (`file://` or path) repos are supported.
    extension_repos: RefCell<BTreeMap<String, String>>,
    /// `ext://` extensions loaded during evaluation: (ext ref, repo name).
    loaded_extensions: RefCell<Vec<(String, String)>>,
    /// `secret_settings(disable_scrub=...)`: when true, secret values are not
    /// redacted from logs.
    disable_scrub: RefCell<bool>,
    log: RefCell<String>,
}

#[derive(Debug, Clone)]
struct K8sImageLocator {
    kind: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    api_version: Option<String>,
    path: Vec<String>,
    object: Option<K8sImageObjectLocator>,
}

#[derive(Debug, Clone)]
struct K8sKindConfig {
    kind: String,
    api_version: Option<String>,
    pod_readiness: Option<String>,
}

#[derive(Debug, Clone)]
struct K8sImageObjectLocator {
    repo_field: String,
    tag_field: String,
}

#[derive(Debug, Clone)]
struct DefaultRegistry {
    host: String,
    host_from_cluster: Option<String>,
    single_name: Option<String>,
}

impl TfState {
    fn logln(&self, msg: &str) {
        let mut log = self.log.borrow_mut();
        log.push_str(msg);
        if !msg.ends_with('\n') {
            log.push('\n');
        }
    }

    /// Resolve an `ext://<repo>[/<sub>]` reference to a local Tiltfile path
    /// using the registered `extension_repo`s, recording the loaded extension.
    /// Errors if the repo isn't registered (e.g. a remote default repo).
    fn resolve_ext(&self, reference: &str) -> Result<PathBuf> {
        let spec = reference
            .strip_prefix("ext://")
            .ok_or_else(|| anyhow!("not an ext:// reference: {reference}"))?;
        let (repo, sub) = match spec.split_once('/') {
            Some((repo, sub)) => (repo, sub),
            None => (spec, ""),
        };
        let repos = self.extension_repos.borrow();
        let base = repos.get(repo).ok_or_else(|| {
            anyhow!(
                "ext://{spec}: extension repo {repo:?} is not registered. \
                 Register a local repo with extension_repo(name={repo:?}, url=\"file:///path\"); \
                 remote extension repos are not supported."
            )
        })?;
        let mut path = PathBuf::from(base);
        if !sub.is_empty() {
            path.push(sub);
        }
        path.push("Tiltfile");
        self.loaded_extensions
            .borrow_mut()
            .push((spec.to_string(), repo.to_string()));
        Ok(path)
    }

    /// The directory of the file currently being evaluated.
    fn cur_dir(&self) -> PathBuf {
        self.dir_stack
            .borrow()
            .last()
            .cloned()
            .unwrap_or_else(|| PathBuf::from("."))
    }
}

fn state<'a>(eval: &'a Evaluator) -> &'a TfState {
    eval.extra
        .expect("Starlingfile evaluator missing TfState")
        .downcast_ref::<TfState>()
        .expect("Starlingfile evaluator extra is not TfState")
}

fn with_compat_prelude(src: String) -> String {
    format!("{COMPAT_PRELUDE}\n{src}")
}

/// Read a Starlark int or float into an `f64`, if it is numeric.
fn as_f64(v: Value) -> Option<f64> {
    use starlark::values::UnpackValue;
    f64::unpack_value(v)
}

/// Convert a Starlark value into a list of strings (string → single element).
fn as_str_vec(v: Value) -> Vec<String> {
    if let Some(s) = coerce_str(v) {
        return vec![s];
    }
    if let Some(list) = ListRef::from_value(v) {
        return list
            .iter()
            .map(|e| coerce_str(e).unwrap_or_else(|| e.to_str()))
            .collect();
    }
    vec![]
}

fn parse_extra_tags(v: Value) -> Result<Vec<String>> {
    as_str_vec(v)
        .into_iter()
        .map(|tag| {
            if tag.trim().is_empty() || tag.chars().any(char::is_whitespace) {
                Err(anyhow!(
                    "Argument extra_tag={tag:?} not a valid image reference"
                ))
            } else {
                Ok(tag)
            }
        })
        .collect()
}

fn value_to_string(v: Value) -> String {
    v.unpack_str()
        .map(str::to_string)
        .unwrap_or_else(|| v.to_str())
}

fn value_to_text(v: Value) -> Result<String> {
    if let Some(s) = v.unpack_str() {
        return Ok(s.to_string());
    }
    let json = v.to_json_value()?;
    Ok(match json {
        serde_json::Value::String(s) => s,
        other => serde_json::to_string(&other)?,
    })
}

fn yaml_to_json_value(v: serde_yaml::Value) -> Result<serde_json::Value> {
    serde_json::to_value(v).context("converting YAML to Starlark value")
}

fn helm_template_args(
    chart: &Path,
    release: &str,
    namespace: Option<&str>,
    values: Vec<PathBuf>,
    set: Vec<String>,
    kube_version: Option<&str>,
    skip_crds: bool,
) -> Vec<String> {
    let mut argv = vec![
        "helm".to_string(),
        "template".to_string(),
        release.to_string(),
        chart.display().to_string(),
    ];
    if let Some(ns) = namespace {
        argv.push("--namespace".to_string());
        argv.push(ns.to_string());
    }
    for value_path in values {
        argv.push("--values".to_string());
        argv.push(value_path.display().to_string());
    }
    for value in set {
        argv.push("--set".to_string());
        argv.push(value);
    }
    if let Some(kube_version) = kube_version {
        argv.push("--kube-version".to_string());
        argv.push(kube_version.to_string());
    }
    if !skip_crds {
        argv.push("--include-crds".to_string());
    }
    argv
}

fn read_tracked_text<'v>(
    path: &str,
    default: Value<'v>,
    eval: &mut Evaluator<'v, '_>,
) -> Result<Option<String>> {
    let st = state(eval);
    let p = resolve(&st.cur_dir(), path);
    st.config_files.borrow_mut().push(p.clone());
    match std::fs::read_to_string(&p) {
        Ok(s) => Ok(Some(s)),
        Err(_) if !default.is_none() => Ok(None),
        Err(e) => Err(anyhow!("{path:?}: {e}")),
    }
}

fn list_dir_entries(base: &Path, recursive: bool) -> Result<Vec<String>> {
    fn walk(root: &Path, dir: &Path, recursive: bool, out: &mut Vec<String>) -> Result<()> {
        let mut entries = std::fs::read_dir(dir)
            .with_context(|| format!("listdir: reading {}", dir.display()))?
            .collect::<std::io::Result<Vec<_>>>()
            .with_context(|| format!("listdir: reading {}", dir.display()))?;
        entries.sort_by_key(|e| e.path());
        for entry in entries {
            let path = entry.path();
            let rel = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            out.push(rel);
            if recursive && path.is_dir() {
                walk(root, &path, recursive, out)?;
            }
        }
        Ok(())
    }

    let mut out = Vec::new();
    walk(base, base, recursive, &mut out)?;
    Ok(out)
}

fn ignore_rules_from_value(base: &Path, value: Value) -> Vec<IgnoreRule> {
    as_str_vec(value)
        .into_iter()
        .filter_map(|pattern| ignore_rule(base, pattern))
        .collect()
}

fn ignore_rule(base: &Path, pattern: String) -> Option<IgnoreRule> {
    let pattern = pattern.trim().to_string();
    if pattern.is_empty() || pattern.starts_with('#') {
        return None;
    }
    Some(IgnoreRule {
        base: base.to_path_buf(),
        pattern,
    })
}

fn read_tiltignore(path: &Path) -> Result<Vec<IgnoreRule>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(anyhow!("reading {}: {e}", path.display())),
    };
    let base = parent_or_dot(path);
    Ok(text
        .lines()
        .filter_map(|line| ignore_rule(&base, line.to_string()))
        .collect())
}

fn shlex_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    if s.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_./:@%+=".contains(&b))
    {
        return s.to_string();
    }
    format!("'{}'", s.replace('\'', "'\"'\"'"))
}

fn kubectl_output(args: &[&str]) -> Option<String> {
    let out = Command::new("kubectl").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&out.stdout).trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn current_kube_context(st: &TfState) -> String {
    if let Some(ctx) = st.kube_context.borrow().clone() {
        return ctx;
    }
    let ctx = kubectl_output(&["config", "current-context"]).unwrap_or_default();
    *st.kube_context.borrow_mut() = Some(ctx.clone());
    ctx
}

fn current_kube_namespace(st: &TfState) -> String {
    if let Some(ns) = st.kube_namespace.borrow().clone() {
        return ns;
    }
    let ns = kubectl_output(&[
        "config",
        "view",
        "--minify",
        "--output",
        "jsonpath={..namespace}",
    ])
    .unwrap_or_else(|| "default".to_string());
    *st.kube_namespace.borrow_mut() = Some(ns.clone());
    ns
}

fn enforce_allowed_kube_contexts(st: &TfState) -> Result<()> {
    let Some(allowed) = st.allowed_kube_contexts.borrow().clone() else {
        return Ok(());
    };
    let current = current_kube_context(st);
    if allowed.iter().any(|ctx| ctx == &current) {
        Ok(())
    } else {
        Err(anyhow!(
            "current Kubernetes context {current:?} is not allowed; expected one of {:?}",
            allowed
        ))
    }
}

fn optional_i32_min(name: &str, value: Value, min: i32) -> Result<Option<i32>> {
    if value.is_none() {
        return Ok(None);
    }
    let Some(n) = value.unpack_i32() else {
        return Err(anyhow!("{name}: got {}, want int", value.get_type()));
    };
    if n < min {
        return Err(anyhow!("{name}: must be >= {min}"));
    }
    Ok(Some(n))
}

fn define_config_setting(
    st: &TfState,
    name: String,
    kind: ConfigKind,
    positional: bool,
) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("config setting name is required"));
    }
    if *st.config_parse_called.borrow() {
        return Err(anyhow!(
            "config.define_* cannot be called after config.parse"
        ));
    }
    let mut defs = st.config_defs.borrow_mut();
    if defs.contains_key(&name) {
        return Err(anyhow!("{name} defined multiple times"));
    }
    if positional && defs.iter().any(|(_, existing)| existing.positional) {
        return Err(anyhow!("only one config setting can use args=True"));
    }
    defs.insert(name, ConfigSetting { kind, positional });
    Ok(())
}

fn set_config_value(
    values: &mut BTreeMap<String, serde_json::Value>,
    defs: &BTreeMap<String, ConfigSetting>,
    name: &str,
    raw: &str,
) -> Result<()> {
    let def = defs
        .get(name)
        .ok_or_else(|| anyhow!("unknown Tiltfile config setting {name:?}"))?;
    match def.kind {
        ConfigKind::String => {
            values.insert(name.to_string(), serde_json::Value::String(raw.to_string()));
        }
        ConfigKind::StringList => {
            let entry = values
                .entry(name.to_string())
                .or_insert_with(|| serde_json::Value::Array(vec![]));
            let Some(items) = entry.as_array_mut() else {
                return Err(anyhow!("setting {name:?} is not a string list"));
            };
            items.push(serde_json::Value::String(raw.to_string()));
        }
        ConfigKind::Bool => {
            let parsed = raw
                .parse::<bool>()
                .with_context(|| format!("invalid boolean value for {name:?}: {raw:?}"))?;
            values.insert(name.to_string(), serde_json::Value::Bool(parsed));
        }
        ConfigKind::Object => {
            let parsed = serde_json::from_str::<serde_json::Value>(raw)
                .with_context(|| format!("decoding JSON for object setting {name:?}: {raw:?}"))?;
            values.insert(name.to_string(), parsed);
        }
    }
    Ok(())
}

fn load_config_file(
    path: &Path,
    defs: &BTreeMap<String, ConfigSetting>,
    values: &mut BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(anyhow!("reading {}: {e}", path.display())),
    };
    let parsed: serde_json::Map<String, serde_json::Value> =
        serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    for (name, value) in parsed {
        let def = defs
            .get(&name)
            .ok_or_else(|| anyhow!("{} specified unknown setting name {name:?}", path.display()))?;
        match (def.kind, value) {
            (ConfigKind::String, serde_json::Value::String(s)) => {
                values.insert(name, serde_json::Value::String(s));
            }
            (ConfigKind::StringList, serde_json::Value::Array(items)) => {
                if items.iter().all(|v| v.as_str().is_some()) {
                    values.insert(name, serde_json::Value::Array(items));
                } else {
                    return Err(anyhow!(
                        "{} specified invalid value for setting {name}",
                        path.display()
                    ));
                }
            }
            (ConfigKind::Bool, serde_json::Value::Bool(b)) => {
                values.insert(name, serde_json::Value::Bool(b));
            }
            (ConfigKind::Object, value) => {
                if !value.is_null() {
                    values.insert(name, value);
                }
            }
            _ => {
                return Err(anyhow!(
                    "{} specified invalid value for setting {name}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

fn parse_config(st: &TfState) -> Result<ParsedConfig> {
    let defs = st.config_defs.borrow().clone();
    let mut values = BTreeMap::new();

    let config_path = st.cur_dir().join("tilt_config.json");
    st.config_files.borrow_mut().push(config_path.clone());
    load_config_file(&config_path, &defs, &mut values)?;

    let positional = defs
        .iter()
        .find_map(|(name, def)| def.positional.then(|| name.clone()));
    let args = st.args.borrow().clone();
    let mut i = 0;
    let mut positionals = Vec::new();
    let mut args_seen = HashSet::new();
    while i < args.len() {
        let arg = &args[i];
        if let Some(flag) = arg.strip_prefix("--") {
            let (name, raw_value) = if let Some((name, value)) = flag.split_once('=') {
                (name.to_string(), value.to_string())
            } else {
                let name = flag.to_string();
                let def = defs
                    .get(&name)
                    .ok_or_else(|| anyhow!("unknown Tiltfile config setting {name:?}"))?;
                match def.kind {
                    ConfigKind::Bool => {
                        if args
                            .get(i + 1)
                            .map(|s| s.starts_with("--") || s.parse::<bool>().is_err())
                            .unwrap_or(true)
                        {
                            (name, "true".to_string())
                        } else {
                            i += 1;
                            (name, args[i].clone())
                        }
                    }
                    _ => {
                        i += 1;
                        let value = args
                            .get(i)
                            .ok_or_else(|| anyhow!("missing value for --{name}"))?
                            .clone();
                        (name, value)
                    }
                }
            };
            let def = defs
                .get(&name)
                .ok_or_else(|| anyhow!("unknown Tiltfile config setting {name:?}"))?;
            if !args_seen.insert(name.clone()) && def.kind != ConfigKind::StringList {
                return Err(anyhow!("setting {name:?} specified multiple times"));
            }
            if def.kind == ConfigKind::StringList
                && values
                    .get(&name)
                    .and_then(|v| v.as_array())
                    .map(|items| !items.is_empty())
                    .unwrap_or(false)
                && !args_seen.contains(&format!("{name}\0reset"))
            {
                values.insert(name.clone(), serde_json::Value::Array(vec![]));
                args_seen.insert(format!("{name}\0reset"));
            }
            set_config_value(&mut values, &defs, &name, &raw_value)?;
        } else {
            positionals.push(arg.clone());
        }
        i += 1;
    }

    if !positionals.is_empty() {
        let Some(name) = positional else {
            return Err(anyhow!(
                "positional CLI args ({:?}) were specified, but none were expected",
                positionals.join(" ")
            ));
        };
        for arg in positionals {
            set_config_value(&mut values, &defs, &name, &arg)?;
        }
    }

    *st.config_parse_called.borrow_mut() = true;
    Ok(ParsedConfig { values })
}

fn filter_enabled_manifests(st: &TfState, manifests: Vec<Manifest>) -> Result<Vec<Manifest>> {
    if *st.disable_all_resources.borrow() {
        return Ok(vec![]);
    }
    let requested = if let Some(resources) = st.enabled_resources.borrow().clone() {
        resources
    } else if !*st.config_parse_called.borrow() {
        st.args.borrow().clone()
    } else {
        vec![]
    };
    if requested.is_empty() {
        return Ok(manifests);
    }

    let names: HashSet<String> = manifests.iter().map(|m| m.name.clone()).collect();
    let unknown: Vec<String> = requested
        .iter()
        .filter(|name| !names.contains(*name))
        .cloned()
        .collect();
    if !unknown.is_empty() {
        return Err(anyhow!(
            "requested resources not found: {}",
            unknown.join(", ")
        ));
    }

    let by_name: HashMap<String, &Manifest> =
        manifests.iter().map(|m| (m.name.clone(), m)).collect();
    let mut keep = HashSet::new();
    fn add_with_deps(name: &str, by_name: &HashMap<String, &Manifest>, keep: &mut HashSet<String>) {
        if !keep.insert(name.to_string()) {
            return;
        }
        if let Some(m) = by_name.get(name) {
            for dep in &m.resource_deps {
                add_with_deps(dep, by_name, keep);
            }
        }
    }
    for name in &requested {
        add_with_deps(name, &by_name, &mut keep);
    }

    Ok(manifests
        .into_iter()
        .filter(|m| keep.contains(&m.name))
        .collect())
}

fn validate_custom_build_image_deps(st: &TfState) -> Result<()> {
    let builds = st.docker_builds.borrow();
    let refs: HashSet<_> = builds
        .iter()
        .map(|build| build.image_ref.as_str())
        .collect();
    for build in builds.iter().filter(|build| build.command.is_some()) {
        for dep in &build.image_deps {
            if !refs.contains(dep.as_str()) {
                return Err(anyhow!(
                    "image {:?}: image dep {:?} not found",
                    build.image_ref,
                    dep
                ));
            }
        }
    }
    Ok(())
}

/// The argv that runs a shell string on the host: `cmd.exe /S /C <s>` on
/// Windows, `sh -c <s>` elsewhere (matching Tilt's host-command behavior).
fn shell_argv(s: &str, windows: bool) -> Vec<String> {
    if windows {
        vec!["cmd.exe".into(), "/S".into(), "/C".into(), s.to_string()]
    } else {
        vec!["sh".into(), "-c".into(), s.to_string()]
    }
}

/// Build a [`Cmd`] from a Starlark value: a string runs through the host shell
/// (`sh -c`, or `cmd.exe /S /C` on Windows); a list is an explicit argv.
fn as_cmd(v: Value, workdir: &Path) -> Cmd {
    let mut cmd = Cmd {
        workdir: Some(workdir.to_path_buf()),
        ..Default::default()
    };
    if let Some(s) = coerce_str(v) {
        if !s.trim().is_empty() {
            cmd.argv = shell_argv(&s, cfg!(windows));
        }
    } else {
        cmd.argv = as_str_vec(v);
    }
    cmd
}

fn as_bat_cmd(v: Value, workdir: &Path) -> Cmd {
    let mut cmd = Cmd {
        workdir: Some(workdir.to_path_buf()),
        ..Default::default()
    };
    if let Some(s) = v.unpack_str() {
        if !s.trim().is_empty() {
            cmd.argv = vec!["cmd.exe".into(), "/S".into(), "/C".into(), s.to_string()];
        }
    } else {
        cmd.argv = as_str_vec(v);
    }
    cmd
}

fn as_platform_cmd(primary: Value, windows: Value, workdir: &Path) -> Cmd {
    if cfg!(windows) && !windows.is_none() {
        as_bat_cmd(windows, workdir)
    } else {
        as_cmd(primary, workdir)
    }
}

/// Strip a tag/digest from a container image reference, returning the repo.
fn image_repo(image: &str) -> &str {
    image
        .split('@')
        .next()
        .unwrap_or(image)
        .rsplit_once(':')
        .map_or(image, |(repo, tag)| {
            // A ':' in the registry host:port isn't a tag; only treat the final
            // path segment's ':' as a tag.
            if tag.contains('/') {
                image
            } else {
                repo
            }
        })
}

/// True if a container image refers to the given docker_build ref.
fn image_matches(image: &str, build_ref: &str) -> bool {
    let img = image_repo(image);
    img == build_ref || img.ends_with(&format!("/{build_ref}"))
}

/// Starlark-facing trigger mode values (match Tilt).
const TRIGGER_MODE_AUTO_VAL: i32 = 1;
const TRIGGER_MODE_MANUAL_VAL: i32 = 2;

/// Combine the Starlark `trigger_mode` (AUTO/MANUAL) with `auto_init` into the
/// model trigger mode (matches Tilt's `pkg/model.TriggerMode`):
/// 0 = Auto, 1 = ManualWithAutoInit, 2 = Manual, 3 = AutoWithManualInit.
fn model_trigger_mode(trigger_mode: Option<i32>, auto_init: bool) -> i32 {
    let manual = trigger_mode == Some(TRIGGER_MODE_MANUAL_VAL);
    match (manual, auto_init) {
        (false, true) => 0,
        (false, false) => 3,
        (true, true) => 1,
        (true, false) => 2,
    }
}

#[allow(clippy::too_many_arguments)]
fn record_local_manifest<'v>(
    st: &TfState,
    builtin_name: &str,
    name: String,
    cmd: Value<'v>,
    cmd_bat: Value<'v>,
    deps: Value<'v>,
    serve_cmd: Value<'v>,
    serve_cmd_bat: Value<'v>,
    resource_deps: Value<'v>,
    trigger_mode: Option<i32>,
    auto_init: bool,
    links: Value<'v>,
    labels: Value<'v>,
    env: Value<'v>,
    dir: Value<'v>,
    serve_env: Value<'v>,
    serve_dir: Value<'v>,
    ignore: Value<'v>,
    readiness_probe: Value<'v>,
    serve_port: Value<'v>,
    allow_parallel: bool,
    is_test: bool,
) -> Result<()> {
    let base = st.cur_dir();
    let cmd_workdir = dir
        .unpack_str()
        .map(|d| resolve(&base, d))
        .unwrap_or_else(|| base.clone());
    let serve_workdir = serve_dir
        .unpack_str()
        .map(|d| resolve(&base, d))
        .unwrap_or_else(|| base.clone());
    let update_cmd = if cmd.is_none() && (!cfg!(windows) || cmd_bat.is_none()) {
        Cmd::default()
    } else {
        let mut c = as_platform_cmd(cmd, cmd_bat, &cmd_workdir);
        c.env = parse_build_args(env);
        c
    };
    let serve_cmd_parsed = if serve_cmd.is_none() && (!cfg!(windows) || serve_cmd_bat.is_none()) {
        Cmd::default()
    } else {
        let mut c = as_platform_cmd(serve_cmd, serve_cmd_bat, &serve_workdir);
        c.env = parse_build_args(serve_env);
        c
    };

    if !dir.is_none() && update_cmd.is_empty() {
        if !serve_cmd_parsed.is_empty() {
            return Err(anyhow!(
                "{builtin_name}: 'dir' only affects 'cmd', not 'serve_cmd'. Did you mean to use 'serve_dir' instead?"
            ));
        }
        return Err(anyhow!(
            "{builtin_name}: 'dir' specified but 'cmd' is empty"
        ));
    }
    if !serve_dir.is_none() && serve_cmd_parsed.is_empty() {
        return Err(anyhow!(
            "{builtin_name}: 'serve_dir' specified but 'serve_cmd' is empty"
        ));
    }
    if update_cmd.is_empty() && serve_cmd_parsed.is_empty() {
        return Err(anyhow!(
            "{builtin_name} must have a cmd and/or a serve_cmd, but both were empty"
        ));
    }
    if st
        .local_manifests
        .borrow()
        .iter()
        .any(|existing| existing.name == name)
    {
        return Err(anyhow!("local_resource named {name:?} already exists"));
    }

    let mut m = Manifest::new(name, TargetKind::Local);
    m.is_test = is_test;
    m.allow_parallel = allow_parallel || update_cmd.is_empty();
    m.ignore_rules
        .extend(ignore_rules_from_value(&base, ignore));
    if let Some(json) = readiness_probe.unpack_str() {
        if !serve_cmd_parsed.is_empty() {
            m.readiness_probe = Some(serde_json::from_str(json).map_err(|_| {
                anyhow!("{builtin_name}: readiness_probe must be a probe(...) value")
            })?);
        } else {
            st.logln(&format!(
                "WARNING: Ignoring readiness probe for local resource {:?} (no serve_cmd was defined)",
                m.name
            ));
        }
    }
    m.trigger_mode = model_trigger_mode(trigger_mode, auto_init);
    m.auto_init = auto_init;
    if let Some(p) = serve_port.unpack_i32() {
        if p > 0 && p < 65536 {
            m.serve_port = Some(p as u16);
        }
    }
    m.update_cmd = update_cmd;
    m.serve_cmd = serve_cmd_parsed;
    if !deps.is_none() {
        m.deps = as_str_vec(deps)
            .into_iter()
            .map(|d| resolve(&base, &d))
            .collect();
    }
    if !resource_deps.is_none() {
        m.resource_deps = as_str_vec(resource_deps);
    }
    if !links.is_none() {
        m.links.extend(parse_links(links));
    }
    if !labels.is_none() {
        m.labels = parse_resource_labels(labels).into_iter().collect();
    }
    st.local_manifests.borrow_mut().push(m);
    Ok(())
}

fn valid_port(port: i32) -> Option<u16> {
    (port > 0 && port < 65536).then_some(port as u16)
}

fn parse_port_forward_specs(v: Value) -> Result<Vec<PortForwardSpec>> {
    let specs = match v.unpack_i32().and_then(valid_port) {
        Some(port) => vec![PortForwardSpec {
            host: "127.0.0.1".to_string(),
            local_port: port,
            container_port: port,
            name: String::new(),
            link_path: String::new(),
        }],
        None => as_str_vec(v)
            .into_iter()
            .map(|s| parse_port_forward_spec(&s))
            .collect::<Result<Vec<_>>>()?,
    };
    Ok(specs)
}

/// Parse a port-forward spec matching Tilt's forms:
/// `"8000"` (local only), `"8000:9000"` (local:container), encoded
/// `port_forward(...)`, and `"host:8000:9000"` (host:local:container).
fn parse_port_forward_spec(pf: &str) -> Result<PortForwardSpec> {
    let encoded_prefix = format!("pf{PORT_FORWARD_SEP}");
    if let Some(encoded) = pf.strip_prefix(&encoded_prefix) {
        let parts: Vec<&str> = encoded.split(PORT_FORWARD_SEP).collect();
        if let [host, local, _container, name, path] = parts.as_slice() {
            return Ok(PortForwardSpec {
                host: if host.is_empty() {
                    "127.0.0.1".to_string()
                } else {
                    (*host).to_string()
                },
                local_port: local
                    .parse::<i32>()
                    .ok()
                    .and_then(valid_port)
                    .ok_or_else(|| anyhow!("port_forward local_port must be in range 1-65535"))?,
                container_port: _container
                    .parse::<i32>()
                    .ok()
                    .and_then(valid_port)
                    .ok_or_else(|| {
                        anyhow!("port_forward container_port must be in range 1-65535")
                    })?,
                name: (*name).to_string(),
                link_path: (*path).to_string(),
            });
        }
    }
    let parts: Vec<&str> = pf.split(':').collect();
    let (host, local, container) = match parts.as_slice() {
        [local] => ("127.0.0.1".to_string(), *local, *local),
        [local, container] => ("127.0.0.1".to_string(), *local, *container),
        [host, local, _container] => {
            let h = if host.is_empty() { "127.0.0.1" } else { host };
            (h.to_string(), *local, *_container)
        }
        _ => {
            return Err(anyhow!(
                "port_forwards value {pf:?} is not a valid port spec"
            ))
        }
    };
    let local_port = local
        .parse::<i32>()
        .ok()
        .and_then(valid_port)
        .ok_or_else(|| anyhow!("port_forward local_port must be in range 1-65535"))?;
    let container_port = container
        .parse::<i32>()
        .ok()
        .and_then(valid_port)
        .ok_or_else(|| anyhow!("port_forward container_port must be in range 1-65535"))?;
    Ok(PortForwardSpec {
        host,
        local_port,
        container_port,
        name: String::new(),
        link_path: String::new(),
    })
}

fn port_forward_link(pf: &PortForwardSpec) -> (String, String) {
    let host = if pf.host == "127.0.0.1" {
        "localhost"
    } else {
        pf.host.as_str()
    };
    let path = if pf.link_path.is_empty() {
        String::new()
    } else if pf.link_path.starts_with('/') {
        pf.link_path.clone()
    } else {
        format!("/{}", pf.link_path)
    };
    let label = if pf.name.is_empty() {
        format!("port {}", pf.local_port)
    } else {
        pf.name.clone()
    };
    (format!("http://{host}:{}{}", pf.local_port, path), label)
}

#[starlark::starlark_module]
fn starling_builtins(builder: &mut GlobalsBuilder) {
    // Trigger-mode constants (match Tilt's Starlark-facing values).
    const TRIGGER_MODE_AUTO: i32 = TRIGGER_MODE_AUTO_VAL;
    const TRIGGER_MODE_MANUAL: i32 = TRIGGER_MODE_MANUAL_VAL;

    /// Compatibility shim for `os.environ.get(...)` in Tiltfiles.
    fn _starling_getenv<'v>(
        name: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        match std::env::var(name) {
            Ok(value) => Ok(eval.heap().alloc(value)),
            Err(_) if default.is_none() => Ok(Value::new_none()),
            Err(_) => Ok(default),
        }
    }

    fn _starling_getcwd() -> anyhow::Result<String> {
        Ok(std::env::current_dir()?.display().to_string())
    }

    fn _starling_putenv(key: String, value: String) -> anyhow::Result<NoneType> {
        std::env::set_var(key, value);
        Ok(NoneType)
    }

    fn _starling_unsetenv(key: String) -> anyhow::Result<NoneType> {
        std::env::remove_var(key);
        Ok(NoneType)
    }

    fn _starling_path_abspath(path: String) -> anyhow::Result<String> {
        let p = PathBuf::from(path);
        Ok(if p.is_absolute() {
            p
        } else {
            std::env::current_dir()?.join(p)
        }
        .display()
        .to_string())
    }

    fn _starling_path_relpath(
        targpath: String,
        #[starlark(default = NoneType)] basepath: Value,
    ) -> anyhow::Result<String> {
        let cwd = std::env::current_dir()?;
        let base = basepath.unpack_str().unwrap_or(".").to_string();
        Ok(posix_relpath(&targpath, &base, &cwd))
    }

    fn _starling_path_basename(path: String) -> anyhow::Result<String> {
        Ok(Path::new(&path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string())
    }

    fn _starling_path_dirname(path: String) -> anyhow::Result<String> {
        Ok(Path::new(&path)
            .parent()
            .map(|p| p.display().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| ".".to_string()))
    }

    fn _starling_path_exists(path: String) -> anyhow::Result<bool> {
        Ok(Path::new(&path).exists())
    }

    fn _starling_path_join<'v>(
        path: String,
        #[starlark(args)] paths: UnpackTuple<String>,
    ) -> anyhow::Result<String> {
        let mut out = PathBuf::from(path);
        for p in paths.items {
            out.push(p);
        }
        Ok(out.display().to_string())
    }

    fn _starling_path_realpath(path: String) -> anyhow::Result<String> {
        Ok(std::fs::canonicalize(path)?.display().to_string())
    }

    fn _starling_sys_executable() -> anyhow::Result<String> {
        Ok(std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_default())
    }

    fn _starling_shlex_quote(s: String) -> anyhow::Result<String> {
        Ok(shlex_quote(&s))
    }

    // -- v1alpha1 typed object constructors --------------------------------
    // Build Kubernetes-style object dicts (apiVersion/kind/metadata/spec) for
    // the common tilt.dev types, mirroring Tilt's `v1alpha1.*` constructors.
    // They return plain dicts the Tiltfile can inspect/pass around.

    fn _starling_v1alpha1_config_map<'v>(
        name: String,
        #[starlark(require = named, default = NoneType)] data: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let data: serde_json::Map<String, serde_json::Value> = parse_build_args(data)
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "ConfigMap",
            "metadata": { "name": name },
            "data": data,
        })))
    }

    fn _starling_v1alpha1_cmd<'v>(
        name: String,
        #[starlark(require = named, default = NoneType)] args: Value<'v>,
        #[starlark(require = named, default = String::new())] dir: String,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "Cmd",
            "metadata": { "name": name },
            "spec": { "args": as_str_vec(args), "dir": dir, "env": as_str_vec(env) },
        })))
    }

    fn _starling_v1alpha1_kubernetes_apply<'v>(
        name: String,
        #[starlark(require = named, default = String::new())] yaml: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "KubernetesApply",
            "metadata": { "name": name },
            "spec": { "yaml": yaml },
        })))
    }

    fn _starling_v1alpha1_kubernetes_discovery<'v>(
        name: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "KubernetesDiscovery",
            "metadata": { "name": name },
            "spec": { "watches": [] },
        })))
    }

    fn _starling_v1alpha1_file_watch<'v>(
        name: String,
        #[starlark(require = named, default = NoneType)] watched_paths: Value<'v>,
        #[starlark(require = named, default = NoneType)] ignores: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "FileWatch",
            "metadata": { "name": name },
            "spec": { "watchedPaths": as_str_vec(watched_paths), "ignores": as_str_vec(ignores) },
        })))
    }

    fn _starling_v1alpha1_ui_button<'v>(
        name: String,
        #[starlark(require = named, default = String::new())] text: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "UIButton",
            "metadata": { "name": name },
            "spec": { "text": text },
        })))
    }

    fn _starling_v1alpha1_extension_repo<'v>(
        name: String,
        #[starlark(require = named)] url: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "ExtensionRepo",
            "metadata": { "name": name },
            "spec": { "url": url },
        })))
    }

    fn _starling_v1alpha1_extension<'v>(
        name: String,
        #[starlark(require = named)] repo_name: String,
        #[starlark(require = named, default = String::new())] repo_path: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(serde_json::json!({
            "apiVersion": "tilt.dev/v1alpha1",
            "kind": "Extension",
            "metadata": { "name": name },
            "spec": { "repoName": repo_name, "repoPath": repo_path },
        })))
    }

    fn _starling_config_main_path(eval: &mut Evaluator) -> anyhow::Result<String> {
        Ok(state(eval)
            .config_files
            .borrow()
            .first()
            .cloned()
            .unwrap_or_else(|| PathBuf::from("Starlingfile"))
            .display()
            .to_string())
    }

    fn _starling_config_main_dir(eval: &mut Evaluator) -> anyhow::Result<String> {
        let main_path = state(eval)
            .config_files
            .borrow()
            .first()
            .cloned()
            .unwrap_or_else(|| PathBuf::from("."));
        Ok(parent_or_dot(&main_path).display().to_string())
    }

    fn _starling_config_define_string(
        name: String,
        #[starlark(require = named, default = false)] args: bool,
        #[starlark(require = named, default = NoneType)] usage: Value,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = usage;
        define_config_setting(state(eval), name, ConfigKind::String, args)?;
        Ok(NoneType)
    }

    fn _starling_config_define_string_list(
        name: String,
        #[starlark(require = named, default = false)] args: bool,
        #[starlark(require = named, default = NoneType)] usage: Value,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = usage;
        define_config_setting(state(eval), name, ConfigKind::StringList, args)?;
        Ok(NoneType)
    }

    fn _starling_config_define_bool(
        name: String,
        #[starlark(require = named, default = false)] args: bool,
        #[starlark(require = named, default = NoneType)] usage: Value,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = usage;
        define_config_setting(state(eval), name, ConfigKind::Bool, args)?;
        Ok(NoneType)
    }

    fn _starling_config_define_object(
        name: String,
        #[starlark(require = named, default = false)] args: bool,
        #[starlark(require = named, default = NoneType)] usage: Value,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = usage;
        define_config_setting(state(eval), name, ConfigKind::Object, args)?;
        Ok(NoneType)
    }

    fn _starling_config_parse<'v>(eval: &mut Evaluator<'v, '_>) -> anyhow::Result<Value<'v>> {
        let parsed = parse_config(state(eval))?;
        Ok(parsed.into_starlark(eval.heap()))
    }

    fn _starling_config_set_enabled_resources<'v>(
        resources: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        *state(eval).enabled_resources.borrow_mut() = Some(as_str_vec(resources));
        *state(eval).disable_all_resources.borrow_mut() = false;
        Ok(NoneType)
    }

    fn _starling_config_clear_enabled_resources(eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        *state(eval).enabled_resources.borrow_mut() = None;
        *state(eval).disable_all_resources.borrow_mut() = true;
        Ok(NoneType)
    }

    /// Return the current Kubernetes context, matching Tilt's `k8s_context()`.
    fn k8s_context(eval: &mut Evaluator) -> anyhow::Result<String> {
        Ok(current_kube_context(state(eval)))
    }

    /// Return the namespace for the current Kubernetes context, matching
    /// Tilt's `k8s_namespace()`. Kubernetes defaults to "default" when unset.
    fn k8s_namespace(eval: &mut Evaluator) -> anyhow::Result<String> {
        Ok(current_kube_namespace(state(eval)))
    }

    /// Build a Tilt-compatible blob value. Starling represents blobs as strings.
    fn blob<'v>(contents: String, eval: &mut Evaluator<'v, '_>) -> anyhow::Result<Value<'v>> {
        Ok(eval.heap().alloc(Blob(contents)))
    }

    /// List entries in a directory, optionally recursively. Paths are relative
    /// to the requested directory and sorted for deterministic Tiltfile output.
    fn listdir<'v>(
        directory: String,
        #[starlark(require = named, default = false)] recursive: bool,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let dir = resolve(&state(eval).cur_dir(), &directory);
        let entries = list_dir_entries(&dir, recursive)?;
        Ok(eval.heap().alloc(AllocList(entries)))
    }

    /// Decode a JSON string/blob into Starlark data.
    fn decode_json<'v>(json: Value<'v>, heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        let text = value_to_text(json)?;
        Ok(heap.alloc(serde_json::from_str::<serde_json::Value>(&text)?))
    }

    /// Encode Starlark data as a JSON string/blob.
    fn encode_json(obj: Value) -> anyhow::Result<String> {
        obj.to_json()
    }

    /// Read and parse JSON from a file relative to the current Starlingfile.
    fn read_json<'v>(
        path: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let Some(text) = read_tracked_text(&path, default, eval)? else {
            return Ok(default);
        };
        Ok(eval
            .heap()
            .alloc(serde_json::from_str::<serde_json::Value>(&text)?))
    }

    /// Decode a YAML document into Starlark data.
    fn decode_yaml<'v>(yaml: Value<'v>, heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        let text = value_to_text(yaml)?;
        let parsed: serde_yaml::Value = serde_yaml::from_str(&text)?;
        Ok(heap.alloc(yaml_to_json_value(parsed)?))
    }

    /// Decode a YAML stream into a list of Starlark data values.
    fn decode_yaml_stream<'v>(yaml: Value<'v>, heap: &'v Heap) -> anyhow::Result<Value<'v>> {
        let text = value_to_text(yaml)?;
        let mut docs = Vec::new();
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let parsed = serde_yaml::Value::deserialize(doc)?;
            if !parsed.is_null() {
                docs.push(yaml_to_json_value(parsed)?);
            }
        }
        Ok(heap.alloc(docs.as_slice()))
    }

    /// Read and parse one YAML document from a file.
    fn read_yaml<'v>(
        path: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let Some(text) = read_tracked_text(&path, default, eval)? else {
            return Ok(default);
        };
        let parsed: serde_yaml::Value = serde_yaml::from_str(&text)?;
        Ok(eval.heap().alloc(yaml_to_json_value(parsed)?))
    }

    /// Read and parse a YAML stream from a file.
    fn read_yaml_stream<'v>(
        path: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let Some(text) = read_tracked_text(&path, default, eval)? else {
            return Ok(default);
        };
        let mut docs = Vec::new();
        for doc in serde_yaml::Deserializer::from_str(&text) {
            let parsed = serde_yaml::Value::deserialize(doc)?;
            if !parsed.is_null() {
                docs.push(yaml_to_json_value(parsed)?);
            }
        }
        Ok(eval.heap().alloc(docs.as_slice()))
    }

    /// Encode Starlark data as a YAML document string/blob.
    fn encode_yaml(obj: Value) -> anyhow::Result<String> {
        Ok(serde_yaml::to_string(&obj.to_json_value()?)?)
    }

    /// Encode a list of Starlark data values as a YAML document stream.
    fn encode_yaml_stream<'v>(objs: Value<'v>) -> anyhow::Result<String> {
        let Some(list) = ListRef::from_value(objs) else {
            return Err(anyhow!("encode_yaml_stream: expected a list"));
        };
        let mut out = String::new();
        for (i, obj) in list.iter().enumerate() {
            if i > 0 {
                out.push_str("---\n");
            }
            out.push_str(&serde_yaml::to_string(&obj.to_json_value()?)?);
        }
        Ok(out)
    }

    fn warn(msg: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        state(eval).logln(&format!("WARNING: {msg}"));
        Ok(NoneType)
    }

    fn fail(msg: String) -> anyhow::Result<NoneType> {
        Err(anyhow!("{msg}"))
    }

    fn exit<'v>(code: Value<'v>) -> anyhow::Result<NoneType> {
        Err(anyhow!("exit({})", value_to_string(code)))
    }

    /// Define a readiness probe (Tilt's `probe(...)`). Takes one of the action
    /// builtins (`exec`/`tcp_socket`/`http_get`) plus timing. Returns a
    /// JSON-encoded [`ReadinessProbe`] that `local_resource(readiness_probe=...)`
    /// parses and executes.
    fn probe<'v>(
        #[starlark(require = named, default = NoneType)] period_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] timeout_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] initial_delay_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] tcp_socket: Value<'v>,
        #[starlark(require = named, default = NoneType)] http_get: Value<'v>,
        #[starlark(require = named, default = NoneType)] exec: Value<'v>,
        #[starlark(require = named, default = NoneType)] success_threshold: Value<'v>,
        #[starlark(require = named, default = NoneType)] failure_threshold: Value<'v>,
    ) -> anyhow::Result<String> {
        // Exactly one action must be supplied (matching Tilt).
        let action_json = [exec, http_get, tcp_socket]
            .into_iter()
            .find_map(|v| v.unpack_str())
            .ok_or_else(|| {
                anyhow!("probe(...) requires one of exec=, http_get=, or tcp_socket=")
            })?;
        let action: ProbeAction = serde_json::from_str(action_json).map_err(|_| {
            anyhow!("probe(...): action must be exec_action(), http_get_action(), or tcp_socket_action()")
        })?;
        let mut probe = ReadinessProbe {
            initial_delay_secs: 0.0,
            period_secs: 1.0,
            timeout_secs: 1.0,
            success_threshold: 1,
            failure_threshold: 1,
            action,
        };
        if let Some(v) = as_f64(initial_delay_secs) {
            probe.initial_delay_secs = v.max(0.0);
        }
        if let Some(v) = as_f64(period_secs) {
            if v > 0.0 {
                probe.period_secs = v;
            }
        }
        if let Some(v) = as_f64(timeout_secs) {
            if v > 0.0 {
                probe.timeout_secs = v;
            }
        }
        if let Some(v) = optional_i32_min("success_threshold", success_threshold, 1)? {
            probe.success_threshold = v;
        }
        if let Some(v) = optional_i32_min("failure_threshold", failure_threshold, 1)? {
            probe.failure_threshold = v;
        }
        Ok(serde_json::to_string(&probe).expect("probe serializes"))
    }

    /// A probe action that runs a command; success = exit status 0 (Tilt's
    /// `exec_action(command)`). Returns a JSON-encoded [`ProbeAction`].
    fn exec_action<'v>(command: Value<'v>) -> anyhow::Result<String> {
        let command = as_str_vec(command);
        if command.is_empty() {
            return Err(anyhow!("exec_action(command): command must be non-empty"));
        }
        Ok(serde_json::to_string(&ProbeAction::Exec { command }).expect("action serializes"))
    }

    /// A probe action that issues an HTTP GET; success = status < 400 (Tilt's
    /// `http_get_action(port, host, scheme, path)`).
    fn http_get_action<'v>(
        port: i32,
        #[starlark(require = named, default = NoneType)] host: Value<'v>,
        #[starlark(require = named, default = NoneType)] scheme: Value<'v>,
        #[starlark(require = named, default = NoneType)] path: Value<'v>,
    ) -> anyhow::Result<String> {
        if port <= 0 || port >= 65536 {
            return Err(anyhow!("http_get_action(port={port}): port out of range"));
        }
        let action = ProbeAction::Http {
            host: host.unpack_str().unwrap_or("127.0.0.1").to_string(),
            port: port as u16,
            scheme: scheme.unpack_str().unwrap_or("http").to_string(),
            path: path.unpack_str().unwrap_or("/").to_string(),
        };
        Ok(serde_json::to_string(&action).expect("action serializes"))
    }

    /// Build a labeled link (Tilt's `link(url, name)`). Encodes `(url, name)` as
    /// a string so it can flow through `links=[...]` lists; consumed by
    /// [`parse_links`]. A missing/empty name falls back to the URL.
    fn link<'v>(
        url: String,
        #[starlark(default = NoneType)] name: Value<'v>,
    ) -> anyhow::Result<String> {
        let name = name.unpack_str().unwrap_or("");
        Ok(format!("{url}{LINK_SEP}{name}"))
    }

    /// A probe action that opens a TCP connection; success = connection
    /// established (Tilt's `tcp_socket_action(port, host)`).
    fn tcp_socket_action<'v>(
        port: i32,
        #[starlark(require = named, default = NoneType)] host: Value<'v>,
    ) -> anyhow::Result<String> {
        if port <= 0 || port >= 65536 {
            return Err(anyhow!("tcp_socket_action(port={port}): port out of range"));
        }
        let action = ProbeAction::Tcp {
            host: host.unpack_str().unwrap_or("127.0.0.1").to_string(),
            port: port as u16,
        };
        Ok(serde_json::to_string(&action).expect("action serializes"))
    }

    /// Define a resource backed by local commands. Argument order matches Tilt:
    /// `local_resource(name, cmd, deps, serve_cmd, ...)`.
    fn local_resource<'v>(
        name: String,
        #[starlark(default = NoneType)] cmd: Value<'v>,
        #[starlark(default = NoneType)] deps: Value<'v>,
        #[starlark(default = NoneType)] serve_cmd: Value<'v>,
        #[starlark(require = named, default = NoneType)] resource_deps: Value<'v>,
        #[starlark(require = named)] trigger_mode: Option<i32>,
        #[starlark(require = named, default = true)] auto_init: bool,
        #[starlark(require = named, default = NoneType)] links: Value<'v>,
        #[starlark(require = named, default = NoneType)] labels: Value<'v>,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        #[starlark(require = named, default = NoneType)] dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_env: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_dir: Value<'v>,
        // Accepted for Tiltfile compatibility (not all affect local execution).
        #[starlark(require = named, default = false)] allow_parallel: bool,
        #[starlark(require = named, default = NoneType)] ignore: Value<'v>,
        #[starlark(require = named, default = NoneType)] cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] readiness_probe: Value<'v>,
        // Starling extension: pin/assign the serve_cmd's proxy port.
        #[starlark(require = named, default = NoneType)] serve_port: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        record_local_manifest(
            state(eval),
            "local_resource",
            name,
            cmd,
            cmd_bat,
            deps,
            serve_cmd,
            serve_cmd_bat,
            resource_deps,
            trigger_mode,
            auto_init,
            links,
            labels,
            env,
            dir,
            serve_env,
            serve_dir,
            ignore,
            readiness_probe,
            serve_port,
            allow_parallel,
            false,
        )?;
        Ok(NoneType)
    }

    /// Deprecated Tilt test resource; equivalent to local_resource with a
    /// deprecation warning and allow_parallel=True by default.
    fn test<'v>(
        name: String,
        #[starlark(default = NoneType)] cmd: Value<'v>,
        #[starlark(default = NoneType)] deps: Value<'v>,
        #[starlark(default = NoneType)] serve_cmd: Value<'v>,
        #[starlark(require = named, default = NoneType)] resource_deps: Value<'v>,
        #[starlark(require = named)] trigger_mode: Option<i32>,
        #[starlark(require = named, default = true)] auto_init: bool,
        #[starlark(require = named, default = NoneType)] links: Value<'v>,
        #[starlark(require = named, default = NoneType)] labels: Value<'v>,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        #[starlark(require = named, default = NoneType)] dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_env: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_dir: Value<'v>,
        #[starlark(require = named, default = true)] allow_parallel: bool,
        #[starlark(require = named, default = NoneType)] ignore: Value<'v>,
        #[starlark(require = named, default = NoneType)] cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] readiness_probe: Value<'v>,
        #[starlark(require = named, default = NoneType)] serve_port: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        st.logln(
            "WARNING: test() is deprecated and will be removed in a future release.\nChange this call to use `local_resource(..., allow_parallel=True)`",
        );
        record_local_manifest(
            st,
            "test",
            name,
            cmd,
            cmd_bat,
            deps,
            serve_cmd,
            serve_cmd_bat,
            resource_deps,
            trigger_mode,
            auto_init,
            links,
            labels,
            env,
            dir,
            serve_env,
            serve_dir,
            ignore,
            readiness_probe,
            serve_port,
            allow_parallel,
            true,
        )?;
        Ok(NoneType)
    }

    /// Run a command during Starlingfile execution and return its stdout.
    fn local<'v>(
        command: Value<'v>,
        #[starlark(require = named, default = false)] quiet: bool,
        #[starlark(require = named, default = NoneType)] command_bat: Value<'v>,
        #[starlark(require = named, default = false)] echo_off: bool,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        #[starlark(require = named, default = NoneType)] dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] stdin: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        use std::io::Write;
        use std::process::Stdio;
        let st = state(eval);
        let base = st.cur_dir();
        let workdir = dir.unpack_str().map(|d| resolve(&base, d)).unwrap_or(base);
        let cmd = as_platform_cmd(command, command_bat, &workdir);
        if cmd.is_empty() {
            return Ok(eval.heap().alloc(Blob(String::new())));
        }
        let display_cmd = if echo_off {
            "<redacted>".to_string()
        } else {
            cmd.display()
        };
        let mut command_obj = Command::new(&cmd.argv[0]);
        command_obj.args(&cmd.argv[1..]).current_dir(&workdir);
        for (k, v) in parse_build_args(env) {
            command_obj.env(k, v);
        }
        let stdin_data = stdin.unpack_str().map(str::to_string);
        if stdin_data.is_some() {
            command_obj.stdin(Stdio::piped());
        }
        command_obj.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = command_obj
            .spawn()
            .with_context(|| format!("local: running {display_cmd}"))?;
        if let (Some(data), Some(mut sin)) = (stdin_data, child.stdin.take()) {
            let _ = sin.write_all(data.as_bytes());
        }
        let out = child
            .wait_with_output()
            .with_context(|| format!("local: running {display_cmd}"))?;
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        if !quiet {
            st.logln(&format!("local: {display_cmd}"));
            for line in stdout.lines() {
                st.logln(line);
            }
        }
        if !out.status.success() {
            return Err(anyhow!(
                "local: command failed ({}): {}",
                out.status,
                String::from_utf8_lossy(&out.stderr)
            ));
        }
        Ok(eval.heap().alloc(Blob(stdout)))
    }

    /// Render a kustomization and return its YAML (Tilt's `kustomize`).
    /// Tries `kustomize build`, falling back to `kubectl kustomize`.
    fn kustomize<'v>(
        paths: String,
        #[starlark(require = named, default = NoneType)] kustomize_bin: Value<'v>,
        #[starlark(require = named, default = NoneType)] flags: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let st = state(eval);
        let dir = st.cur_dir();
        let p = resolve(&dir, &paths);
        st.config_files.borrow_mut().push(p.clone());
        let extra = as_str_vec(flags);
        let bin = kustomize_bin
            .unpack_str()
            .unwrap_or("kustomize")
            .to_string();
        let mut argv = vec![bin, "build".to_string(), p.display().to_string()];
        argv.extend(extra.clone());
        let yaml = match run_capture(&argv, &dir) {
            Ok(s) => s,
            Err(_) => {
                // Fall back to `kubectl kustomize`.
                let mut a2 = vec![
                    "kubectl".to_string(),
                    "kustomize".to_string(),
                    p.display().to_string(),
                ];
                a2.extend(extra);
                run_capture(&a2, &dir)?
            }
        };
        Ok(eval.heap().alloc(Blob(yaml)))
    }

    /// Render a Helm chart and return its YAML (Tilt's `helm`).
    fn helm<'v>(
        paths: String,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] namespace: Value<'v>,
        #[starlark(require = named, default = NoneType)] values: Value<'v>,
        #[starlark(require = named, default = NoneType)] set: Value<'v>,
        #[starlark(require = named, default = NoneType)] kube_version: Value<'v>,
        #[starlark(require = named, default = false)] skip_crds: bool,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let st = state(eval);
        let dir = st.cur_dir();
        let p = resolve(&dir, &paths);
        st.config_files.borrow_mut().push(p.clone());
        let release = name.unpack_str().unwrap_or("chart").to_string();
        let argv = helm_template_args(
            &p,
            &release,
            namespace.unpack_str(),
            as_str_vec(values)
                .into_iter()
                .map(|v| resolve(&dir, &v))
                .collect(),
            as_str_vec(set),
            kube_version.unpack_str(),
            skip_crds,
        );
        let yaml = run_capture(&argv, &dir)?;
        Ok(eval.heap().alloc(Blob(yaml)))
    }

    /// Read a file relative to the Starlingfile, returning `default` if missing.
    /// The path is tracked so edits trigger a reload.
    fn read_file<'v>(
        path: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<Value<'v>> {
        let st = state(eval);
        let p = resolve(&st.cur_dir(), &path);
        st.config_files.borrow_mut().push(p.clone());
        let text = match std::fs::read_to_string(&p) {
            Ok(s) => s,
            Err(_) if !default.is_none() => coerce_str(default).unwrap_or_else(|| default.to_str()),
            Err(e) => return Err(anyhow!("read_file({path:?}): {e}")),
        };
        Ok(eval.heap().alloc(Blob(text)))
    }

    /// Like `include()`, but returns the loaded module's exported symbols as a
    /// dict (Tilt's `load_dynamic`). Side effects (resource registrations) still
    /// run against the current shared Starlingfile state.
    fn load_dynamic<'v>(
        path: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
        let module = run_starlingfile_into(&path, eval)?;
        let mut entries = Vec::new();
        for name in module.names() {
            let value = module
                .get(name.as_str())
                .with_context(|| format!("load_dynamic({path:?}): reading export {name}"))?;
            entries.push((
                eval.heap().alloc(name.as_str()),
                value.owned_value(eval.frozen_heap()),
            ));
        }
        Ok(eval
            .heap()
            .alloc(starlark::values::dict::AllocDict(entries)))
    }

    /// Evaluate another Starlingfile for its side effects (resource
    /// registrations, etc.) — Tilt's `include()`.
    fn include(path: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        run_starlingfile_into(&path, eval)?;
        Ok(NoneType)
    }

    /// Split YAML by selectors, returning `(matching, rest)` as two blobs.
    /// Matches Tilt's `filter_yaml(yaml, labels, name, namespace, kind, api_version)`.
    fn filter_yaml<'v>(
        yaml: Value<'v>,
        #[starlark(require = named, default = NoneType)] labels: Value<'v>,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] namespace: Value<'v>,
        #[starlark(require = named, default = NoneType)] kind: Value<'v>,
        #[starlark(require = named, default = NoneType)] api_version: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<(Value<'v>, Value<'v>)> {
        let content = as_str_vec(yaml).join("\n---\n");
        let want_labels = parse_build_args(labels);
        let (mut matching, mut rest) = (Vec::new(), Vec::new());
        for e in k8s::parse_yaml(&content) {
            let name_ok = match name.unpack_str() {
                Some(n) => selector_matches(&e.name, n)?,
                None => true,
            };
            let ok = kind
                .unpack_str()
                .map_or(true, |k| e.kind.eq_ignore_ascii_case(k))
                && name_ok
                && doc_matches(
                    &e.raw,
                    namespace.unpack_str(),
                    api_version.unpack_str(),
                    &want_labels,
                );
            if ok {
                matching.push(e.raw);
            } else {
                rest.push(e.raw);
            }
        }
        let heap = eval.heap();
        Ok((
            heap.alloc(Blob(matching.join("---\n"))),
            heap.alloc(Blob(rest.join("---\n"))),
        ))
    }

    fn k8s_kind<'v>(
        kind: String,
        #[starlark(require = named, default = NoneType)] image_json_path: Value<'v>,
        #[starlark(require = named, default = NoneType)] api_version: Value<'v>,
        #[starlark(require = named, default = NoneType)] image_object: Value<'v>,
        #[starlark(require = named, default = NoneType)] pod_readiness: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let readiness = if pod_readiness.is_none() {
            None
        } else {
            let mode = pod_readiness.unpack_str().ok_or_else(|| {
                anyhow!(
                    "pod_readiness: got {}, want string",
                    pod_readiness.get_type()
                )
            })?;
            if mode != "ignore" && mode != "wait" {
                return Err(anyhow!(
                    "Invalid value. Allowed: {{ignore, wait}}. Got: {mode}"
                ));
            }
            Some(mode.to_string())
        };
        state(eval)
            .k8s_kind_configs
            .borrow_mut()
            .push(K8sKindConfig {
                kind: kind.clone(),
                api_version: api_version.unpack_str().map(str::to_string),
                pod_readiness: readiness,
            });
        if !image_json_path.is_none() && !image_object.is_none() {
            return Err(anyhow!(
                "Cannot specify both image_json_path and image_object"
            ));
        }
        if !image_object.is_none() {
            let locator = parse_image_object_locator(
                image_object,
                Some(kind.clone()),
                None,
                None,
                api_version.unpack_str().map(str::to_string),
            )?;
            state(eval).k8s_image_locators.borrow_mut().push(locator);
        }
        if !image_json_path.is_none() {
            let locators = parse_image_json_paths(
                as_str_vec(image_json_path),
                Some(kind),
                None,
                None,
                api_version.unpack_str().map(str::to_string),
            )?;
            state(eval).k8s_image_locators.borrow_mut().extend(locators);
        }
        Ok(NoneType)
    }
    fn k8s_image_json_path<'v>(
        paths: Value<'v>,
        #[starlark(require = named, default = NoneType)] kind: Value<'v>,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] namespace: Value<'v>,
        #[starlark(require = named, default = NoneType)] api_version: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let locators = parse_image_json_paths(
            as_str_vec(paths),
            kind.unpack_str().map(str::to_string),
            name.unpack_str().map(str::to_string),
            namespace.unpack_str().map(str::to_string),
            api_version.unpack_str().map(str::to_string),
        )?;
        state(eval).k8s_image_locators.borrow_mut().extend(locators);
        Ok(NoneType)
    }

    /// Register Kubernetes YAML files. Parsed into entities now; assembled into
    /// resources after the Starlingfile finishes.
    fn k8s_yaml<'v>(yaml: Value<'v>, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let dir = st.cur_dir();
        for entry in as_str_vec(yaml) {
            // A multi-line string is inline YAML content (e.g. from kustomize(),
            // helm(), local(), read_file()); a single line is a file path.
            let content = if entry.contains('\n') {
                entry.clone()
            } else {
                let p = resolve(&dir, &entry);
                st.config_files.borrow_mut().push(p.clone());
                std::fs::read_to_string(&p)
                    .with_context(|| format!("k8s_yaml: reading {}", p.display()))?
            };
            let entities = k8s::parse_yaml(&content);
            if entities.is_empty() {
                st.logln("k8s_yaml: no entities found in input");
            }
            // Dedup by object identity (name:kind:namespace:group): a duplicate
            // entity from overlapping includes/lists is dropped, matching Tilt's
            // entity dedup rather than applying the same object twice.
            let mut registered = st.k8s_entities.borrow_mut();
            for e in entities {
                let id = entity_object_id_string(&e);
                if registered.iter().any(|x| entity_object_id_string(x) == id) {
                    st.logln(&format!("k8s_yaml: skipping duplicate entity {id}"));
                    continue;
                }
                registered.push(e);
            }
        }
        Ok(NoneType)
    }

    /// Register a Kubernetes resource deployed by custom commands.
    fn k8s_custom_deploy<'v>(
        name: String,
        apply_cmd: Value<'v>,
        delete_cmd: Value<'v>,
        deps: Value<'v>,
        #[starlark(require = named, default = NoneType)] image_selector: Value<'v>,
        #[starlark(require = named, default = NoneType)] live_update: Value<'v>,
        #[starlark(require = named, default = NoneType)] apply_dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] apply_env: Value<'v>,
        #[starlark(require = named, default = NoneType)] apply_cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] delete_dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] delete_env: Value<'v>,
        #[starlark(require = named, default = NoneType)] delete_cmd_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] container_selector: Value<'v>,
        #[starlark(require = named, default = NoneType)] image_deps: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let base = st.cur_dir();
        let apply_workdir = apply_dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        let delete_workdir = delete_dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        let mut apply = as_platform_cmd(apply_cmd, apply_cmd_bat, &apply_workdir);
        apply.env = parse_build_args(apply_env);
        if apply.is_empty() {
            return Err(anyhow!("k8s_custom_deploy: apply_cmd cannot be empty"));
        }
        let mut delete = as_platform_cmd(delete_cmd, delete_cmd_bat, &delete_workdir);
        delete.env = parse_build_args(delete_env);
        if delete.is_empty() {
            return Err(anyhow!("k8s_custom_deploy: delete_cmd cannot be empty"));
        }
        let lu = if live_update.is_none() {
            vec![]
        } else {
            parse_live_update(live_update, &base)?
        };
        let selector_count =
            usize::from(image_selector.unpack_str().is_some_and(|s| !s.is_empty()))
                + usize::from(
                    container_selector
                        .unpack_str()
                        .is_some_and(|s| !s.is_empty()),
                );
        if !lu.is_empty() && selector_count == 0 {
            return Err(anyhow!(
                "k8s_custom_deploy: no Live Update selector specified"
            ));
        }
        if selector_count > 1 {
            return Err(anyhow!(
                "k8s_custom_deploy: cannot specify more than one Live Update selector"
            ));
        }
        let mut m = Manifest::new(name.clone(), TargetKind::Kubernetes);
        m.k8s_custom_apply_cmd = Some(apply);
        m.k8s_custom_delete_cmd = Some(delete);
        m.deps = as_str_vec(deps)
            .into_iter()
            .map(|dep| resolve(&base, &dep))
            .collect();
        m.live_update = lu;
        m.k8s_custom_image_deps = as_str_vec(image_deps);
        st.k8s_custom_manifests.borrow_mut().push(m);
        Ok(NoneType)
    }

    /// Register a docker image build (optionally with live_update steps).
    fn docker_build<'v>(
        r#ref: String,
        context: String,
        #[starlark(require = named, default = NoneType)] build_args: Value<'v>,
        #[starlark(require = named, default = NoneType)] dockerfile: Value<'v>,
        #[starlark(require = named, default = NoneType)] dockerfile_contents: Value<'v>,
        #[starlark(require = named, default = NoneType)] live_update: Value<'v>,
        #[starlark(require = named, default = NoneType)] target: Value<'v>,
        #[starlark(require = named, default = NoneType)] platform: Value<'v>,
        #[starlark(require = named, default = NoneType)] ignore: Value<'v>,
        #[starlark(require = named, default = NoneType)] only: Value<'v>,
        #[starlark(require = named, default = NoneType)] cache: Value<'v>,
        #[starlark(require = named, default = NoneType)] cache_from: Value<'v>,
        #[starlark(require = named, default = NoneType)] ssh: Value<'v>,
        #[starlark(require = named, default = NoneType)] secret: Value<'v>,
        #[starlark(require = named, default = false)] pull: bool,
        #[starlark(require = named, default = NoneType)] network: Value<'v>,
        #[starlark(require = named, default = NoneType)] extra_hosts: Value<'v>,
        #[starlark(require = named, default = NoneType)] extra_tag: Value<'v>,
        #[starlark(require = named, default = NoneType)] entrypoint: Value<'v>,
        #[starlark(require = named, default = NoneType)] container_args: Value<'v>,
        #[starlark(require = named, default = false)] match_in_env_vars: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let dir = st.cur_dir();
        let context_path = resolve(&dir, &context);
        let df = dockerfile.unpack_str().map(|d| resolve(&dir, d));
        let inline_dockerfile = if dockerfile_contents.is_none() {
            None
        } else {
            Some(value_to_text(dockerfile_contents)?)
        };
        let lu = if live_update.is_none() {
            vec![]
        } else {
            parse_live_update(live_update, &dir)?
        };
        let build_args = parse_build_args(build_args);
        if !cache.is_none() {
            st.logln("WARNING: docker_build(cache=...) is obsolete; use cache_from=... instead");
        }
        st.docker_builds.borrow_mut().push(DockerBuild {
            image_ref: r#ref,
            context: context_path.clone(),
            dockerfile: df,
            dockerfile_contents: inline_dockerfile,
            target: target.unpack_str().map(str::to_string),
            platform: platform.unpack_str().map(str::to_string),
            extra_tags: parse_extra_tags(extra_tag)?,
            entrypoint: if entrypoint.is_none() {
                vec![]
            } else {
                as_cmd(entrypoint, &dir).argv
            },
            container_args: if container_args.is_none() {
                None
            } else {
                Some(as_str_vec(container_args))
            },
            match_in_env_vars,
            build_args,
            cache_from: as_str_vec(cache_from),
            ssh: as_str_vec(ssh),
            secrets: as_str_vec(secret),
            pull,
            network: network.unpack_str().map(str::to_string),
            extra_hosts: as_str_vec(extra_hosts),
            ignore_rules: ignore_rules_from_value(&context_path, ignore),
            only: as_str_vec(only).into_iter().map(PathBuf::from).collect(),
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: lu,
        });
        Ok(NoneType)
    }

    /// Build an image with an arbitrary command (Tilt's `custom_build`). The
    /// command must build + tag the image; `EXPECTED_REF` is set in its env.
    fn custom_build<'v>(
        r#ref: String,
        command: Value<'v>,
        deps: Value<'v>,
        #[starlark(require = named, default = NoneType)] command_bat: Value<'v>,
        #[starlark(require = named, default = NoneType)] live_update: Value<'v>,
        #[starlark(require = named, default = NoneType)] dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        #[starlark(require = named, default = NoneType)] entrypoint: Value<'v>,
        #[starlark(require = named, default = NoneType)] container_args: Value<'v>,
        #[starlark(require = named, default = false)] match_in_env_vars: bool,
        // Accepted for compatibility.
        #[starlark(require = named, default = NoneType)] tag: Value<'v>,
        #[starlark(require = named, default = NoneType)] outputs_image_ref_to: Value<'v>,
        #[starlark(require = named, default = NoneType)] image_deps: Value<'v>,
        #[starlark(require = named, default = false)] disable_push: bool,
        #[starlark(require = named, default = false)] skips_local_docker: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let base = st.cur_dir();
        let workdir = dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        let mut cmd = as_platform_cmd(command, command_bat, &workdir);
        cmd.env = parse_build_args(env);
        let dep_paths = as_str_vec(deps)
            .into_iter()
            .map(|d| resolve(&base, &d))
            .collect();
        let lu = if live_update.is_none() {
            vec![]
        } else {
            parse_live_update(live_update, &base)?
        };
        let entrypoint = if entrypoint.is_none() {
            vec![]
        } else {
            as_cmd(entrypoint, &base).argv
        };
        let container_args = if container_args.is_none() {
            None
        } else {
            Some(as_str_vec(container_args))
        };
        let output_path = outputs_image_ref_to
            .unpack_str()
            .map(|path| resolve(&base, path));
        if tag.unpack_str().is_some() && output_path.is_some() {
            return Err(anyhow!(
                "Cannot specify both tag= and outputs_image_ref_to="
            ));
        }
        st.docker_builds.borrow_mut().push(DockerBuild {
            image_ref: r#ref,
            context: base,
            dockerfile: None,
            dockerfile_contents: None,
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint,
            container_args,
            match_in_env_vars,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![],
            only: vec![],
            command: Some(cmd),
            custom_tag: tag.unpack_str().map(str::to_string),
            outputs_image_ref_to: output_path,
            image_deps: as_str_vec(image_deps),
            disable_push,
            skips_local_docker,
            deps: dep_paths,
            live_update: lu,
        });
        Ok(NoneType)
    }

    /// A live_update sync step: copy `local` into the container at `remote`.
    fn sync(local: String, remote: String) -> anyhow::Result<String> {
        Ok(format!("sync{LU_SEP}{local}{LU_SEP}{remote}"))
    }

    /// A live_update run step: run `cmd` inside the container after syncing.
    fn run<'v>(
        cmd: String,
        #[starlark(require = named, default = NoneType)] trigger: Value<'v>,
        #[starlark(require = named, default = false)] echo_off: bool,
    ) -> anyhow::Result<String> {
        let triggers = if trigger.is_none() {
            vec![]
        } else {
            as_str_vec(trigger)
        };
        if !trigger.is_none() && triggers.is_empty() {
            return Err(anyhow!(
                "run(trigger=...): expected a string or list of strings"
            ));
        }
        let mut encoded = format!(
            "run{LU_SEP}{cmd}{LU_SEP}{}",
            if echo_off { "1" } else { "0" }
        );
        if !triggers.is_empty() {
            encoded.push(LU_SEP);
            encoded.push_str(&triggers.join(&LU_SEP.to_string()));
        }
        Ok(encoded)
    }

    /// A live_update step: changes to `paths` force a full rebuild.
    fn fall_back_on<'v>(paths: Value<'v>) -> anyhow::Result<String> {
        let joined = as_str_vec(paths).join(&LU_SEP.to_string());
        Ok(format!("fallback{LU_SEP}{joined}"))
    }

    /// A live_update step: restart the container after syncing.
    fn restart_container() -> anyhow::Result<String> {
        Ok("restart".to_string())
    }

    /// A live_update step: sync the synced files once at container startup.
    fn initial_sync() -> anyhow::Result<String> {
        Ok("initialsync".to_string())
    }

    /// Configure a k8s resource (recorded; applied during assembly). Argument
    /// order matches Tilt: `k8s_resource(workload, new_name, port_forwards, ...)`.
    fn k8s_resource<'v>(
        #[starlark(default = NoneType)] workload: Value<'v>,
        #[starlark(default = NoneType)] new_name: Value<'v>,
        #[starlark(default = NoneType)] port_forwards: Value<'v>,
        #[starlark(require = named, default = NoneType)] resource_deps: Value<'v>,
        #[starlark(require = named, default = NoneType)] links: Value<'v>,
        #[starlark(require = named)] trigger_mode: Option<i32>,
        #[starlark(require = named, default = true)] auto_init: bool,
        #[starlark(require = named, default = NoneType)] labels: Value<'v>,
        #[starlark(require = named, default = NoneType)] objects: Value<'v>,
        #[starlark(require = named, default = NoneType)] extra_pod_selectors: Value<'v>,
        #[starlark(require = named, default = NoneType)] pod_readiness: Value<'v>,
        #[starlark(require = named, default = NoneType)] discovery_strategy: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let mut cfg = K8sResourceConfig {
            workload: workload.unpack_str().unwrap_or("").to_string(),
            new_name: new_name.unpack_str().map(str::to_string),
            trigger_mode,
            auto_init,
            ..Default::default()
        };
        if !port_forwards.is_none() {
            cfg.port_forwards = parse_port_forward_specs(port_forwards)?;
        }
        if !links.is_none() {
            cfg.links = parse_links(links);
        }
        if !resource_deps.is_none() {
            cfg.resource_deps = as_str_vec(resource_deps);
        }
        if !labels.is_none() {
            cfg.labels = parse_resource_labels(labels);
        }
        if !extra_pod_selectors.is_none() {
            cfg.extra_pod_selectors = parse_build_args(extra_pod_selectors);
        }
        if !pod_readiness.is_none() {
            let mode = pod_readiness.unpack_str().ok_or_else(|| {
                anyhow!(
                    "pod_readiness: got {}, want string",
                    pod_readiness.get_type()
                )
            })?;
            if mode != "ignore" && mode != "wait" {
                return Err(anyhow!(
                    "Invalid value. Allowed: {{ignore, wait}}. Got: {mode}"
                ));
            }
            cfg.pod_readiness = Some(mode.to_string());
        }
        if !objects.is_none() {
            cfg.objects = as_str_vec(objects);
            // Validate selector syntax eagerly (matches Tilt's parse-time error).
            for obj in &cfg.objects {
                parse_object_selector(obj)?;
            }
        }
        if !discovery_strategy.is_none() {
            let strategy = discovery_strategy.unpack_str().ok_or_else(|| {
                anyhow!(
                    "discovery_strategy: got {}, want string",
                    discovery_strategy.get_type()
                )
            })?;
            if strategy != "" && strategy != "default" && strategy != "selectors-only" {
                return Err(anyhow!(
                    "Invalid value. Allowed: {{, default, selectors-only}}. Got: {strategy}"
                ));
            }
        }
        st.k8s_configs.borrow_mut().push(cfg);
        Ok(NoneType)
    }

    /// Register a function that names Kubernetes resources (Tilt's
    /// `workload_to_resource_function`). The function must take a single
    /// `K8sObjectID`-style argument (a struct with `name`/`kind`/`namespace`/
    /// `group` fields) and return a string. It is stashed on the module and
    /// invoked once per workload after the Starlingfile finishes evaluating.
    fn workload_to_resource_function<'v>(
        func: Value<'v>,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<NoneType> {
        if let Some(spec) = func.parameters_spec() {
            let n = spec.len();
            if n != 1 {
                let sig = spec.signature();
                let name = sig.split('(').next().unwrap_or(&sig);
                return Err(anyhow!(
                    "workload_to_resource_function arg must take 1 argument. {name} takes {n}"
                ));
            }
        }
        eval.module().set_extra_value(func);
        Ok(NoneType)
    }

    /// Build a port-forward spec (string form consumed by `k8s_resource`).
    /// Matches Tilt's `port_forward(local_port, container_port, name, link_path, host)`.
    fn port_forward<'v>(
        local_port: i32,
        #[starlark(default = NoneType)] container_port: Value<'v>,
        #[starlark(require = named, default = NoneType)] host: Value<'v>,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] link_path: Value<'v>,
    ) -> anyhow::Result<String> {
        let container = container_port.unpack_i32().unwrap_or(local_port);
        let host = host.unpack_str().unwrap_or("");
        let name = name.unpack_str().unwrap_or("");
        let link_path = link_path.unpack_str().unwrap_or("");
        Ok(format!(
            "pf{PORT_FORWARD_SEP}{host}{PORT_FORWARD_SEP}{local_port}{PORT_FORWARD_SEP}{container}{PORT_FORWARD_SEP}{name}{PORT_FORWARD_SEP}{link_path}"
        ))
    }

    /// Configure a Docker Compose service resource. The service itself is
    /// loaded by `docker_compose(...)`; this records Tilt-style customizations
    /// and applies them after evaluation so call order does not matter.
    fn dc_resource<'v>(
        name: String,
        #[starlark(default = String::new())] image: String,
        #[starlark(default = String::new())] new_name: String,
        #[starlark(require = named)] trigger_mode: Option<i32>,
        #[starlark(require = named, default = NoneType)] resource_deps: Value<'v>,
        #[starlark(require = named, default = NoneType)] links: Value<'v>,
        #[starlark(require = named, default = NoneType)] labels: Value<'v>,
        #[starlark(require = named, default = String::new())] project_name: String,
        #[starlark(require = named, default = true)] auto_init: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let project_name = if project_name.is_empty() {
            None
        } else {
            Some(project_name)
        };
        if st
            .dc_configs
            .borrow()
            .iter()
            .any(|cfg| cfg.name == name && cfg.project_name == project_name)
        {
            return Err(anyhow!("dc_resource named {name:?} already exists"));
        }
        let mut cfg = DcResourceConfig {
            name,
            image: if image.is_empty() { None } else { Some(image) },
            new_name: if new_name.is_empty() {
                None
            } else {
                Some(new_name)
            },
            trigger_mode,
            auto_init,
            project_name,
            ..Default::default()
        };
        if !resource_deps.is_none() {
            cfg.resource_deps = as_str_vec(resource_deps);
        }
        if !links.is_none() {
            cfg.links = parse_links(links);
        }
        if !labels.is_none() {
            cfg.labels = parse_resource_labels(labels);
        }
        st.dc_configs.borrow_mut().push(cfg);
        Ok(NoneType)
    }

    /// Run a Docker Compose project: each service becomes a resource whose
    /// serve_cmd is `docker compose up <service>` (builds, starts, streams logs).
    fn docker_compose<'v>(
        config_paths: Value<'v>,
        #[starlark(require = named, default = NoneType)] env_file: Value<'v>,
        #[starlark(require = named, default = String::new())] project_name: String,
        #[starlark(require = named, default = NoneType)] profiles: Value<'v>,
        #[starlark(require = named, default = false)] wait: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let dir = st.cur_dir();
        let inputs = compose_config_inputs(config_paths, st, &dir)?;
        let mut services = BTreeMap::<String, ()>::new();
        for input in &inputs {
            let doc: serde_yaml::Value = serde_yaml::from_str(&input.content)
                .with_context(|| format!("docker_compose: parsing {}", input.path.display()))?;
            if let Some(mapping) = doc.get("services").and_then(|s| s.as_mapping()) {
                for (name, _) in mapping {
                    if let Some(svc) = name.as_str() {
                        services.insert(svc.to_string(), ());
                    }
                }
            }
        }
        if services.is_empty() {
            return Err(anyhow!("docker_compose: no services found"));
        }

        let env_file = if env_file.is_none() {
            let default_env = dir.join(".env");
            if default_env.exists() {
                st.config_files.borrow_mut().push(default_env);
            }
            None
        } else {
            let Some(path) = env_file.unpack_str() else {
                return Err(anyhow!(
                    "docker_compose: env_file got {}, want string",
                    env_file.get_type()
                ));
            };
            let path = resolve(&dir, path);
            st.config_files.borrow_mut().push(path.clone());
            Some(path)
        };
        let profiles = if profiles.is_none() {
            vec![]
        } else {
            as_str_vec(profiles)
        };
        let project_name = if project_name.is_empty() {
            inputs
                .iter()
                .find_map(|input| input.project_name.clone())
                .or_else(|| {
                    let first = &inputs[0].path;
                    parent_or_dot(first)
                        .file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .or_else(|| {
                    dir.file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
        } else {
            Some(project_name)
        };
        for svc in services.keys() {
            let mut m = Manifest::new(svc, TargetKind::DockerCompose);
            m.docker_compose_project = project_name.clone();
            let mut argv = vec!["docker".into(), "compose".into()];
            if let Some(project) = &project_name {
                argv.extend(["--project-name".into(), project.clone()]);
            }
            if let Some(path) = &env_file {
                argv.extend(["--env-file".into(), path.display().to_string()]);
            }
            for profile in &profiles {
                argv.extend(["--profile".into(), profile.clone()]);
            }
            for input in &inputs {
                argv.extend(["-f".into(), input.path.display().to_string()]);
            }
            argv.push("up".into());
            if wait {
                argv.push("--wait".into());
            }
            argv.push(svc.to_string());
            m.serve_cmd = Cmd {
                argv,
                workdir: Some(dir.clone()),
                env: vec![],
            };
            st.local_manifests.borrow_mut().push(m);
        }
        Ok(NoneType)
    }

    /// Register a static proxy route `<name>.<tld>` → `127.0.0.1:<port>`.
    /// Equivalent to `portless alias <name> <port>` — useful for pointing at an
    /// already-running server, a Docker container, or a k8s port-forward.
    fn alias(name: String, port: i32, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        if port <= 0 || port >= 65536 {
            return Err(anyhow!("alias({name:?}, {port}): port out of range"));
        }
        let st = state(eval);
        st.aliases.borrow_mut().push((name, port as u16));
        Ok(NoneType)
    }

    /// Request a centrally leased host TCP port and return a shell expansion
    /// for the env var Starling injects at command runtime.
    fn starling_port<'v>(
        name: String,
        #[starlark(require = named, default = NoneType)] preferred: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<String> {
        let preferred = if preferred.is_none() {
            None
        } else {
            let Some(port) = preferred.unpack_i32() else {
                return Err(anyhow!(
                    "starling_port({name:?}): preferred must be an integer"
                ));
            };
            if port <= 0 || port >= 65536 {
                return Err(anyhow!(
                    "starling_port({name:?}): preferred port out of range"
                ));
            }
            Some(port as u16)
        };
        let st = state(eval);
        st.port_leases.borrow_mut().push(NamedPortLease {
            name: name.clone(),
            preferred,
        });
        Ok(format!("${{{}}}", service_port_env_name(&name)))
    }

    fn default_registry<'v>(
        host: String,
        #[starlark(require = named, default = NoneType)] host_from_cluster: Value<'v>,
        #[starlark(require = named, default = NoneType)] single_name: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let mut default_registry = st.default_registry.borrow_mut();
        if default_registry.is_some() {
            return Err(anyhow!("default_registry is already defined"));
        }
        *default_registry = Some(DefaultRegistry {
            host: host.trim_end_matches('/').to_string(),
            host_from_cluster: host_from_cluster
                .unpack_str()
                .map(|s| s.trim_end_matches('/').to_string())
                .filter(|s| !s.is_empty()),
            single_name: single_name
                .unpack_str()
                .map(|s| s.trim_matches('/').to_string())
                .filter(|s| !s.is_empty()),
        });
        Ok(NoneType)
    }

    fn allow_k8s_contexts<'v>(
        contexts: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let contexts = as_str_vec(contexts);
        if contexts.is_empty() {
            return Err(anyhow!("allow_k8s_contexts: expected at least one context"));
        }
        *state(eval).allowed_kube_contexts.borrow_mut() = Some(contexts);
        Ok(NoneType)
    }

    fn enable_feature(feature_name: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        state(eval)
            .feature_flags
            .borrow_mut()
            .insert(feature_name, true);
        Ok(NoneType)
    }

    fn disable_feature(feature_name: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        state(eval)
            .feature_flags
            .borrow_mut()
            .insert(feature_name, false);
        Ok(NoneType)
    }

    /// Register an extension repository for `ext://` loads. Only local repos
    /// (`file://...` or a filesystem path) are supported; the URL is resolved
    /// relative to the current file's directory.
    fn extension_repo(
        #[starlark(require = named)] name: String,
        #[starlark(require = named)] url: String,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let path = if let Some(rest) = url.strip_prefix("file://") {
            // Local dir used in place (live local extension development).
            resolve(&st.cur_dir(), rest)
        } else {
            // Anything else (https://, git@…, git://, or a git repo path) is
            // cloned into a local cache via git, then used like a local repo.
            clone_extension_repo(&name, &url)?
        };
        st.extension_repos
            .borrow_mut()
            .insert(name, path.to_string_lossy().to_string());
        Ok(NoneType)
    }

    fn disable_snapshots(eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        state(eval).logln("WARNING: disable_snapshots() is accepted but snapshots are not implemented by Starling");
        Ok(NoneType)
    }

    fn set_team(team_id: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        if team_id.is_empty() {
            return Err(anyhow!("team_id cannot be empty"));
        }
        let st = state(eval);
        let mut existing = st.team_id.borrow_mut();
        if let Some(prev) = existing.as_ref() {
            return Err(anyhow!(
                "team_id set multiple times (to '{prev}' and '{team_id}')"
            ));
        }
        *existing = Some(team_id);
        Ok(NoneType)
    }

    fn docker_prune_settings<'v>(
        #[starlark(require = named, default = false)] disable: bool,
        #[starlark(require = named, default = NoneType)] max_age_mins: Value<'v>,
        #[starlark(require = named, default = NoneType)] num_builds: Value<'v>,
        #[starlark(require = named, default = NoneType)] interval_hrs: Value<'v>,
        #[starlark(require = named, default = NoneType)] keep_recent: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = disable;
        let _ = optional_i32_min("docker_prune_settings: max_age_mins", max_age_mins, 0)?;
        let _ = optional_i32_min("docker_prune_settings: num_builds", num_builds, 0)?;
        let _ = optional_i32_min("docker_prune_settings: interval_hrs", interval_hrs, 0)?;
        let _ = optional_i32_min("docker_prune_settings: keep_recent", keep_recent, 0)?;
        state(eval).logln("WARNING: docker_prune_settings() is accepted but Docker prune scheduling is not implemented by Starling");
        Ok(NoneType)
    }

    fn analytics_settings(enable: bool, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        let _ = enable;
        state(eval).logln(
            "WARNING: analytics_settings() is accepted but analytics are not implemented by Starling",
        );
        Ok(NoneType)
    }

    fn experimental_analytics_report<'v>(
        tags: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = tags.to_json_value()?;
        state(eval).logln("WARNING: experimental_analytics_report() is accepted but analytics are not implemented by Starling");
        Ok(NoneType)
    }

    fn version_settings(
        #[starlark(require = named, default = true)] check_updates: bool,
        #[starlark(require = named, default = String::new())] constraint: String,
    ) -> anyhow::Result<NoneType> {
        // `check_updates` is a no-op (Starling has no update checker), but a
        // `constraint` is enforced against the running version, like Tilt.
        let _ = check_updates;
        if !constraint.trim().is_empty() {
            check_version_constraint(&constraint, env!("CARGO_PKG_VERSION"))?;
        }
        Ok(NoneType)
    }

    fn secret_settings(
        #[starlark(require = named, default = false)] disable_scrub: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        *state(eval).disable_scrub.borrow_mut() = disable_scrub;
        Ok(NoneType)
    }

    fn update_settings<'v>(
        #[starlark(require = named, default = NoneType)] max_parallel_updates: Value<'v>,
        #[starlark(require = named, default = NoneType)] k8s_upsert_timeout_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] suppress_unused_image_warnings: Value<'v>,
        #[starlark(require = named, default = "auto".to_string())] k8s_server_side_apply: String,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        if let Some(n) = optional_i32_min(
            "update_settings: max_parallel_updates",
            max_parallel_updates,
            1,
        )? {
            *state(eval).max_parallel_updates.borrow_mut() = Some(n as usize);
        }
        let _ = optional_i32_min(
            "update_settings: k8s_upsert_timeout_secs",
            k8s_upsert_timeout_secs,
            1,
        )?;
        if !suppress_unused_image_warnings.is_none() {
            let _ = as_str_vec(suppress_unused_image_warnings);
        }
        if !matches!(k8s_server_side_apply.as_str(), "auto" | "true" | "false") {
            return Err(anyhow!(
                "update_settings: k8s_server_side_apply must be \"true\", \"false\", or \"auto\"; got {k8s_server_side_apply:?}"
            ));
        }
        state(eval).logln(
            "WARNING: update_settings() is accepted but update scheduler settings are not implemented by Starling",
        );
        Ok(NoneType)
    }

    fn ci_settings(
        #[starlark(require = named, default = String::new())] k8s_grace_period: String,
        #[starlark(require = named, default = "30m".to_string())] timeout: String,
        #[starlark(require = named, default = "5m".to_string())] readiness_timeout: String,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = (k8s_grace_period, readiness_timeout);
        let st = state(eval);
        match crate::ci::parse_duration_secs(&timeout) {
            Some(secs) => *st.ci_timeout_secs.borrow_mut() = Some(secs),
            None => st.logln(&format!(
                "WARNING: ci_settings(timeout={timeout:?}) is not a valid duration; ignoring"
            )),
        }
        Ok(NoneType)
    }

    fn watch_settings<'v>(
        #[starlark(require = named, default = NoneType)] ignore: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let rules = ignore_rules_from_value(&st.cur_dir(), ignore);
        if !rules.is_empty() {
            st.watch_ignores.borrow_mut().extend(rules);
        }
        Ok(NoneType)
    }

    /// Explicitly add a file to the reload watch set.
    fn watch_file(path: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let p = resolve(&st.cur_dir(), &path);
        st.config_files.borrow_mut().push(p);
        Ok(NoneType)
    }
}

/// Check a single YAML doc against optional namespace/apiVersion/label filters.
fn doc_matches(
    raw: &str,
    namespace: Option<&str>,
    api_version: Option<&str>,
    labels: &[(String, String)],
) -> bool {
    if namespace.is_none() && api_version.is_none() && labels.is_empty() {
        return true;
    }
    let Ok(v) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
        return false;
    };
    if let Some(ns) = namespace {
        if v.get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(|x| x.as_str())
            != Some(ns)
        {
            return false;
        }
    }
    if let Some(av) = api_version {
        if v.get("apiVersion").and_then(|x| x.as_str()) != Some(av) {
            return false;
        }
    }
    if !labels.is_empty() {
        let doc_labels = v.get("metadata").and_then(|m| m.get("labels"));
        for (k, want) in labels {
            let got = doc_labels
                .and_then(|l| l.get(k.as_str()))
                .and_then(|x| x.as_str());
            if got != Some(want.as_str()) {
                return false;
            }
        }
    }
    true
}

fn selector_matches(value: &str, selector: &str) -> Result<bool> {
    const REGEX_META: &[char] = &[
        '^', '$', '.', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|',
    ];
    if selector.chars().any(|ch| REGEX_META.contains(&ch)) {
        return Ok(regex::Regex::new(selector)
            .with_context(|| format!("invalid selector regex {selector:?}"))?
            .is_match(value));
    }
    Ok(value == selector)
}

fn parse_image_json_paths(
    paths: Vec<String>,
    kind: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    api_version: Option<String>,
) -> Result<Vec<K8sImageLocator>> {
    if paths.is_empty() {
        return Err(anyhow!("k8s_image_json_path: paths must not be empty"));
    }
    paths
        .into_iter()
        .map(|path| {
            Ok(K8sImageLocator {
                kind: kind.clone(),
                name: name.clone(),
                namespace: namespace.clone(),
                api_version: api_version.clone(),
                path: parse_simple_json_path(&path)?,
                object: None,
            })
        })
        .collect()
}

fn parse_image_object_locator(
    value: Value,
    kind: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    api_version: Option<String>,
) -> Result<K8sImageLocator> {
    use starlark::values::dict::DictRef;

    let dict = DictRef::from_value(value)
        .ok_or_else(|| anyhow!("k8s_kind(image_object=...): expected a dict"))?;
    let get = |key: &str| -> Result<String> {
        dict.iter()
            .find_map(|(k, v)| (k.unpack_str() == Some(key)).then_some(v))
            .and_then(|v| v.unpack_str().map(str::to_string))
            .ok_or_else(|| anyhow!("k8s_kind(image_object=...): missing string field {key:?}"))
    };
    Ok(K8sImageLocator {
        kind,
        name,
        namespace,
        api_version,
        path: parse_simple_json_path(&get("json_path")?)?,
        object: Some(K8sImageObjectLocator {
            repo_field: get("repo_field")?,
            tag_field: get("tag_field")?,
        }),
    })
}

/// A `k8s_resource(objects=...)` selector. Mirrors Tilt's `name[:kind[:namespace]]`
/// string form: 1-3 colon-separated parts matched case-insensitively and
/// exactly, where an empty/absent part matches anything.
#[derive(Debug, Clone)]
struct ObjectSelector {
    name: Option<String>,
    kind: Option<String>,
    namespace: Option<String>,
}

impl ObjectSelector {
    fn matches(&self, e: &K8sEntity) -> bool {
        let eq = |sel: &Option<String>, actual: &str| {
            sel.as_ref()
                .map_or(true, |s| s.eq_ignore_ascii_case(actual))
        };
        eq(&self.name, &e.name) && eq(&self.kind, &e.kind) && eq(&self.namespace, &e.namespace)
    }
}

/// Parse a Tilt-style object selector string. Returns an error for >3 parts,
/// matching Tilt's `SelectorFromString`.
fn parse_object_selector(s: &str) -> Result<ObjectSelector> {
    let nonempty = |p: &str| (!p.is_empty()).then(|| p.to_string());
    let parts: Vec<&str> = s.split(':').collect();
    match parts.as_slice() {
        [name] => Ok(ObjectSelector {
            name: nonempty(name),
            kind: None,
            namespace: None,
        }),
        [name, kind] => Ok(ObjectSelector {
            name: nonempty(name),
            kind: nonempty(kind),
            namespace: None,
        }),
        [name, kind, namespace] => Ok(ObjectSelector {
            name: nonempty(name),
            kind: nonempty(kind),
            namespace: nonempty(namespace),
        }),
        _ => Err(anyhow!(
            "Too many parts in selector. Selectors must contain between 1 and 3 parts (colon separated), found {} parts in {s}",
            parts.len()
        )),
    }
}

fn parse_simple_json_path(path: &str) -> Result<Vec<String>> {
    let trimmed = path.trim();
    let inner = trimmed
        .strip_prefix("{.")
        .and_then(|s| s.strip_suffix('}'))
        .or_else(|| trimmed.strip_prefix('.'))
        .ok_or_else(|| anyhow!("unsupported image JSONPath {path:?}; expected {{.a.b}}"))?;
    let parts: Vec<String> = inner
        .split('.')
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect();
    if parts.is_empty() || parts.iter().any(|part| part.contains(['[', ']', '*', '?'])) {
        return Err(anyhow!(
            "unsupported image JSONPath {path:?}; only simple field paths are supported"
        ));
    }
    Ok(parts)
}

fn entity_matching_image_locators(
    entity: &k8s::K8sEntity,
    locators: &[K8sImageLocator],
) -> Vec<K8sImageLocator> {
    locators
        .iter()
        .filter(|locator| locator_matches_entity(locator, entity))
        .cloned()
        .collect()
}

fn entity_matching_kind_configs(
    entity: &k8s::K8sEntity,
    configs: &[K8sKindConfig],
) -> Vec<K8sKindConfig> {
    configs
        .iter()
        .filter(|cfg| kind_config_matches_entity(cfg, entity))
        .cloned()
        .collect()
}

fn kind_config_matches_entity(cfg: &K8sKindConfig, entity: &k8s::K8sEntity) -> bool {
    if cfg.kind != entity.kind {
        return false;
    }
    if let Some(api_version) = &cfg.api_version {
        let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(&entity.raw) else {
            return false;
        };
        if value.get("apiVersion").and_then(serde_yaml::Value::as_str) != Some(api_version.as_str())
        {
            return false;
        }
    }
    true
}

fn locator_matches_entity(locator: &K8sImageLocator, entity: &k8s::K8sEntity) -> bool {
    if locator
        .kind
        .as_deref()
        .is_some_and(|kind| kind != entity.kind)
    {
        return false;
    }
    if locator
        .name
        .as_deref()
        .is_some_and(|name| name != entity.name)
    {
        return false;
    }
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(&entity.raw) else {
        return false;
    };
    if let Some(api_version) = &locator.api_version {
        if value.get("apiVersion").and_then(serde_yaml::Value::as_str) != Some(api_version.as_str())
        {
            return false;
        }
    }
    if let Some(namespace) = &locator.namespace {
        if value
            .get("metadata")
            .and_then(|m| m.get("namespace"))
            .and_then(serde_yaml::Value::as_str)
            != Some(namespace.as_str())
        {
            return false;
        }
    }
    image_from_locator(&value, locator).is_some()
}

fn extract_locator_images(raw: &str, locators: &[K8sImageLocator]) -> Vec<String> {
    let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
        return vec![];
    };
    locators
        .iter()
        .filter_map(|locator| image_from_locator(&value, locator))
        .collect()
}

fn image_from_locator(value: &serde_yaml::Value, locator: &K8sImageLocator) -> Option<String> {
    if let Some(object) = &locator.object {
        image_at_object_path(value, &locator.path, object)
    } else {
        image_at_simple_path(value, &locator.path)
    }
}

fn image_at_simple_path(value: &serde_yaml::Value, path: &[String]) -> Option<String> {
    let mut cur = value;
    for part in path {
        cur = cur.get(part.as_str())?;
    }
    cur.as_str().map(str::to_string)
}

fn image_at_object_path(
    value: &serde_yaml::Value,
    path: &[String],
    object: &K8sImageObjectLocator,
) -> Option<String> {
    let mut cur = value;
    for part in path {
        cur = cur.get(part.as_str())?;
    }
    let repo = cur.get(object.repo_field.as_str())?.as_str()?;
    let tag = cur.get(object.tag_field.as_str())?.as_str()?;
    Some(format!("{repo}:{tag}"))
}

fn rewrite_locator_images(
    raw: &str,
    locators: &[K8sImageLocator],
    original_ref: &str,
    rewritten_ref: &str,
) -> String {
    let Ok(mut value) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
        return raw.to_string();
    };
    for locator in locators {
        if let Some(object) = &locator.object {
            rewrite_image_at_object_path(
                &mut value,
                &locator.path,
                object,
                original_ref,
                rewritten_ref,
            );
        } else {
            rewrite_image_at_simple_path(&mut value, &locator.path, original_ref, rewritten_ref);
        }
    }
    serde_yaml::to_string(&value).unwrap_or_else(|_| raw.to_string())
}

fn rewrite_image_at_simple_path(
    value: &mut serde_yaml::Value,
    path: &[String],
    original_ref: &str,
    rewritten_ref: &str,
) {
    let mut cur = value;
    for part in path {
        let Some(next) = cur.get_mut(part.as_str()) else {
            return;
        };
        cur = next;
    }
    if cur
        .as_str()
        .is_some_and(|image| image_matches(image, original_ref))
    {
        *cur = serde_yaml::Value::String(rewritten_ref.to_string());
    }
}

fn rewrite_image_at_object_path(
    value: &mut serde_yaml::Value,
    path: &[String],
    object: &K8sImageObjectLocator,
    original_ref: &str,
    rewritten_ref: &str,
) {
    let mut cur = value;
    for part in path {
        let Some(next) = cur.get_mut(part.as_str()) else {
            return;
        };
        cur = next;
    }
    let Some(repo) = cur
        .get(object.repo_field.as_str())
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    let Some(tag) = cur
        .get(object.tag_field.as_str())
        .and_then(serde_yaml::Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    if !image_matches(&format!("{repo}:{tag}"), original_ref) {
        return;
    }
    let (new_repo, new_tag) = split_image_ref_repo_tag(rewritten_ref);
    if let Some(map) = cur.as_mapping_mut() {
        map.insert(
            serde_yaml::Value::String(object.repo_field.clone()),
            serde_yaml::Value::String(new_repo),
        );
        map.insert(
            serde_yaml::Value::String(object.tag_field.clone()),
            serde_yaml::Value::String(new_tag),
        );
    }
}

fn split_image_ref_repo_tag(image: &str) -> (String, String) {
    if let Some((repo, digest)) = image.split_once('@') {
        return (repo.to_string(), digest.to_string());
    }
    let repo = image_repo(image);
    if repo != image {
        let tag = image
            .strip_prefix(repo)
            .and_then(|rest| rest.strip_prefix(':'))
            .unwrap_or("latest");
        (repo.to_string(), tag.to_string())
    } else {
        (image.to_string(), "latest".to_string())
    }
}

/// Evaluate another Starlingfile into the current shared state (for `include`
/// and `load_dynamic`): its resource registrations etc. take effect, relative
/// paths resolve against its own directory, and it's tracked for reload.
fn run_starlingfile_into(path: &str, eval: &mut Evaluator) -> Result<FrozenModule> {
    let st = state(eval);
    let target = resolve(&st.cur_dir(), path);
    st.config_files.borrow_mut().push(target.clone());
    let src = std::fs::read_to_string(&target)
        .with_context(|| format!("include: reading {}", target.display()))?;
    let src = with_compat_prelude(src);
    let ast = AstModule::parse(&target.to_string_lossy(), src, &Dialect::Extended)
        .map_err(|e| anyhow!("parsing {}: {e}", target.display()))?;
    let globals = build_globals();
    let module = Module::new();
    let printer = LogPrint(&st.log);
    let loader = StarlingLoader {
        st,
        globals: &globals,
    };
    st.dir_stack.borrow_mut().push(parent_or_dot(&target));
    let result = {
        let mut sub = Evaluator::new(&module);
        sub.extra = Some(st);
        sub.set_loader(&loader);
        sub.set_print_handler(&printer);
        sub.eval_module(ast, &globals)
    };
    st.dir_stack.borrow_mut().pop();
    result.map_err(|e| anyhow!("include({path:?}): {e}"))?;
    module
        .freeze()
        .map_err(|e| anyhow!("freezing included Starlingfile {path:?}: {e}"))
}

/// Run a command and return its stdout, erroring on non-zero exit.
fn run_capture(argv: &[String], dir: &Path) -> Result<String> {
    let out = Command::new(&argv[0])
        .args(&argv[1..])
        .current_dir(dir)
        .output()
        .with_context(|| format!("running {}", argv.join(" ")))?;
    if !out.status.success() {
        return Err(anyhow!(
            "{} failed: {}",
            argv.join(" "),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}

fn apply_container_overrides(raw: &str, db: &DockerBuild) -> String {
    if db.entrypoint.is_empty() && db.container_args.is_none() {
        return raw.to_string();
    }
    let Ok(mut value) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
        return raw.to_string();
    };
    override_matching_containers(&mut value, db);
    serde_yaml::to_string(&value).unwrap_or_else(|_| raw.to_string())
}

fn override_matching_containers(value: &mut serde_yaml::Value, db: &DockerBuild) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for key in ["containers", "initContainers"] {
                let key = serde_yaml::Value::String(key.to_string());
                if let Some(serde_yaml::Value::Sequence(seq)) = map.get_mut(&key) {
                    for container in seq {
                        override_container_if_image_matches(container, db);
                    }
                }
            }
            for value in map.values_mut() {
                override_matching_containers(value, db);
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for value in seq {
                override_matching_containers(value, db);
            }
        }
        _ => {}
    }
}

fn override_container_if_image_matches(value: &mut serde_yaml::Value, db: &DockerBuild) {
    let serde_yaml::Value::Mapping(map) = value else {
        return;
    };
    let Some(image) = map
        .get(serde_yaml::Value::String("image".to_string()))
        .and_then(serde_yaml::Value::as_str)
    else {
        return;
    };
    if !image_matches(image, &db.image_ref) {
        return;
    }
    if !db.entrypoint.is_empty() {
        map.insert(
            serde_yaml::Value::String("command".to_string()),
            string_sequence_value(&db.entrypoint),
        );
    }
    if let Some(args) = &db.container_args {
        map.insert(
            serde_yaml::Value::String("args".to_string()),
            string_sequence_value(args),
        );
    }
}

pub(crate) fn rewrite_container_image(
    raw: &str,
    original_ref: &str,
    rewritten_ref: &str,
) -> String {
    if original_ref == rewritten_ref {
        return raw.to_string();
    }
    let Ok(mut value) = serde_yaml::from_str::<serde_yaml::Value>(raw) else {
        return raw.to_string();
    };
    rewrite_matching_container_images(&mut value, original_ref, rewritten_ref);
    serde_yaml::to_string(&value).unwrap_or_else(|_| raw.to_string())
}

fn rewrite_matching_container_images(
    value: &mut serde_yaml::Value,
    original_ref: &str,
    rewritten_ref: &str,
) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if let Some(image) = map
                .get_mut(serde_yaml::Value::String("image".to_string()))
                .and_then(|value| value.as_str())
                .map(str::to_string)
            {
                if image_matches(&image, original_ref) {
                    map.insert(
                        serde_yaml::Value::String("image".to_string()),
                        serde_yaml::Value::String(rewritten_ref.to_string()),
                    );
                }
            }
            for value in map.values_mut() {
                rewrite_matching_container_images(value, original_ref, rewritten_ref);
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for value in seq {
                rewrite_matching_container_images(value, original_ref, rewritten_ref);
            }
        }
        _ => {}
    }
}

fn string_sequence_value(values: &[String]) -> serde_yaml::Value {
    serde_yaml::Value::Sequence(
        values
            .iter()
            .map(|value| serde_yaml::Value::String(value.clone()))
            .collect(),
    )
}

fn default_registry_image_ref(
    registry: Option<&DefaultRegistry>,
    image_ref: &str,
    from_cluster: bool,
) -> String {
    let Some(registry) = registry else {
        return image_ref.to_string();
    };
    let host = if from_cluster {
        registry
            .host_from_cluster
            .as_ref()
            .unwrap_or(&registry.host)
    } else {
        &registry.host
    };
    if host.is_empty() || image_ref.starts_with(&format!("{host}/")) {
        return image_ref.to_string();
    }
    if let Some(single_name) = &registry.single_name {
        return format!("{host}/{single_name}");
    }
    format!("{host}/{}", escape_image_ref(image_ref))
}

fn escape_image_ref(image_ref: &str) -> String {
    image_repo(image_ref)
        .chars()
        .map(|ch| match ch {
            '/' | ':' | '@' => '_',
            _ => ch,
        })
        .collect()
}

fn resolve(dir: &Path, p: &str) -> PathBuf {
    let pb = PathBuf::from(p);
    if pb.is_absolute() {
        pb
    } else {
        dir.join(pb)
    }
}

fn apply_dc_resource_configs(st: &TfState, manifests: &mut Vec<Manifest>) -> Result<()> {
    for cfg in st.dc_configs.borrow().iter() {
        let matching: Vec<usize> = manifests
            .iter()
            .enumerate()
            .filter(|(_, m)| {
                m.kind == TargetKind::DockerCompose
                    && m.name == cfg.name
                    && cfg.project_name.as_ref().map_or(true, |project| {
                        m.docker_compose_project.as_deref() == Some(project.as_str())
                    })
            })
            .map(|(idx, _)| idx)
            .collect();

        if matching.is_empty() {
            if let Some(project) = &cfg.project_name {
                return Err(anyhow!(
                    "dc_resource({:?}, project_name={:?}): no matching docker_compose service",
                    cfg.name,
                    project
                ));
            }
            return Err(anyhow!(
                "dc_resource({:?}): no matching docker_compose service",
                cfg.name
            ));
        }
        if matching.len() > 1 {
            return Err(anyhow!(
                "dc_resource({:?}) is ambiguous; specify project_name",
                cfg.name
            ));
        }

        let idx = matching[0];
        if let Some(new_name) = &cfg.new_name {
            if manifests
                .iter()
                .enumerate()
                .any(|(other, m)| other != idx && m.name == *new_name)
            {
                return Err(anyhow!(
                    "dc_resource({:?}): new_name {:?} conflicts with an existing resource",
                    cfg.name,
                    new_name
                ));
            }
        }

        let m = &mut manifests[idx];
        if !cfg.resource_deps.is_empty() {
            m.resource_deps = cfg.resource_deps.clone();
        }
        m.auto_init = cfg.auto_init;
        m.trigger_mode = model_trigger_mode(cfg.trigger_mode, cfg.auto_init);
        m.links.extend(cfg.links.iter().cloned());
        for (k, v) in &cfg.labels {
            m.labels.insert(k.clone(), v.clone());
        }
        if let Some(image) = &cfg.image {
            // Attach the matching docker_build/custom_build so the engine builds
            // it before `docker compose up` (Compose image-build integration).
            let builds = st.docker_builds.borrow();
            match builds
                .iter()
                .find(|b| b.image_ref == *image || image_matches(image, &b.image_ref))
            {
                Some(build)
                    if !m
                        .docker_builds
                        .iter()
                        .any(|d| d.image_ref == build.image_ref) =>
                {
                    m.docker_builds.push(build.clone());
                }
                Some(_) => {}
                None => m.notes.push(format!(
                    "dc_resource(image={image:?}) has no matching docker_build/custom_build"
                )),
            }
        }
        if let Some(new_name) = &cfg.new_name {
            m.name = new_name.clone();
        }
    }
    Ok(())
}

fn validate_unique_manifest_names(manifests: &[Manifest]) -> Result<()> {
    let mut seen = HashMap::<&str, &TargetKind>::new();
    for manifest in manifests {
        if let Some(existing_kind) = seen.insert(&manifest.name, &manifest.kind) {
            return Err(anyhow!(
                "resource named {:?} already exists (kinds: {}, {})",
                manifest.name,
                existing_kind.target_type(),
                manifest.kind.target_type()
            ));
        }
    }
    Ok(())
}

/// Collect the secret values (`data` base64 strings + `stringData` plaintext)
/// from every Secret entity, for redacting from logs. Values are taken as they
/// appear in the YAML (no base64 decode), which is what leaks via apply logs.
fn collect_secret_values(entities: &[K8sEntity]) -> Vec<String> {
    let mut values = Vec::new();
    for e in entities.iter().filter(|e| e.kind == "Secret") {
        let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&e.raw) else {
            continue;
        };
        for field in ["data", "stringData"] {
            if let Some(map) = doc.get(field).and_then(|v| v.as_mapping()) {
                for v in map.values() {
                    if let Some(s) = v.as_str() {
                        if !s.is_empty() {
                            values.push(s.to_string());
                        }
                    }
                }
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

/// The lowercased `name:kind:namespace:group` identity Tilt passes to
/// `workload_to_resource_function` and uses in its conflict/error messages.
fn entity_object_id_string(e: &K8sEntity) -> String {
    format!("{}:{}:{}:{}", e.name, e.kind, e.namespace, e.group()).to_lowercase()
}

/// The workload entity name embedded in a manifest's `k8s_workload` (`Kind/name`).
fn manifest_workload_name(m: &Manifest) -> Option<&str> {
    m.k8s_workload
        .as_deref()
        .and_then(|w| w.split_once('/').map(|(_, name)| name))
}

/// Invoke a registered `workload_to_resource_function` once per workload,
/// recording the resulting resource names in [`TfState`] for `assemble_k8s`.
/// Must run while the evaluator (and its module) are still alive.
fn apply_workload_to_resource_function<'v>(
    st: &TfState,
    eval: &mut Evaluator<'v, '_>,
) -> Result<()> {
    let Some(func) = eval.module().extra_value() else {
        return Ok(());
    };
    // Collect workloads up front so no `TfState` borrow is held across the
    // Starlark call (the user's function could re-enter Starling builtins).
    let workloads: Vec<(String, String, String, String, String)> = {
        let entities = st.k8s_entities.borrow();
        let image_locators = st.k8s_image_locators.borrow().clone();
        let kind_configs = st.k8s_kind_configs.borrow().clone();
        entities
            .iter()
            .filter(|e| {
                e.is_workload()
                    || !entity_matching_image_locators(e, &image_locators).is_empty()
                    || !entity_matching_kind_configs(e, &kind_configs).is_empty()
            })
            .map(|e| {
                (
                    entity_object_id_string(e),
                    e.name.clone(),
                    e.kind.clone(),
                    e.namespace.clone(),
                    e.group(),
                )
            })
            .collect()
    };

    let mut taken: HashMap<String, String> = HashMap::new();
    let mut renames: HashMap<String, String> = HashMap::new();
    for (id_str, name, kind, namespace, group) in workloads {
        let id = eval.heap().alloc(AllocStruct([
            ("name", name),
            ("kind", kind),
            ("namespace", namespace),
            ("group", group),
        ]));
        let ret = eval.eval_function(func, &[id], &[]).map_err(|err| {
            anyhow!("workload_to_resource_function: error determining resource name for '{id_str}': {err}")
        })?;
        let resource_name = ret
            .unpack_str()
            .ok_or_else(|| {
                anyhow!(
                    "workload_to_resource_function: invalid return value for '{id_str}': wanted string, got {}",
                    ret.get_type()
                )
            })?
            .to_string();
        if let Some(prev) = taken.get(&resource_name) {
            return Err(anyhow!(
                "workload_to_resource_function: both '{prev}' and '{id_str}' mapped to resource name '{resource_name}'"
            ));
        }
        taken.insert(resource_name.clone(), id_str.clone());
        renames.insert(id_str, resource_name);
    }
    *st.workload_resource_names.borrow_mut() = renames;
    Ok(())
}

/// Assemble k8s entities + configs + docker builds into k8s manifests.
fn assemble_k8s(st: &TfState) -> Vec<Manifest> {
    let entities = st.k8s_entities.borrow();
    let docker_builds = st.docker_builds.borrow();
    let default_registry = st.default_registry.borrow().clone();
    let workload_resource_names = st.workload_resource_names.borrow();
    let mut manifests: Vec<Manifest> = st.k8s_custom_manifests.borrow().clone();
    for m in &mut manifests {
        for dep in &m.k8s_custom_image_deps {
            if let Some(build) = docker_builds.iter().find(|build| &build.image_ref == dep) {
                m.docker_builds.push(build.clone());
            }
        }
    }

    // One manifest per workload.
    let image_locators = st.k8s_image_locators.borrow().clone();
    let kind_configs = st.k8s_kind_configs.borrow().clone();
    let is_locator_workload =
        |e: &k8s::K8sEntity| entity_matching_image_locators(e, &image_locators).len() > 0;
    let is_declared_kind_workload =
        |e: &k8s::K8sEntity| entity_matching_kind_configs(e, &kind_configs).len() > 0;
    for e in entities
        .iter()
        .filter(|e| e.is_workload() || is_locator_workload(e) || is_declared_kind_workload(e))
    {
        let resource_name = workload_resource_names
            .get(&entity_object_id_string(e))
            .cloned()
            .unwrap_or_else(|| e.name.clone());
        let mut m = Manifest::new(resource_name, TargetKind::Kubernetes);
        let mut apply_doc = e.raw.clone();
        m.k8s_workload = Some(format!("{}/{}", e.kind, e.name));
        m.pod_selector = e.match_labels.clone();
        if entity_matching_kind_configs(e, &kind_configs)
            .iter()
            .any(|cfg| cfg.pod_readiness.as_deref() == Some("ignore"))
        {
            m.pod_readiness_ignore = true;
        }
        // Match docker builds to this workload's image fields, and optionally
        // env var values when match_in_env_vars=True.
        let matching_locators = entity_matching_image_locators(e, &image_locators);
        let mut images = e.images.clone();
        images.extend(extract_locator_images(&e.raw, &matching_locators));
        for db in docker_builds.iter() {
            let matches_image = images.iter().any(|img| image_matches(img, &db.image_ref));
            let matches_env = db.match_in_env_vars
                && e.env_values
                    .iter()
                    .any(|value| image_matches(value, &db.image_ref));
            if (matches_image || matches_env)
                && !m.docker_builds.iter().any(|d| d.image_ref == db.image_ref)
            {
                // Inherit live_update + watch its sync sources for changes.
                for step in &db.live_update {
                    match step {
                        LiveUpdateStep::Sync { local, .. } => {
                            m.deps.push(PathBuf::from(local));
                        }
                        LiveUpdateStep::FallBackOn(paths) => {
                            m.deps.extend(paths.iter().map(PathBuf::from));
                        }
                        _ => {}
                    }
                }
                // custom_build deps trigger image rebuilds.
                m.deps.extend(db.deps.clone());
                m.live_update.extend(db.live_update.clone());
                apply_doc = apply_container_overrides(&apply_doc, db);
                let mut build = db.clone();
                let local_ref =
                    default_registry_image_ref(default_registry.as_ref(), &db.image_ref, false);
                let cluster_ref =
                    default_registry_image_ref(default_registry.as_ref(), &db.image_ref, true);
                if cluster_ref != db.image_ref {
                    apply_doc = rewrite_container_image(&apply_doc, &db.image_ref, &cluster_ref);
                    apply_doc = rewrite_locator_images(
                        &apply_doc,
                        &matching_locators,
                        &db.image_ref,
                        &cluster_ref,
                    );
                }
                build.image_ref = local_ref;
                m.docker_builds.push(build);
                for dep in &db.image_deps {
                    if let Some(dep_build) =
                        docker_builds.iter().find(|build| &build.image_ref == dep)
                    {
                        let mut dep_build = dep_build.clone();
                        dep_build.image_ref = default_registry_image_ref(
                            default_registry.as_ref(),
                            &dep_build.image_ref,
                            false,
                        );
                        if !m
                            .docker_builds
                            .iter()
                            .any(|existing| existing.image_ref == dep_build.image_ref)
                        {
                            m.docker_builds.push(dep_build);
                        }
                    }
                }
            }
        }
        m.k8s_apply_docs.push(apply_doc);
        manifests.push(m);
    }

    let k8s_configs = st.k8s_configs.borrow();
    // Selectors from every k8s_resource(objects=...); entities matching one are
    // held out of the implicit grouping below for explicit grouping instead.
    let object_selectors: Vec<ObjectSelector> = k8s_configs
        .iter()
        .flat_map(|cfg| cfg.objects.iter())
        .filter_map(|obj| parse_object_selector(obj).ok())
        .collect();
    let is_explicitly_grouped =
        |e: &k8s::K8sEntity| object_selectors.iter().any(|sel| sel.matches(e));

    // Attach non-workload docs (Service/ConfigMap/...) to the workload of the
    // same name, else to the first workload, else as their own resource. Objects
    // named by k8s_resource(objects=...) are held out for explicit grouping.
    for e in entities.iter().filter(|e| {
        !e.is_workload()
            && !is_locator_workload(e)
            && !is_declared_kind_workload(e)
            && !is_explicitly_grouped(e)
    }) {
        // Attach to a workload sharing this object's name. The match is by the
        // workload's own entity name (not the resource name) so renames from
        // workload_to_resource_function don't strand same-name objects.
        if let Some(m) = manifests
            .iter_mut()
            .find(|m| m.name == e.name || manifest_workload_name(m) == Some(e.name.as_str()))
        {
            m.k8s_apply_docs.push(e.raw.clone());
        } else if let Some(m) = manifests.first_mut() {
            m.k8s_apply_docs.push(e.raw.clone());
        } else {
            let mut m = Manifest::new(e.name.clone(), TargetKind::Kubernetes);
            m.k8s_apply_docs.push(e.raw.clone());
            m.k8s_workload = Some(format!("{}/{}", e.kind, e.name));
            manifests.push(m);
        }
    }

    // Apply k8s_resource configuration.
    for cfg in k8s_configs.iter() {
        let target_idx = if cfg.workload.is_empty() {
            if cfg.objects.is_empty() {
                None
            } else {
                let name = cfg
                    .new_name
                    .clone()
                    .unwrap_or_else(|| cfg.objects[0].split(':').next().unwrap_or("").to_string());
                let mut m = Manifest::new(name, TargetKind::Kubernetes);
                for obj in &cfg.objects {
                    let Ok(sel) = parse_object_selector(obj) else {
                        continue;
                    };
                    for e in entities
                        .iter()
                        .filter(|e| !e.is_workload() && sel.matches(e))
                    {
                        m.k8s_apply_docs.push(e.raw.clone());
                        if m.k8s_workload.is_none() {
                            m.k8s_workload = Some(format!("{}/{}", e.kind, e.name));
                        }
                    }
                }
                manifests.push(m);
                Some(manifests.len() - 1)
            }
        } else {
            manifests.iter().position(|m| m.name == cfg.workload)
        };

        if let Some(idx) = target_idx {
            let m = &mut manifests[idx];
            for pf in &cfg.port_forwards {
                m.links.push(port_forward_link(pf));
                m.k8s_port_forwards.push(pf.clone());
            }
            for (url, label) in &cfg.links {
                m.links.push((url.clone(), label.clone()));
            }
            if !cfg.resource_deps.is_empty() {
                m.resource_deps = cfg.resource_deps.clone();
            }
            m.auto_init = cfg.auto_init;
            m.trigger_mode = model_trigger_mode(cfg.trigger_mode, cfg.auto_init);
            for (k, v) in &cfg.labels {
                m.labels.insert(k.clone(), v.clone());
            }
            for (k, v) in &cfg.extra_pod_selectors {
                m.pod_selector.insert(k.clone(), v.clone());
            }
            if !cfg.workload.is_empty() {
                // Attach explicitly-selected objects' docs to this resource.
                for obj in &cfg.objects {
                    let Ok(sel) = parse_object_selector(obj) else {
                        continue;
                    };
                    for e in entities
                        .iter()
                        .filter(|e| !e.is_workload() && sel.matches(e))
                    {
                        m.k8s_apply_docs.push(e.raw.clone());
                    }
                }
                if let Some(nn) = &cfg.new_name {
                    m.name = nn.clone();
                }
            }
            if let Some(mode) = &cfg.pod_readiness {
                m.pod_readiness_ignore = mode == "ignore";
            }
        }
    }

    manifests
}

#[cfg(test)]
mod tests {
    use super::{
        as_bat_cmd, check_version_constraint, helm_template_args, image_matches, image_repo, load,
        load_with_options, parse_object_selector, posix_relpath, shell_argv, LiveUpdateStep,
        LoadOptions, ProbeAction, TargetKind,
    };

    #[test]
    fn version_constraint_is_enforced() {
        // Satisfied / unsatisfied / invalid.
        assert!(check_version_constraint(">=0.1.0", "0.2.0").is_ok());
        assert!(check_version_constraint(">=9.0.0", "0.2.0").is_err());
        assert!(check_version_constraint(">=0.1, <0.3", "0.2.0").is_ok());
        assert!(check_version_constraint("not-a-constraint!!", "0.2.0").is_err());
    }

    #[test]
    fn shell_argv_is_platform_specific() {
        assert_eq!(shell_argv("echo hi", false), vec!["sh", "-c", "echo hi"]);
        assert_eq!(
            shell_argv("echo hi", true),
            vec!["cmd.exe", "/S", "/C", "echo hi"]
        );
    }
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn object_selector_parses_parts_and_matches_kind() {
        let entity = &crate::k8s::parse_yaml(
            "apiVersion: v1\nkind: Secret\nmetadata:\n  name: shared\n  namespace: prod\n",
        )[0];
        // name-only matches.
        assert!(parse_object_selector("shared").unwrap().matches(entity));
        // name:kind matches case-insensitively.
        assert!(parse_object_selector("shared:secret")
            .unwrap()
            .matches(entity));
        // Wrong kind does not match.
        assert!(!parse_object_selector("shared:configmap")
            .unwrap()
            .matches(entity));
        // name:kind:namespace matches.
        assert!(parse_object_selector("shared:Secret:prod")
            .unwrap()
            .matches(entity));
        assert!(!parse_object_selector("shared:Secret:dev")
            .unwrap()
            .matches(entity));
        // More than 3 parts is an error.
        assert!(parse_object_selector("a:b:c:d").is_err());
    }

    #[test]
    fn set_team_and_feature_flags_flow_to_load_result() {
        let dir = std::env::temp_dir().join(format!("starling-team-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            "set_team(\"team-42\")\nenable_feature(\"snapshots\")\ndisable_feature(\"beta\")\n",
        )
        .unwrap();
        let result = load(&file).unwrap();
        assert_eq!(result.team_id.as_deref(), Some("team-42"));
        let mut flags = result.feature_flags.clone();
        flags.sort();
        assert_eq!(
            flags,
            vec![("beta".to_string(), false), ("snapshots".to_string(), true)]
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn update_settings_max_parallel_flows_to_load_result() {
        let dir = std::env::temp_dir().join(format!("starling-ups-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(&file, "update_settings(max_parallel_updates=2)\n").unwrap();
        assert_eq!(load(&file).unwrap().max_parallel_updates, Some(2));
        let _ = fs::remove_dir_all(&dir);

        // Absent -> None (engine imposes no practical cap).
        fs::create_dir_all(&dir).unwrap();
        fs::write(&file, "print('none')\n").unwrap();
        assert_eq!(load(&file).unwrap().max_parallel_updates, None);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn ext_loading_from_local_repo() {
        let dir = std::env::temp_dir().join(format!("starling-ext-{}", uuid::Uuid::new_v4()));
        let repo = dir.join("repo");
        let ext = repo.join("restart_process");
        fs::create_dir_all(&ext).unwrap();
        // The extension defines a symbol and registers a local_resource.
        fs::write(
            ext.join("Tiltfile"),
            "x = 1\nlocal_resource(\"from-ext\", \"echo hi\")\n",
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            format!(
                "extension_repo(name=\"myrepo\", url=\"file://{}\")\nload(\"ext://myrepo/restart_process\", \"x\")\n",
                repo.display()
            ),
        )
        .unwrap();

        let result = load(&file).unwrap();
        // The extension's local_resource was registered into the shared state.
        assert!(result.manifests.iter().any(|m| m.name == "from-ext"));
        assert_eq!(result.extension_repos.len(), 1);
        assert_eq!(
            result.extensions,
            vec![("myrepo/restart_process".to_string(), "myrepo".to_string())]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore] // needs `git`; run with STARLING_DC_IT=1 cargo test -- --ignored ext_loading_clones
    fn ext_loading_clones_git_repo() {
        use std::process::Command;
        if std::env::var("STARLING_DC_IT").is_err() {
            return;
        }
        let root = std::env::temp_dir().join(format!("starling-extgit-{}", uuid::Uuid::new_v4()));
        let repo = root.join("repo");
        let ext = repo.join("restart_process");
        fs::create_dir_all(&ext).unwrap();
        fs::write(
            ext.join("Tiltfile"),
            "x = 1\nlocal_resource(\"from-git-ext\", \"true\")\n",
        )
        .unwrap();
        // Make `repo` a git repo with one commit so it can be cloned.
        let git = |args: &[&str]| {
            Command::new("git")
                .args(args)
                .current_dir(&repo)
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
        };
        assert!(git(&["init", "-q"]), "git init (is git installed?)");
        git(&["add", "."]);
        assert!(git(&[
            "-c",
            "user.email=t@example.com",
            "-c",
            "user.name=t",
            "commit",
            "-q",
            "-m",
            "init",
        ]));

        let cache = root.join("cache");
        std::env::set_var("STARLING_EXT_CACHE", &cache);
        let file = root.join("Tiltfile");
        fs::write(
            &file,
            format!(
                "extension_repo(name=\"r\", url=\"{}\")\nload(\"ext://r/restart_process\", \"x\")\n",
                repo.display()
            ),
        )
        .unwrap();
        let result = load(&file).unwrap();
        std::env::remove_var("STARLING_EXT_CACHE");

        // The cloned extension's local_resource registered into shared state.
        assert!(
            result.manifests.iter().any(|m| m.name == "from-git-ext"),
            "cloned extension did not load"
        );
        assert!(cache.join("r").join(".git").exists(), "repo was not cloned");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn ext_loading_unregistered_repo_errors() {
        let dir = std::env::temp_dir().join(format!("starling-ext-err-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(&file, "load(\"ext://nope/thing\", \"x\")\n").unwrap();
        let err = match load(&file) {
            Ok(_) => panic!("unregistered ext repo should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("not registered"), "err: {err}");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn secret_settings_controls_secret_value_collection() {
        let dir = std::env::temp_dir().join(format!("starling-secret-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        let secret = r#"
apiVersion: v1
kind: Secret
metadata:
  name: creds
data:
  token: aHVudGVyMg==
stringData:
  plain: opensesame
"#;
        // Default: secret values are collected for scrubbing.
        fs::write(&file, format!("k8s_yaml(\"\"\"{secret}\"\"\")\n")).unwrap();
        let result = load(&file).unwrap();
        assert!(result.secret_values.contains(&"aHVudGVyMg==".to_string()));
        assert!(result.secret_values.contains(&"opensesame".to_string()));

        // disable_scrub=True opts out.
        fs::write(
            &file,
            format!("secret_settings(disable_scrub=True)\nk8s_yaml(\"\"\"{secret}\"\"\")\n"),
        )
        .unwrap();
        assert!(load(&file).unwrap().secret_values.is_empty());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn blob_type_behaves_like_a_string() {
        let dir = std::env::temp_dir().join(format!("starling-blob-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("hello.txt"), "hi-from-file").unwrap();
        let file = dir.join("Tiltfile");
        // Blob from blob() and read_file(); check type(), concatenation both ways,
        // and that a blob feeds a string-consuming builtin (local_resource name).
        fs::write(
            &file,
            r#"
b = blob("xyz")
print("type %s" % type(b))
f = read_file("hello.txt")
local_resource("r-" + f, cmd="true")          # str + blob (radd)
local_resource(b + "-tail", cmd="true")        # blob + str (add)
"#,
        )
        .unwrap();
        let result = load(&file).unwrap();
        assert!(result.log.contains("type blob"));
        let names: Vec<&str> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"r-hi-from-file"), "names: {names:?}");
        assert!(names.contains(&"xyz-tail"), "names: {names:?}");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn v1alpha1_constructors_build_objects() {
        let dir = std::env::temp_dir().join(format!("starling-v1a1-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        // The constructors return dicts; print their kinds so we can assert they
        // evaluated without error and carry the expected shape.
        fs::write(
            &file,
            r#"
cm = v1alpha1.config_map("settings", data={"k": "v"})
c = v1alpha1.cmd("build", args=["make"], dir="/repo")
ka = v1alpha1.kubernetes_apply("web", yaml="kind: Deployment")
print("kinds %s %s %s" % (cm["kind"], c["kind"], ka["kind"]))
print("cmdargs %s" % c["spec"]["args"][0])
print("cmdata %s" % cm["data"]["k"])
"#,
        )
        .unwrap();
        let result = load(&file).unwrap();
        assert!(result.log.contains("kinds ConfigMap Cmd KubernetesApply"));
        assert!(result.log.contains("cmdargs make"));
        assert!(result.log.contains("cmdata v"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn k8s_yaml_dedups_identical_entities() {
        let dir = std::env::temp_dir().join(format!("starling-dedup-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        // The same ConfigMap is registered twice; it should produce one resource.
        let cm = r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: shared
data:
  k: v
"#;
        fs::write(
            &file,
            format!("k8s_yaml(\"\"\"{cm}\"\"\")\nk8s_yaml(\"\"\"{cm}\"\"\")\n"),
        )
        .unwrap();
        let result = load(&file).unwrap();
        let shared = result
            .manifests
            .iter()
            .filter(|m| m.name == "shared")
            .count();
        assert_eq!(shared, 1, "duplicate entity should be deduped");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn ci_settings_timeout_flows_to_load_result() {
        let dir = std::env::temp_dir().join(format!("starling-ci-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(&file, "ci_settings(timeout=\"12m\")\n").unwrap();
        let result = load(&file).unwrap();
        assert_eq!(result.ci_timeout_secs, Some(720));
        let _ = fs::remove_dir_all(&dir);

        // Absent ci_settings -> None (ci falls back to its own default).
        fs::create_dir_all(&dir).unwrap();
        fs::write(&file, "print('no ci settings')\n").unwrap();
        assert_eq!(load(&file).unwrap().ci_timeout_secs, None);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn posix_relpath_matches_python_semantics() {
        let cwd = Path::new("/home/user/proj");
        // Target under base.
        assert_eq!(posix_relpath("/a/b/c", "/a/b", cwd), "c");
        // Sibling directories require `..` segments.
        assert_eq!(posix_relpath("/a/b/c", "/a/x", cwd), "../b/c");
        // Identical paths collapse to ".".
        assert_eq!(posix_relpath("/a/b", "/a/b", cwd), ".");
        // Base deeper than target.
        assert_eq!(posix_relpath("/a", "/a/b/c", cwd), "../..");
        // Relative inputs are absolutized against cwd.
        assert_eq!(posix_relpath("src/main.rs", ".", cwd), "src/main.rs");
        assert_eq!(posix_relpath(".", "src", cwd), "..");
        // `.`/`..` segments are normalized lexically.
        assert_eq!(posix_relpath("/a/b/../c", "/a", cwd), "c");
    }

    #[test]
    fn matches_images_to_build_refs() {
        assert!(image_matches("myreg/web:dev", "myreg/web"));
        assert!(image_matches("web:latest", "web"));
        assert!(image_matches("gcr.io/proj/web:abc123", "web"));
        assert!(!image_matches("other:dev", "web"));
        // Registry host:port must not be mistaken for a tag.
        assert_eq!(image_repo("localhost:5000/web"), "localhost:5000/web");
        assert_eq!(image_repo("web:tag"), "web");
    }

    #[test]
    fn windows_bat_commands_use_cmd_shell() {
        let heap = starlark::values::Heap::new();
        let cmd = as_bat_cmd(heap.alloc("echo hi"), std::path::Path::new("."));
        assert_eq!(cmd.argv, vec!["cmd.exe", "/S", "/C", "echo hi"]);
    }

    /// The host-shell selection logic, exercised explicitly for both platforms
    /// (the `windows` flag is a parameter, so this verifies the Windows command
    /// construction on any OS — the part of Windows parity testable without a
    /// Windows runner). On Windows a string command runs through `cmd.exe /S /C`;
    /// elsewhere through `sh -c`.
    #[test]
    fn shell_argv_selects_host_shell_per_platform() {
        assert_eq!(
            shell_argv("echo hi && ls", true),
            vec!["cmd.exe", "/S", "/C", "echo hi && ls"],
            "Windows string commands must use cmd.exe /S /C"
        );
        assert_eq!(
            shell_argv("echo hi && ls", false),
            vec!["sh", "-c", "echo hi && ls"],
            "Unix string commands must use sh -c"
        );
        // The command string is passed through verbatim as the final argument on
        // both platforms (no quoting/splitting), so shell operators survive.
        assert_eq!(shell_argv("a|b>c", true).last().unwrap(), "a|b>c");
        assert_eq!(shell_argv("a|b>c", false).last().unwrap(), "a|b>c");
    }

    #[test]
    fn tilt_compat_docker_build_overrides_container_command() {
        let dir =
            std::env::temp_dir().join(format!("starling-entrypoint-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        // docker_build(entrypoint=, container_args=) overrides the matching
        // container's command/args in the deployed YAML.
        fs::write(
            &file,
            r#"
docker_build("example/web", ".", entrypoint=["/bin/serve"], container_args=["--port", "9000"])
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: example/web
""")
"#,
        )
        .unwrap();
        let result = load(&file).unwrap();
        let web = result
            .manifests
            .iter()
            .find(|m| m.name == "web")
            .expect("workload resource");
        let doc = web.k8s_apply_docs.join("\n");
        assert!(
            doc.contains("/bin/serve"),
            "command override missing: {doc}"
        );
        assert!(doc.contains("--port"), "args override missing: {doc}");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_docker_build_options_are_parsed() {
        let dir = std::env::temp_dir().join(format!("starling-docker-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build(
    "example/web",
    ".",
    target="runtime",
    platform="linux/amd64",
    cache=True,
    cache_from=["example/web:cache"],
    ssh="default",
    secret=["id=shibboleth"],
    pull=True,
    network="host",
    extra_hosts=["host.docker.internal:host-gateway"],
    extra_tag=["example/web:latest"],
    entrypoint=["/bin/web"],
    container_args=["--port=8080"],
    match_in_env_vars=True,
    live_update=[
        sync("src", "/app/src"),
        fall_back_on(["Dockerfile", "requirements.txt"]),
        run("echo secret", echo_off=True),
        run("echo triggered", trigger=["src/trigger.txt"]),
    ],
)
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: example/web
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        let db = result.manifests[0]
            .docker_builds
            .first()
            .expect("docker_build attached to manifest");
        assert_eq!(db.target.as_deref(), Some("runtime"));
        assert_eq!(db.platform.as_deref(), Some("linux/amd64"));
        assert_eq!(db.extra_tags, vec!["example/web:latest".to_string()]);
        assert_eq!(db.entrypoint, vec!["/bin/web".to_string()]);
        assert_eq!(
            db.container_args.as_ref(),
            Some(&vec!["--port=8080".to_string()])
        );
        match db
            .live_update
            .iter()
            .find(|step| matches!(step, LiveUpdateStep::Run { .. }))
            .expect("run step")
        {
            LiveUpdateStep::Run {
                cmd,
                echo_off,
                triggers,
            } => {
                assert_eq!(cmd, "echo secret");
                assert!(*echo_off);
                assert_eq!(triggers.len(), 0);
            }
            other => panic!("expected run step, got {other:?}"),
        }
        assert!(db.match_in_env_vars);
        assert_eq!(db.cache_from, vec!["example/web:cache".to_string()]);
        assert_eq!(db.ssh, vec!["default".to_string()]);
        assert_eq!(db.secrets, vec!["id=shibboleth".to_string()]);
        let triggered = db
            .live_update
            .iter()
            .find_map(|step| match step {
                LiveUpdateStep::Run { cmd, triggers, .. } if cmd == "echo triggered" => {
                    Some(triggers)
                }
                _ => None,
            })
            .expect("triggered run step");
        assert!(triggered
            .iter()
            .any(|path| path.ends_with("src/trigger.txt")));
        assert!(result.log.contains("docker_build(cache=...) is obsolete"));
        assert!(result.manifests[0]
            .deps
            .iter()
            .any(|path| path.ends_with("Dockerfile")));
        assert!(result.manifests[0]
            .deps
            .iter()
            .any(|path| path.ends_with("requirements.txt")));
        assert!(db.pull);
        assert_eq!(db.network.as_deref(), Some("host"));
        assert_eq!(
            db.extra_hosts,
            vec!["host.docker.internal:host-gateway".to_string()]
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        let container = &applied["spec"]["template"]["spec"]["containers"][0];
        assert_eq!(
            container["command"],
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::String("/bin/web".to_string())])
        );
        assert_eq!(
            container["args"],
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::String("--port=8080".to_string())])
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_initial_sync_must_be_first_and_unique() {
        let dir =
            std::env::temp_dir().join(format!("starling-initial-sync-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();

        let not_first = dir.join("Tiltfile.not-first");
        fs::write(
            &not_first,
            r#"
docker_build("example/web", ".", live_update=[
    sync("src", "/app/src"),
    initial_sync(),
])
"#,
        )
        .unwrap();
        let err = match load(&not_first) {
            Ok(_) => panic!("initial_sync after sync should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("initial_sync must appear at most once"));

        let duplicate = dir.join("Tiltfile.duplicate");
        fs::write(
            &duplicate,
            r#"
docker_build("example/web", ".", live_update=[
    initial_sync(),
    initial_sync(),
    sync("src", "/app/src"),
])
"#,
        )
        .unwrap();
        let err = match load(&duplicate) {
            Ok(_) => panic!("duplicate initial_sync should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("initial_sync must appear at most once"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_docker_build_can_match_env_vars() {
        let dir =
            std::env::temp_dir().join(format!("starling-docker-env-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("example/worker", ".", match_in_env_vars=True)
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      containers:
      - name: worker
        image: busybox
        env:
        - name: WORKER_IMAGE
          value: example/worker
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].docker_builds.len(), 1);
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "example/worker"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_default_registry_rewrites_local_images() {
        let dir =
            std::env::temp_dir().join(format!("starling-default-reg-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("web", ".")
default_registry("registry.dev/team")
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: web
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "registry.dev/team/web"
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        assert_eq!(
            applied["spec"]["template"]["spec"]["containers"][0]["image"],
            serde_yaml::Value::String("registry.dev/team/web".to_string())
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_default_registry_rewrites_qualified_and_cluster_refs() {
        let dir = std::env::temp_dir().join(format!(
            "starling-default-reg-cluster-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("gcr.io/foo", ".")
default_registry("abc.io", host_from_cluster="def.io")
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: foo
spec:
  selector:
    matchLabels:
      app: foo
  template:
    metadata:
      labels:
        app: foo
    spec:
      containers:
      - name: foo
        image: gcr.io/foo
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "abc.io/gcr.io_foo"
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        assert_eq!(
            applied["spec"]["template"]["spec"]["containers"][0]["image"],
            serde_yaml::Value::String("def.io/gcr.io_foo".to_string())
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_default_registry_single_name() {
        let dir = std::env::temp_dir().join(format!(
            "starling-default-reg-single-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("frontend", ".")
default_registry("123.dkr.ecr.us-east-1.amazonaws.com", single_name="team-a/dev")
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: frontend
spec:
  selector:
    matchLabels:
      app: frontend
  template:
    metadata:
      labels:
        app: frontend
    spec:
      containers:
      - name: frontend
        image: frontend
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "123.dkr.ecr.us-east-1.amazonaws.com/team-a/dev"
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        assert_eq!(
            applied["spec"]["template"]["spec"]["containers"][0]["image"],
            serde_yaml::Value::String("123.dkr.ecr.us-east-1.amazonaws.com/team-a/dev".to_string())
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_image_json_path_extracts_crd_images() {
        let dir = std::env::temp_dir().join(format!("starling-crd-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("runtime", ".")
default_registry("registry.dev/team")
k8s_image_json_path("{.spec.runtime.image}", kind="Environment", api_version="fission.io/v1")
k8s_yaml("""
apiVersion: fission.io/v1
kind: Environment
metadata:
  name: py
spec:
  runtime:
    image: runtime
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "py");
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "registry.dev/team/runtime"
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        assert_eq!(
            applied["spec"]["runtime"]["image"],
            serde_yaml::Value::String("registry.dev/team/runtime".to_string())
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_kind_image_object_extracts_crd_images() {
        let dir =
            std::env::temp_dir().join(format!("starling-crd-object-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("example/runtime", ".")
default_registry("registry.dev/team")
k8s_kind(
    kind="Environment",
    api_version="fission.io/v1",
    image_object={"json_path": "{.spec.runtime.image}", "repo_field": "repo", "tag_field": "tag"},
)
k8s_yaml("""
apiVersion: fission.io/v1
kind: Environment
metadata:
  name: py
spec:
  runtime:
    image:
      repo: example/runtime
      tag: dev
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "py");
        assert_eq!(
            result.manifests[0].docker_builds[0].image_ref,
            "registry.dev/team/example_runtime"
        );
        let applied: serde_yaml::Value =
            serde_yaml::from_str(&result.manifests[0].k8s_apply_docs[0]).unwrap();
        assert_eq!(
            applied["spec"]["runtime"]["image"]["repo"],
            serde_yaml::Value::String("registry.dev/team/example_runtime".to_string())
        );
        assert_eq!(
            applied["spec"]["runtime"]["image"]["tag"],
            serde_yaml::Value::String("latest".to_string())
        );

        let bad = dir.join("Tiltfile.bad");
        fs::write(
            &bad,
            r#"
k8s_kind(kind="Environment", image_json_path="{.spec.image}", image_object={"json_path": "{.spec.image}", "repo_field": "repo", "tag_field": "tag"})
"#,
        )
        .unwrap();
        let err = match load(&bad) {
            Ok(_) => panic!("image_json_path and image_object conflict should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Cannot specify both"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_kind_marks_crd_workload_and_pod_readiness() {
        let dir =
            std::env::temp_dir().join(format!("starling-crd-workload-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_kind("Widget", api_version="example.dev/v1", pod_readiness="ignore")
k8s_yaml("""
apiVersion: example.dev/v1
kind: Widget
metadata:
  name: sample
spec: {}
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "sample");
        assert!(result.manifests[0].pod_readiness_ignore);

        let bad = dir.join("Tiltfile.bad-readiness");
        fs::write(&bad, r#"k8s_kind("Widget", pod_readiness="sometimes")"#).unwrap();
        let err = match load(&bad) {
            Ok(_) => panic!("invalid pod_readiness should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Allowed: {ignore, wait}"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_filter_yaml_name_regex() {
        let dir =
            std::env::temp_dir().join(format!("starling-filter-yaml-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
matching, rest = filter_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-one
spec:
  selector:
    matchLabels:
      app: api-one
  template:
    metadata:
      labels:
        app: api-one
    spec:
      containers:
      - name: api-one
        image: busybox
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: busybox
""", kind="Deployment", name="^api-.+$")
k8s_yaml(matching)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let names: Vec<_> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["api-one"]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_helm_kube_version_flag() {
        let argv = helm_template_args(
            Path::new("chart"),
            "rose",
            Some("garnet"),
            vec![PathBuf::from("values-dev.yaml")],
            vec!["image.tag=dev".to_string()],
            Some("1.28.0"),
            false,
        );
        assert_eq!(
            argv,
            vec![
                "helm",
                "template",
                "rose",
                "chart",
                "--namespace",
                "garnet",
                "--values",
                "values-dev.yaml",
                "--set",
                "image.tag=dev",
                "--kube-version",
                "1.28.0",
                "--include-crds",
            ]
        );
    }

    #[test]
    fn tilt_compat_custom_build_tag_is_parsed() {
        let dir =
            std::env::temp_dir().join(format!("starling-custom-tag-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
custom_build(
  "gcr.io/foo",
  "docker build -t $EXPECTED_REF .",
  [],
  tag="dev",
  disable_push=True,
  skips_local_docker=True,
  match_in_env_vars=True,
)
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: foo
spec:
  selector:
    matchLabels:
      app: foo
  template:
    metadata:
      labels:
        app: foo
    spec:
      containers:
      - name: foo
        image: busybox
        env:
        - name: FOO_IMAGE
          value: gcr.io/foo
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let db = &result.manifests[0].docker_builds[0];
        assert_eq!(db.custom_tag.as_deref(), Some("dev"));
        assert!(db.disable_push);
        assert!(db.skips_local_docker);
        assert!(db.match_in_env_vars);
        assert!(db.command.is_some());

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_port_forward_link_name_and_path() {
        let dir =
            std::env::temp_dir().join(format!("starling-port-forward-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: busybox
""")
k8s_resource("web", port_forwards=[port_forward(8080, name="web ui", link_path="/healthz")])
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(
            result.manifests[0].links,
            vec![(
                "http://localhost:8080/healthz".to_string(),
                "web ui".to_string()
            )]
        );
        assert_eq!(result.manifests[0].k8s_port_forwards.len(), 1);
        assert_eq!(result.manifests[0].k8s_port_forwards[0].local_port, 8080);
        assert_eq!(
            result.manifests[0].k8s_port_forwards[0].container_port,
            8080
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_resource_validates_discovery_strategy() {
        let dir = std::env::temp_dir().join(format!("starling-discovery-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_resource("web", discovery_strategy="selectors-only")
"#,
        )
        .unwrap();
        assert!(load(&file).is_ok());

        let bad = dir.join("Tiltfile.bad");
        fs::write(
            &bad,
            r#"
k8s_resource("web", discovery_strategy="typo")
"#,
        )
        .unwrap();
        let err = match load(&bad) {
            Ok(_) => panic!("invalid discovery_strategy should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("selectors-only"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_custom_deploy_registers_commands_and_deps() {
        let dir =
            std::env::temp_dir().join(format!("starling-custom-deploy-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("image-a", ".")
k8s_custom_deploy(
    "foo",
    "echo apply",
    "echo delete",
    deps=["deploy"],
    apply_dir=".",
    apply_env={"APPLY_KEY": "1"},
    delete_dir=".",
    delete_env={"DELETE_KEY": "1"},
    image_deps=["image-a"],
)
k8s_resource("foo", labels="custom", port_forwards=8000)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let m = result
            .manifests
            .iter()
            .find(|m| m.name == "foo")
            .expect("custom deploy manifest");
        assert_eq!(m.kind, TargetKind::Kubernetes);
        assert!(m.k8s_custom_apply_cmd.is_some());
        assert!(m.k8s_custom_delete_cmd.is_some());
        assert!(m.deps.iter().any(|path| path.ends_with("deploy")));
        assert_eq!(m.docker_builds.len(), 1);
        assert_eq!(m.docker_builds[0].image_ref, "image-a");
        assert!(m.labels.contains_key("custom"));
        assert_eq!(m.k8s_port_forwards.len(), 1);

        let missing_apply = dir.join("Tiltfile.missing-apply");
        fs::write(
            &missing_apply,
            r#"k8s_custom_deploy("foo", [], "echo delete", deps=[])"#,
        )
        .unwrap();
        let err = match load(&missing_apply) {
            Ok(_) => panic!("empty apply_cmd should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("apply_cmd cannot be empty"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_dc_resource_customizes_compose_service() {
        let dir =
            std::env::temp_dir().join(format!("starling-dc-resource-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("compose.yml"),
            r#"
name: demo
services:
  web:
    image: example/web
  db:
    image: postgres
"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
dc_resource(
    "web",
    "example/web",
    new_name="frontend",
    resource_deps=["db"],
    links=[link("http://localhost:8080", "web")],
    labels="frontend",
    trigger_mode=TRIGGER_MODE_MANUAL,
    auto_init=False,
)
docker_compose("compose.yml")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let frontend = result
            .manifests
            .iter()
            .find(|m| m.name == "frontend")
            .expect("renamed compose resource");
        assert_eq!(frontend.kind, TargetKind::DockerCompose);
        assert_eq!(frontend.resource_deps, vec!["db".to_string()]);
        assert_eq!(
            frontend.links,
            vec![("http://localhost:8080".to_string(), "web".to_string())]
        );
        assert!(frontend.labels.contains_key("frontend"));
        assert_eq!(frontend.trigger_mode, 2);
        assert!(!frontend.auto_init);
        // No docker_build matches "example/web", so a note is recorded.
        assert!(frontend
            .notes
            .iter()
            .any(|note| note.contains("has no matching docker_build")));
        assert!(frontend
            .serve_cmd
            .argv
            .ends_with(&["up".to_string(), "web".to_string()]));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_dc_resource_image_attaches_docker_build() {
        let dir = std::env::temp_dir().join(format!("starling-dc-img-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("compose.yml"),
            "name: demo\nservices:\n  web:\n    image: example/web\n",
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_build("example/web", ".")
docker_compose("compose.yml")
dc_resource("web", "example/web")
"#,
        )
        .unwrap();
        let result = load(&file).unwrap();
        let web = result
            .manifests
            .iter()
            .find(|m| m.name == "web")
            .expect("compose resource");
        // The matching docker_build is attached so the engine builds it first.
        assert!(web
            .docker_builds
            .iter()
            .any(|b| b.image_ref == "example/web"));
        assert!(!web.notes.iter().any(|n| n.contains("no matching")));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_docker_compose_accepts_list_and_options() {
        let dir =
            std::env::temp_dir().join(format!("starling-docker-compose-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("compose.yml"),
            r#"
services:
  web:
    image: example/web
"#,
        )
        .unwrap();
        fs::write(dir.join(".env"), "TAG=dev\n").unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_compose(
    ["compose.yml", blob("""
services:
  worker:
    image: example/worker
""")],
    env_file=".env",
    project_name="hello",
    profiles=["debug"],
    wait=True,
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert!(result.manifests.iter().any(|m| m.name == "web"));
        let worker = result
            .manifests
            .iter()
            .find(|m| m.name == "worker")
            .expect("inline compose service");
        assert_eq!(worker.docker_compose_project.as_deref(), Some("hello"));
        assert!(worker
            .serve_cmd
            .argv
            .windows(2)
            .any(|args| args[0] == "--project-name" && args[1] == "hello"));
        let env_file = dir.join(".env").display().to_string();
        assert!(worker
            .serve_cmd
            .argv
            .windows(2)
            .any(|args| args[0] == "--env-file" && args[1] == env_file));
        assert!(worker
            .serve_cmd
            .argv
            .windows(2)
            .any(|args| args[0] == "--profile" && args[1] == "debug"));
        assert!(worker.serve_cmd.argv.iter().any(|arg| arg == "--wait"));
        assert_eq!(
            worker
                .serve_cmd
                .argv
                .iter()
                .filter(|arg| *arg == "-f")
                .count(),
            2
        );
        assert!(result
            .config_files
            .iter()
            .any(|p| p.ends_with("compose.yml")));
        assert!(result.config_files.iter().any(|p| p.ends_with(".env")));

        let default_env = dir.join("Tiltfile.default-env");
        fs::write(&default_env, r#"docker_compose("compose.yml")"#).unwrap();
        let result = load(&default_env).unwrap();
        assert!(result.config_files.iter().any(|p| p.ends_with(".env")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_dc_resource_errors_and_project_disambiguation() {
        let dir =
            std::env::temp_dir().join(format!("starling-dc-resource-{}", uuid::Uuid::new_v4()));
        let first = dir.join("first");
        let second = dir.join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        fs::write(
            first.join("compose.yml"),
            r#"
name: first
services:
  web:
    image: example/first
"#,
        )
        .unwrap();
        fs::write(
            second.join("compose.yml"),
            r#"
name: second
services:
  web:
    image: example/second
"#,
        )
        .unwrap();

        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
docker_compose("first/compose.yml")
docker_compose("second/compose.yml")
dc_resource("web", new_name="web2", project_name="second")
"#,
        )
        .unwrap();
        let result = load(&file).unwrap();
        assert!(result.manifests.iter().any(|m| m.name == "web"));
        assert!(result.manifests.iter().any(|m| m.name == "web2"));

        let duplicate = dir.join("Tiltfile.duplicate");
        fs::write(
            &duplicate,
            r#"
dc_resource("web")
dc_resource("web")
"#,
        )
        .unwrap();
        let err = match load(&duplicate) {
            Ok(_) => panic!("duplicate dc_resource should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("dc_resource named \"web\" already exists"));

        let missing = dir.join("Tiltfile.missing");
        fs::write(
            &missing,
            r#"
dc_resource("api")
docker_compose("first/compose.yml")
"#,
        )
        .unwrap();
        let err = match load(&missing) {
            Ok(_) => panic!("missing compose service should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("no matching docker_compose service"));

        let ambiguous = dir.join("Tiltfile.ambiguous");
        fs::write(
            &ambiguous,
            r#"
docker_compose("first/compose.yml")
docker_compose("second/compose.yml")
dc_resource("web")
"#,
        )
        .unwrap();
        let err = match load(&ambiguous) {
            Ok(_) => panic!("ambiguous compose service should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("is ambiguous; specify project_name"));

        let duplicate_resource = dir.join("Tiltfile.duplicate-resource");
        fs::write(
            &duplicate_resource,
            r#"
local_resource("web", "echo web")
docker_compose("first/compose.yml")
"#,
        )
        .unwrap();
        let err = match load(&duplicate_resource) {
            Ok(_) => panic!("duplicate resource names should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("resource named \"web\" already exists"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_custom_build_outputs_ref_and_image_deps_are_parsed() {
        let dir =
            std::env::temp_dir().join(format!("starling-custom-deps-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
custom_build("base", "true", [])
custom_build(
  "fe",
  "true",
  ["src"],
  outputs_image_ref_to="ref.txt",
  image_deps=["base"],
)
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fe
spec:
  selector:
    matchLabels:
      app: fe
  template:
    metadata:
      labels:
        app: fe
    spec:
      containers:
      - name: fe
        image: fe
""")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let builds = &result.manifests[0].docker_builds;
        assert_eq!(builds.len(), 2);
        assert_eq!(builds[0].image_ref, "fe");
        assert!(builds[0]
            .outputs_image_ref_to
            .as_ref()
            .is_some_and(|p| p.ends_with("ref.txt")));
        assert_eq!(builds[0].image_deps, vec!["base".to_string()]);
        assert_eq!(builds[1].image_ref, "base");

        let missing_file = dir.join("Tiltfile.missing");
        fs::write(
            &missing_file,
            r#"
custom_build("fe", "true", [], image_deps=["missing"])
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fe
spec:
  selector:
    matchLabels:
      app: fe
  template:
    metadata:
      labels:
        app: fe
    spec:
      containers:
      - name: fe
        image: fe
""")
"#,
        )
        .unwrap();
        let err = match load(&missing_file) {
            Ok(_) => panic!("missing image dep should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("image dep \"missing\" not found"));

        let conflict_file = dir.join("Tiltfile.conflict");
        fs::write(
            &conflict_file,
            r#"custom_build("fe", "true", [], tag="dev", outputs_image_ref_to="ref.txt")"#,
        )
        .unwrap();
        let err = match load(&conflict_file) {
            Ok(_) => panic!("tag and outputs_image_ref_to conflict should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Cannot specify both tag= and outputs_image_ref_to="));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_resource_objects_selector_disambiguates_by_kind() {
        let dir = std::env::temp_dir().join(format!("starling-objsel-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        // Two objects share the name "shared" but differ by kind; the selector
        // "shared:secret" must group only the Secret, leaving the ConfigMap free.
        fs::write(
            &file,
            r#"
k8s_yaml("""
apiVersion: v1
kind: ConfigMap
metadata:
  name: shared
data:
  key: value
---
apiVersion: v1
kind: Secret
metadata:
  name: shared
stringData:
  token: abc
""")
k8s_resource(new_name="secrets", objects=["shared:secret"], labels="ops")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let secrets = result
            .manifests
            .iter()
            .find(|m| m.name == "secrets")
            .expect("object-only resource");
        assert_eq!(secrets.k8s_apply_docs.len(), 1);
        assert!(secrets.k8s_apply_docs[0].contains("kind: Secret"));
        // The ConfigMap was not captured by the secret selector; it becomes its
        // own resource rather than joining "secrets".
        assert!(!secrets.k8s_apply_docs[0].contains("kind: ConfigMap"));
        let configmap = result
            .manifests
            .iter()
            .find(|m| m.name == "shared")
            .expect("ungrouped ConfigMap resource");
        assert!(configmap.k8s_apply_docs[0].contains("kind: ConfigMap"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_resource_objects_rejects_too_many_parts() {
        let dir =
            std::env::temp_dir().join(format!("starling-objsel-err-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_yaml("""
apiVersion: v1
kind: ConfigMap
metadata:
  name: cfg
data:
  key: value
""")
k8s_resource(new_name="x", objects=["a:b:c:d"])
"#,
        )
        .unwrap();
        let err = match load(&file) {
            Ok(_) => panic!("selector with 4 parts should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Too many parts in selector"), "err: {err}");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_resource_pod_readiness_ignore() {
        let dir =
            std::env::temp_dir().join(format!("starling-pod-readiness-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: busybox
""")
k8s_resource("web", pod_readiness="ignore")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert!(result.manifests[0].pod_readiness_ignore);

        let bad = dir.join("Tiltfile.bad-readiness");
        fs::write(
            &bad,
            r#"
k8s_resource("web", pod_readiness="sometimes")
"#,
        )
        .unwrap();
        let err = match load(&bad) {
            Ok(_) => panic!("invalid pod_readiness should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("Allowed: {ignore, wait}"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_resource_groups_object_only_resources() {
        let dir =
            std::env::temp_dir().join(format!("starling-k8s-objects-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
k8s_yaml("""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
      - name: web
        image: busybox
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: config
data:
  key: value
""")
k8s_resource(new_name="config", objects=["config"], labels="ops")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 2);
        let config = result
            .manifests
            .iter()
            .find(|m| m.name == "config")
            .expect("object-only resource");
        assert_eq!(config.kind, TargetKind::Kubernetes);
        assert_eq!(config.k8s_apply_docs.len(), 1);
        assert!(config.k8s_apply_docs[0].contains("kind: ConfigMap"));
        assert!(config.labels.contains_key("ops"));
        let web = result
            .manifests
            .iter()
            .find(|m| m.name == "web")
            .expect("workload resource");
        assert!(!web
            .k8s_apply_docs
            .iter()
            .any(|doc| doc.contains("kind: ConfigMap")));

        let _ = fs::remove_dir_all(dir);
    }

    /// A two-workload + Service Starlingfile body for workload-naming tests.
    fn wtrf_fixture_yaml() -> &'static str {
        r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: foo
spec:
  selector:
    matchLabels:
      app: foo
  template:
    metadata:
      labels:
        app: foo
    spec:
      containers:
      - name: foo
        image: gcr.io/foo
---
apiVersion: v1
kind: Service
metadata:
  name: foo
spec:
  ports:
  - port: 80
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bar
spec:
  selector:
    matchLabels:
      app: bar
  template:
    metadata:
      labels:
        app: bar
    spec:
      containers:
      - name: bar
        image: gcr.io/bar
"#
    }

    #[test]
    fn tilt_compat_workload_to_resource_function_renames_and_groups() {
        let dir = std::env::temp_dir().join(format!("starling-wtrf-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            format!(
                r#"
k8s_yaml("""{yaml}""")
def wtrf(id):
    return "hello-" + id.name
workload_to_resource_function(wtrf)
k8s_resource("hello-foo", labels="ui")
"#,
                yaml = wtrf_fixture_yaml()
            ),
        )
        .unwrap();

        let result = load(&file).unwrap();
        // Two workloads -> hello-foo, hello-bar. The Service named "foo" attaches
        // to its (renamed) workload rather than becoming its own resource.
        let names: Vec<&str> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert!(names.contains(&"hello-foo"), "names: {names:?}");
        assert!(names.contains(&"hello-bar"), "names: {names:?}");
        let foo = result
            .manifests
            .iter()
            .find(|m| m.name == "hello-foo")
            .expect("renamed workload");
        // k8s_resource referencing the new name applied (labels set).
        assert!(foo.labels.contains_key("ui"));
        // The same-named Service rode along with the renamed workload.
        assert!(foo
            .k8s_apply_docs
            .iter()
            .any(|doc| doc.contains("kind: Service")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_workload_to_resource_function_conflict_errors() {
        let dir =
            std::env::temp_dir().join(format!("starling-wtrf-conflict-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            format!(
                r#"
k8s_yaml("""{yaml}""")
def wtrf(id):
    return "baz"
workload_to_resource_function(wtrf)
"#,
                yaml = wtrf_fixture_yaml()
            ),
        )
        .unwrap();

        let err = match load(&file) {
            Ok(_) => panic!("conflicting resource names should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("mapped to resource name 'baz'"), "err: {err}");
        assert!(err.contains("foo:deployment:default:apps"), "err: {err}");
        assert!(err.contains("bar:deployment:default:apps"), "err: {err}");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_workload_to_resource_function_arity_and_return_checks() {
        // Wrong arity is rejected at registration.
        let dir =
            std::env::temp_dir().join(format!("starling-wtrf-arity-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            format!(
                r#"
k8s_yaml("""{yaml}""")
def wtrf():
    return "hello"
workload_to_resource_function(wtrf)
"#,
                yaml = wtrf_fixture_yaml()
            ),
        )
        .unwrap();
        let err = match load(&file) {
            Ok(_) => panic!("zero-arg function should fail"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("must take 1 argument") && err.contains("wtrf takes 0"),
            "err: {err}"
        );
        let _ = fs::remove_dir_all(&dir);

        // Non-string return is rejected when applied to a workload.
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            &file,
            format!(
                r#"
k8s_yaml("""{yaml}""")
def wtrf(id):
    return 1
workload_to_resource_function(wtrf)
"#,
                yaml = wtrf_fixture_yaml()
            ),
        )
        .unwrap();
        let err = match load(&file) {
            Ok(_) => panic!("non-string return should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("invalid return value"), "err: {err}");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_os_environ_and_readiness_probe() {
        let dir =
            std::env::temp_dir().join(format!("starling-tilt-compat-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
port = int(os.environ.get("STARLING_TEST_PORT", "4321"))
local_resource(
    "web",
    serve_cmd="echo serving",
    readiness_probe=probe(
        period_secs=2,
        success_threshold=2,
        failure_threshold=3,
        tcp_socket=tcp_socket_action(port),
    ),
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "web");
        // The probe is parsed onto the manifest, not discarded.
        let probe = result.manifests[0]
            .readiness_probe
            .as_ref()
            .expect("readiness probe parsed");
        assert_eq!(probe.period_secs, 2.0);
        assert_eq!(probe.success_threshold, 2);
        assert_eq!(probe.failure_threshold, 3);
        match &probe.action {
            ProbeAction::Tcp { host, port } => {
                assert_eq!(host, "127.0.0.1");
                assert_eq!(*port, 4321);
            }
            other => panic!("expected tcp probe, got {other:?}"),
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_link_and_exec_action() {
        let dir = std::env::temp_dir().join(format!("starling-link-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
local_resource(
    "web",
    serve_cmd="echo serving",
    links=[link("http://localhost:8080", "litellm"), "http://localhost:9090"],
    readiness_probe=probe(exec=exec_action(["sh", "-c", "true"])),
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        let links = &result.manifests[0].links;
        assert_eq!(
            links,
            &vec![
                ("http://localhost:8080".to_string(), "litellm".to_string()),
                (
                    "http://localhost:9090".to_string(),
                    "http://localhost:9090".to_string()
                ),
            ]
        );
        // The exec probe is parsed onto the manifest.
        match &result.manifests[0]
            .readiness_probe
            .as_ref()
            .expect("readiness probe parsed")
            .action
        {
            ProbeAction::Exec { command } => {
                assert_eq!(
                    command,
                    &vec!["sh".to_string(), "-c".to_string(), "true".to_string()]
                );
            }
            other => panic!("expected exec probe, got {other:?}"),
        }

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn starling_port_records_named_port_and_returns_env_reference() {
        let dir = std::env::temp_dir().join(format!("starling-port-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Starlingfile");
        fs::write(
            &file,
            r#"
pg_port = starling_port("control-plane-postgres", preferred=54330)
local_resource("api", serve_cmd="echo ${STARLING_CONTROL_PLANE_POSTGRES_PORT} " + pg_port)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.port_leases.len(), 1);
        assert_eq!(result.port_leases[0].name, "control-plane-postgres");
        assert_eq!(result.port_leases[0].preferred, Some(54330));
        assert_eq!(
            result.manifests[0].serve_cmd.argv[2],
            "echo ${STARLING_CONTROL_PLANE_POSTGRES_PORT} ${STARLING_CONTROL_PLANE_POSTGRES_PORT}"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_pure_stdlib_helpers() {
        let dir = std::env::temp_dir().join(format!("starling-stdlib-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(dir.join("files/nested")).unwrap();
        fs::write(dir.join("files/a.txt"), "a").unwrap();
        fs::write(dir.join("files/nested/b.txt"), "b").unwrap();
        fs::write(dir.join("app.json"), r#"{"name":"api","port":8080}"#).unwrap();
        fs::write(
            dir.join("app.yaml"),
            r#"
metadata:
  name: app
spec:
  replicas: 2
"#,
        )
        .unwrap();
        fs::write(
            dir.join("stream.yaml"),
            r#"
kind: ConfigMap
metadata:
  name: one
---
kind: Service
metadata:
  name: two
"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
cfg = read_json("app.json")
cfg_default = read_json("missing.json", {"name": "fallback"})
yaml_doc = read_yaml("app.yaml")
yaml_stream = read_yaml_stream("stream.yaml")
decoded = decode_json(encode_json({"name": cfg["name"], "port": cfg["port"]}))
decoded_yaml = decode_yaml(encode_yaml({"metadata": {"name": "roundtrip"}}))
decoded_stream = decode_yaml_stream(encode_yaml_stream([
    {"kind": "ConfigMap", "metadata": {"name": "encoded-one"}},
    {"kind": "Service", "metadata": {"name": "encoded-two"}},
]))
contents = blob("hello")
quoted = shlex.quote("hello world")
joined = os.path.join("files", "nested", "b.txt")
warn("stdlib warning")
config.define_string("mode", usage="test mode")
parsed = config.parse()
local_resource(
    "stdlib-" + decoded["name"] + "-" + cfg_default["name"] + "-" +
        yaml_doc["metadata"]["name"] + "-" + yaml_stream[1]["metadata"]["name"] + "-" +
        decoded_yaml["metadata"]["name"] + "-" + decoded_stream[1]["metadata"]["name"],
    cmd="true",
    deps=listdir("files", recursive=True) + [joined],
    links=[link("http://localhost:8080", quoted), contents],
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        let m = &result.manifests[0];
        assert_eq!(m.name, "stdlib-api-fallback-app-two-roundtrip-encoded-two");
        assert!(m.deps.iter().any(|p| p.ends_with("files/nested/b.txt")));
        assert!(m
            .links
            .iter()
            .any(|(url, name)| url == "http://localhost:8080" && name == "'hello world'"));
        assert!(m
            .links
            .iter()
            .any(|(url, name)| url == "hello" && name == "hello"));
        assert!(result.log.contains("WARNING: stdlib warning"));
        assert!(result.config_files.iter().any(|p| p.ends_with("app.json")));
        assert!(result.config_files.iter().any(|p| p.ends_with("app.yaml")));
        assert!(result
            .config_files
            .iter()
            .any(|p| p.ends_with("stream.yaml")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_local_echo_off_redacts_command() {
        let dir =
            std::env::temp_dir().join(format!("starling-local-echo-off-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
out = local("printf visible-output", echo_off=True)
local_resource("echo-off-" + out, cmd="true")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests[0].name, "echo-off-visible-output");
        assert!(result.log.contains("local: <redacted>"));
        assert!(result.log.contains("visible-output"));
        assert!(!result.log.contains("printf visible-output"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_fail_and_exit_stop_execution() {
        let dir = std::env::temp_dir().join(format!("starling-fail-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let fail_file = dir.join("Tiltfile.fail");
        fs::write(&fail_file, r#"fail("nope")"#).unwrap();
        let fail_err = match load(&fail_file) {
            Ok(_) => panic!("fail() should stop execution"),
            Err(e) => e.to_string(),
        };
        assert!(fail_err.contains("nope"));

        let exit_file = dir.join("Tiltfile.exit");
        fs::write(&exit_file, r#"exit(17)"#).unwrap();
        let exit_err = match load(&exit_file) {
            Ok(_) => panic!("exit() should stop execution"),
            Err(e) => e.to_string(),
        };
        assert!(exit_err.contains("exit(17)"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_config_parse_args_and_file() {
        let dir = std::env::temp_dir().join(format!("starling-config-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("tilt_config.json"),
            r#"{"mode":"file","names":["from-file"],"debug":false}"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
config.define_string("mode")
config.define_string_list("names")
config.define_bool("debug")
config.define_string_list("pos", args=True)
cfg = config.parse()
local_resource(
    "cfg-" + cfg["mode"] + "-" + cfg["names"][0] + "-" + cfg["pos"][0] + "-" + str(cfg["debug"]),
    cmd="true",
)
"#,
        )
        .unwrap();

        let result = load_with_options(
            &file,
            LoadOptions {
                args: vec![
                    "--mode=cli".to_string(),
                    "--names".to_string(),
                    "from-cli".to_string(),
                    "--debug".to_string(),
                    "frontend".to_string(),
                ],
                ..LoadOptions::default()
            },
        )
        .unwrap();

        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "cfg-cli-from-cli-frontend-True");
        assert!(result
            .config_files
            .iter()
            .any(|p| p.ends_with("tilt_config.json")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_config_define_object() {
        let dir =
            std::env::temp_dir().join(format!("starling-config-object-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("tilt_config.json"),
            r#"{"file_payload":{"name":"from-file","port":7000}}"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
config.define_object("payload")
config.define_object("file_payload")
config.define_string("unset")
cfg = config.parse()
local_resource(
    "obj-" + cfg["payload"]["name"] + "-" + str(cfg["payload"]["enabled"]) +
        "-" + str(cfg["payload"]["ports"][0]) + "-" + cfg["file_payload"]["name"] +
        "-" + cfg.get("unset", "missing"),
    cmd="true",
)
"#,
        )
        .unwrap();

        let result = load_with_options(
            &file,
            LoadOptions {
                args: vec![
                    "--payload".to_string(),
                    r#"{"name":"from-cli","enabled":true,"ports":[8080]}"#.to_string(),
                ],
                ..LoadOptions::default()
            },
        )
        .unwrap();

        assert_eq!(result.manifests.len(), 1);
        assert_eq!(
            result.manifests[0].name,
            "obj-from-cli-True-8080-from-file-missing"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_k8s_context_and_namespace() {
        let dir = std::env::temp_dir().join(format!("starling-k8sctx-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
local_resource("ctx-" + k8s_context() + "-" + k8s_namespace(), cmd="true")
"#,
        )
        .unwrap();

        let result = load_with_options(
            &file,
            LoadOptions {
                kube_context: Some("kind-starling".to_string()),
                kube_namespace: Some("dev".to_string()),
                ..LoadOptions::default()
            },
        )
        .unwrap();

        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "ctx-kind-starling-dev");

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_allow_k8s_contexts_is_enforced() {
        let dir = std::env::temp_dir().join(format!("starling-allow-k8s-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
allow_k8s_contexts(["kind-dev"])
local_resource("ok", cmd="true")
"#,
        )
        .unwrap();

        let ok = load_with_options(
            &file,
            LoadOptions {
                kube_context: Some("kind-dev".to_string()),
                ..LoadOptions::default()
            },
        )
        .unwrap();
        assert_eq!(ok.manifests.len(), 1);

        let err = match load_with_options(
            &file,
            LoadOptions {
                kube_context: Some("prod".to_string()),
                ..LoadOptions::default()
            },
        ) {
            Ok(_) => panic!("disallowed context should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("not allowed"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_settings_builtins_are_accepted() {
        let dir = std::env::temp_dir().join(format!("starling-settings-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
enable_feature("example")
disable_feature("other")
disable_snapshots()
docker_prune_settings(disable=True, max_age_mins=30, num_builds=0, keep_recent=1)
analytics_settings(enable=False)
experimental_analytics_report({"team": "dev"})
version_settings(check_updates=False, constraint=">=0.0.0")
secret_settings(disable_scrub=True)
update_settings(max_parallel_updates=1, k8s_upsert_timeout_secs=2, suppress_unused_image_warnings=["unused"], k8s_server_side_apply="auto")
ci_settings(k8s_grace_period="3m", timeout="30m", readiness_timeout="5m")
watch_settings(ignore=["tmp", "*.log"])
set_team("sharks")
local_resource("settings-ok", cmd="true")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "settings-ok");
        // enable_feature/disable_feature now record flags on the result.
        assert!(result
            .feature_flags
            .contains(&("example".to_string(), true)));
        assert!(result.feature_flags.contains(&("other".to_string(), false)));
        assert!(result.manifests[0]
            .ignore_rules
            .iter()
            .any(|rule| rule.pattern == "tmp"));

        let duplicate_team = dir.join("Tiltfile.dup-team");
        fs::write(
            &duplicate_team,
            r#"
set_team("sharks")
set_team("jets")
"#,
        )
        .unwrap();
        let err = match load(&duplicate_team) {
            Ok(_) => panic!("duplicate set_team should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("team_id set multiple times"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_test_builtin_and_local_resource_validation() {
        let dir = std::env::temp_dir().join(format!("starling-test-fn-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
test("unit", "echo hi")
local_resource("serve", serve_cmd="echo serving", serve_dir=".")
local_resource("parallel", cmd="echo parallel", allow_parallel=True)
local_resource("serial", cmd="echo serial")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 4);
        assert_eq!(result.manifests[0].name, "unit");
        assert!(result.manifests[0].is_test);
        assert!(!result.manifests[1].is_test);
        assert!(result.manifests[0].allow_parallel);
        assert!(result.manifests[1].allow_parallel);
        assert!(result.manifests[2].allow_parallel);
        assert!(!result.manifests[3].allow_parallel);
        assert!(result.log.contains("test() is deprecated"));

        let empty_file = dir.join("Tiltfile.empty");
        fs::write(&empty_file, r#"local_resource("empty")"#).unwrap();
        let err = match load(&empty_file) {
            Ok(_) => panic!("empty local_resource should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("must have a cmd and/or a serve_cmd"));

        let duplicate_file = dir.join("Tiltfile.dup");
        fs::write(
            &duplicate_file,
            r#"
local_resource("dup", cmd="true")
test("dup", "true")
"#,
        )
        .unwrap();
        let err = match load(&duplicate_file) {
            Ok(_) => panic!("duplicate local resources should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("local_resource named \"dup\" already exists"));

        let bad_dir_file = dir.join("Tiltfile.bad-dir");
        fs::write(
            &bad_dir_file,
            r#"local_resource("bad", serve_cmd="true", dir=".")"#,
        )
        .unwrap();
        let err = match load(&bad_dir_file) {
            Ok(_) => panic!("dir without cmd should fail"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("'dir' only affects 'cmd'"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_watch_ignore_rules_are_collected() {
        let dir = std::env::temp_dir().join(format!("starling-ignore-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join(".tiltignore"),
            r#"
# comment
ignored-by-tiltignore
"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
watch_settings(ignore=["ignored-by-watch"])
local_resource("api", cmd="true", deps=["."], ignore=["ignored-by-resource"])
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        let patterns: Vec<_> = result.manifests[0]
            .ignore_rules
            .iter()
            .map(|rule| rule.pattern.as_str())
            .collect();
        assert!(patterns.contains(&"ignored-by-resource"));
        assert!(patterns.contains(&"ignored-by-watch"));
        assert!(patterns.contains(&"ignored-by-tiltignore"));
        assert!(result
            .config_files
            .iter()
            .any(|p| p.ends_with(".tiltignore")));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_load_dynamic_returns_exports_and_side_effects() {
        let dir =
            std::env::temp_dir().join(format!("starling-load-dynamic-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("lib.Tiltfile"),
            r#"
name = "api"
answer = 42
values = ["one", "two"]
private = "_hidden"
local_resource("side-effect", cmd="true")
"#,
        )
        .unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
loaded = load_dynamic("lib.Tiltfile")
local_resource(
  loaded["name"] + "-" + str(loaded["answer"]) + "-" + loaded["values"][1],
  cmd="true",
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let names: Vec<_> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["side-effect", "api-42-two"]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_args_select_resources_without_config_parse() {
        let dir =
            std::env::temp_dir().join(format!("starling-resource-args-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
local_resource("db", cmd="true")
local_resource("api", cmd="true", resource_deps=["db"])
local_resource("web", cmd="true")
"#,
        )
        .unwrap();

        let result = load_with_options(
            &file,
            LoadOptions {
                args: vec!["api".to_string()],
                ..LoadOptions::default()
            },
        )
        .unwrap();
        let names: Vec<_> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["db", "api"]);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tilt_compat_config_set_enabled_resources_filters() {
        let dir =
            std::env::temp_dir().join(format!("starling-config-enabled-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("Tiltfile");
        fs::write(
            &file,
            r#"
config.set_enabled_resources(["web"])
local_resource("api", cmd="true")
local_resource("web", cmd="true")
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        let names: Vec<_> = result.manifests.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, vec!["web"]);

        let _ = fs::remove_dir_all(dir);
    }
}

/// Parse and execute the Starlingfile at `path`.
pub fn load(path: &Path) -> Result<LoadResult> {
    load_with_options(path, LoadOptions::default())
}

/// Parse and execute the Starlingfile at `path` with Tiltfile args.
pub fn load_with_options(path: &Path, options: LoadOptions) -> Result<LoadResult> {
    let src = std::fs::read_to_string(path)
        .with_context(|| format!("reading Starlingfile at {}", path.display()))?;
    // `Path::parent` of a bare filename is an empty path; treat that as ".".
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };

    let src = with_compat_prelude(src);
    let ast = AstModule::parse(&path.to_string_lossy(), src, &Dialect::Extended)
        .map_err(|e| anyhow!("parsing Starlingfile: {e}"))?;

    let globals = build_globals();
    let module = Module::new();
    let st = TfState::default();
    st.dir_stack.borrow_mut().push(dir.clone());
    st.config_files.borrow_mut().push(path.to_path_buf());
    let tiltignore = dir.join(".tiltignore");
    st.config_files.borrow_mut().push(tiltignore.clone());
    st.watch_ignores
        .borrow_mut()
        .extend(read_tiltignore(&tiltignore)?);
    *st.args.borrow_mut() = options.args;
    *st.kube_context.borrow_mut() = options.kube_context;
    *st.kube_namespace.borrow_mut() = options.kube_namespace;

    let printer = LogPrint(&st.log);
    let loader = StarlingLoader {
        st: &st,
        globals: &globals,
    };

    {
        let mut eval = Evaluator::new(&module);
        eval.extra = Some(&st);
        eval.set_loader(&loader);
        eval.set_print_handler(&printer);
        eval.eval_module(ast, &globals)
            .map_err(|e| anyhow!("executing Starlingfile: {e}"))?;
        // Resource naming runs while the evaluator/module are still alive so the
        // registered workload_to_resource_function can be invoked.
        apply_workload_to_resource_function(&st, &mut eval)?;
    }
    validate_custom_build_image_deps(&st)?;
    enforce_allowed_kube_contexts(&st)?;

    // Local resources, then assembled k8s resources.
    let mut manifests = st.local_manifests.borrow().clone();
    apply_dc_resource_configs(&st, &mut manifests)?;
    manifests.extend(assemble_k8s(&st));
    let watch_ignores = st.watch_ignores.borrow().clone();
    if !watch_ignores.is_empty() {
        for manifest in &mut manifests {
            manifest.ignore_rules.extend(watch_ignores.clone());
        }
    }
    let manifests = filter_enabled_manifests(&st, manifests)?;
    validate_unique_manifest_names(&manifests)?;

    let aliases = st.aliases.borrow().clone();
    let port_leases = st.port_leases.borrow().clone();
    let log = st.log.borrow().clone();
    let mut config_files = st.config_files.borrow().clone();
    config_files.sort();
    config_files.dedup();
    let ci_timeout_secs = *st.ci_timeout_secs.borrow();
    let max_parallel_updates = *st.max_parallel_updates.borrow();
    let team_id = st.team_id.borrow().clone();
    let feature_flags: Vec<(String, bool)> = st
        .feature_flags
        .borrow()
        .iter()
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let extension_repos: Vec<(String, String)> = st
        .extension_repos
        .borrow()
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let extensions = st.loaded_extensions.borrow().clone();
    let secret_values = if *st.disable_scrub.borrow() {
        Vec::new()
    } else {
        collect_secret_values(&st.k8s_entities.borrow())
    };
    Ok(LoadResult {
        manifests,
        aliases,
        port_leases,
        log,
        config_dir: dir,
        config_files,
        ci_timeout_secs,
        max_parallel_updates,
        team_id,
        feature_flags,
        extension_repos,
        extensions,
        secret_values,
    })
}
