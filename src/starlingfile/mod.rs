//! Starlingfile execution: parse and run a Starlingfile with the Starlark interpreter,
//! producing a set of [`Manifest`]s.
//!
//! Mirrors the role of Go's `internal/tiltfile`. Implements a pragmatic subset
//! of Tilt's builtins: `local_resource` (fully executed), plus `docker_build` /
//! `k8s_yaml` / `k8s_resource` which drive real `docker build` + `kubectl apply`
//! via the engine.

pub mod manifest;

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use starlark::any::ProvidesStaticType;
use starlark::environment::{FrozenModule, Globals, GlobalsBuilder, LibraryExtension, Module};
use starlark::eval::{Evaluator, FileLoader};
use starlark::syntax::{AstModule, Dialect};
use starlark::values::list::ListRef;
use starlark::values::none::NoneType;
use starlark::values::Value;

pub use manifest::{Cmd, DockerBuild, LiveUpdateStep, Manifest, NamedPortLease, TargetKind};

/// Separator for encoding live_update steps as strings returned by sync()/run().
const LU_SEP: char = '\u{1}';

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
fn parse_live_update(v: Value, dir: &Path) -> Vec<LiveUpdateStep> {
    as_str_vec(v)
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
        .collect()
}

use crate::k8s::{self, K8sEntity};

const COMPAT_PRELUDE: &str = r#"
os = struct(environ = struct(get = _starling_getenv))
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
}

/// The directory of a file path, or "." if it has no parent.
fn parent_or_dot(path: &Path) -> PathBuf {
    match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
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
        let target = resolve(&self.st.cur_dir(), path);
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
    port_forwards: Vec<String>,
    links: Vec<String>,
    resource_deps: Vec<String>,
    trigger_mode: Option<i32>,
    auto_init: bool,
    labels: Vec<(String, String)>,
    extra_pod_selectors: Vec<(String, String)>,
    objects: Vec<String>,
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
    k8s_entities: RefCell<Vec<K8sEntity>>,
    k8s_configs: RefCell<Vec<K8sResourceConfig>>,
    /// Static proxy routes registered via `alias(name, port)`.
    aliases: RefCell<Vec<(String, u16)>>,
    /// Named host TCP ports requested via `starling_port(...)`.
    port_leases: RefCell<Vec<NamedPortLease>>,
    log: RefCell<String>,
}

impl TfState {
    fn logln(&self, msg: &str) {
        let mut log = self.log.borrow_mut();
        log.push_str(msg);
        if !msg.ends_with('\n') {
            log.push('\n');
        }
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

/// Convert a Starlark value into a list of strings (string → single element).
fn as_str_vec(v: Value) -> Vec<String> {
    if let Some(s) = v.unpack_str() {
        return vec![s.to_string()];
    }
    if let Some(list) = ListRef::from_value(v) {
        return list
            .iter()
            .map(|e| {
                e.unpack_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| e.to_str())
            })
            .collect();
    }
    vec![]
}

/// Build a [`Cmd`] from a Starlark value: a string runs through `sh -c`,
/// a list is taken as an explicit argv.
fn as_cmd(v: Value, workdir: &Path) -> Cmd {
    let mut cmd = Cmd {
        workdir: Some(workdir.to_path_buf()),
        ..Default::default()
    };
    if let Some(s) = v.unpack_str() {
        if !s.trim().is_empty() {
            cmd.argv = vec!["sh".into(), "-c".into(), s.to_string()];
        }
    } else {
        cmd.argv = as_str_vec(v);
    }
    cmd
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

/// Parse a port-forward spec into a `(url, name)` link, matching Tilt's forms:
/// `"8000"` (local only), `"8000:9000"` (local:container),
/// `"host:8000:9000"` (host:local:container).
fn port_forward_link(pf: &str) -> (String, String) {
    let parts: Vec<&str> = pf.split(':').collect();
    let (host, local) = match parts.as_slice() {
        [local] => ("localhost".to_string(), local.to_string()),
        [local, _container] => ("localhost".to_string(), local.to_string()),
        [host, local, _container] => {
            let h = if host.is_empty() { "localhost" } else { host };
            (h.to_string(), local.to_string())
        }
        _ => ("localhost".to_string(), pf.to_string()),
    };
    (format!("http://{host}:{local}"), format!("port {local}"))
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

    /// Compatibility shim for Tilt readiness probes. Starling accepts but does
    /// not yet execute readiness probes for local resources.
    fn probe<'v>(
        #[starlark(require = named, default = NoneType)] period_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] timeout_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] initial_delay_secs: Value<'v>,
        #[starlark(require = named, default = NoneType)] tcp_socket: Value<'v>,
        #[starlark(require = named, default = NoneType)] http_get: Value<'v>,
    ) -> anyhow::Result<NoneType> {
        let _ = (
            period_secs,
            timeout_secs,
            initial_delay_secs,
            tcp_socket,
            http_get,
        );
        Ok(NoneType)
    }

    /// Compatibility shim for `probe(tcp_socket=tcp_socket_action(...))`.
    fn tcp_socket_action<'v>(
        port: Value<'v>,
        #[starlark(require = named, default = NoneType)] host: Value<'v>,
    ) -> anyhow::Result<NoneType> {
        let _ = (port, host);
        Ok(NoneType)
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
        let _ = (
            allow_parallel,
            ignore,
            cmd_bat,
            serve_cmd_bat,
            readiness_probe,
        );
        let st = state(eval);
        let base = st.cur_dir();
        let mut m = Manifest::new(name, TargetKind::Local);
        m.trigger_mode = model_trigger_mode(trigger_mode, auto_init);
        m.auto_init = auto_init;
        if let Some(p) = serve_port.unpack_i32() {
            if p > 0 && p < 65536 {
                m.serve_port = Some(p as u16);
            }
        }
        let cmd_workdir = dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        let serve_workdir = serve_dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        if !cmd.is_none() {
            m.update_cmd = as_cmd(cmd, &cmd_workdir);
            m.update_cmd.env = parse_build_args(env);
        }
        if !serve_cmd.is_none() {
            m.serve_cmd = as_cmd(serve_cmd, &serve_workdir);
            m.serve_cmd.env = parse_build_args(serve_env);
        }
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
            for url in as_str_vec(links) {
                m.links.push((url.clone(), url));
            }
        }
        if !labels.is_none() {
            m.labels = parse_build_args(labels).into_iter().collect();
        }
        st.local_manifests.borrow_mut().push(m);
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
        eval: &mut Evaluator,
    ) -> anyhow::Result<String> {
        use std::io::Write;
        use std::process::Stdio;
        let _ = (command_bat, echo_off);
        let st = state(eval);
        let base = st.cur_dir();
        let workdir = dir.unpack_str().map(|d| resolve(&base, d)).unwrap_or(base);
        let cmd = as_cmd(command, &workdir);
        if cmd.is_empty() {
            return Ok(String::new());
        }
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
            .with_context(|| format!("local: running {}", cmd.display()))?;
        if let (Some(data), Some(mut sin)) = (stdin_data, child.stdin.take()) {
            let _ = sin.write_all(data.as_bytes());
        }
        let out = child
            .wait_with_output()
            .with_context(|| format!("local: running {}", cmd.display()))?;
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        if !quiet {
            st.logln(&format!("local: {}", cmd.display()));
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
        Ok(stdout)
    }

    /// Render a kustomization and return its YAML (Tilt's `kustomize`).
    /// Tries `kustomize build`, falling back to `kubectl kustomize`.
    fn kustomize<'v>(
        paths: String,
        #[starlark(require = named, default = NoneType)] kustomize_bin: Value<'v>,
        #[starlark(require = named, default = NoneType)] flags: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<String> {
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
        match run_capture(&argv, &dir) {
            Ok(s) => Ok(s),
            Err(_) => {
                // Fall back to `kubectl kustomize`.
                let mut a2 = vec![
                    "kubectl".to_string(),
                    "kustomize".to_string(),
                    p.display().to_string(),
                ];
                a2.extend(extra);
                run_capture(&a2, &dir)
            }
        }
    }

    /// Render a Helm chart and return its YAML (Tilt's `helm`).
    fn helm<'v>(
        paths: String,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] namespace: Value<'v>,
        #[starlark(require = named, default = NoneType)] values: Value<'v>,
        #[starlark(require = named, default = NoneType)] set: Value<'v>,
        #[starlark(require = named, default = false)] skip_crds: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<String> {
        let st = state(eval);
        let dir = st.cur_dir();
        let p = resolve(&dir, &paths);
        st.config_files.borrow_mut().push(p.clone());
        let release = name.unpack_str().unwrap_or("chart").to_string();
        let mut argv = vec![
            "helm".to_string(),
            "template".to_string(),
            release,
            p.display().to_string(),
        ];
        if let Some(ns) = namespace.unpack_str() {
            argv.push("--namespace".to_string());
            argv.push(ns.to_string());
        }
        for v in as_str_vec(values) {
            argv.push("--values".to_string());
            argv.push(resolve(&dir, &v).display().to_string());
        }
        for s in as_str_vec(set) {
            argv.push("--set".to_string());
            argv.push(s);
        }
        if !skip_crds {
            argv.push("--include-crds".to_string());
        }
        run_capture(&argv, &dir)
    }

    /// Read a file relative to the Starlingfile, returning `default` if missing.
    /// The path is tracked so edits trigger a reload.
    fn read_file<'v>(
        path: String,
        #[starlark(default = NoneType)] default: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<String> {
        let st = state(eval);
        let p = resolve(&st.cur_dir(), &path);
        st.config_files.borrow_mut().push(p.clone());
        match std::fs::read_to_string(&p) {
            Ok(s) => Ok(s),
            Err(_) if !default.is_none() => Ok(default.to_str()),
            Err(e) => Err(anyhow!("read_file({path:?}): {e}")),
        }
    }

    /// Like `include()`, but returns the loaded module's exported symbols as a
    /// dict (Tilt's `load_dynamic`). Symbol introspection across heaps isn't
    /// supported yet, so the returned dict is currently empty; side effects
    /// (resource registrations) still run. Prefer `load()` for importing symbols.
    fn load_dynamic<'v>(
        path: String,
        eval: &mut Evaluator<'v, '_>,
    ) -> anyhow::Result<starlark::values::Value<'v>> {
        run_starlingfile_into(&path, eval)?;
        Ok(eval.heap().alloc(starlark::values::dict::AllocDict(Vec::<(
            starlark::values::Value,
            starlark::values::Value,
        )>::new())))
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
    ) -> anyhow::Result<(String, String)> {
        let content = as_str_vec(yaml).join("\n---\n");
        let want_labels = parse_build_args(labels);
        let (mut matching, mut rest) = (Vec::new(), Vec::new());
        for e in k8s::parse_yaml(&content) {
            let ok = kind
                .unpack_str()
                .map_or(true, |k| e.kind.eq_ignore_ascii_case(k))
                && name.unpack_str().map_or(true, |n| e.name == n)
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
        Ok((matching.join("---\n"), rest.join("---\n")))
    }

    /// Accepted for CRD image extraction (Tilt's `k8s_kind`/`k8s_image_json_path`).
    /// Recorded as no-ops; image injection into CRDs isn't implemented yet.
    fn k8s_kind<'v>(
        _kind: String,
        #[starlark(require = named, default = NoneType)] image_json_path: Value<'v>,
        #[starlark(require = named, default = NoneType)] api_version: Value<'v>,
        #[starlark(require = named, default = NoneType)] image_object: Value<'v>,
    ) -> anyhow::Result<NoneType> {
        let _ = (image_json_path, api_version, image_object);
        Ok(NoneType)
    }
    fn k8s_image_json_path<'v>(
        _paths: Value<'v>,
        #[starlark(require = named, default = NoneType)] kind: Value<'v>,
        #[starlark(require = named, default = NoneType)] name: Value<'v>,
        #[starlark(require = named, default = NoneType)] namespace: Value<'v>,
        #[starlark(require = named, default = NoneType)] api_version: Value<'v>,
    ) -> anyhow::Result<NoneType> {
        let _ = (kind, name, namespace, api_version);
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
            st.k8s_entities.borrow_mut().extend(entities);
        }
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
        // Accepted for Tiltfile compatibility (not all wired to the builder yet).
        #[starlark(require = named, default = NoneType)] platform: Value<'v>,
        #[starlark(require = named, default = NoneType)] ignore: Value<'v>,
        #[starlark(require = named, default = NoneType)] only: Value<'v>,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = (dockerfile_contents, platform, ignore, only);
        let st = state(eval);
        let dir = st.cur_dir();
        let df = dockerfile.unpack_str().map(|d| resolve(&dir, d));
        let lu = if live_update.is_none() {
            vec![]
        } else {
            parse_live_update(live_update, &dir)
        };
        let build_args = parse_build_args(build_args);
        st.docker_builds.borrow_mut().push(DockerBuild {
            image_ref: r#ref,
            context: resolve(&dir, &context),
            dockerfile: df,
            target: target.unpack_str().map(str::to_string),
            build_args,
            command: None,
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
        #[starlark(require = named, default = NoneType)] live_update: Value<'v>,
        #[starlark(require = named, default = NoneType)] dir: Value<'v>,
        #[starlark(require = named, default = NoneType)] env: Value<'v>,
        // Accepted for compatibility.
        #[starlark(require = named, default = NoneType)] tag: Value<'v>,
        #[starlark(require = named, default = false)] disable_push: bool,
        #[starlark(require = named, default = false)] skips_local_docker: bool,
        eval: &mut Evaluator,
    ) -> anyhow::Result<NoneType> {
        let _ = (tag, disable_push, skips_local_docker);
        let st = state(eval);
        let base = st.cur_dir();
        let workdir = dir
            .unpack_str()
            .map(|d| resolve(&base, d))
            .unwrap_or_else(|| base.clone());
        let mut cmd = as_cmd(command, &workdir);
        cmd.env = parse_build_args(env);
        let dep_paths = as_str_vec(deps)
            .into_iter()
            .map(|d| resolve(&base, &d))
            .collect();
        let lu = if live_update.is_none() {
            vec![]
        } else {
            parse_live_update(live_update, &base)
        };
        st.docker_builds.borrow_mut().push(DockerBuild {
            image_ref: r#ref,
            context: base,
            dockerfile: None,
            target: None,
            build_args: vec![],
            command: Some(cmd),
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
        let _ = (trigger, echo_off);
        Ok(format!("run{LU_SEP}{cmd}"))
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
        let _ = (pod_readiness, discovery_strategy);
        let st = state(eval);
        let mut cfg = K8sResourceConfig {
            workload: workload.unpack_str().unwrap_or("").to_string(),
            new_name: new_name.unpack_str().map(str::to_string),
            trigger_mode,
            auto_init,
            ..Default::default()
        };
        if !port_forwards.is_none() {
            // Accept an int, a string, a port_forward() spec, or a list of these.
            cfg.port_forwards = match port_forwards.unpack_i32() {
                Some(i) => vec![i.to_string()],
                None => as_str_vec(port_forwards),
            };
        }
        if !links.is_none() {
            cfg.links = as_str_vec(links);
        }
        if !resource_deps.is_none() {
            cfg.resource_deps = as_str_vec(resource_deps);
        }
        if !labels.is_none() {
            cfg.labels = parse_build_args(labels);
        }
        if !extra_pod_selectors.is_none() {
            cfg.extra_pod_selectors = parse_build_args(extra_pod_selectors);
        }
        if !objects.is_none() {
            cfg.objects = as_str_vec(objects);
        }
        st.k8s_configs.borrow_mut().push(cfg);
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
        let _ = (name, link_path); // accepted for compatibility; not yet surfaced
        let container = container_port.unpack_i32().unwrap_or(local_port);
        Ok(match host.unpack_str() {
            Some(h) => format!("{h}:{local_port}:{container}"),
            None => format!("{local_port}:{container}"),
        })
    }

    /// Run a Docker Compose project: each service becomes a resource whose
    /// serve_cmd is `docker compose up <service>` (builds, starts, streams logs).
    fn docker_compose(path: String, eval: &mut Evaluator) -> anyhow::Result<NoneType> {
        let st = state(eval);
        let dir = st.cur_dir();
        let p = resolve(&dir, &path);
        st.config_files.borrow_mut().push(p.clone());
        let content = std::fs::read_to_string(&p)
            .with_context(|| format!("docker_compose: reading {}", p.display()))?;
        let doc: serde_yaml::Value = serde_yaml::from_str(&content)
            .with_context(|| format!("docker_compose: parsing {}", p.display()))?;
        let abs = p.display().to_string();
        let services = doc.get("services").and_then(|s| s.as_mapping());
        let Some(services) = services else {
            return Err(anyhow!("docker_compose({path:?}): no services found"));
        };
        for (name, _) in services {
            let Some(svc) = name.as_str() else { continue };
            let mut m = Manifest::new(svc, TargetKind::DockerCompose);
            m.serve_cmd = Cmd {
                argv: vec![
                    "docker".into(),
                    "compose".into(),
                    "-f".into(),
                    abs.clone(),
                    "up".into(),
                    svc.to_string(),
                ],
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

    /// Accepted no-ops (config that doesn't affect local execution yet).
    fn default_registry<'v>(
        _host: String,
        #[starlark(require = named, default = NoneType)] host_from_cluster: Value<'v>,
        #[starlark(require = named, default = NoneType)] single_name: Value<'v>,
    ) -> anyhow::Result<NoneType> {
        let _ = (host_from_cluster, single_name);
        Ok(NoneType)
    }
    fn allow_k8s_contexts<'v>(_contexts: Value<'v>) -> anyhow::Result<NoneType> {
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

/// Evaluate another Starlingfile into the current shared state (for `include`
/// and `load_dynamic`): its resource registrations etc. take effect, relative
/// paths resolve against its own directory, and it's tracked for reload.
fn run_starlingfile_into(path: &str, eval: &mut Evaluator) -> Result<()> {
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
    Ok(())
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

fn resolve(dir: &Path, p: &str) -> PathBuf {
    let pb = PathBuf::from(p);
    if pb.is_absolute() {
        pb
    } else {
        dir.join(pb)
    }
}

/// Assemble k8s entities + configs + docker builds into k8s manifests.
fn assemble_k8s(st: &TfState) -> Vec<Manifest> {
    let entities = st.k8s_entities.borrow();
    let docker_builds = st.docker_builds.borrow();
    let mut manifests: Vec<Manifest> = vec![];

    // One manifest per workload.
    for e in entities.iter().filter(|e| e.is_workload()) {
        let mut m = Manifest::new(e.name.clone(), TargetKind::Kubernetes);
        m.k8s_apply_docs.push(e.raw.clone());
        m.k8s_workload = Some(format!("{}/{}", e.kind, e.name));
        m.pod_selector = e.match_labels.clone();
        // Match docker builds to this workload's images.
        for img in &e.images {
            for db in docker_builds.iter() {
                if image_matches(img, &db.image_ref)
                    && !m.docker_builds.iter().any(|d| d.image_ref == db.image_ref)
                {
                    // Inherit live_update + watch its sync sources for changes.
                    for step in &db.live_update {
                        if let LiveUpdateStep::Sync { local, .. } = step {
                            m.deps.push(PathBuf::from(local));
                        }
                    }
                    // custom_build deps trigger image rebuilds.
                    m.deps.extend(db.deps.clone());
                    m.live_update.extend(db.live_update.clone());
                    m.docker_builds.push(db.clone());
                }
            }
        }
        manifests.push(m);
    }

    // Attach non-workload docs (Service/ConfigMap/...) to the workload of the
    // same name, else to the first workload, else as their own resource.
    for e in entities.iter().filter(|e| !e.is_workload()) {
        if let Some(m) = manifests.iter_mut().find(|m| m.name == e.name) {
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
    for cfg in st.k8s_configs.borrow().iter() {
        if let Some(m) = manifests.iter_mut().find(|m| m.name == cfg.workload) {
            for pf in &cfg.port_forwards {
                m.links.push(port_forward_link(pf));
            }
            for url in &cfg.links {
                m.links.push((url.clone(), url.clone()));
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
            // Attach explicitly-grouped bare objects' docs to this resource.
            for obj in &cfg.objects {
                let obj_name = obj.split(':').next().unwrap_or(obj);
                if let Some(e) = entities
                    .iter()
                    .find(|e| !e.is_workload() && e.name == obj_name)
                {
                    m.k8s_apply_docs.push(e.raw.clone());
                }
            }
            if let Some(nn) = &cfg.new_name {
                m.name = nn.clone();
            }
        }
    }

    manifests
}

#[cfg(test)]
mod tests {
    use super::{image_matches, image_repo, load};
    use std::fs;

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
    readiness_probe=probe(period_secs=2, tcp_socket=tcp_socket_action(port)),
)
"#,
        )
        .unwrap();

        let result = load(&file).unwrap();
        assert_eq!(result.manifests.len(), 1);
        assert_eq!(result.manifests[0].name, "web");

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
}

/// Parse and execute the Starlingfile at `path`.
pub fn load(path: &Path) -> Result<LoadResult> {
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
    }

    // Local resources, then assembled k8s resources.
    let mut manifests = st.local_manifests.borrow().clone();
    manifests.extend(assemble_k8s(&st));

    let aliases = st.aliases.borrow().clone();
    let port_leases = st.port_leases.borrow().clone();
    let log = st.log.borrow().clone();
    let mut config_files = st.config_files.borrow().clone();
    config_files.sort();
    config_files.dedup();
    Ok(LoadResult {
        manifests,
        aliases,
        port_leases,
        log,
        config_dir: dir,
        config_files,
    })
}
