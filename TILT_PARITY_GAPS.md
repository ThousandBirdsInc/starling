# Starling vs Tilt Parity Gaps

This document compares the current Starling implementation in this repo against
the reference Tilt tree at `/Users/coltonpierson/workspace/reference/tilt`.

Scope checked:

- Starling CLI, daemon, server, store, engine, Kubernetes parser, and
  Starlingfile implementation under `src/`.
- Tilt CLI under `internal/cli`.
- Tiltfile API stubs under `internal/tiltfile/api` and implementation under
  `internal/tiltfile`.
- Tilt core API types under `pkg/apis/core/v1alpha1`.
- Tilt controllers/engine/store/HUD server directories under `internal/`.

The short version: Starling currently covers a useful subset of Tilt's inner
loop, plus Starling-specific daemon/TUI/named URL features. It is not close to
complete Tilt parity yet. The largest gaps are Tilt's Kubernetes-style API
server/object model, full Tiltfile standard library, image build/injection
pipeline, Kubernetes discovery/port-forward/live-update controllers, Docker
Compose fidelity, CI mode, and the broad Tilt CLI surface.

## Current Starling Coverage

Starling has working support for:

- CLI commands: `up`, `down`, `status`, `logs`, `skills`, `daemon`, `dash`,
  `trust`, `hosts`.
- A central daemon with a Unix-socket JSON protocol, central port allocation,
  route registry, per-instance state, recent logs, and dashboard commands.
- A shared named-URL proxy, including route registration, HTTP proxying,
  WebSocket/streaming, optional TLS, and Starling-specific `alias()`,
  `serve_port`, and `starling_port()`.
- Optional legacy Tilt web UI served from `starling up --web`, with the main
  `/api/view`, `/ws/view`, trigger, trigger-mode, snapshot, analytics no-op,
  and UIButton status routes.
- Starlingfile execution with a subset of Tiltfile globals:
  `local_resource`, `local`, `read_file`, `watch_file`, `include`, Starlark
  `load`, `load_dynamic`, `docker_build`, `custom_build`, `k8s_yaml`,
  `k8s_resource`, `k8s_custom_deploy`, `filter_yaml`, `kustomize`, `helm`, `docker_compose`,
  `port_forward`, `link`, `sync`, `run`, `fall_back_on`, `restart_container`,
  `initial_sync`, `probe`, `exec_action`, `http_get_action`,
  `tcp_socket_action`, `default_registry`, `allow_k8s_contexts`, `k8s_kind`,
  `k8s_image_json_path`, `k8s_context`, `k8s_namespace`, `alias`, and
  `starling_port`, `dc_resource`, plus settings-style builtins such as
  `enable_feature`, `disable_feature`, `disable_snapshots`,
  `docker_prune_settings`, `analytics_settings`, `version_settings`,
  `secret_settings`, `update_settings`, `ci_settings`, `watch_settings`, and
  `set_team`, plus deprecated `test(...)` resources.
- Local resource update and serve commands, file watching for declared deps,
  manual trigger modes, basic disable/pause, resource deps for initial ordering,
  readiness probes for `serve_cmd`, and environment injection for known
  Starling services.
- Basic Docker image builds via Bollard, `custom_build` via subprocess,
  `kubectl apply`, kind image loading, pod polling, `kubectl logs -f`, and a
  simplified Kubernetes live update path using `kubectl cp` / `kubectl exec`.

## Top-Level Gaps

### 1. Tilt's API Server Model Is Mostly Missing

Tilt is built around a local Kubernetes-style API server with versioned objects,
controllers, reducers, watchers, trigger queues, and CLI CRUD operations.
Starling currently has an in-memory `Store` that materializes only the objects
needed by the web UI and daemon.

A first piece of this now exists: `src/api/store.rs` is a generic in-memory API
object store with `create`/`get`/`list`/`replace`/`apply`/`patch`/`delete`,
monotonic `resourceVersion` bumping, `uid` assignment, metadata stamping, and a
broadcast watch stream of add/modify/delete events. The engine populates it (and
reconciles it to the desired set on reload) with sixteen always-on object types:
a `Tiltfile` and `Session` singleton, plus `KubernetesApply`,
`KubernetesDiscovery`, `PodLogStream`, `FileWatch`, `Cmd`, `PortForward`,
`LiveUpdate`, `DockerImage`, `ImageMap`, `CmdImage`, `DockerComposeService`,
`DockerComposeLogStream`, `ToggleButton`, and `ConfigMap` (one per resource as
its disable source); plus `ExtensionRepo`/`Extension` when local extension repos
are used (see section 3). That covers all 18 of Tilt's core `v1alpha1` types.
The web server exposes full CRUD + watch over it (see section 11). What remains
is reconcilers that act on the objects (rather than just mirroring engine
state).

Starling only models enough of these frontend-facing types to render:

- `UISession`
- `UIResource`
- `UIButton`
- `Cluster`
- Shared status/link/button/input structs.

Consequences:

- A generic in-memory object store exists with full CRUD + watch; sixteen object
  types are populated (the list above), leaving only `Extension`/`ExtensionRepo`
  (which Starling has no backing feature for). The objects are descriptive
  snapshots, not yet driven by reconcilers.
- `get`/`list`/`watch`/`create`/`replace`/`patch`/`delete` are all exposed over
  HTTP.
- A small in-process reconciler framework exists (a `Reconciler` trait + a
  registry that dispatches every object-store watch event to registered
  reconcilers): `ForceTriggerReconciler` (`tilt.dev/force-trigger` → build) and
  `DisableConfigMapReconciler` (`{name}-disable` ConfigMap → toggle disable).
- Three **cluster-backed reconcilers** exist and are verified against a real kind
  cluster (section 14): `reconcile_kubernetes_apply` (applies a `KubernetesApply`
  object's `spec.yaml`, writes apply status), `reconcile_kubernetes_discovery`
  (lists pods for a `KubernetesDiscovery` selector, writes ready/total + runtime),
  and `reconcile_pod_log_stream` (fetches a `PodLogStream` selector's logs into
  the store, records line count). All are object-driven (the controller pattern)
  and exposed at `POST /api/v1alpha1/:kind/:name/reconcile`. For **real (non
  dry-run) deploys the engine now routes its apply through the apply reconciler**
  — it publishes the final post-build YAML to the `KubernetesApply` object and
  `reconcile_kubernetes_apply` performs the apply (verified by the kind e2e test).
  Dry-run still applies inline (offline). A `reconcile_port_forward` controller
  resolves the target pod for a `PortForward` object's selector onto its status,
  and the **controller manager now also owns the long-running forward
  *process*** for each `PortForward` object: it starts a persistent
  `kubectl port-forward` when a target first resolves, restarts it when the
  target pod changes, and tears it down when the object is deleted (the process
  lifecycle moved into the controller). Verified against kind (section 14) by
  making a real HTTP request through the forwarded port and confirming it stops
  after the object is deleted. (The engine's per-resource pod watcher still
  drives forwards declared directly on a `k8s_resource`.)
- A **pod-watch controller** (`reconcile_pod_watch`) writes the detailed
  per-pod records to a `KubernetesDiscovery` object's `status.pods[]` — name,
  namespace, phase, readiness, restart count, and per-container name/ready/image
  — the stateful per-pod tracking the dashboard and live-update build on (vs. the
  aggregate ready/total that `reconcile_kubernetes_discovery` writes). Idempotent
  (replaces the list each run), exposed at `POST …/PodWatch/:name/reconcile`, and
  verified against kind (section 14).
- A **live-update controller** (`reconcile_live_update`) resolves the target pod
  for a `LiveUpdate` object's selector and applies its spec to the running
  container: each sync via `kubectl cp localPath pod:containerPath`, each exec via
  `kubectl exec pod -- sh -c <args>`, recording `podName`/`lastExecTime` (or
  `failed`/`message`) on the object's status. It is the object-driven one-shot
  apply of a live-update spec — distinct from, and complementary to, the engine's
  file-watch-triggered `live_update` path. Exposed at
  `POST …/LiveUpdate/:name/reconcile` and verified against kind (section 14): the
  test syncs a file in and execs a command, then independently `kubectl exec`s
  into the container to confirm the change landed. It is intentionally **not** in
  the maintained loop — it mutates the container, so it is not idempotent for
  continuous re-runs.
- A **maintained-controller loop** exists (`spawn_controller_manager`): a
  background task spawned by the engine on real (non dry-run) `up` that
  continuously reconciles the idempotent status controllers — discovery
  aggregate, pod-watch (per-pod detail), and port-forward target resolution — on
  a 5s interval, so their objects' status stays converged with the cluster
  without an external reconcile call. This is the **continuous-reconciliation**
  model (vs. the one-shot `POST …/reconcile` endpoint). Verified against kind
  (section 14): the loop converges a `KubernetesDiscovery` object's `totalPods`
  with no manual reconcile call. (Pod-log is excluded from the loop — it appends,
  so it is not idempotent for continuous runs.)
- An in-process **kube-rs transport** (`src/kube_client.rs`, built on `kube` +
  `k8s-openapi`) is the typed alternative to shelling out to `kubectl` for pod
  listing. The pod-listing reconcilers (discovery, pod-watch, port-forward and
  live-update target resolution) route through `list_pods_for_selector`, which
  uses the typed client when `STARLING_KUBE_RS=1` and `kubectl` otherwise. Both
  transports return the same Kubernetes JSON shape, so the status pipeline is
  transport-agnostic; a gated integration test (section 14) deploys a pod and
  asserts the two transports agree on pod count and resolved name, and that the
  discovery reconciler converges via the typed client. The kube-rs transport also
  covers the **write/exec path** for live-update: `kube_client::exec` runs a
  command via the WebSocket **attach API** (the typed equivalent of `kubectl
  exec`) and `kube_client::copy_file` streams a local file to the container's
  stdin via the same API (the equivalent of `kubectl cp`). Under
  `STARLING_KUBE_RS=1` the live-update reconciler's sync+exec route through these
  instead of shelling out, verified against kind (section 14) by reading the
  synced+exec'd files back out of the container. The transport also covers
  **apply** (`kube_client::apply_yaml` — server-side apply of each document as a
  `DynamicObject`, with the GVK resolved against live API discovery to pick the
  namespaced/cluster-scoped resource) and **pod logs**
  (`kube_client::pod_logs` via the typed `Api::logs`); under `STARLING_KUBE_RS=1`
  the apply and pod-log reconcilers route through these, verified against kind
  (section 14) by applying a pod and reading its logs through the typed path.
  The **persistent `-f` log stream** is also typed: `kube_client::log_stream`
  follows a pod's logs via `Api::log_stream` (converted to a tokio reader with
  `tokio_util`'s `.compat()`), and `stream_pod_logs` uses it under
  `STARLING_KUBE_RS=1`, verified against kind (section 14) by following a pod that
  emits a line per second and asserting the streamed lines arrive over time.
  Port-forward is also native: `kube_client::port_forward_listener` binds a local
  TCP listener and proxies each connection to the pod over the typed
  `Api::portforward` streamed channel (`copy_bidirectional`), so under
  `STARLING_KUBE_RS=1` `stream_port_forward` runs no `kubectl` child. Verified
  against kind (section 14) by serving HTTP through the forwarded port.
  `kubectl` remains the default transport; with the kube-rs path enabled the
  **entire Kubernetes surface — list, apply, exec, cp, one-shot logs, the follow
  log stream, and port-forward — runs through the in-process typed client with no
  per-operation shell-out**.
- No persistent status history beyond the current in-memory process.
- Kubernetes-style API discovery is served at `GET /proxy/apis/tilt.dev/v1alpha1`
  (an `APIResourceList` of the group's kinds with their plural names + verbs).
  Per-kind object schemas are served at
  `GET /proxy/apis/tilt.dev/v1alpha1/:kind/schema`, a full generated OpenAPI 3.0
  document at `GET /openapi.json` (also `starling dump openapi`), and
  `starling explain <kind>` lists a kind's spec fields. Spec fields are typed
  (string/array/boolean/object); field descriptions and request schema
  validation are still missing.
- No typed API object constructors in Tiltfile for most `v1alpha1` resources.

### 2. CLI Parity Is Very Incomplete

Starling's CLI is intentionally different and daemon-first. Tilt has many CLI
commands that Starling does not implement.

Tilt top-level commands missing in Starling:

- `docker`
- `verify-install`
- `docker-prune`
- `edit`
- `delete`
- `apply`
- `create`
- `patch`
- `wait`
- `demo`
- `analytics`
- `alpha`
- `lsp`

Tilt subcommands missing under those commands include:

- `create filewatch`
- `create cmd`
- `create repo`
- `create ext`
- `alpha tiltfile-result`
- `alpha updog`
- `alpha get`
- `alpha apiresources`
- `alpha shell`
- `alpha tree-view`
- `snapshot create`
- `snapshot view`
- `dump api-docs` / `dump cli-docs` / `dump image-deploy-ref`
  (`starling dump state`/`objects`/`openapi` cover the engine/webview, api-object,
  and OpenAPI dumps)

Important CLI behavior gaps:

- `starling ci` is implemented: it brings the project up headless (no daemon /
  proxy), waits for every enabled resource to settle (update + runtime
  statuses), and exits 0 when all are up or non-zero on the first error / after
  the timeout. The exit-condition logic lives in `src/ci.rs` and is unit-tested.
  `ci_settings(timeout=...)` from the Tiltfile sets the default timeout (a Go
  duration like `"30m"`), overridable with `--timeout`. It does not yet support
  per-resource exit-condition customization or `ci_settings`'
  `k8s_grace_period`/`readiness_timeout`.
- `starling get [kind] [name] [--json]`, `starling describe <kind> <name>`,
  `starling api-resources`, and `starling explain [kind]` are implemented: reads
  go through the daemon (the engine mirrors its object store each report tick),
  and `explain` describes a kind's spec fields. `apply`/`delete`/`patch`/`wait`/
  `edit` write verbs are still missing — writes are reachable over HTTP only, and
  `delete` of an engine-managed object would be undone on the next reload anyway
  (no reconciler owns the reverse direction yet).
- `starling args [-- <args>]` replaces the running instance's Tiltfile args and
  reloads (via the daemon), equivalent to `tilt args`; `starling up -- <args>`
  sets them at launch, and `/api/set_tiltfile_args` is the web equivalent.
- `starling version` prints the build version.
- `starling trigger <resource>` queues a build via the daemon (equivalent to
  `tilt trigger`), targeting every instance of the current project.
- `starling enable <resource>` / `starling disable <resource>` resume/pause a
  resource via the daemon (equivalent to `tilt enable`/`tilt disable`).
- `starling doctor` prints a diagnostic bundle (version, OS/arch, kubectl/docker
  availability, daemon health, and resources in error). It is lighter than
  `tilt doctor`'s full system probe.
- No LSP server command.
- `starling snapshot [--out file]` writes a JSON snapshot of the aggregated
  dashboard state (instances/resources/routes). There is no share/upload or
  snapshot-viewer workflow like Tilt Cloud's.
- Tiltfile args passthrough after `--` is implemented for `starling up`,
  `starling ci`, and live web updates, but not yet for `down`.
- Many Tilt `up` flags are missing, including namespace/context-style flags,
  terminal/HUD/browser behavior, and update/debug/analytics settings.

### 3. Tiltfile Standard Library Coverage Is Partial

Starling's Starlark interpreter exposes a pragmatic subset, but Tilt's API
surface is much larger.

Missing Tiltfile builtins from `internal/tiltfile/api/__init__.py`:

- (none) — `workload_to_resource_function` is now implemented. It is invoked
  once per workload after evaluation with a `K8sObjectID`-style struct
  (`name`/`kind`/`namespace`/`group`), validates a single-argument signature,
  enforces string returns, and errors on duplicate resource names.

Missing or incomplete Tiltfile modules:

- `config`: `define_string`, `define_string_list`, `define_bool`,
  `define_object`, `parse`, `set_enabled_resources`, and
  `clear_enabled_resources` are implemented for common Tiltfile flows.
  `config.parse()` reads `tilt_config.json`, merges `starling up -- <args>`,
  supports positional args via `args=True`, leaves unset settings absent, and
  watches `tilt_config.json` for reload, and can be reloaded through
  `/api/set_tiltfile_args`. Remaining difference: no full pflag-compatible
  usage output.
- `os`: `os.name`, `os.environ.get`, `os.getenv`, `os.getcwd`, `os.putenv`,
  and `os.unsetenv` are available. Environment mutation is process-local.
- `os.path`: `abspath`, `relpath`, `basename`, `dirname`, `exists`, `join`, and
  `realpath` are available. `relpath` now matches Python's POSIX algorithm,
  absolutizing both paths against the cwd and emitting `..` segments for
  cross-directory cases.
- `sys.executable` is available.
- `shlex.quote` is available for POSIX-style shell quoting.
- `k8s_context()` and `k8s_namespace()` return the current kubectl context and
  namespace.
- Settings-style builtins are accepted with basic validation. `watch_settings`,
  `ci_settings(timeout=...)`, `update_settings(max_parallel_updates=...)`,
  `secret_settings(disable_scrub=...)`, `set_team`, and
  `enable_feature`/`disable_feature` all change runtime behavior (the last three
  surface on the UISession status / log scrubbing); `version_settings(constraint=)`
  enforces a semver range against the running version. No runtime behavior yet:
  `disable_snapshots`, `docker_prune_settings`, `analytics_settings`,
  `version_settings(check_updates=)`, and the other `update_settings` fields.
- `v1alpha1`: constructors for `config_map`, `cmd`, `kubernetes_apply`,
  `kubernetes_discovery`, `file_watch`, `ui_button`, `extension_repo`, and
  `extension` are available and return object dicts
  (`apiVersion`/`kind`/`metadata`/`spec`). They don't yet cover every type or
  the full nested struct builders, and the returned objects aren't auto-registered
  into the API store (they're values to pass around, as in Tilt).

Extension gaps:

- `extension_repo(name, url)` accepts both **local** (`file://`) repos, used in
  place, and **remote/git** repos (`https://`, `git@…`, `git://`, or a git repo
  path), which are `git clone`d into a cache (`$STARLING_EXT_CACHE` or
  `~/.starling/extension-repos`) and then used like a local repo. `load("ext://
  <repo>/<path>", ...)` resolves to `<repo>/<path>/Tiltfile` and evaluates it
  into shared state. The clone path is verified by a `git`-backed integration
  test. `ExtensionRepo`/`Extension` API objects are populated.
- The default implicit github extension repo isn't auto-configured (you register
  repos explicitly), there's no `ExtensionRepo`/`Extension` reconciler, and
  `ext://` works in `load()` but not `include()`/`load_dynamic()`.

### 4. Accepted Tiltfile Syntax Often Has Partial or No Runtime Semantics

Several Starling builtins accept Tilt-compatible arguments but ignore them or
only record them.

`local_resource` gaps:

- `ignore` is applied to local resource file-change rebuilds.
- `allow_parallel=True` is recorded and local update-only resources run through
  a parallel build task. Serve-only resources are treated as parallel-safe
  because they have no update command; resources that both update and replace a
  `serve_cmd` still use the main engine path to preserve process lifecycle.
- `cmd_bat` and `serve_cmd_bat` are honored on Windows.
- `dir` / `serve_dir` validation is weaker than Tilt's validation.
- Duplicate local resource-name validation and final cross-type resource-name
  validation are implemented.
- A resource with neither `cmd` nor `serve_cmd` errors, matching Tilt.
- `resource_deps` only influences initial auto-init order. It does not provide
  full Tilt build-graph behavior for every trigger/update path.
- File-change dependency propagation from dependencies to dependents is not
  Tilt-equivalent.

`local` gaps:

- `command_bat` is honored on Windows and `echo_off` redacts command logging.
- Returns a `Blob` (a string-like value with `type() == "blob"`), like Tilt.
- Shell/argv conversion is simpler than Tilt's command model.

`read_file` gaps:

- Returns a `Blob` (string-like, `type() == "blob"`), like Tilt.
- Default handling is looser and uses `to_str()` for non-string defaults.

`load_dynamic` gaps:

- Executes the target file for side effects and returns its exported symbols.
- Does not support extension repository loading.

`filter_yaml` gaps:

- Returns `(matching, rest)` as `Blob`s, like Tilt.
- Name matching supports exact names and regex-style selectors such as `^foo$`.
  Kind matching is still case-insensitive exact matching.
- Label matching is a flat exact metadata-label match.

`k8s_kind` / `k8s_image_json_path` gaps:

- Simple image JSONPaths such as `{.spec.image}` are used for CRD image
  extraction and registry rewrite.
- `image_object` is implemented for simple object paths with repo/tag fields,
  including default-registry rewrite back into separate fields.
- `pod_readiness` is validated; `ignore` marks declared CRD workload resources
  as readiness-ignored.
- Full JSONPath expressions are not implemented.

`default_registry` / `allow_k8s_contexts` gaps:

- `default_registry(host)` rewrites docker_build refs and matching Kubernetes
  image fields with Tilt-style escaped image names.
- `default_registry(host_from_cluster=..., single_name=...)` is implemented for
  local build refs and Kubernetes deploy refs.
- `allow_k8s_contexts` enforces the current Kubernetes context during
  Starlingfile load.

`port_forward` / `k8s_resource(port_forwards=...)` gaps:

- Produces UI links and starts `kubectl port-forward` processes for the
  currently observed pod.
- `name` and `link_path` are surfaced in the generated UI link.
- Forwarding is still tied to Starling's simple first-pod watcher and does not
  expose Tilt-style `PortForward` API object status.

### 5. Docker Build Parity Is Limited

Tilt's `docker_build` supports a much richer image model than Starling's Docker
builder currently uses.

Implemented `docker_build` coverage:

- `dockerfile_contents` is converted into a generated Dockerfile in the build
  context.
- `target` and `platform` are passed through to the Docker builder.
- `cache_from`, `pull`, `network`, and `extra_hosts` are passed through to the
  Docker builder.
- Obsolete `cache` is accepted with a warning.
- `ssh` and `secret` are parsed and recorded as BuildKit metadata.
- `extra_tag` is applied with Docker's tag API after a successful build.
- `entrypoint` and `container_args` override matching Kubernetes container
  `command`/`args` fields before apply.
- `match_in_env_vars` lets Docker builds attach to workloads whose container
  env var values reference the image.
- `ignore`, `only`, and `.dockerignore` are applied while creating the build
  context tar.

Missing or ignored `docker_build` behavior:

- `ssh` and `secret` (and `cache_from`/`platform`/`network`/`build_args`/extra
  tags) are passed through to **BuildKit**: builds that declare `ssh`/`secret`
  route through the `docker build` CLI (which uses BuildKit) instead of bollard,
  via `docker_build_args` + `build_image_buildkit`. Secret passthrough is
  verified against the local Docker daemon by a `STARLING_DC_IT=1`-gated
  integration test. Remaining: bollard builds (no ssh/secret) don't use BuildKit
  caching, and there's no BuildKit progress/output parity.
- No Tilt-style build result object or ImageMap reconciliation.
- No digest/ref tracking equivalent to Tilt's image deployment refs.

Image injection gaps:

- Starling matches Docker builds to workload image strings and rewrites
  Kubernetes YAML for `default_registry`, but does not deploy immutable
  digest-like refs.
- `ImageMap` (and `DockerImage`/`CmdImage`) API objects are populated per built
  image ref, but they carry no resolved digest/cluster-ref status and nothing
  reconciles them into deploys.
- `default_registry(host)` handles Tilt-style escaped registry refs, including
  `host_from_cluster` and `single_name`.
- No registry hosting discovery.
- `match_in_env_vars` image discovery is implemented for container env values.
- CRD image locators are limited to simple field paths.
- No local registry optimization beyond `kind load docker-image` when the
  current context name starts with `kind-`.

`custom_build` gaps:

- `tag` is used to populate `EXPECTED_REF`/`EXPECTED_TAG`/`TAG` for the custom
  build command.
- `skips_local_docker` skips Starling's kind-image-load step after the custom
  build command.
- `disable_push` is accepted; Starling does not push custom build outputs.
- `match_in_env_vars` attaches custom builds to workloads whose container env
  var values reference the image.
- `command_bat` is honored on Windows.
- `outputs_image_ref_to` is accepted; after the custom command succeeds,
  Starling reads the output file and rewrites deploy YAML to that ref before
  apply.
- `image_deps` attaches referenced image builds and errors if the dependency is
  missing.
- No ImageMap output/status.

### 6. Kubernetes Deploy/Discovery Parity Is Limited

Starling deploys by shelling out to `kubectl apply` and polls pods by a simple
label selector. Tilt has dedicated controllers for apply, discovery, pod log
streams, port forwards, image maps, live update, trigger queues, session state,
and UI resource aggregation.

Kubernetes gaps:

- No native Kubernetes client/controller loop (deploy/discovery still shell out
  to `kubectl`), though that path is now verified end-to-end against a real kind
  cluster via gated integration tests (see section 14).
- `KubernetesApply` and `KubernetesDiscovery` API objects are populated in the
  store (section 1). After each deploy the engine writes the apply result onto
  the `KubernetesApply` object's `status` (`lastApplyTime` + `error`), so it
  carries live state; but there is still no reconciler driving apply *from* the
  object — the actual apply/discovery runs through the engine's `kubectl` path.
- A `PodLogStream` API object is populated per Kubernetes resource, but there is
  no reconciler; log streaming still runs through the engine's `kubectl logs`.
- A `PortForward` API object is populated, but there is no reconciler; forwarding
  still runs through the engine's simple pod watcher.
- No server-side apply/diff-style status model.
- No object owner-reference traversal.
- No event watching.
- Runtime status aggregates across all matching pods (ready/total, any-failed →
  error, readiness-ignored → ok), shown as "<ready>/<total> ready". Log
  streaming / port-forward / initial-sync still target the first pod, and there's
  no per-pod status list.
- No namespace-aware deploy/status override beyond whatever `kubectl` current
  context uses.
- `k8s_custom_deploy` creates a Kubernetes resource, runs its custom apply
  command, watches deps, and can attach image deps declared elsewhere in the
  Tiltfile. Its `delete_cmd` now runs during `starling down` (and
  `daemon --shutdown`), but not on Ctrl-C, matching Tilt; in `--dry-run` the
  delete command is logged but not executed. It does not yet derive pod
  selectors from apply output.
- `allow_k8s_contexts()` is enforced during Starlingfile load.
- `pod_readiness="ignore"` prevents pod readiness from gating runtime status;
  `wait` is the default behavior.
- `discovery_strategy` values are validated. Because Starling does not trace
  owner references yet, `default` and `selectors-only` are effectively the same
  simple selector-based watcher behavior.
- No CRD locator/image extraction behavior.
- Non-workload object grouping is heuristic by default: attach same-name
  objects, else first workload, else standalone. Explicit
  `k8s_resource(objects=...)` grouping, including object-only resources, is
  implemented.
- `objects=` selectors support Tilt's `name[:kind[:namespace]]` form
  (case-insensitive exact matching, 1-3 parts, >3 parts errors at load), so
  same-named objects can be disambiguated by kind/namespace. Regexp selector
  parts and the API-group field are not yet supported.
- `workload_to_resource_function()` renames workloads at assembly time, with
  conflict detection and same-name object regrouping onto the renamed resource.
  The `K8sObjectID` passed in is a plain struct (no Tilt-style `str(id)` form),
  and workloads registered after the function call are not renamed.
- Identical entities are deduped by object identity at `k8s_yaml` registration;
  there is no `allow_duplicates`-style opt-in for intentionally repeated objects.
- Empty YAML handling differs from Tilt.

Port-forward gaps:

- Port-forward processes are started for the first observed pod and restarted
  when that pod changes.
- A `PortForward` API object carries a resolved-target status (`podName`/`ready`)
  via `reconcile_port_forward`; there's no per-forward connection health beyond
  the target resolution.
- No automatic service port inference.
- Reconciliation is limited to Starling's simple pod polling loop.

Pod/log gaps:

- `kubectl logs -f --all-containers --tail 20` starts for the first observed pod
  only.
- No structured per-container log status.
- No pod source abstraction.
- No restart-aware log stream reconciliation equivalent to Tilt.

### 7. Live Update Is Only a Simplified Fast Path

Starling records live update steps and can run `kubectl cp` / `kubectl exec`
against a currently known pod after an initial deploy. Tilt's live update system
is a controller-backed feature with precise file change matching, container
selection, restart strategies, initial sync, and status.

Live update gaps:

- `fall_back_on` paths are watched and force a full image rebuild/apply when
  matching file changes are detected.
- `initial_sync` runs sync/run live-update steps once when Starling observes a
  new pod for the resource, and Starling validates that it appears at most once
  at the start of the live-update list.
- `run(trigger=...)` filters live-update run steps based on the changed paths
  seen by Starling's resource watcher.
- `run(echo_off=...)` redacts the command from live-update logs.
- No container selection when a pod has multiple containers.
- No Docker Compose live update path.
- A `LiveUpdate` API object is populated from the steps, but it carries no live
  status and no reconciler drives it.
- No per-step status or failed container status.
- No sync dependency model equivalent to Tilt's file watch/live update graph.
- `restart_container` deletes the pod as an approximation.

### 8. Docker Compose Parity Is Very Basic

Starling's `docker_compose(configPaths, env_file=..., project_name=...,
profiles=..., wait=...)` parses one or more Compose YAML paths or inline blob
contents and creates one resource per service whose serve command runs
`docker compose ... up <service>`.

Docker Compose gaps:

- Inline `Blob` configs (from `blob()`/`read_file()`) are accepted and written to
  temporary files so `docker compose` can consume them.
- Multiple Compose configs are merged only far enough to discover service names;
  full Docker Compose override semantics are delegated to the generated
  `docker compose -f ...` command at runtime.
- `dc_resource` supports service rename, resource deps, trigger mode,
  auto-init, links, labels, duplicate validation, and `project_name`
  disambiguation. Its `image` argument attaches the matching
  `docker_build`/`custom_build`, which the engine builds before `docker compose
  up` so the service uses the freshly built image.
- `DockerComposeService` and `DockerComposeLogStream` API objects are populated
  per Compose resource. A `reconcile_docker_compose_service` controller queries
  `docker compose ps` and writes the service's running state onto the object
  (exposed at `POST /api/v1alpha1/:kind/:name/reconcile`, verified against the
  local Docker daemon via a `STARLING_DC_IT=1`-gated integration test). It is a
  status reader, not a full lifecycle controller.
- No Compose health/status tracking.
- No Compose port/link inference.
- Compose build integration is partial: `dc_resource(image=...)` builds the
  matched image before `compose up`, but there's no `docker compose build`
  integration or live-update for Compose images.
- A Compose status controller (`reconcile_docker_compose_service`) reports
  running state from `docker compose ps`; full health/lifecycle management is
  still missing.
- No Compose-specific down/cleanup semantics.
- No Compose live update.

### 9. File Watching and Ignore Semantics Are Much Simpler

Starling uses `notify` watchers for manifest deps and config files.

Gaps:

- A `FileWatch` API object is populated per resource (watched paths + ignores),
  but it is descriptive only — file watching still runs through the engine's
  `notify` watchers, not a `FileWatch` reconciler.
- `.tiltignore` is read and applied to local resource file-change rebuilds.
- `watch_settings(ignore=...)` is applied to local resource file-change rebuilds.
- `local_resource(ignore=...)` is applied to that resource's file-change
  rebuilds.
- Docker `ignore` / `only` and `.dockerignore` semantics are not applied.
- Config reload watches direct files read through includes/loads/read_file, but
  no broader dependency graph equivalent to Tilt's file watch controllers.
- No API-visible file event status.

### 10. Triggering, Disable, and Build Control Are Simplified

Tilt has build control, trigger queues, holds, disable sources, start/stop specs,
manual/auto modes, and richer dependency behavior.

Gaps:

- No `TriggerQueue` / build-control equivalent.
- No `StartOnSpec`, `RestartOnSpec`, or `StopOnSpec` object semantics.
- A disable-source `ConfigMap` (`isDisabled` = "true"/"false") is populated per
  resource and is authoritative: writing it (PATCH/PUT via the API object store)
  toggles the resource's disable state, via the engine's object-store watcher.
  Persistence across reloads still relies on re-deriving it from runtime state.
- A `ToggleButton` API object is populated per resource, but it is descriptive;
  the actual disable/pause toggle still flows through the UIButton path.
- Disable/pause is an in-memory UI status change (toggleable via the UIButton
  path or by writing the disable-source ConfigMap). It survives engine reloads
  but is not persisted to disk across `starling up` restarts.
- No full build queue visibility.
- `allow_parallel=True` permits local update-only resources to run concurrently
  with the main build loop, capped by `update_settings(max_parallel_updates=N)`
  when set (otherwise uncapped).
- No crash rebuild semantics.
- `starling ci` handles batch-mode exit conditions (settle/error/timeout); what
  remains is Tiltfile-driven CI customization (`ci_settings`, per-resource exit
  conditions) and crash-rebuild semantics.
- Resource selection from `starling up -- resource-name` is implemented when
  `config.parse()` is not called, including resource dependencies.

### 11. Web UI / HUD Server Is Partial

Starling is protocol-compatible enough for the vendored React UI to render core
resources, but Tilt's HUD server and apiserver surfaces are broader.

Gaps:

- The UIButton list/get/status routes live under `/proxy/apis/tilt.dev/v1alpha1/`.
  A generic CRUD + watch surface over the API object store is served under
  `/api/v1alpha1/`: `GET :kind` (list), `POST :kind` (create), `GET :kind/:name`
  (get), `PUT :kind/:name` (replace), `PATCH :kind/:name` (RFC 7386 merge),
  `DELETE :kind/:name`, `GET _kinds`, and `GET _watch` (Server-Sent Events of
  add/modify/delete). Reads/writes are namespace-default.
- The generic routes are not under Tilt's exact `/proxy/apis/...` path (kept
  separate to avoid colliding with the literal `uibuttons` routes), and there is
  no OpenAPI discovery/schema validation behind them.
- Writes are a partial control surface: PATCHing a `tilt.dev/force-trigger`
  annotation onto an object keyed by a resource name enqueues a build for that
  resource (the engine watches the store's event stream). Other object writes
  are recorded but do not yet drive engine behavior.
- `/api/set_tiltfile_args` reloads the running engine with replacement args.
- Analytics routes are accepted no-ops.
- Snapshot route returns a simple snapshot around the current view; no complete
  snapshot create/share workflow.
- Many `View` fields and object statuses are absent or defaulted.
- No terminal HUD equivalent to Tilt's default terminal UI.
- No Tilt Cloud/user integrations. `set_team(team_id)` surfaces the team id on
  the UISession status (and `enable_feature`/`disable_feature` surface feature
  flags), but there is no actual Tilt Cloud connection.
- No update/version suggestion behavior.
- No help/docs API support.

### 12. Logs, Secrets, and Output Handling Lag Tilt

Gaps:

- Secret values from k8s `Secret` objects (`data` + `stringData`) are scrubbed
  from log output (replaced with `[redacted]`), toggleable via
  `secret_settings(disable_scrub=True)`. Values are redacted as they appear in
  the YAML (no base64 decode), so a decoded secret echoed by an app is not yet
  caught.
- The log store keeps per-span segments (`span_id`/`time`/`level`/`text`) in a
  ring capped at 5000 segments, with *absolute* checkpoints (`log_start`) so the
  websocket delta cursor and `logs_since` stay valid as old segments drop. A
  structured runtime log reader (`Store::query_logs`, filtering by span,
  minimum level, and an RFC3339 `since`/`until` time range) is exposed at
  `GET /api/v1alpha1/_logs?resource=&level=&since=&until=`. Build logs (local,
  Kubernetes, and live-update paths) go to a per-attempt span (`{name}:build:{n}`)
  that rolls up to the resource for the dashboard/webview, so a specific build's
  logs are directly addressable (query the exact span) while still grouping under
  the resource.
- Daemon retains only a capped recent ring per instance/resource.
- Log levels are simplified (DEBUG/INFO/WARN/ERROR ordering only).
- Build warnings are not modeled.
- No copy/clear log operations outside web UI state conventions.

### 13. Platform Parity Is Not Complete

Tilt is cross-platform. Starling currently makes Unix/macOS-centric choices.

Gaps:

- Daemon transport is cross-platform: a Unix domain socket under `~/.starling`
  on Unix, and a `127.0.0.1` TCP port on Windows (which has no AF_UNIX in tokio),
  with the chosen port recorded in `daemon.port` for the client. `handle_conn` is
  generic over the stream type. (Previously the daemon was Unix-socket-only and
  did not compile for Windows — surfaced and fixed via the cross-compile below.)
- String commands run through the host shell — `sh -c` on Unix, `cmd.exe /S /C`
  on Windows (even without an explicit `cmd_bat`/`command_bat`), matching Tilt's
  host-command behavior.
- `cmd_bat`, `serve_cmd_bat`, and `command_bat` are parsed for Windows. The
  host-shell selection logic (`cmd.exe /S /C` vs `sh -c`) is unit-tested for both
  platforms cross-platform (`shell_argv_selects_host_shell_per_platform`,
  `windows_bat_commands_use_cmd_shell`).
- **The whole crate compiles for Windows.** `cargo check --target
  x86_64-pc-windows-gnu` builds clean (only the two pre-existing dead-code
  warnings), with the C dependencies (aws-lc-sys, ring) building via mingw-w64.
  `.cargo/config.toml` records the mingw linker/toolchain so the check is
  reproducible from a Unix host. This is **compile-level** Windows parity, verified
  locally; it caught the Unix-socket daemon gap above.
- **Windows runtime behavior** beyond the command paths is still unverified: that
  needs execution on Windows, which the local dev environment cannot do (arm64
  macOS, no Windows host or x86-Windows emulator). The `parity.yml` CI workflow
  runs the full unit suite on a real `windows-latest` runner — that is the path to
  runtime verification, and it has not yet been executed here.
- Service installation is macOS LaunchAgent only.
- `/etc/hosts` sync and trust-store operations are platform-sensitive.

### 14. Testing Parity Is Far Behind Tilt

Tilt has extensive unit, controller, CLI, engine, web, and integration tests.
Starling now has ~140 unit/integration tests, including a growing `tilt_compat_*`
suite covering Tiltfile builtins (docker_build options + container command/args
override, default_registry rewrite, custom_build, k8s_resource grouping/selectors,
workload_to_resource_function, filter_yaml, helm/kustomize, dc_resource, ext
loading, Blob, settings) plus the API object store CRUD/watch, HTTP routes, CI
exit conditions, log scrubbing, and pod-status aggregation. There are also eighteen
`#[ignore]`d **integration tests** gated behind env flags + a `kind-*` context.
Fifteen Kubernetes tests (`STARLING_K8S_IT=1`): pod-status aggregation, the full
`Engine` deploy path end-to-end, the six object-driven reconcilers — apply
(`KubernetesApply` → cluster + status), discovery (`KubernetesDiscovery` → pod
ready/total), pod-watch (`KubernetesDiscovery` → per-pod `status.pods[]` detail),
pod logs (`PodLogStream` → logs into the store), port-forward
(`PortForward` → resolved target pod), and live-update (`LiveUpdate` → sync+exec
into a running container, verified by reading the change back) — the
maintained-controller loop (`spawn_controller_manager` converges a discovery
object's status with no manual reconcile call), and three kube-rs transport
tests: the read path (the typed client and `kubectl` agree on pods, and discovery
converges via the typed client), the write/exec path (live-update's cp+exec
through the WebSocket attach API, verified by reading the result back), and the
apply+log path (server-side apply creates a pod and the typed log API reads its
output, verified from a guaranteed-clean state), and the controller-managed
port-forward process lifecycle (the controller starts a persistent forward,
verified by a real HTTP request through it, then tears it down on object
deletion), the typed follow log stream (`stream_pod_logs` over
`Api::log_stream` delivers a pod's per-second output into the store as it is
produced), and the native kube-rs port-forward (`stream_port_forward` proxies a
local port over `Api::portforward`, verified by an HTTP request through it).
Three Docker tests (`STARLING_DC_IT=1`):
Compose status reconciler, BuildKit secret passthrough, and git extension clone.
Run: `STARLING_K8S_IT=1 STARLING_DC_IT=1 cargo test -- --ignored`.

A **parity CI matrix** is defined in `.github/workflows/parity.yml`: it runs the
hermetic unit suite on Linux, macOS, and `windows-latest` (so the Windows
host-shell command paths execute on a real Windows host), plus `fmt`/`clippy`,
plus a Linux job that stands up a real kind cluster + Docker and runs the
env-gated integration tests above. This is authored but has not been executed in
this environment (no GitHub Actions runners here); it is the mechanism by which
the Windows and multi-OS legs get verified once run in CI.

What's still missing is broader coverage that needs live runtimes:

- More cluster scenarios (pod restarts, multi-container live update, port
  forwards, owner-ref discovery) and Docker build / Compose execution.
- Kubernetes image injection and immutable deploy refs.
- Cross-platform (Windows) command behavior.
- Web UI flows beyond core rendering/button interactions.

## Builtin-by-Builtin Gap Matrix

| Tiltfile area | Starling state | Gaps |
| --- | --- | --- |
| `local_resource` / `test` | Implemented for basic update/serve/deps/links/labels/env/dirs/probe, file-watch `ignore`, Windows `cmd_bat`/`serve_cmd_bat`, deprecated `test(...)`, empty cmd/serve validation, duplicate local names, final cross-type resource-name validation, and update-only `allow_parallel=True` execution | `allow_parallel` does not yet parallelize resources that must replace `serve_cmd`; incomplete `resource_deps` behavior |
| `local` | Subprocess execution with Windows `command_bat`, `echo_off`, returns a `Blob` | Command model is simpler than Tilt's |
| `read_file` / `watch_file` | File read/watch reload, returns a `Blob` | No full file-watch object model |
| `include` / `load` | Side-effect loading, normal Starlark `load`, and `ext://` from local `extension_repo`s | `include()` doesn't take `ext://`; no remote repos |
| `load_dynamic` | Side-effect loading plus exported-symbol dict | No `ext://` in `load_dynamic` |
| `docker_build` | Basic Bollard build with build args, Dockerfile path/contents, target/platform/cache_from/pull/network/extra_hosts/extra_tag, live update metadata, env-var matching, container command/args overrides, default-registry rewrite, BuildKit metadata parsing, and build-context filtering from `.dockerignore`, `ignore`, and `only` | BuildKit session support still missing; no ImageMap/digest/build-ref status model |
| `custom_build` | Runs command with `EXPECTED_REF`/`EXPECTED_IMAGE`/`EXPECTED_TAG`, tag parsing, Windows command override, env-var matching, default-registry rewrite metadata, output ref files, image deps, and kind load skip for `skips_local_docker` | No ImageMap output/status |
| `k8s_yaml` | Parses multi-doc YAML; dedups identical entities by `name:kind:namespace:group` | No full entity index; empty-YAML handling differs from Tilt |
| `k8s_resource` | Basic grouping, explicit `objects=` grouping (object-only resources + `name:kind:namespace` selectors) , labels, links, `pod_readiness="ignore"`, discovery-strategy validation, and pod-scoped port-forward processes | No `PortForward` API status; no owner-reference discovery; selector parts are exact (no regexp) and the API-group field is unsupported |
| `k8s_custom_deploy` | Runs custom apply commands, runs `delete_cmd` on `starling down`, watches deps, supports env/dir/Windows command overrides, live-update metadata validation, image deps, and `k8s_resource` customization | No apply-output discovery/status equivalent to Tilt's `KubernetesApply` controller |
| `filter_yaml` | Split with exact/regex name matching, returns `Blob`s | Incomplete full selector fidelity |
| `kustomize` | Runs `kustomize build` or `kubectl kustomize` | Limited flags/tracking |
| `helm` | Runs `helm template` with values, set, namespace, CRD, and kube-version flags | Limited chart dependency tracking/validation |
| `docker_compose` | One service resource per compose service; accepts string/blob/list config paths plus `env_file`, `project_name`, `profiles`, and `wait` command options; tracks the default `.env` when present | No Compose controller/status/log parity; inline blobs use temporary files; service discovery only partially models multi-file override semantics |
| `dc_resource` | Compose service rename, deps, trigger mode, auto-init, links, labels, duplicate/missing validation, `project_name` disambiguation, and `image=` attaching a docker_build the engine builds before `compose up` | No Compose controller/status integration |
| `port_forward` | UI link plus `kubectl port-forward` command data for Kubernetes resources | No standalone `PortForward` API object/status |
| live update steps | Basic `sync`/`run`/pod delete plus `fall_back_on` full-rebuild discrimination, `run(trigger=...)` path filtering, and pod-observed `initial_sync` | Missing container selection and detailed status |
| probes | Local readiness probes enforce `success_threshold` / `failure_threshold` | No Kubernetes pod-readiness integration |
| `k8s_kind` / image paths | Simple CRD image field extraction/rewrite, simple `image_object` repo/tag extraction/rewrite, and `pod_readiness="ignore"` for declared CRD workloads | No full JSONPath behavior |
| `default_registry` | Tilt-style escaped image rewrite, `host_from_cluster`, and `single_name` | No ImageMap/ref status model or push behavior |
| `allow_k8s_contexts` | Enforces current Kubernetes context during load | No advanced context policy beyond exact names |
| config API | Implemented for string, string_list, bool, object, `tilt_config.json`, CLI args, positional args, absent unset settings, enabled resources, and live `/api/set_tiltfile_args` reloads | Missing full pflag-compatible usage |
| JSON/YAML helpers | Implemented for common structured data | Uses JSON-compatible Starlark values |
| `blob` / `listdir` | Implemented | `blob` returns a real `Blob` value; `listdir` returns sorted relative paths |
| settings builtins | `watch_settings` filters file events; `ci_settings(timeout)` sets the `ci` timeout; `update_settings(max_parallel_updates)` caps parallel updates; `secret_settings(disable_scrub)` controls log scrubbing; `version_settings(constraint)` enforces a semver range; `set_team`/`enable_feature`/`disable_feature` surface on the UISession status | No runtime behavior for analytics / `disable_snapshots` / `docker_prune_settings` / `version_settings(check_updates)` |
| `warn` / `fail` / `exit` | Implemented | `exit()` reports an execution error rather than preserving a process exit code |
| `os` / `os.path` / `sys` / `shlex` modules | Partial shim | Common functions are available; not a complete Python stdlib clone |
| Starling-specific named URL builtins | Implemented | Not part of Tilt parity; additive Starling behavior |

## Suggested Parity Milestones

1. Define the target: decide whether Starling is meant to be a Tilt-compatible
   runtime, a Tiltfile-compatible subset, or a Starling-first tool with best
   effort Tilt import. The implementation path differs significantly.
2. Add a generated Tiltfile API compatibility test suite from
   `internal/tiltfile/api/__init__.py`, with each builtin classified as
   implemented, accepted-no-op, partial, or missing.
3. Continue the pure-Starlark/stdlib layer: add settings functions and resource
   constructors.
4. Make accepted-no-op builtins either real or visibly unsupported. Silent
   no-ops are the highest migration risk.
5. Introduce a minimal API object store for core Tilt resources before adding
   more CLI commands. CLI parity depends on object CRUD/list/watch semantics.
6. Build a real image pipeline: ignore rules, Docker options, immutable deploy
   refs, image injection, ImageMap-like status, and registry/default-registry
   behavior.
7. Replace the simple Kubernetes loop with controller-like reconciliation for
   apply, discovery, pod logs, port forwards, and live update.
8. Add Docker Compose controllers and image-build integration.
9. Add `ci`, `args`, `trigger`, `enable`, `disable`, and API CLI commands once
   the underlying object/build-control model exists.
10. Add parity integration tests from representative Tilt examples before
    declaring drop-in support.

## High-Risk Silent Mismatches

No high-risk silent Tiltfile argument mismatches are currently called out here.
The remaining gaps above are either unsupported, partial with visible behavior,
or require larger API/controller/CLI parity work.
