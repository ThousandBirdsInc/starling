# Starling → Tilt Parity: Work Log

This document explains the parity work done on Starling (the Tilt port in this
repo) and how it maps onto Tilt's design. It is the human-readable companion to
[`TILT_PARITY_GAPS.md`](./TILT_PARITY_GAPS.md), which tracks the remaining gaps
gap-by-gap. Everything described here is implemented, unit-tested, and reflected
in the gaps doc.

Test count grew from **85 → 142** over this work (`cargo test -q`), with
`cargo check` clean and all touched files `rustfmt`-clean — plus eighteen `#[ignore]`d
**integration tests** that pass against local runtimes. Fifteen run against a
**kind** cluster (`STARLING_K8S_IT=1 cargo test -- --ignored k8s_integration`):
real apply + pod-status aggregation, the full `Engine` deploy path end-to-end,
six object-driven **cluster-backed reconcilers** — `reconcile_kubernetes_apply`
(applies a `KubernetesApply` object's YAML, writes status),
`reconcile_kubernetes_discovery` (lists pods for a `KubernetesDiscovery`
selector, writes ready/total), `reconcile_pod_watch` (writes per-pod detail to
the same object's `status.pods[]`), `reconcile_pod_log_stream` (fetches a
`PodLogStream` selector's logs into the store), `reconcile_port_forward`
(resolves the target pod onto a `PortForward` object's status), and
`reconcile_live_update` (syncs files + execs commands into a running container
from a `LiveUpdate` object's spec) — and the **maintained-controller loop**
(`spawn_controller_manager`, which continuously reconciles the idempotent status
controllers and converges a discovery object with no manual reconcile call). Two more
(`STARLING_DC_IT=1 cargo test -- --ignored dc_integration`) verify, against the
local Docker daemon, the **Docker Compose** status reconciler
(`reconcile_docker_compose_service`) and **BuildKit secret passthrough** (a build
whose `RUN --mount=type=secret` requires the secret to be mounted — proving
`docker_build_args`/`build_image_buildkit` route `ssh`/`secret`/`cache` through
`docker build`/BuildKit, which bollard cannot). The reconcilers are exposed at
`POST /api/v1alpha1/:kind/:name/reconcile`. This is the previously-impossible
verification of the controller + BuildKit paths, enabled by kind + Docker locally.

---

## 1. The API object store (the biggest piece)

Tilt is built around a local Kubernetes-style apiserver: versioned objects,
`resourceVersion`/`uid`/metadata bookkeeping, watch streams, and CRUD verbs.
Starling had none of this — just an in-memory `Store` materializing the
frontend's `UIResource`/`UIButton` views.

**What was built** (`src/api/store.rs`): a generic, thread-safe, in-memory
object store with

- `create` / `get` / `list` / `replace` / `apply` / `patch` / `delete` / `watch`,
- monotonic `resourceVersion`, `uid` assignment, and `kind`/`apiVersion`/
  `metadata` stamping (k8s semantics: `AlreadyExists`/`NotFound`),
- RFC 7386 JSON **merge-patch**,
- a broadcast **watch** stream of `Added`/`Modified`/`Deleted` events.

**Populated and reconciled each load/reload** by the engine — **16 of Tilt's 18
core `v1alpha1` object types**:

| | | |
|---|---|---|
| `Tiltfile` (singleton) | `Session` (singleton) | `KubernetesApply` |
| `KubernetesDiscovery` | `PodLogStream` | `FileWatch` |
| `Cmd` | `PortForward` | `LiveUpdate` |
| `DockerImage` | `ImageMap` | `CmdImage` |
| `DockerComposeService` | `DockerComposeLogStream` | `ToggleButton` |
| `ConfigMap` (disable source) | | |

`ExtensionRepo`/`Extension` are also populated when extension repos are used
(`extension_repo(name, url)` + `load("ext://<repo>/<path>", ...)`) — both local
(`file://`) and remote/git URLs, the latter `git clone`d into a cache — covering
all 18 of Tilt's core object types.

**Exposed over HTTP** (per-instance web server): full CRUD + discovery + watch:

- `GET /api/v1alpha1/:kind` (list), `POST` (create)
- `GET/PUT/PATCH/DELETE /api/v1alpha1/:kind/:name`
- `GET /api/v1alpha1/_kinds`, `GET /api/v1alpha1/_watch` (SSE)
- `GET /proxy/apis/tilt.dev/v1alpha1` — Kubernetes-style `APIResourceList`
  discovery

**Reverse-path reconcilers** (the store as a control surface, not just a mirror):

- writing a `tilt.dev/force-trigger` annotation onto an object enqueues a build
  for that resource;
- writing a `{name}-disable` `ConfigMap`'s `isDisabled` toggles the resource's
  disable state.

---

## 2. Object-backed CLI (reads through the daemon)

The object store lives in each `starling up` engine process; the CLI talks to
the daemon. The engine now mirrors its objects to the daemon each report tick
(new `objects` field on the `Update` request + a `GetObjects` request), so the
CLI can read them:

- `starling get [kind] [name] [--json]` — list/get objects (no kind → list kinds)
- `starling describe <kind> <name>` — detailed single-object view
- `starling api-resources` — list available kinds
- `starling trigger <resource>` — queue a build (Tilt `tilt trigger`)
- `starling enable <resource>` / `starling disable <resource>` — resume/pause
- `starling args [-- <args>]` — replace the running Tiltfile args and reload
- `starling version` — print the build version
- `starling doctor` — diagnostic bundle (version, env, daemon + resource health)
- `starling snapshot [--out file]` — write a JSON snapshot of dashboard state
- `starling dump <state|objects>` — print internal state as JSON (Tilt `tilt dump`)

`trigger`/`enable`/`disable`/`args` reach the running instance through the
daemon's command queue; `get`/`describe`/`api-resources`/`snapshot` read the
daemon's aggregated state.

(Write verbs `apply`/`delete`/`patch` over the CLI are intentionally not added:
the daemon↔engine channel is poll-based/fire-and-forget, so a CLI write couldn't
report apiserver conflicts and engine-managed objects regenerate each
materialize. The object store's HTTP surface is the proper write path until a
general reconciler exists.)

---

## 3. CI mode

`starling ci` (`src/ci.rs` + `main.rs`) brings the project up **headless** (no
daemon/proxy), waits for every enabled resource to settle, and exits 0 / non-zero
on first error / after a timeout. The exit-condition decision (`ci_outcome`) is a
pure, unit-tested function. `ci_settings(timeout=...)` from the Tiltfile sets the
default timeout (Go-duration parsing), overridable with `--timeout`.

---

## 4. Logs (the structured log store)

- **Structured runtime reader** (`Store::query_logs`): filter by span (resource),
  minimum level (`DEBUG`<`INFO`<`WARN`<`ERROR`), and an RFC3339 `since`/`until`
  time range. Exposed at `GET /api/v1alpha1/_logs?resource=&level=&since=&until=`.
- **Ring-capped buffer**: the engine log buffer was unbounded; it's now capped at
  5000 segments with an absolute-offset (`log_start`) checkpoint scheme, so the
  websocket delta cursor and `logs_since` stay valid as old segments drop.
- **Per-attempt build spans**: build logs (local, Kubernetes, and live-update
  paths) go to `{name}:build:{n}`, which rolls up to the resource for the
  dashboard/webview (via `base_resource`), so a specific build's logs are
  directly addressable.
- **Secret scrubbing**: values from k8s `Secret` objects (`data`/`stringData`)
  are redacted from log output as `[redacted]`, toggleable via
  `secret_settings(disable_scrub=True)`.

---

## 5. Settings builtins made real (no longer accepted-no-ops)

- `ci_settings(timeout=...)` → drives `starling ci` (§3)
- `update_settings(max_parallel_updates=N)` → caps concurrent parallel local
  updates via an engine `Semaphore` (uncapped when unset, so no behavior change)
- `set_team(team_id)` → surfaces on the `UISession` status (`tiltCloudTeamID`)
- `enable_feature` / `disable_feature` → surface on the `UISession` status as
  feature flags
- `secret_settings(disable_scrub=...)` → toggles log secret scrubbing
- `version_settings(constraint=...)` → enforces a semver range against the
  running version (fails the load if unsatisfied)
- `watch_settings(ignore=...)` (pre-existing) → filters file-change events

Disabled/paused state now also survives engine reloads (preserved across the
re-materialize rather than reset to enabled).

---

## 6. Platform parity

String commands now run through the host shell appropriate to the platform —
`sh -c` on Unix, `cmd.exe /S /C` on Windows — even without an explicit
`cmd_bat`/`command_bat` (matching Tilt's host-command behavior).

---

## 7. Bounded Starlingfile / engine closures

- **`workload_to_resource_function`** — the last named silent no-op: renames
  workloads at assembly time with a `K8sObjectID` struct, conflict detection, and
  same-name object regrouping.
- **`k8s_custom_deploy` `delete_cmd`** now runs on `starling down` (not Ctrl-C),
  logged-only under `--dry-run`.
- **`os.path.relpath`** — full POSIX algorithm (absolutize, normalize, `..`).
- **`k8s_resource(objects=...)`** — Tilt's `name[:kind[:namespace]]` selector
  (case-insensitive, 1–3 parts, errors on >3), disambiguating same-named objects.

---

## What remains (see `TILT_PARITY_GAPS.md`)

Built and verified against local runtimes (kind + Docker + git) this arc:

- a **reconciler lifecycle**: an in-process `Reconciler` trait + registry, plus
  five object-driven, cluster/daemon-backed reconcilers — apply, discovery,
  pod-log, port-forward (resolve target), and Compose-status — exposed at
  `POST /…/:kind/:name/reconcile`. For real (non dry-run) deploys, the engine's
  apply now routes through the apply reconciler (object-authoritative), verified
  by the kind e2e test.
- a **pod-watch controller** (`reconcile_pod_watch`): writes detailed per-pod
  records (name, phase, readiness, restarts, per-container detail) to a
  `KubernetesDiscovery` object's `status.pods[]` — the stateful per-pod tracking
  the dashboard and live-update build on. Verified against kind.
- a **live-update controller** (`reconcile_live_update`): resolves the target
  pod for a `LiveUpdate` object and applies its spec — `kubectl cp` for each
  sync, `kubectl exec` for each run — recording `podName`/`lastExecTime` or
  `failed`/`message` on the object. Verified against kind by syncing a file +
  exec'ing a command, then reading the change back out of the container. (Kept
  out of the maintained loop — it mutates the container, so it is not idempotent
  for continuous re-runs.)
- a **maintained-controller loop** (`spawn_controller_manager`): the engine
  spawns a background task on real `up` that continuously reconciles the
  idempotent status controllers (discovery aggregate + pod-watch detail +
  port-forward target) on an interval, converging their objects' status without
  an external reconcile call — the continuous-reconciliation model rather than
  one-shot reconcile. Verified against kind (the loop converges a discovery
  object's `totalPods` on its own).
- **BuildKit** secret/SSH/cache passthrough (via `docker build`), verified.
- **remote extension repos** (`git clone`d into a cache), verified.

All six object-driven reconcilers Tilt's controller model centers on — apply,
discovery, pod-watch, pod-log, port-forward, and live-update — now exist and are
verified against a real kind cluster, with three running continuously in the
maintained loop.

- a **kube-rs transport** (`src/kube_client.rs`, on `kube` + `k8s-openapi`): an
  in-process typed client covering the full reconciler surface. Reads —
  `list_pods` backs `list_pods_for_selector` (discovery, pod-watch, port-forward
  and live-update target resolution) and `pod_logs` backs the pod-log reconciler.
  Writes — `apply_yaml` does server-side apply of each document as a
  `DynamicObject` (GVK resolved via live API discovery), and `exec`/`copy_file`
  run commands and stream files into a container via the WebSocket **attach API**
  (the typed equivalents of `kubectl exec`/`cp`). Selected by `STARLING_KUBE_RS=1`
  (else `kubectl`); both transports produce identical results, so the reconcilers
  are transport-agnostic. Verified against kind: the read path matches `kubectl`
  and discovery converges via the typed client; live-update's cp+exec land the
  synced/exec'd files (read back to confirm); and the apply path creates a pod
  whose logs the typed log API then reads (from a guaranteed-clean state).
  `kubectl` stays the default transport.

With the kube-rs path enabled, the **entire Kubernetes surface runs through the
in-process typed client with no per-operation shell-out** — list, apply, exec,
cp, one-shot logs, the persistent follow log stream (`kube_client::log_stream`
via `Api::log_stream`, adapted to a tokio reader with `tokio_util`'s `.compat()`),
and port-forward (`kube_client::port_forward_listener`: a local TCP listener
proxying each connection to the pod over `Api::portforward` with
`copy_bidirectional`).

- the **controller-managed port-forward process lifecycle**: the maintained
  controller loop now owns the persistent `kubectl port-forward` process for each
  `PortForward` object — starting it when a target resolves, restarting on pod
  change, and tearing it down on object deletion (`ForwardProcesses` + `Drop`).
  Verified against kind by making a real HTTP request through the forwarded port
  and confirming it stops after the object is deleted.
- the **Windows host-shell command logic**, unit-tested cross-platform
  (`shell_argv_selects_host_shell_per_platform`): `cmd.exe /S /C` on Windows,
  `sh -c` elsewhere, command string passed through verbatim.
- a **cross-platform daemon transport**: a Unix domain socket on Unix, a
  `127.0.0.1` TCP port (recorded in `daemon.port`) on Windows, with `handle_conn`
  generic over the stream type. This fixed a real bug — the daemon was
  Unix-socket-only and would not compile for Windows.
- **whole-crate Windows compilation, verified locally**: `cargo check --target
  x86_64-pc-windows-gnu` builds clean (C deps included, via mingw-w64), with a
  committed `.cargo/config.toml` making it reproducible from a Unix host. This is
  the cross-compile that surfaced the daemon gap above.
- a **parity CI matrix** (`.github/workflows/parity.yml`): the hermetic unit
  suite on Linux/macOS/`windows-latest` (so the Windows command paths execute on
  a real Windows host) + fmt/clippy + a Linux kind+Docker job for the gated
  integration tests. Authored here; runs in GitHub Actions.

Genuinely remaining (blocked on infrastructure, not missing capabilities):

- **End-to-end Windows runtime behavior.** The crate now *compiles* for Windows
  (verified here) and its command logic is unit-tested, but actually *running* on
  Windows needs a Windows host: this dev environment is arm64 macOS with no
  Windows host or x86-Windows emulator. `parity.yml` runs the unit suite on a real
  `windows-latest` runner — that is the runtime-verification path, not yet executed
  here.
- (No Kubernetes operation remains shelling out under the kube-rs path; the
  port-forward is now a native in-process TCP proxy over `Api::portforward`.)
