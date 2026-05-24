<p align="center">
  <img src=".github/starling-banner.svg" alt="Starling — a murmuration of boids wheeling around the wordmark" width="820" />
</p>

<h1 align="center">Starling</h1>

<p align="center">
  A <b>local dev orchestrator</b>, written in Rust &mdash; a fork/port of
  <a href="https://tilt.dev">Tilt</a> with <b>portless</b>-style named URLs built
  in, <b>redesigned for scaled, agent-first engineering</b>: a central daemon, a
  shared named-URL proxy, and a <b>k9s-style TUI</b> over every running instance.
</p>

<p align="center">
  <a href="#why-starling--built-for-agent-first-engineering"><b>🐦 Why Starling</b></a> &middot;
  <a href="#architecture"><b>🏛️ Architecture</b></a> &middot;
  <a href="#running"><b>🚀 Running</b></a> &middot;
  <a href="#named-urls-integrated-portless"><b>🔗 Named URLs</b></a> &middot;
  <a href="#status--roadmap"><b>🗺️ Roadmap</b></a>
</p>

<p align="center">
<a href="https://github.com/ThousandBirdsInc/starling/commits"><img alt="GitHub Last Commit" src="https://img.shields.io/github/last-commit/ThousandBirdsInc/starling" /></a>
<a href="https://crates.io/crates/starling-devex"><img alt="crates.io version" src="https://img.shields.io/crates/v/starling-devex.svg?cacheSeconds=60" /></a>
<a href="Cargo.toml"><img alt="License Apache-2.0" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" /></a>
</p>

A local dev orchestrator, written in Rust. Starling is a fork/port of
[Tilt](https://tilt.dev) with **portless**-style named URLs built in,
**redesigned for scaled, agent-first engineering** — many humans and AI agents
running many environments in parallel. It's organized around a **central
daemon** with a **k9s-style TUI dashboard**.

- A single background **daemon** owns one shared named-URL proxy, allocates
  ports centrally (so multiple projects never collide), and aggregates every
  running instance's resources.
- `starling up` runs the **engine** for one project (executes real
  Starlingfiles, watches files, runs `local_resource` commands, builds docker
  images, applies Kubernetes manifests) and reports to the daemon.
- `starling` (or `starling dash`) opens a **k9s-style terminal dashboard** of
  every instance's resources, with live logs and trigger.

Serving resources get stable, named `<resource>.<project>.<tld>` URLs through
the shared proxy instead of raw `localhost:PORT`. It also remains
**protocol-compatible with Tilt's React frontend** (`starling up --web` serves
the original web UI for a single instance).

## Architecture

```
                          ┌──────────────────────────┐
   starling (TUI) ───────►│        starling daemon     │
                          │  • shared proxy  :1360     │◄─── browser
   starling up (proj A) ─►│  • central port allocation │     <name>.A.localhost
   starling up (proj B) ─►│  • aggregated dashboard    │     <name>.B.localhost
                          └──────────────────────────┘
```

Control plane: newline-JSON over a Unix socket at `~/.starling/daemon.sock`
(one request/response per connection). The daemon is auto-started by `up`/`dash`
if not already running.

> Lineage: the web UI and the wire protocol come from Tilt (Apache-2.0), and
> the named-URL proxy is ported from portless. Because the frontend is Tilt's,
> a few on-the-wire identifiers keep their original names (`tiltStartTime`,
> `tiltfileKey`, the `/api/set_tiltfile_args` route) — those are the frontend
> contract, not Starling branding.

## Why Starling — built for agent-first engineering

Starling is a fork/port of [Tilt](https://tilt.dev), but with a different
design goal: **work well when much of the engineering is done by AI agents, at
scale, in parallel.** Tilt was built around one developer at one web UI.
Starling assumes a fleet of humans *and* agents spinning up many environments at
once — across projects, git worktrees, and concurrent tasks — and optimizes for
that:

- **Names, not ports.** Every service is addressable as
  `<resource>.<project>.localhost` instead of an ephemeral `localhost:PORT`.
  Agents reference services by stable name, so prompts, configs, and generated
  scripts don't break when ports shuffle. (This is portless's "for humans and
  agents" idea, built in.)
- **A central daemon that coordinates many instances.** Dozens of `starling up`
  processes — one per project / worktree / agent task — share a single proxy and
  a single port-allocation authority, so parallel agents never collide on ports
  or step on each other's URLs.
- **A machine-readable control plane.** The daemon speaks newline-delimited JSON
  over a Unix socket (`~/.starling/daemon.sock`): an agent can register
  environments, query the aggregated state of *every* running instance, stream
  logs, and trigger builds programmatically — the same API the dashboard uses.
  No scraping a web UI to find out what's running.
- **One pane of glass for the whole fleet.** The k9s-style TUI shows every
  instance's resources together, so a human supervising a swarm of agents has a
  single place to watch builds, statuses, and logs.

Honest scope: Starling keeps Tilt's wire protocol and an optional web UI for
compatibility, but the default experience is daemon + TUI + named URLs. It is
not yet a drop-in replacement for all of Tilt — see the roadmap below.

## What's here

- `web/` — Tilt's React frontend, vendored unchanged. Built with Yarn (Berry) +
  Create React App into `web/build`.
- `src/` — the Rust server + engine:
  - `api/v1alpha1.rs` — Kubernetes-style resource types (`UISession`,
    `UIResource`, `UIButton`, `Cluster`) matching `web/src/core.d.ts`.
  - `api/webview.rs` — the `View` envelope and log model (`web/src/webview.d.ts`).
  - `store.rs` — in-memory object store + change-notification channel; serves a
    full `View` on connect and incremental deltas (the log-checkpoint protocol).
  - `server.rs` — axum routes + the `/ws/view` websocket.
  - `starlingfile/` — Starlark Starlingfile execution (`starlark` crate)
    producing `Manifest`s. Builtins match Tilt's API: `local_resource`
    (full kwargs + `trigger_mode`), `local`, `read_file`, `watch_file`,
    `docker_build`, `custom_build`, `k8s_yaml`, `k8s_resource`, `filter_yaml`,
    `kustomize`, `helm`, `docker_compose`, `port_forward`, live_update steps
    (`sync`/`run`/`fall_back_on`/`restart_container`/`initial_sync`), `include`,
    `load()`, `load_dynamic`, `default_registry`, `allow_k8s_contexts`,
    `k8s_kind`, plus the `alias` extension. `TRIGGER_MODE_AUTO`/`_MANUAL` constants.
  - `k8s.rs` — multi-doc YAML parsing → workloads, container images, selectors.
  - `proxy.rs` — embedded named-URL reverse proxy (ported from portless): a
    Host-header router mapping `<name>.<tld>` → `127.0.0.1:<port>`, with
    `X-Forwarded-*` injection, loop detection, WebSocket/streaming, route registry.
  - `engine.rs` — the build/run loop: runs update/serve commands, watches deps
    (`notify`), builds images (`docker build`), deploys (`kubectl apply`),
    watches pod status + streams pod logs (`kubectl logs -f`), and assigns each
    `serve_cmd` a `PORT` + named proxy route.
  - `daemon/` — the central daemon: `protocol.rs` (UDS request/response +
    snapshot types), `client.rs` (client + auto-start), `mod.rs` (state,
    port leasing, shared proxy, command queue, instance pruning).
  - `tui/` — the k9s-style dashboard (`ratatui` + `crossterm`): resource table
    across all instances, live log pane, navigation, trigger.
  - `seed.rs` — session + cluster environment info.
  - `main.rs` — CLI: `up`, `daemon`, `dash` (+ the per-instance reporter loop).

## API surface (matches Tilt's `internal/hud/server/server.go`)

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/websocket_token` | CSRF token required by the websocket |
| GET | `/ws/view` | streams `View` JSON (full, then deltas); needs `?csrf=<token>` |
| GET | `/api/view` | full `View` as JSON |
| GET | `/api/snapshot/:id` | a `Snapshot` wrapping the view |
| POST | `/api/trigger` | queue a build (`{manifest_names, build_reason}`) |
| POST | `/api/override/trigger_mode` | set trigger mode on manifests |
| POST | `/api/set_tiltfile_args` | replace Starlingfile args (route name fixed by the frontend) |
| POST | `/api/analytics` / `/api/analytics_opt` | accepted, no-op |
| * | (fallback) | static frontend assets with SPA index fallback |

## Running

The crate is published as **`starling-devex`**; the installed CLI is **`starling`**:

```bash
cargo install starling-devex          # installs the `starling` command
```

```bash
# In each project directory (auto-starts the daemon on first run):
cargo run -- up                       # or: starling up
cargo run -- up --file path/to/Starlingfile
cargo run -- up --dry-run             # k8s applies use --dry-run=client (safe)
cargo run -- down                     # stop the instance for this project
cargo run -- down --file path/to/Starlingfile

# Open the shared dashboard (k9s-style TUI) from anywhere:
cargo run                             # or: starling   /   starling dash
cargo run -- status --json            # machine-readable daemon/resource/routes
cargo run -- logs paas-ui --tail 80    # recent logs for one resource
cargo run -- skills install --target all
cargo run -- skills install --target claude --scope project

# Run the daemon explicitly (optional; up/dash auto-start it):
cargo run -- daemon --proxy-port 1360 --tld localhost
cargo run -- daemon --shutdown        # stop all instances and the daemon
cargo run -- daemon --restart         # restart it, or start it if absent
```

**Drop-in for existing Tilt projects:** `starling up` loads `./Starlingfile` if
present, otherwise falls back to `./Tiltfile` — so you can run it in an existing
Tilt repo with no renaming. (`--file <path>` overrides the auto-detection.)
Starling implements Tilt's Tiltfile builtins, so most existing Tiltfiles run
unchanged.

In the **TUI**: `j`/`k` (or ↑/↓) move, `↵` opens a detail view, `o` opens the
selected resource's URL in the browser, `l` opens full-screen logs, `t` triggers,
`R` restarts, `p` changes the preferred backend port, `/` filters resources,
`r` refreshes, `q` quits. The table shows
every instance's resources (instance · resource · type · update · runtime · pod ·
backend port · URL) with a live log pane for the selection. In full-screen logs,
`/` filters log lines by regex (case-insensitive, with substring fallback) and
`PgUp`/`PgDn` scroll.

The bundled `./Starlingfile` demonstrates `local_resource` (one-shot `cmd`,
dependency ordering, a `serve_cmd` that gets a named URL, and `deps` file-watch
rebuilds). Run `starling up` in two different project directories to see
central port allocation (distinct ports, no conflicts) and per-project named
URLs in one dashboard.

### Legacy web UI

`starling up --web --port 10360` additionally serves Tilt's original React UI
for that one instance (the websocket `View` protocol is still implemented).

### Kubernetes (local cluster)

Starling deploys to whatever cluster your current kube-context points at, via
`kubectl apply` + pod-status watch + `kubectl logs`. For local development the
expectation — same as Tilt — is a **local cluster** (kind / k3d / minikube /
Docker Desktop k8s), *not* a remote/production cluster.

```bash
# one-time: a local cluster (kind shown; k3d/minikube/Docker Desktop also work)
kind create cluster --name starling

# point Starling at it (kind set your context to kind-starling); then:
starling up         # builds images (bollard) + applies manifests + watches pods
```

To target a cluster without changing your default context, run Starling with an
explicit `KUBECONFIG` (the engine shells `kubectl`, which respects it):

```bash
KUBECONFIG=~/.kube/kind.yaml starling up
```

### `--dry-run`

`kubectl apply` is invoked with `--dry-run=client --validate=false`, so nothing
on the cluster is mutated. Useful for validating the deploy pipeline against any
context — including when you don't have a local cluster up yet. (Pod watching is
skipped in dry-run since nothing is deployed.)

## Named URLs (integrated portless)

portless's functionality is built in: instead of juggling random `localhost:PORT`
numbers, every serving resource gets a stable, named URL through an embedded
reverse proxy.

- Each `local_resource` with a `serve_cmd` is assigned a free port (passed as
  `$PORT`/`$HOST` to the child) and registered as `<name>.<tld>`. Its UI link
  becomes e.g. `http://webserver.localhost:1360`.
- Every `cmd` and `serve_cmd` receives service-discovery env vars for known
  services: `STARLING_<RESOURCE>_URL`, `STARLING_<RESOURCE>_HOST`,
  `STARLING_<RESOURCE>_PORT`, plus `STARLING_SERVICES_JSON`.
- The Starling UI itself is mounted at `starling.<tld>`.
- `alias(name, port)` (Starlingfile builtin) registers a static route to any
  already-running server — a Docker container, a k8s port-forward, etc.
- `local_resource(..., serve_port=N)` prefers a fixed port; if that port is busy
  or already claimed by another route, Starling falls back to a free `$PORT` and
  logs a warning.
- `starling_port(name, preferred=N)` leases a named host TCP port through the
  same central allocator and returns a shell expansion for the matching
  `STARLING_<NAME>_PORT` env var. Use this for non-HTTP services such as
  Postgres that need port negotiation but cannot use the HTTP named-URL proxy.

`.localhost` hostnames resolve to `127.0.0.1` automatically in browsers, so the
URLs just work. Flags: `--proxy-port` (default `1360`), `--tld` (default
`localhost`), `--no-proxy` to disable. The proxy injects `X-Forwarded-*`
headers, detects forwarding loops, and proxies WebSockets/streaming.

**HTTPS:** pass `--tls` (to `up`/`daemon`) and the daemon mints a per-hostname
cert on the fly from a local CA; run `starling trust` once to install the CA and
avoid browser warnings. Plain HTTP on the TLS port 308-redirects to HTTPS.

```bash
# webserver serve_cmd reachable at its named URL through the proxy
curl -H "Host: webserver.localhost" http://127.0.0.1:1360/
# with --tls:
starling trust
curl https://webserver.localhost:1360/
```

## Default ports

| Service | Port |
| --- | --- |
| Web UI / HUD | `10360` (`--port`) |
| Named-URL proxy | `1360` (`--proxy-port`) |

(Tilt's own defaults are `10350`/`1355`; Starling uses `10360`/`1360` so it can
run alongside a real Tilt without colliding.)

## Status & roadmap

A working dev tool for local + Kubernetes resources.

1. ✅ HTTP/websocket server, full `View` type model, frontend served & rendering.
2. ✅ Starlingfile (Starlark) execution + file watching → real `local_resource`
   (`cmd`, `serve_cmd`, `deps`, `resource_deps`, links), `local()`, `read_file`.
3. ✅ Docker image builds (matched to workload images).
4. ✅ Kubernetes deploy via `kubectl apply` + pod status watch + `kubectl logs`
   streaming, with automatic `kind load docker-image` for local kind clusters.
   Verified end-to-end against kind (`examples/k8s`).
5. ✅ Starlingfile live reload: editing it re-executes and reconciles resources
   (adds/removes), then rebuilds.
6. ✅ Integrated portless: embedded reverse proxy + named URLs for serving
   resources, `alias()`/`serve_port`, WebSocket/streaming support.
7. ✅ Central daemon + k9s-style TUI: shared proxy, central port allocation
   (no cross-instance conflicts), per-project named URLs, aggregated dashboard
   with live logs and trigger.
8. ✅ Embedded apiserver subset (`/proxy/apis/tilt.dev/v1alpha1/uibuttons` +
   `/status`): the web UI can click buttons and toggle resource disable.
9. ✅ `load()` / `include()` multi-file Starlingfiles; every read file
   (includes, load targets, `read_file`, `watch_file`) is watched for reload.
10. ✅ `docker_compose()` (each service becomes a resource) and `live_update`
    (`sync()`/`run()` steps that `kubectl cp`/`exec` into a live pod instead of a
    full rebuild). _live_update's in-pod execution needs a running cluster to
    exercise; the Starlark model + watch wiring are complete._
11. ✅ Native Docker builds via **bollard** (Docker API) instead of shelling out.
    _k8s stays on `kubectl` — see note below._
12. ✅ HTTPS proxy: per-hostname certs minted on the fly (SNI) from a local CA,
    `starling trust` to install the CA, `starling hosts` to sync `/etc/hosts` for
    non-`.localhost` TLDs, plain-HTTP→HTTPS redirect on the same port.
    `--lan` (mDNS) and `--tailscale` modes are wired but experimental.
13. ✅ TUI: `/` filter, Enter detail view, `t` trigger, `R` restart, `p` change
    preferred backend port, PgUp/PgDn log scroll.
14. ✅ Tiltfile API parity: corrected `local_resource` arg order, `trigger_mode`
    (+ manual-mode pending behavior), full `local`/`local_resource` kwargs
    (env/dir/serve_env/serve_dir/labels), `custom_build`, `kustomize`/`helm`,
    `filter_yaml`, `port_forward()`, `k8s_resource` extras (labels, objects,
    extra_pod_selectors), all live_update steps, `load_dynamic`, `k8s_kind`.

### Notes / honest limitations

- **Native k8s client:** Kubernetes deploys by shelling out to `kubectl` (apply
  / get pods / logs) plus `kind load docker-image` to load locally-built images
  into a kind cluster (like Tilt). The full inner loop — build (bollard) → kind
  load → apply → pod-status watch → `live_update` sync into the running pod — is
  **verified end-to-end against a local kind cluster** (see `examples/k8s`).
  Swapping the shell-outs for a native `kube` client is a clean follow-up.
- **live_update / `--lan` / `--tailscale`:** implemented but not exercised by the
  test suite (they need a live pod / LAN mDNS / a tailnet respectively).
- **Partial-fidelity builtins:** `load_dynamic` runs the target's side effects
  but returns an empty symbol dict (use `load()` for imports); `k8s_kind`/
  `k8s_image_json_path` are accepted but don't yet inject images into CRDs;
  `docker_build(target=…)` is accepted but bollard's classic builder ignores it.

### Tests

`cargo test` covers the k8s YAML parser (workload/image/selector extraction),
docker-image↔build-ref matching, proxy hostname/URL formatting, and the route
registry. Daemon, reload, named-URL proxy (HTTP+HTTPS), docker_compose, and
native image builds are verified end-to-end against a local Docker daemon and a
kind cluster.
