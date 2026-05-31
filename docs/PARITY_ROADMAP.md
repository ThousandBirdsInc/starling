# Starling → Tilt Parity Roadmap

Prioritized plan for closing the remaining gaps in
[`../TILT_PARITY_GAPS.md`](../TILT_PARITY_GAPS.md), written after the
`7310c42` parity push (object store, 6 cluster-backed reconcilers, kube-rs
transport, CI mode, Windows compile). It supersedes the generic 10-step list
at the end of that doc, which predates the push.

Ordering principle: do the work that **unblocks** the most downstream parity
first. The object store exists but is still mostly a mirror — the throughline of
this roadmap is moving the engine from "imperative loop that mirrors state into
objects" to "objects that drive behavior via reconcilers." Items are sequenced
so each tier's prerequisites land in an earlier tier.

Legend: **P0** = highest leverage / blocks the most; **P3** = polish.

---

## Status (updated 2026-05-31)

**P0 (image pipeline reconciliation) — DONE.** Builds capture a content digest
(local image ID); `ImageMap`/`DockerImage`/`CmdImage` carry it on
`status.image`/`status.imageID`; an immutable content-addressed deploy tag
(`<repo>:starling-<digest>`) is tagged + kind-loaded; and the `KubernetesApply`
reconciler resolves + injects `ImageMap.status.image` into workloads at apply
time (object-driven). Unit-tested (`content_addressed_ref_derives_immutable_tag_from_digest`,
`injects_resolved_image_map_refs_into_workload_yaml`) + a gated kind integration
test (`k8s_integration_apply_injects_image_map`). Known limitation: with
`default_registry`, injection falls back to the load-time cluster-ref rewrite
(content tag not injected) — tracked in P3.

**P1 (object-driven engine) — DONE.**
- *FileWatch reconciler*: a `spawn_file_watch_controller` owns the notify
  watchers, driven by `FileWatch` object add/modify/delete; the object spec
  (watched paths, ignores, `manual`, `fallbackPaths`) is the source of truth.
  Replaced the manifest-wired `start_watchers` + `watcher_generation`.
- *TriggerQueue + crash-rebuild*: a `TriggerQueue` singleton — `spec.queue` is a
  client-writable trigger control surface (nonce-deduped `TriggerQueueReconciler`),
  `status.queue` is the engine-maintained build-queue view. Serve crashes now
  auto-restart (RestartOn-crash) with backoff + a cap.
- *Pod-log in the maintained loop*: the controller manager follows every
  `PodLogStream` pod (one follow per pod), which is idempotent under continuous
  reconciliation without a cursor; replaced the per-resource first-pod follow.
- *Per-pod fan-out*: logs follow all pods (above); initial-sync runs once per
  observed pod. Port-forward stays first-pod (a local port binds one pod).

Tests: 149 unit/integration pass; +20 gated integration tests; fmt + clippy
clean. Not yet implemented under P1 #2: `StartOnSpec`/`StopOnSpec` as distinct
objects (crash-rebuild covers the RestartOn-crash case) — moved to P3.

**P2 (CLI write verbs) — DONE.** The engine always binds its authoritative
object-store API server and reports `api_addr` to the daemon; the CLI discovers
it via `GetState` and does direct HTTP CRUD — a real request/response path that
surfaces `201`/`409`/`404`. Added `apply`/`create`/`patch`/`delete`/`wait`/`edit`,
`down -- <args>`, and an `lsp` unsupported stub. 152 tests pass (incl. an
in-process end-to-end write test). See the P2 section for details/caveats.

---

## P0 — Image pipeline reconciliation (immutable deploy refs)

**Why first:** This is the weakest area vs. Tilt and the one users *notice* — without
digest-pinned image injection, deploys aren't reproducible and rebuilds don't
reliably roll pods. `ImageMap`/`DockerImage`/`CmdImage` objects are already
populated but carry no resolved status and nothing consumes them. Everything in
the build→deploy path depends on this being real.

Work:
1. On build success, write the resolved ref + **digest** onto the `DockerImage`/
   `CmdImage` object status, then onto an `ImageMap` (`spec.selector` →
   `status.image`, the cluster-reachable ref).
2. Add an `ImageMap` reconciler (or fold into the apply path): the
   `KubernetesApply` reconciler resolves each workload image placeholder against
   the matching `ImageMap.status.image` **at apply time**, injecting the
   immutable digest ref rather than the build-time tag.
3. Make `default_registry` / `host_from_cluster` / `single_name` rewrites flow
   through the `ImageMap` ref, not the current inline YAML rewrite.
4. Tests: extend the kind integration suite — build → digest on ImageMap →
   apply injects digest → pod runs the digest ref (read back from the pod spec).

Exit: a rebuild produces a new digest, the ImageMap updates, and the next apply
rolls the workload to that digest with no tag mutation.

Touch points: `src/engine.rs` (build result handoff), `src/api/store.rs`,
`src/kube_client.rs` (`apply_yaml`), `src/starlingfile/mod.rs` (registry rewrite).

---

## P1 — Make the engine object-driven (close the architecture gap)

**Why:** The maintained controller loop owns only 3 status controllers
(discovery, pod-watch, port-forward target). The *mutating* runtime — builds,
pod-log streaming, live-update, file watching — still runs through the engine's
imperative loop. Until these are object-driven, CLI write verbs (P2) and most
remaining Kubernetes/live-update fidelity can't be honored.

Work, in dependency order:
1. **`FileWatch` reconciler** — drive file watching from `FileWatch` objects
   instead of the engine's `notify` watchers wired directly to resources. This
   is the smallest mutating reconciler and unblocks the build-trigger graph.
2. **Build/trigger control objects** — introduce `TriggerQueue` semantics and
   route `force-trigger` + manual triggers through it; add `StartOnSpec`/
   `RestartOnSpec`/`StopOnSpec` for `Cmd`/serve lifecycle. Add crash-rebuild.
3. **`PodLogStream` reconciler in the maintained loop** — currently excluded
   because it appends (non-idempotent). Make it cursor-based (track last offset
   per stream) so continuous reconciliation is safe, then move it in.
4. **Per-pod fan-out** — stop selecting "first pod only" for log stream /
   port-forward / initial-sync; iterate `KubernetesDiscovery.status.pods[]`
   (already populated by `reconcile_pod_watch`).

Exit: deleting/patching a `FileWatch` or trigger object changes engine behavior;
logs and forwards cover all pods of a resource.

Touch points: `spawn_controller_manager` in `src/engine.rs`, `src/api/store.rs`,
the reconciler registry.

---

## P2 — CLI write verbs + reconciler-owned reverse direction

**Why:** Blocked on P1. Write verbs (`apply`/`delete`/`patch`/`wait`/`edit`/
`create`) are intentionally absent because no reconciler owns the reverse
direction — a CLI `delete` of an engine-managed object is undone on the next
reload. Once P1 makes objects authoritative, these become safe.

Work:
- `starling create filewatch|cmd|repo|ext`, `apply`, `delete`, `patch`, `wait`,
  `edit` — routed through the object store, with the daemon↔engine channel
  upgraded from poll/fire-and-forget to request/response so conflicts surface.
- `starling args` passthrough for `down` (currently `up`/`ci`/web only).
- `starling lsp` server command (or stub + clear "unsupported").

Exit: `starling delete <obj>` sticks; `starling apply -f` round-trips through
the store and reports apiserver conflicts.

**Status: DONE (2026-05-31).** Realized via a **direct request/response HTTP
path** rather than upgrading the poll channel: the engine always binds its
authoritative object-store API server (ephemeral localhost port when `--web` is
off), reports `api_addr` to the daemon (in `Update`, surfaced on
`InstanceState`), and the CLI discovers it via `GetState` and issues HTTP CRUD
directly — so real apiserver status (`201`/`409`/`404`) flows back. Added
`apply -f`, `create -f`, `patch -p`, `delete`, `wait --for=<path>=<value>`,
`edit` (`$EDITOR`), `down -- <args>` (accepted), and an `lsp` unsupported stub.
Verified by an in-process end-to-end test (`cli_write_verbs_round_trip_against_object_store`)
plus pure-helper tests. `create -f`/`apply -f` use generic object files;
typed `create filewatch|cmd|repo|ext` constructors aren't added (the file form
covers them). Note: deleting an *engine-managed* object is transient (the next
reload re-materializes it); client-created objects persist until reload.

---

## P3 — Fidelity & polish (parallelizable, no hard ordering)

These are independent and can be picked up opportunistically once P0–P2 land.

**Docker Compose** — promote from status *reader* to lifecycle controller:
health/status tracking, port/link inference, `docker compose build` integration,
Compose live-update, Compose-specific down/cleanup.

**Live update** — multi-container selection, per-step status, real
`restart_container` (not pod delete), Compose live-update path.

**Kubernetes fidelity** — owner-reference traversal (makes `discovery_strategy`
`default` ≠ `selectors-only` real), event watching, server-side diff status,
`objects=` regexp + API-group selector parts, `k8s_custom_deploy` selector
derivation from apply output, `allow_duplicates`.

**Tiltfile stdlib** — full `v1alpha1` constructors (all types, auto-register),
`config.parse` pflag usage output, real runtime for `disable_snapshots` /
`docker_prune_settings` / `analytics_settings` / `version_settings(check_updates)`,
`ext://` in `include()`/`load_dynamic()`, default github extension repo +
`Extension`/`ExtensionRepo` reconciler, full JSONPath for `k8s_image_json_path`.

**Build control** — `StartOnSpec`/`StopOnSpec` as distinct objects (the
RestartOn-crash case is covered by P1's crash-rebuild); `ToggleButton`
reconciler (toggle still flows through the UIButton path).

**Build** — BuildKit caching for plain bollard builds; BuildKit progress parity.
Apply-time ImageMap injection under `default_registry` (P0 injects only when the
ImageMap selector matches the workload's ref; with a registry rewrite the
load-time cluster-ref path is used instead).

**Web/HUD** — move generic CRUD under Tilt's exact `/proxy/apis/...` path with
schema validation; drive more object writes into engine behavior; complete
snapshot create/share; base64-decode secret scrubbing; model build warnings.

**Remaining CLI** — `docker`, `docker-prune`, `demo`, `analytics`,
`alpha *`, `verify-install`, snapshot share/upload, more `up` flags.

---

## Infra-blocked — verification only (no capability work)

Not on the priority track because the capability exists; only execution is
pending and needs hardware/CI this dev box can't provide:

- **Windows runtime** — crate compiles (`x86_64-pc-windows-gnu`) and command
  logic is unit-tested cross-platform; needs a real run. Mechanism exists:
  `parity.yml`'s `windows-latest` leg. **Action: run `parity.yml` in CI.**
- **`parity.yml`** authored but never executed here (no Actions runners) — the
  kind+Docker integration leg and multi-OS matrix are only locally validated.
- Service install is macOS LaunchAgent only (Linux systemd / Windows service
  units still to write).

---

## Suggested sequencing summary

```
P0  Image pipeline + digest injection      ── DONE (reproducible deploys)
P1  Engine → object-driven (FileWatch,      ── DONE (unblocks P2 + most fidelity)
     trigger queue, pod-log loop, per-pod)
P2  CLI write verbs (needs P1)              ── DONE (direct HTTP to engine API)
P3  Compose / live-update / k8s fidelity /  ── parallelizable polish
     stdlib / build / web / remaining CLI
─── Infra: run parity.yml in CI (Windows + integration verification)
```
