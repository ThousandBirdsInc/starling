//! The build/run engine: turns Starlingfile manifests into live resources.
//!
//! Mirrors the role of Go's `internal/engine`. Responsibilities:
//!   * materialize each [`Manifest`] as a `UIResource` in the store,
//!   * run each resource's `update_cmd` as a subprocess (the "build"),
//!     streaming stdout/stderr into the log store under the resource span,
//!   * keep `serve_cmd` processes running and reflect their runtime status,
//!   * watch each resource's `deps` and rebuild on change,
//!   * service manual triggers arriving on the build channel.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use notify::{Event, RecursiveMode, Watcher};
use regex::Regex;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::api::v1alpha1::*;
use crate::proxy::PortReservation;
use crate::starlingfile::{
    self, Cmd, IgnoreRule, LoadOptions, Manifest, NamedPortLease, PortForwardSpec, ProbeAction,
    ProbePort, ReadinessProbe, TargetKind,
};
use crate::store::{BuildRequest, Store};

const GENERATED_DOCKERFILE: &str = ".starling/Dockerfile.generated";

pub struct Engine {
    store: Arc<Store>,
    manifests: Vec<Manifest>,
    /// Index for quick lookup by name.
    by_name: HashMap<String, usize>,
    /// Incoming build requests (from /api/trigger and file watchers).
    build_rx: mpsc::UnboundedReceiver<BuildRequest>,
    /// Cloned for file watchers to enqueue rebuilds.
    build_tx: mpsc::UnboundedSender<BuildRequest>,
    /// When true, `kubectl apply` runs with `--dry-run=client` and pod watching
    /// is skipped (nothing is mutated on the cluster).
    dry_run: bool,
    /// Path to the Starlingfile, re-executed on reload.
    config_path: PathBuf,
    /// Tiltfile args passed after `starling up --`, reused on config reload.
    tiltfile_args: Vec<String>,
    /// All config files to watch (Starlingfile + includes + read_file paths).
    config_files: Vec<PathBuf>,
    /// Resources whose `serve_cmd` is already running (avoid duplicates on reload).
    started_serves: HashSet<String>,
    /// Number of times each serve_cmd has started during this engine session.
    serve_start_counts: HashMap<String, u32>,
    /// Abort handles for running serve_cmd tasks (used to restart/kill).
    serve_tasks: HashMap<String, ServeTask>,
    /// Restart requests (resource names) from the dashboard.
    restart_rx: mpsc::UnboundedReceiver<String>,
    /// Live Tiltfile arg replacements from the web frontend.
    tiltfile_args_rx: mpsc::UnboundedReceiver<Vec<String>>,
    /// Preferred port changes from the dashboard.
    port_rx: mpsc::UnboundedReceiver<(String, u16)>,
    /// Session-scoped preferred port overrides from the dashboard.
    port_overrides: HashMap<String, u16>,
    /// Named TCP port leases requested by the Starlingfile.
    port_leases: Vec<NamedPortLease>,
    /// Resolved named TCP port leases, keyed by Starlingfile name.
    named_ports: HashMap<String, PortReservation>,
    /// Runtime service references exposed to commands as STARLING_* env vars.
    service_registry: Arc<Mutex<HashMap<String, ServiceEndpoint>>>,
    /// Named-URL proxy handle (daemon or local); `None` disables named URLs.
    proxy: Option<crate::proxy::ProxyHandle>,
    /// Kubernetes-style API object store. Holds the declarative objects derived
    /// from the manifests (`KubernetesApply`, `ImageMap`, `FileWatch`, ...) and
    /// is the control surface several reconcilers act on: image injection at
    /// apply time, the FileWatch controller, the TriggerQueue, and the
    /// maintained controller manager (discovery / pod-watch / port-forward /
    /// pod-log following).
    api_objects: Arc<crate::api::store::ApiObjectStore>,
    /// Caps concurrent parallel local-resource updates
    /// (`update_settings(max_parallel_updates=...)`).
    parallel_sem: Arc<tokio::sync::Semaphore>,
}

struct ServeTask {
    abort: tokio::task::AbortHandle,
    pid: Arc<Mutex<Option<u32>>>,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ServiceEndpoint {
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<Store>,
        manifests: Vec<Manifest>,
        build_rx: mpsc::UnboundedReceiver<BuildRequest>,
        build_tx: mpsc::UnboundedSender<BuildRequest>,
        dry_run: bool,
        config_path: PathBuf,
        tiltfile_args: Vec<String>,
        config_files: Vec<PathBuf>,
        port_leases: Vec<NamedPortLease>,
        restart_rx: mpsc::UnboundedReceiver<String>,
        tiltfile_args_rx: mpsc::UnboundedReceiver<Vec<String>>,
        port_rx: mpsc::UnboundedReceiver<(String, u16)>,
        proxy: Option<crate::proxy::ProxyHandle>,
        api_objects: Arc<crate::api::store::ApiObjectStore>,
        max_parallel_updates: Option<usize>,
    ) -> Self {
        let by_name = manifests
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.clone(), i))
            .collect();
        Engine {
            store,
            manifests,
            by_name,
            build_rx,
            build_tx,
            dry_run,
            config_path,
            tiltfile_args,
            config_files,
            started_serves: HashSet::new(),
            serve_start_counts: HashMap::new(),
            serve_tasks: HashMap::new(),
            restart_rx,
            tiltfile_args_rx,
            port_rx,
            port_overrides: HashMap::new(),
            port_leases,
            named_ports: HashMap::new(),
            service_registry: Arc::new(Mutex::new(HashMap::new())),
            proxy,
            api_objects,
            // Unset means no practical cap (Tilt's default of 3 is not imposed
            // so existing behavior is unchanged unless the setting is given).
            parallel_sem: Arc::new(tokio::sync::Semaphore::new(
                max_parallel_updates.unwrap_or(usize::MAX >> 4),
            )),
        }
    }

    fn reindex(&mut self) {
        self.by_name = self
            .manifests
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.clone(), i))
            .collect();
    }

    /// Materialize each manifest as a `UIResource` in the store, plus a
    /// per-resource DisableToggle button for the web UI.
    fn materialize_all(&self) {
        for (i, m) in self.manifests.iter().enumerate() {
            self.store.upsert_resource(initial_resource(m, i as i32));
            self.store.upsert_button(disable_button(&m.name));
            for note in &m.notes {
                self.store
                    .append_log(Some(&m.name), "INFO", &format!("{note}\n"));
            }
        }
        self.materialize_api_objects();
    }

    /// Mirror the manifests into the API object store as declarative objects:
    /// the `Tiltfile` singleton, a `KubernetesApply` per Kubernetes resource, a
    /// `FileWatch` per resource with watched deps, and a `Cmd` per local
    /// resource. `apply` semantics keep this idempotent across reloads.
    fn materialize_api_objects(&self) {
        let names: Vec<String> = self.manifests.iter().map(|m| m.name.clone()).collect();
        let tiltfile_name = self
            .config_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Tiltfile")
            .to_string();
        self.api_objects.apply(
            "Tiltfile",
            "default",
            &tiltfile_name,
            tiltfile_object(&self.config_path, &names),
        );

        let kubernetes_applies: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::Kubernetes)
            .map(|m| (m.name.clone(), kubernetes_apply_object(m)))
            .collect();
        self.reconcile_kind("KubernetesApply", kubernetes_applies);

        // One FileWatch per resource that watches files (any kind).
        let file_watches: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| !m.deps.is_empty())
            .map(|m| (m.name.clone(), file_watch_object(m)))
            .collect();
        self.reconcile_kind("FileWatch", file_watches);

        // One Cmd per local resource with a one-shot update command.
        let cmds: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::Local && !m.update_cmd.is_empty())
            .map(|m| (m.name.clone(), cmd_object(m)))
            .collect();
        self.reconcile_kind("Cmd", cmds);

        // One PortForward per resource that declares port forwards.
        let port_forwards: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| !m.k8s_port_forwards.is_empty())
            .map(|m| (m.name.clone(), port_forward_object(m)))
            .collect();
        self.reconcile_kind("PortForward", port_forwards);

        // One LiveUpdate per resource that declares live-update steps.
        let live_updates: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| !m.live_update.is_empty())
            .map(|m| (m.name.clone(), live_update_object(m)))
            .collect();
        self.reconcile_kind("LiveUpdate", live_updates);

        // DockerImage/ImageMap per unique built ref; CmdImage for custom builds.
        let mut docker_images: Vec<(String, serde_json::Value)> = Vec::new();
        let mut image_maps: Vec<(String, serde_json::Value)> = Vec::new();
        let mut cmd_images: Vec<(String, serde_json::Value)> = Vec::new();
        let mut seen_images = HashSet::new();
        for m in &self.manifests {
            for build in &m.docker_builds {
                if seen_images.insert(build.image_ref.clone()) {
                    docker_images.push((build.image_ref.clone(), docker_image_object(build)));
                    image_maps.push((build.image_ref.clone(), image_map_object(&build.image_ref)));
                    if build.command.is_some() {
                        cmd_images.push((build.image_ref.clone(), cmd_image_object(build)));
                    }
                }
            }
        }
        self.reconcile_kind("DockerImage", docker_images);
        self.reconcile_kind("ImageMap", image_maps);
        self.reconcile_kind("CmdImage", cmd_images);

        // One KubernetesDiscovery per Kubernetes resource with a pod selector.
        let discoveries: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::Kubernetes && !m.pod_selector.is_empty())
            .map(|m| (m.name.clone(), kubernetes_discovery_object(m)))
            .collect();
        self.reconcile_kind("KubernetesDiscovery", discoveries);

        // One PodLogStream per Kubernetes resource with a pod selector.
        let pod_log_streams: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::Kubernetes && !m.pod_selector.is_empty())
            .map(|m| (m.name.clone(), pod_log_stream_object(m)))
            .collect();
        self.reconcile_kind("PodLogStream", pod_log_streams);

        // DockerComposeService + DockerComposeLogStream per Compose resource.
        let dc_services: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::DockerCompose)
            .map(|m| (m.name.clone(), dc_service_object(m)))
            .collect();
        self.reconcile_kind("DockerComposeService", dc_services);
        let dc_log_streams: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .filter(|m| m.kind == TargetKind::DockerCompose)
            .map(|m| (m.name.clone(), dc_log_stream_object(m)))
            .collect();
        self.reconcile_kind("DockerComposeLogStream", dc_log_streams);

        // One ToggleButton per resource (the enable/disable toggle).
        let toggle_buttons: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .map(|m| (m.name.clone(), toggle_button_object(m)))
            .collect();
        self.reconcile_kind("ToggleButton", toggle_buttons);

        // One disable-source ConfigMap per resource (current disable state).
        let disable_config_maps: Vec<(String, serde_json::Value)> = self
            .manifests
            .iter()
            .map(|m| {
                let name = format!("{}-disable", m.name);
                let disabled = self.store.is_resource_disabled(&m.name);
                (name, disable_config_map_object(disabled))
            })
            .collect();
        self.reconcile_kind("ConfigMap", disable_config_maps);

        // The TriggerQueue singleton. `spec.queue` is a client-writable control
        // surface — entries enqueue builds via the TriggerQueueReconciler — and
        // `status.queue` is the engine-maintained view of what is currently
        // queued to build (Tilt's build-queue visibility).
        self.api_objects.apply(
            "TriggerQueue",
            "default",
            "queue",
            serde_json::json!({ "spec": { "queue": [] }, "status": { "queue": [] } }),
        );

        // The Session singleton records the run's target set.
        self.api_objects
            .apply("Session", "default", "Tiltfile", session_object(&names));
    }

    /// Record the resources currently queued to build on the `TriggerQueue`
    /// object's status (build-queue visibility). A merge patch, so the
    /// client-writable `spec.queue` is preserved.
    fn set_trigger_queue_status(&self, names: &[String]) {
        let _ = self.api_objects.patch(
            "TriggerQueue",
            "default",
            "queue",
            serde_json::json!({ "status": { "queue": names } }),
        );
    }

    /// Record a completed build's resolved deploy ref + content digest on the
    /// image objects. `ImageMap.status.image` is the immutable ref the
    /// `KubernetesApply` reconciler injects into workloads at apply time
    /// (object-driven image injection); `imageID` is the content digest.
    fn write_image_status(&self, image_ref: &str, digest: Option<&str>, deploy_ref: &str) {
        let image_id = digest.unwrap_or_default();
        let _ = self.api_objects.patch(
            "ImageMap",
            "default",
            image_ref,
            serde_json::json!({ "status": { "image": deploy_ref, "imageID": image_id } }),
        );
        let _ = self.api_objects.patch(
            "DockerImage",
            "default",
            image_ref,
            serde_json::json!({ "status": { "ref": deploy_ref, "imageID": image_id } }),
        );
        if self
            .api_objects
            .get("CmdImage", "default", image_ref)
            .is_some()
        {
            let _ = self.api_objects.patch(
                "CmdImage",
                "default",
                image_ref,
                serde_json::json!({ "status": { "ref": deploy_ref, "imageID": image_id } }),
            );
        }
    }

    /// Watch the API object store for force-trigger annotations and turn them
    /// into engine builds. This makes the apiserver a control surface: a client
    /// that PATCHes `metadata.annotations["tilt.dev/force-trigger"]` onto an
    /// object keyed by a resource name enqueues a build for that resource
    /// (mirroring Tilt's button/`tilt trigger` flow). Unknown names are ignored
    /// by `Store::trigger`, so image-keyed objects are harmless.
    fn spawn_api_trigger_watcher(&self) {
        let mut watch = self.api_objects.watch();
        let store = self.store.clone();
        let reconcilers = default_reconcilers();
        self.store.append_log(
            None,
            "INFO",
            &format!("{} object reconcilers active\n", reconcilers.len()),
        );
        tokio::spawn(async move {
            while let Ok(event) = watch.recv().await {
                reconcilers.dispatch(&event, &store);
            }
        });
    }

    /// Upsert `desired` objects of `kind` and delete any stored object of that
    /// kind whose name is no longer present (reconcile to the desired set).
    fn reconcile_kind(&self, kind: &str, desired: Vec<(String, serde_json::Value)>) {
        let live: HashSet<&str> = desired.iter().map(|(n, _)| n.as_str()).collect();
        for stored in self.api_objects.list(kind) {
            if !live.contains(stored.name.as_str()) {
                self.api_objects
                    .delete(kind, &stored.namespace, &stored.name);
            }
        }
        for (name, object) in desired {
            self.api_objects.apply(kind, "default", &name, object);
        }
    }

    /// Run the engine until the build channel closes.
    pub async fn run(mut self) {
        self.reconcile_named_ports().await;
        self.refresh_declared_services();
        self.materialize_all();
        self.spawn_file_watch_controller();
        self.spawn_api_trigger_watcher();
        // Continuously reconcile discovery/port-forward objects against the
        // cluster (maintained-controller model). Skipped in dry-run, where there
        // is no cluster to reconcile against.
        if !self.dry_run {
            spawn_controller_manager(
                self.api_objects.clone(),
                self.store.clone(),
                Duration::from_secs(5),
            );
        }
        for name in self.initial_build_order() {
            self.run_build(&name, true, None).await;
        }

        // Watch all config files (Starlingfile + includes + read_file paths).
        let (reload_tx, mut reload_rx) = mpsc::unbounded_channel::<()>();
        let config_watch_generation = Arc::new(AtomicU64::new(0));
        spawn_config_watcher(
            self.config_watch_files(),
            reload_tx.clone(),
            config_watch_generation.clone(),
        );

        // Service build requests (triggers + file changes) and reloads.
        loop {
            tokio::select! {
                maybe = self.build_rx.recv() => {
                    let Some(first_request) = maybe else { break };
                    // Coalesce a burst of requests.
                    let mut pending: HashMap<String, (bool, Vec<PathBuf>)> = HashMap::new();
                    pending.insert(
                        first_request.name().to_string(),
                        (
                            first_request.force_full(),
                            first_request.changed_paths().to_vec(),
                        ),
                    );
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    while let Ok(extra) = self.build_rx.try_recv() {
                        let force_full = extra.force_full();
                        let changed_paths = extra.changed_paths();
                        pending
                            .entry(extra.name().to_string())
                            .and_modify(|existing| {
                                existing.0 |= force_full;
                                existing.1.extend(changed_paths.iter().cloned());
                                existing.1.sort();
                                existing.1.dedup();
                            })
                            .or_insert((force_full, changed_paths.to_vec()));
                    }
                    // Reflect the coalesced batch on the TriggerQueue status.
                    let queued: Vec<String> = pending.keys().cloned().collect();
                    self.set_trigger_queue_status(&queued);
                    for (name, (force_full, changed_paths)) in pending {
                        let changed_paths = (!changed_paths.is_empty()).then_some(changed_paths);
                        if self.can_run_local_update_in_parallel(&name) {
                            let m = self.manifests[*self.by_name.get(&name).unwrap()].clone();
                            let store = self.store.clone();
                            let registry = self.service_registry.clone();
                            let sem = self.parallel_sem.clone();
                            tokio::spawn(async move {
                                // Cap concurrent parallel updates; the permit is
                                // held for the duration of the build.
                                let _permit = sem.acquire_owned().await;
                                run_parallel_local_build(name, m, store, registry).await;
                            });
                        } else {
                            self.run_build(&name, force_full, changed_paths).await;
                        }
                    }
                    // Batch drained: nothing left queued.
                    self.set_trigger_queue_status(&[]);
                }
                _ = reload_rx.recv() => {
                    // Drain extra reload signals from the same save burst.
                    while reload_rx.try_recv().is_ok() {}
                    self.reload().await;
                    spawn_config_watcher(
                        self.config_watch_files(),
                        reload_tx.clone(),
                        config_watch_generation.clone(),
                    );
                }
                restart = self.restart_rx.recv() => {
                    let Some(name) = restart else { continue };
                    self.restart(&name).await;
                }
                args = self.tiltfile_args_rx.recv() => {
                    let Some(args) = args else { continue };
                    self.tiltfile_args = args;
                    let span = self.config_span();
                    self.store
                        .append_log(Some(&span), "INFO", "Tiltfile args changed; reloading...\n");
                    self.reload().await;
                    spawn_config_watcher(
                        self.config_watch_files(),
                        reload_tx.clone(),
                        config_watch_generation.clone(),
                    );
                }
                port = self.port_rx.recv() => {
                    let Some((name, port)) = port else { continue };
                    self.change_port(&name, port).await;
                }
            }
        }
    }

    /// Log-span label for the config file, e.g. `(Tiltfile)` / `(Starlingfile)`.
    fn config_span(&self) -> String {
        let name = self
            .config_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("Starlingfile");
        format!("({name})")
    }

    fn config_watch_files(&self) -> Vec<PathBuf> {
        let mut watch = self.config_files.clone();
        if watch.is_empty() {
            watch.push(self.config_path.clone());
        }
        watch
    }

    /// Re-execute the config and reconcile resources with the new manifests.
    async fn reload(&mut self) {
        let span = self.config_span();
        self.store
            .append_log(Some(&span), "INFO", "Config changed; reloading...\n");
        let result = match starlingfile::load_with_options(
            &self.config_path,
            LoadOptions {
                args: self.tiltfile_args.clone(),
                ..LoadOptions::default()
            },
        ) {
            Ok(r) => r,
            Err(e) => {
                self.store
                    .append_log(Some(&span), "ERROR", &format!("Reload failed: {e}\n"));
                return;
            }
        };
        for line in result.log.lines() {
            self.store
                .append_log(Some(&span), "INFO", &format!("{line}\n"));
        }

        // Remove resources that no longer exist.
        let new_names: HashSet<String> = result.manifests.iter().map(|m| m.name.clone()).collect();
        let removed: Vec<String> = self
            .manifests
            .iter()
            .map(|m| m.name.clone())
            .filter(|n| !new_names.contains(n))
            .collect();
        for name in removed {
            self.port_overrides.remove(&name);
            self.serve_start_counts.remove(&name);
            self.stop_serve(&name, "Stopping serve_cmd; resource was removed...\n")
                .await;
            self.store.remove_resource(&name);
        }

        self.store.set_scrub_secrets(result.secret_values.clone());
        self.manifests = result.manifests;
        self.port_leases = result.port_leases;
        self.apply_port_overrides();
        self.reconcile_named_ports().await;
        self.refresh_declared_services();
        self.config_files = result.config_files;
        self.reindex();
        let stopped_serves: Vec<String> = self
            .started_serves
            .iter()
            .filter(|name| {
                self.by_name
                    .get(name.as_str())
                    .map(|&i| self.manifests[i].serve_cmd.is_empty())
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        for name in stopped_serves {
            self.stop_serve(
                &name,
                "Stopping serve_cmd; resource no longer has serve_cmd...\n",
            )
            .await;
        }
        // Re-materialize updates the FileWatch objects; the FileWatch controller
        // reacts to those object events to (re)start/stop watchers — no explicit
        // re-wiring needed here (the object-driven path).
        self.materialize_all();
        self.store.append_log(
            Some(&span),
            "INFO",
            &format!("Config reloaded ({} resources)\n", self.manifests.len()),
        );
        for name in self.initial_build_order() {
            self.run_build(&name, true, None).await;
        }
    }

    fn apply_port_overrides(&mut self) {
        for m in &mut self.manifests {
            if let Some(port) = self.port_overrides.get(&m.name) {
                m.serve_port = Some(*port);
            }
        }
    }

    async fn reconcile_named_ports(&mut self) {
        let desired = desired_port_leases(&self.port_leases);
        let removed: Vec<(String, PortReservation)> = self
            .named_ports
            .iter()
            .filter(|(name, existing)| desired.get(*name) != Some(&existing.preferred))
            .map(|(name, reservation)| (name.clone(), reservation.clone()))
            .collect();
        for (name, reservation) in removed {
            if let Some(proxy) = &self.proxy {
                proxy.release_port(reservation.port).await;
            }
            self.named_ports.remove(&name);
            self.service_registry.lock().unwrap().remove(&name);
        }

        for (name, preferred) in desired {
            if self.named_ports.contains_key(&name) {
                continue;
            }
            let reservation = match &self.proxy {
                Some(proxy) => proxy.reserve_port(preferred).await,
                None => preferred.map(|port| PortReservation {
                    port,
                    preferred,
                    conflict: false,
                }),
            };
            let reservation = match reservation {
                Some(reservation) => reservation,
                None => match crate::proxy::find_free_port().await {
                    Ok(port) => PortReservation {
                        port,
                        preferred,
                        conflict: preferred.is_some(),
                    },
                    Err(err) => {
                        self.store.append_log(
                            Some(&self.config_span()),
                            "ERROR",
                            &format!("Could not allocate starling_port({name:?}): {err}\n"),
                        );
                        continue;
                    }
                },
            };
            if reservation.conflict {
                if let Some(preferred) = reservation.preferred {
                    self.store.append_log(
                        Some(&self.config_span()),
                        "WARN",
                        &format!(
                            "Configured starling_port({name:?}, preferred={preferred}) is unavailable; using fallback port {}\n",
                            reservation.port
                        ),
                    );
                }
            }
            self.named_ports.insert(name, reservation);
        }
    }

    fn refresh_declared_services(&self) {
        let mut registry = self.service_registry.lock().unwrap();
        let mut keep = HashSet::new();
        for m in &self.manifests {
            if m.serve_cmd.is_empty() {
                continue;
            }
            keep.insert(m.name.clone());
            let entry = registry.entry(m.name.clone()).or_default();
            if let Some(proxy) = &self.proxy {
                entry.host = Some(proxy.hostname(&m.name));
                entry.url = Some(proxy.url_for(&m.name));
            }
        }
        for (name, reservation) in &self.named_ports {
            keep.insert(name.clone());
            registry.insert(
                name.clone(),
                ServiceEndpoint {
                    host: Some("127.0.0.1".to_string()),
                    port: Some(reservation.port),
                    url: None,
                },
            );
        }
        registry.retain(|name, _| keep.contains(name));
    }

    /// Topologically order auto-init resources by `resource_deps`.
    fn initial_build_order(&self) -> Vec<String> {
        let mut ordered = vec![];
        let mut visited = std::collections::HashSet::new();
        // Simple DFS; cycles are broken by the visited set.
        fn visit(
            name: &str,
            engine: &Engine,
            ordered: &mut Vec<String>,
            visited: &mut std::collections::HashSet<String>,
        ) {
            if !visited.insert(name.to_string()) {
                return;
            }
            if let Some(&i) = engine.by_name.get(name) {
                for dep in engine.manifests[i].resource_deps.clone() {
                    visit(&dep, engine, ordered, visited);
                }
            }
            ordered.push(name.to_string());
        }
        for m in &self.manifests {
            if m.auto_init {
                visit(&m.name, self, &mut ordered, &mut visited);
            }
        }
        // Keep only auto-init resources that actually exist with an update cmd
        // or notes worth showing; everything materialized is fine to "build".
        ordered
            .into_iter()
            .filter(|n| self.by_name.contains_key(n))
            .collect()
    }

    /// Spawn the **FileWatch controller**: a background task that owns the file
    /// watchers, driven by the `FileWatch` API objects rather than wired directly
    /// to the manifests. It starts a watcher per existing `FileWatch` object and
    /// then reacts to the object-store event stream — (re)starting a watcher when
    /// a `FileWatch` is added/modified (e.g. on config reload, when the
    /// materialized object's `watchedPaths`/`ignores` change) and tearing one
    /// down when its object is deleted. The object's spec is the source of truth:
    /// watched paths, ignore rules, the `manual` trigger flag, and the
    /// `fallbackPaths` that force a full rebuild all come from it.
    fn spawn_file_watch_controller(&self) {
        let api = self.api_objects.clone();
        let store = self.store.clone();
        let build_tx = self.build_tx.clone();
        let mut watch = api.watch();
        tokio::spawn(async move {
            use crate::api::store::ObjectEvent;
            // object name -> stop flag for the watcher thread we own for it.
            let mut handles: HashMap<String, Arc<AtomicBool>> = HashMap::new();
            // Initial sync: start a watcher for every materialized FileWatch.
            for obj in api.list("FileWatch") {
                start_file_watch(&obj.name, &obj.object, &build_tx, &store, &mut handles);
            }
            while let Ok(event) = watch.recv().await {
                match event {
                    ObjectEvent::Added(o) | ObjectEvent::Modified(o) if o.kind == "FileWatch" => {
                        start_file_watch(&o.name, &o.object, &build_tx, &store, &mut handles);
                    }
                    ObjectEvent::Deleted(o) if o.kind == "FileWatch" => {
                        if let Some(stop) = handles.remove(&o.name) {
                            stop.store(true, Ordering::Relaxed);
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    /// Spawn a long-running serve command and reflect its runtime status.
    /// When the proxy is enabled, assigns a port via `PORT` and registers a
    /// named route (`<name>.<tld>`) so the resource gets a stable URL.
    fn spawn_serve(&mut self, mut m: Manifest) {
        let store = self.store.clone();
        let name = m.name.clone();
        let proxy = self.proxy.clone();
        let service_registry = self.service_registry.clone();
        let start_count = self.serve_start_counts.entry(name.clone()).or_insert(0);
        *start_count += 1;
        let restart_count = start_count.saturating_sub(1);
        let last_start_time = Utc::now().to_rfc3339();
        let pid_slot = Arc::new(Mutex::new(None));
        let task_pid = pid_slot.clone();
        let task = tokio::spawn(async move {
            // Decide the port: explicit serve_port, else ask the proxy handle to
            // allocate one. In daemon mode this leases centrally so multiple
            // instances never collide. Docker Compose manages its own ports, so
            // it doesn't get a proxy-allocated port/URL.
            let proxy = if m.kind == TargetKind::Local {
                proxy
            } else {
                None
            };
            let reservation = match (m.serve_port, &proxy) {
                (Some(p), Some(handle)) => handle.reserve_port(Some(p)).await,
                (Some(p), None) => Some(crate::proxy::PortReservation {
                    port: p,
                    preferred: Some(p),
                    conflict: false,
                }),
                (None, Some(handle)) => handle.reserve_port(None).await,
                (None, None) => None,
            };
            let port = reservation.as_ref().map(|r| r.port);
            if let Some(p) = port {
                m.serve_cmd.env.push(("PORT".to_string(), p.to_string()));
                m.serve_cmd
                    .env
                    .push(("HOST".to_string(), "127.0.0.1".to_string()));
                if rewrite_serve_cmd_port_args(&mut m.serve_cmd, p) {
                    store.append_log(
                        Some(&name),
                        "INFO",
                        &format!("Rewrote serve_cmd port flag to use allocated PORT={p}\n"),
                    );
                }
            }
            if let Some(reservation) = &reservation {
                if reservation.conflict {
                    if let Some(preferred) = reservation.preferred {
                        store.append_log(
                            Some(&name),
                            "WARN",
                            &format!(
                                "Configured serve_port={preferred} is unavailable or already claimed by another route; using fallback PORT={}. Make serve_cmd bind to $PORT for fallback to work.\n",
                                reservation.port
                            ),
                        );
                    }
                }
            }
            if let (Some(handle), Some(p)) = (&proxy, port) {
                let host = handle.hostname(&name);
                handle.register(&name, p).await;
                let url = handle.url_for(&name);
                service_registry.lock().unwrap().insert(
                    name.clone(),
                    ServiceEndpoint {
                        host: Some(host.clone()),
                        port: Some(p),
                        url: Some(url.clone()),
                    },
                );
                m.serve_cmd
                    .env
                    .push(("PORTLESS_URL".to_string(), url.clone()));
                store.update_status(&name, |st| {
                    st.endpoint_links
                        .retain(|l| l.name.as_deref() != Some(&host));
                    st.endpoint_links.insert(
                        0,
                        UIResourceLink {
                            url: Some(url.clone()),
                            name: Some(host.clone()),
                        },
                    );
                });
                store.append_log(
                    Some(&name),
                    "INFO",
                    &format!("Serving on {url} (PORT={p})\n"),
                );
            }
            if proxy.is_none() {
                if let Some(p) = port {
                    service_registry.lock().unwrap().insert(
                        name.clone(),
                        ServiceEndpoint {
                            host: Some("127.0.0.1".to_string()),
                            port: Some(p),
                            url: None,
                        },
                    );
                }
            }

            m.serve_cmd = with_service_env(m.serve_cmd, &service_registry);

            store.append_log(
                Some(&name),
                "INFO",
                &format!("Running serve_cmd: {}\n", m.serve_cmd.display()),
            );
            store.update_status(&name, |st| {
                st.runtime_status = Some("pending".to_string());
                let local = st.local_resource_info.get_or_insert_with(Default::default);
                local.restart_count = Some(restart_count as i32);
                local.last_start_time = Some(last_start_time.clone());
            });
            let readiness = m.readiness_probe.clone();
            let route_monitor = match (&proxy, port) {
                (Some(handle), Some(p)) => Some(ServeRouteMonitor::new(
                    handle.clone(),
                    store.clone(),
                    name.clone(),
                    handle.hostname(&name),
                    handle.proxy_port(),
                    p,
                )),
                _ => None,
            };
            match spawn_streaming_observed(&m.serve_cmd, &store, &name, route_monitor.clone()).await
            {
                Ok(mut child) => {
                    let pid = child.id();
                    *task_pid.lock().unwrap() = pid;
                    let has_probe = readiness.is_some();
                    store.update_status(&name, |st| {
                        // With a readiness probe the resource isn't "ok" until the
                        // probe first passes; hold it "pending" until then.
                        st.runtime_status =
                            Some(if has_probe { "pending" } else { "ok" }.to_string());
                        let local = st.local_resource_info.get_or_insert_with(Default::default);
                        local.pid = pid.map(|pid| pid as i64);
                        local.is_test = Some(m.is_test);
                        local.restart_count = Some(restart_count as i32);
                        local.last_start_time = Some(last_start_time.clone());
                    });
                    let health_task = route_monitor
                        .as_ref()
                        .map(ServeRouteMonitor::start_health_checks);
                    let probe_task = readiness.map(|p| {
                        spawn_readiness_probe(
                            p,
                            store.clone(),
                            name.clone(),
                            service_registry.clone(),
                        )
                    });
                    let status = child.wait().await;
                    if let Some(task) = health_task {
                        task.abort();
                    }
                    if let Some(task) = probe_task {
                        task.abort();
                    }
                    let ok = status.map(|s| s.success()).unwrap_or(false);
                    store.update_status(&name, |st| {
                        st.runtime_status = Some(if ok { "none" } else { "error" }.to_string());
                    });
                    store.append_log(
                        Some(&name),
                        if ok { "INFO" } else { "ERROR" },
                        &format!("serve_cmd exited (ok={ok})\n"),
                    );
                    // Crash rebuild (Tilt's RestartOn-crash): a serve that exits
                    // non-zero while still enabled is restarted, with a backoff
                    // that grows with the crash count and a cap so a serve that
                    // crashes immediately and repeatedly doesn't loop forever.
                    if !ok && !store.is_resource_disabled(&name) {
                        const MAX_CRASH_RESTARTS: u32 = 10;
                        if restart_count < MAX_CRASH_RESTARTS {
                            let backoff =
                                Duration::from_secs(2 * (restart_count as u64 + 1).min(8));
                            tokio::time::sleep(backoff).await;
                            if !store.is_resource_disabled(&name) {
                                store.append_log(
                                    Some(&name),
                                    "INFO",
                                    "serve_cmd crashed; restarting (crash rebuild)\n",
                                );
                                let _ = store.restart(&name);
                            }
                        } else {
                            store.append_log(
                                Some(&name),
                                "ERROR",
                                &format!(
                                    "serve_cmd crashed {MAX_CRASH_RESTARTS} times; giving up on crash rebuild\n"
                                ),
                            );
                        }
                    }
                }
                Err(e) => {
                    store.update_status(&name, |st| {
                        st.runtime_status = Some("error".to_string());
                    });
                    store.append_log(Some(&name), "ERROR", &format!("serve_cmd failed: {e}\n"));
                }
            }
        });
        self.serve_tasks.insert(
            m.name.clone(),
            ServeTask {
                abort: task.abort_handle(),
                pid: pid_slot,
            },
        );
    }

    /// Restart a resource's serve_cmd: kill the running process and start it
    /// again (gets a fresh port + route).
    async fn restart(&mut self, name: &str) {
        // Reflect the restart immediately. stop_serve aborts the running task
        // without touching runtime_status, and spawn_serve only sets "pending"
        // after async port/route setup — so without this the dashboard keeps
        // showing a stale "ok" through the whole kill-and-respawn window.
        self.store.update_status(name, |st| {
            st.runtime_status = Some("in_progress".to_string());
        });
        self.stop_serve(name, "Restarting serve_cmd...\n").await;
        // Respawn just this resource's serve.
        if let Some(&i) = self.by_name.get(name) {
            let m = self.manifests[i].clone();
            if !m.serve_cmd.is_empty() {
                self.started_serves.insert(name.to_string());
                self.spawn_serve(m);
            }
        }
    }

    async fn change_port(&mut self, name: &str, port: u16) {
        let Some(&i) = self.by_name.get(name) else {
            self.store.append_log(
                Some(name),
                "ERROR",
                "Cannot change port: resource not found\n",
            );
            return;
        };
        if self.manifests[i].serve_cmd.is_empty() {
            self.store.append_log(
                Some(name),
                "ERROR",
                "Cannot change port: resource has no serve_cmd\n",
            );
            return;
        }
        self.manifests[i].serve_port = Some(port);
        self.port_overrides.insert(name.to_string(), port);
        self.store.append_log(
            Some(name),
            "INFO",
            &format!("Changing preferred serve_port to {port}; restarting serve_cmd...\n"),
        );
        self.restart(name).await;
    }

    async fn stop_serve(&mut self, name: &str, message: &str) {
        let task = self.serve_tasks.remove(name);
        self.started_serves.remove(name);
        if task.is_some() {
            self.store.append_log(Some(name), "INFO", message);
        }
        if let Some(task) = task {
            terminate_process_group(task.pid).await;
            task.abort.abort();
        }
        if let Some(proxy) = &self.proxy {
            proxy.remove(name).await;
        }
        self.service_registry.lock().unwrap().remove(name);
        self.refresh_declared_services();
    }

    async fn replace_serve_after_update(&mut self, name: &str, m: Manifest) {
        if m.serve_cmd.is_empty() {
            return;
        }
        let restarting = self.started_serves.contains(name);
        let message = if restarting {
            "Restarting serve_cmd after successful update...\n"
        } else {
            "Starting serve_cmd after successful update...\n"
        };
        // An already-running serve is being killed and respawned: show the
        // restart right away instead of leaving a stale "ok" (see restart()).
        if restarting {
            self.store.update_status(name, |st| {
                st.runtime_status = Some("in_progress".to_string());
            });
        }
        self.stop_serve(name, message).await;
        self.started_serves.insert(name.to_string());
        self.spawn_serve(m);
    }

    fn can_run_local_update_in_parallel(&self, name: &str) -> bool {
        let Some(&i) = self.by_name.get(name) else {
            return false;
        };
        let m = &self.manifests[i];
        m.kind == TargetKind::Local
            && m.allow_parallel
            && !m.update_cmd.is_empty()
            && m.serve_cmd.is_empty()
    }

    /// Run a resource's update command as a one-shot build.
    async fn run_build(
        &mut self,
        name: &str,
        force_full: bool,
        changed_paths: Option<Vec<PathBuf>>,
    ) {
        let Some(&i) = self.by_name.get(name) else {
            return;
        };
        if self.store.is_resource_disabled(name) {
            let now = Utc::now().to_rfc3339();
            self.store.update_status(name, |st| {
                st.queued = Some(false);
                st.has_pending_changes = Some(true);
                st.pending_build_since = Some(now);
                st.update_status = Some("none".to_string());
            });
            self.store
                .append_log(Some(name), "INFO", "Resource is paused; build skipped\n");
            return;
        }
        let m = self.manifests[i].clone();

        if m.kind == TargetKind::Kubernetes {
            self.run_k8s_build(name, &m, force_full, changed_paths.as_deref())
                .await;
            return;
        }

        let now = Utc::now().to_rfc3339();
        // Per-attempt build span so each build's logs are individually
        // addressable (rolled up to the resource for the dashboard/webview).
        let span = format!("{name}:build:{}", self.store.build_count(name) + 1);

        // Resources without an update command (e.g. k8s placeholders) are
        // marked up-to-date immediately.
        if m.update_cmd.is_empty() {
            // Compose resources with an attached image build (dc_resource(image=))
            // build the image before `docker compose up` can use it.
            if m.kind == TargetKind::DockerCompose && !m.docker_builds.is_empty() && !self.dry_run {
                for db in &m.docker_builds {
                    self.store.append_log(
                        Some(&span),
                        "INFO",
                        &format!("Building image: {}\n", db.image_ref),
                    );
                    if let Err(e) = build_image(db, &self.store, &span).await {
                        self.store.append_log(
                            Some(&span),
                            "ERROR",
                            &format!("Image build failed: {e}\n"),
                        );
                        self.store.update_status(name, |st| {
                            st.queued = Some(false);
                            st.pending_build_since = None;
                            st.update_status = Some("error".to_string());
                        });
                        return;
                    }
                }
            }
            self.store.update_status(name, |st| {
                st.queued = Some(false);
                st.pending_build_since = None;
                st.update_status = Some("ok".to_string());
                if m.serve_cmd.is_empty() && st.runtime_status.is_none() {
                    st.runtime_status = Some(match m.kind {
                        TargetKind::Local => "not_applicable".to_string(),
                        _ => "pending".to_string(),
                    });
                }
            });
            if !m.serve_cmd.is_empty() {
                self.replace_serve_after_update(name, m).await;
            }
            return;
        }

        self.store.update_status(name, |st| {
            st.queued = Some(false);
            st.pending_build_since = None;
            st.update_status = Some("in_progress".to_string());
            st.current_build = Some(UIBuildRunning {
                start_time: Some(now.clone()),
                span_id: Some(span.clone()),
            });
        });
        let update_cmd = with_service_env(m.update_cmd.clone(), &self.service_registry);

        self.store.append_log(
            Some(&span),
            "INFO",
            &format!("Building: {}\n", update_cmd.display()),
        );

        let result = run_to_completion(&update_cmd, &self.store, &span).await;
        let finish = Utc::now().to_rfc3339();
        let error = match result {
            Ok(true) => None,
            Ok(false) => Some("command exited non-zero".to_string()),
            Err(e) => Some(e),
        };
        let ok = error.is_none();
        if let Some(err) = &error {
            self.store
                .append_log(Some(&span), "ERROR", &format!("Build failed: {err}\n"));
        } else {
            self.store
                .append_log(Some(&span), "INFO", "Build succeeded\n");
        }
        self.store.update_status(name, |st| {
            st.current_build = None;
            st.last_deploy_time = Some(finish.clone());
            st.update_status = Some(if ok { "ok" } else { "error" }.to_string());
            if m.serve_cmd.is_empty() && st.runtime_status.is_none() {
                st.runtime_status = Some("not_applicable".to_string());
            }
            st.build_history.insert(
                0,
                UIBuildTerminated {
                    start_time: Some(now.clone()),
                    finish_time: Some(finish.clone()),
                    span_id: Some(span.clone()),
                    error: error.clone(),
                    ..Default::default()
                },
            );
            st.build_history.truncate(10);
        });
        if ok && !m.serve_cmd.is_empty() {
            self.replace_serve_after_update(name, m).await;
        }
    }

    /// Build a Kubernetes resource: build referenced images with `docker build`,
    /// then `kubectl apply` the manifest's documents, then watch its pods.
    async fn run_k8s_build(
        &self,
        name: &str,
        m: &Manifest,
        force_full: bool,
        changed_paths: Option<&[PathBuf]>,
    ) {
        // Live-update fast path: if the resource is already deployed with a live
        // pod and has live_update steps, sync into the container instead of a
        // full rebuild + redeploy.
        if !force_full
            && !m.live_update.is_empty()
            && self.store.build_count(name) > 0
            && !self.dry_run
        {
            if let Some(pod) = self.store.current_pod(name) {
                self.live_update(name, m, &pod, changed_paths).await;
                return;
            }
        } else if force_full && !m.live_update.is_empty() && self.store.build_count(name) > 0 {
            self.store
                .append_log(Some(name), "INFO", "Full rebuild selected\n");
        }

        let now = Utc::now().to_rfc3339();
        let span = format!("{name}:build:{}", self.store.build_count(name) + 1);
        self.store.update_status(name, |st| {
            st.queued = Some(false);
            st.pending_build_since = None;
            st.update_status = Some("in_progress".to_string());
            st.current_build = Some(UIBuildRunning {
                start_time: Some(now.clone()),
                span_id: Some(span.clone()),
            });
        });

        let mut error: Option<String> = None;
        let apply_docs = m.k8s_apply_docs.clone();

        // 1. Build images via the native Docker API (bollard), then load them
        //    into a kind cluster if that's where we're deploying.
        for db in &m.docker_builds {
            self.store.append_log(
                Some(&span),
                "INFO",
                &format!("Building image: {}\n", db.image_ref),
            );
            match build_image(db, &self.store, &span).await {
                Ok(result) => {
                    // Resolve the immutable ref to deploy: a custom_build's reported
                    // output ref, else a content-addressed tag derived from the
                    // build digest, else the declared ref (no digest available).
                    let deploy_ref = if let Some(output_ref) = &result.output_ref {
                        output_ref.clone()
                    } else if let Some(digest) = &result.digest {
                        content_addressed_ref(&db.image_ref, digest)
                    } else {
                        db.image_ref.clone()
                    };
                    // Tag the freshly built image with the content ref so the
                    // cluster pulls exactly this build (skip for custom_build,
                    // which controls its own tags, and when the ref is unchanged).
                    if result.output_ref.is_none() && deploy_ref != db.image_ref && !self.dry_run {
                        if let Err(e) = tag_image(&db.image_ref, &deploy_ref).await {
                            self.store
                                .append_log(Some(&span), "WARN", &format!("{e}\n"));
                        }
                    }
                    // Record the resolved ref + digest on the image objects; the
                    // KubernetesApply reconciler injects ImageMap.status.image into
                    // the workload at apply time (object-driven image injection).
                    self.write_image_status(&db.image_ref, result.digest.as_deref(), &deploy_ref);
                    if !self.dry_run && !db.skips_local_docker {
                        kind_load(&deploy_ref, &self.store, &span).await;
                    }
                }
                Err(e) => error = Some(e),
            }
            if error.is_some() {
                break;
            }
        }

        // 2. Apply deploy state (unless an image build already failed).
        if error.is_none() {
            if let Some(cmd) = &m.k8s_custom_apply_cmd {
                self.store
                    .append_log(Some(&span), "INFO", "Running k8s_custom_deploy apply_cmd\n");
                match run_to_completion(cmd, &self.store, &span).await {
                    Ok(true) => {}
                    Ok(false) => error = Some("k8s_custom_deploy apply_cmd failed".to_string()),
                    Err(e) => error = Some(e),
                }
            }
        }
        // True once the apply was performed by the KubernetesApply reconciler
        // (the authoritative, object-driven path), so we don't re-stamp status.
        let mut applied_via_reconciler = false;
        if error.is_none() && m.k8s_custom_apply_cmd.is_none() && !apply_docs.is_empty() {
            let docs = apply_docs.join("\n---\n");
            if self.dry_run {
                // Client-side only: no API calls, nothing mutated. `--validate=false`
                // avoids the openapi fetch so it works fully offline. Inject the
                // resolved ImageMap refs inline so the dry-run reflects the same
                // image injection the reconciler does for real deploys.
                let maps = resolve_image_maps_for(&self.api_objects, name);
                let docs = inject_image_maps(&docs, &maps);
                let argv = vec![
                    "kubectl".to_string(),
                    "apply".to_string(),
                    "-f".to_string(),
                    "-".to_string(),
                    "--dry-run=client".to_string(),
                    "--validate=false".to_string(),
                ];
                self.store
                    .append_log(Some(&span), "INFO", "kubectl apply (dry-run=client)\n");
                match run_with_stdin(&argv, &docs, &self.store, &span).await {
                    Ok(true) => {}
                    Ok(false) => error = Some("kubectl apply failed".to_string()),
                    Err(e) => error = Some(e),
                }
            } else {
                // Authoritative path: publish the final YAML to the KubernetesApply
                // object (a merge patch, so the materialized `spec.imageMaps` list
                // survives), then let the reconciler resolve + inject the ImageMap
                // refs and apply it.
                let _ = self.api_objects.patch(
                    "KubernetesApply",
                    "default",
                    name,
                    serde_json::json!({ "spec": { "yaml": docs } }),
                );
                self.store
                    .append_log(Some(&span), "INFO", "kubectl apply (via reconciler)\n");
                if let Err(e) = reconcile_kubernetes_apply(&self.api_objects, name).await {
                    error = Some(e);
                }
                applied_via_reconciler = true;
            }
        }

        let finish = Utc::now().to_rfc3339();
        let ok = error.is_none();
        if let Some(err) = &error {
            self.store
                .append_log(Some(&span), "ERROR", &format!("Deploy failed: {err}\n"));
        } else {
            self.store
                .append_log(Some(&span), "INFO", "Deploy succeeded\n");
        }
        self.store.update_status(name, |st| {
            st.current_build = None;
            st.last_deploy_time = Some(finish.clone());
            st.update_status = Some(if ok { "ok" } else { "error" }.to_string());
            if !ok {
                st.runtime_status = Some("error".to_string());
            } else if self.dry_run {
                st.runtime_status = Some("not_applicable".to_string());
            }
            st.build_history.insert(
                0,
                UIBuildTerminated {
                    start_time: Some(now.clone()),
                    finish_time: Some(finish.clone()),
                    span_id: Some(span.clone()),
                    error: error.clone(),
                    ..Default::default()
                },
            );
            st.build_history.truncate(10);
        });

        // Reflect the apply result onto the KubernetesApply object's status, so
        // the API object carries live state (not just the declared spec). When
        // the reconciler applied it, it already wrote the object's yaml+status.
        if !applied_via_reconciler {
            let obj = kubernetes_apply_object_with_status(&m, &finish, error.as_deref());
            self.api_objects
                .apply("KubernetesApply", "default", name, obj);
        }

        // 3. Watch pods (only for a real deploy with a selector).
        if ok && !self.dry_run && !m.pod_selector.is_empty() {
            self.spawn_pod_watch(
                name.to_string(),
                m.pod_selector.clone(),
                m.pod_readiness_ignore,
                m.live_update.clone(),
                m.k8s_port_forwards.clone(),
            );
        }
    }

    /// Perform a live update: `kubectl cp` each sync source into the pod and
    /// `kubectl exec` each run command, instead of a full rebuild + redeploy.
    async fn live_update(
        &self,
        name: &str,
        m: &Manifest,
        pod: &str,
        changed_paths: Option<&[PathBuf]>,
    ) {
        use crate::starlingfile::LiveUpdateStep;
        let now = Utc::now().to_rfc3339();
        let span = format!("{name}:build:{}", self.store.build_count(name) + 1);
        self.store.update_status(name, |st| {
            st.update_status = Some("in_progress".to_string());
            st.current_build = Some(UIBuildRunning {
                start_time: Some(now.clone()),
                span_id: Some(span.clone()),
            });
        });
        self.store
            .append_log(Some(&span), "INFO", "Live update (no rebuild)\n");

        let mut error: Option<String> = None;
        for step in &m.live_update {
            let argv = match step {
                LiveUpdateStep::Sync { local, remote } => {
                    self.store.append_log(
                        Some(&span),
                        "INFO",
                        &format!("  sync {local} -> {remote}\n"),
                    );
                    vec![
                        "kubectl".to_string(),
                        "cp".to_string(),
                        local.clone(),
                        format!("{pod}:{remote}"),
                    ]
                }
                LiveUpdateStep::Run {
                    cmd,
                    echo_off,
                    triggers,
                } => {
                    if !live_update_run_matches_triggers(triggers, changed_paths) {
                        self.store.append_log(
                            Some(&span),
                            "INFO",
                            &format!("  run skipped (trigger did not match): {cmd}\n"),
                        );
                        continue;
                    }
                    self.store.append_log(
                        Some(&span),
                        "INFO",
                        &format!(
                            "  run {}\n",
                            if *echo_off {
                                "<redacted>"
                            } else {
                                cmd.as_str()
                            }
                        ),
                    );
                    vec![
                        "kubectl".to_string(),
                        "exec".to_string(),
                        pod.to_string(),
                        "--".to_string(),
                        "sh".to_string(),
                        "-c".to_string(),
                        cmd.clone(),
                    ]
                }
                LiveUpdateStep::RestartContainer => {
                    // Delete the pod so the Deployment recreates it (the closest
                    // k8s analog to restarting the container).
                    self.store
                        .append_log(Some(&span), "INFO", "  restart_container\n");
                    vec![
                        "kubectl".to_string(),
                        "delete".to_string(),
                        "pod".to_string(),
                        pod.to_string(),
                        "--wait=false".to_string(),
                    ]
                }
                // fall_back_on / initial_sync don't run a command during a live
                // update; they're handled by the watch/deploy logic.
                LiveUpdateStep::FallBackOn(_) | LiveUpdateStep::InitialSync => continue,
            };
            let cmd = Cmd {
                argv,
                workdir: None,
                env: vec![],
            };
            match run_to_completion(&cmd, &self.store, &span).await {
                Ok(true) => {}
                Ok(false) => error = Some("live_update step failed".to_string()),
                Err(e) => error = Some(e),
            }
            if error.is_some() {
                break;
            }
        }

        let finish = Utc::now().to_rfc3339();
        let ok = error.is_none();
        self.store.append_log(
            Some(&span),
            if ok { "INFO" } else { "ERROR" },
            &format!("Live update {}\n", if ok { "complete" } else { "failed" }),
        );
        self.store.update_status(name, |st| {
            st.current_build = None;
            st.last_deploy_time = Some(finish.clone());
            st.update_status = Some(if ok { "ok" } else { "error" }.to_string());
            st.build_history.insert(
                0,
                UIBuildTerminated {
                    start_time: Some(now.clone()),
                    finish_time: Some(finish.clone()),
                    span_id: Some(span.clone()),
                    error: error.clone(),
                    ..Default::default()
                },
            );
            st.build_history.truncate(10);
        });
    }

    /// Poll pod status for a workload selector and reflect it in the UI.
    fn spawn_pod_watch(
        &self,
        name: String,
        selector: std::collections::BTreeMap<String, String>,
        readiness_ignored: bool,
        live_update: Vec<crate::starlingfile::LiveUpdateStep>,
        port_forwards: Vec<PortForwardSpec>,
    ) {
        let store = self.store.clone();
        let sel = selector
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(async move {
            // Initial-sync is fanned out across every observed pod (each replica
            // gets the startup sync once). Log streaming is owned by the
            // controller manager (per-pod follow). Port-forward targets the first
            // pod, since a local port can bind only one pod.
            let mut initial_synced_pods: HashSet<String> = HashSet::new();
            let mut port_forward_pod: Option<String> = None;
            let mut port_forward_tasks: Vec<tokio::task::JoinHandle<()>> = vec![];
            loop {
                let out = Command::new("kubectl")
                    .args(["get", "pods", "-l", &sel, "-o", "json"])
                    .output()
                    .await;
                let Ok(out) = out else {
                    break;
                };
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                    let empty = Vec::new();
                    let items = json["items"].as_array().unwrap_or(&empty);
                    let summary = aggregate_pod_status(items, readiness_ignored);
                    if let Some(pod) = items.first() {
                        // Status reflects the whole pod set; the first pod's name
                        // is shown as the representative pod.
                        let pod_name = pod["metadata"]["name"].as_str().unwrap_or("").to_string();
                        store.update_status(&name, |st| {
                            st.runtime_status = Some(summary.runtime.to_string());
                            st.k8s_resource_info = Some(UIResourceKubernetes {
                                pod_name: Some(pod_name.clone()),
                                pod_status: Some(summary.status_label()),
                                all_containers_ready: Some(summary.all_ready()),
                                pod_restarts: Some(summary.restarts as i32),
                                span_id: Some(format!("{name}:pod")),
                                ..Default::default()
                            });
                        });
                    }
                    // Initial sync: once per observed pod (per-pod fan-out).
                    if live_update_has_initial_sync(&live_update) {
                        for pod in items {
                            let pn = pod["metadata"]["name"].as_str().unwrap_or("").to_string();
                            if pn.is_empty() || initial_synced_pods.contains(&pn) {
                                continue;
                            }
                            initial_synced_pods.insert(pn.clone());
                            tokio::spawn(run_initial_sync(
                                store.clone(),
                                name.clone(),
                                pn,
                                live_update.clone(),
                            ));
                        }
                    }
                    // Port-forward: (re)bind to the first pod when it changes.
                    if !port_forwards.is_empty() {
                        let pod_name = items
                            .first()
                            .and_then(|p| p["metadata"]["name"].as_str())
                            .unwrap_or("")
                            .to_string();
                        if !pod_name.is_empty()
                            && port_forward_pod.as_deref() != Some(pod_name.as_str())
                        {
                            for task in port_forward_tasks.drain(..) {
                                task.abort();
                            }
                            port_forward_pod = Some(pod_name.clone());
                            for spec in port_forwards.clone() {
                                port_forward_tasks.push(stream_port_forward(
                                    pod_name.clone(),
                                    name.clone(),
                                    spec,
                                    store.clone(),
                                ));
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
    }
}

/// If the current kube-context is a kind cluster, load a locally-built image
/// into it (kind nodes can't pull from the local Docker daemon). Matches Tilt's
/// automatic image loading for kind. No-op for other clusters.
async fn kind_load(image_ref: &str, store: &Arc<Store>, span: &str) {
    let ctx = match Command::new("kubectl")
        .args(["config", "current-context"])
        .output()
        .await
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        _ => return,
    };
    let Some(cluster) = ctx.strip_prefix("kind-") else {
        return;
    };
    store.append_log(
        Some(span),
        "INFO",
        &format!("Loading {image_ref} into kind cluster '{cluster}'\n"),
    );
    let cmd = Cmd {
        argv: vec![
            "kind".to_string(),
            "load".to_string(),
            "docker-image".to_string(),
            image_ref.to_string(),
            "--name".to_string(),
            cluster.to_string(),
        ],
        workdir: None,
        env: vec![],
    };
    let _ = run_to_completion(&cmd, store, span).await;
}

/// Build a docker image via the native Docker API (bollard), streaming build
/// output to the resource log. Replaces shelling out to `docker build`.
/// Build the `docker build` argv for a `DockerBuild`, passing through every
/// option Docker's BuildKit supports (secrets, SSH, cache, platform, network,
/// build args, extra tags). `dockerfile` overrides `db.dockerfile` (used for
/// inline `dockerfile_contents` written to a temp file).
fn docker_build_args(
    db: &crate::starlingfile::DockerBuild,
    dockerfile: Option<&std::path::Path>,
) -> Vec<String> {
    let mut a = vec!["build".to_string(), "-t".to_string(), db.image_ref.clone()];
    for t in &db.extra_tags {
        a.push("-t".to_string());
        a.push(t.clone());
    }
    if let Some(df) = dockerfile
        .map(|p| p.to_path_buf())
        .or(db.dockerfile.clone())
    {
        a.push("-f".to_string());
        a.push(df.display().to_string());
    }
    if let Some(t) = &db.target {
        a.push("--target".to_string());
        a.push(t.clone());
    }
    if let Some(p) = &db.platform {
        a.push("--platform".to_string());
        a.push(p.clone());
    }
    if let Some(n) = &db.network {
        a.push("--network".to_string());
        a.push(n.clone());
    }
    if db.pull {
        a.push("--pull".to_string());
    }
    for (k, v) in &db.build_args {
        a.push("--build-arg".to_string());
        a.push(format!("{k}={v}"));
    }
    for c in &db.cache_from {
        a.push("--cache-from".to_string());
        a.push(c.clone());
    }
    for s in &db.ssh {
        a.push("--ssh".to_string());
        a.push(s.clone());
    }
    for s in &db.secrets {
        a.push("--secret".to_string());
        a.push(s.clone());
    }
    for h in &db.extra_hosts {
        a.push("--add-host".to_string());
        a.push(h.clone());
    }
    a.push(db.context.display().to_string());
    a
}

/// Build an image via the `docker build` CLI (BuildKit), for builds needing
/// secrets/SSH that bollard can't do. Returns Ok(()) on success.
async fn build_image_buildkit(
    db: &crate::starlingfile::DockerBuild,
    store: &Arc<Store>,
    span: &str,
) -> Result<(), String> {
    // Inline dockerfile_contents -> a temp Dockerfile passed via -f.
    let tmp_df = if let Some(contents) = &db.dockerfile_contents {
        let p = std::env::temp_dir().join(format!("starling-df-{}", uuid::Uuid::new_v4()));
        std::fs::write(&p, contents).map_err(|e| format!("writing dockerfile: {e}"))?;
        Some(p)
    } else {
        None
    };
    let mut argv = vec!["docker".to_string()];
    argv.extend(docker_build_args(db, tmp_df.as_deref()));
    store.append_log(
        Some(span),
        "INFO",
        &format!("docker build (BuildKit): {}\n", db.image_ref),
    );
    let cmd = Cmd {
        argv,
        workdir: None,
        env: vec![("DOCKER_BUILDKIT".to_string(), "1".to_string())],
    };
    let result = run_to_completion(&cmd, store, span).await;
    if let Some(p) = tmp_df {
        let _ = std::fs::remove_file(p);
    }
    match result {
        Ok(true) => Ok(()),
        Ok(false) => Err(format!("docker build {} failed", db.image_ref)),
        Err(e) => Err(e),
    }
}

/// The outcome of building a single image. `output_ref` is the rewritten ref a
/// `custom_build` reported via `outputs_image_ref_to` (else None). `digest` is
/// the locally-resolved immutable image ID (`sha256:...`) when it could be
/// inspected — the content identity used to derive an immutable deploy ref and
/// recorded on the `ImageMap`/`DockerImage` object status.
#[derive(Debug, Default, Clone)]
struct BuildResult {
    output_ref: Option<String>,
    digest: Option<String>,
}

/// Inspect a locally-available image and return its content image ID
/// (`sha256:...`). Returns None if the daemon/image is unavailable — callers
/// degrade to a non-digest deploy ref rather than failing the build.
async fn inspect_image_id(image_ref: &str) -> Option<String> {
    let docker = bollard::Docker::connect_with_local_defaults().ok()?;
    docker.inspect_image(image_ref).await.ok()?.id
}

/// Extract a `@sha256:...` digest already embedded in an image ref, if present
/// (e.g. a `custom_build` script that emits a digest-pinned ref).
fn digest_from_ref(image_ref: &str) -> Option<String> {
    image_ref
        .split_once('@')
        .map(|(_, d)| d.to_string())
        .filter(|d| d.starts_with("sha256:"))
}

/// A short, deterministic tag derived from an image's content digest, so each
/// distinct build produces a distinct immutable tag (and an unchanged build
/// reuses it). Mirrors Tilt's content-addressed `tilt-<hash>` deploy tags.
fn immutable_image_tag(digest: &str) -> String {
    let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
    format!("starling-{}", &hex[..hex.len().min(12)])
}

/// The immutable, content-addressed ref to deploy for a build: the build repo
/// with a digest-derived tag. Reusing the repo keeps it pullable from the same
/// place (kind-loaded or local), while the tag rolls the workload on any change.
fn content_addressed_ref(image_ref: &str, digest: &str) -> String {
    let (repo, _) = image_ref_repo_and_tag(image_ref);
    format!("{repo}:{}", immutable_image_tag(digest))
}

/// Tag an already-built `source` image as `target` via the Docker daemon, so the
/// immutable content ref resolves to the freshly built image.
async fn tag_image(source: &str, target: &str) -> Result<(), String> {
    use bollard::image::TagImageOptions;
    let docker = bollard::Docker::connect_with_local_defaults()
        .map_err(|e| format!("connecting to Docker daemon: {e}"))?;
    let (repo, tag) = image_ref_repo_and_tag(target);
    docker
        .tag_image(source, Some(TagImageOptions { repo, tag }))
        .await
        .map_err(|e| format!("tagging {source} as {target}: {e}"))
}

async fn build_image(
    db: &crate::starlingfile::DockerBuild,
    store: &Arc<Store>,
    span: &str,
) -> Result<BuildResult, String> {
    use bollard::image::TagImageOptions;
    use futures::StreamExt;

    // custom_build: run the user's command (with EXPECTED_REF) instead of bollard.
    if let Some(command) = &db.command {
        let mut cmd = command.clone();
        let expected_ref = custom_build_expected_ref(db);
        let (expected_image, expected_tag) = image_ref_repo_and_tag(&expected_ref);
        cmd.env.push(("EXPECTED_REF".to_string(), expected_ref));
        cmd.env.push(("EXPECTED_IMAGE".to_string(), expected_image));
        cmd.env
            .push(("EXPECTED_TAG".to_string(), expected_tag.clone()));
        cmd.env.push(("TAG".to_string(), expected_tag));
        return match run_to_completion(&cmd, store, span).await {
            Ok(true) => {
                let output_ref = if let Some(path) = &db.outputs_image_ref_to {
                    let text = tokio::fs::read_to_string(path).await.map_err(|e| {
                        format!(
                            "custom_build {} could not read outputs_image_ref_to {}: {e}",
                            db.image_ref,
                            path.display()
                        )
                    })?;
                    let output_ref = text.trim().to_string();
                    if output_ref.is_empty() {
                        return Err(format!(
                            "custom_build {} wrote an empty outputs_image_ref_to {}",
                            db.image_ref,
                            path.display()
                        ));
                    }
                    store.append_log(
                        Some(span),
                        "INFO",
                        &format!("custom_build output image ref: {output_ref}\n"),
                    );
                    Some(output_ref)
                } else {
                    None
                };
                // Prefer a digest already pinned in the output ref; otherwise try
                // to inspect the built image locally (it may not be present when
                // the script pushed directly, in which case digest stays None).
                let inspect_target = output_ref.as_deref().unwrap_or(&db.image_ref);
                let digest = match digest_from_ref(inspect_target) {
                    Some(d) => Some(d),
                    None => inspect_image_id(inspect_target).await,
                };
                Ok(BuildResult { output_ref, digest })
            }
            Ok(false) => Err(format!("custom_build {} command failed", db.image_ref)),
            Err(e) => Err(e),
        };
    }

    // BuildKit-only options (secrets / SSH) can't go through bollard, which has
    // no BuildKit session. Route those builds through the `docker build` CLI,
    // which uses BuildKit and supports --secret/--ssh/--cache-from passthrough.
    if !db.ssh.is_empty() || !db.secrets.is_empty() {
        build_image_buildkit(db, store, span).await?;
        let digest = inspect_image_id(&db.image_ref).await;
        return Ok(BuildResult {
            output_ref: None,
            digest,
        });
    }

    let docker = bollard::Docker::connect_with_local_defaults()
        .map_err(|e| format!("connecting to Docker daemon: {e}"))?;

    // Tar the build context (blocking work on a worker thread).
    let db_for_tar = db.clone();
    let tar = tokio::task::spawn_blocking(move || build_context_tar(&db_for_tar))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("taring build context {}: {e}", db.context.display()))?;

    let options = build_image_options(db);

    let mut stream = docker.build_image(options, None, Some(tar.into()));
    while let Some(item) = stream.next().await {
        match item {
            Ok(info) => {
                if let Some(s) = info.stream {
                    for line in s.lines() {
                        store.append_log(Some(span), "INFO", &format!("{line}\n"));
                    }
                }
                if let Some(err) = info.error {
                    store.append_log(Some(span), "ERROR", &format!("{err}\n"));
                    return Err(format!("docker build {}: {err}", db.image_ref));
                }
            }
            Err(e) => return Err(format!("docker build {}: {e}", db.image_ref)),
        }
    }
    for extra_tag in &db.extra_tags {
        let (repo, tag) = image_ref_repo_and_tag(extra_tag);
        docker
            .tag_image(
                &db.image_ref,
                Some(TagImageOptions {
                    repo: repo.clone(),
                    tag: tag.clone(),
                }),
            )
            .await
            .map_err(|e| format!("tagging image {} as {extra_tag}: {e}", db.image_ref))?;
        store.append_log(
            Some(span),
            "INFO",
            &format!("Tagged {} as {extra_tag}\n", db.image_ref),
        );
    }
    let digest = inspect_image_id(&db.image_ref).await;
    Ok(BuildResult {
        output_ref: None,
        digest,
    })
}

fn custom_build_expected_ref(db: &crate::starlingfile::DockerBuild) -> String {
    let Some(tag) = &db.custom_tag else {
        return db.image_ref.clone();
    };
    if tag.contains('/') {
        tag.clone()
    } else {
        format!("{}:{tag}", image_ref_repo_and_tag(&db.image_ref).0)
    }
}

fn build_image_options(
    db: &crate::starlingfile::DockerBuild,
) -> bollard::image::BuildImageOptions<String> {
    bollard::image::BuildImageOptions {
        dockerfile: dockerfile_name_for_build(db),
        t: db.image_ref.clone(),
        rm: true,
        forcerm: true,
        buildargs: db.build_args.iter().cloned().collect(),
        cachefrom: db.cache_from.clone(),
        pull: db.pull,
        networkmode: db.network.clone().unwrap_or_default(),
        extrahosts: docker_extra_hosts(&db.extra_hosts),
        target: db.target.clone().unwrap_or_default(),
        platform: db.platform.clone().unwrap_or_default(),
        ..Default::default()
    }
}

fn docker_extra_hosts(hosts: &[String]) -> Option<String> {
    match hosts {
        [] => None,
        [one] => Some(one.clone()),
        many => Some(many.join(",")),
    }
}

fn image_ref_repo_and_tag(image_ref: &str) -> (String, String) {
    let last_slash = image_ref.rfind('/');
    let last_colon = image_ref.rfind(':');
    if let Some(colon) = last_colon {
        if last_slash.map(|slash| colon > slash).unwrap_or(true) {
            return (
                image_ref[..colon].to_string(),
                image_ref[colon + 1..].to_string(),
            );
        }
    }
    (image_ref.to_string(), "latest".to_string())
}

fn build_context_tar(db: &crate::starlingfile::DockerBuild) -> std::io::Result<Vec<u8>> {
    let mut rules = db.ignore_rules.clone();
    rules.extend(read_dockerignore_rules(&db.context)?);
    let mut only = db.only.clone();
    if !only.is_empty() {
        let dockerfile = db
            .dockerfile
            .clone()
            .unwrap_or_else(|| db.context.join("Dockerfile"));
        if let Ok(rel) = dockerfile.strip_prefix(&db.context) {
            only.push(rel.to_path_buf());
        }
    }
    let mut builder = tar::Builder::new(Vec::new());
    append_context_path(&mut builder, &db.context, &db.context, &rules, &only)?;
    if let Some(contents) = &db.dockerfile_contents {
        append_generated_dockerfile(&mut builder, contents)?;
    }
    builder.into_inner()
}

fn dockerfile_name_for_build(db: &crate::starlingfile::DockerBuild) -> String {
    if db.dockerfile_contents.is_some() {
        return GENERATED_DOCKERFILE.to_string();
    }
    db.dockerfile
        .as_ref()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("Dockerfile")
        .to_string()
}

fn append_generated_dockerfile(
    builder: &mut tar::Builder<Vec<u8>>,
    contents: &str,
) -> std::io::Result<()> {
    let bytes = contents.as_bytes();
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    builder.append_data(&mut header, GENERATED_DOCKERFILE, bytes)
}

fn read_dockerignore_rules(context: &Path) -> std::io::Result<Vec<IgnoreRule>> {
    let path = context.join(".dockerignore");
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(e) => return Err(e),
    };
    Ok(text
        .lines()
        .filter_map(|line| {
            let pattern = line.trim();
            if pattern.is_empty() || pattern.starts_with('#') {
                None
            } else {
                Some(IgnoreRule {
                    base: context.to_path_buf(),
                    pattern: pattern.to_string(),
                })
            }
        })
        .collect())
}

fn append_context_path(
    builder: &mut tar::Builder<Vec<u8>>,
    root: &Path,
    path: &Path,
    ignore_rules: &[IgnoreRule],
    only: &[PathBuf],
) -> std::io::Result<()> {
    let rel = path.strip_prefix(root).unwrap_or(path);
    if !rel.as_os_str().is_empty() && !build_context_included(root, rel, ignore_rules, only) {
        return Ok(());
    }
    if path.is_dir() {
        if !rel.as_os_str().is_empty() {
            builder.append_dir(rel, path)?;
        }
        let mut entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            append_context_path(builder, root, &entry.path(), ignore_rules, only)?;
        }
    } else if path.is_file() {
        builder.append_path_with_name(path, rel)?;
    }
    Ok(())
}

fn build_context_included(
    root: &Path,
    rel: &Path,
    ignore_rules: &[IgnoreRule],
    only: &[PathBuf],
) -> bool {
    let rel_text = rel.to_string_lossy().replace('\\', "/");
    if !only.is_empty() && !only.iter().any(|only| path_is_selected_by_only(rel, only)) {
        return false;
    }
    !is_ignored_by_rules(&root.join(&rel_text), ignore_rules)
}

fn path_is_selected_by_only(path: &Path, only: &Path) -> bool {
    path == only || path.starts_with(only) || only.starts_with(path)
}

/// Watch a single file (via its parent directory, so atomic saves are caught)
/// and send a unit signal on each content change.
/// Watch every config file (Starlingfile + includes + load targets + read_file
/// paths). Fires a reload signal when any of them changes. Watches each file's
/// parent directory (so atomic saves are caught) and matches events by
/// canonical path.
fn spawn_config_watcher(
    files: Vec<std::path::PathBuf>,
    tx: mpsc::UnboundedSender<()>,
    generation: Arc<AtomicU64>,
) {
    let current_generation = generation.fetch_add(1, Ordering::Relaxed) + 1;
    std::thread::spawn(move || {
        let (raw_tx, raw_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = raw_tx.send(res);
        }) {
            Ok(w) => w,
            Err(_) => return,
        };
        // Canonical set of files we care about, plus the unique parent dirs.
        let canon: std::collections::HashSet<std::path::PathBuf> = files
            .iter()
            .map(|f| std::fs::canonicalize(f).unwrap_or_else(|_| f.clone()))
            .collect();
        let mut dirs: Vec<std::path::PathBuf> = files
            .iter()
            .map(|f| match f.parent() {
                Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
                _ => std::path::PathBuf::from("."),
            })
            .collect();
        dirs.sort();
        dirs.dedup();
        for dir in &dirs {
            let _ = watcher.watch(dir, RecursiveMode::NonRecursive);
        }
        while let Ok(ev) = raw_rx.recv() {
            if !is_content_event(&ev) {
                continue;
            }
            let touches = match &ev {
                Ok(e) => e.paths.iter().any(|p| {
                    let c = std::fs::canonicalize(p).unwrap_or_else(|_| p.clone());
                    canon.contains(&c) || canon.contains(p)
                }),
                _ => false,
            };
            if !touches {
                continue;
            }
            while raw_rx.recv_timeout(Duration::from_millis(200)).is_ok() {}
            if generation.load(Ordering::Relaxed) != current_generation {
                break;
            }
            if tx.send(()).is_err() {
                break;
            }
        }
    });
}

/// Apply (or delete) a YAML blob via `kubectl` — the cluster transport the
/// object reconcilers use. Returns the trimmed stderr on failure.
async fn kubectl_apply_yaml(yaml: &str, delete: bool) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let verb = if delete { "delete" } else { "apply" };
    let mut child = Command::new("kubectl")
        .args([verb, "-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn kubectl {verb}: {e}"))?;
    if let Some(mut sin) = child.stdin.take() {
        sin.write_all(yaml.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
    }
    let out = child.wait_with_output().await.map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Reconcile a `KubernetesApply` object: apply its `spec.yaml` to the cluster
/// (object-driven — the controller pattern, as opposed to the engine's build
/// loop) and write the apply result onto the object's `status`. This is a real
/// cluster-backed reconciler; it is exercised by the gated k8s integration test
/// and is the basis for making the object store the authoritative apply path.
/// Collect the `(selector, resolved-image)` pairs for the ImageMaps a
/// `KubernetesApply` references. Reads each ImageMap's `spec.selector` (the
/// Tiltfile image the workload uses) and `status.image` (the immutable deploy
/// ref a completed build recorded). ImageMaps with no resolved image yet are
/// skipped, so a not-yet-built image leaves the workload's ref untouched.
fn resolve_image_maps_for(
    api: &Arc<crate::api::store::ApiObjectStore>,
    apply_name: &str,
) -> Vec<(String, String)> {
    let Some(obj) = api.get("KubernetesApply", "default", apply_name) else {
        return Vec::new();
    };
    let names = obj.object["spec"]["imageMaps"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut maps = Vec::new();
    for name in names {
        let Some(name) = name.as_str() else { continue };
        let Some(im) = api.get("ImageMap", "default", name) else {
            continue;
        };
        let selector = im.object["spec"]["selector"]
            .as_str()
            .unwrap_or(name)
            .to_string();
        let image = im.object["status"]["image"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if !image.is_empty() {
            maps.push((selector, image));
        }
    }
    maps
}

/// Rewrite each document's matching container images to the resolved ImageMap
/// refs. Multi-doc YAML is split on the `\n---\n` joiner the engine uses, each
/// document rewritten, then rejoined (matching `rewrite_container_image`'s
/// single-document contract).
fn inject_image_maps(yaml: &str, maps: &[(String, String)]) -> String {
    if maps.is_empty() {
        return yaml.to_string();
    }
    yaml.split("\n---\n")
        .map(|doc| {
            let mut d = doc.to_string();
            for (selector, image) in maps {
                d = crate::starlingfile::rewrite_container_image(&d, selector, image);
            }
            d
        })
        .collect::<Vec<_>>()
        .join("\n---\n")
}

pub async fn reconcile_kubernetes_apply(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("KubernetesApply", "default", name)
        .ok_or_else(|| format!("KubernetesApply {name} not found"))?;
    let yaml = obj.object["spec"]["yaml"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if yaml.trim().is_empty() {
        return Err(format!("KubernetesApply {name} has no spec.yaml to apply"));
    }
    // Resolve + inject the immutable ImageMap refs into the workloads (the
    // object-driven image-injection step Tilt's apply controller performs).
    let maps = resolve_image_maps_for(api, name);
    let yaml = inject_image_maps(&yaml, &maps);
    let result = if crate::kube_client::use_kube_rs() {
        crate::kube_client::apply_yaml(&yaml).await
    } else {
        kubectl_apply_yaml(&yaml, false).await
    };
    let now = Utc::now().to_rfc3339();
    let error = result.as_ref().err().cloned().unwrap_or_default();
    // Record the outcome on the object's status (controller writes status back).
    let _ = api.patch(
        "KubernetesApply",
        "default",
        name,
        serde_json::json!({ "status": { "lastApplyTime": now, "error": error } }),
    );
    result
}

/// List the pods matching a label selector, via whichever transport is active:
/// the in-process kube-rs client when `STARLING_KUBE_RS=1`, else `kubectl get
/// pods -o json`. Both return the items as the same Kubernetes JSON shape, so
/// callers (`aggregate_pod_status`, `pod_record`, target resolution) are
/// transport-agnostic.
async fn list_pods_for_selector(selector: &str) -> Result<Vec<serde_json::Value>, String> {
    if crate::kube_client::use_kube_rs() {
        return crate::kube_client::list_pods(selector).await;
    }
    let out = Command::new("kubectl")
        .args(["get", "pods", "-l", selector, "-o", "json"])
        .output()
        .await
        .map_err(|e| format!("kubectl get pods: {e}"))?;
    let json: serde_json::Value =
        serde_json::from_slice(&out.stdout).map_err(|e| format!("parsing pods: {e}"))?;
    Ok(json["items"].as_array().cloned().unwrap_or_default())
}

/// Reconcile a `KubernetesDiscovery` object: list the pods matching its
/// selector and write aggregated status (ready/total + runtime) back onto the
/// object — the discovery controller, object-driven and cluster-backed.
pub async fn reconcile_kubernetes_discovery(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("KubernetesDiscovery", "default", name)
        .ok_or_else(|| format!("KubernetesDiscovery {name} not found"))?;
    let selector = obj.object["spec"]["selectors"][0]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("KubernetesDiscovery {name} has no selector"));
    }
    let items = list_pods_for_selector(&selector).await?;
    let summary = aggregate_pod_status(&items, false);
    let _ = api.patch(
        "KubernetesDiscovery",
        "default",
        name,
        serde_json::json!({
            "status": {
                "readyPods": summary.ready,
                "totalPods": summary.total,
                "runtime": summary.runtime,
            }
        }),
    );
    Ok(())
}

/// Build a per-pod status record from a `kubectl get pods -o json` item,
/// mirroring the slice of Tilt's `KubernetesDiscovery.status.pods[]` the
/// dashboard and live-update target on: identity, phase, readiness, restarts,
/// and per-container name/ready/image/restartCount.
fn pod_record(pod: &serde_json::Value) -> serde_json::Value {
    let phase = pod["status"]["phase"].as_str().unwrap_or("Unknown");
    let empty = Vec::new();
    let cstatuses = pod["status"]["containerStatuses"]
        .as_array()
        .unwrap_or(&empty);
    let all_ready = !cstatuses.is_empty()
        && cstatuses
            .iter()
            .all(|c| c["ready"].as_bool().unwrap_or(false));
    let restarts: i64 = cstatuses
        .iter()
        .map(|c| c["restartCount"].as_i64().unwrap_or(0))
        .sum();
    let containers: Vec<serde_json::Value> = cstatuses
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c["name"].as_str().unwrap_or(""),
                "ready": c["ready"].as_bool().unwrap_or(false),
                "image": c["image"].as_str().unwrap_or(""),
                "restartCount": c["restartCount"].as_i64().unwrap_or(0),
            })
        })
        .collect();
    serde_json::json!({
        "name": pod["metadata"]["name"].as_str().unwrap_or(""),
        "namespace": pod["metadata"]["namespace"].as_str().unwrap_or("default"),
        "phase": phase,
        "ready": all_ready || phase == "Succeeded",
        "podID": pod["metadata"]["uid"].as_str().unwrap_or(""),
        "restartCount": restarts,
        "containers": containers,
    })
}

/// Reconcile the **pod-watch** view of a `KubernetesDiscovery` object: list the
/// pods matching its selector and write the detailed per-pod records to
/// `status.pods` (the stateful per-pod tracking that the dashboard and
/// live-update build on). Idempotent — replaces the list each run, so it is safe
/// in the maintained controller loop. Complements `reconcile_kubernetes_discovery`,
/// which writes the aggregate ready/total counts onto the same object.
pub async fn reconcile_pod_watch(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("KubernetesDiscovery", "default", name)
        .ok_or_else(|| format!("KubernetesDiscovery {name} not found"))?;
    let selector = obj.object["spec"]["selectors"][0]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("KubernetesDiscovery {name} has no selector"));
    }
    let items = list_pods_for_selector(&selector).await?;
    let pods: Vec<serde_json::Value> = items.iter().map(pod_record).collect();
    let _ = api.patch(
        "KubernetesDiscovery",
        "default",
        name,
        serde_json::json!({ "status": { "pods": pods } }),
    );
    Ok(())
}

/// Parse a `PortForward` object's `spec.forwards` into [`PortForwardSpec`]s for
/// the long-running forward processes the controller manager maintains.
fn port_forward_specs_from_object(obj: &serde_json::Value) -> Vec<PortForwardSpec> {
    obj["spec"]["forwards"]
        .as_array()
        .map(|fs| {
            fs.iter()
                .map(|f| PortForwardSpec {
                    host: f["host"].as_str().unwrap_or("127.0.0.1").to_string(),
                    local_port: f["localPort"].as_u64().unwrap_or(0) as u16,
                    container_port: f["containerPort"].as_u64().unwrap_or(0) as u16,
                    name: f["name"].as_str().unwrap_or("").to_string(),
                    link_path: f["path"].as_str().unwrap_or("").to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Tracks the persistent forward processes the controller owns for one
/// `PortForward` object: the pod they target (to detect pod changes) and the
/// running tasks (to abort on pod change or object deletion).
struct ForwardProcesses {
    pod: String,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for ForwardProcesses {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

/// Spawn the **controller manager**: a background loop that continuously
/// reconciles the API objects on an interval (the maintained-controller model,
/// vs. one-shot reconcile) AND owns the long-running port-forward *processes*.
///
/// Each tick it (1) runs the idempotent status reconcilers — discovery
/// aggregate, pod-watch (per-pod detail), and port-forward target resolution —
/// keeping their objects' status converged, and (2) reconciles a persistent
/// `kubectl port-forward` process per `PortForward` object against its resolved
/// `status.podName`: starts the forwards when a target first appears, restarts
/// them when the target pod changes, and tears them down (via `ForwardProcesses`'
/// `Drop`) when the object is deleted. This is the long-lived process lifecycle
/// moved into the controller.
///
/// It also owns **pod-log following**: for each `PodLogStream` object it follows
/// every matching pod's logs (one follow stream per pod — per-pod fan-out),
/// starting a stream when a pod first appears and dropping it when the pod is
/// gone. Following each pod exactly once is what makes pod-log safe under
/// continuous reconciliation (the reason it was previously excluded from the
/// loop: a tail-and-append reconcile duplicates lines; a per-pod follow does
/// not). The one-shot `reconcile_pod_log_stream` (tail) remains for the
/// `POST …/reconcile` endpoint.
pub fn spawn_controller_manager(
    api: Arc<crate::api::store::ApiObjectStore>,
    store: Arc<Store>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    use std::collections::HashMap;
    tokio::spawn(async move {
        // object name -> the forward processes we own for it.
        let mut forwards: HashMap<String, ForwardProcesses> = HashMap::new();
        // pod name -> the follow-log task we own for it.
        let mut log_follows: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
        loop {
            for obj in api.list("KubernetesDiscovery") {
                let _ = reconcile_kubernetes_discovery(&api, &obj.name).await;
                let _ = reconcile_pod_watch(&api, &obj.name).await;
            }

            // Follow logs for every pod of every PodLogStream (per-pod fan-out).
            let mut live_log_pods: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for obj in api.list("PodLogStream") {
                let selector = pod_log_selector(&obj.object);
                if selector.is_empty() {
                    continue;
                }
                let pods = list_pods_for_selector(&selector).await.unwrap_or_default();
                for p in &pods {
                    let pod = p["metadata"]["name"].as_str().unwrap_or("").to_string();
                    if pod.is_empty() {
                        continue;
                    }
                    live_log_pods.insert(pod.clone());
                    // Start a follow stream the first time we see this pod; logs
                    // go to the resource span (the PodLogStream object's name).
                    log_follows.entry(pod.clone()).or_insert_with(|| {
                        stream_pod_logs(pod.clone(), obj.name.clone(), store.clone())
                    });
                }
            }
            // Stop following pods that no longer exist.
            log_follows.retain(|pod, handle| {
                let live = live_log_pods.contains(pod);
                if !live {
                    handle.abort();
                }
                live
            });

            let pf_objects = api.list("PortForward");
            let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
            for obj in &pf_objects {
                live.insert(obj.name.clone());
                let _ = reconcile_port_forward(&api, &obj.name).await;
                // Re-read to get the freshly-resolved target pod.
                let Some(current) = api.get("PortForward", "default", &obj.name) else {
                    continue;
                };
                let pod = current.object["status"]["podName"]
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                let specs = port_forward_specs_from_object(&current.object);
                if pod.is_empty() || specs.is_empty() {
                    continue;
                }
                // (Re)start the forwards if we have none, or the pod changed.
                let needs_start = forwards
                    .get(&obj.name)
                    .map(|f| f.pod != pod)
                    .unwrap_or(true);
                if needs_start {
                    let tasks = specs
                        .into_iter()
                        .map(|spec| {
                            stream_port_forward(pod.clone(), obj.name.clone(), spec, store.clone())
                        })
                        .collect();
                    // Replacing the entry drops the old one, aborting its tasks.
                    forwards.insert(obj.name.clone(), ForwardProcesses { pod, tasks });
                }
            }
            // Tear down forwards for objects that no longer exist.
            forwards.retain(|name, _| live.contains(name));
            tokio::time::sleep(interval).await;
        }
    })
}

/// Reconcile a `PortForward` object: resolve the target pod for its selector and
/// record it on the object's status (the port-forward target controller). The
/// long-running forward process itself is still managed by the engine's pod
/// watcher; this is the object-driven target-resolution step.
pub async fn reconcile_port_forward(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("PortForward", "default", name)
        .ok_or_else(|| format!("PortForward {name} not found"))?;
    let selector = obj.object["spec"]["selector"]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("PortForward {name} has no selector"));
    }
    let items = list_pods_for_selector(&selector).await?;
    let pod = items
        .first()
        .and_then(|p| p["metadata"]["name"].as_str())
        .unwrap_or("")
        .to_string();
    let _ = api.patch(
        "PortForward",
        "default",
        name,
        serde_json::json!({ "status": { "podName": pod, "ready": !pod.is_empty() } }),
    );
    Ok(())
}

/// Reconcile a `LiveUpdate` object: resolve the target pod for its selector,
/// then apply each sync (`kubectl cp localPath pod:containerPath`) and each exec
/// (`kubectl exec pod -- sh -c <args>`), and record the outcome on the object's
/// status (`podName`, `lastExecTime`, and `failed`/`message` on error). This is
/// the object-driven live-update controller — the one-shot apply of a
/// `LiveUpdate` spec to a running pod, distinct from the engine's
/// file-watch-triggered `live_update` path which streams build logs.
pub async fn reconcile_live_update(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("LiveUpdate", "default", name)
        .ok_or_else(|| format!("LiveUpdate {name} not found"))?;
    let selector = obj.object["spec"]["selector"]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("LiveUpdate {name} has no selector"));
    }
    let items = list_pods_for_selector(&selector).await?;
    let pod = items
        .first()
        .and_then(|p| p["metadata"]["name"].as_str())
        .unwrap_or("")
        .to_string();
    if pod.is_empty() {
        let _ = api.patch(
            "LiveUpdate",
            "default",
            name,
            serde_json::json!({ "status": { "podName": "", "failed": true, "message": "no target pod" } }),
        );
        return Err(format!("LiveUpdate {name}: no target pod"));
    }

    let empty = Vec::new();
    let use_kube_rs = crate::kube_client::use_kube_rs();
    let mut failure: Option<String> = None;
    // Syncs: copy each local path into the container (typed `copy_file` via the
    // attach API under kube-rs, else `kubectl cp`).
    for sync in obj.object["spec"]["syncs"].as_array().unwrap_or(&empty) {
        let local = sync["localPath"].as_str().unwrap_or("");
        let remote = sync["containerPath"].as_str().unwrap_or("");
        if local.is_empty() || remote.is_empty() {
            continue;
        }
        let result = if use_kube_rs {
            crate::kube_client::copy_file(&pod, local, remote).await
        } else {
            Command::new("kubectl")
                .args(["cp", local, &format!("{pod}:{remote}")])
                .output()
                .await
                .map_err(|e| format!("kubectl cp: {e}"))
                .and_then(|o| {
                    if o.status.success() {
                        Ok(())
                    } else {
                        Err(String::from_utf8_lossy(&o.stderr).trim().to_string())
                    }
                })
        };
        if let Err(e) = result {
            failure = Some(format!("sync {local} -> {remote} failed: {e}"));
            break;
        }
    }
    // Execs: run each command in the container (typed `exec` via the attach API
    // under kube-rs, else `kubectl exec`). Skipped if a sync already failed.
    if failure.is_none() {
        for exec in obj.object["spec"]["execs"].as_array().unwrap_or(&empty) {
            let args: Vec<String> = exec["args"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|v| v.as_str().unwrap_or("").to_string())
                        .collect()
                })
                .unwrap_or_default();
            if args.is_empty() {
                continue;
            }
            let sh = vec!["sh".to_string(), "-c".to_string(), args.join(" ")];
            let result = if use_kube_rs {
                crate::kube_client::exec(&pod, &sh).await
            } else {
                let mut argv = vec!["exec".to_string(), pod.clone(), "--".to_string()];
                argv.extend(sh);
                Command::new("kubectl")
                    .args(&argv)
                    .output()
                    .await
                    .map_err(|e| format!("kubectl exec: {e}"))
                    .and_then(|o| {
                        if o.status.success() {
                            Ok(())
                        } else {
                            Err(String::from_utf8_lossy(&o.stderr).trim().to_string())
                        }
                    })
            };
            if let Err(e) = result {
                failure = Some(format!("exec {args:?} failed: {e}"));
                break;
            }
        }
    }

    let now = Utc::now().to_rfc3339();
    let failed = failure.is_some();
    let _ = api.patch(
        "LiveUpdate",
        "default",
        name,
        serde_json::json!({
            "status": {
                "podName": pod,
                "lastExecTime": now,
                "failed": failed,
                "message": failure.clone().unwrap_or_default(),
            }
        }),
    );
    match failure {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Build a `key=value,...` label selector string from an object's
/// `spec.selector.matchLabels`.
fn pod_log_selector(object: &serde_json::Value) -> String {
    object["spec"]["selector"]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// Reconcile a `PodLogStream` object: fetch recent logs for its selector and
/// append them to the store under the resource span, recording the line count
/// on the object — the one-shot pod-log reconcile behind `POST …/reconcile`.
/// Continuous following is owned by the controller manager (per-pod follow).
pub async fn reconcile_pod_log_stream(
    api: &Arc<crate::api::store::ApiObjectStore>,
    store: &Arc<Store>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("PodLogStream", "default", name)
        .ok_or_else(|| format!("PodLogStream {name} not found"))?;
    let selector = obj.object["spec"]["selector"]["matchLabels"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| format!("{k}={}", v.as_str().unwrap_or("")))
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    if selector.is_empty() {
        return Err(format!("PodLogStream {name} has no selector"));
    }
    let text = if crate::kube_client::use_kube_rs() {
        crate::kube_client::pod_logs(&selector, 50).await?
    } else {
        let out = Command::new("kubectl")
            .args([
                "logs",
                "-l",
                &selector,
                "--tail=50",
                "--all-containers=true",
                "--prefix=false",
            ])
            .output()
            .await
            .map_err(|e| format!("kubectl logs: {e}"))?;
        String::from_utf8_lossy(&out.stdout).to_string()
    };
    let mut lines = 0u64;
    for line in text.lines() {
        store.append_log(Some(name), "INFO", &format!("{line}\n"));
        lines += 1;
    }
    let _ = api.patch(
        "PodLogStream",
        "default",
        name,
        serde_json::json!({ "status": { "lineCount": lines } }),
    );
    Ok(())
}

/// Reconcile a `DockerComposeService` object: query `docker compose ps` for the
/// service and write its running state onto the object — the Compose status
/// controller, object-driven and backed by the local Docker daemon.
pub async fn reconcile_docker_compose_service(
    api: &Arc<crate::api::store::ApiObjectStore>,
    name: &str,
) -> Result<(), String> {
    let obj = api
        .get("DockerComposeService", "default", name)
        .ok_or_else(|| format!("DockerComposeService {name} not found"))?;
    let service = obj.object["spec"]["service"]
        .as_str()
        .unwrap_or(name)
        .to_string();
    let project = obj.object["spec"]["project"]["name"]
        .as_str()
        .unwrap_or("")
        .to_string();
    let mut args: Vec<String> = vec!["compose".to_string()];
    if !project.is_empty() {
        args.push("-p".to_string());
        args.push(project);
    }
    args.extend([
        "ps".to_string(),
        "--format".to_string(),
        "json".to_string(),
        "--all".to_string(),
        service.clone(),
    ]);
    let out = Command::new("docker")
        .args(&args)
        .output()
        .await
        .map_err(|e| format!("docker compose ps: {e}"))?;
    // compose v2 prints one JSON object per line.
    let text = String::from_utf8_lossy(&out.stdout);
    let mut state = String::new();
    for line in text.lines() {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
            if v["Service"].as_str() == Some(service.as_str()) {
                state = v["State"].as_str().unwrap_or("").to_string();
            }
        }
    }
    let running = state == "running";
    let _ = api.patch(
        "DockerComposeService",
        "default",
        name,
        serde_json::json!({ "status": { "running": running, "state": state } }),
    );
    Ok(())
}

/// Aggregated status across all pods matching a workload's selector.
struct PodSummary {
    total: usize,
    ready: usize,
    restarts: i64,
    /// Runtime status for the resource: "ok" / "pending" / "error" / "none".
    runtime: &'static str,
}

impl PodSummary {
    fn all_ready(&self) -> bool {
        self.total > 0 && self.ready == self.total
    }
    /// Dashboard label: a phase for a single pod, else "<ready>/<total> ready".
    fn status_label(&self) -> String {
        if self.total == 1 && self.ready == 1 {
            "Running".to_string()
        } else if self.total == 1 {
            "Pending".to_string()
        } else {
            format!("{}/{} ready", self.ready, self.total)
        }
    }
}

/// Aggregate runtime status across every pod in a `kubectl get pods -o json`
/// item list: a pod is "ready" when all its containers are ready or it has
/// Succeeded; the resource is `ok` when all pods are ready, `error` if any pod
/// Failed, else `pending` (or `ok` when readiness is ignored).
fn aggregate_pod_status(items: &[serde_json::Value], readiness_ignored: bool) -> PodSummary {
    let total = items.len();
    let mut ready = 0usize;
    let mut restarts = 0i64;
    let mut any_failed = false;
    for pod in items {
        let phase = pod["status"]["phase"].as_str().unwrap_or("Unknown");
        let containers = pod["status"]["containerStatuses"].as_array();
        let containers_ready = containers
            .map(|cs| cs.iter().all(|c| c["ready"].as_bool().unwrap_or(false)))
            .unwrap_or(false);
        restarts += containers
            .map(|cs| {
                cs.iter()
                    .map(|c| c["restartCount"].as_i64().unwrap_or(0))
                    .sum::<i64>()
            })
            .unwrap_or(0);
        if containers_ready || phase == "Succeeded" {
            ready += 1;
        }
        if phase == "Failed" {
            any_failed = true;
        }
    }
    let runtime = if total == 0 {
        "pending"
    } else if any_failed {
        "error"
    } else if readiness_ignored || ready == total {
        "ok"
    } else {
        "pending"
    };
    PodSummary {
        total,
        ready,
        restarts,
        runtime,
    }
}

/// Stream a pod's logs (`kubectl logs -f`) into the resource span.
fn stream_pod_logs(pod: String, span: String, store: Arc<Store>) -> tokio::task::JoinHandle<()> {
    // Typed `Api::log_stream` follow under kube-rs, else `kubectl logs -f`.
    if crate::kube_client::use_kube_rs() {
        return tokio::spawn(async move {
            match crate::kube_client::log_stream(&pod, 20).await {
                Ok(reader) => stream_lines(reader, store, span, "INFO", None),
                Err(e) => store.append_log(Some(&span), "ERROR", &format!("log stream: {e}\n")),
            }
        });
    }
    tokio::spawn(async move {
        let mut child = match Command::new("kubectl")
            .args(["logs", "-f", "--all-containers", "--tail", "20", &pod])
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => return,
        };
        if let Some(out) = child.stdout.take() {
            stream_lines(out, store, span, "INFO", None);
        }
        let _ = child.wait().await;
    })
}

fn port_forward_args(pod: &str, spec: &PortForwardSpec) -> Vec<String> {
    vec![
        "port-forward".to_string(),
        format!("pod/{pod}"),
        format!("{}:{}", spec.local_port, spec.container_port),
        "--address".to_string(),
        spec.host.clone(),
    ]
}

fn stream_port_forward(
    pod: String,
    resource: String,
    spec: PortForwardSpec,
    store: Arc<Store>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        store.append_log(
            Some(&resource),
            "INFO",
            &format!(
                "Starting port-forward {}:{} -> {pod}:{}\n",
                spec.host, spec.local_port, spec.container_port
            ),
        );
        // Native kube-rs port-forward (in-process TCP proxy) under kube-rs, else
        // a `kubectl port-forward` child process.
        if crate::kube_client::use_kube_rs() {
            if let Err(e) = crate::kube_client::port_forward_listener(
                pod.clone(),
                spec.host.clone(),
                spec.local_port,
                spec.container_port,
            )
            .await
            {
                store.append_log(
                    Some(&resource),
                    "ERROR",
                    &format!("port-forward (kube-rs): {e}\n"),
                );
            }
            return;
        }
        let mut child = match Command::new("kubectl")
            .args(port_forward_args(&pod, &spec))
            .kill_on_drop(true)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                store.append_log(
                    Some(&resource),
                    "ERROR",
                    &format!("failed to start port-forward: {e}\n"),
                );
                return;
            }
        };
        if let Some(stderr) = child.stderr.take() {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                store.append_log(Some(&resource), "INFO", &format!("port-forward: {line}\n"));
            }
        }
        let _ = child.wait().await;
    })
}

/// A DisableToggle button bound to a resource (consumed by the web UI).
fn disable_button(resource: &str) -> UIButton {
    let mut annotations = std::collections::BTreeMap::new();
    annotations.insert(
        "tilt.dev/uibutton-type".to_string(),
        "DisableToggle".to_string(),
    );
    UIButton {
        metadata: Some(ObjectMeta {
            name: format!("{resource}-disable"),
            uid: uuid::Uuid::new_v4().to_string(),
            annotations: Some(annotations),
            ..Default::default()
        }),
        spec: Some(UIButtonSpec {
            location: UIComponentLocation {
                component_id: resource.to_string(),
                component_type: "Resource".to_string(),
            },
            text: "Disable".to_string(),
            icon_name: Some("toggle_on".to_string()),
            ..Default::default()
        }),
        status: Some(Default::default()),
    }
}

/// A `KubernetesApply` object with a `status` reflecting the last apply result
/// (`lastApplyTime` + `error`), layered onto the declared spec.
fn kubernetes_apply_object_with_status(
    m: &Manifest,
    last_apply_time: &str,
    error: Option<&str>,
) -> serde_json::Value {
    let mut obj = kubernetes_apply_object(m);
    if let Some(map) = obj.as_object_mut() {
        map.insert(
            "status".to_string(),
            serde_json::json!({
                "lastApplyTime": last_apply_time,
                "error": error.unwrap_or_default(),
            }),
        );
    }
    obj
}

/// Build a `KubernetesApply` API object from a Kubernetes manifest. Mirrors the
/// slice of Tilt's `KubernetesApplySpec` Starling can populate today: the YAML
/// to apply (or the custom apply command), plus the matched image build refs.
fn kubernetes_apply_object(m: &Manifest) -> serde_json::Value {
    let mut spec = serde_json::Map::new();
    if let Some(cmd) = &m.k8s_custom_apply_cmd {
        spec.insert(
            "applyCmd".to_string(),
            serde_json::json!({ "args": cmd.argv }),
        );
    } else {
        spec.insert(
            "yaml".to_string(),
            serde_json::json!(m.k8s_apply_docs.join("\n---\n")),
        );
    }
    let images: Vec<String> = m
        .docker_builds
        .iter()
        .map(|b| b.image_ref.clone())
        .collect();
    if !images.is_empty() {
        spec.insert("imageMaps".to_string(), serde_json::json!(images));
    }
    serde_json::json!({ "spec": serde_json::Value::Object(spec) })
}

/// Build the `Tiltfile` singleton API object. `resourceNames` is a Starling
/// convenience field (not in Tilt's `TiltfileStatus`) that records the
/// resources this config produced.
fn tiltfile_object(path: &Path, resource_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "spec": { "path": path.display().to_string() },
        "status": { "resourceNames": resource_names },
    })
}

/// Build a `FileWatch` API object from a manifest's watched deps + ignore rules,
/// mirroring the slice of Tilt's `FileWatchSpec` Starling populates. The object
/// is the source of truth the FileWatch controller starts watchers from, so it
/// also carries the trigger behavior: `manual` (file changes mark pending
/// instead of building) and `fallbackPaths` (changes that force a full rebuild
/// rather than a live update).
fn file_watch_object(m: &Manifest) -> serde_json::Value {
    let watched: Vec<String> = m.deps.iter().map(|p| p.display().to_string()).collect();
    let ignores: Vec<serde_json::Value> = m
        .ignore_rules
        .iter()
        .map(|r| {
            serde_json::json!({
                "basePath": r.base.display().to_string(),
                "patterns": [r.pattern],
            })
        })
        .collect();
    let fallback_paths: Vec<String> = live_update_fallback_paths(m)
        .iter()
        .map(|p| p.display().to_string())
        .collect();
    serde_json::json!({
        "spec": {
            "watchedPaths": watched,
            "ignores": ignores,
            "manual": !m.auto_on_change(),
            "fallbackPaths": fallback_paths,
        }
    })
}

/// Build a `PortForward` API object from a Kubernetes resource's port forwards,
/// mirroring the slice of Tilt's `PortForwardSpec` Starling populates.
fn port_forward_object(m: &Manifest) -> serde_json::Value {
    let forwards: Vec<serde_json::Value> = m
        .k8s_port_forwards
        .iter()
        .map(|pf| {
            serde_json::json!({
                "localPort": pf.local_port,
                "containerPort": pf.container_port,
                "host": pf.host,
                "path": pf.link_path,
                "name": pf.name,
            })
        })
        .collect();
    // Carry the pod selector so the port-forward reconciler can resolve a target.
    let match_labels: serde_json::Map<String, serde_json::Value> = m
        .pod_selector
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    serde_json::json!({
        "spec": {
            "forwards": forwards,
            "selector": { "matchLabels": serde_json::Value::Object(match_labels) },
        }
    })
}

/// Build a `LiveUpdate` API object from a manifest's live-update steps,
/// mirroring the slice of Tilt's `LiveUpdateSpec` Starling populates.
fn live_update_object(m: &Manifest) -> serde_json::Value {
    let mut syncs = Vec::new();
    let mut execs = Vec::new();
    let mut stop_paths = Vec::new();
    let mut restart = false;
    for step in &m.live_update {
        match step {
            crate::starlingfile::LiveUpdateStep::Sync { local, remote } => {
                syncs.push(serde_json::json!({ "localPath": local, "containerPath": remote }));
            }
            crate::starlingfile::LiveUpdateStep::Run { cmd, triggers, .. } => {
                execs.push(serde_json::json!({ "args": [cmd], "triggerPaths": triggers }));
            }
            crate::starlingfile::LiveUpdateStep::FallBackOn(paths) => {
                stop_paths.extend(paths.iter().cloned());
            }
            crate::starlingfile::LiveUpdateStep::RestartContainer => restart = true,
            crate::starlingfile::LiveUpdateStep::InitialSync => {}
        }
    }
    // Carry the pod selector so the live-update reconciler can resolve a target.
    let match_labels: serde_json::Map<String, serde_json::Value> = m
        .pod_selector
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    serde_json::json!({
        "spec": {
            "syncs": syncs,
            "execs": execs,
            "stopPaths": stop_paths,
            "restart": restart,
            "selector": { "matchLabels": serde_json::Value::Object(match_labels) },
        },
    })
}

/// Build a `DockerImage` API object for a built image reference, mirroring the
/// slice of Tilt's `DockerImageSpec` Starling populates.
fn docker_image_object(build: &crate::starlingfile::DockerBuild) -> serde_json::Value {
    serde_json::json!({
        "spec": {
            "ref": build.image_ref,
            "context": build.context.display().to_string(),
        },
    })
}

/// Build a `Cmd` API object from a local resource's one-shot update command,
/// mirroring the slice of Tilt's `CmdSpec` Starling populates.
fn cmd_object(m: &Manifest) -> serde_json::Value {
    let mut spec = serde_json::Map::new();
    spec.insert("args".to_string(), serde_json::json!(m.update_cmd.argv));
    if let Some(dir) = &m.update_cmd.workdir {
        spec.insert(
            "dir".to_string(),
            serde_json::json!(dir.display().to_string()),
        );
    }
    if !m.update_cmd.env.is_empty() {
        let env: Vec<String> = m
            .update_cmd
            .env
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        spec.insert("env".to_string(), serde_json::json!(env));
    }
    serde_json::json!({ "spec": serde_json::Value::Object(spec) })
}

/// Build an `ImageMap` API object for a built image reference, mirroring the
/// slice of Tilt's `ImageMapSpec` (the selector that maps a Tiltfile image
/// reference to the ref actually deployed).
fn image_map_object(image_ref: &str) -> serde_json::Value {
    serde_json::json!({ "spec": { "selector": image_ref } })
}

/// Build a `CmdImage` API object for a `custom_build` image (one whose build is
/// an arbitrary command), mirroring the slice of Tilt's `CmdImageSpec`.
fn cmd_image_object(build: &crate::starlingfile::DockerBuild) -> serde_json::Value {
    let args = build
        .command
        .as_ref()
        .map(|c| c.argv.clone())
        .unwrap_or_default();
    serde_json::json!({ "spec": { "ref": build.image_ref, "args": args } })
}

/// Build a `KubernetesDiscovery` API object from a Kubernetes resource's pod
/// selector, mirroring the slice of Tilt's `KubernetesDiscoverySpec`.
fn kubernetes_discovery_object(m: &Manifest) -> serde_json::Value {
    let match_labels: serde_json::Map<String, serde_json::Value> = m
        .pod_selector
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    serde_json::json!({
        "spec": { "selectors": [{ "matchLabels": serde_json::Value::Object(match_labels) }] },
    })
}

/// The annotation a client sets to ask the engine to rebuild a resource via the
/// API object store (Tilt's button-driven trigger, generalized).
const FORCE_TRIGGER_ANNOTATION: &str = "tilt.dev/force-trigger";

/// If an object-store event carries a non-empty force-trigger annotation,
/// return the resource name to rebuild. Add/Modify only — deletes never trigger.
/// The engine populates objects without this annotation, so its own writes do
/// not cause trigger loops.
fn force_trigger_target(event: &crate::api::store::ObjectEvent) -> Option<String> {
    use crate::api::store::ObjectEvent;
    let stored = match event {
        ObjectEvent::Added(o) | ObjectEvent::Modified(o) => o,
        ObjectEvent::Deleted(_) => return None,
    };
    let triggered = stored
        .object
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get(FORCE_TRIGGER_ANNOTATION))
        .and_then(|v| v.as_str())
        .is_some_and(|s| !s.is_empty());
    triggered.then(|| stored.name.clone())
}

/// A reconciler reacts to an API object store event by driving engine state —
/// the in-process analog of a Tilt controller-runtime controller. Reconcilers
/// are pure-dispatch: each inspects the event and applies any effect via the
/// `Store`. (Cluster-backed controllers are future work; these run in-process.)
trait Reconciler: Send + Sync {
    fn reconcile(&self, event: &crate::api::store::ObjectEvent, store: &Arc<Store>);
}

/// The set of registered reconcilers, dispatched in order for each event.
struct Reconcilers {
    items: Vec<Box<dyn Reconciler>>,
}

impl Reconcilers {
    fn new() -> Self {
        Self { items: Vec::new() }
    }
    fn register(&mut self, r: Box<dyn Reconciler>) {
        self.items.push(r);
    }
    fn dispatch(&self, event: &crate::api::store::ObjectEvent, store: &Arc<Store>) {
        for r in &self.items {
            r.reconcile(event, store);
        }
    }
    fn len(&self) -> usize {
        self.items.len()
    }
}

/// A `tilt.dev/force-trigger` annotation write enqueues a build for the resource.
struct ForceTriggerReconciler;
impl Reconciler for ForceTriggerReconciler {
    fn reconcile(&self, event: &crate::api::store::ObjectEvent, store: &Arc<Store>) {
        if let Some(target) = force_trigger_target(event) {
            let _ = store.trigger(&target);
        }
    }
}

/// A disable-source `ConfigMap` write toggles the resource's disable state.
/// Guarded on a real change so the engine's own materialize writes don't churn.
struct DisableConfigMapReconciler;
impl Reconciler for DisableConfigMapReconciler {
    fn reconcile(&self, event: &crate::api::store::ObjectEvent, store: &Arc<Store>) {
        if let Some((resource, disabled)) = disable_change_from_event(event) {
            if store.resource_exists(&resource) && store.is_resource_disabled(&resource) != disabled
            {
                store.set_resource_disabled(&resource, disabled);
                store.append_log(
                    Some(&resource),
                    "INFO",
                    &format!(
                        "{} via API ConfigMap\n",
                        if disabled { "Disabled" } else { "Enabled" }
                    ),
                );
            }
        }
    }
}

/// A `TriggerQueue` write enqueues builds for the resources in `spec.queue`.
/// Entries are plain names or `{name, nonce}` objects; a fresh `nonce` lets the
/// same resource be re-triggered (without one, a name triggers once per engine
/// lifetime). Dedupe is by `name:nonce`, so the engine's own `status.queue`
/// writes — which re-emit the object — don't re-fire builds.
struct TriggerQueueReconciler {
    seen: Mutex<HashSet<String>>,
}
impl Reconciler for TriggerQueueReconciler {
    fn reconcile(&self, event: &crate::api::store::ObjectEvent, store: &Arc<Store>) {
        use crate::api::store::ObjectEvent;
        let stored = match event {
            ObjectEvent::Added(o) | ObjectEvent::Modified(o) => o,
            ObjectEvent::Deleted(_) => return,
        };
        if stored.kind != "TriggerQueue" {
            return;
        }
        let Some(queue) = stored.object["spec"]["queue"].as_array() else {
            return;
        };
        let mut seen = self.seen.lock().unwrap();
        for entry in queue {
            let (name, nonce) = match entry {
                serde_json::Value::String(s) => (s.clone(), String::new()),
                serde_json::Value::Object(_) => (
                    entry["name"].as_str().unwrap_or("").to_string(),
                    entry["nonce"].as_str().unwrap_or("").to_string(),
                ),
                _ => continue,
            };
            if name.is_empty() {
                continue;
            }
            if seen.insert(format!("{name}:{nonce}")) {
                let _ = store.trigger(&name);
            }
        }
    }
}

/// The reconcilers the engine runs against its API object store.
fn default_reconcilers() -> Reconcilers {
    let mut r = Reconcilers::new();
    r.register(Box::new(ForceTriggerReconciler));
    r.register(Box::new(DisableConfigMapReconciler));
    r.register(Box::new(TriggerQueueReconciler {
        seen: Mutex::new(HashSet::new()),
    }));
    r
}

/// If an object-store event is a disable-source `ConfigMap` write, return the
/// resource it controls and whether it should be disabled. Lets a client toggle
/// a resource by writing `data.isDisabled` (Tilt's ConfigMap-backed disable).
fn disable_change_from_event(event: &crate::api::store::ObjectEvent) -> Option<(String, bool)> {
    use crate::api::store::ObjectEvent;
    let stored = match event {
        ObjectEvent::Added(o) | ObjectEvent::Modified(o) => o,
        ObjectEvent::Deleted(_) => return None,
    };
    if stored.kind != "ConfigMap" {
        return None;
    }
    let resource = stored.name.strip_suffix("-disable")?;
    let value = stored
        .object
        .get("data")
        .and_then(|d| d.get("isDisabled"))
        .and_then(|v| v.as_str())?;
    Some((resource.to_string(), value == "true"))
}

/// Build the `Session` singleton API object, mirroring the slice of Tilt's
/// `Session` that records the set of targets for this run.
fn session_object(resource_names: &[String]) -> serde_json::Value {
    serde_json::json!({
        "spec": {},
        "status": { "targets": resource_names },
    })
}

/// Build a `DockerComposeService` API object for a Compose-backed resource,
/// mirroring the slice of Tilt's `DockerComposeServiceSpec` Starling populates.
fn dc_service_object(m: &Manifest) -> serde_json::Value {
    serde_json::json!({
        "spec": { "service": m.name, "project": { "name": m.docker_compose_project } },
    })
}

/// Build a `DockerComposeLogStream` API object for a Compose-backed resource.
fn dc_log_stream_object(m: &Manifest) -> serde_json::Value {
    serde_json::json!({ "spec": { "service": m.name } })
}

/// Build a `PodLogStream` API object for a Kubernetes resource, mirroring the
/// slice of Tilt's `PodLogStreamSpec` (the pod selector whose logs to stream).
fn pod_log_stream_object(m: &Manifest) -> serde_json::Value {
    let match_labels: serde_json::Map<String, serde_json::Value> = m
        .pod_selector
        .iter()
        .map(|(k, v)| (k.clone(), serde_json::json!(v)))
        .collect();
    serde_json::json!({
        "spec": { "selector": { "matchLabels": serde_json::Value::Object(match_labels) } },
    })
}

/// Build the disable-source `ConfigMap` for a resource, mirroring how Tilt
/// stores enable/disable state in a `ConfigMap` (`isDisabled` = "true"/"false").
/// This is a descriptive snapshot of the current disable state.
fn disable_config_map_object(disabled: bool) -> serde_json::Value {
    serde_json::json!({
        "data": { "isDisabled": if disabled { "true" } else { "false" } },
    })
}

/// Build a `ToggleButton` API object for a resource's enable/disable toggle,
/// mirroring the slice of Tilt's `ToggleButtonSpec` Starling can express.
fn toggle_button_object(m: &Manifest) -> serde_json::Value {
    serde_json::json!({
        "spec": {
            "location": { "componentType": "Resource", "componentID": m.name },
            "on": { "text": "Disable" },
            "off": { "text": "Enable" },
        },
    })
}

/// Build the initial `UIResource` for a manifest before any build runs.
fn initial_resource(m: &Manifest, order: i32) -> UIResource {
    let mut st = UIResourceStatus {
        order: Some(order),
        trigger_mode: Some(m.trigger_mode),
        update_status: Some("pending".to_string()),
        runtime_status: Some(match m.kind {
            TargetKind::Local if m.serve_cmd.is_empty() => "not_applicable".to_string(),
            _ => "pending".to_string(),
        }),
        specs: vec![UIResourceTargetSpec {
            id: Some(m.name.clone()),
            target_type: Some(m.kind.target_type().to_string()),
            has_live_update: Some(false),
        }],
        disable_status: Some(DisableResourceStatus {
            enabled_count: 1,
            disabled_count: 0,
            state: "Enabled".to_string(),
            sources: vec![],
        }),
        endpoint_links: m
            .links
            .iter()
            .map(|(url, name)| UIResourceLink {
                url: Some(url.clone()),
                name: Some(name.clone()),
            })
            .collect(),
        ..Default::default()
    };
    if m.kind == TargetKind::Local {
        st.local_resource_info = Some(UIResourceLocal {
            pid: Some(0),
            is_test: Some(m.is_test),
            restart_count: None,
            last_start_time: None,
        });
    }
    UIResource {
        metadata: Some(ObjectMeta {
            name: m.name.clone(),
            uid: uuid::Uuid::new_v4().to_string(),
            labels: if m.labels.is_empty() {
                None
            } else {
                Some(m.labels.clone())
            },
            ..Default::default()
        }),
        spec: Some(UIResourceSpec {}),
        status: Some(st),
    }
}

/// Poll a resource's readiness probe in the background, reflecting the result
/// as a `Ready` condition and gating `runtime_status` (pending until the probe
/// first succeeds; back to pending if a previously-ready probe starts failing).
/// The handle is aborted by `spawn_serve` when the serve process exits.
fn spawn_readiness_probe(
    probe: ReadinessProbe,
    store: Arc<Store>,
    name: String,
    registry: Arc<Mutex<HashMap<String, ServiceEndpoint>>>,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        let timeout = Duration::from_secs_f64(probe.timeout_secs.max(0.1));
        let period = Duration::from_secs_f64(probe.period_secs.max(0.1));
        let success_threshold = probe.success_threshold.max(1);
        let failure_threshold = probe.failure_threshold.max(1);
        if probe.initial_delay_secs > 0.0 {
            tokio::time::sleep(Duration::from_secs_f64(probe.initial_delay_secs)).await;
        }
        let mut success_count = 0;
        let mut failure_count = 0;
        let mut ready = false;
        let mut last_ready: Option<bool> = None;
        let mut tick = tokio::time::interval(period);
        loop {
            tick.tick().await;
            let action = resolve_probe_action_ports(&probe.action, &registry);
            let result = match action {
                Ok(action) => crate::probe::run_probe_action(&action, timeout).await,
                Err(err) => Err(err),
            };
            if result.is_ok() {
                success_count += 1;
                failure_count = 0;
                if success_count >= success_threshold {
                    ready = true;
                }
            } else {
                failure_count += 1;
                success_count = 0;
                if failure_count >= failure_threshold {
                    ready = false;
                }
            }
            let changed = last_ready != Some(ready);
            last_ready = Some(ready);
            let message = result.as_ref().err().cloned();
            store.update_status(&name, |st| {
                st.conditions.retain(|c| c.condition_type != "Ready");
                st.conditions.push(UIResourceCondition {
                    condition_type: "Ready".to_string(),
                    status: if ready { "True" } else { "False" }.to_string(),
                    last_transition_time: Some(Utc::now().to_rfc3339()),
                    reason: Some(
                        if ready {
                            "ProbeSucceeded"
                        } else if result.is_ok() {
                            "ProbePending"
                        } else {
                            "ProbeFailed"
                        }
                        .to_string(),
                    ),
                    message: message.clone(),
                });
                if ready {
                    st.runtime_status = Some("ok".to_string());
                } else if st.runtime_status.as_deref() == Some("ok") {
                    // Was ready, now failing — drop back to not-ready.
                    st.runtime_status = Some("pending".to_string());
                }
            });
            if changed {
                if ready {
                    store.append_log(
                        Some(&name),
                        "INFO",
                        "Readiness probe succeeded; resource is ready\n",
                    );
                } else {
                    match &result {
                        Ok(()) => store.append_log(
                            Some(&name),
                            "INFO",
                            "Readiness probe waiting for success threshold\n",
                        ),
                        Err(e) => store.append_log(
                            Some(&name),
                            "WARN",
                            &format!("Readiness probe failed: {e}\n"),
                        ),
                    }
                }
            }
        }
    })
    .abort_handle()
}

fn resolve_probe_action_ports(
    action: &ProbeAction,
    registry: &Arc<Mutex<HashMap<String, ServiceEndpoint>>>,
) -> Result<ProbeAction, String> {
    match action {
        ProbeAction::Exec { command } => Ok(ProbeAction::Exec {
            command: command.clone(),
        }),
        ProbeAction::Tcp { host, port } => Ok(ProbeAction::Tcp {
            host: host.clone(),
            port: resolve_probe_port(port, registry)?,
        }),
        ProbeAction::Http {
            host,
            port,
            scheme,
            path,
        } => Ok(ProbeAction::Http {
            host: host.clone(),
            port: resolve_probe_port(port, registry)?,
            scheme: scheme.clone(),
            path: path.clone(),
        }),
    }
}

fn resolve_probe_port(
    port: &ProbePort,
    registry: &Arc<Mutex<HashMap<String, ServiceEndpoint>>>,
) -> Result<ProbePort, String> {
    match port {
        ProbePort::Number(_) => Ok(port.clone()),
        ProbePort::Deferred(raw) => {
            let key = raw
                .strip_prefix("${")
                .and_then(|s| s.strip_suffix('}'))
                .unwrap_or(raw);
            let value = service_env(registry)
                .into_iter()
                .find_map(|(name, value)| (name == key).then_some(value))
                .ok_or_else(|| format!("could not resolve probe port {raw}"))?;
            let port = value
                .parse::<u16>()
                .map_err(|_| format!("probe port {raw} resolved to invalid value {value:?}"))?;
            Ok(ProbePort::Number(port))
        }
    }
}

#[derive(Clone)]
struct ServeRouteMonitor {
    handle: crate::proxy::ProxyHandle,
    store: Arc<Store>,
    name: String,
    hostname: String,
    proxy_port: u16,
    current_port: Arc<Mutex<u16>>,
    proxy_reachable: Arc<Mutex<Option<bool>>>,
}

impl ServeRouteMonitor {
    fn new(
        handle: crate::proxy::ProxyHandle,
        store: Arc<Store>,
        name: String,
        hostname: String,
        proxy_port: u16,
        current_port: u16,
    ) -> Self {
        Self {
            handle,
            store,
            name,
            hostname,
            proxy_port,
            current_port: Arc::new(Mutex::new(current_port)),
            proxy_reachable: Arc::new(Mutex::new(None)),
        }
    }

    fn observe_line(&self, line: &str) {
        let Some(detected_port) = detect_local_listen_port(line) else {
            return;
        };
        let previous_port = {
            let mut current = self.current_port.lock().unwrap();
            if *current == detected_port {
                return;
            }
            let previous = *current;
            *current = detected_port;
            previous
        };

        let handle = self.handle.clone();
        let store = self.store.clone();
        let name = self.name.clone();
        tokio::spawn(async move {
            handle.register(&name, detected_port).await;
            store.append_log(
                Some(&name),
                "WARN",
                &format!(
                    "Detected serve_cmd listening on 127.0.0.1:{detected_port}; updated named route from PORT={previous_port}. This usually means the command ignored $PORT; set serve_port={detected_port} or make serve_cmd bind to $PORT to avoid this.\n"
                ),
            );
        });
    }

    fn start_health_checks(&self) -> tokio::task::AbortHandle {
        let monitor = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let mut tick = tokio::time::interval(Duration::from_secs(2));
            loop {
                tick.tick().await;
                monitor.check_proxy_health().await;
            }
        })
        .abort_handle()
    }

    async fn check_proxy_health(&self) {
        let health =
            crate::health::check_proxy_route(&self.hostname, self.proxy_port, "starling-health")
                .await;
        let changed = {
            let mut current = self.proxy_reachable.lock().unwrap();
            let changed = *current != Some(health.ok);
            *current = Some(health.ok);
            changed
        };

        self.store.update_status(&self.name, |st| {
            st.conditions
                .retain(|c| c.condition_type != "ProxyReachable");
            st.conditions.push(UIResourceCondition {
                condition_type: "ProxyReachable".to_string(),
                status: if health.ok { "True" } else { "False" }.to_string(),
                last_transition_time: Some(Utc::now().to_rfc3339()),
                reason: Some(health.reason.clone()),
                message: Some(health.message.clone()),
            });
        });

        if changed {
            self.store.append_log(
                Some(&self.name),
                if health.ok { "INFO" } else { "WARN" },
                &format!("Proxy health check: {}\n", health.message),
            );
        }
    }
}

/// Reconstruct the ignore rules recorded on a `FileWatch` object's
/// `spec.ignores` (each `{ basePath, patterns: [...] }`).
fn parse_ignore_rules(value: &serde_json::Value) -> Vec<IgnoreRule> {
    let mut rules = Vec::new();
    let Some(arr) = value.as_array() else {
        return rules;
    };
    for r in arr {
        let base = PathBuf::from(r["basePath"].as_str().unwrap_or(""));
        if let Some(patterns) = r["patterns"].as_array() {
            for p in patterns {
                if let Some(p) = p.as_str() {
                    rules.push(IgnoreRule {
                        base: base.clone(),
                        pattern: p.to_string(),
                    });
                }
            }
        }
    }
    rules
}

/// (Re)start the file watcher for a `FileWatch` object, reading its watched
/// paths / ignores / trigger behavior from the object spec. Any watcher already
/// running for this object name is stopped first (its spec may have changed).
fn start_file_watch(
    name: &str,
    object: &serde_json::Value,
    build_tx: &mpsc::UnboundedSender<BuildRequest>,
    store: &Arc<Store>,
    handles: &mut HashMap<String, Arc<AtomicBool>>,
) {
    if let Some(prev) = handles.remove(name) {
        prev.store(true, Ordering::Relaxed);
    }
    let spec = &object["spec"];
    let watched: Vec<PathBuf> = spec["watchedPaths"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default();
    if watched.is_empty() {
        return;
    }
    let ignore_rules = parse_ignore_rules(&spec["ignores"]);
    let fallback_paths: Vec<PathBuf> = spec["fallbackPaths"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_default();
    let manual = spec["manual"].as_bool().unwrap_or(false);
    let stop = Arc::new(AtomicBool::new(false));
    handles.insert(name.to_string(), stop.clone());
    spawn_notify_watcher(
        name.to_string(),
        watched,
        ignore_rules,
        fallback_paths,
        manual,
        build_tx.clone(),
        store.clone(),
        stop,
    );
}

/// Run the notify watcher for one `FileWatch` on a blocking thread, forwarding
/// debounced content changes as build requests. Exits when `stop` is set (the
/// controller stops/replaces the watcher) — checked after each debounce so a
/// replaced watcher yields to its successor without a duplicate build.
#[allow(clippy::too_many_arguments)]
fn spawn_notify_watcher(
    name: String,
    deps: Vec<PathBuf>,
    ignore_rules: Vec<IgnoreRule>,
    fallback_paths: Vec<PathBuf>,
    manual: bool,
    tx: mpsc::UnboundedSender<BuildRequest>,
    store: Arc<Store>,
    stop: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let (raw_tx, raw_rx) = std::sync::mpsc::channel::<notify::Result<Event>>();
        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = raw_tx.send(res);
        }) {
            Ok(w) => w,
            Err(e) => {
                store.append_log(
                    Some(&name),
                    "ERROR",
                    &format!("failed to create file watcher: {e}\n"),
                );
                return;
            }
        };
        for dep in &deps {
            let mode = if dep.is_dir() {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };
            if let Err(e) = watcher.watch(dep, mode) {
                store.append_log(
                    Some(&name),
                    "WARN",
                    &format!("can't watch {}: {e}\n", dep.display()),
                );
            }
        }
        // Debounce file events: collect a burst, then enqueue once.
        while let Ok(first) = raw_rx.recv() {
            if !is_content_event(&first) {
                continue;
            }
            let mut changed_paths = event_paths(&first);
            while let Ok(ev) = raw_rx.recv_timeout(Duration::from_millis(200)) {
                if is_content_event(&ev) {
                    changed_paths.extend(event_paths(&ev));
                }
            }
            if stop.load(Ordering::Relaxed) {
                break;
            }
            if !changed_paths.is_empty()
                && changed_paths
                    .iter()
                    .all(|path| is_ignored_by_rules(path, &ignore_rules))
            {
                store.append_log(
                    Some(&name),
                    "INFO",
                    "Ignored file change matched ignore rules\n",
                );
                continue;
            }
            store.append_log(Some(&name), "INFO", "Detected file change\n");
            let force_full = changed_paths
                .iter()
                .any(|path| matches_any_path(path, &fallback_paths));
            if store.is_resource_disabled(&name) {
                store.update_status(&name, |st| {
                    st.has_pending_changes = Some(true);
                    st.pending_build_since = Some(chrono::Utc::now().to_rfc3339());
                });
                store.append_log(
                    Some(&name),
                    "INFO",
                    "Resource is paused; live reload skipped\n",
                );
                continue;
            }
            if !manual {
                let request = if force_full {
                    store.append_log(
                        Some(&name),
                        "INFO",
                        "File change matched fall_back_on; forcing full rebuild\n",
                    );
                    BuildRequest::force_full_request(name.clone(), changed_paths.clone())
                } else {
                    BuildRequest::auto(name.clone(), changed_paths.clone())
                };
                if tx.send(request).is_err() {
                    break;
                }
            } else {
                // Manual mode: mark pending instead of building.
                store.update_status(&name, |st| {
                    st.has_pending_changes = Some(true);
                    st.pending_build_since = Some(chrono::Utc::now().to_rfc3339());
                });
            }
        }
    });
}

/// True for events that represent content/metadata changes worth rebuilding on.
fn is_content_event(res: &notify::Result<Event>) -> bool {
    match res {
        Ok(ev) => matches!(
            ev.kind,
            notify::EventKind::Create(_)
                | notify::EventKind::Modify(_)
                | notify::EventKind::Remove(_)
        ),
        Err(_) => false,
    }
}

fn event_paths(res: &notify::Result<Event>) -> Vec<PathBuf> {
    match res {
        Ok(ev) => ev.paths.clone(),
        Err(_) => vec![],
    }
}

fn is_ignored_by_rules(path: &Path, rules: &[IgnoreRule]) -> bool {
    let mut ignored = false;
    for rule in rules {
        let Some(rel) = path_relative_to(path, &rule.base) else {
            continue;
        };
        if ignore_pattern_matches(&rule.pattern, &rel) {
            ignored = !rule.pattern.trim_start().starts_with('!');
        }
    }
    ignored
}

fn live_update_fallback_paths(manifest: &Manifest) -> Vec<PathBuf> {
    manifest
        .live_update
        .iter()
        .flat_map(|step| match step {
            crate::starlingfile::LiveUpdateStep::FallBackOn(paths) => {
                paths.iter().map(PathBuf::from).collect::<Vec<PathBuf>>()
            }
            _ => Vec::new(),
        })
        .collect()
}

fn live_update_run_matches_triggers(
    triggers: &[String],
    changed_paths: Option<&[PathBuf]>,
) -> bool {
    if triggers.is_empty() {
        return true;
    }
    let Some(changed_paths) = changed_paths else {
        return true;
    };
    let triggers: Vec<PathBuf> = triggers.iter().map(PathBuf::from).collect();
    changed_paths
        .iter()
        .any(|path| matches_any_path(path, &triggers))
}

fn live_update_has_initial_sync(steps: &[crate::starlingfile::LiveUpdateStep]) -> bool {
    steps
        .iter()
        .any(|step| matches!(step, crate::starlingfile::LiveUpdateStep::InitialSync))
}

fn initial_sync_command(
    step: &crate::starlingfile::LiveUpdateStep,
    pod: &str,
) -> Option<(Cmd, String)> {
    match step {
        crate::starlingfile::LiveUpdateStep::Sync { local, remote } => Some((
            Cmd {
                argv: vec![
                    "kubectl".to_string(),
                    "cp".to_string(),
                    local.clone(),
                    format!("{pod}:{remote}"),
                ],
                workdir: None,
                env: vec![],
            },
            format!("  initial_sync {local} -> {remote}\n"),
        )),
        crate::starlingfile::LiveUpdateStep::Run { cmd, echo_off, .. } => Some((
            Cmd {
                argv: vec![
                    "kubectl".to_string(),
                    "exec".to_string(),
                    pod.to_string(),
                    "--".to_string(),
                    "sh".to_string(),
                    "-c".to_string(),
                    cmd.clone(),
                ],
                workdir: None,
                env: vec![],
            },
            format!(
                "  initial_sync run {}\n",
                if *echo_off {
                    "<redacted>"
                } else {
                    cmd.as_str()
                }
            ),
        )),
        _ => None,
    }
}

async fn run_initial_sync(
    store: Arc<Store>,
    name: String,
    pod: String,
    steps: Vec<crate::starlingfile::LiveUpdateStep>,
) {
    store.append_log(Some(&name), "INFO", "Initial sync started\n");
    for step in &steps {
        let Some((cmd, log)) = initial_sync_command(step, &pod) else {
            continue;
        };
        store.append_log(Some(&name), "INFO", &log);
        match run_to_completion(&cmd, &store, &name).await {
            Ok(true) => {}
            Ok(false) => {
                store.append_log(Some(&name), "ERROR", "Initial sync step failed\n");
                return;
            }
            Err(e) => {
                store.append_log(Some(&name), "ERROR", &format!("Initial sync failed: {e}\n"));
                return;
            }
        }
    }
    store.append_log(Some(&name), "INFO", "Initial sync complete\n");
}

fn matches_any_path(path: &Path, candidates: &[PathBuf]) -> bool {
    candidates
        .iter()
        .any(|candidate| path == candidate || path.starts_with(candidate))
}

fn path_relative_to(path: &Path, base: &Path) -> Option<String> {
    let path = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let base = std::fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
    let rel = path.strip_prefix(&base).ok()?;
    Some(rel.to_string_lossy().replace('\\', "/"))
}

fn ignore_pattern_matches(pattern: &str, rel: &str) -> bool {
    let mut pattern = pattern.trim();
    if let Some(rest) = pattern.strip_prefix('!') {
        pattern = rest.trim_start();
    }
    if pattern.is_empty() || pattern.starts_with('#') {
        return false;
    }
    let dir_only = pattern.ends_with('/');
    pattern = pattern.trim_start_matches('/').trim_end_matches('/');
    if pattern.is_empty() {
        return false;
    }
    let rel = rel.trim_start_matches("./");
    if dir_only {
        let prefix = format!("{}/", pattern);
        return rel == pattern || rel.starts_with(&prefix) || path_component_matches(rel, pattern);
    }
    if pattern.contains('/') {
        glob_match(pattern, rel)
    } else {
        rel.split('/')
            .any(|component| glob_match(pattern, component))
    }
}

fn path_component_matches(rel: &str, pattern: &str) -> bool {
    rel.split('/')
        .any(|component| glob_match(pattern, component))
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '*' if chars.peek() == Some(&'*') => {
                while chars.peek() == Some(&'*') {
                    chars.next();
                }
                regex.push_str(".*");
            }
            '*' => regex.push_str("[^/]*"),
            '?' => regex.push_str("[^/]"),
            '.' | '+' | '(' | ')' | '|' | '^' | '$' | '{' | '}' | '[' | ']' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            other => regex.push(other),
        }
    }
    regex.push('$');
    regex::Regex::new(&regex)
        .map(|re| re.is_match(text))
        .unwrap_or(false)
}

/// Spawn a command with stdout/stderr piped and streamed to the log store.
async fn spawn_streaming(
    cmd: &Cmd,
    store: &Arc<Store>,
    span: &str,
) -> std::io::Result<tokio::process::Child> {
    spawn_streaming_observed(cmd, store, span, None).await
}

/// Spawn a command with stdout/stderr piped, optionally observing each line.
async fn spawn_streaming_observed(
    cmd: &Cmd,
    store: &Arc<Store>,
    span: &str,
    observer: Option<ServeRouteMonitor>,
) -> std::io::Result<tokio::process::Child> {
    let mut command = Command::new(&cmd.argv[0]);
    command
        .args(&cmd.argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Dropping the child still kills the direct process; serve restarts also
        // signal the process group so shell/npm grandchildren don't survive.
        .kill_on_drop(true);
    configure_process_group(&mut command);
    if let Some(dir) = &cmd.workdir {
        command.current_dir(dir);
    }
    for (k, v) in &cmd.env {
        command.env(k, v);
    }
    let mut child = command.spawn()?;
    if let Some(out) = child.stdout.take() {
        stream_lines(
            out,
            store.clone(),
            span.to_string(),
            "INFO",
            observer.clone(),
        );
    }
    if let Some(err) = child.stderr.take() {
        stream_lines(err, store.clone(), span.to_string(), "INFO", observer);
    }
    Ok(child)
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

async fn terminate_process_group(pid: Arc<Mutex<Option<u32>>>) {
    let mut child_pid = None;
    for _ in 0..10 {
        child_pid = *pid.lock().unwrap();
        if child_pid.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let Some(child_pid) = child_pid else {
        return;
    };
    terminate_process_group_by_pid(child_pid).await;
}

#[cfg(unix)]
async fn terminate_process_group_by_pid(pid: u32) {
    let pgid = -(pid as i32);
    unsafe {
        libc::kill(pgid, libc::SIGTERM);
    }
    tokio::time::sleep(Duration::from_millis(750)).await;
    let still_running = unsafe { libc::kill(pgid, 0) == 0 };
    if still_running {
        unsafe {
            libc::kill(pgid, libc::SIGKILL);
        }
    }
}

#[cfg(not(unix))]
async fn terminate_process_group_by_pid(_pid: u32) {}

/// Run a command, feeding `stdin_data` to its stdin, streaming output.
async fn run_with_stdin(
    argv: &[String],
    stdin_data: &str,
    store: &Arc<Store>,
    span: &str,
) -> Result<bool, String> {
    let mut child = Command::new(&argv[0])
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", argv.join(" ")))?;
    if let Some(mut stdin) = child.stdin.take() {
        let data = stdin_data.to_string();
        // Write+close stdin before awaiting exit to avoid deadlock.
        stdin
            .write_all(data.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        drop(stdin);
    }
    if let Some(out) = child.stdout.take() {
        stream_lines(out, store.clone(), span.to_string(), "INFO", None);
    }
    if let Some(err) = child.stderr.take() {
        stream_lines(err, store.clone(), span.to_string(), "ERROR", None);
    }
    let status = child.wait().await.map_err(|e| e.to_string())?;
    Ok(status.success())
}

/// Run a command to completion, streaming output. Returns Ok(success).
async fn run_to_completion(cmd: &Cmd, store: &Arc<Store>, span: &str) -> Result<bool, String> {
    let mut child = spawn_streaming(cmd, store, span)
        .await
        .map_err(|e| format!("spawn {}: {e}", cmd.display()))?;
    let status = child.wait().await.map_err(|e| e.to_string())?;
    Ok(status.success())
}

/// Resources that registered a `k8s_custom_deploy(delete_cmd=...)`, paired with
/// the command to run when tearing them down. Used by `starling down`.
pub fn k8s_down_specs(manifests: &[Manifest]) -> Vec<(String, Cmd)> {
    manifests
        .iter()
        .filter_map(|m| {
            m.k8s_custom_delete_cmd
                .clone()
                .map(|cmd| (m.name.clone(), cmd))
        })
        .collect()
}

/// Run the custom delete commands for `k8s_custom_deploy` resources during
/// `starling down`. In dry-run mode the commands are logged but not executed.
/// Errors are logged per resource and do not abort the remaining deletes.
pub async fn run_k8s_down(specs: &[(String, Cmd)], store: &Arc<Store>, dry_run: bool) {
    for (name, cmd) in specs {
        if dry_run {
            store.append_log(
                Some(name),
                "INFO",
                &format!("[dry-run] would run delete_cmd: {}\n", cmd.display()),
            );
            continue;
        }
        store.append_log(
            Some(name),
            "INFO",
            &format!("Running k8s_custom_deploy delete_cmd: {}\n", cmd.display()),
        );
        let span = format!("{name}:down");
        match run_to_completion(cmd, store, &span).await {
            Ok(true) => store.append_log(Some(name), "INFO", "delete_cmd succeeded\n"),
            Ok(false) => store.append_log(Some(name), "ERROR", "delete_cmd failed\n"),
            Err(e) => store.append_log(Some(name), "ERROR", &format!("delete_cmd error: {e}\n")),
        }
    }
}

fn desired_port_leases(port_leases: &[NamedPortLease]) -> HashMap<String, Option<u16>> {
    let mut desired = HashMap::new();
    for lease in port_leases {
        desired.insert(lease.name.clone(), lease.preferred);
    }
    desired
}

async fn run_parallel_local_build(
    name: String,
    m: Manifest,
    store: Arc<Store>,
    service_registry: Arc<Mutex<HashMap<String, ServiceEndpoint>>>,
) {
    if store.is_resource_disabled(&name) {
        let now = Utc::now().to_rfc3339();
        store.update_status(&name, |st| {
            st.queued = Some(false);
            st.has_pending_changes = Some(true);
            st.pending_build_since = Some(now);
            st.update_status = Some("none".to_string());
        });
        store.append_log(Some(&name), "INFO", "Resource is paused; build skipped\n");
        return;
    }

    let now = Utc::now().to_rfc3339();
    let span = format!("{name}:build");
    store.update_status(&name, |st| {
        st.queued = Some(false);
        st.pending_build_since = None;
        st.update_status = Some("in_progress".to_string());
        st.current_build = Some(UIBuildRunning {
            start_time: Some(now.clone()),
            span_id: Some(span.clone()),
        });
    });

    let update_cmd = with_service_env(m.update_cmd.clone(), &service_registry);
    store.append_log(
        Some(&name),
        "INFO",
        &format!("Building: {}\n", update_cmd.display()),
    );
    let result = run_to_completion(&update_cmd, &store, &name).await;
    let finish = Utc::now().to_rfc3339();
    let error = match result {
        Ok(true) => None,
        Ok(false) => Some("command exited non-zero".to_string()),
        Err(e) => Some(e),
    };
    let ok = error.is_none();
    if let Some(err) = &error {
        store.append_log(Some(&name), "ERROR", &format!("Build failed: {err}\n"));
    } else {
        store.append_log(Some(&name), "INFO", "Build succeeded\n");
    }
    store.update_status(&name, |st| {
        st.current_build = None;
        st.last_deploy_time = Some(finish.clone());
        st.update_status = Some(if ok { "ok" } else { "error" }.to_string());
        if st.runtime_status.is_none() {
            st.runtime_status = Some("not_applicable".to_string());
        }
        st.build_history.insert(
            0,
            UIBuildTerminated {
                start_time: Some(now.clone()),
                finish_time: Some(finish.clone()),
                span_id: Some(span.clone()),
                error: error.clone(),
                ..Default::default()
            },
        );
        st.build_history.truncate(10);
    });
}

fn with_service_env(mut cmd: Cmd, registry: &Arc<Mutex<HashMap<String, ServiceEndpoint>>>) -> Cmd {
    let mut env = service_env(registry);
    env.extend(cmd.env);
    cmd.env = env;
    cmd
}

fn service_env(registry: &Arc<Mutex<HashMap<String, ServiceEndpoint>>>) -> Vec<(String, String)> {
    let services = registry.lock().unwrap().clone();
    let mut env = vec![];
    let mut service_json = serde_json::Map::new();
    for (name, endpoint) in services {
        let key = service_env_key(&name);
        if let Some(host) = &endpoint.host {
            env.push((format!("STARLING_{key}_HOST"), host.clone()));
        }
        if let Some(port) = endpoint.port {
            env.push((format!("STARLING_{key}_PORT"), port.to_string()));
        }
        if let Some(url) = &endpoint.url {
            env.push((format!("STARLING_{key}_URL"), url.clone()));
        }
        service_json.insert(
            name,
            serde_json::to_value(endpoint).unwrap_or_else(|_| serde_json::Value::Null),
        );
    }
    env.push((
        "STARLING_SERVICES_JSON".to_string(),
        serde_json::Value::Object(service_json).to_string(),
    ));
    env
}

fn service_env_key(name: &str) -> String {
    let mut out = String::new();
    let mut previous_sep = false;
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            previous_sep = false;
        } else if !previous_sep && !out.is_empty() {
            out.push('_');
            previous_sep = true;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        "SERVICE".to_string()
    } else {
        out
    }
}

/// Forward each line of an async reader into the log store under `span`.
fn stream_lines<R>(
    reader: R,
    store: Arc<Store>,
    span: String,
    level: &'static str,
    observer: Option<ServeRouteMonitor>,
) where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            store.append_log(Some(&span), level, &format!("{line}\n"));
            if let Some(observer) = &observer {
                observer.observe_line(&line);
            }
        }
    });
}

const SERVE_PORT_FLAGS: &[&str] = &[
    "--port",
    "-p",
    "--listen-port",
    "--http-port",
    "--server-port",
    "--serve-port",
    "--web-port",
];

fn rewrite_serve_cmd_port_args(cmd: &mut Cmd, port: u16) -> bool {
    if let Some(command_index) = shell_command_index(&cmd.argv) {
        if let Some(rewritten) = rewrite_shell_port_args(&cmd.argv[command_index], port) {
            cmd.argv[command_index] = rewritten;
            return true;
        }
        return false;
    }

    rewrite_argv_port_args(&mut cmd.argv, port)
}

fn shell_command_index(argv: &[String]) -> Option<usize> {
    let shell = argv.first()?.rsplit('/').next().unwrap_or_default();
    let flag = argv.get(1)?.as_str();
    if matches!(shell, "sh" | "bash" | "zsh") && matches!(flag, "-c" | "-lc") && argv.len() > 2 {
        Some(2)
    } else {
        None
    }
}

fn rewrite_argv_port_args(argv: &mut [String], port: u16) -> bool {
    let mut changed = false;
    let port = port.to_string();
    let mut i = 0;
    while i < argv.len() {
        if let Some((flag, value)) = argv[i].split_once('=') {
            if is_serve_port_flag(flag) && is_numeric_port(value) {
                argv[i] = format!("{flag}={port}");
                changed = true;
            }
        } else if is_serve_port_flag(&argv[i])
            && argv.get(i + 1).is_some_and(|arg| is_numeric_port(arg))
        {
            argv[i + 1] = port.clone();
            changed = true;
            i += 1;
        }
        i += 1;
    }
    changed
}

fn rewrite_shell_port_args(command: &str, port: u16) -> Option<String> {
    let port = port.to_string();
    let flags = r"--(?:port|listen-port|http-port|server-port|serve-port|web-port)|-p";
    let equals = Regex::new(&format!(
        r"(?P<flag>(?:{flags})=)(?P<port>[1-9][0-9]{{1,4}})(?P<end>\b)"
    ))
    .expect("serve port equals regex compiles");
    let after_equals = equals.replace_all(command, |caps: &regex::Captures<'_>| {
        format!("{}{}{}", &caps["flag"], port, &caps["end"])
    });
    let separated = Regex::new(&format!(
        r"(?P<flag>(?:{flags}))(?P<sep>\s+)(?P<port>[1-9][0-9]{{1,4}})(?P<end>\s|$)"
    ))
    .expect("serve port separated regex compiles");
    let rewritten = separated.replace_all(&after_equals, |caps: &regex::Captures<'_>| {
        format!("{}{}{}{}", &caps["flag"], &caps["sep"], port, &caps["end"])
    });
    let rewritten = rewritten.into_owned();
    (rewritten != command).then_some(rewritten)
}

fn is_serve_port_flag(flag: &str) -> bool {
    SERVE_PORT_FLAGS.contains(&flag)
}

fn is_numeric_port(value: &str) -> bool {
    value.parse::<u16>().is_ok_and(|port| port != 0)
}

fn detect_local_listen_port(line: &str) -> Option<u16> {
    let lower = line.to_ascii_lowercase();
    for prefix in [
        "http://127.0.0.1:",
        "http://localhost:",
        "https://127.0.0.1:",
        "https://localhost:",
    ] {
        if let Some(port) = find_port_after(&lower, prefix) {
            return Some(port);
        }
    }

    let looks_like_listen_line = ["listen", "serving", "running", "ready", "local:", "started"]
        .iter()
        .any(|needle| lower.contains(needle));
    if !looks_like_listen_line {
        return None;
    }

    find_port_after(&lower, "127.0.0.1:").or_else(|| find_port_after(&lower, "localhost:"))
}

fn find_port_after(line: &str, prefix: &str) -> Option<u16> {
    let mut search_start = 0;
    while let Some(relative_index) = line[search_start..].find(prefix) {
        let port_start = search_start + relative_index + prefix.len();
        if let Some(port) = parse_port_at(line, port_start) {
            return Some(port);
        }
        search_start = port_start;
    }
    None
}

fn parse_port_at(line: &str, start: usize) -> Option<u16> {
    let bytes = line.as_bytes();
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == start {
        return None;
    }
    line[start..end]
        .parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_vite_local_url() {
        assert_eq!(
            detect_local_listen_port("  ➜  Local:   http://127.0.0.1:8090/"),
            Some(8090)
        );
    }

    #[test]
    fn detects_plain_listening_message() {
        assert_eq!(
            detect_local_listen_port("api listening on 127.0.0.1:8088"),
            Some(8088)
        );
    }

    #[test]
    fn ignores_incidental_localhost_ports_without_listener_context() {
        assert_eq!(
            detect_local_listen_port("database url postgres://127.0.0.1:5432/app"),
            None
        );
    }

    /// End-to-end Kubernetes integration test against a real cluster. Gated:
    /// only runs with `STARLING_K8S_IT=1` AND a `kind-*` kube context, so it
    /// never touches a non-local cluster and is skipped in normal `cargo test`.
    /// Run with: `STARLING_K8S_IT=1 cargo test -- --ignored k8s_integration`
    #[test]
    #[ignore]
    fn k8s_integration_apply_discovery_and_status() {
        use std::io::Write;
        use std::process::{Command, Stdio};

        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        // Hard safety guard: refuse to run against anything but a kind cluster.
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .expect("kubectl")
                .stdout,
        )
        .unwrap();
        let ctx = ctx.trim();
        assert!(
            ctx.starts_with("kind-"),
            "refusing to run against non-kind context: {ctx}"
        );

        let apply = |verb: &str, yaml: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };

        let yaml = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: starling-it
spec:
  replicas: 1
  selector:
    matchLabels:
      app: starling-it
  template:
    metadata:
      labels:
        app: starling-it
    spec:
      containers:
      - name: app
        image: registry.k8s.io/pause:3.9
"#;
        // 1. Apply (mirrors the engine's `kubectl apply -f -`).
        assert!(apply("apply", yaml), "apply failed");

        // 2. Discover pods and run the engine's status aggregation on real JSON.
        let mut summary = None;
        for _ in 0..30 {
            let out = Command::new("kubectl")
                .args(["get", "pods", "-l", "app=starling-it", "-o", "json"])
                .output()
                .unwrap();
            if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                if let Some(items) = json["items"].as_array() {
                    if !items.is_empty() {
                        let s = aggregate_pod_status(items, false);
                        if s.runtime == "ok" {
                            summary = Some(s);
                            break;
                        }
                        summary = Some(s);
                    }
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let summary = summary.expect("no pods discovered");
        assert!(summary.total >= 1, "expected >=1 pod");
        // The pause container becomes ready, so aggregation should reach "ok".
        assert_eq!(summary.runtime, "ok", "pod never became ready");

        // 3. Clean up.
        apply("delete", yaml);
    }

    /// The object-driven cluster-backed reconciler: create a `KubernetesApply`
    /// object, reconcile it against kind, and verify the resource lands and the
    /// object's status is updated. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_apply_reconciler() {
        use std::process::Command;

        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(
            ctx.trim().starts_with("kind-"),
            "refusing to run against non-kind context: {}",
            ctx.trim()
        );

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        let yaml = r#"
apiVersion: v1
kind: ConfigMap
metadata:
  name: starling-rec
data:
  hello: world
"#;
        api.create(
            "KubernetesApply",
            "default",
            "starling-rec",
            serde_json::json!({ "spec": { "yaml": yaml } }),
        )
        .unwrap();

        // Reconcile: object-driven apply to the real cluster.
        reconcile_kubernetes_apply(&api, "starling-rec")
            .await
            .expect("reconcile applied to cluster");

        let on_cluster = Command::new("kubectl")
            .args(["get", "configmap", "starling-rec", "-o", "name"])
            .output()
            .unwrap();
        let found = String::from_utf8_lossy(&on_cluster.stdout).contains("starling-rec");

        let obj = api
            .get("KubernetesApply", "default", "starling-rec")
            .unwrap();
        let status_ok = obj.object["status"]["lastApplyTime"].is_string()
            && obj.object["status"]["error"] == serde_json::json!("");

        // Clean up.
        Command::new("kubectl")
            .args(["delete", "configmap", "starling-rec", "--ignore-not-found"])
            .output()
            .ok();

        assert!(
            found,
            "reconciler did not apply the ConfigMap to the cluster"
        );
        assert!(status_ok, "reconciler did not record success on the object");
    }

    /// Apply-time image injection: a `KubernetesApply` referencing an `ImageMap`
    /// has its workload image rewritten to the ImageMap's resolved
    /// `status.image` before being applied — the object-driven image-injection
    /// path, verified end-to-end. The placeholder image (`example/...`) is never
    /// deployed; the pod runs the resolved, pullable ref. Gated
    /// (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_apply_injects_image_map() {
        use std::process::Command;

        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(
            ctx.trim().starts_with("kind-"),
            "refusing to run against non-kind context: {}",
            ctx.trim()
        );

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        // The workload references the Tiltfile placeholder image, which must
        // never reach the cluster as-is — it is unpullable.
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-im-it
spec:
  containers:
  - name: app
    image: example/starling-im-it
    command: ["sh", "-c", "sleep 3600"]
"#;
        api.create(
            "KubernetesApply",
            "default",
            "starling-im-it",
            serde_json::json!({
                "spec": { "yaml": yaml, "imageMaps": ["example/starling-im-it"] }
            }),
        )
        .unwrap();
        // A completed build's resolved, pullable ref recorded on the ImageMap.
        api.create(
            "ImageMap",
            "default",
            "example/starling-im-it",
            serde_json::json!({
                "spec": { "selector": "example/starling-im-it" },
                "status": { "image": "busybox:1.36" }
            }),
        )
        .unwrap();

        reconcile_kubernetes_apply(&api, "starling-im-it")
            .await
            .expect("apply with image-map injection");

        let img = Command::new("kubectl")
            .args([
                "get",
                "pod",
                "starling-im-it",
                "-o",
                "jsonpath={.spec.containers[0].image}",
            ])
            .output()
            .unwrap();
        let deployed = String::from_utf8_lossy(&img.stdout).to_string();

        // Clean up.
        Command::new("kubectl")
            .args([
                "delete",
                "pod",
                "starling-im-it",
                "--ignore-not-found",
                "--force",
                "--grace-period=0",
            ])
            .output()
            .ok();

        assert_eq!(
            deployed, "busybox:1.36",
            "ImageMap status.image was not injected into the deployed workload"
        );
    }

    /// BuildKit secret passthrough: build an image whose `RUN` mounts a secret,
    /// proving `--secret` reaches the BuildKit builder. Gated `STARLING_DC_IT=1`
    /// (needs the local Docker daemon).
    #[tokio::test]
    #[ignore]
    async fn dc_integration_buildkit_secret_passthrough() {
        if std::env::var("STARLING_DC_IT").is_err() {
            eprintln!("skipping: set STARLING_DC_IT=1 to run");
            return;
        }
        let dir = std::env::temp_dir().join(format!("starling-bk-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let secret_file = dir.join("secret.txt");
        std::fs::write(&secret_file, "top-secret-value").unwrap();

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let db = crate::starlingfile::DockerBuild {
            // RUN fails unless the secret is actually mounted by BuildKit.
            dockerfile_contents: Some(
                "FROM busybox:1.36\nRUN --mount=type=secret,id=sek test -s /run/secrets/sek\n"
                    .to_string(),
            ),
            secrets: vec![format!("id=sek,src={}", secret_file.display())],
            ..plain_docker_build("starling-bk-it:latest")
        };
        let mut db = db;
        db.context = dir.clone();

        let result = build_image(&db, &store, "bk").await;

        // Clean up image + dir.
        std::process::Command::new("docker")
            .args(["rmi", "-f", "starling-bk-it:latest"])
            .output()
            .ok();
        let _ = std::fs::remove_dir_all(&dir);

        result.expect("BuildKit build with secret should succeed");
    }

    /// The Docker Compose status reconciler: bring up a real Compose service via
    /// the local Docker daemon, reconcile a `DockerComposeService` object, and
    /// verify it records the running state. Gated behind `STARLING_DC_IT=1`.
    #[tokio::test]
    #[ignore]
    async fn dc_integration_compose_status_reconciler() {
        use std::process::Command;
        if std::env::var("STARLING_DC_IT").is_err() {
            eprintln!("skipping: set STARLING_DC_IT=1 to run");
            return;
        }
        let dir = std::env::temp_dir().join(format!("starling-dc-it-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let compose = dir.join("docker-compose.yml");
        std::fs::write(
            &compose,
            "services:\n  web:\n    image: busybox:1.36\n    command: [\"sh\", \"-c\", \"sleep 3600\"]\n",
        )
        .unwrap();
        let project = "starling-dc-it";
        let file = compose.to_string_lossy().to_string();

        let compose_cmd = |args: &[&str]| {
            Command::new("docker")
                .args(["compose", "-p", project, "-f", &file])
                .args(args)
                .output()
                .unwrap()
                .status
                .success()
        };
        assert!(compose_cmd(&["up", "-d"]), "compose up failed");

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "DockerComposeService",
            "default",
            "web",
            serde_json::json!({ "spec": { "service": "web", "project": { "name": project } } }),
        )
        .unwrap();

        let mut running = false;
        for _ in 0..15 {
            reconcile_docker_compose_service(&api, "web")
                .await
                .expect("compose reconcile");
            let obj = api.get("DockerComposeService", "default", "web").unwrap();
            if obj.object["status"]["running"].as_bool() == Some(true) {
                running = true;
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        compose_cmd(&["down"]);
        let _ = std::fs::remove_dir_all(&dir);
        assert!(running, "compose service status was not reported running");
    }

    /// The pod-log reconciler: deploy a pod that logs a known line, reconcile a
    /// `PodLogStream` object, and verify the line reached the store + line count.
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_pod_log_reconciler() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let apply = |verb: &str, yaml: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-logger
  labels:
    app: starling-logger
spec:
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "echo starling-log-marker; sleep 3600"]
"#;
        assert!(apply("apply", yaml), "pod apply failed");

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "PodLogStream",
            "default",
            "starling-logger",
            serde_json::json!({ "spec": { "selector": { "matchLabels": { "app": "starling-logger" } } } }),
        )
        .unwrap();

        // Reconcile until the log line shows up (pod needs to start).
        let mut found = false;
        for _ in 0..30 {
            let _ = reconcile_pod_log_stream(&api, &store, "starling-logger").await;
            let logs = store.query_logs(&crate::store::LogQuery {
                span: Some("starling-logger".to_string()),
                ..Default::default()
            });
            if logs.iter().any(|l| l.text.contains("starling-log-marker")) {
                found = true;
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        apply("delete", yaml);
        assert!(found, "pod log line was not captured into the store");
    }

    /// The port-forward reconciler: deploy a pod, reconcile a `PortForward`
    /// object, and verify it resolves the target pod onto its status.
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_port_forward_reconciler() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-pf
  labels:
    app: starling-pf
spec:
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "PortForward",
            "default",
            "starling-pf",
            serde_json::json!({ "spec": { "forwards": [{ "localPort": 0, "containerPort": 80 }], "selector": { "matchLabels": { "app": "starling-pf" } } } }),
        )
        .unwrap();

        let mut pod = String::new();
        for _ in 0..30 {
            reconcile_port_forward(&api, "starling-pf")
                .await
                .expect("port-forward reconcile");
            let obj = api.get("PortForward", "default", "starling-pf").unwrap();
            pod = obj.object["status"]["podName"]
                .as_str()
                .unwrap_or("")
                .to_string();
            if !pod.is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        apply("delete");
        assert!(pod.starts_with("starling-pf"), "no target pod resolved");
    }

    /// The **controller manager** (maintained-controller model): spawn the
    /// background reconcile loop, then create a `KubernetesDiscovery` object and
    /// deploy a pod — and verify the loop converges the object's status WITHOUT
    /// any manual `reconcile_*` call. This is what distinguishes a continuous
    /// controller from one-shot reconcile. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_controller_manager_converges() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-cm
  labels:
    app: starling-cm
spec:
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "KubernetesDiscovery",
            "default",
            "starling-cm",
            serde_json::json!({ "spec": { "selectors": [{ "matchLabels": { "app": "starling-cm" } }] } }),
        )
        .unwrap();

        // Spawn the maintained controller loop with a short interval. We never
        // call a reconciler directly below — only the loop does.
        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let handle = spawn_controller_manager(api.clone(), store, Duration::from_millis(500));

        let mut total = 0u64;
        for _ in 0..40 {
            // Yield to the runtime (not std::thread::sleep, which would block the
            // single-threaded test executor and starve the spawned loop).
            tokio::time::sleep(Duration::from_millis(500)).await;
            let obj = api
                .get("KubernetesDiscovery", "default", "starling-cm")
                .unwrap();
            total = obj.object["status"]["totalPods"].as_u64().unwrap_or(0);
            if total >= 1 {
                break;
            }
        }
        handle.abort();
        apply("delete");
        assert!(
            total >= 1,
            "controller loop never converged discovery status"
        );
    }

    /// The controller-managed **port-forward process lifecycle**: deploy a pod
    /// serving HTTP, create a `PortForward` object, and spawn the controller
    /// manager — which must resolve the target, start a persistent
    /// `kubectl port-forward` process, and keep it running. Verify by making a
    /// real HTTP request through the forwarded local port. Then delete the object
    /// and confirm the controller tears the forward down (the port stops
    /// accepting). Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn k8s_integration_controller_managed_port_forward() {
        use std::io::{Read, Write};
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        // A fixed, likely-free local port; the container serves HTTP on 80.
        let local_port: u16 = 18099;
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-pflife
  labels:
    app: starling-pflife
spec:
  containers:
  - name: web
    image: nginx:1.27-alpine
    ports:
    - containerPort: 80
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");
        let _ = Command::new("kubectl")
            .args([
                "wait",
                "--for=condition=Ready",
                "pod/starling-pflife",
                "--timeout=90s",
            ])
            .output()
            .unwrap();

        // Try an HTTP GET to the forwarded local port; Ok(true) if it responds.
        let http_get = move || -> bool {
            match std::net::TcpStream::connect(("127.0.0.1", local_port)) {
                Ok(mut s) => {
                    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                    if s.write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
                        .is_err()
                    {
                        return false;
                    }
                    let mut buf = String::new();
                    let _ = s.read_to_string(&mut buf);
                    buf.contains("HTTP/1") && buf.to_lowercase().contains("nginx")
                }
                Err(_) => false,
            }
        };

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "PortForward",
            "default",
            "starling-pflife",
            serde_json::json!({
                "spec": {
                    "forwards": [{ "localPort": local_port, "containerPort": 80, "host": "127.0.0.1" }],
                    "selector": { "matchLabels": { "app": "starling-pflife" } }
                }
            }),
        )
        .unwrap();

        // The controller manager must resolve the target and start the forward.
        let handle =
            spawn_controller_manager(api.clone(), store.clone(), Duration::from_millis(500));

        let mut forwarded = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if http_get() {
                forwarded = true;
                break;
            }
        }

        // Delete the object; the controller should tear the forward down.
        api.delete("PortForward", "default", "starling-pflife");
        let mut torn_down = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if !http_get() {
                torn_down = true;
                break;
            }
        }

        handle.abort();
        apply("delete");

        assert!(
            forwarded,
            "controller did not establish a working port-forward"
        );
        assert!(
            torn_down,
            "controller did not tear down the forward after object deletion"
        );
    }

    /// The **native kube-rs port-forward**: with `STARLING_KUBE_RS=1`,
    /// `stream_port_forward` proxies a local TCP port to the pod over the typed
    /// `Api::portforward` channel (no `kubectl` child). Verified by an HTTP
    /// request through the forwarded port. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn k8s_integration_kube_rs_port_forward() {
        use std::io::{Read, Write};
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let local_port: u16 = 18098;
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-kpf
  labels:
    app: starling-kpf
spec:
  containers:
  - name: web
    image: nginx:1.27-alpine
    ports:
    - containerPort: 80
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");
        let _ = Command::new("kubectl")
            .args([
                "wait",
                "--for=condition=Ready",
                "pod/starling-kpf",
                "--timeout=90s",
            ])
            .output()
            .unwrap();

        let http_get = move || -> bool {
            match std::net::TcpStream::connect(("127.0.0.1", local_port)) {
                Ok(mut s) => {
                    let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
                    if s.write_all(b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n")
                        .is_err()
                    {
                        return false;
                    }
                    let mut buf = String::new();
                    let _ = s.read_to_string(&mut buf);
                    buf.contains("HTTP/1") && buf.to_lowercase().contains("nginx")
                }
                Err(_) => false,
            }
        };

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let spec = PortForwardSpec {
            host: "127.0.0.1".to_string(),
            local_port,
            container_port: 80,
            name: "web".to_string(),
            link_path: String::new(),
        };

        // SAFETY: single-threaded gated test; var removed before returning.
        unsafe { std::env::set_var("STARLING_KUBE_RS", "1") };
        let handle = stream_port_forward(
            "starling-kpf".to_string(),
            "starling-kpf".to_string(),
            spec,
            store.clone(),
        );
        let mut forwarded = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if http_get() {
                forwarded = true;
                break;
            }
        }
        handle.abort();
        unsafe { std::env::remove_var("STARLING_KUBE_RS") };
        apply("delete");

        assert!(
            forwarded,
            "native kube-rs port-forward did not serve HTTP through the local port"
        );
    }

    /// The discovery reconciler: deploy pods, then reconcile a
    /// `KubernetesDiscovery` object and verify it records the pod count/status.
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_discovery_reconciler() {
        use std::process::Command;
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let apply = |verb: &str, yaml: &str| {
            use std::io::Write;
            use std::process::Stdio;
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        let yaml = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: starling-disc
spec:
  replicas: 1
  selector:
    matchLabels:
      app: starling-disc
  template:
    metadata:
      labels:
        app: starling-disc
    spec:
      containers:
      - name: app
        image: registry.k8s.io/pause:3.9
"#;
        assert!(apply("apply", yaml), "deploy failed");

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "KubernetesDiscovery",
            "default",
            "starling-disc",
            serde_json::json!({ "spec": { "selectors": [{ "matchLabels": { "app": "starling-disc" } }] } }),
        )
        .unwrap();

        // Reconcile a few times until the pod is up.
        let mut total = 0u64;
        for _ in 0..30 {
            reconcile_kubernetes_discovery(&api, "starling-disc")
                .await
                .expect("discovery reconcile");
            let obj = api
                .get("KubernetesDiscovery", "default", "starling-disc")
                .unwrap();
            total = obj.object["status"]["totalPods"].as_u64().unwrap_or(0);
            if total >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        apply("delete", yaml);
        assert!(total >= 1, "discovery did not find the pod");
    }

    /// The **pod-watch** controller: deploy a labeled pod, reconcile pod-watch on
    /// a `KubernetesDiscovery` object, and verify `status.pods[]` carries the
    /// per-pod detail (name, phase, containers) — not just the aggregate count.
    /// Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_pod_watch_reconciler() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-pw
  labels:
    app: starling-pw
spec:
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "KubernetesDiscovery",
            "default",
            "starling-pw",
            serde_json::json!({ "spec": { "selectors": [{ "matchLabels": { "app": "starling-pw" } }] } }),
        )
        .unwrap();

        let mut record = serde_json::Value::Null;
        for _ in 0..30 {
            reconcile_pod_watch(&api, "starling-pw")
                .await
                .expect("pod-watch reconcile");
            let obj = api
                .get("KubernetesDiscovery", "default", "starling-pw")
                .unwrap();
            let pods = obj.object["status"]["pods"].as_array().cloned();
            if let Some(p) = pods {
                if let Some(first) = p.into_iter().next() {
                    record = first;
                    break;
                }
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        apply("delete");
        let name = record["name"].as_str().unwrap_or("");
        assert!(
            name.starts_with("starling-pw"),
            "pod-watch recorded no per-pod detail: {record}"
        );
        assert!(
            record["containers"]
                .as_array()
                .map(|c| !c.is_empty())
                .unwrap_or(false),
            "pod-watch record missing container detail: {record}"
        );
    }

    /// The **live-update** controller: deploy a busybox pod (it has a shell),
    /// create a `LiveUpdate` object that syncs a local file into the container
    /// and execs a command, reconcile it, and verify (a) the object's status is
    /// `failed: false` with the target pod resolved, and (b) the synced file is
    /// actually present in the container with the expected content. Gated
    /// (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_live_update_reconciler() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-lu
  labels:
    app: starling-lu
spec:
  containers:
  - name: app
    image: busybox:1.36
    command: ["sleep", "3600"]
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");
        // Wait for the pod to be Ready so cp/exec can run.
        let _ = Command::new("kubectl")
            .args([
                "wait",
                "--for=condition=Ready",
                "pod/starling-lu",
                "--timeout=60s",
            ])
            .output()
            .unwrap();

        // A local file to sync into the container.
        let local = std::env::temp_dir().join("starling-lu-sync.txt");
        std::fs::write(&local, "live-update-payload\n").unwrap();

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "LiveUpdate",
            "default",
            "starling-lu",
            serde_json::json!({
                "spec": {
                    "syncs": [{ "localPath": local.display().to_string(), "containerPath": "/tmp/synced.txt" }],
                    "execs": [{ "args": ["cp /tmp/synced.txt /tmp/done.txt"], "triggerPaths": [] }],
                    "selector": { "matchLabels": { "app": "starling-lu" } },
                }
            }),
        )
        .unwrap();

        reconcile_live_update(&api, "starling-lu")
            .await
            .expect("live-update reconcile");

        let obj = api.get("LiveUpdate", "default", "starling-lu").unwrap();
        let failed = obj.object["status"]["failed"].as_bool().unwrap_or(true);
        let pod = obj.object["status"]["podName"].as_str().unwrap_or("");

        // Independently verify the exec ran by reading the file it produced.
        let in_container = Command::new("kubectl")
            .args(["exec", "starling-lu", "--", "cat", "/tmp/done.txt"])
            .output()
            .unwrap();
        let content = String::from_utf8_lossy(&in_container.stdout).to_string();

        apply("delete");
        let _ = std::fs::remove_file(&local);

        assert!(
            !failed,
            "live-update reported failure: {:?}",
            obj.object["status"]
        );
        assert!(pod.starts_with("starling-lu"), "no target pod resolved");
        assert!(
            content.contains("live-update-payload"),
            "synced+exec'd file not found in container: {content:?}"
        );
    }

    /// The **kube-rs transport**: deploy a labeled pod, then list it through the
    /// in-process typed client (`kube_client::list_pods`) and through `kubectl`,
    /// and assert they agree on pod count and the resolved name — proving the
    /// kube-rs transport is a drop-in for the shell-out path. Also runs the
    /// discovery reconciler with `STARLING_KUBE_RS=1` and verifies it converges
    /// `totalPods` via the typed client. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_kube_rs_transport_parity() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-kube
  labels:
    app: starling-kube
spec:
  containers:
  - name: app
    image: registry.k8s.io/pause:3.9
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");

        // Poll the typed client until it observes the pod.
        let mut kube_pods = Vec::new();
        for _ in 0..30 {
            kube_pods = crate::kube_client::list_pods("app=starling-kube")
                .await
                .expect("kube-rs list_pods");
            if !kube_pods.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // The kubectl path, for parity comparison.
        let out = Command::new("kubectl")
            .args(["get", "pods", "-l", "app=starling-kube", "-o", "json"])
            .output()
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        let kubectl_pods = json["items"].as_array().cloned().unwrap_or_default();

        // Drive the discovery reconciler over the typed transport end-to-end.
        // SAFETY: single-threaded gated test; the var is unset before any await
        // that could let another test observe it, and ITs share one process.
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "KubernetesDiscovery",
            "default",
            "starling-kube",
            serde_json::json!({ "spec": { "selectors": [{ "matchLabels": { "app": "starling-kube" } }] } }),
        )
        .unwrap();
        unsafe { std::env::set_var("STARLING_KUBE_RS", "1") };
        let recon = reconcile_kubernetes_discovery(&api, "starling-kube").await;
        unsafe { std::env::remove_var("STARLING_KUBE_RS") };
        recon.expect("discovery via kube-rs");
        let total = api
            .get("KubernetesDiscovery", "default", "starling-kube")
            .unwrap()
            .object["status"]["totalPods"]
            .as_u64()
            .unwrap_or(0);

        apply("delete");

        assert_eq!(
            kube_pods.len(),
            kubectl_pods.len(),
            "kube-rs and kubectl disagree on pod count"
        );
        assert!(!kube_pods.is_empty(), "kube-rs found no pods");
        let kube_name = kube_pods[0]["metadata"]["name"].as_str().unwrap_or("");
        let kubectl_name = kubectl_pods[0]["metadata"]["name"].as_str().unwrap_or("");
        assert_eq!(
            kube_name, kubectl_name,
            "transports resolved different pods"
        );
        assert!(kube_name.starts_with("starling-kube"));
        assert!(
            total >= 1,
            "discovery via kube-rs did not converge totalPods"
        );
    }

    /// The **kube-rs exec/cp path**: run the live-update reconciler with
    /// `STARLING_KUBE_RS=1` so its sync and exec go through the typed attach API
    /// (`kube_client::copy_file` / `kube_client::exec`) rather than `kubectl
    /// cp`/`kubectl exec`, then independently read the result back out of the
    /// container with `kubectl`. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_kube_rs_exec_cp() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-kx
  labels:
    app: starling-kx
spec:
  containers:
  - name: app
    image: busybox:1.36
    command: ["sleep", "3600"]
"#;
        let apply = |verb: &str| {
            let mut child = Command::new("kubectl")
                .args([verb, "-f", "-"])
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .spawn()
                .unwrap();
            child
                .stdin
                .take()
                .unwrap()
                .write_all(yaml.as_bytes())
                .unwrap();
            child.wait().unwrap().success()
        };
        assert!(apply("apply"), "pod apply failed");
        let _ = Command::new("kubectl")
            .args([
                "wait",
                "--for=condition=Ready",
                "pod/starling-kx",
                "--timeout=60s",
            ])
            .output()
            .unwrap();

        let local = std::env::temp_dir().join("starling-kx-sync.txt");
        std::fs::write(&local, "kube-rs-attach-payload\n").unwrap();

        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "LiveUpdate",
            "default",
            "starling-kx",
            serde_json::json!({
                "spec": {
                    "syncs": [{ "localPath": local.display().to_string(), "containerPath": "/tmp/kx-synced.txt" }],
                    "execs": [{ "args": ["cp /tmp/kx-synced.txt /tmp/kx-done.txt"], "triggerPaths": [] }],
                    "selector": { "matchLabels": { "app": "starling-kx" } },
                }
            }),
        )
        .unwrap();

        // Drive the reconciler over the typed attach transport.
        // SAFETY: single-threaded gated test; var removed before returning.
        unsafe { std::env::set_var("STARLING_KUBE_RS", "1") };
        let recon = reconcile_live_update(&api, "starling-kx").await;
        unsafe { std::env::remove_var("STARLING_KUBE_RS") };

        let obj = api.get("LiveUpdate", "default", "starling-kx").unwrap();
        let failed = obj.object["status"]["failed"].as_bool().unwrap_or(true);

        // Independently confirm both the cp (synced file) and exec (copied file).
        let synced = Command::new("kubectl")
            .args(["exec", "starling-kx", "--", "cat", "/tmp/kx-synced.txt"])
            .output()
            .unwrap();
        let done = Command::new("kubectl")
            .args(["exec", "starling-kx", "--", "cat", "/tmp/kx-done.txt"])
            .output()
            .unwrap();
        let synced = String::from_utf8_lossy(&synced.stdout).to_string();
        let done = String::from_utf8_lossy(&done.stdout).to_string();

        apply("delete");
        let _ = std::fs::remove_file(&local);

        recon.expect("live-update via kube-rs attach");
        assert!(!failed, "kube-rs live-update reported failure");
        assert!(
            synced.contains("kube-rs-attach-payload"),
            "kube-rs copy_file did not land the synced file: {synced:?}"
        );
        assert!(
            done.contains("kube-rs-attach-payload"),
            "kube-rs exec did not run the copy command: {done:?}"
        );
    }

    /// The **kube-rs apply + log path**: with `STARLING_KUBE_RS=1`, drive the
    /// apply reconciler (server-side apply via the dynamic client) to create a
    /// pod, then the pod-log reconciler (typed `Api::logs`) to pull its logs into
    /// the store. Independently confirm the pod exists via `kubectl`. This
    /// exercises the last two reconciler paths over the typed transport. Gated
    /// (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_kube_rs_apply_and_logs() {
        use std::process::Command;
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        // A pod that prints a known line, so the log path has something to read.
        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-ka
  labels:
    app: starling-ka
spec:
  restartPolicy: Never
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "echo kube-rs-apply-log-marker; sleep 3600"]
"#;

        // Ensure no leftover pod from a previous run — wait for full deletion so
        // the apply below genuinely creates a fresh pod (not finds a Terminating
        // one, which would let the test pass without exercising apply→ready→logs).
        let _ = Command::new("kubectl")
            .args([
                "delete",
                "pod",
                "starling-ka",
                "--ignore-not-found",
                "--wait=true",
                "--timeout=60s",
            ])
            .output()
            .unwrap();

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        api.create(
            "KubernetesApply",
            "default",
            "starling-ka",
            serde_json::json!({ "spec": { "yaml": yaml } }),
        )
        .unwrap();
        api.create(
            "PodLogStream",
            "default",
            "starling-ka",
            serde_json::json!({ "spec": { "selector": { "matchLabels": { "app": "starling-ka" } } } }),
        )
        .unwrap();

        // SAFETY: single-threaded gated test; var removed before returning.
        unsafe { std::env::set_var("STARLING_KUBE_RS", "1") };
        let apply = reconcile_kubernetes_apply(&api, "starling-ka").await;
        // Wait for the pod to come up and log, then pull logs via the typed path.
        let mut log_lines = 0u64;
        let mut log_err = None;
        if apply.is_ok() {
            let _ = Command::new("kubectl")
                .args([
                    "wait",
                    "--for=condition=Ready",
                    "pod/starling-ka",
                    "--timeout=60s",
                ])
                .output();
            for _ in 0..15 {
                match reconcile_pod_log_stream(&api, &store, "starling-ka").await {
                    Ok(()) => {}
                    Err(e) => log_err = Some(e),
                }
                log_lines = api
                    .get("PodLogStream", "default", "starling-ka")
                    .unwrap()
                    .object["status"]["lineCount"]
                    .as_u64()
                    .unwrap_or(0);
                if log_lines > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
        unsafe { std::env::remove_var("STARLING_KUBE_RS") };

        // Independently confirm the object was applied to the cluster.
        let got = Command::new("kubectl")
            .args(["get", "pod", "starling-ka", "-o", "name"])
            .output()
            .unwrap();
        let exists = String::from_utf8_lossy(&got.stdout).contains("starling-ka");
        let logged = store.query_logs(&Default::default());

        let _ = Command::new("kubectl")
            .args(["delete", "pod", "starling-ka", "--wait=false"])
            .output();

        apply.expect("apply via kube-rs");
        assert!(exists, "kube-rs apply did not create the pod");
        assert!(
            log_err.is_none(),
            "pod-log via kube-rs errored: {log_err:?}"
        );
        assert!(log_lines > 0, "pod-log via kube-rs recorded no lines");
        assert!(
            logged
                .iter()
                .any(|l| l.text.contains("kube-rs-apply-log-marker")),
            "expected log marker not captured via the typed log path"
        );
    }

    /// The **kube-rs follow log stream**: deploy a pod that emits a line every
    /// second, then follow it via the typed `Api::log_stream` path (`stream_pod_logs`
    /// under `STARLING_KUBE_RS=1`) and verify the streamed lines land in the store
    /// as they are produced — exercising the persistent `-f` stream over the typed
    /// client, not a one-shot fetch. Gated (`STARLING_K8S_IT=1` + `kind-*`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore]
    async fn k8s_integration_kube_rs_log_stream_follow() {
        use std::io::Write;
        use std::process::{Command, Stdio};
        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(ctx.trim().starts_with("kind-"), "non-kind: {}", ctx.trim());

        let yaml = r#"
apiVersion: v1
kind: Pod
metadata:
  name: starling-follow
spec:
  restartPolicy: Never
  containers:
  - name: app
    image: busybox:1.36
    command: ["sh", "-c", "i=0; while true; do echo kube-rs-follow-$i; i=$((i+1)); sleep 1; done"]
"#;
        // Guarantee a fresh pod (no Terminating leftover seeding old lines).
        let _ = Command::new("kubectl")
            .args([
                "delete",
                "pod",
                "starling-follow",
                "--ignore-not-found",
                "--wait=true",
                "--timeout=60s",
            ])
            .output()
            .unwrap();
        let mut child = Command::new("kubectl")
            .args(["apply", "-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(yaml.as_bytes())
            .unwrap();
        assert!(child.wait().unwrap().success(), "pod apply failed");
        let _ = Command::new("kubectl")
            .args([
                "wait",
                "--for=condition=Ready",
                "pod/starling-follow",
                "--timeout=60s",
            ])
            .output()
            .unwrap();

        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));

        // SAFETY: single-threaded gated test; var removed before returning.
        unsafe { std::env::set_var("STARLING_KUBE_RS", "1") };
        stream_pod_logs(
            "starling-follow".to_string(),
            "follow-span".to_string(),
            store.clone(),
        );

        // Wait for at least two distinct streamed lines (proves it's following,
        // not a one-shot read).
        let mut distinct = 0usize;
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let logs = store.query_logs(&Default::default());
            distinct = logs
                .iter()
                .filter(|l| l.text.contains("kube-rs-follow-"))
                .count();
            if distinct >= 2 {
                break;
            }
        }
        unsafe { std::env::remove_var("STARLING_KUBE_RS") };

        let _ = Command::new("kubectl")
            .args(["delete", "pod", "starling-follow", "--wait=false"])
            .output();

        assert!(
            distinct >= 2,
            "typed follow stream did not deliver streamed lines (got {distinct})"
        );
    }

    /// End-to-end: drive the real `Engine` Kubernetes deploy path against a kind
    /// cluster and verify the resource lands + status reaches "ok". Gated like
    /// the other integration test (`STARLING_K8S_IT=1` + `kind-*` context).
    #[tokio::test]
    #[ignore]
    async fn k8s_integration_engine_deploys_to_cluster() {
        use std::process::Command;

        if std::env::var("STARLING_K8S_IT").is_err() {
            eprintln!("skipping: set STARLING_K8S_IT=1 to run");
            return;
        }
        let ctx = String::from_utf8(
            Command::new("kubectl")
                .args(["config", "current-context"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        assert!(
            ctx.trim().starts_with("kind-"),
            "refusing to run against non-kind context: {}",
            ctx.trim()
        );

        let yaml = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: starling-e2e
spec:
  replicas: 1
  selector:
    matchLabels:
      app: starling-e2e
  template:
    metadata:
      labels:
        app: starling-e2e
    spec:
      containers:
      - name: app
        image: registry.k8s.io/pause:3.9
"#;
        let (build_tx, build_rx) = mpsc::unbounded_channel();
        let (_restart_tx, restart_rx) = mpsc::unbounded_channel();
        let (_args_tx, args_rx) = mpsc::unbounded_channel();
        let (_port_tx, port_rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(build_tx.clone()));
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        let mut m = Manifest::new("starling-e2e".to_string(), TargetKind::Kubernetes);
        m.k8s_apply_docs = vec![yaml.to_string()];
        m.pod_selector
            .insert("app".to_string(), "starling-e2e".to_string());
        let mut eng = Engine::new(
            store.clone(),
            vec![m],
            build_rx,
            build_tx.clone(),
            false,
            std::path::PathBuf::from("Tiltfile"),
            vec![],
            vec![],
            vec![],
            restart_rx,
            args_rx,
            port_rx,
            None,
            api.clone(),
            None,
        );
        eng.materialize_all();
        // Drive the real deploy (kubectl apply) through the engine.
        eng.run_build("starling-e2e", true, None).await;

        let on_cluster = Command::new("kubectl")
            .args(["get", "deployment", "starling-e2e", "-o", "name"])
            .output()
            .unwrap();
        let found = String::from_utf8_lossy(&on_cluster.stdout).contains("starling-e2e");

        let view = store.full_view();
        let status = view
            .ui_resources
            .iter()
            .find(|r| r.metadata.as_ref().map(|mm| mm.name.as_str()) == Some("starling-e2e"))
            .and_then(|r| r.status.as_ref())
            .and_then(|s| s.update_status.clone());

        // The KubernetesApply object also carries the apply status.
        let ka = api.get("KubernetesApply", "default", "starling-e2e");

        // Clean up before asserting.
        Command::new("kubectl")
            .args(["delete", "deployment", "starling-e2e", "--ignore-not-found"])
            .output()
            .ok();

        assert!(found, "deployment was not applied to the cluster");
        assert_eq!(status.as_deref(), Some("ok"), "engine update_status != ok");
        assert!(
            ka.map(|o| o.object["status"]["lastApplyTime"].is_string())
                .unwrap_or(false),
            "KubernetesApply object missing apply status"
        );
    }

    #[tokio::test]
    async fn reconcilers_drive_engine_on_object_events() {
        use crate::api::store::{ApiObjectStore, ObjectEvent};
        use crate::api::v1alpha1::{ObjectMeta, UIResource, UIResourceStatus};

        let (tx, mut rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        store.upsert_resource(UIResource {
            metadata: Some(ObjectMeta {
                name: "web".to_string(),
                ..Default::default()
            }),
            spec: None,
            status: Some(UIResourceStatus::default()),
        });

        let recs = default_reconcilers();
        assert_eq!(recs.len(), 3);
        let api = ApiObjectStore::new();

        // Force-trigger annotation -> a build is enqueued for "web".
        let triggered = api
            .create(
                "Cmd",
                "default",
                "web",
                serde_json::json!({"metadata": {"annotations": {"tilt.dev/force-trigger": "t1"}}}),
            )
            .unwrap();
        recs.dispatch(&ObjectEvent::Added(triggered), &store);
        let req = rx.try_recv().expect("a build was enqueued");
        assert_eq!(req.name(), "web");

        // Disable-source ConfigMap -> the resource becomes disabled.
        let cm = api
            .create(
                "ConfigMap",
                "default",
                "web-disable",
                serde_json::json!({"data": {"isDisabled": "true"}}),
            )
            .unwrap();
        recs.dispatch(&ObjectEvent::Added(cm), &store);
        assert!(store.is_resource_disabled("web"));
    }

    #[tokio::test]
    async fn trigger_queue_enqueues_builds_with_nonce_dedupe() {
        use crate::api::store::{ApiObjectStore, ObjectEvent};
        use crate::api::v1alpha1::{ObjectMeta, UIResource, UIResourceStatus};

        let (tx, mut rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx));
        store.upsert_resource(UIResource {
            metadata: Some(ObjectMeta {
                name: "web".to_string(),
                ..Default::default()
            }),
            spec: None,
            status: Some(UIResourceStatus::default()),
        });
        let recs = default_reconcilers();
        let api = ApiObjectStore::new();

        // Writing the resource into spec.queue with a nonce triggers one build.
        let q = api
            .create(
                "TriggerQueue",
                "default",
                "queue",
                serde_json::json!({"spec": {"queue": [{"name": "web", "nonce": "n1"}]}}),
            )
            .unwrap();
        recs.dispatch(&ObjectEvent::Added(q.clone()), &store);
        assert_eq!(rx.try_recv().unwrap().name(), "web");

        // Re-emitting the same object (e.g. an engine status.queue write) does
        // not re-fire — the nonce is already seen.
        recs.dispatch(&ObjectEvent::Modified(q), &store);
        assert!(rx.try_recv().is_err(), "same nonce should not re-trigger");

        // A new nonce re-triggers.
        let q2 = api
            .replace(
                "TriggerQueue",
                "default",
                "queue",
                serde_json::json!({"spec": {"queue": [{"name": "web", "nonce": "n2"}]}}),
            )
            .unwrap();
        recs.dispatch(&ObjectEvent::Modified(q2), &store);
        assert_eq!(rx.try_recv().unwrap().name(), "web");
    }

    #[test]
    fn pod_log_selector_joins_match_labels() {
        let obj = serde_json::json!({
            "spec": {"selector": {"matchLabels": {"app": "web"}}}
        });
        assert_eq!(pod_log_selector(&obj), "app=web");
        // Missing selector -> empty (the controller skips it).
        assert_eq!(pod_log_selector(&serde_json::json!({"spec": {}})), "");
    }

    #[test]
    fn content_addressed_ref_derives_immutable_tag_from_digest() {
        assert_eq!(
            immutable_image_tag("sha256:0123456789abcdef0000"),
            "starling-0123456789ab"
        );
        // Short / unprefixed digests are handled without panicking.
        assert_eq!(immutable_image_tag("abcd"), "starling-abcd");
        assert_eq!(
            content_addressed_ref("registry.local/web:dev", "sha256:deadbeefcafe1234"),
            "registry.local/web:starling-deadbeefcafe"
        );
        // A digest already pinned in a ref is extracted; a plain tag is not.
        assert_eq!(
            digest_from_ref("web@sha256:abc"),
            Some("sha256:abc".to_string())
        );
        assert_eq!(digest_from_ref("web:tag"), None);
    }

    #[test]
    fn injects_resolved_image_map_refs_into_workload_yaml() {
        let api = Arc::new(crate::api::store::ApiObjectStore::new());
        // The apply object references one ImageMap by name (as materialize sets it).
        api.apply(
            "KubernetesApply",
            "default",
            "web",
            serde_json::json!({"spec": {"imageMaps": ["example/web"]}}),
        );
        api.apply(
            "ImageMap",
            "default",
            "example/web",
            serde_json::json!({"spec": {"selector": "example/web"}}),
        );
        // No resolved image yet -> nothing to inject (a not-yet-built image).
        assert!(resolve_image_maps_for(&api, "web").is_empty());

        // A completed build records the immutable deploy ref on status.image.
        api.patch(
            "ImageMap",
            "default",
            "example/web",
            serde_json::json!({"status": {"image": "example/web:starling-abc123def456"}}),
        )
        .unwrap();
        let maps = resolve_image_maps_for(&api, "web");
        assert_eq!(
            maps,
            vec![(
                "example/web".to_string(),
                "example/web:starling-abc123def456".to_string()
            )]
        );

        let yaml = "apiVersion: apps/v1\nkind: Deployment\nspec:\n  template:\n    spec:\n      containers:\n      - name: web\n        image: example/web\n---\nkind: Service";
        let out = inject_image_maps(yaml, &maps);
        assert!(
            out.contains("image: example/web:starling-abc123def456"),
            "image not injected: {out}"
        );
        assert!(out.contains("kind: Service"), "second doc dropped: {out}");
        // Empty maps leave the YAML byte-for-byte untouched.
        assert_eq!(inject_image_maps(yaml, &[]), yaml);
    }

    #[test]
    fn file_watch_object_round_trips_watch_spec() {
        let mut m = Manifest::new("web", TargetKind::Local);
        m.deps = vec![PathBuf::from("/tmp/src")];
        m.trigger_mode = 2; // Manual: file changes mark pending, don't build.
        m.ignore_rules = vec![IgnoreRule {
            base: PathBuf::from("/tmp"),
            pattern: "*.tmp".to_string(),
        }];
        let obj = file_watch_object(&m);
        let spec = &obj["spec"];
        assert_eq!(spec["watchedPaths"], serde_json::json!(["/tmp/src"]));
        assert_eq!(spec["manual"], serde_json::json!(true));
        let rules = parse_ignore_rules(&spec["ignores"]);
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].pattern, "*.tmp");
        assert_eq!(rules[0].base, PathBuf::from("/tmp"));
    }

    #[test]
    fn file_watch_controller_tracks_and_replaces_handles() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let store = Arc::new(Store::new(tx.clone()));
        let mut handles: HashMap<String, Arc<AtomicBool>> = HashMap::new();

        // Empty watchedPaths -> no watcher registered.
        let empty = serde_json::json!({"spec": {"watchedPaths": []}});
        start_file_watch("web", &empty, &tx, &store, &mut handles);
        assert!(!handles.contains_key("web"));

        // A real path registers a live (un-stopped) handle.
        let obj = serde_json::json!({"spec": {"watchedPaths": ["/nonexistent-starling-it"]}});
        start_file_watch("web", &obj, &tx, &store, &mut handles);
        let first = handles.get("web").cloned().unwrap();
        assert!(!first.load(Ordering::Relaxed));

        // Re-starting (e.g. on reload / object Modified) signals the old watcher
        // to stop and installs a fresh handle.
        start_file_watch("web", &obj, &tx, &store, &mut handles);
        assert!(
            first.load(Ordering::Relaxed),
            "previous watcher should be signalled to stop"
        );
        let second = handles.get("web").cloned().unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(!second.load(Ordering::Relaxed));
    }

    #[test]
    fn docker_build_args_pass_through_buildkit_options() {
        let db = crate::starlingfile::DockerBuild {
            ssh: vec!["default".to_string()],
            secrets: vec!["id=sek,src=/tmp/s".to_string()],
            cache_from: vec!["example/web:cache".to_string()],
            platform: Some("linux/amd64".to_string()),
            build_args: vec![("K".to_string(), "v".to_string())],
            extra_tags: vec!["example/web:extra".to_string()],
            ..plain_docker_build("example/web")
        };
        let args = docker_build_args(&db, None);
        let joined = args.join(" ");
        assert!(args.starts_with(&[
            "build".to_string(),
            "-t".to_string(),
            "example/web".to_string()
        ]));
        assert!(joined.contains("--ssh default"), "ssh missing: {joined}");
        assert!(
            joined.contains("--secret id=sek,src=/tmp/s"),
            "secret missing: {joined}"
        );
        assert!(
            joined.contains("--cache-from example/web:cache"),
            "cache missing: {joined}"
        );
        assert!(joined.contains("--platform linux/amd64"));
        assert!(joined.contains("--build-arg K=v"));
        assert!(joined.contains("-t example/web:extra"));
        // context is the final arg.
        assert_eq!(args.last().unwrap(), ".");
    }

    #[test]
    fn aggregate_pod_status_across_multiple_pods() {
        let ready_pod = serde_json::json!({
            "metadata": {"name": "p1"},
            "status": {"phase": "Running", "containerStatuses": [{"ready": true, "restartCount": 1}]}
        });
        let pending_pod = serde_json::json!({
            "metadata": {"name": "p2"},
            "status": {"phase": "Running", "containerStatuses": [{"ready": false, "restartCount": 0}]}
        });
        let failed_pod = serde_json::json!({
            "metadata": {"name": "p3"},
            "status": {"phase": "Failed", "containerStatuses": []}
        });

        // All ready -> ok, restarts summed.
        let s = aggregate_pod_status(&[ready_pod.clone(), ready_pod.clone()], false);
        assert_eq!(s.runtime, "ok");
        assert_eq!(s.ready, 2);
        assert_eq!(s.total, 2);
        assert_eq!(s.restarts, 2);
        assert_eq!(s.status_label(), "2/2 ready");

        // One not ready -> pending; readiness-ignored -> ok.
        assert_eq!(
            aggregate_pod_status(&[ready_pod.clone(), pending_pod.clone()], false).runtime,
            "pending"
        );
        assert_eq!(
            aggregate_pod_status(&[ready_pod.clone(), pending_pod], true).runtime,
            "ok"
        );

        // Any failed -> error (even with another ready).
        assert_eq!(
            aggregate_pod_status(&[ready_pod, failed_pod], false).runtime,
            "error"
        );

        // No pods -> pending.
        assert_eq!(aggregate_pod_status(&[], false).runtime, "pending");
    }

    #[test]
    fn builds_kubernetes_apply_and_tiltfile_api_objects() {
        // Plain k8s manifest -> KubernetesApply with spec.yaml + image refs.
        let mut k8s = Manifest {
            k8s_apply_docs: vec!["kind: Deployment".to_string()],
            ..Manifest::new("web".to_string(), TargetKind::Kubernetes)
        };
        k8s.docker_builds.push(crate::starlingfile::DockerBuild {
            image_ref: "gcr.io/web".to_string(),
            context: std::path::PathBuf::from("."),
            dockerfile: None,
            dockerfile_contents: None,
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![],
            only: vec![],
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: vec![],
        });
        let obj = kubernetes_apply_object(&k8s);
        assert_eq!(obj["spec"]["yaml"], serde_json::json!("kind: Deployment"));
        assert_eq!(obj["spec"]["imageMaps"], serde_json::json!(["gcr.io/web"]));

        // Custom-deploy manifest -> KubernetesApply with spec.applyCmd.
        let custom = Manifest {
            k8s_custom_apply_cmd: Some(Cmd {
                argv: vec!["helm".into(), "install".into()],
                ..Cmd::default()
            }),
            ..Manifest::new("chart".to_string(), TargetKind::Kubernetes)
        };
        let obj = kubernetes_apply_object(&custom);
        assert_eq!(
            obj["spec"]["applyCmd"]["args"],
            serde_json::json!(["helm", "install"])
        );
        assert!(obj["spec"].get("yaml").is_none());

        // With-status variant carries lastApplyTime + error.
        let okobj = kubernetes_apply_object_with_status(&k8s, "2026-01-01T00:00:00Z", None);
        assert_eq!(
            okobj["status"]["lastApplyTime"],
            serde_json::json!("2026-01-01T00:00:00Z")
        );
        assert_eq!(okobj["status"]["error"], serde_json::json!(""));
        let errobj = kubernetes_apply_object_with_status(&k8s, "t", Some("boom"));
        assert_eq!(errobj["status"]["error"], serde_json::json!("boom"));

        // Tiltfile object carries the config path and produced resource names.
        let tf = tiltfile_object(Path::new("/proj/Tiltfile"), &["web".to_string()]);
        assert_eq!(tf["spec"]["path"], serde_json::json!("/proj/Tiltfile"));
        assert_eq!(tf["status"]["resourceNames"], serde_json::json!(["web"]));

        // FileWatch object carries watched paths.
        let watched = Manifest {
            deps: vec![PathBuf::from("/repo/src")],
            ..Manifest::new("api".to_string(), TargetKind::Local)
        };
        let fw = file_watch_object(&watched);
        assert_eq!(fw["spec"]["watchedPaths"], serde_json::json!(["/repo/src"]));

        // Cmd object carries the update command's args + env.
        let local = Manifest {
            update_cmd: Cmd {
                argv: vec!["make".into(), "build".into()],
                env: vec![("K".into(), "v".into())],
                ..Cmd::default()
            },
            ..Manifest::new("job".to_string(), TargetKind::Local)
        };
        let cmd = cmd_object(&local);
        assert_eq!(cmd["spec"]["args"], serde_json::json!(["make", "build"]));
        assert_eq!(cmd["spec"]["env"], serde_json::json!(["K=v"]));

        // PortForward object carries the declared forwards.
        let pf = Manifest {
            k8s_port_forwards: vec![crate::starlingfile::PortForwardSpec {
                host: "127.0.0.1".into(),
                local_port: 8080,
                container_port: 80,
                name: "ui".into(),
                link_path: "/health".into(),
            }],
            ..Manifest::new("web".to_string(), TargetKind::Kubernetes)
        };
        let obj = port_forward_object(&pf);
        assert_eq!(
            obj["spec"]["forwards"][0]["localPort"],
            serde_json::json!(8080)
        );
        assert_eq!(
            obj["spec"]["forwards"][0]["containerPort"],
            serde_json::json!(80)
        );

        // LiveUpdate object maps sync/run/fall_back_on/restart steps.
        use crate::starlingfile::LiveUpdateStep;
        let lu = Manifest {
            live_update: vec![
                LiveUpdateStep::Sync {
                    local: "src".into(),
                    remote: "/app".into(),
                },
                LiveUpdateStep::Run {
                    cmd: "make".into(),
                    echo_off: false,
                    triggers: vec!["src/x".into()],
                },
                LiveUpdateStep::FallBackOn(vec!["Dockerfile".into()]),
                LiveUpdateStep::RestartContainer,
            ],
            ..Manifest::new("web".to_string(), TargetKind::Kubernetes)
        };
        let obj = live_update_object(&lu);
        assert_eq!(
            obj["spec"]["syncs"][0]["localPath"],
            serde_json::json!("src")
        );
        assert_eq!(obj["spec"]["execs"][0]["args"], serde_json::json!(["make"]));
        assert_eq!(obj["spec"]["stopPaths"], serde_json::json!(["Dockerfile"]));
        assert_eq!(obj["spec"]["restart"], serde_json::json!(true));

        // ImageMap selector + CmdImage args for a custom build.
        let custom_build = crate::starlingfile::DockerBuild {
            command: Some(Cmd {
                argv: vec!["./build.sh".into()],
                ..Cmd::default()
            }),
            ..plain_docker_build("gcr.io/custom")
        };
        assert_eq!(
            image_map_object("gcr.io/web")["spec"]["selector"],
            serde_json::json!("gcr.io/web")
        );
        let ci = cmd_image_object(&custom_build);
        assert_eq!(ci["spec"]["args"], serde_json::json!(["./build.sh"]));
        assert_eq!(ci["spec"]["ref"], serde_json::json!("gcr.io/custom"));

        // KubernetesDiscovery carries the pod selector match labels.
        let mut kd_manifest = Manifest::new("web".to_string(), TargetKind::Kubernetes);
        kd_manifest
            .pod_selector
            .insert("app".to_string(), "web".to_string());
        let kd = kubernetes_discovery_object(&kd_manifest);
        assert_eq!(
            kd["spec"]["selectors"][0]["matchLabels"]["app"],
            serde_json::json!("web")
        );

        // Session records the target set.
        let session = session_object(&["web".to_string(), "api".to_string()]);
        assert_eq!(
            session["status"]["targets"],
            serde_json::json!(["web", "api"])
        );

        // DockerComposeService/LogStream carry the service + project.
        let mut dc = Manifest::new("frontend".to_string(), TargetKind::DockerCompose);
        dc.docker_compose_project = Some("hello".to_string());
        assert_eq!(
            dc_service_object(&dc)["spec"]["service"],
            serde_json::json!("frontend")
        );
        assert_eq!(
            dc_service_object(&dc)["spec"]["project"]["name"],
            serde_json::json!("hello")
        );
        assert_eq!(
            dc_log_stream_object(&dc)["spec"]["service"],
            serde_json::json!("frontend")
        );

        // PodLogStream carries the pod selector.
        let pls = pod_log_stream_object(&kd_manifest);
        assert_eq!(
            pls["spec"]["selector"]["matchLabels"]["app"],
            serde_json::json!("web")
        );

        // ToggleButton references the resource it toggles.
        let tb = toggle_button_object(&Manifest::new("job".to_string(), TargetKind::Local));
        assert_eq!(
            tb["spec"]["location"]["componentID"],
            serde_json::json!("job")
        );

        // Disable-source ConfigMap reflects the disable state.
        assert_eq!(
            disable_config_map_object(true)["data"]["isDisabled"],
            serde_json::json!("true")
        );
        assert_eq!(
            disable_config_map_object(false)["data"]["isDisabled"],
            serde_json::json!("false")
        );
    }

    /// A `DockerBuild` with all the optional fields defaulted, for tests.
    fn plain_docker_build(image_ref: &str) -> crate::starlingfile::DockerBuild {
        crate::starlingfile::DockerBuild {
            image_ref: image_ref.to_string(),
            context: std::path::PathBuf::from("."),
            dockerfile: None,
            dockerfile_contents: None,
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![],
            only: vec![],
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: vec![],
        }
    }

    #[test]
    fn force_trigger_target_reads_annotation() {
        use crate::api::store::ApiObjectStore;
        let store = ApiObjectStore::new();
        // No annotation -> no trigger.
        let added = store
            .create("Cmd", "default", "web", serde_json::json!({}))
            .unwrap();
        assert_eq!(
            force_trigger_target(&crate::api::store::ObjectEvent::Added(added)),
            None
        );
        // Annotation present -> trigger that resource.
        let patched = store
            .patch(
                "Cmd",
                "default",
                "web",
                serde_json::json!({"metadata": {"annotations": {"tilt.dev/force-trigger": "t1"}}}),
            )
            .unwrap();
        assert_eq!(
            force_trigger_target(&crate::api::store::ObjectEvent::Modified(patched.clone())),
            Some("web".to_string())
        );
        // Deletes never trigger, even with the annotation set.
        assert_eq!(
            force_trigger_target(&crate::api::store::ObjectEvent::Deleted(patched)),
            None
        );
    }

    #[test]
    fn disable_change_reads_config_map() {
        use crate::api::store::{ApiObjectStore, ObjectEvent};
        let store = ApiObjectStore::new();
        // A disable ConfigMap with isDisabled=true -> (resource, true).
        let cm = store
            .create(
                "ConfigMap",
                "default",
                "web-disable",
                serde_json::json!({"data": {"isDisabled": "true"}}),
            )
            .unwrap();
        assert_eq!(
            disable_change_from_event(&ObjectEvent::Added(cm)),
            Some(("web".to_string(), true))
        );
        // A non-ConfigMap object is ignored.
        let other = store
            .create("Cmd", "default", "web-disable", serde_json::json!({}))
            .unwrap();
        assert_eq!(disable_change_from_event(&ObjectEvent::Added(other)), None);
        // A ConfigMap not named *-disable is ignored.
        let plain = store
            .create(
                "ConfigMap",
                "default",
                "settings",
                serde_json::json!({"data": {"isDisabled": "true"}}),
            )
            .unwrap();
        assert_eq!(disable_change_from_event(&ObjectEvent::Added(plain)), None);
    }

    #[test]
    fn k8s_down_specs_selects_custom_delete_commands() {
        let with_delete = Manifest {
            k8s_custom_delete_cmd: Some(Cmd {
                argv: vec!["helm".into(), "uninstall".into(), "web".into()],
                ..Cmd::default()
            }),
            ..Manifest::new("web".to_string(), TargetKind::Kubernetes)
        };
        let plain_k8s = Manifest::new("api".to_string(), TargetKind::Kubernetes);
        let local = Manifest::new("job".to_string(), TargetKind::Local);

        let specs = k8s_down_specs(&[with_delete, plain_k8s, local]);
        // Only the resource with a delete_cmd is selected.
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].0, "web");
        assert_eq!(specs[0].1.display(), "helm uninstall web");
    }

    #[test]
    fn live_update_fallback_paths_match_changed_files() {
        let manifest = Manifest {
            live_update: vec![crate::starlingfile::LiveUpdateStep::FallBackOn(vec![
                "/repo/Dockerfile".to_string(),
                "/repo/config".to_string(),
            ])],
            ..Manifest::new("web".to_string(), TargetKind::Kubernetes)
        };
        let paths = live_update_fallback_paths(&manifest);
        assert!(matches_any_path(Path::new("/repo/Dockerfile"), &paths));
        assert!(matches_any_path(Path::new("/repo/config/dev.yaml"), &paths));
        assert!(!matches_any_path(Path::new("/repo/src/main.rs"), &paths));
    }

    #[test]
    fn live_update_run_triggers_match_changed_files() {
        let triggers = vec![
            "/repo/src/schema.sql".to_string(),
            "/repo/scripts".to_string(),
        ];
        assert!(live_update_run_matches_triggers(
            &triggers,
            Some(&[PathBuf::from("/repo/src/schema.sql")])
        ));
        assert!(live_update_run_matches_triggers(
            &triggers,
            Some(&[PathBuf::from("/repo/scripts/regen.sh")])
        ));
        assert!(!live_update_run_matches_triggers(
            &triggers,
            Some(&[PathBuf::from("/repo/src/main.rs")])
        ));
        assert!(live_update_run_matches_triggers(&triggers, None));
        assert!(live_update_run_matches_triggers(
            &[],
            Some(&[PathBuf::from("/repo/src/main.rs")])
        ));
    }

    #[test]
    fn initial_sync_builds_sync_and_run_commands() {
        use crate::starlingfile::LiveUpdateStep;

        let steps = vec![
            LiveUpdateStep::InitialSync,
            LiveUpdateStep::Sync {
                local: "/repo/src".to_string(),
                remote: "/app/src".to_string(),
            },
            LiveUpdateStep::Run {
                cmd: "npm install".to_string(),
                echo_off: false,
                triggers: vec![],
            },
        ];
        assert!(live_update_has_initial_sync(&steps));

        let (sync_cmd, _) = initial_sync_command(&steps[1], "pod-123").expect("sync command");
        assert_eq!(
            sync_cmd.argv,
            vec!["kubectl", "cp", "/repo/src", "pod-123:/app/src"]
        );

        let (run_cmd, _) = initial_sync_command(&steps[2], "pod-123").expect("run command");
        assert_eq!(
            run_cmd.argv,
            vec![
                "kubectl",
                "exec",
                "pod-123",
                "--",
                "sh",
                "-c",
                "npm install"
            ]
        );
    }

    #[test]
    fn port_forward_args_target_pod_and_ports() {
        let spec = PortForwardSpec {
            host: "127.0.0.1".to_string(),
            local_port: 8080,
            container_port: 3000,
            name: "web".to_string(),
            link_path: String::new(),
        };
        assert_eq!(
            port_forward_args("web-pod", &spec),
            vec![
                "port-forward",
                "pod/web-pod",
                "8080:3000",
                "--address",
                "127.0.0.1"
            ]
        );
    }

    #[test]
    fn matches_watch_ignore_rules() {
        let base =
            std::env::temp_dir().join(format!("starling-ignore-test-{}", uuid::Uuid::new_v4()));
        let rules = vec![
            IgnoreRule {
                base: base.clone(),
                pattern: "tmp/".to_string(),
            },
            IgnoreRule {
                base: base.clone(),
                pattern: "*.log".to_string(),
            },
            IgnoreRule {
                base: base.clone(),
                pattern: "generated/**".to_string(),
            },
            IgnoreRule {
                base: base.clone(),
                pattern: "!generated/keep.log".to_string(),
            },
        ];

        assert!(is_ignored_by_rules(&base.join("tmp/cache.txt"), &rules));
        assert!(is_ignored_by_rules(&base.join("app/server.log"), &rules));
        assert!(is_ignored_by_rules(
            &base.join("generated/deep/out.txt"),
            &rules
        ));
        assert!(!is_ignored_by_rules(
            &base.join("generated/keep.log"),
            &rules
        ));
        assert!(!is_ignored_by_rules(&base.join("src/main.rs"), &rules));
    }

    #[test]
    fn docker_build_context_honors_ignore_and_only_rules() {
        let base =
            std::env::temp_dir().join(format!("starling-dockerignore-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(base.join("src")).unwrap();
        std::fs::create_dir_all(base.join("tmp")).unwrap();
        std::fs::write(base.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(base.join("src/main.rs"), "fn main() {}\n").unwrap();
        std::fs::write(base.join("src/debug.log"), "debug\n").unwrap();
        std::fs::write(base.join("tmp/cache.txt"), "cache\n").unwrap();
        std::fs::write(base.join("README.md"), "readme\n").unwrap();
        std::fs::write(base.join(".dockerignore"), "tmp/\n*.log\n").unwrap();

        let db = crate::starlingfile::DockerBuild {
            image_ref: "example".to_string(),
            context: base.clone(),
            dockerfile: None,
            dockerfile_contents: None,
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![IgnoreRule {
                base: base.clone(),
                pattern: "README.md".to_string(),
            }],
            only: vec![PathBuf::from("src")],
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: vec![],
        };

        let tar = build_context_tar(&db).unwrap();
        let mut archive = tar::Archive::new(std::io::Cursor::new(tar));
        let mut entries = archive
            .entries()
            .unwrap()
            .map(|entry| {
                entry
                    .unwrap()
                    .path()
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect::<Vec<_>>();
        entries.sort();

        assert!(entries.contains(&"Dockerfile".to_string()));
        assert!(entries.contains(&"src/main.rs".to_string()));
        assert!(!entries.contains(&"src/debug.log".to_string()));
        assert!(!entries.contains(&"tmp/cache.txt".to_string()));
        assert!(!entries.contains(&"README.md".to_string()));

        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn dockerfile_contents_are_injected_into_build_context() {
        let base =
            std::env::temp_dir().join(format!("starling-dockerfile-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&base).unwrap();
        std::fs::write(base.join("app.txt"), "app\n").unwrap();

        let db = crate::starlingfile::DockerBuild {
            image_ref: "example".to_string(),
            context: base.clone(),
            dockerfile: None,
            dockerfile_contents: Some("FROM scratch\nCOPY app.txt /app.txt\n".to_string()),
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![],
            only: vec![PathBuf::from("app.txt")],
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: vec![],
        };

        assert_eq!(dockerfile_name_for_build(&db), GENERATED_DOCKERFILE);
        let tar = build_context_tar(&db).unwrap();
        let mut archive = tar::Archive::new(std::io::Cursor::new(tar));
        let mut entries = archive
            .entries()
            .unwrap()
            .map(|entry| {
                let mut entry = entry.unwrap();
                let path = entry.path().unwrap().to_string_lossy().replace('\\', "/");
                let mut contents = String::new();
                if entry.header().entry_type().is_file() {
                    use std::io::Read;
                    entry.read_to_string(&mut contents).unwrap();
                }
                (path, contents)
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        assert!(entries.iter().any(|(path, _)| path == "app.txt"));
        assert!(entries.iter().any(|(path, contents)| {
            path == GENERATED_DOCKERFILE && contents.contains("COPY app.txt /app.txt")
        }));

        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn docker_build_options_apply_target_and_platform() {
        let db = crate::starlingfile::DockerBuild {
            image_ref: "example:dev".to_string(),
            context: PathBuf::from("."),
            dockerfile: None,
            dockerfile_contents: None,
            target: Some("runtime".to_string()),
            platform: Some("linux/amd64".to_string()),
            extra_tags: vec!["example:latest".to_string()],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![("MODE".to_string(), "dev".to_string())],
            cache_from: vec!["example:cache".to_string()],
            ssh: vec![],
            secrets: vec![],
            pull: true,
            network: Some("host".to_string()),
            extra_hosts: vec![
                "host.docker.internal:host-gateway".to_string(),
                "db:127.0.0.1".to_string(),
            ],
            ignore_rules: vec![],
            only: vec![],
            command: None,
            custom_tag: None,
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: false,
            deps: vec![],
            live_update: vec![],
        };

        let options = build_image_options(&db);
        assert_eq!(options.t, "example:dev");
        assert_eq!(options.target, "runtime");
        assert_eq!(options.platform, "linux/amd64");
        assert_eq!(options.buildargs.get("MODE"), Some(&"dev".to_string()));
        assert_eq!(options.cachefrom, vec!["example:cache".to_string()]);
        assert!(options.pull);
        assert_eq!(options.networkmode, "host");
        assert_eq!(
            options.extrahosts,
            Some("host.docker.internal:host-gateway,db:127.0.0.1".to_string())
        );
    }

    #[test]
    fn parses_image_ref_for_extra_tag() {
        assert_eq!(
            image_ref_repo_and_tag("example/web:dev"),
            ("example/web".to_string(), "dev".to_string())
        );
        assert_eq!(
            image_ref_repo_and_tag("localhost:5000/web:dev"),
            ("localhost:5000/web".to_string(), "dev".to_string())
        );
        assert_eq!(
            image_ref_repo_and_tag("localhost:5000/web"),
            ("localhost:5000/web".to_string(), "latest".to_string())
        );
    }

    #[test]
    fn custom_build_expected_ref_uses_tag() {
        let mut db = crate::starlingfile::DockerBuild {
            image_ref: "gcr.io/foo".to_string(),
            context: PathBuf::from("."),
            dockerfile: None,
            dockerfile_contents: None,
            target: None,
            platform: None,
            extra_tags: vec![],
            entrypoint: vec![],
            container_args: None,
            match_in_env_vars: false,
            build_args: vec![],
            cache_from: vec![],
            ssh: vec![],
            secrets: vec![],
            pull: false,
            network: None,
            extra_hosts: vec![],
            ignore_rules: vec![],
            only: vec![],
            command: Some(Cmd::default()),
            custom_tag: Some("dev".to_string()),
            outputs_image_ref_to: None,
            image_deps: vec![],
            disable_push: false,
            skips_local_docker: true,
            deps: vec![],
            live_update: vec![],
        };
        assert_eq!(custom_build_expected_ref(&db), "gcr.io/foo:dev");
        db.custom_tag = Some("example.com/foo:prod".to_string());
        assert_eq!(custom_build_expected_ref(&db), "example.com/foo:prod");
    }

    #[test]
    fn rewrites_shell_serve_port_flag() {
        let mut cmd = Cmd {
            argv: vec![
                "sh".to_string(),
                "-c".to_string(),
                "cd ui && npm run dev -- --host 127.0.0.1 --port 8090 --strictPort".to_string(),
            ],
            ..Default::default()
        };

        assert!(rewrite_serve_cmd_port_args(&mut cmd, 54756));
        assert_eq!(
            cmd.argv[2],
            "cd ui && npm run dev -- --host 127.0.0.1 --port 54756 --strictPort"
        );
    }

    #[test]
    fn rewrites_shell_serve_port_equals_flag() {
        let mut cmd = Cmd {
            argv: vec![
                "sh".to_string(),
                "-c".to_string(),
                "vite --host 127.0.0.1 --port=8090".to_string(),
            ],
            ..Default::default()
        };

        assert!(rewrite_serve_cmd_port_args(&mut cmd, 54756));
        assert_eq!(cmd.argv[2], "vite --host 127.0.0.1 --port=54756");
    }

    #[test]
    fn rewrites_argv_serve_port_flag() {
        let mut cmd = Cmd {
            argv: vec![
                "npm".to_string(),
                "run".to_string(),
                "dev".to_string(),
                "--".to_string(),
                "--port".to_string(),
                "8090".to_string(),
                "--strictPort".to_string(),
            ],
            ..Default::default()
        };

        assert!(rewrite_serve_cmd_port_args(&mut cmd, 54756));
        assert_eq!(cmd.argv[5], "54756");
    }

    #[test]
    fn does_not_rewrite_dynamic_port_args() {
        let mut cmd = Cmd {
            argv: vec![
                "sh".to_string(),
                "-c".to_string(),
                "vite --host 127.0.0.1 --port $PORT".to_string(),
            ],
            ..Default::default()
        };

        assert!(!rewrite_serve_cmd_port_args(&mut cmd, 54756));
        assert_eq!(cmd.argv[2], "vite --host 127.0.0.1 --port $PORT");
    }

    #[test]
    fn service_env_uses_stable_resource_keys() {
        let registry = Arc::new(Mutex::new(HashMap::from([(
            "control-plane-api".to_string(),
            ServiceEndpoint {
                host: Some("control-plane-api-paas.localhost".to_string()),
                port: Some(54756),
                url: Some("http://control-plane-api-paas.localhost:1360".to_string()),
            },
        )])));

        let env = service_env(&registry);

        assert!(env.contains(&(
            "STARLING_CONTROL_PLANE_API_HOST".to_string(),
            "control-plane-api-paas.localhost".to_string()
        )));
        assert!(env.contains(&(
            "STARLING_CONTROL_PLANE_API_PORT".to_string(),
            "54756".to_string()
        )));
        assert!(env.contains(&(
            "STARLING_CONTROL_PLANE_API_URL".to_string(),
            "http://control-plane-api-paas.localhost:1360".to_string()
        )));
        assert!(env
            .iter()
            .any(|(key, value)| key == "STARLING_SERVICES_JSON"
                && value.contains("control-plane-api")));
    }
}
