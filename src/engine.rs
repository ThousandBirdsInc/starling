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
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use notify::{Event, RecursiveMode, Watcher};
use regex::Regex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::api::v1alpha1::*;
use crate::starlingfile::{self, Cmd, Manifest, TargetKind};
use crate::store::Store;

pub struct Engine {
    store: Arc<Store>,
    manifests: Vec<Manifest>,
    /// Index for quick lookup by name.
    by_name: HashMap<String, usize>,
    /// Incoming build requests (from /api/trigger and file watchers).
    build_rx: mpsc::UnboundedReceiver<String>,
    /// Cloned for file watchers to enqueue rebuilds.
    build_tx: mpsc::UnboundedSender<String>,
    /// When true, `kubectl apply` runs with `--dry-run=client` and pod watching
    /// is skipped (nothing is mutated on the cluster).
    dry_run: bool,
    /// Path to the Starlingfile, re-executed on reload.
    config_path: PathBuf,
    /// All config files to watch (Starlingfile + includes + read_file paths).
    config_files: Vec<PathBuf>,
    /// Resources whose `serve_cmd` is already running (avoid duplicates on reload).
    started_serves: HashSet<String>,
    /// Number of times each serve_cmd has started during this engine session.
    serve_start_counts: HashMap<String, u32>,
    /// Abort handles for running serve_cmd tasks (used to restart/kill).
    serve_tasks: HashMap<String, ServeTask>,
    /// Incremented on config reload so stale file watcher threads stop
    /// triggering builds for old manifests.
    watcher_generation: Arc<AtomicU64>,
    /// Restart requests (resource names) from the dashboard.
    restart_rx: mpsc::UnboundedReceiver<String>,
    /// Preferred port changes from the dashboard.
    port_rx: mpsc::UnboundedReceiver<(String, u16)>,
    /// Session-scoped preferred port overrides from the dashboard.
    port_overrides: HashMap<String, u16>,
    /// Named-URL proxy handle (daemon or local); `None` disables named URLs.
    proxy: Option<crate::proxy::ProxyHandle>,
}

struct ServeTask {
    abort: tokio::task::AbortHandle,
    pid: Arc<Mutex<Option<u32>>>,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<Store>,
        manifests: Vec<Manifest>,
        build_rx: mpsc::UnboundedReceiver<String>,
        build_tx: mpsc::UnboundedSender<String>,
        dry_run: bool,
        config_path: PathBuf,
        config_files: Vec<PathBuf>,
        restart_rx: mpsc::UnboundedReceiver<String>,
        port_rx: mpsc::UnboundedReceiver<(String, u16)>,
        proxy: Option<crate::proxy::ProxyHandle>,
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
            config_files,
            started_serves: HashSet::new(),
            serve_start_counts: HashMap::new(),
            serve_tasks: HashMap::new(),
            watcher_generation: Arc::new(AtomicU64::new(0)),
            restart_rx,
            port_rx,
            port_overrides: HashMap::new(),
            proxy,
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
    }

    /// Run the engine until the build channel closes.
    pub async fn run(mut self) {
        self.materialize_all();
        self.start_watchers();
        for name in self.initial_build_order() {
            self.run_build(&name).await;
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
                    let Some(name) = maybe else { break };
                    // Coalesce a burst of requests.
                    let mut pending = vec![name];
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    while let Ok(extra) = self.build_rx.try_recv() {
                        if !pending.contains(&extra) {
                            pending.push(extra);
                        }
                    }
                    for name in pending {
                        self.run_build(&name).await;
                    }
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
        let result = match starlingfile::load(&self.config_path) {
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

        self.manifests = result.manifests;
        self.apply_port_overrides();
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
        self.materialize_all();
        self.start_watchers();
        self.store.append_log(
            Some(&span),
            "INFO",
            &format!("Config reloaded ({} resources)\n", self.manifests.len()),
        );
        for name in self.initial_build_order() {
            self.run_build(&name).await;
        }
    }

    fn apply_port_overrides(&mut self) {
        for m in &mut self.manifests {
            if let Some(port) = self.port_overrides.get(&m.name) {
                m.serve_port = Some(*port);
            }
        }
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

    /// Watch each resource's deps; on change, enqueue a rebuild.
    fn start_watchers(&self) {
        let generation = self.watcher_generation.fetch_add(1, Ordering::Relaxed) + 1;
        for m in &self.manifests {
            if m.deps.is_empty() {
                continue;
            }
            let name = m.name.clone();
            let tx = self.build_tx.clone();
            let store = self.store.clone();
            let deps = m.deps.clone();
            let watcher_generation = self.watcher_generation.clone();
            // Manual trigger modes only mark pending changes; they don't build.
            let auto_on_change = m.auto_on_change();
            // Each watcher runs on a blocking thread; notify delivers events to
            // a std channel we forward as build requests.
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
                    while raw_rx.recv_timeout(Duration::from_millis(200)).is_ok() {}
                    if watcher_generation.load(Ordering::Relaxed) != generation {
                        break;
                    }
                    store.append_log(Some(&name), "INFO", "Detected file change\n");
                    if auto_on_change {
                        if tx.send(name.clone()).is_err() {
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
    }

    /// Spawn a long-running serve command and reflect its runtime status.
    /// When the proxy is enabled, assigns a port via `PORT` and registers a
    /// named route (`<name>.<tld>`) so the resource gets a stable URL.
    fn spawn_serve(&mut self, mut m: Manifest) {
        let store = self.store.clone();
        let name = m.name.clone();
        let proxy = self.proxy.clone();
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
                    store.update_status(&name, |st| {
                        st.runtime_status = Some("ok".to_string());
                        let local = st.local_resource_info.get_or_insert_with(Default::default);
                        local.pid = pid.map(|pid| pid as i64);
                        local.is_test = Some(false);
                        local.restart_count = Some(restart_count as i32);
                        local.last_start_time = Some(last_start_time.clone());
                    });
                    let health_task = route_monitor
                        .as_ref()
                        .map(ServeRouteMonitor::start_health_checks);
                    let status = child.wait().await;
                    if let Some(task) = health_task {
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
    }

    async fn replace_serve_after_update(&mut self, name: &str, m: Manifest) {
        if m.serve_cmd.is_empty() {
            return;
        }
        let message = if self.started_serves.contains(name) {
            "Restarting serve_cmd after successful update...\n"
        } else {
            "Starting serve_cmd after successful update...\n"
        };
        self.stop_serve(name, message).await;
        self.started_serves.insert(name.to_string());
        self.spawn_serve(m);
    }

    /// Run a resource's update command as a one-shot build.
    async fn run_build(&mut self, name: &str) {
        let Some(&i) = self.by_name.get(name) else {
            return;
        };
        let m = self.manifests[i].clone();

        if m.kind == TargetKind::Kubernetes {
            self.run_k8s_build(name, &m).await;
            return;
        }

        let now = Utc::now().to_rfc3339();
        let span = format!("{name}:build");

        // Resources without an update command (e.g. k8s placeholders) are
        // marked up-to-date immediately.
        if m.update_cmd.is_empty() {
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
        self.store.append_log(
            Some(name),
            "INFO",
            &format!("Building: {}\n", m.update_cmd.display()),
        );

        let result = run_to_completion(&m.update_cmd, &self.store, name).await;
        let finish = Utc::now().to_rfc3339();
        let error = match result {
            Ok(true) => None,
            Ok(false) => Some("command exited non-zero".to_string()),
            Err(e) => Some(e),
        };
        let ok = error.is_none();
        if let Some(err) = &error {
            self.store
                .append_log(Some(name), "ERROR", &format!("Build failed: {err}\n"));
        } else {
            self.store
                .append_log(Some(name), "INFO", "Build succeeded\n");
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
    async fn run_k8s_build(&self, name: &str, m: &Manifest) {
        // Live-update fast path: if the resource is already deployed with a live
        // pod and has live_update steps, sync into the container instead of a
        // full rebuild + redeploy.
        if !m.live_update.is_empty() && self.store.build_count(name) > 0 && !self.dry_run {
            if let Some(pod) = self.store.current_pod(name) {
                self.live_update(name, m, &pod).await;
                return;
            }
        }

        let now = Utc::now().to_rfc3339();
        let span = format!("{name}:build");
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

        // 1. Build images via the native Docker API (bollard), then load them
        //    into a kind cluster if that's where we're deploying.
        for db in &m.docker_builds {
            self.store.append_log(
                Some(name),
                "INFO",
                &format!("Building image: {}\n", db.image_ref),
            );
            match build_image(db, &self.store, name).await {
                Ok(()) => {
                    if !self.dry_run {
                        kind_load(&db.image_ref, &self.store, name).await;
                    }
                }
                Err(e) => error = Some(e),
            }
            if error.is_some() {
                break;
            }
        }

        // 2. kubectl apply (unless an image build already failed).
        if error.is_none() && !m.k8s_apply_docs.is_empty() {
            let docs = m.k8s_apply_docs.join("\n---\n");
            let mut argv = vec![
                "kubectl".to_string(),
                "apply".to_string(),
                "-f".to_string(),
                "-".to_string(),
            ];
            if self.dry_run {
                // Client-side only: no API calls, nothing mutated. `--validate=false`
                // avoids the openapi fetch so it works fully offline.
                argv.push("--dry-run=client".to_string());
                argv.push("--validate=false".to_string());
            }
            self.store.append_log(
                Some(name),
                "INFO",
                &format!(
                    "kubectl apply{}\n",
                    if self.dry_run {
                        " (dry-run=client)"
                    } else {
                        ""
                    }
                ),
            );
            match run_with_stdin(&argv, &docs, &self.store, name).await {
                Ok(true) => {}
                Ok(false) => error = Some("kubectl apply failed".to_string()),
                Err(e) => error = Some(e),
            }
        }

        let finish = Utc::now().to_rfc3339();
        let ok = error.is_none();
        if let Some(err) = &error {
            self.store
                .append_log(Some(name), "ERROR", &format!("Deploy failed: {err}\n"));
        } else {
            self.store
                .append_log(Some(name), "INFO", "Deploy succeeded\n");
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

        // 3. Watch pods (only for a real deploy with a selector).
        if ok && !self.dry_run && !m.pod_selector.is_empty() {
            self.spawn_pod_watch(name.to_string(), m.pod_selector.clone());
        }
    }

    /// Perform a live update: `kubectl cp` each sync source into the pod and
    /// `kubectl exec` each run command, instead of a full rebuild + redeploy.
    async fn live_update(&self, name: &str, m: &Manifest, pod: &str) {
        use crate::starlingfile::LiveUpdateStep;
        let now = Utc::now().to_rfc3339();
        let span = format!("{name}:build");
        self.store.update_status(name, |st| {
            st.update_status = Some("in_progress".to_string());
            st.current_build = Some(UIBuildRunning {
                start_time: Some(now.clone()),
                span_id: Some(span.clone()),
            });
        });
        self.store
            .append_log(Some(name), "INFO", "Live update (no rebuild)\n");

        let mut error: Option<String> = None;
        for step in &m.live_update {
            let argv = match step {
                LiveUpdateStep::Sync { local, remote } => {
                    self.store.append_log(
                        Some(name),
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
                LiveUpdateStep::Run { cmd } => {
                    self.store
                        .append_log(Some(name), "INFO", &format!("  run {cmd}\n"));
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
                        .append_log(Some(name), "INFO", "  restart_container\n");
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
            match run_to_completion(&cmd, &self.store, name).await {
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
            Some(name),
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
    fn spawn_pod_watch(&self, name: String, selector: std::collections::BTreeMap<String, String>) {
        let store = self.store.clone();
        let sel = selector
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        tokio::spawn(async move {
            let mut streaming_pod: Option<String> = None;
            loop {
                let out = Command::new("kubectl")
                    .args(["get", "pods", "-l", &sel, "-o", "json"])
                    .output()
                    .await;
                let Ok(out) = out else {
                    break;
                };
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&out.stdout) {
                    if let Some(pod) = json["items"].as_array().and_then(|a| a.first()) {
                        let pod_name = pod["metadata"]["name"].as_str().unwrap_or("").to_string();
                        let phase = pod["status"]["phase"]
                            .as_str()
                            .unwrap_or("Unknown")
                            .to_string();
                        let restarts = pod["status"]["containerStatuses"]
                            .as_array()
                            .map(|cs| {
                                cs.iter()
                                    .map(|c| c["restartCount"].as_i64().unwrap_or(0))
                                    .sum::<i64>()
                            })
                            .unwrap_or(0);
                        let ready = pod["status"]["containerStatuses"]
                            .as_array()
                            .map(|cs| cs.iter().all(|c| c["ready"].as_bool().unwrap_or(false)))
                            .unwrap_or(false);
                        let runtime = match phase.as_str() {
                            "Running" if ready => "ok",
                            "Running" | "Pending" => "pending",
                            "Succeeded" => "ok",
                            "Failed" => "error",
                            _ => "pending",
                        };
                        store.update_status(&name, |st| {
                            st.runtime_status = Some(runtime.to_string());
                            st.k8s_resource_info = Some(UIResourceKubernetes {
                                pod_name: Some(pod_name.clone()),
                                pod_status: Some(phase.clone()),
                                all_containers_ready: Some(ready),
                                pod_restarts: Some(restarts as i32),
                                span_id: Some(format!("{name}:pod")),
                                ..Default::default()
                            });
                        });
                        // Start streaming logs once, for the first live pod.
                        if streaming_pod.as_deref() != Some(pod_name.as_str())
                            && !pod_name.is_empty()
                        {
                            streaming_pod = Some(pod_name.clone());
                            stream_pod_logs(pod_name, name.clone(), store.clone());
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
async fn build_image(
    db: &crate::starlingfile::DockerBuild,
    store: &Arc<Store>,
    span: &str,
) -> Result<(), String> {
    use bollard::image::BuildImageOptions;
    use futures::StreamExt;

    // custom_build: run the user's command (with EXPECTED_REF) instead of bollard.
    if let Some(command) = &db.command {
        let mut cmd = command.clone();
        cmd.env
            .push(("EXPECTED_REF".to_string(), db.image_ref.clone()));
        return match run_to_completion(&cmd, store, span).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(format!("custom_build {} command failed", db.image_ref)),
            Err(e) => Err(e),
        };
    }

    let docker = bollard::Docker::connect_with_local_defaults()
        .map_err(|e| format!("connecting to Docker daemon: {e}"))?;

    // Tar the build context (blocking work on a worker thread).
    let context = db.context.clone();
    let tar = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
        let mut builder = tar::Builder::new(Vec::new());
        builder.append_dir_all(".", &context)?;
        builder.into_inner()
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| format!("taring build context {}: {e}", db.context.display()))?;

    let dockerfile = db
        .dockerfile
        .as_ref()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("Dockerfile")
        .to_string();
    let options = BuildImageOptions {
        dockerfile,
        t: db.image_ref.clone(),
        rm: true,
        forcerm: true,
        buildargs: db.build_args.iter().cloned().collect(),
        ..Default::default()
    };
    // Note: bollard's classic builder doesn't expose `--target`; db.target is
    // accepted for Tiltfile compatibility but not applied here.

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
    Ok(())
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

/// Stream a pod's logs (`kubectl logs -f`) into the resource span.
fn stream_pod_logs(pod: String, span: String, store: Arc<Store>) {
    tokio::spawn(async move {
        let mut child = match Command::new("kubectl")
            .args(["logs", "-f", "--all-containers", "--tail", "20", &pod])
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
    });
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
            is_test: Some(false),
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
}
