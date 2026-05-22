//! In-memory engine state plus a change-notification channel.
//!
//! An in-memory, Kubernetes-style object store. The websocket reads a full
//! `View` on connect and incremental deltas thereafter. Resources are populated
//! by the engine from the Starlingfile; builds and logs reflect real subprocess
//! execution. `/api/trigger` enqueues a build on `build_tx`, which the engine
//! consumes.

use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc};

use crate::api::v1alpha1::*;
use crate::api::webview::{LogList, LogSegment, LogSpan, View};

/// The mutable engine state guarded by a single lock.
struct Inner {
    session: UISession,
    resources: Vec<UIResource>,
    buttons: Vec<UIButton>,
    clusters: Vec<Cluster>,
    /// Append-only log; index into this slice is the log "checkpoint".
    log_segments: Vec<LogSegment>,
    log_spans: BTreeMap<String, LogSpan>,
}

pub struct Store {
    inner: Mutex<Inner>,
    /// Bumped whenever state changes; websocket tasks wake and send a delta.
    notify: broadcast::Sender<()>,
    /// Build requests (resource names) sent to the engine.
    build_tx: mpsc::UnboundedSender<String>,
    /// Restart requests (resource names) sent to the engine.
    restart_tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
    start_time: String,
}

impl Store {
    /// Create a store seeded with environment info (session + cluster) but no
    /// resources; the engine adds those from the Starlingfile.
    pub fn new(build_tx: mpsc::UnboundedSender<String>) -> Self {
        let (notify, _) = broadcast::channel(64);
        let start_time = Utc::now().to_rfc3339();
        let (session, clusters) = crate::seed::env_seed(&start_time);
        let store = Store {
            inner: Mutex::new(Inner {
                session,
                resources: vec![],
                buttons: vec![],
                clusters,
                log_segments: vec![],
                log_spans: BTreeMap::new(),
            }),
            notify,
            build_tx,
            restart_tx: Mutex::new(None),
            start_time,
        };
        store.append_log(None, "INFO", "Starling started\n");
        store
    }

    /// Wire the channel the engine listens on for serve_cmd restart requests.
    pub fn set_restart_tx(&self, tx: mpsc::UnboundedSender<String>) {
        *self.restart_tx.lock().unwrap() = Some(tx);
    }

    /// Request a serve_cmd restart for `name` (no-op if it doesn't exist).
    pub fn restart(&self, name: &str) -> Result<(), TriggerError> {
        if !self.resource_exists(name) {
            return Err(TriggerError::NotFound);
        }
        if let Some(tx) = self.restart_tx.lock().unwrap().as_ref() {
            tx.send(name.to_string()).map_err(|_| TriggerError::EngineGone)?;
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify.subscribe()
    }

    fn notify(&self) {
        let _ = self.notify.send(());
    }

    pub fn log_len(&self) -> i32 {
        self.inner.lock().unwrap().log_segments.len() as i32
    }

    /// Last `tail` log lines per span (resource name), for the daemon dashboard.
    /// The empty global span is reported under "(system)".
    pub fn recent_logs_by_resource(&self, tail: usize) -> BTreeMap<String, Vec<String>> {
        let inner = self.inner.lock().unwrap();
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for seg in &inner.log_segments {
            let span = seg.span_id.clone().unwrap_or_default();
            let key = if span.is_empty() { "(system)".to_string() } else { span };
            let text = seg.text.clone().unwrap_or_default();
            out.entry(key).or_default().push(text.trim_end().to_string());
        }
        for lines in out.values_mut() {
            if lines.len() > tail {
                let start = lines.len() - tail;
                *lines = lines.split_off(start);
            }
        }
        out
    }

    // -- view assembly -----------------------------------------------------

    pub fn full_view(&self) -> View {
        let inner = self.inner.lock().unwrap();
        let to = inner.log_segments.len() as i32;
        View {
            tilt_start_time: Some(self.start_time.clone()),
            tiltfile_key: Some("Starlingfile".to_string()),
            ui_session: Some(inner.session.clone()),
            ui_resources: inner.resources.clone(),
            ui_buttons: inner.buttons.clone(),
            clusters: inner.clusters.clone(),
            log_list: Some(LogList {
                spans: Some(inner.log_spans.clone()),
                segments: inner.log_segments.clone(),
                from_checkpoint: Some(0),
                to_checkpoint: Some(to),
            }),
            is_complete: Some(true),
            ..Default::default()
        }
    }

    pub fn delta_view(&self, from: i32) -> (View, i32) {
        let inner = self.inner.lock().unwrap();
        let to = inner.log_segments.len() as i32;
        let from = from.clamp(0, to);
        let segments = inner.log_segments[from as usize..to as usize].to_vec();
        let view = View {
            ui_session: Some(inner.session.clone()),
            ui_resources: inner.resources.clone(),
            ui_buttons: inner.buttons.clone(),
            clusters: inner.clusters.clone(),
            log_list: Some(LogList {
                spans: Some(inner.log_spans.clone()),
                segments,
                from_checkpoint: Some(from),
                to_checkpoint: Some(to),
            }),
            is_complete: Some(false),
            ..Default::default()
        };
        (view, to)
    }

    // -- resource management (used by the engine) --------------------------

    pub fn upsert_resource(&self, resource: UIResource) {
        {
            let mut inner = self.inner.lock().unwrap();
            let name = resource
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default();
            if let Some(existing) = inner
                .resources
                .iter_mut()
                .find(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(name.as_str()))
            {
                *existing = resource;
            } else {
                inner.resources.push(resource);
            }
        }
        self.notify();
    }

    /// Mutate a resource's status in place and notify subscribers.
    pub fn update_status(&self, name: &str, f: impl FnOnce(&mut UIResourceStatus)) {
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(r) = inner
                .resources
                .iter_mut()
                .find(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(name))
            {
                f(r.status.get_or_insert_with(Default::default));
            }
        }
        self.notify();
    }

    /// Remove a resource (used when a Starlingfile reload drops it).
    pub fn remove_resource(&self, name: &str) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner
                .resources
                .retain(|r| r.metadata.as_ref().map(|m| m.name.as_str()) != Some(name));
        }
        self.notify();
    }

    pub fn resource_exists(&self, name: &str) -> bool {
        self.inner
            .lock()
            .unwrap()
            .resources
            .iter()
            .any(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(name))
    }

    // -- mutations from the HTTP API ---------------------------------------

    /// Enqueue a build for a manifest. Returns Err if it doesn't exist.
    pub fn trigger(&self, name: &str) -> Result<(), TriggerError> {
        if !self.resource_exists(name) {
            return Err(TriggerError::NotFound);
        }
        self.update_status(name, |st| {
            st.queued = Some(true);
            st.pending_build_since = Some(Utc::now().to_rfc3339());
        });
        self.build_tx
            .send(name.to_string())
            .map_err(|_| TriggerError::EngineGone)?;
        Ok(())
    }

    pub fn set_trigger_mode(&self, names: &[String], mode: i32) -> Result<(), TriggerError> {
        if !(0..=3).contains(&mode) {
            return Err(TriggerError::BadMode);
        }
        for name in names {
            if !self.resource_exists(name) {
                return Err(TriggerError::NotFound);
            }
        }
        for name in names {
            self.update_status(name, |st| st.trigger_mode = Some(mode));
        }
        Ok(())
    }

    // -- UIButtons (web UI apiserver) --------------------------------------

    pub fn upsert_button(&self, button: UIButton) {
        {
            let mut inner = self.inner.lock().unwrap();
            let name = button.metadata.as_ref().map(|m| m.name.clone()).unwrap_or_default();
            if let Some(existing) = inner
                .buttons
                .iter_mut()
                .find(|b| b.metadata.as_ref().map(|m| m.name.as_str()) == Some(name.as_str()))
            {
                *existing = button;
            } else {
                inner.buttons.push(button);
            }
        }
        self.notify();
    }

    pub fn list_buttons(&self) -> Vec<UIButton> {
        self.inner.lock().unwrap().buttons.clone()
    }

    pub fn get_button(&self, name: &str) -> Option<UIButton> {
        self.inner
            .lock()
            .unwrap()
            .buttons
            .iter()
            .find(|b| b.metadata.as_ref().map(|m| m.name.as_str()) == Some(name))
            .cloned()
    }

    /// Record a button click: stamp `lastClickedAt`, store input values, and
    /// return the updated button (or None if it doesn't exist).
    pub fn record_button_click(
        &self,
        name: &str,
        inputs: Vec<UIInputStatus>,
    ) -> Option<UIButton> {
        let updated = {
            let mut inner = self.inner.lock().unwrap();
            let now = Utc::now().to_rfc3339();
            let b = inner
                .buttons
                .iter_mut()
                .find(|b| b.metadata.as_ref().map(|m| m.name.as_str()) == Some(name))?;
            let st = b.status.get_or_insert_with(Default::default);
            st.last_clicked_at = Some(now);
            if !inputs.is_empty() {
                st.inputs = inputs;
            }
            b.clone()
        };
        self.notify();
        Some(updated)
    }

    /// Enable/disable a resource (the DisableToggle button effect).
    pub fn set_resource_disabled(&self, resource: &str, disabled: bool) {
        self.update_status(resource, |st| {
            st.disable_status = Some(DisableResourceStatus {
                enabled_count: if disabled { 0 } else { 1 },
                disabled_count: if disabled { 1 } else { 0 },
                state: if disabled { "Disabled" } else { "Enabled" }.to_string(),
                sources: vec![],
            });
            if disabled {
                st.runtime_status = Some("none".to_string());
                st.update_status = Some("none".to_string());
            }
        });
    }

    /// Number of completed builds for a resource (0 = never built/deployed).
    pub fn build_count(&self, resource: &str) -> usize {
        let inner = self.inner.lock().unwrap();
        inner
            .resources
            .iter()
            .find(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(resource))
            .and_then(|r| r.status.as_ref())
            .map(|s| s.build_history.len())
            .unwrap_or(0)
    }

    /// The current pod name for a k8s resource, if known.
    pub fn current_pod(&self, resource: &str) -> Option<String> {
        let inner = self.inner.lock().unwrap();
        inner
            .resources
            .iter()
            .find(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(resource))
            .and_then(|r| r.status.as_ref())
            .and_then(|s| s.k8s_resource_info.as_ref())
            .and_then(|k| k.pod_name.clone())
    }

    /// Whether a resource is currently disabled.
    pub fn is_resource_disabled(&self, resource: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .resources
            .iter()
            .find(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(resource))
            .and_then(|r| r.status.as_ref())
            .and_then(|s| s.disable_status.as_ref())
            .map(|d| d.state == "Disabled")
            .unwrap_or(false)
    }

    // -- logging -----------------------------------------------------------

    fn append_log_locked(inner: &mut Inner, manifest: Option<&str>, level: &str, text: &str) {
        let span_id = manifest.unwrap_or("").to_string();
        inner
            .log_spans
            .entry(span_id.clone())
            .or_insert_with(|| LogSpan {
                manifest_name: manifest.map(str::to_string),
            });
        inner.log_segments.push(LogSegment {
            span_id: Some(span_id),
            time: Some(Utc::now().to_rfc3339()),
            text: Some(text.to_string()),
            level: Some(level.to_string()),
            ..Default::default()
        });
    }

    pub fn append_log(&self, manifest: Option<&str>, level: &str, text: &str) {
        {
            let mut inner = self.inner.lock().unwrap();
            Self::append_log_locked(&mut inner, manifest, level, text);
        }
        self.notify();
    }
}

#[derive(Debug)]
pub enum TriggerError {
    NotFound,
    BadMode,
    EngineGone,
}
