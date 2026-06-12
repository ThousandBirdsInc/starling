//! In-memory engine state plus a change-notification channel.
//!
//! An in-memory, Kubernetes-style object store. The websocket reads a full
//! `View` on connect and incremental deltas thereafter. Resources are populated
//! by the engine from the Starlingfile; builds and logs reflect real subprocess
//! execution. `/api/trigger` enqueues a build on `build_tx`, which the engine
//! consumes.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::Utc;
use tokio::sync::{broadcast, mpsc};

use crate::api::v1alpha1::*;
use crate::api::webview::{LogList, LogSegment, LogSpan, View};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BuildRequest {
    Auto(String, Vec<PathBuf>),
    ForceFull(String, Vec<PathBuf>),
}

impl BuildRequest {
    pub fn auto(name: impl Into<String>, changed_paths: Vec<PathBuf>) -> Self {
        BuildRequest::Auto(name.into(), changed_paths)
    }

    pub fn force_full_request(name: impl Into<String>, changed_paths: Vec<PathBuf>) -> Self {
        BuildRequest::ForceFull(name.into(), changed_paths)
    }

    pub fn name(&self) -> &str {
        match self {
            BuildRequest::Auto(name, _) | BuildRequest::ForceFull(name, _) => name,
        }
    }

    pub fn force_full(&self) -> bool {
        matches!(self, BuildRequest::ForceFull(_, _))
    }

    pub fn changed_paths(&self) -> &[PathBuf] {
        match self {
            BuildRequest::Auto(_, paths) | BuildRequest::ForceFull(_, paths) => paths,
        }
    }
}

/// The mutable engine state guarded by a single lock.
struct Inner {
    session: UISession,
    resources: Vec<UIResource>,
    buttons: Vec<UIButton>,
    clusters: Vec<Cluster>,
    /// Recent log segments (capped ring). Checkpoints are *absolute* indices;
    /// `log_start` is the absolute index of `log_segments[0]`, so dropping old
    /// segments doesn't shift the checkpoints clients already hold.
    log_segments: Vec<LogSegment>,
    /// Absolute index of the first retained segment (advances as old ones drop).
    log_start: usize,
    /// Secret values to redact from log text (from k8s Secret objects), unless
    /// `secret_settings(disable_scrub=True)`.
    scrub_secrets: Vec<String>,
    log_spans: BTreeMap<String, LogSpan>,
}

/// Max log segments retained in memory before the oldest are dropped.
const MAX_LOG_SEGMENTS: usize = 5000;

pub struct Store {
    inner: Mutex<Inner>,
    /// Bumped whenever state changes; websocket tasks wake and send a delta.
    notify: broadcast::Sender<()>,
    /// Build requests sent to the engine.
    build_tx: mpsc::UnboundedSender<BuildRequest>,
    /// Restart requests (resource names) sent to the engine.
    restart_tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
    /// Live Tiltfile arg replacements sent to the engine.
    tiltfile_args_tx: Mutex<Option<mpsc::UnboundedSender<Vec<String>>>>,
    start_time: String,
}

impl Store {
    /// Create a store seeded with environment info (session + cluster) but no
    /// resources; the engine adds those from the Starlingfile.
    pub fn new(build_tx: mpsc::UnboundedSender<BuildRequest>) -> Self {
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
                log_start: 0,
                scrub_secrets: vec![],
                log_spans: BTreeMap::new(),
            }),
            notify,
            build_tx,
            restart_tx: Mutex::new(None),
            tiltfile_args_tx: Mutex::new(None),
            start_time,
        };
        store.append_log(None, "INFO", "Starling started\n");
        store
    }

    /// Wire the channel the engine listens on for serve_cmd restart requests.
    pub fn set_restart_tx(&self, tx: mpsc::UnboundedSender<String>) {
        *self.restart_tx.lock().unwrap() = Some(tx);
    }

    /// Wire the channel the engine listens on for live Tiltfile arg changes.
    pub fn set_tiltfile_args_tx(&self, tx: mpsc::UnboundedSender<Vec<String>>) {
        *self.tiltfile_args_tx.lock().unwrap() = Some(tx);
    }

    /// Replace Tiltfile args and request an engine reload.
    pub fn set_tiltfile_args(&self, args: Vec<String>) -> Result<(), TriggerError> {
        if let Some(tx) = self.tiltfile_args_tx.lock().unwrap().as_ref() {
            tx.send(args).map_err(|_| TriggerError::EngineGone)?;
        }
        Ok(())
    }

    /// Request a serve_cmd restart for `name` (no-op if it doesn't exist).
    pub fn restart(&self, name: &str) -> Result<(), TriggerError> {
        if !self.resource_exists(name) {
            return Err(TriggerError::NotFound);
        }
        if let Some(tx) = self.restart_tx.lock().unwrap().as_ref() {
            tx.send(name.to_string())
                .map_err(|_| TriggerError::EngineGone)?;
        }
        Ok(())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify.subscribe()
    }

    fn notify(&self) {
        let _ = self.notify.send(());
    }

    /// The absolute end checkpoint (total segments ever appended, including any
    /// since dropped from the front).
    pub fn log_len(&self) -> i32 {
        let inner = self.inner.lock().unwrap();
        (inner.log_start + inner.log_segments.len()) as i32
    }

    /// Log lines appended since `checkpoint`, grouped by span (resource name),
    /// for the daemon dashboard. The empty global span is reported under
    /// "(system)". Returns the new checkpoint (the current segment count) to
    /// pass back on the next call so only fresh lines are sent each tick.
    pub fn logs_since(&self, checkpoint: usize) -> (BTreeMap<String, Vec<String>>, usize) {
        let inner = self.inner.lock().unwrap();
        let total = inner.log_start + inner.log_segments.len();
        // Translate the absolute checkpoint into a local index (a checkpoint
        // older than what's retained yields everything still in the ring).
        let local = checkpoint
            .saturating_sub(inner.log_start)
            .min(inner.log_segments.len());
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for seg in &inner.log_segments[local..] {
            let span = seg.span_id.as_deref().unwrap_or("");
            // Roll sub-spans (e.g. "web:build:2") up to their base resource so
            // build logs appear under the resource in the dashboard.
            let base = base_resource(span);
            let key = if base.is_empty() {
                "(system)".to_string()
            } else {
                base.to_string()
            };
            let text = seg.text.clone().unwrap_or_default();
            out.entry(key)
                .or_default()
                .push(text.trim_end().to_string());
        }
        (out, total)
    }

    /// Structured read over the log store, filtered by span (resource name) and
    /// minimum level. This is the "runtime log reader" the CLI/API needs to pull
    /// a resource's logs by level rather than the dashboard's tail-by-cursor.
    pub fn query_logs(&self, query: &LogQuery) -> Vec<LogLine> {
        let inner = self.inner.lock().unwrap();
        let min = query.min_level.as_deref().map(log_level_rank).unwrap_or(0);
        inner
            .log_segments
            .iter()
            .filter(|seg| {
                // Span filter matches the exact span or the base resource, so
                // querying "web" includes its "web:build:N" sub-spans.
                match &query.span {
                    Some(span) => {
                        let s = seg.span_id.as_deref().unwrap_or("");
                        s == span || base_resource(s) == span
                    }
                    None => true,
                }
            })
            .filter(|seg| log_level_rank(seg.level.as_deref().unwrap_or("INFO")) >= min)
            .filter(|seg| {
                // RFC3339 timestamps from the same UTC offset order lexically.
                let time = seg.time.as_deref().unwrap_or("");
                query.since.as_deref().is_none_or(|s| time >= s)
                    && query.until.as_deref().is_none_or(|u| time < u)
            })
            .map(|seg| LogLine {
                span: seg.span_id.clone().unwrap_or_default(),
                level: seg.level.clone().unwrap_or_else(|| "INFO".to_string()),
                time: seg.time.clone().unwrap_or_default(),
                text: seg.text.clone().unwrap_or_default(),
            })
            .collect()
    }

    /// Apply Starlingfile session settings to the `UISession` status: the team
    /// id (`set_team`) and feature flags (`enable_feature`/`disable_feature`).
    pub fn apply_session_settings(
        &self,
        team_id: Option<String>,
        feature_flags: &[(String, bool)],
    ) {
        {
            let mut inner = self.inner.lock().unwrap();
            let status = inner.session.status.get_or_insert_with(Default::default);
            if let Some(team) = team_id {
                status.tilt_cloud_team_id = Some(team);
            }
            status.feature_flags = feature_flags
                .iter()
                .map(|(name, value)| UIFeatureFlag {
                    name: Some(name.clone()),
                    value: Some(*value),
                })
                .collect();
        }
        self.notify();
    }

    // -- view assembly -----------------------------------------------------

    pub fn full_view(&self) -> View {
        let inner = self.inner.lock().unwrap();
        let from = inner.log_start as i32;
        let to = (inner.log_start + inner.log_segments.len()) as i32;
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
                from_checkpoint: Some(from),
                to_checkpoint: Some(to),
            }),
            is_complete: Some(true),
            ..Default::default()
        }
    }

    pub fn delta_view(&self, from: i32) -> (View, i32) {
        let inner = self.inner.lock().unwrap();
        let start = inner.log_start;
        let to = (start + inner.log_segments.len()) as i32;
        // Translate the absolute `from` checkpoint into a local index.
        let local = (from.max(0) as usize)
            .saturating_sub(start)
            .min(inner.log_segments.len());
        let segments = inner.log_segments[local..].to_vec();
        let view = View {
            ui_session: Some(inner.session.clone()),
            ui_resources: inner.resources.clone(),
            ui_buttons: inner.buttons.clone(),
            clusters: inner.clusters.clone(),
            log_list: Some(LogList {
                spans: Some(inner.log_spans.clone()),
                segments,
                from_checkpoint: Some((start + local) as i32),
                to_checkpoint: Some(to),
            }),
            is_complete: Some(false),
            ..Default::default()
        };
        (view, to)
    }

    // -- resource management (used by the engine) --------------------------

    pub fn upsert_resource(&self, mut resource: UIResource) {
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
                // Preserve disabled state across reloads: the engine re-materializes
                // resources with a fresh (enabled) status, but a resource the user
                // disabled should stay disabled until they re-enable it.
                if let (Some(new), Some(old)) = (resource.status.as_mut(), existing.status.as_ref())
                {
                    if new.disable_status.is_none() {
                        new.disable_status = old.disable_status.clone();
                    }
                }
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
            let Some(index) = inner
                .resources
                .iter()
                .position(|r| r.metadata.as_ref().map(|m| m.name.as_str()) == Some(name))
            else {
                return;
            };

            let mut status_changes = Vec::new();
            {
                let status = inner.resources[index]
                    .status
                    .get_or_insert_with(Default::default);
                let old_update = status.update_status.clone();
                let old_runtime = status.runtime_status.clone();
                f(status);

                if old_update != status.update_status {
                    status_changes.push(("update", old_update, status.update_status.clone()));
                }
                if old_runtime != status.runtime_status {
                    status_changes.push(("runtime", old_runtime, status.runtime_status.clone()));
                }
            }

            if !status_changes.is_empty() {
                let now = Utc::now().to_rfc3339();
                for (kind, old, new) in status_changes {
                    let text = format!(
                        "Status change {now}: {kind} {} -> {}\n",
                        status_log_value(kind, old.as_deref()),
                        status_log_value(kind, new.as_deref())
                    );
                    Self::append_log_locked(&mut inner, Some(name), "INFO", &text);
                }
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
            .send(BuildRequest::force_full_request(name.to_string(), vec![]))
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
            let name = button
                .metadata
                .as_ref()
                .map(|m| m.name.clone())
                .unwrap_or_default();
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
    pub fn record_button_click(&self, name: &str, inputs: Vec<UIInputStatus>) -> Option<UIButton> {
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

    /// Replace the set of secret values redacted from log output.
    pub fn set_scrub_secrets(&self, secrets: Vec<String>) {
        self.inner.lock().unwrap().scrub_secrets = secrets;
    }

    fn append_log_locked(inner: &mut Inner, manifest: Option<&str>, level: &str, text: &str) {
        let text = scrub_secrets(text, &inner.scrub_secrets);
        let text = text.as_str();
        let span_id = manifest.unwrap_or("").to_string();
        // A span like "web:build:2" belongs to resource "web"; the LogSpan's
        // manifest_name is the base resource so the frontend groups it there.
        let manifest_name = base_resource(&span_id);
        inner
            .log_spans
            .entry(span_id.clone())
            .or_insert_with(|| LogSpan {
                manifest_name: (!manifest_name.is_empty()).then(|| manifest_name.to_string()),
            });
        inner.log_segments.push(LogSegment {
            span_id: Some(span_id),
            time: Some(Utc::now().to_rfc3339()),
            text: Some(text.to_string()),
            level: Some(level.to_string()),
            ..Default::default()
        });
        // Cap memory: drop the oldest segments, advancing the absolute start so
        // checkpoints clients already hold remain valid.
        if inner.log_segments.len() > MAX_LOG_SEGMENTS {
            let drop = inner.log_segments.len() - MAX_LOG_SEGMENTS;
            inner.log_segments.drain(0..drop);
            inner.log_start += drop;
        }
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

/// A structured log line returned by [`Store::query_logs`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// The span (resource name); empty for the global/system span.
    pub span: String,
    pub level: String,
    pub time: String,
    pub text: String,
}

/// Filter for [`Store::query_logs`].
#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    /// Restrict to one span (resource name); `None` returns all spans.
    pub span: Option<String>,
    /// Minimum level (`DEBUG` < `INFO` < `WARN` < `ERROR`); `None` returns all.
    pub min_level: Option<String>,
    /// Only lines at/after this RFC3339 timestamp (inclusive); `None` = no bound.
    pub since: Option<String>,
    /// Only lines strictly before this RFC3339 timestamp; `None` = no bound.
    pub until: Option<String>,
}

/// Redact every (non-empty) secret value from `text`, replacing it with
/// `[redacted]`. Used to keep k8s Secret material out of the log store.
fn scrub_secrets(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_string();
    for secret in secrets {
        if !secret.is_empty() {
            out = out.replace(secret.as_str(), "[redacted]");
        }
    }
    out
}

/// The base resource a log span belongs to: the part before the first `:`.
/// `"web:build:2"` -> `"web"`, `"web"` -> `"web"`, `""` -> `""`.
fn base_resource(span: &str) -> &str {
    span.split(':').next().unwrap_or(span)
}

fn status_log_value(kind: &str, value: Option<&str>) -> String {
    match value.unwrap_or_default() {
        "" | "none" | "not_applicable" => "none".to_string(),
        "in_progress" if kind == "runtime" => "restarting".to_string(),
        "in_progress" if kind == "update" => "building".to_string(),
        other => other.replace('_', " "),
    }
}

/// Severity ordering for level filtering. Unknown levels sort as `INFO`.
fn log_level_rank(level: &str) -> u8 {
    match level.to_ascii_uppercase().as_str() {
        "DEBUG" | "VERBOSE" => 0,
        "WARN" | "WARNING" => 2,
        "ERROR" => 3,
        _ => 1, // INFO and anything unrecognized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_store() -> Store {
        let (tx, _rx) = mpsc::unbounded_channel();
        Store::new(tx)
    }

    #[test]
    fn log_buffer_is_capped_with_stable_absolute_checkpoints() {
        let store = test_store();
        let total = MAX_LOG_SEGMENTS + 100;
        for i in 0..total {
            store.append_log(Some("web"), "INFO", &format!("line {i}\n"));
        }
        // The buffer is capped, but log_len reports the absolute total (incl. the
        // seed "Starling started" line + all appended).
        let absolute_end = store.log_len() as usize;
        assert!(absolute_end >= total);
        // logs_since with an absolute checkpoint near the end returns only the
        // tail and a matching new checkpoint (no panic from shifted indices).
        let (logs, next) = store.logs_since(absolute_end - 3);
        assert_eq!(next, absolute_end);
        let web = logs.get("web").map(|v| v.len()).unwrap_or(0);
        assert_eq!(web, 3);
        // A stale checkpoint (older than retained) yields the whole retained ring.
        let (all, _) = store.logs_since(0);
        assert_eq!(all.get("web").unwrap().len(), MAX_LOG_SEGMENTS);
    }

    #[test]
    fn query_logs_filters_by_span_and_level() {
        let store = test_store();
        // Store::new appends a global "Starling started" INFO line.
        store.append_log(Some("web"), "INFO", "building\n");
        store.append_log(Some("web"), "ERROR", "boom\n");
        store.append_log(Some("api"), "WARN", "slow\n");

        // Span filter: only "web" lines.
        let web = store.query_logs(&LogQuery {
            span: Some("web".to_string()),
            ..Default::default()
        });
        assert_eq!(web.len(), 2);
        assert!(web.iter().all(|l| l.span == "web"));

        // Level filter: WARN+ across all spans (web ERROR + api WARN).
        let warns = store.query_logs(&LogQuery {
            min_level: Some("WARN".to_string()),
            ..Default::default()
        });
        assert_eq!(warns.len(), 2);
        assert!(warns
            .iter()
            .all(|l| l.level == "ERROR" || l.level == "WARN"));

        // Span + level combined: web ERROR only.
        let web_err = store.query_logs(&LogQuery {
            span: Some("web".to_string()),
            min_level: Some("ERROR".to_string()),
            ..Default::default()
        });
        assert_eq!(web_err.len(), 1);
        assert_eq!(web_err[0].text.trim(), "boom");
    }

    #[test]
    fn status_changes_are_logged_with_timestamps() {
        let store = test_store();
        store.upsert_resource(UIResource {
            metadata: Some(ObjectMeta {
                name: "web".to_string(),
                ..Default::default()
            }),
            spec: None,
            status: Some(UIResourceStatus {
                update_status: Some("pending".to_string()),
                runtime_status: Some("pending".to_string()),
                ..Default::default()
            }),
        });

        store.update_status("web", |st| {
            st.update_status = Some("in_progress".to_string());
            st.runtime_status = Some("in_progress".to_string());
        });

        let logs = store.query_logs(&LogQuery {
            span: Some("web".to_string()),
            ..Default::default()
        });
        let changes: Vec<_> = logs
            .iter()
            .filter(|line| line.text.starts_with("Status change "))
            .collect();
        assert_eq!(changes.len(), 2);

        for line in &changes {
            let text = line.text.trim();
            let rest = text.strip_prefix("Status change ").unwrap();
            let (timestamp, _) = rest.split_once(": ").unwrap();
            chrono::DateTime::parse_from_rfc3339(timestamp).unwrap();
        }
        assert!(changes
            .iter()
            .any(|line| line.text.contains("update pending -> building")));
        assert!(changes
            .iter()
            .any(|line| line.text.contains("runtime pending -> restarting")));
    }

    #[test]
    fn disabled_state_survives_reload() {
        let store = test_store();
        let res = |name: &str| UIResource {
            metadata: Some(ObjectMeta {
                name: name.to_string(),
                ..Default::default()
            }),
            spec: None,
            status: Some(UIResourceStatus::default()),
        };
        store.upsert_resource(res("web"));
        store.set_resource_disabled("web", true);
        assert!(store.is_resource_disabled("web"));

        // Simulate a reload: the engine re-materializes a fresh resource.
        store.upsert_resource(res("web"));
        assert!(
            store.is_resource_disabled("web"),
            "disabled state should survive reload"
        );
    }

    #[test]
    fn scrub_secrets_redacts_values_from_logs() {
        // Pure function.
        assert_eq!(
            scrub_secrets("token=s3cr3t done", &["s3cr3t".to_string()]),
            "token=[redacted] done"
        );
        assert_eq!(scrub_secrets("nothing", &["".to_string()]), "nothing");

        // Integrated: appended log lines are scrubbed.
        let store = test_store();
        store.set_scrub_secrets(vec!["hunter2".to_string()]);
        store.append_log(Some("web"), "INFO", "password is hunter2\n");
        let logs = store.query_logs(&LogQuery {
            span: Some("web".to_string()),
            ..Default::default()
        });
        assert!(logs.iter().any(|l| l.text.contains("[redacted]")));
        assert!(!logs.iter().any(|l| l.text.contains("hunter2")));
    }

    #[test]
    fn apply_session_settings_sets_team_and_feature_flags() {
        let store = test_store();
        store.apply_session_settings(
            Some("team-42".to_string()),
            &[("snapshots".to_string(), true), ("beta".to_string(), false)],
        );
        let view = store.full_view();
        let status = view.ui_session.unwrap().status.unwrap();
        assert_eq!(status.tilt_cloud_team_id.as_deref(), Some("team-42"));
        assert_eq!(status.feature_flags.len(), 2);
        let snapshots = status
            .feature_flags
            .iter()
            .find(|f| f.name.as_deref() == Some("snapshots"))
            .unwrap();
        assert_eq!(snapshots.value, Some(true));
    }

    #[test]
    fn build_sub_spans_roll_up_to_resource() {
        let store = test_store();
        store.append_log(Some("web"), "INFO", "serving\n");
        store.append_log(Some("web:build:1"), "INFO", "compiling\n");
        store.append_log(Some("web:build:2"), "INFO", "recompiling\n");

        // Dashboard grouping rolls build sub-spans up to "web".
        let (logs, _) = store.logs_since(0);
        assert!(logs.get("web:build:1").is_none());
        assert_eq!(logs.get("web").unwrap().len(), 3);

        // Querying by resource includes the build sub-spans...
        let by_resource = store.query_logs(&LogQuery {
            span: Some("web".to_string()),
            ..Default::default()
        });
        assert_eq!(by_resource.len(), 3);
        // ...while an exact build span isolates that one build's logs.
        let build2 = store.query_logs(&LogQuery {
            span: Some("web:build:2".to_string()),
            ..Default::default()
        });
        assert_eq!(build2.len(), 1);
        assert_eq!(build2[0].text.trim(), "recompiling");

        // The webview LogSpan for a build sub-span names the base resource.
        let view = store.full_view();
        let spans = view.log_list.unwrap().spans.unwrap();
        assert_eq!(
            spans.get("web:build:1").unwrap().manifest_name.as_deref(),
            Some("web")
        );
    }

    #[test]
    fn query_logs_filters_by_time_range() {
        let store = test_store();
        store.append_log(Some("web"), "INFO", "hello\n");
        let base = LogQuery {
            span: Some("web".to_string()),
            ..Default::default()
        };
        let past = "1970-01-01T00:00:00+00:00".to_string();
        let future = "9999-01-01T00:00:00+00:00".to_string();

        // since in the past / until in the future include the line.
        assert_eq!(
            store
                .query_logs(&LogQuery {
                    since: Some(past.clone()),
                    ..base.clone()
                })
                .len(),
            1
        );
        assert_eq!(
            store
                .query_logs(&LogQuery {
                    until: Some(future.clone()),
                    ..base.clone()
                })
                .len(),
            1
        );
        // since in the future / until in the past exclude it.
        assert_eq!(
            store
                .query_logs(&LogQuery {
                    since: Some(future),
                    ..base.clone()
                })
                .len(),
            0
        );
        assert_eq!(
            store
                .query_logs(&LogQuery {
                    until: Some(past),
                    ..base
                })
                .len(),
            0
        );
    }
}
