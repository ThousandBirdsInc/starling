//! The `View` envelope streamed over the websocket, plus the log model.
//!
//! Mirrors `pkg/webview/view.go` and `web/src/webview.d.ts`. The websocket
//! sends two kinds of `View` messages:
//!   1. On connect: the complete view state (`is_complete = true`).
//!   2. On change: only the resources/logs that changed since the last send.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::v1alpha1::{Cluster, TiltBuild, UIButton, UIResource, UISession, VersionSettings};

/// LogLevel severity. Values: NONE INFO VERBOSE DEBUG WARN ERROR.
pub type LogLevel = String;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSegment {
    #[serde(rename = "spanId", skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<LogLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogSpan {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogList {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spans: Option<BTreeMap<String, LogSpan>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub segments: Vec<LogSegment>,
    /// Inclusive start of the interval on the central log store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from_checkpoint: Option<i32>,
    /// Exclusive end of the interval on the central log store.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_checkpoint: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct View {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feature_flags: Option<BTreeMap<String, bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub needs_analytics_nudge: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub running_tilt_build: Option<TiltBuild>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggested_tilt_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version_settings: Option<VersionSettings>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt_cloud_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt_cloud_team_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt_cloud_scheme_host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt_cloud_team_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fatal_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_list: Option<LogList>,
    /// Lets the UI detect when Tilt has restarted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilt_start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiltfile_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_session: Option<UISession>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ui_resources: Vec<UIResource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ui_buttons: Vec<UIButton>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clusters: Vec<Cluster>,
    /// True for the initial full view, false for delta updates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_complete: Option<bool>,
}

/// Wraps a `View` for the `/api/snapshot/{id}` endpoint.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view: Option<View>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_sidebar_closed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
}
