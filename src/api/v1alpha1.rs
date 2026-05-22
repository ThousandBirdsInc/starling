//! Kubernetes-style API types that the web frontend consumes.
//!
//! These mirror the Go types in `pkg/apis/core/v1alpha1` (and the generated
//! TypeScript in `web/src/core.d.ts`). Field names serialize as camelCase to
//! match the existing frontend's expectations exactly.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Kubernetes ObjectMeta. Every API object embeds one.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    #[serde(default)]
    pub uid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub creation_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<String>,
}

// ---------------------------------------------------------------------------
// Shared status enums (serialized as the lowercase string values the UI checks)
// ---------------------------------------------------------------------------

/// RuntimeStatus: high-level summary of a server's runtime state.
/// Valid values: "unknown" "ok" "pending" "error" "not_applicable" "none".
pub type RuntimeStatus = String;

/// UpdateStatus: high-level summary of update tasks bringing a resource up to date.
/// Valid values: "none" "in_progress" "ok" "pending" "error" "not_applicable".
pub type UpdateStatus = String;

/// DisableState: "" (pending) "Enabled" "Disabled" "Error".
pub type DisableState = String;

// ---------------------------------------------------------------------------
// UISession
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UISession {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<UISessionSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<UISessionStatus>,
}

/// UISessionSpec is intentionally empty (a kludge for surfacing internal status).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UISessionSpec {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UISessionStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub feature_flags: Vec<UIFeatureFlag>,
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
    pub tilt_start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tiltfile_key: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIFeatureFlag {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TiltBuild {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check_updates: Option<bool>,
}

// ---------------------------------------------------------------------------
// UIResource
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIResource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<UIResourceSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<UIResourceStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIResourceSpec {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_deploy_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_mode: Option<i32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub build_history: Vec<UIBuildTerminated>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_build: Option<UIBuildRunning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_build_since: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_pending_changes: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub endpoint_links: Vec<UIResourceLink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub k8s_resource_info: Option<UIResourceKubernetes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compose_resource_info: Option<UIResourceCompose>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_resource_info: Option<UIResourceLocal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_status: Option<RuntimeStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_status: Option<UpdateStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub specs: Vec<UIResourceTargetSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queued: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disable_status: Option<DisableResourceStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waiting: Option<UIResourceStateWaiting>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<UIResourceCondition>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIResourceLink {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceTargetSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub target_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_live_update: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIBuildRunning {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(rename = "spanID", skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIBuildTerminated {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_time: Option<String>,
    #[serde(rename = "spanID", skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_crash_rebuild: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceKubernetes {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_creation_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_update_start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_status_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_containers_ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pod_restarts: Option<i32>,
    #[serde(rename = "spanID", skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub display_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceCompose {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_status: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceLocal {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_test: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceStateWaiting {
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on: Vec<UIResourceStateWaitingOnRef>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceStateWaitingOnRef {
    pub group: String,
    pub api_version: String,
    pub kind: String,
    pub name: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIResourceCondition {
    #[serde(rename = "type")]
    pub condition_type: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_transition_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisableResourceStatus {
    pub enabled_count: i32,
    pub disabled_count: i32,
    pub state: DisableState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<DisableSource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisableSource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_map: Option<ConfigMapDisableSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub every_config_map: Vec<ConfigMapDisableSource>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigMapDisableSource {
    pub name: String,
    pub key: String,
}

// ---------------------------------------------------------------------------
// UIButton
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIButton {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<UIButtonSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<UIButtonStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIButtonSpec {
    pub location: UIComponentLocation,
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_name: Option<String>,
    #[serde(rename = "iconSVG", skip_serializing_if = "Option::is_none")]
    pub icon_svg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requires_confirmation: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<UIInputSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIComponentLocation {
    #[serde(rename = "componentID")]
    pub component_id: String,
    pub component_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIInputSpec {
    pub name: String,
    pub label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<UITextInputSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bool: Option<UIBoolInputSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden: Option<UIHiddenInputSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choice: Option<UIChoiceInputSpec>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UITextInputSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIBoolInputSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub true_string: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub false_string: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIHiddenInputSpec {
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIChoiceInputSpec {
    #[serde(default)]
    pub choices: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIButtonStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_clicked_at: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inputs: Vec<UIInputStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UIInputStatus {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<UITextInputStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bool: Option<UIBoolInputStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hidden: Option<UIHiddenInputStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choice: Option<UIChoiceInputStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UITextInputStatus {
    pub value: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIBoolInputStatus {
    pub value: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIHiddenInputStatus {
    pub value: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UIChoiceInputStatus {
    pub value: String,
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cluster {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ObjectMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<ClusterSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ClusterStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSpec {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<ClusterConnection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_registry: Option<RegistryHosting>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubernetesClusterConnection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docker: Option<DockerClusterConnection>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesClusterConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DockerClusterConnection {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connected_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registry: Option<RegistryHosting>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<ClusterConnectionStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterConnectionStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kubernetes: Option<KubernetesClusterConnectionStatus>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KubernetesClusterConnectionStatus {
    pub context: String,
    pub namespace: String,
    pub cluster: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryHosting {
    pub host: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_from_cluster_network: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_from_container_runtime: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub single_name: Option<String>,
}
