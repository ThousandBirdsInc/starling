//! Minimal Kubernetes YAML parsing.
//!
//! Mirrors the slice of Go's `internal/k8s` that the Starlingfile needs: split a
//! multi-document YAML stream into entities, identify workloads, and extract
//! their container images and pod selector. We re-serialize each document so
//! the engine can `kubectl apply` it individually.

use std::collections::BTreeMap;

use serde_yaml::Value;

#[derive(Debug, Clone)]
pub struct K8sEntity {
    pub kind: String,
    pub name: String,
    /// `metadata.namespace`, defaulting to "default" when unset (matches the
    /// identity Tilt uses for `workload_to_resource_function`).
    pub namespace: String,
    /// The document's `apiVersion` (e.g. `apps/v1`), used to derive the API
    /// group for object identity.
    pub api_version: String,
    /// The single document re-serialized as YAML (for `kubectl apply -f -`).
    pub raw: String,
    /// Container images referenced (workloads only).
    pub images: Vec<String>,
    /// Container env var string values. Used for Tilt's match_in_env_vars.
    pub env_values: Vec<String>,
    /// `spec.selector.matchLabels` for workloads (used to watch pods).
    pub match_labels: BTreeMap<String, String>,
}

impl K8sEntity {
    pub fn is_workload(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "Deployment" | "StatefulSet" | "DaemonSet" | "ReplicaSet" | "Job" | "CronJob" | "Pod"
        )
    }

    /// The API group, i.e. the portion of `apiVersion` before the `/`. The core
    /// group (`v1`) and an empty `apiVersion` both yield an empty group.
    pub fn group(&self) -> String {
        match self.api_version.split_once('/') {
            Some((group, _)) => group.to_string(),
            None => String::new(),
        }
    }
}

/// Parse a multi-document YAML string into entities. Documents that aren't
/// mappings (or lack kind/name) are skipped.
pub fn parse_yaml(content: &str) -> Vec<K8sEntity> {
    let mut out = vec![];
    for doc in serde_yaml::Deserializer::from_str(content) {
        let value = match Value::deserialize(doc) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if value.is_null() {
            continue;
        }
        let kind = str_at(&value, &["kind"]).unwrap_or_default();
        let name = str_at(&value, &["metadata", "name"]).unwrap_or_default();
        if kind.is_empty() || name.is_empty() {
            continue;
        }
        let namespace = str_at(&value, &["metadata", "namespace"])
            .filter(|ns| !ns.is_empty())
            .unwrap_or_else(|| "default".to_string());
        let api_version = str_at(&value, &["apiVersion"]).unwrap_or_default();
        let raw = serde_yaml::to_string(&value).unwrap_or_default();
        let images = extract_images(&value);
        let env_values = extract_env_values(&value);
        let match_labels = extract_match_labels(&value);
        out.push(K8sEntity {
            kind,
            name,
            namespace,
            api_version,
            raw,
            images,
            env_values,
            match_labels,
        });
    }
    out
}

use serde::Deserialize;

/// Follow a path of mapping keys, returning a string leaf if present.
fn str_at(v: &Value, path: &[&str]) -> Option<String> {
    let mut cur = v;
    for key in path {
        cur = cur.get(Value::String((*key).to_string()))?;
    }
    cur.as_str().map(str::to_string)
}

/// Collect all `image:` fields under every `containers`/`initContainers` list.
fn extract_images(v: &Value) -> Vec<String> {
    let mut images = vec![];
    collect_images(v, &mut images);
    images.sort();
    images.dedup();
    images
}

fn extract_env_values(v: &Value) -> Vec<String> {
    let mut values = vec![];
    collect_env_values(v, &mut values);
    values.sort();
    values.dedup();
    values
}

fn collect_env_values(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Mapping(map) => {
            for (k, val) in map {
                if k.as_str() == Some("env") {
                    if let Value::Sequence(seq) = val {
                        for env in seq {
                            if let Some(value) = env.get("value").and_then(Value::as_str) {
                                out.push(value.to_string());
                            }
                        }
                    }
                }
                collect_env_values(val, out);
            }
        }
        Value::Sequence(seq) => {
            for item in seq {
                collect_env_values(item, out);
            }
        }
        _ => {}
    }
}

fn collect_images(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Mapping(map) => {
            for (k, val) in map {
                if k.as_str() == Some("containers") || k.as_str() == Some("initContainers") {
                    if let Value::Sequence(seq) = val {
                        for c in seq {
                            if let Some(img) = c.get("image").and_then(Value::as_str) {
                                out.push(img.to_string());
                            }
                        }
                    }
                }
                collect_images(val, out);
            }
        }
        Value::Sequence(seq) => {
            for item in seq {
                collect_images(item, out);
            }
        }
        _ => {}
    }
}

fn extract_match_labels(v: &Value) -> BTreeMap<String, String> {
    // 1. An explicit pod selector (Deployments, StatefulSets, ReplicaSets, …).
    let selector = labels_at(v, &["spec", "selector", "matchLabels"]);
    if !selector.is_empty() {
        return selector;
    }
    // 2. The pod template's labels — the labels the pods actually carry. Jobs
    //    don't set `spec.selector` in their manifest (Kubernetes auto-generates
    //    a controller-uid selector at apply time), so their `metadata.labels`
    //    (e.g. Helm chart labels) are NOT what their pods carry. Selecting on the
    //    pod template labels is what makes a Job's pod logs discoverable.
    let template = labels_at(v, &["spec", "template", "metadata", "labels"]);
    if !template.is_empty() {
        return template;
    }
    // CronJobs nest the pod template one level deeper.
    let cron = labels_at(
        v,
        &[
            "spec",
            "jobTemplate",
            "spec",
            "template",
            "metadata",
            "labels",
        ],
    );
    if !cron.is_empty() {
        return cron;
    }
    // 3. Bare Pods: their own labels are the selector.
    labels_at(v, &["metadata", "labels"])
}

/// Collect a `string: string` label map at a mapping path. Empty if any path
/// segment is absent or the leaf isn't a string→string mapping.
fn labels_at(v: &Value, path: &[&str]) -> BTreeMap<String, String> {
    let mut cur = v;
    for key in path {
        match cur.get(*key) {
            Some(next) => cur = next,
            None => return BTreeMap::new(),
        }
    }
    let mut out = BTreeMap::new();
    if let Value::Mapping(m) = cur {
        for (k, val) in m {
            if let (Some(k), Some(val)) = (k.as_str(), val.as_str()) {
                out.insert(k.to_string(), val.to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const YAML: &str = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
spec:
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      initContainers:
      - name: init
        image: busybox:1.36
      containers:
      - name: web
        image: myreg/web:dev
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  ports:
  - port: 80
"#;

    #[test]
    fn parses_workload_and_service() {
        let entities = parse_yaml(YAML);
        assert_eq!(entities.len(), 2);

        let dep = &entities[0];
        assert_eq!(dep.kind, "Deployment");
        assert_eq!(dep.name, "web");
        assert!(dep.is_workload());
        assert_eq!(dep.images, vec!["busybox:1.36", "myreg/web:dev"]);
        assert_eq!(dep.match_labels.get("app").map(String::as_str), Some("web"));
        // No metadata.namespace -> "default"; apps/v1 -> group "apps".
        assert_eq!(dep.namespace, "default");
        assert_eq!(dep.api_version, "apps/v1");
        assert_eq!(dep.group(), "apps");

        let svc = &entities[1];
        assert_eq!(svc.kind, "Service");
        assert!(!svc.is_workload());
        assert!(svc.images.is_empty());
        // Core group (apiVersion "v1") -> empty group.
        assert_eq!(svc.group(), "");
    }

    #[test]
    fn skips_non_resource_docs() {
        let entities = parse_yaml("---\nfoo: bar\n---\n# just a comment\n");
        assert!(entities.is_empty());
    }

    #[test]
    fn job_selector_uses_pod_template_labels_not_metadata() {
        // A Job sets no spec.selector (k8s auto-generates it); its own
        // metadata.labels (e.g. Helm chart labels) are NOT what its pods carry.
        // The selector must come from the pod template labels.
        let yaml = r#"
apiVersion: batch/v1
kind: Job
metadata:
  name: db-migrate
  labels:
    app.kubernetes.io/managed-by: Helm
    helm.sh/chart: app-agent-0.1.0
spec:
  template:
    metadata:
      labels:
        app: db-migrate
    spec:
      containers:
      - name: migrate
        image: app-agent-grpc:dev
"#;
        let entities = parse_yaml(yaml);
        let job = &entities[0];
        assert_eq!(job.kind, "Job");
        assert!(job.is_workload());
        // The selector is the pod template label, not the Helm chart labels.
        assert_eq!(
            job.match_labels.get("app").map(String::as_str),
            Some("db-migrate")
        );
        assert!(!job.match_labels.contains_key("helm.sh/chart"));
    }
}
