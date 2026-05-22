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
    /// The single document re-serialized as YAML (for `kubectl apply -f -`).
    pub raw: String,
    /// Container images referenced (workloads only).
    pub images: Vec<String>,
    /// `spec.selector.matchLabels` for workloads (used to watch pods).
    pub match_labels: BTreeMap<String, String>,
}

impl K8sEntity {
    pub fn is_workload(&self) -> bool {
        matches!(
            self.kind.as_str(),
            "Deployment"
                | "StatefulSet"
                | "DaemonSet"
                | "ReplicaSet"
                | "Job"
                | "CronJob"
                | "Pod"
        )
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
        let raw = serde_yaml::to_string(&value).unwrap_or_default();
        let images = extract_images(&value);
        let match_labels = extract_match_labels(&value);
        out.push(K8sEntity {
            kind,
            name,
            raw,
            images,
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
    let mut labels = BTreeMap::new();
    if let Some(Value::Mapping(m)) = v
        .get("spec")
        .and_then(|s| s.get("selector"))
        .and_then(|s| s.get("matchLabels"))
        .map(|x| x.clone())
        .as_ref()
    {
        for (k, val) in m {
            if let (Some(k), Some(val)) = (k.as_str(), val.as_str()) {
                labels.insert(k.to_string(), val.to_string());
            }
        }
    }
    // Bare Pods: use their own labels as the selector.
    if labels.is_empty() {
        if let Some(Value::Mapping(m)) = v
            .get("metadata")
            .and_then(|md| md.get("labels"))
            .map(|x| x.clone())
            .as_ref()
        {
            for (k, val) in m {
                if let (Some(k), Some(val)) = (k.as_str(), val.as_str()) {
                    labels.insert(k.to_string(), val.to_string());
                }
            }
        }
    }
    labels
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

        let svc = &entities[1];
        assert_eq!(svc.kind, "Service");
        assert!(!svc.is_workload());
        assert!(svc.images.is_empty());
    }

    #[test]
    fn skips_non_resource_docs() {
        let entities = parse_yaml("---\nfoo: bar\n---\n# just a comment\n");
        assert!(entities.is_empty());
    }
}
