//! Minimal in-memory API object store.
//!
//! Mirrors the slice of Tilt's apiserver (`pkg/apis` + the controller-runtime
//! client) that controllers and CLI verbs depend on: typed objects keyed by
//! `(kind, namespace, name)` with monotonic `resourceVersion` bumping, `uid`
//! assignment, and a watch stream of add/modify/delete events.
//!
//! This is deliberately storage-only — it does not run reconcilers. It is the
//! foundation the object-backed CLI (`get` / `describe` / `delete` / `wait`)
//! and the future controllers build on, replacing the ad-hoc maps that
//! currently materialize only the frontend-facing `UIResource`/`UIButton`
//! objects.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{json, Value};
use tokio::sync::broadcast;

/// A stored object plus the bookkeeping the apiserver maintains for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObject {
    pub kind: String,
    pub namespace: String,
    pub name: String,
    pub uid: String,
    pub resource_version: u64,
    /// The full object JSON, with `kind`/`apiVersion`/`metadata` kept in sync
    /// with the bookkeeping fields above.
    pub object: Value,
}

/// A watch event, mirroring Kubernetes watch semantics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectEvent {
    Added(StoredObject),
    Modified(StoredObject),
    Deleted(StoredObject),
}

/// Errors mirroring the apiserver status responses the CLI maps to exit codes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// `create` on a key that already exists.
    AlreadyExists { kind: String, name: String },
    /// `replace`/`patch` on a key that does not exist.
    NotFound { kind: String, name: String },
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::AlreadyExists { kind, name } => {
                write!(f, "{kind} \"{name}\" already exists")
            }
            ApiError::NotFound { kind, name } => write!(f, "{kind} \"{name}\" not found"),
        }
    }
}

impl std::error::Error for ApiError {}

/// `tilt.dev` is the API group for Tilt's core objects; the version is
/// `v1alpha1`, so `apiVersion` is `tilt.dev/v1alpha1`.
pub const API_VERSION: &str = "tilt.dev/v1alpha1";

/// The spec fields Starling populates for each `tilt.dev/v1alpha1` kind. Backs
/// `starling explain` and the per-kind OpenAPI schema endpoint. `None` for an
/// unknown kind.
pub fn spec_fields(kind: &str) -> Option<&'static [&'static str]> {
    let fields: &[&str] = match kind {
        "KubernetesApply" => &["yaml", "applyCmd", "imageMaps"],
        "KubernetesDiscovery" => &["selectors"],
        "PodLogStream" => &["selector"],
        "FileWatch" => &["watchedPaths", "ignores"],
        "Cmd" => &["args", "dir", "env"],
        "PortForward" => &["forwards"],
        "LiveUpdate" => &["syncs", "execs", "stopPaths", "restart"],
        "DockerImage" => &["ref", "context"],
        "ImageMap" => &["selector"],
        "CmdImage" => &["ref", "args"],
        "DockerComposeService" => &["service", "project"],
        "DockerComposeLogStream" => &["service"],
        "ToggleButton" => &["location", "on", "off"],
        "ConfigMap" => &["data"],
        "Tiltfile" => &["path"],
        "Session" => &["targets"],
        "ExtensionRepo" => &["url"],
        "Extension" => &["repoName", "repoPath"],
        "UIButton" => &["text", "location"],
        _ => return None,
    };
    Some(fields)
}

/// The JSON-schema `type` for a known spec field name (used to give the
/// generated schemas real field types rather than bare `{}`).
pub fn field_type(field: &str) -> &'static str {
    match field {
        // arrays
        "args" | "imageMaps" | "selectors" | "watchedPaths" | "ignores" | "forwards" | "syncs"
        | "execs" | "stopPaths" | "env" | "targets" => "array",
        // booleans
        "restart" => "boolean",
        // objects
        "data" | "project" | "location" | "on" | "off" | "selector" | "applyCmd" => "object",
        // strings (yaml/dir/ref/context/service/path/url/repoName/repoPath/text/...)
        _ => "string",
    }
}

/// Build the `spec` JSON-schema object for a kind: each populated field typed
/// via [`field_type`].
pub fn spec_schema(kind: &str) -> Value {
    let props: serde_json::Map<String, Value> = spec_fields(kind)
        .unwrap_or(&[])
        .iter()
        .map(|f| (f.to_string(), json!({ "type": field_type(f) })))
        .collect();
    json!({ "type": "object", "properties": props })
}

/// Generate an OpenAPI 3.0 document describing the `tilt.dev/v1alpha1` object
/// types Starling serves: one component schema per kind (with `apiVersion`,
/// `kind`, `metadata`, and a `spec` object listing the populated fields). Backs
/// `GET /openapi.json` and `starling dump openapi`.
pub fn openapi_document() -> Value {
    let mut schemas = serde_json::Map::new();
    for kind in known_kinds() {
        schemas.insert(
            kind.to_string(),
            json!({
                "type": "object",
                "properties": {
                    "apiVersion": { "type": "string" },
                    "kind": { "type": "string" },
                    "metadata": { "type": "object" },
                    "spec": spec_schema(kind),
                },
            }),
        );
    }
    json!({
        "openapi": "3.0.0",
        "info": { "title": "Starling tilt.dev API", "version": "v1alpha1" },
        "components": { "schemas": schemas },
    })
}

/// All `tilt.dev/v1alpha1` kinds Starling knows, sorted.
pub fn known_kinds() -> Vec<&'static str> {
    let mut kinds = vec![
        "Cmd",
        "CmdImage",
        "ConfigMap",
        "DockerComposeLogStream",
        "DockerComposeService",
        "DockerImage",
        "Extension",
        "ExtensionRepo",
        "FileWatch",
        "ImageMap",
        "KubernetesApply",
        "KubernetesDiscovery",
        "LiveUpdate",
        "PodLogStream",
        "PortForward",
        "Session",
        "Tiltfile",
        "ToggleButton",
        "UIButton",
    ];
    kinds.sort();
    kinds
}

type Key = (String, String, String); // (kind, namespace, name)

struct Inner {
    objects: BTreeMap<Key, StoredObject>,
    /// Global monotonic counter, like the etcd revision Kubernetes exposes as
    /// `resourceVersion`. Every mutating op increments it.
    resource_version: u64,
    next_uid: u64,
}

/// An in-memory, thread-safe store of API objects.
pub struct ApiObjectStore {
    inner: Mutex<Inner>,
    events: broadcast::Sender<ObjectEvent>,
}

impl Default for ApiObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ApiObjectStore {
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(1024);
        ApiObjectStore {
            inner: Mutex::new(Inner {
                objects: BTreeMap::new(),
                resource_version: 0,
                next_uid: 0,
            }),
            events,
        }
    }

    /// Subscribe to the watch stream. Late subscribers only see future events
    /// (callers that need a snapshot should `list` first, like a k8s informer).
    pub fn watch(&self) -> broadcast::Receiver<ObjectEvent> {
        self.events.subscribe()
    }

    /// Create a new object. Errors if `(kind, namespace, name)` already exists.
    /// Assigns a fresh `uid` and `resourceVersion` and stamps the metadata.
    pub fn create(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
        object: Value,
    ) -> Result<StoredObject, ApiError> {
        let mut inner = self.inner.lock().unwrap();
        let key = (kind.to_string(), namespace.to_string(), name.to_string());
        if inner.objects.contains_key(&key) {
            return Err(ApiError::AlreadyExists {
                kind: kind.to_string(),
                name: name.to_string(),
            });
        }
        inner.next_uid += 1;
        let uid = format!("uid-{}", inner.next_uid);
        inner.resource_version += 1;
        let rv = inner.resource_version;
        let stored = stamp(kind, namespace, name, &uid, rv, object);
        inner.objects.insert(key, stored.clone());
        let _ = self.events.send(ObjectEvent::Added(stored.clone()));
        Ok(stored)
    }

    /// Replace an existing object's contents, bumping `resourceVersion` and
    /// preserving its `uid`. Errors if it does not exist.
    pub fn replace(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
        object: Value,
    ) -> Result<StoredObject, ApiError> {
        let mut inner = self.inner.lock().unwrap();
        let key = (kind.to_string(), namespace.to_string(), name.to_string());
        let Some(existing) = inner.objects.get(&key) else {
            return Err(ApiError::NotFound {
                kind: kind.to_string(),
                name: name.to_string(),
            });
        };
        let uid = existing.uid.clone();
        inner.resource_version += 1;
        let rv = inner.resource_version;
        let stored = stamp(kind, namespace, name, &uid, rv, object);
        inner.objects.insert(key, stored.clone());
        let _ = self.events.send(ObjectEvent::Modified(stored.clone()));
        Ok(stored)
    }

    /// Create the object if absent, otherwise replace it (server-side apply
    /// semantics). Always returns the stored object.
    pub fn apply(&self, kind: &str, namespace: &str, name: &str, object: Value) -> StoredObject {
        match self.replace(kind, namespace, name, object.clone()) {
            Ok(stored) => stored,
            Err(ApiError::NotFound { .. }) => self
                .create(kind, namespace, name, object)
                .expect("create after NotFound cannot conflict"),
            Err(ApiError::AlreadyExists { .. }) => {
                unreachable!("replace never returns AlreadyExists")
            }
        }
    }

    /// Apply an RFC 7386 JSON merge patch to an existing object, bumping
    /// `resourceVersion` and preserving its `uid`. Errors if it does not exist.
    pub fn patch(
        &self,
        kind: &str,
        namespace: &str,
        name: &str,
        patch: Value,
    ) -> Result<StoredObject, ApiError> {
        let mut inner = self.inner.lock().unwrap();
        let key = (kind.to_string(), namespace.to_string(), name.to_string());
        let Some(existing) = inner.objects.get(&key) else {
            return Err(ApiError::NotFound {
                kind: kind.to_string(),
                name: name.to_string(),
            });
        };
        let uid = existing.uid.clone();
        let mut merged = existing.object.clone();
        merge_patch(&mut merged, &patch);
        inner.resource_version += 1;
        let rv = inner.resource_version;
        let stored = stamp(kind, namespace, name, &uid, rv, merged);
        inner.objects.insert(key, stored.clone());
        let _ = self.events.send(ObjectEvent::Modified(stored.clone()));
        Ok(stored)
    }

    pub fn get(&self, kind: &str, namespace: &str, name: &str) -> Option<StoredObject> {
        let inner = self.inner.lock().unwrap();
        inner
            .objects
            .get(&(kind.to_string(), namespace.to_string(), name.to_string()))
            .cloned()
    }

    /// All objects of a kind, sorted by `(namespace, name)`.
    pub fn list(&self, kind: &str) -> Vec<StoredObject> {
        let inner = self.inner.lock().unwrap();
        inner
            .objects
            .iter()
            .filter(|((k, _, _), _)| k == kind)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Every object across all kinds, sorted by `(kind, namespace, name)`.
    pub fn all(&self) -> Vec<StoredObject> {
        let inner = self.inner.lock().unwrap();
        inner.objects.values().cloned().collect()
    }

    /// The distinct kinds currently held, sorted — backs `api-resources`.
    pub fn kinds(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let mut kinds: Vec<String> = inner.objects.keys().map(|(k, _, _)| k.clone()).collect();
        kinds.dedup();
        kinds
    }

    /// Delete an object, returning the removed copy if it existed.
    pub fn delete(&self, kind: &str, namespace: &str, name: &str) -> Option<StoredObject> {
        let mut inner = self.inner.lock().unwrap();
        let key = (kind.to_string(), namespace.to_string(), name.to_string());
        let removed = inner.objects.remove(&key);
        if let Some(stored) = &removed {
            inner.resource_version += 1;
            let _ = self.events.send(ObjectEvent::Deleted(stored.clone()));
        }
        removed
    }
}

/// Apply an RFC 7386 JSON merge patch in place: object members are merged
/// recursively, a `null` value deletes the key, and any non-object patch
/// replaces the target wholesale.
fn merge_patch(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(target_map), Value::Object(patch_map)) => {
            for (key, patch_value) in patch_map {
                if patch_value.is_null() {
                    target_map.remove(key);
                } else {
                    merge_patch(
                        target_map.entry(key.clone()).or_insert(Value::Null),
                        patch_value,
                    );
                }
            }
        }
        (target, patch) => *target = patch.clone(),
    }
}

/// Stamp `kind`/`apiVersion`/`metadata` into the object JSON so the stored
/// `Value` is internally consistent with the store's bookkeeping.
fn stamp(
    kind: &str,
    namespace: &str,
    name: &str,
    uid: &str,
    resource_version: u64,
    mut object: Value,
) -> StoredObject {
    if !object.is_object() {
        object = json!({});
    }
    let map = object.as_object_mut().expect("ensured object above");
    map.insert("kind".to_string(), json!(kind));
    map.insert("apiVersion".to_string(), json!(API_VERSION));
    let metadata = map.entry("metadata").or_insert_with(|| json!({}));
    if !metadata.is_object() {
        *metadata = json!({});
    }
    let meta = metadata.as_object_mut().expect("ensured object above");
    meta.insert("name".to_string(), json!(name));
    meta.insert("namespace".to_string(), json!(namespace));
    meta.insert("uid".to_string(), json!(uid));
    meta.insert(
        "resourceVersion".to_string(),
        json!(resource_version.to_string()),
    );
    StoredObject {
        kind: kind.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
        uid: uid.to_string(),
        resource_version,
        object,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openapi_document_has_per_kind_schemas() {
        let doc = openapi_document();
        assert_eq!(doc["openapi"], json!("3.0.0"));
        let schemas = &doc["components"]["schemas"];
        // One schema per known kind, with the spec fields as properties.
        for kind in known_kinds() {
            assert!(schemas.get(kind).is_some(), "missing schema for {kind}");
        }
        // Fields are typed (not bare {}).
        assert_eq!(
            schemas["KubernetesApply"]["properties"]["spec"]["properties"]["yaml"]["type"],
            json!("string")
        );
        assert_eq!(
            schemas["Cmd"]["properties"]["spec"]["properties"]["args"]["type"],
            json!("array")
        );
        assert_eq!(
            schemas["LiveUpdate"]["properties"]["spec"]["properties"]["restart"]["type"],
            json!("boolean")
        );
        assert_eq!(
            schemas["ConfigMap"]["properties"]["spec"]["properties"]["data"]["type"],
            json!("object")
        );
    }

    #[test]
    fn create_get_list_delete_roundtrip() {
        let store = ApiObjectStore::new();
        store
            .create(
                "KubernetesApply",
                "default",
                "web",
                json!({"spec": {"yaml": "a"}}),
            )
            .unwrap();
        store
            .create(
                "KubernetesApply",
                "default",
                "api",
                json!({"spec": {"yaml": "b"}}),
            )
            .unwrap();

        let got = store.get("KubernetesApply", "default", "web").unwrap();
        assert_eq!(got.name, "web");
        assert_eq!(got.uid, "uid-1");
        // Metadata is stamped into the stored JSON.
        assert_eq!(got.object["metadata"]["name"], json!("web"));
        assert_eq!(got.object["metadata"]["uid"], json!("uid-1"));
        assert_eq!(got.object["apiVersion"], json!("tilt.dev/v1alpha1"));

        // list is sorted by (namespace, name).
        let list = store.list("KubernetesApply");
        assert_eq!(
            list.iter().map(|o| o.name.as_str()).collect::<Vec<_>>(),
            vec!["api", "web"]
        );

        let removed = store.delete("KubernetesApply", "default", "web").unwrap();
        assert_eq!(removed.name, "web");
        assert!(store.get("KubernetesApply", "default", "web").is_none());
        assert_eq!(store.list("KubernetesApply").len(), 1);
    }

    #[test]
    fn resource_version_is_monotonic_and_create_is_unique() {
        let store = ApiObjectStore::new();
        let a = store.create("Cmd", "default", "x", json!({})).unwrap();
        let b = store.create("Cmd", "default", "y", json!({})).unwrap();
        assert!(b.resource_version > a.resource_version);

        // Duplicate create errors.
        let err = store.create("Cmd", "default", "x", json!({})).unwrap_err();
        assert_eq!(
            err,
            ApiError::AlreadyExists {
                kind: "Cmd".into(),
                name: "x".into()
            }
        );

        // replace bumps resourceVersion but keeps uid.
        let r = store
            .replace("Cmd", "default", "x", json!({"spec": {"args": ["echo"]}}))
            .unwrap();
        assert_eq!(r.uid, a.uid);
        assert!(r.resource_version > b.resource_version);
        assert_eq!(r.object["spec"]["args"], json!(["echo"]));

        // replace on a missing object errors.
        assert_eq!(
            store
                .replace("Cmd", "default", "missing", json!({}))
                .unwrap_err(),
            ApiError::NotFound {
                kind: "Cmd".into(),
                name: "missing".into()
            }
        );
    }

    #[test]
    fn apply_creates_then_replaces() {
        let store = ApiObjectStore::new();
        let first = store.apply("FileWatch", "default", "fw", json!({"spec": {"a": 1}}));
        assert_eq!(first.resource_version, 1);
        let second = store.apply("FileWatch", "default", "fw", json!({"spec": {"a": 2}}));
        // Same object (uid preserved), new resourceVersion + contents.
        assert_eq!(second.uid, first.uid);
        assert!(second.resource_version > first.resource_version);
        assert_eq!(second.object["spec"]["a"], json!(2));
        assert_eq!(store.list("FileWatch").len(), 1);
    }

    #[test]
    fn patch_merges_and_deletes_keys() {
        let store = ApiObjectStore::new();
        store
            .create(
                "Cmd",
                "default",
                "x",
                json!({"spec": {"args": ["a"], "dir": "/tmp", "env": ["K=v"]}}),
            )
            .unwrap();
        // Merge: replace args, delete dir (null), keep env.
        let patched = store
            .patch(
                "Cmd",
                "default",
                "x",
                json!({"spec": {"args": ["b"], "dir": null}}),
            )
            .unwrap();
        assert_eq!(patched.object["spec"]["args"], json!(["b"]));
        assert_eq!(patched.object["spec"]["env"], json!(["K=v"]));
        assert!(patched.object["spec"].get("dir").is_none());

        // patch on a missing object errors.
        assert_eq!(
            store
                .patch("Cmd", "default", "missing", json!({}))
                .unwrap_err(),
            ApiError::NotFound {
                kind: "Cmd".into(),
                name: "missing".into()
            }
        );
    }

    #[tokio::test]
    async fn watch_emits_add_modify_delete() {
        let store = ApiObjectStore::new();
        let mut rx = store.watch();

        store.create("Cmd", "default", "x", json!({})).unwrap();
        store
            .replace("Cmd", "default", "x", json!({"spec": {}}))
            .unwrap();
        store.delete("Cmd", "default", "x");

        assert!(matches!(rx.recv().await.unwrap(), ObjectEvent::Added(o) if o.name == "x"));
        assert!(matches!(rx.recv().await.unwrap(), ObjectEvent::Modified(o) if o.name == "x"));
        assert!(matches!(rx.recv().await.unwrap(), ObjectEvent::Deleted(o) if o.name == "x"));
    }
}
