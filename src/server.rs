//! HTTP/WebSocket server exposing the API surface the web frontend expects.
//!
//! Routes mirror `internal/hud/server/server.go`:
//!   GET  /api/websocket_token         CSRF token (text) for the websocket
//!   GET  /ws/view                      websocket streaming `View` JSON
//!   GET  /api/view                     full `View` as JSON
//!   GET  /api/snapshot/{id}            a `Snapshot` wrapping the view
//!   POST /api/trigger                  queue a build for a manifest
//!   POST /api/override/trigger_mode    set trigger mode on manifests
//!   POST /api/set_tiltfile_args        replace Starlingfile args (route name fixed by frontend)
//!   POST /api/analytics                analytics events (accepted, no-op)
//!   POST /api/analytics_opt            analytics opt in/out (accepted, no-op)
//!   *                                  static frontend assets (SPA fallback)

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use tower_http::services::{ServeDir, ServeFile};

use crate::api::webview::Snapshot;
use crate::store::{Store, TriggerError};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    /// Generated once at startup; the websocket requires `?csrf=<token>`.
    pub csrf_token: String,
    /// Kubernetes-style API object store (KubernetesApply, Tiltfile, ...).
    pub api_objects: Arc<crate::api::store::ApiObjectStore>,
}

pub fn router(state: AppState, web_dir: &str) -> Router {
    // Serve the built CRA frontend, falling back to index.html for client-side
    // routes (e.g. /r/frontend, /overview).
    let index = format!("{web_dir}/index.html");
    let static_service = ServeDir::new(web_dir).fallback(ServeFile::new(index));

    Router::new()
        .route("/api/websocket_token", get(websocket_token))
        .route("/ws/view", get(view_websocket))
        .route("/api/view", get(view_json))
        .route("/api/snapshot/:id", get(snapshot_json))
        .route("/api/trigger", post(handle_trigger))
        .route(
            "/api/override/trigger_mode",
            post(handle_override_trigger_mode),
        )
        .route("/api/set_tiltfile_args", post(handle_set_tiltfile_args))
        .route("/api/analytics", post(handle_analytics))
        .route("/api/analytics_opt", post(handle_analytics_opt))
        // Generated OpenAPI document for the tilt.dev/v1alpha1 types.
        .route("/openapi.json", get(openapi_json))
        // API discovery: the APIResourceList for the tilt.dev/v1alpha1 group.
        .route("/proxy/apis/tilt.dev/v1alpha1", get(api_resource_list))
        // OpenAPI-style object schema for a single kind (`explain`).
        .route(
            "/proxy/apis/tilt.dev/v1alpha1/:kind/schema",
            get(object_schema),
        )
        // Minimal apiserver surface the web UI writes to (UIButtons).
        .route("/proxy/apis/tilt.dev/v1alpha1/uibuttons", get(list_buttons))
        .route(
            "/proxy/apis/tilt.dev/v1alpha1/uibuttons/:name",
            get(get_button),
        )
        .route(
            "/proxy/apis/tilt.dev/v1alpha1/uibuttons/:name/status",
            axum::routing::put(put_button_status),
        )
        // Generic read/watch surface over the API object store. (Kept under
        // /api/v1alpha1 to avoid colliding with the literal uibuttons routes
        // above; reads are namespace-default and keyed by object Kind.)
        .route("/api/v1alpha1/_kinds", get(list_kinds))
        .route("/api/v1alpha1/_logs", get(read_logs))
        .route("/api/v1alpha1/_watch", get(watch_objects))
        .route("/api/v1alpha1/:kind", get(list_objects).post(create_object))
        .route(
            "/api/v1alpha1/:kind/:name",
            get(get_object)
                .delete(delete_object)
                .put(replace_object)
                .patch(patch_object),
        )
        // Object-driven reconcile: drive a KubernetesApply/KubernetesDiscovery
        // object against the cluster (apply / pod discovery) on demand.
        .route(
            "/api/v1alpha1/:kind/:name/reconcile",
            post(reconcile_object),
        )
        .fallback_service(static_service)
        .with_state(state)
}

// -- /api/v1alpha1 (generic API object store reads) --------------------------

/// List every object of a Kind, as a Kubernetes-style `<Kind>List`.
async fn list_objects(
    State(state): State<AppState>,
    Path(kind): Path<String>,
) -> impl IntoResponse {
    let items: Vec<_> = state
        .api_objects
        .list(&kind)
        .into_iter()
        .map(|o| o.object)
        .collect();
    Json(json!({
        "apiVersion": crate::api::store::API_VERSION,
        "kind": format!("{kind}List"),
        "metadata": {},
        "items": items,
    }))
}

/// Get a single object by Kind + name (namespace defaults to "default").
async fn get_object(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Response {
    match state.api_objects.get(&kind, "default", &name) {
        Some(o) => Json(o.object).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("{kind} \"{name}\" not found\n"),
        )
            .into_response(),
    }
}

/// The distinct object Kinds currently held (backs an `api-resources`-style view).
async fn list_kinds(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({ "kinds": state.api_objects.kinds() }))
}

#[derive(Deserialize)]
struct LogParams {
    /// Restrict to one resource span.
    resource: Option<String>,
    /// Minimum level (DEBUG/INFO/WARN/ERROR).
    level: Option<String>,
    /// Only lines at/after this RFC3339 timestamp (inclusive).
    since: Option<String>,
    /// Only lines strictly before this RFC3339 timestamp.
    until: Option<String>,
}

/// Structured log read: the runtime log reader, filtered by resource span and
/// minimum level. Returns `{items: [{span, level, time, text}]}`.
async fn read_logs(
    State(state): State<AppState>,
    Query(params): Query<LogParams>,
) -> impl IntoResponse {
    let lines = state.store.query_logs(&crate::store::LogQuery {
        span: params.resource,
        min_level: params.level,
        since: params.since,
        until: params.until,
    });
    let items: Vec<_> = lines
        .into_iter()
        .map(|l| json!({ "span": l.span, "level": l.level, "time": l.time, "text": l.text }))
        .collect();
    Json(json!({ "items": items }))
}

/// Create an object of `kind` from a posted JSON body (name taken from
/// `metadata.name`). 201 on success, 409 if it already exists, 400 if unnamed.
async fn create_object(
    State(state): State<AppState>,
    Path(kind): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return (StatusCode::BAD_REQUEST, "metadata.name is required\n").into_response();
    }
    match state.api_objects.create(&kind, "default", &name, body) {
        Ok(stored) => (StatusCode::CREATED, Json(stored.object)).into_response(),
        Err(e @ crate::api::store::ApiError::AlreadyExists { .. }) => {
            (StatusCode::CONFLICT, format!("{e}\n")).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, format!("{e}\n")).into_response(),
    }
}

/// Delete an object by Kind + name. 200 with the deleted object, or 404.
async fn delete_object(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Response {
    match state.api_objects.delete(&kind, "default", &name) {
        Some(o) => Json(o.object).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("{kind} \"{name}\" not found\n"),
        )
            .into_response(),
    }
}

/// Replace an object's contents (PUT). 200 on success, 404 if it does not exist.
async fn replace_object(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    match state.api_objects.replace(&kind, "default", &name, body) {
        Ok(stored) => Json(stored.object).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e}\n")).into_response(),
    }
}

/// Reconcile an object against the cluster on demand (the object-driven,
/// cluster-backed controllers): `KubernetesApply` applies its YAML,
/// `KubernetesDiscovery` lists matching pods. Returns the updated object.
async fn reconcile_object(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
) -> Response {
    let result = if kind.eq_ignore_ascii_case("KubernetesApply") {
        crate::engine::reconcile_kubernetes_apply(&state.api_objects, &name).await
    } else if kind.eq_ignore_ascii_case("KubernetesDiscovery") {
        crate::engine::reconcile_kubernetes_discovery(&state.api_objects, &name).await
    } else if kind.eq_ignore_ascii_case("PodWatch") {
        // Pod-watch is the per-pod-detail view of a KubernetesDiscovery object.
        crate::engine::reconcile_pod_watch(&state.api_objects, &name).await
    } else if kind.eq_ignore_ascii_case("PodLogStream") {
        crate::engine::reconcile_pod_log_stream(&state.api_objects, &state.store, &name).await
    } else if kind.eq_ignore_ascii_case("DockerComposeService") {
        crate::engine::reconcile_docker_compose_service(&state.api_objects, &name).await
    } else if kind.eq_ignore_ascii_case("PortForward") {
        crate::engine::reconcile_port_forward(&state.api_objects, &name).await
    } else if kind.eq_ignore_ascii_case("LiveUpdate") {
        crate::engine::reconcile_live_update(&state.api_objects, &name).await
    } else {
        return (
            StatusCode::BAD_REQUEST,
            format!(
                "reconcile is supported for KubernetesApply/KubernetesDiscovery/PodWatch/PodLogStream/PortForward/LiveUpdate/DockerComposeService, not {kind}\n"
            ),
        )
            .into_response();
    };
    match result {
        Ok(()) => {
            // Look the object back up (under its real kind) to return current state.
            // PodWatch is an alias controller over the KubernetesDiscovery object.
            let lookup_kind = if kind.eq_ignore_ascii_case("PodWatch") {
                "KubernetesDiscovery"
            } else {
                kind.as_str()
            };
            let resolved = crate::api::store::known_kinds()
                .into_iter()
                .find(|k| k.eq_ignore_ascii_case(lookup_kind))
                .unwrap_or(lookup_kind);
            let obj = state.api_objects.get(resolved, "default", &name);
            Json(obj.map(|o| o.object).unwrap_or_else(|| json!({}))).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{e}\n")).into_response(),
    }
}

/// Apply an RFC 7386 merge patch (PATCH). 200 on success, 404 if it does not exist.
async fn patch_object(
    State(state): State<AppState>,
    Path((kind, name)): Path<(String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    match state.api_objects.patch(&kind, "default", &name, body) {
        Ok(stored) => Json(stored.object).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, format!("{e}\n")).into_response(),
    }
}

/// Stream object add/modify/delete events as Server-Sent Events, mirroring a
/// Kubernetes watch. Each event's data is `{type, object}`.
async fn watch_objects(
    State(state): State<AppState>,
) -> axum::response::sse::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use crate::api::store::ObjectEvent;
    use axum::response::sse::Event;
    use tokio::sync::broadcast::error::RecvError;

    let rx = state.api_objects.watch();
    let stream = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let (typ, stored) = match event {
                        ObjectEvent::Added(o) => ("ADDED", o),
                        ObjectEvent::Modified(o) => ("MODIFIED", o),
                        ObjectEvent::Deleted(o) => ("DELETED", o),
                    };
                    let data = json!({ "type": typ, "object": stored.object });
                    let sse = Event::default()
                        .json_data(data)
                        .unwrap_or_else(|_| Event::default());
                    return Some((Ok(sse), rx));
                }
                // Slow consumer: skip dropped events and keep streaming.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    });
    axum::response::sse::Sse::new(stream)
}

/// Kubernetes-style API discovery: `GET /proxy/apis/tilt.dev/v1alpha1` returns
/// the `APIResourceList` for the group (the slice of `tilt api-resources` /
/// `kubectl api-resources` discovery clients read).
async fn api_resource_list() -> impl IntoResponse {
    let resources: Vec<_> = crate::api::store::known_kinds()
        .iter()
        .map(|kind| {
            json!({
                "name": format!("{}s", kind.to_lowercase()),
                "singularName": kind.to_lowercase(),
                "kind": kind,
                // tilt.dev objects are cluster-scoped (no namespace).
                "namespaced": false,
                "verbs": ["get", "list", "watch", "create", "update", "patch", "delete"],
            })
        })
        .collect();
    Json(json!({
        "kind": "APIResourceList",
        "apiVersion": "v1",
        "groupVersion": crate::api::store::API_VERSION,
        "resources": resources,
    }))
}

/// The generated OpenAPI 3.0 document for the `tilt.dev/v1alpha1` types.
async fn openapi_json() -> impl IntoResponse {
    Json(crate::api::store::openapi_document())
}

/// An OpenAPI-style object schema for one kind: `spec` is an object whose known
/// properties are the fields Starling populates. Backs `tilt explain`.
async fn object_schema(Path(kind): Path<String>) -> Response {
    let resolved = crate::api::store::known_kinds()
        .into_iter()
        .find(|k| k.eq_ignore_ascii_case(&kind));
    let Some(kind) = resolved else {
        return (StatusCode::NOT_FOUND, format!("unknown kind {kind:?}\n")).into_response();
    };
    Json(json!({
        "kind": kind,
        "apiVersion": crate::api::store::API_VERSION,
        "schema": {
            "type": "object",
            "properties": {
                "apiVersion": { "type": "string" },
                "kind": { "type": "string" },
                "metadata": { "type": "object" },
                "spec": crate::api::store::spec_schema(kind),
            },
        },
    }))
    .into_response()
}

// -- /proxy/apis/.../uibuttons (web UI button writes) ------------------------

async fn list_buttons(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "apiVersion": "tilt.dev/v1alpha1",
        "kind": "UIButtonList",
        "metadata": {},
        "items": state.store.list_buttons(),
    }))
}

async fn get_button(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.store.get_button(&name) {
        Some(b) => Json(b).into_response(),
        None => (StatusCode::NOT_FOUND, "no such uibutton\n").into_response(),
    }
}

/// PUT a UIButton's status: stamp lastClickedAt, store inputs, and dispatch the
/// button's effect (DisableToggle flips the target resource's disable state).
async fn put_button_status(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<crate::api::v1alpha1::UIButton>,
) -> Response {
    let inputs = body
        .status
        .as_ref()
        .map(|s| s.inputs.clone())
        .unwrap_or_default();
    let Some(button) = state.store.record_button_click(&name, inputs) else {
        return (StatusCode::NOT_FOUND, "no such uibutton\n").into_response();
    };

    // Dispatch known button types.
    let btype = button
        .metadata
        .as_ref()
        .and_then(|m| m.annotations.as_ref())
        .and_then(|a| a.get("tilt.dev/uibutton-type"))
        .cloned()
        .unwrap_or_default();
    let target = button
        .spec
        .as_ref()
        .map(|s| s.location.component_id.clone())
        .unwrap_or_default();
    if btype == "DisableToggle" && !target.is_empty() {
        let now_disabled = !state.store.is_resource_disabled(&target);
        state.store.set_resource_disabled(&target, now_disabled);
        state.store.append_log(
            Some(&target),
            "INFO",
            &format!(
                "{} via web UI\n",
                if now_disabled { "Disabled" } else { "Enabled" }
            ),
        );
    }

    Json(button).into_response()
}

// -- GET /api/websocket_token ------------------------------------------------

async fn websocket_token(State(state): State<AppState>) -> impl IntoResponse {
    state.csrf_token.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tower::ServiceExt; // for `oneshot`

    fn test_state() -> AppState {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        AppState {
            store: Arc::new(Store::new(tx)),
            csrf_token: "t".to_string(),
            api_objects: Arc::new(crate::api::store::ApiObjectStore::new()),
        }
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn generic_api_routes_list_get_and_404() {
        let state = test_state();
        state.api_objects.apply(
            "KubernetesApply",
            "default",
            "web",
            json!({"spec": {"yaml": "x"}}),
        );
        let app = router(state, "web/build");

        // list
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1alpha1/KubernetesApply")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["kind"], json!("KubernetesApplyList"));
        assert_eq!(body["items"][0]["metadata"]["name"], json!("web"));

        // get
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1alpha1/KubernetesApply/web")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // missing -> 404
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1alpha1/KubernetesApply/nope")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn generic_api_routes_create_conflict_and_delete() {
        let state = test_state();
        let app = router(state, "web/build");

        let create = |body: Value| {
            Request::builder()
                .method("POST")
                .uri("/api/v1alpha1/Cmd")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };

        // create -> 201
        let resp = app
            .clone()
            .oneshot(create(
                json!({"metadata": {"name": "c1"}, "spec": {"args": ["echo"]}}),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // duplicate create -> 409
        let resp = app
            .clone()
            .oneshot(create(json!({"metadata": {"name": "c1"}})))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // unnamed -> 400
        let resp = app
            .clone()
            .oneshot(create(json!({"spec": {}})))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // delete -> 200, then 404
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1alpha1/Cmd/c1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1alpha1/Cmd/c1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn api_discovery_lists_tilt_resources() {
        let app = router(test_state(), "web/build");
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/proxy/apis/tilt.dev/v1alpha1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["kind"], json!("APIResourceList"));
        assert_eq!(body["groupVersion"], json!("tilt.dev/v1alpha1"));
        let resources = body["resources"].as_array().unwrap();
        // KubernetesApply is registered with its lowercase-plural resource name.
        let ka = resources
            .iter()
            .find(|r| r["kind"] == json!("KubernetesApply"))
            .expect("KubernetesApply in discovery");
        assert_eq!(ka["name"], json!("kubernetesapplys"));
        assert_eq!(ka["namespaced"], json!(false));
    }

    #[tokio::test]
    async fn reconcile_route_rejects_unsupported_kind() {
        // The wrong-kind branch is validated without touching a cluster.
        let app = router(test_state(), "web/build");
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1alpha1/Cmd/x/reconcile")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn object_schema_route_returns_spec_fields() {
        let app = router(test_state(), "web/build");
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/proxy/apis/tilt.dev/v1alpha1/KubernetesApply/schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["kind"], json!("KubernetesApply"));
        assert!(body["schema"]["properties"]["spec"]["properties"]
            .get("yaml")
            .is_some());
    }

    #[tokio::test]
    async fn read_logs_route_filters_by_resource_and_level() {
        let state = test_state();
        state.store.append_log(Some("web"), "INFO", "hello\n");
        state.store.append_log(Some("web"), "ERROR", "boom\n");
        state.store.append_log(Some("api"), "INFO", "other\n");
        let app = router(state, "web/build");

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1alpha1/_logs?resource=web&level=ERROR")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        let items = body["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["span"], json!("web"));
        assert_eq!(items[0]["level"], json!("ERROR"));
    }

    #[tokio::test]
    async fn generic_api_routes_put_and_patch() {
        let state = test_state();
        state.api_objects.apply(
            "Cmd",
            "default",
            "c1",
            json!({"spec": {"args": ["a"], "dir": "/tmp"}}),
        );
        let app = router(state, "web/build");

        // PUT replaces wholesale.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1alpha1/Cmd/c1")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"spec": {"args": ["b"]}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["spec"]["args"], json!(["b"]));
        assert!(body["spec"].get("dir").is_none());

        // PATCH merges (delete args via null, add env).
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/api/v1alpha1/Cmd/c1")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"spec": {"args": null, "env": ["K=v"]}}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert!(body["spec"].get("args").is_none());
        assert_eq!(body["spec"]["env"], json!(["K=v"]));

        // PUT on a missing object -> 404.
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/v1alpha1/Cmd/missing")
                    .header("content-type", "application/json")
                    .body(Body::from(json!({"spec": {}}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}

// -- GET /api/view -----------------------------------------------------------

async fn view_json(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.store.full_view())
}

// -- GET /api/snapshot/:id ---------------------------------------------------

async fn snapshot_json(
    State(state): State<AppState>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    let view = state.store.full_view();
    Json(Snapshot {
        created_at: view.tilt_start_time.clone(),
        view: Some(view),
        ..Default::default()
    })
}

// -- GET /ws/view (websocket) ------------------------------------------------

async fn view_websocket(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // CSRF check: the frontend fetches /api/websocket_token and passes it here.
    if params.get("csrf").map(String::as_str) != Some(state.csrf_token.as_str()) {
        return (StatusCode::FORBIDDEN, "bad csrf token").into_response();
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    // 1. Send the complete view immediately.
    let full = state.store.full_view();
    if send_view(&mut socket, &full).await.is_err() {
        return;
    }
    let mut checkpoint = state.store.log_len();

    // 2. Stream deltas on each change notification, debounced ~200ms.
    let mut rx = state.store.subscribe();
    loop {
        tokio::select! {
            // Drain any inbound frames (acks, pings, close).
            inbound = socket.recv() => {
                match inbound {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(_)) => break,
                    _ => continue,
                }
            }
            recv = rx.recv() => {
                match recv {
                    Ok(()) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    // Lagged: state changed more than we saw; just send latest.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
                // Debounce: coalesce a burst of changes into one delta.
                tokio::time::sleep(Duration::from_millis(200)).await;
                while rx.try_recv().is_ok() {}
                let (delta, next) = state.store.delta_view(checkpoint);
                checkpoint = next;
                if send_view(&mut socket, &delta).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn send_view(
    socket: &mut WebSocket,
    view: &crate::api::webview::View,
) -> Result<(), axum::Error> {
    let text = serde_json::to_string(view).expect("View serializes");
    socket.send(Message::Text(text)).await
}

// -- POST /api/trigger -------------------------------------------------------

#[derive(Deserialize)]
struct TriggerRequest {
    manifest_names: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)]
    build_reason: i32,
}

async fn handle_trigger(
    State(state): State<AppState>,
    Json(req): Json<TriggerRequest>,
) -> Response {
    if req.manifest_names.len() != 1 {
        return (
            StatusCode::BAD_REQUEST,
            "/api/trigger requires exactly one manifest name\n",
        )
            .into_response();
    }
    match state.store.trigger(&req.manifest_names[0]) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TriggerError::NotFound) => (
            StatusCode::NOT_FOUND,
            format!("no resource named {:?}\n", req.manifest_names[0]),
        )
            .into_response(),
        Err(TriggerError::EngineGone) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "build engine not running\n",
        )
            .into_response(),
        Err(TriggerError::BadMode) => StatusCode::BAD_REQUEST.into_response(),
    }
}

// -- POST /api/override/trigger_mode -----------------------------------------

#[derive(Deserialize)]
struct OverrideTriggerModeRequest {
    manifest_names: Vec<String>,
    trigger_mode: i32,
}

async fn handle_override_trigger_mode(
    State(state): State<AppState>,
    Json(req): Json<OverrideTriggerModeRequest>,
) -> Response {
    match state
        .store
        .set_trigger_mode(&req.manifest_names, req.trigger_mode)
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TriggerError::BadMode) => {
            (StatusCode::BAD_REQUEST, "invalid trigger mode\n").into_response()
        }
        Err(TriggerError::NotFound) => {
            (StatusCode::BAD_REQUEST, "unknown manifest\n").into_response()
        }
        Err(TriggerError::EngineGone) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "build engine not running\n",
        )
            .into_response(),
    }
}

// -- POST /api/set_tiltfile_args ---------------------------------------------

async fn handle_set_tiltfile_args(
    State(state): State<AppState>,
    Json(args): Json<Vec<String>>,
) -> Response {
    tracing::info!(?args, "set_tiltfile_args");
    match state.store.set_tiltfile_args(args) {
        Ok(()) => StatusCode::OK.into_response(),
        Err(TriggerError::EngineGone) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "build engine not running\n",
        )
            .into_response(),
        Err(_) => StatusCode::OK.into_response(),
    }
}

// -- POST /api/analytics & /api/analytics_opt --------------------------------

async fn handle_analytics(Json(_payload): Json<serde_json::Value>) -> Response {
    StatusCode::OK.into_response()
}

async fn handle_analytics_opt(Json(_payload): Json<serde_json::Value>) -> Response {
    StatusCode::OK.into_response()
}

/// Convenience for callers that want a JSON error body.
#[allow(dead_code)]
fn json_err(status: StatusCode, msg: &str) -> Response {
    (status, Json(json!({ "error": msg }))).into_response()
}
