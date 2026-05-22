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
        .route("/api/override/trigger_mode", post(handle_override_trigger_mode))
        .route("/api/set_tiltfile_args", post(handle_set_tiltfile_args))
        .route("/api/analytics", post(handle_analytics))
        .route("/api/analytics_opt", post(handle_analytics_opt))
        // Minimal apiserver surface the web UI writes to (UIButtons).
        .route("/proxy/apis/tilt.dev/v1alpha1/uibuttons", get(list_buttons))
        .route("/proxy/apis/tilt.dev/v1alpha1/uibuttons/:name", get(get_button))
        .route(
            "/proxy/apis/tilt.dev/v1alpha1/uibuttons/:name/status",
            axum::routing::put(put_button_status),
        )
        .fallback_service(static_service)
        .with_state(state)
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
        Err(TriggerError::EngineGone) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "build engine not running\n").into_response()
        }
    }
}

// -- POST /api/set_tiltfile_args ---------------------------------------------

async fn handle_set_tiltfile_args(Json(args): Json<Vec<String>>) -> Response {
    tracing::info!(?args, "set_tiltfile_args (accepted; live arg-injection into the Starlingfile not implemented yet)");
    StatusCode::OK.into_response()
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
