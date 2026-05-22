//! API data model shared between the server, store, and websocket.
//!
//! Split to mirror the Go layout: `v1alpha1` holds the Kubernetes-style
//! resource types, `webview` holds the `View` envelope streamed to the UI.

pub mod v1alpha1;
pub mod webview;
