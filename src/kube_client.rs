//! In-process Kubernetes client transport (kube-rs).
//!
//! The reconcilers' default transport shells out to `kubectl`. This module is
//! the typed, in-process alternative built on `kube` + `k8s-openapi`: it opens a
//! client from the ambient kubeconfig and lists pods for a label selector,
//! returning each pod as a `serde_json::Value` so the result slots directly into
//! the existing status pipeline (`aggregate_pod_status` / `pod_record`), which
//! already consumes the raw Kubernetes JSON shape.
//!
//! It is selected at runtime by `STARLING_KUBE_RS=1` (see
//! [`use_kube_rs`]) so the verified `kubectl` path stays the default; the two
//! transports produce the same JSON, verified by a gated integration test.

use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, AttachParams, ListParams, LogParams};
use kube::Client;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Whether the reconcilers should use the in-process kube-rs transport instead
/// of shelling out to `kubectl`. Off unless `STARLING_KUBE_RS=1`.
pub fn use_kube_rs() -> bool {
    std::env::var("STARLING_KUBE_RS").as_deref() == Ok("1")
}

/// List the pods matching a `key=value,key2=value2` label selector in the
/// `default` namespace via the typed client, returning each pod serialized to
/// the same JSON shape `kubectl get pods -o json`'s `items` produce. The
/// returned values therefore feed `aggregate_pod_status`/`pod_record` unchanged.
pub async fn list_pods(selector: &str) -> Result<Vec<serde_json::Value>, String> {
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let lp = ListParams::default().labels(selector);
    let list = pods
        .list(&lp)
        .await
        .map_err(|e| format!("kube list: {e}"))?;
    list.items
        .iter()
        .map(|p| serde_json::to_value(p).map_err(|e| format!("serialize pod: {e}")))
        .collect()
}

/// Fetch recent logs (`tail` lines) for every pod matching a label selector via
/// the typed client and concatenate them — the kube-rs equivalent of `kubectl
/// logs -l <selector> --tail=<n> --all-containers`.
pub async fn pod_logs(selector: &str, tail: i64) -> Result<String, String> {
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let list = pods
        .list(&ListParams::default().labels(selector))
        .await
        .map_err(|e| format!("kube list: {e}"))?;
    let lp = LogParams {
        tail_lines: Some(tail),
        ..Default::default()
    };
    let mut out = String::new();
    for p in list.items {
        if let Some(name) = p.metadata.name {
            if let Ok(logs) = pods.logs(&name, &lp).await {
                out.push_str(&logs);
            }
        }
    }
    Ok(out)
}

/// Follow a single pod's logs via the typed client (`Api::log_stream` with
/// `follow: true`), returning the byte stream — the kube-rs equivalent of
/// `kubectl logs -f --tail=<n>`. The caller feeds it line-by-line into the log
/// store. `tail` seeds the stream with the last N lines before following.
pub async fn log_stream(
    pod: &str,
    tail: i64,
) -> Result<impl tokio::io::AsyncRead + Unpin + Send + 'static, String> {
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let lp = LogParams {
        follow: true,
        tail_lines: Some(tail),
        ..Default::default()
    };
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    pods.log_stream(pod, &lp)
        .await
        .map(|s| s.compat())
        .map_err(|e| format!("kube log_stream: {e}"))
}

/// Run a native kube-rs port-forward: bind a local TCP listener on
/// `host:local_port` and proxy each accepted connection to the pod's
/// `container_port` over the typed `Api::portforward` streamed channel (the
/// in-process equivalent of `kubectl port-forward`). Loops accepting connections
/// until the task is aborted; returns only on a bind/accept error. Each
/// connection is proxied with `copy_bidirectional`.
pub async fn port_forward_listener(
    pod: String,
    host: String,
    local_port: u16,
    container_port: u16,
) -> Result<(), String> {
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let listener = tokio::net::TcpListener::bind((host.as_str(), local_port))
        .await
        .map_err(|e| format!("bind {host}:{local_port}: {e}"))?;
    loop {
        let (mut conn, _) = listener
            .accept()
            .await
            .map_err(|e| format!("accept: {e}"))?;
        let pods = pods.clone();
        let pod = pod.clone();
        tokio::spawn(async move {
            let mut pf = match pods.portforward(&pod, &[container_port]).await {
                Ok(p) => p,
                Err(_) => return,
            };
            let Some(mut upstream) = pf.take_stream(container_port) else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut conn, &mut upstream).await;
        });
    }
}

/// Server-side apply a multi-document YAML stream via the typed dynamic client —
/// the kube-rs equivalent of `kubectl apply -f -`. Each document is parsed into a
/// `DynamicObject`, its GVK resolved against the cluster's API discovery to pick
/// the right (namespaced vs cluster-scoped) resource, and applied with
/// `Patch::Apply` under the `starling` field manager.
pub async fn apply_yaml(yaml: &str) -> Result<(), String> {
    use kube::api::{DynamicObject, Patch, PatchParams};
    use kube::core::GroupVersionKind;
    use kube::discovery::{Discovery, Scope};

    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let discovery = Discovery::new(client.clone())
        .run()
        .await
        .map_err(|e| format!("api discovery: {e}"))?;
    let ssapply = PatchParams::apply("starling").force();

    // Parse every document up front (the `serde_yaml::Deserializer` borrow is not
    // `Send`, so it must not be held across an await) into owned objects.
    let mut objects: Vec<DynamicObject> = Vec::new();
    for doc in serde_yaml::Deserializer::from_str(yaml) {
        let value = serde_yaml::Value::deserialize(doc).map_err(|e| format!("parse yaml: {e}"))?;
        if value.is_null() {
            continue;
        }
        objects.push(serde_yaml::from_value(value).map_err(|e| format!("parse object: {e}"))?);
    }

    for obj in &objects {
        let tm = obj
            .types
            .as_ref()
            .ok_or_else(|| "object missing apiVersion/kind".to_string())?;
        let (group, version) = match tm.api_version.split_once('/') {
            Some((g, v)) => (g.to_string(), v.to_string()),
            None => (String::new(), tm.api_version.clone()),
        };
        let gvk = GroupVersionKind::gvk(&group, &version, &tm.kind);
        let (ar, caps) = discovery
            .resolve_gvk(&gvk)
            .ok_or_else(|| format!("unknown resource for {}/{}", tm.api_version, tm.kind))?;
        let api: Api<DynamicObject> = if caps.scope == Scope::Namespaced {
            let ns = obj.metadata.namespace.as_deref().unwrap_or("default");
            Api::namespaced_with(client.clone(), ns, &ar)
        } else {
            Api::all_with(client.clone(), &ar)
        };
        let name = obj
            .metadata
            .name
            .clone()
            .ok_or_else(|| format!("{} object missing metadata.name", tm.kind))?;
        api.patch(&name, &ssapply, &Patch::Apply(&obj))
            .await
            .map_err(|e| format!("apply {}/{name}: {e}", tm.kind))?;
    }
    Ok(())
}

/// Whether an `AttachedProcess`'s terminated `Status` indicates success. The
/// k8s exec channel reports `status: "Success"` on exit 0, else `"Failure"`.
fn attach_succeeded(
    status: Option<k8s_openapi::apimachinery::pkg::apis::meta::v1::Status>,
) -> Result<(), String> {
    match status {
        Some(s) if s.status.as_deref() == Some("Success") => Ok(()),
        Some(s) => Err(format!(
            "exec failed: {}",
            s.message.unwrap_or_else(|| "non-zero exit".to_string())
        )),
        None => Err("exec produced no terminal status".to_string()),
    }
}

/// Exec a command in a pod's first container via the WebSocket attach API (the
/// typed equivalent of `kubectl exec pod -- <cmd>`). Drains stdout/stderr so the
/// stream can close, then returns Ok only on a `Success` terminal status.
pub async fn exec(pod: &str, cmd: &[String]) -> Result<(), String> {
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let ap = AttachParams::default()
        .stdin(false)
        .stdout(true)
        .stderr(true);
    let mut attached = pods
        .exec(pod, cmd, &ap)
        .await
        .map_err(|e| format!("kube exec: {e}"))?;
    // Drain output so the channel can reach its terminal status.
    if let Some(mut out) = attached.stdout() {
        let mut sink = Vec::new();
        let _ = out.read_to_end(&mut sink).await;
    }
    let status = attached.take_status().map(|f| f);
    let status = match status {
        Some(f) => f.await,
        None => None,
    };
    attach_succeeded(status)
}

/// Copy a local file into a pod's first container at `remote` (the typed
/// equivalent of `kubectl cp local pod:remote`). Implemented by exec-ing
/// `sh -c 'cat > remote'` and streaming the file's bytes to the container's
/// stdin — the same primitive `kubectl cp` uses, without the tar wrapper, which
/// is sufficient for live-update's single-file syncs.
pub async fn copy_file(pod: &str, local: &str, remote: &str) -> Result<(), String> {
    let bytes = std::fs::read(local).map_err(|e| format!("read {local}: {e}"))?;
    let client = Client::try_default()
        .await
        .map_err(|e| format!("kube client: {e}"))?;
    let pods: Api<Pod> = Api::default_namespaced(client);
    let ap = AttachParams::default()
        .stdin(true)
        .stdout(false)
        .stderr(true);
    let cmd = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("cat > {remote}"),
    ];
    let mut attached = pods
        .exec(pod, cmd, &ap)
        .await
        .map_err(|e| format!("kube exec (cp): {e}"))?;
    {
        let mut stdin = attached
            .stdin()
            .ok_or_else(|| "exec stdin unavailable".to_string())?;
        stdin
            .write_all(&bytes)
            .await
            .map_err(|e| format!("write stdin: {e}"))?;
        stdin
            .flush()
            .await
            .map_err(|e| format!("flush stdin: {e}"))?;
        // Drop stdin to send EOF so `cat` finishes.
    }
    let status = match attached.take_status() {
        Some(f) => f.await,
        None => None,
    };
    attach_succeeded(status)
}
