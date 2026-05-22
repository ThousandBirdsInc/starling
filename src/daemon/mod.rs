//! The Starling daemon: one per machine.
//!
//! Owns the single shared named-URL proxy, allocates ports centrally so
//! multiple `starling up` instances never collide, and aggregates every
//! instance's resources for the shared TUI dashboard.

pub mod client;
pub mod protocol;

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::Mutex;

use crate::proxy::{self, ProxyRegistry};
use protocol::*;

/// Max recent log lines retained per (instance, resource).
const LOG_RING: usize = 400;
/// Instances not seen within this window are pruned from the dashboard.
const INSTANCE_TTL: Duration = Duration::from_secs(10);

struct Instance {
    state: InstanceState,
    logs: HashMap<String, VecDeque<String>>,
    commands: Vec<Command>,
    last_seen: Instant,
}

#[derive(Default)]
struct Inner {
    instances: HashMap<String, Instance>,
    seq: u64,
    leased_ports: std::collections::HashSet<u16>,
    routes: Vec<RouteInfo>,
    shutting_down: bool,
    /// mDNS advertiser processes (kept alive while routes are active).
    advertisers: Vec<std::process::Child>,
}

struct Daemon {
    inner: Mutex<Inner>,
    registry: ProxyRegistry,
    proxy_port: u16,
    tld: String,
    /// Advertise routes over mDNS for LAN access.
    lan: bool,
    lan_ip: String,
}

/// Run the daemon until the process is killed. If another daemon is already
/// listening, this returns immediately.
pub async fn run(proxy_port: u16, tld: String, host: String, tls: bool, lan: bool) {
    let dir = state_dir();
    std::fs::create_dir_all(&dir).ok();
    let sock = socket_path();

    // If a daemon is already up, don't start a second one.
    if client::DaemonClient::new().is_running().await {
        println!("starling daemon already running at {}", sock.display());
        return;
    }
    // Clear a stale socket file.
    if sock.exists() {
        std::fs::remove_file(&sock).ok();
    }

    let listener = match UnixListener::bind(&sock) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("daemon: failed to bind {}: {e}", sock.display());
            return;
        }
    };
    std::fs::write(pid_path(), std::process::id().to_string()).ok();

    let registry = ProxyRegistry::new();
    // Start the shared proxy (HTTPS if requested, else HTTP).
    {
        let registry = registry.clone();
        let addr = format!("{host}:{proxy_port}");
        if tls {
            match crate::certs::tls_server_config() {
                Ok(config) => {
                    tokio::spawn(proxy::serve_tls(addr, registry, proxy_port, config));
                }
                Err(e) => {
                    eprintln!("daemon: TLS setup failed ({e}); serving plain HTTP");
                    tokio::spawn(proxy::serve(addr, registry, proxy_port));
                }
            }
        } else {
            tokio::spawn(proxy::serve(addr, registry, proxy_port));
        }
    }

    let lan_ip = if lan {
        crate::netmodes::lan_ip()
    } else {
        String::new()
    };
    if lan {
        println!("LAN mode: advertising routes over mDNS as <name>.local at {lan_ip}");
    }
    let daemon = Arc::new(Daemon {
        inner: Mutex::new(Inner::default()),
        registry,
        proxy_port,
        tld,
        lan,
        lan_ip,
    });

    println!(
        "starling daemon listening on {} (shared proxy :{})",
        sock.display(),
        proxy_port
    );

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let daemon = daemon.clone();
                tokio::spawn(async move {
                    let _ = handle_conn(stream, daemon).await;
                });
            }
            Err(e) => {
                eprintln!("daemon accept error: {e}");
            }
        }
    }
}

async fn handle_conn(stream: tokio::net::UnixStream, daemon: Arc<Daemon>) -> std::io::Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Ok(());
    }
    let resp = match serde_json::from_str::<Request>(&line) {
        Ok(req) => daemon.handle(req).await,
        Err(e) => Response::Error(format!("bad request: {e}")),
    };
    let mut out = serde_json::to_string(&resp).unwrap_or_else(|_| "{}".into());
    out.push('\n');
    write_half.write_all(out.as_bytes()).await?;
    write_half.flush().await
}

impl Daemon {
    async fn handle(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Ok,

            Request::Register { name, dir, pid } => {
                let mut inner = self.inner.lock().await;
                inner.seq += 1;
                let id = format!("{}-{}", sanitize(&name), inner.seq);
                inner.instances.insert(
                    id.clone(),
                    Instance {
                        state: InstanceState {
                            id: id.clone(),
                            name,
                            dir,
                            pid,
                            resources: vec![],
                        },
                        logs: HashMap::new(),
                        commands: vec![],
                        last_seen: Instant::now(),
                    },
                );
                Response::Registered { instance: id }
            }

            Request::Deregister { instance } => {
                let mut inner = self.inner.lock().await;
                inner.instances.remove(&instance);
                inner.routes.retain(|r| r.instance != instance);
                // Re-sync the proxy registry with the surviving routes.
                self.resync_registry(&inner);
                Response::Ok
            }

            Request::Update {
                instance,
                resources,
                logs,
            } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.state.resources = resources;
                    inst.last_seen = Instant::now();
                    // The reporter sends the full recent tail each tick, so
                    // replace (don't append) to avoid duplicating lines.
                    for (res, lines) in logs {
                        let mut dq: VecDeque<String> = lines.into_iter().collect();
                        while dq.len() > LOG_RING {
                            dq.pop_front();
                        }
                        inst.logs.insert(res, dq);
                    }
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::AllocatePort { instance: _ } => {
                // Find a free port not already leased.
                for _ in 0..50 {
                    if let Ok(port) = proxy::find_free_port().await {
                        let mut inner = self.inner.lock().await;
                        if inner.leased_ports.insert(port) {
                            return Response::Port { port };
                        }
                    }
                }
                Response::Error("could not allocate a free port".into())
            }

            Request::RegisterRoute {
                instance,
                hostname,
                port,
            } => {
                self.registry.register(&hostname, port);
                if self.lan {
                    if let Some(child) = crate::netmodes::advertise_lan(&hostname, &self.lan_ip) {
                        self.inner.lock().await.advertisers.push(child);
                    }
                }
                let mut inner = self.inner.lock().await;
                inner.routes.retain(|r| r.hostname != hostname);
                inner.routes.push(RouteInfo {
                    hostname,
                    port,
                    instance,
                });
                Response::Ok
            }

            Request::RemoveRoute { hostname } => {
                self.registry.remove(&hostname);
                let mut inner = self.inner.lock().await;
                inner.routes.retain(|r| r.hostname != hostname);
                Response::Ok
            }

            Request::GetState => {
                let mut inner = self.inner.lock().await;
                self.prune(&mut inner);
                let instances = inner
                    .instances
                    .values()
                    .map(|i| i.state.clone())
                    .collect();
                Response::State(DashboardState {
                    instances,
                    routes: inner.routes.clone(),
                    proxy_port: self.proxy_port,
                    tld: self.tld.clone(),
                })
            }

            Request::GetLogs { instance, resource } => {
                let inner = self.inner.lock().await;
                let lines = inner
                    .instances
                    .get(&instance)
                    .and_then(|i| i.logs.get(&resource))
                    .map(|r| r.iter().cloned().collect())
                    .unwrap_or_default();
                Response::Logs(lines)
            }

            Request::PollCommands { instance } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.last_seen = Instant::now();
                    Response::Commands(std::mem::take(&mut inst.commands))
                } else {
                    Response::Commands(vec![])
                }
            }

            Request::Trigger { instance, resource } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.commands.push(Command::Trigger { resource });
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::Restart { instance, resource } => {
                let mut inner = self.inner.lock().await;
                if let Some(inst) = inner.instances.get_mut(&instance) {
                    inst.commands.push(Command::Restart { resource });
                    Response::Ok
                } else {
                    Response::Error(format!("unknown instance {instance}"))
                }
            }

            Request::ShutdownProject { dir } => {
                let mut inner = self.inner.lock().await;
                let mut instances = Vec::new();
                for inst in inner.instances.values_mut() {
                    if inst.state.dir == dir {
                        inst.commands.push(Command::Shutdown);
                        instances.push(inst.state.clone());
                    }
                }
                Response::ShutdownQueued { instances }
            }

            Request::ShutdownDaemon => {
                let mut inner = self.inner.lock().await;
                let instances: Vec<InstanceState> = inner
                    .instances
                    .values_mut()
                    .map(|inst| {
                        inst.commands.push(Command::Shutdown);
                        inst.state.clone()
                    })
                    .collect();
                inner.routes.clear();
                inner.leased_ports.clear();
                self.resync_registry(&inner);
                if !inner.shutting_down {
                    inner.shutting_down = true;
                    tokio::spawn(async {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        std::fs::remove_file(socket_path()).ok();
                        std::fs::remove_file(pid_path()).ok();
                        std::process::exit(0);
                    });
                }
                Response::ShutdownQueued { instances }
            }
        }
    }

    /// Drop instances we haven't heard from recently and their routes.
    fn prune(&self, inner: &mut Inner) {
        let now = Instant::now();
        let dead: Vec<String> = inner
            .instances
            .iter()
            .filter(|(_, i)| now.duration_since(i.last_seen) > INSTANCE_TTL)
            .map(|(id, _)| id.clone())
            .collect();
        if dead.is_empty() {
            return;
        }
        for id in &dead {
            inner.instances.remove(id);
        }
        inner.routes.retain(|r| !dead.contains(&r.instance));
        self.resync_registry(inner);
    }

    /// Rebuild the proxy registry from the authoritative route list.
    fn resync_registry(&self, inner: &Inner) {
        for existing in self.registry.snapshot() {
            if !inner.routes.iter().any(|r| r.hostname == existing.hostname) {
                self.registry.remove(&existing.hostname);
            }
        }
        for r in &inner.routes {
            self.registry.register(&r.hostname, r.port);
        }
    }
}

fn sanitize(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let s = s.trim_matches('-').to_string();
    if s.is_empty() {
        "app".into()
    } else {
        s
    }
}
