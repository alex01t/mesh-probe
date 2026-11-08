//! Continuous pod-to-pod application RTT and transport lifetime monitor.
//!
//! One instance per node (DaemonSet). Discover peer Pod IPs from EndpointSlices,
//! then establish ONE persistent gRPC/HTTP2 channel to each peer. Send a unary
//! Ping every 100ms over that channel; DO NOT intentionally reconnect after
//! every RPC. Reconnect only when an RPC fails or the peer IP changes.
//!
//! A separate TCP connect() side-channel every 5s measures SYN/connect latency.
//! It MUST NOT replace or disturb the primary gRPC connection.
//!
//! "connection lifetime" here is the duration of a successfully used gRPC
//! channel epoch (first successful RPC -> first failed RPC / peer replacement).
//! Tonic can reconnect transparently; therefore this is NOT a guaranteed
//! kernel TCP socket lifetime. See README for precise semantics and roadmap.

use anyhow::{Context, Result};
use axum::{routing::get, Router};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::{api::ListParams, Api, Client};
use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::{
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, task::JoinHandle, time};
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};
use tracing::{info, warn};

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub mod pb {
    tonic::include_proto!("meshprobe");
}
use pb::{
    probe_client::ProbeClient,
    probe_server::{Probe, ProbeServer},
    PingReply, PingRequest,
};

#[derive(Default)]
struct PingService;
#[tonic::async_trait]
impl Probe for PingService {
    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> std::result::Result<Response<PingReply>, Status> {
        Ok(Response::new(PingReply {
            sequence: request.into_inner().sequence,
        }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Peer {
    node: String,
    ip: String,
}

struct Metrics {
    registry: Registry,
    rtt: HistogramVec,
    tcp_connect: HistogramVec,
    lifetime: HistogramVec,
    requests: IntCounterVec,
    disconnects: IntCounterVec,
    reconnects: IntCounterVec,
    connected: IntGaugeVec,
}
impl Metrics {
    fn new() -> Result<Self> {
        let registry = Registry::new();
        let labels = &["src_node", "dst_node"];
        let histogram_labels = &["src_node", "dst_node", "self_probe"];
        let buckets = vec![
            0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.003, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5,
            1.0, 5.0,
        ];
        let rtt = HistogramVec::new(
            HistogramOpts::new(
                "mesh_probe_rtt_seconds",
                "Successful unary gRPC round-trip time",
            )
            .buckets(buckets.clone()),
            histogram_labels,
        )?;
        let tcp_connect = HistogramVec::new(
            HistogramOpts::new(
                "mesh_probe_tcp_connect_seconds",
                "Side-channel TCP connect duration",
            )
            .buckets(buckets),
            histogram_labels,
        )?;
        let lifetime = HistogramVec::new(
            HistogramOpts::new(
                "mesh_probe_connection_lifetime_seconds",
                "Observed successful gRPC channel epoch duration",
            )
            .buckets(vec![
                1., 5., 10., 30., 60., 120., 300., 600., 1800., 3600., 14400., 86400.,
            ]),
            histogram_labels,
        )?;
        let requests = IntCounterVec::new(
            Opts::new("mesh_probe_requests_total", "Probe RPC results"),
            &["src_node", "dst_node", "status"],
        )?;
        let disconnects = IntCounterVec::new(
            Opts::new(
                "mesh_probe_disconnects_total",
                "Established channel epochs ended after an RPC failure",
            ),
            labels,
        )?;
        let reconnects = IntCounterVec::new(
            Opts::new(
                "mesh_probe_reconnects_total",
                "New successful channel epochs after a prior established epoch",
            ),
            labels,
        )?;
        let connected = IntGaugeVec::new(
            Opts::new(
                "mesh_probe_connected",
                "1 if primary channel epoch has succeeded and not subsequently failed",
            ),
            labels,
        )?;
        registry.register(Box::new(rtt.clone()))?;
        registry.register(Box::new(tcp_connect.clone()))?;
        registry.register(Box::new(lifetime.clone()))?;
        registry.register(Box::new(requests.clone()))?;
        registry.register(Box::new(disconnects.clone()))?;
        registry.register(Box::new(reconnects.clone()))?;
        registry.register(Box::new(connected.clone()))?;
        Ok(Self {
            registry,
            rtt,
            tcp_connect,
            lifetime,
            requests,
            disconnects,
            reconnects,
            connected,
        })
    }
}

fn setting(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
// EndpointSlices are API discovery ONLY. Data packets go straight to Pod IPs,
// never through the headless Service name, VIP, kube-proxy, or hostPort.
async fn discover(api: &Api<EndpointSlice>, service: &str, port: u16) -> Result<Vec<Peer>> {
    let slices = api
        .list(&ListParams::default().labels(&format!("kubernetes.io/service-name={service}")))
        .await?;
    let mut peers = Vec::new();
    for slice in slices {
        if slice.address_type != "IPv4" {
            continue;
        }
        let valid_port = slice
            .ports
            .as_ref()
            .map(|ports| {
                ports
                    .iter()
                    .any(|p| p.name.as_deref() == Some("grpc") && p.port == Some(port as i32))
            })
            .unwrap_or(false);
        if !valid_port {
            continue;
        }
        for ep in slice.endpoints {
            if ep
                .conditions
                .as_ref()
                .and_then(|c| c.terminating)
                .unwrap_or(false)
            {
                continue;
            }
            let Some(node) = ep.node_name else { continue };
            if let Some(ip) = ep.addresses.first() {
                peers.push(Peer {
                    node,
                    ip: ip.clone(),
                });
            }
        }
    }
    Ok(peers)
}

// A channel is created lazily by tonic. Only a successful RPC starts an epoch.
// We intentionally discard the channel after failure to make recovery explicit.
async fn primary_loop(peer: Peer, src: String, port: u16, m: Arc<Metrics>) {
    let labels = [src.as_str(), peer.node.as_str()];
    let self_probe = if peer.node == "localhost" {
        "local"
    } else if src == peer.node {
        "true"
    } else {
        "false"
    };
    let histogram_labels = [src.as_str(), peer.node.as_str(), self_probe];
    let uri = format!("http://{}:{}", peer.ip, port);
    let mut client: Option<ProbeClient<Channel>> = None;
    let mut epoch: Option<Instant> = None;
    let mut had_epoch = false;
    let mut sequence = 0u64;
    let mut tick = time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    // Stagger peers so that 20 nodes do not all wake up in lockstep.
    time::sleep(Duration::from_millis(
        (peer.node.bytes().fold(0u64, |a, b| a + b as u64)
            + src.bytes().fold(0u64, |a, b| a + b as u64))
            % 100,
    ))
    .await;
    loop {
        tick.tick().await;
        if client.is_none() {
            match Endpoint::from_shared(uri.clone()) {
                Ok(endpoint) => client = Some(ProbeClient::new(endpoint.connect_lazy())),
                Err(e) => {
                    warn!(%uri, "invalid peer endpoint: {e}");
                    return;
                }
            }
        }
        sequence += 1;
        let start = Instant::now();
        let result = time::timeout(
            Duration::from_millis(500),
            client.as_mut().unwrap().ping(PingRequest { sequence }),
        )
        .await;
        let outcome = match result {
            Ok(Ok(response)) if response.get_ref().sequence == sequence => Ok(()),
            Err(_) => Err("timeout"),
            _ => Err("error"),
        };
        match outcome {
            Ok(()) => {
                m.rtt
                    .with_label_values(&histogram_labels)
                    .observe(start.elapsed().as_secs_f64());
                m.requests
                    .with_label_values(&[&src, &peer.node, "ok"])
                    .inc();
                if epoch.is_none() {
                    if had_epoch {
                        m.reconnects.with_label_values(&labels).inc();
                    }
                    epoch = Some(Instant::now());
                    had_epoch = true;
                    info!(src=%src, dst=%peer.node, "gRPC channel epoch established");
                }
                m.connected.with_label_values(&labels).set(1);
            }
            Err(status) => {
                m.requests
                    .with_label_values(&[&src, &peer.node, status])
                    .inc();
                if let Some(since) = epoch.take() {
                    m.lifetime
                        .with_label_values(&histogram_labels)
                        .observe(since.elapsed().as_secs_f64());
                    m.disconnects.with_label_values(&labels).inc();
                    warn!(src=%src, dst=%peer.node, status, "gRPC channel epoch lost");
                }
                m.connected.with_label_values(&labels).set(0);
                client = None;
                // Avoid reconnect storms if the destination is down.
                time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

async fn tcp_side_channel(peer: Peer, src: String, port: u16, m: Arc<Metrics>) {
    let self_probe = if peer.node == "localhost" {
        "local"
    } else if src == peer.node {
        "true"
    } else {
        "false"
    };
    let histogram_labels = [src.as_str(), peer.node.as_str(), self_probe];
    let mut tick = time::interval(Duration::from_secs(5));
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let addr = format!("{}:{}", peer.ip, port);
        let start = Instant::now();
        match time::timeout(Duration::from_secs(1), TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => {
                m.tcp_connect
                    .with_label_values(&histogram_labels)
                    .observe(start.elapsed().as_secs_f64());
                m.requests
                    .with_label_values(&[&src, &peer.node, "tcp_ok"])
                    .inc();
                drop(stream);
            }
            _ => {
                m.requests
                    .with_label_values(&[&src, &peer.node, "tcp_error"])
                    .inc();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let src = env::var("NODE_NAME").context("NODE_NAME required")?;
    let ns = setting("POD_NAMESPACE", "monitoring");
    let service = setting("SERVICE_NAME", "network-probe");
    let port: u16 = setting("GRPC_PORT", "8080").parse()?;
    let metrics_port: u16 = setting("METRICS_PORT", "9090").parse()?;
    let m = Arc::new(Metrics::new()?);

    let grpc_addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    tokio::spawn(async move {
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(ProbeServer::new(PingService))
            .serve(grpc_addr)
            .await
        {
            tracing::error!("gRPC server failed: {e}");
        }
    });
    let mm = m.clone();
    tokio::spawn(async move {
        let app = Router::new()
            .route(
                "/metrics",
                get(move || {
                    let mm = mm.clone();
                    async move {
                        let encoded = tokio::task::spawn_blocking(move || {
                            let encoder = TextEncoder::new();
                            let mut buffer = Vec::new();
                            encoder
                                .encode(&mm.registry.gather(), &mut buffer)
                                .map_err(|_| ())?;
                            Ok::<_, ()>(String::from_utf8_lossy(&buffer).into_owned())
                        })
                        .await;
                        match encoded {
                            Ok(Ok(body)) => (axum::http::StatusCode::OK, body),
                            _ => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, String::new()),
                        }
                    }
                }),
            )
            .route("/healthz", get(|| async { "ok" }));
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{metrics_port}"))
            .await
            .expect("metrics bind");
        axum::serve(listener, app).await.expect("metrics serve");
    });

    let client = Client::try_default().await?;
    let api: Api<EndpointSlice> = Api::namespaced(client, &ns);
    // MVP: periodic EndpointSlice LIST (every 10s). Easy to reason about and
    // sufficient for 20 nodes. Upgrade to kube-runtime watcher for fast changes.
    let mut workers: HashMap<String, (Peer, JoinHandle<()>, JoinHandle<()>)> = HashMap::new();
    loop {
        match discover(&api, &service, port).await {
            Ok(peers) => {
                let mut peers = peers;
                peers.push(Peer {
                    node: "localhost".into(),
                    ip: "127.0.0.1".into(),
                });
                let peers: HashMap<String, Peer> =
                    peers.into_iter().map(|p| (p.node.clone(), p)).collect();
                let desired: HashSet<_> = peers.keys().cloned().collect();
                let old: Vec<_> = workers
                    .keys()
                    .filter(|n| {
                        !desired.contains(*n) || peers.get(*n) != workers.get(*n).map(|w| &w.0)
                    })
                    .cloned()
                    .collect();
                for node in old {
                    if let Some((_, primary, side)) = workers.remove(&node) {
                        primary.abort();
                        side.abort();
                        m.connected.with_label_values(&[&src, &node]).set(0);
                    }
                }
                for (node, peer) in peers {
                    if workers.contains_key(&node) {
                        continue;
                    }
                    info!(src=%src, dst=%node, ip=%peer.ip, "discovered peer");
                    let primary =
                        tokio::spawn(primary_loop(peer.clone(), src.clone(), port, m.clone()));
                    let side =
                        tokio::spawn(tcp_side_channel(peer.clone(), src.clone(), port, m.clone()));
                    workers.insert(node, (peer, primary, side));
                }
            }
            Err(e) => warn!("EndpointSlice discovery failed; retaining existing peers: {e}"),
        }
        time::sleep(Duration::from_secs(10)).await;
    }
}
