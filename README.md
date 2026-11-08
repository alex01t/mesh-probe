# Kubernetes persistent TCP mesh probe (Rust MVP)

## Purpose

Run one normal (non-hostNetwork, non-hostPort) pod per Kubernetes node. Each
pod discovers all other instances from a headless Service's EndpointSlices,
then talks directly to the peer Pod IPs. The goal is to measure application
observed latency across the *real Cilium pod datapath*, including VXLAN where
configured, without conflating it with ICMP ping or Service load balancing.

Each directed pair has two independent channels:

1. **Primary**: one long-lived gRPC/HTTP2 channel; unary ping every 100ms;
   500ms RPC deadline. No deliberate reconnection between successful RPCs.
   On timeout/error, discard the channel and try again after 500ms.
2. **Side channel**: open a new plain TCP connection to the same gRPC listener
   every 5 seconds; measure connect() time, then close it. This intentionally
   exercises new TCP handshakes and potentially new conntrack entries. It does
   not send a gRPC request.

**No Service VIP is used for probes.** The headless Service exists only to
populate EndpointSlices. A separate metrics Service is optional.

## Build / deploy

```sh
cargo build --release
# or
 docker build -t YOUR_REGISTRY/network-probe:latest .
 docker push YOUR_REGISTRY/network-probe:latest
# edit image in k8s/deploy.yaml; create namespace monitoring if needed
kubectl apply -f k8s/deploy.yaml
kubectl -n monitoring get pods -l app=network-probe -o wide
kubectl -n monitoring port-forward ds/network-probe 9090:9090
curl localhost:9090/metrics
```

Configure Prometheus pod scraping on port 9090 (PodMonitor/ServiceMonitor or
annotations depending on your stack). Scrape each pod separately, not one
random backend behind a Service. `instance`/pod labels distinguish exporters.

## Metrics

- `mesh_probe_rtt_seconds{src_node,dst_node}` successful gRPC RTT histogram.
- `mesh_probe_requests_total{src_node,dst_node,status}` status `ok`, `error`,
  `timeout`, `tcp_ok`, `tcp_error`.
- `mesh_probe_connected{src_node,dst_node}` 1 if last primary RPC succeeded
  and no subsequent RPC failed; this is NOT a kernel socket state.
- `mesh_probe_disconnects_total{src_node,dst_node}` increments when a
  previously successful primary channel epoch ends due to failed RPC.
- `mesh_probe_reconnects_total{src_node,dst_node}` counts successful channel
  epochs after the first one for a peer task.
- `mesh_probe_connection_lifetime_seconds{src_node,dst_node}` histogram of
  **completed** successful epochs, first successful RPC to first failed RPC.
- `mesh_probe_tcp_connect_seconds{src_node,dst_node}` successful TCP connect
  durations. Failed attempts counted in `requests_total{status="tcp_error"}`.
- `mesh_probe_last_success_timestamp_seconds` last successful primary RPC.

Example PromQL:

```promql
histogram_quantile(0.99, sum by (le, src_node, dst_node) (rate(mesh_probe_rtt_seconds_bucket[5m])))

# Mean lifetime of COMPLETED epochs in the time window:
sum by (src_node,dst_node) (rate(mesh_probe_connection_lifetime_seconds_sum[1h]))
/
sum by (src_node,dst_node) (rate(mesh_probe_connection_lifetime_seconds_count[1h]))

# Failures as fraction of all primary probes:
sum by (src_node,dst_node) (rate(mesh_probe_requests_total{status=~"error|timeout"}[5m]))
/
sum by (src_node,dst_node) (rate(mesh_probe_requests_total{status=~"ok|error|timeout"}[5m]))
```

## Critical measurement caveat: gRPC channel != TCP socket

Tonic's Channel is a pooled HTTP/2 client abstraction. It may transparently
reconnect the underlying socket. A successful 2-hour channel epoch does **not**
prove that the same TCP socket survived for 2 hours. Likewise, an RPC timeout
may be caused by scheduler pressure, not a broken TCP connection.

**To measure true TCP connection age, hand off the following extension to Codex:**

- Use a single explicitly owned TCP socket per peer, perform HTTP/2 handshake
  over it, and prohibit transport-level transparent reconnection. This may
  require a custom tower `Service<Uri>` connector for tonic/hyper and careful
  control over retries and reconnects.
- Record socket creation timestamp and close timestamp, distinguishing clean
  close, EOF/RST, local timeout, and pod termination. Expose
  `mesh_probe_tcp_socket_lifetime_seconds` and `mesh_probe_tcp_socket_age_seconds`.
- Expose live connection age as a gauge to avoid **right-censoring bias**:
  long-lived connections do not appear in completed-lifetime histograms until
  they terminate. A histogram of completed lifetimes alone is NOT the mean
  lifetime of all connections.
- On shutdown / EndpointSlice replacement, record censored epochs separately;
  do not count intentional pod rollout as a network disconnect.
- To detect a silent dead connection quickly, maintain the 100ms RPC probes;
  a failed RPC is an application failure, not automatically a TCP reset.

## MVP limitations / suggested improvements

1. Discovery currently LISTs EndpointSlices every 10 seconds. For prompt
   changes, use kube-runtime's EndpointSlice watcher/informer; keep the same
   direct Pod IP transport and restart workers on peer IP changes.
2. A primary channel is discarded after *any* failed RPC. For strict socket
   lifetime measurement, classify errors before deciding to reconnect.
3. No server processing-time subtraction; RTT includes both endpoint runtimes.
4. The TCP side-channel uses the same listener but does not complete HTTP/2
   handshake. This is intentionally a TCP handshake benchmark.
5. The DaemonSet tolerates NoSchedule taints; adapt for NoExecute taints and
   node selectors as appropriate. Pod security policies must allow pod traffic.
6. Pod churn may leave old Prometheus label series in the registry until pod
   restart; add metric vector cleanup when removing a peer.
7. Peer restart on the same Pod IP is not distinguished by discovery; track
   targetRef UID and endpoint identity in a production version.
8. Startup/readiness probes should check gRPC listener too; current `/healthz`
   only checks the metrics server.
9. `k8s/deploy.yaml` is an example, not a production-tuned deployment.

## Verification

After deployment: verify 20 DaemonSet pods, 19 peer connections per pod,
~3,800 primary RPCs/sec total, `connected == 1` for healthy pairs, and that
TCP connect side-channel succeeds. Restart one pod and verify only its edges
recover. Block traffic to one peer (Cilium policy) and verify failure counters
and recovery. Compare same-node, same-zone, cross-zone RTTs where available.
