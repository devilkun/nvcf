# Tunnel Transport Selection

Stargate proxies OpenAI-compatible inference requests such as `/v1/chat/completions`, `/v1/responses`, and `/v1/embeddings` over an already-established QUIC connection. The tunnel protocol is selected with `--tunnel-protocol=custom|http3|webtransport` on both `stargate` and `pylon`; the two sides must use the same value. `custom` is the default for backward compatibility.

Use this guide when choosing a protocol for direct QUIC backends, reverse-tunnel backends, Kubernetes deployments, and network benchmarks.

## Summary

| Protocol | Wire shape | Use when | Load balancer requirement | Reverse-tunnel fit |
| --- | --- | --- | --- | --- |
| `custom` | Raw QUIC bidirectional streams carrying Stargate Cap'n Proto tunnel frames. | You own both endpoints and need the fastest, simplest current Stargate path. | L4 UDP passthrough. No HTTP/3 inspection. | Good with L4 only. Not an HTTP/3/WebTransport contract. |
| `http3` | One HTTP/3 request stream per proxied inference request. | You want request-shaped HTTP/3 framing for direct client-initiated streams and do not need server-opened streams through an H3-aware intermediary. | L4 UDP passthrough for reverse tunnels. H3-aware L7 intermediaries can reject server-initiated bidirectional streams. | Limited: Stargate's reverse path opens streams toward the client, which plain HTTP/3 does not standardize. |
| `webtransport` | One HTTP/3 extended CONNECT session, then one WebTransport bidirectional stream per proxied request carrying WebTransport-specific HTTP proxy heads and raw body bytes. | You need an HTTP/3-compatible session that permits either endpoint to open bidirectional streams, especially behind an H3/WebTransport-aware L7 hop. | L4 UDP passthrough or an L7 proxy that explicitly supports WebTransport over HTTP/3 CONNECT and forwards WebTransport streams. | Best fit for reverse tunnels behind an H3/WebTransport L7 proxy. |

## Protocol Details

### `custom`

`custom` opens a fresh QUIC bidirectional stream per proxied request and writes Stargate's existing tunnel frames directly on that stream. It does not negotiate an ALPN value and does not expose HTTP semantics to intermediaries.

Choose `custom` when:

- Stargate and pylon are deployed as trusted paired components.
- The network path is L4 UDP passthrough, such as the default Kubernetes Service load balancer or a UDP load balancer.
- Lowest overhead is more important than HTTP/3 intermediary compatibility.
- You need the safest backward-compatible default.

Avoid `custom` when an intermediary must understand, terminate, or route HTTP/3/WebTransport traffic.

### `http3`

`http3` uses H3 ALPN and opens one HTTP/3 request stream per proxied request. This maps well to direct client-initiated request traffic. It does not solve reverse-tunnel server-initiated stream routing through an H3-aware L7 load balancer.

Plain HTTP/3 reserves request streams for client-initiated bidirectional streams. RFC 9114 says HTTP/3 does not use server-initiated bidirectional streams unless an extension has been negotiated. That is the core concern with reverse tunnels behind an HTTP/3-aware L7 hop: Stargate's reverse proxy direction needs to open additional streams toward the client, and a plain H3 intermediary is allowed to treat that as a protocol error.

Choose `http3` when:

- You want H3 request/response framing for direct tunnel requests.
- The reverse path, if used, stays behind an L4 UDP passthrough load balancer.
- You are testing HTTP/3 implementation overhead without the WebTransport CONNECT/session layer.

Avoid `http3` for reverse tunnels behind H3-aware L7 load balancers unless that intermediary explicitly supports the extension semantics Stargate needs.

### `webtransport`

`webtransport` uses H3 ALPN, establishes a long-lived WebTransport extended CONNECT session, and then opens a WebTransport bidirectional stream for each proxied request. The payload on each WebTransport stream is HTTP-proxy-specific: a request head block, raw request body bytes until FIN, a response head block, then raw response body bytes until FIN.

This path is intentionally independent from `custom`. It does not carry Stargate's Cap'n Proto tunnel frames inside WebTransport streams.

WebTransport over HTTP/3 is designed for sessions with bidirectional streams initiated by either endpoint. That makes it the protocol-shaped answer for reverse tunnels where Stargate needs to open request streams toward pylon and an H3-aware intermediary sits in between.

Choose `webtransport` when:

- Reverse-tunnel traffic must pass through an H3/WebTransport-aware L7 proxy or load balancer.
- The intermediary must terminate or inspect HTTP/3 while preserving a WebTransport session.
- You want the clearest standards-aligned protocol contract for server-initiated request streams over an established HTTP/3 session.

Avoid `webtransport` when raw throughput is the only goal and the path can stay L4, because the CONNECT/session handling and WebTransport stream prelude add overhead.

## Direct And Reverse Connections

Direct backends advertise `quic://...` inference server URLs. Stargate dials the backend and opens a fresh request stream over the selected protocol. `--direct-quic-connections` can shard direct request streams across multiple outbound QUIC connections per backend; it defaults to `1`.

Reverse-tunnel backends advertise their upstream HTTP URL, set `reverse_tunnel=true`, and have pylon initiate the QUIC connection back to Stargate. Stargate then opens a fresh stream per proxied request over that reverse connection.

The reverse shape is why transport choice matters:

- `custom` works when the network is L4 and does not interpret QUIC streams.
- `http3` works in controlled L4 paths but is not a valid contract for H3-aware L7 intermediaries that see server-initiated bidirectional streams.
- `webtransport` gives the reverse path an H3 session model where both endpoints can open streams.

## Kubernetes And Load Balancers

For standard local Kubernetes manifests, `TUNNEL_PROTOCOL=custom` is the default on both the Stargate StatefulSet and pylon Deployments.

### Backend-Facing Deployment Matrix

This matrix applies only to the pylon-to-Stargate registration and reverse-tunnel interface. API gateway traffic continues to use `stargate-model-discovery` and `stargate-proxy`, both selecting real Stargate pods.

| Tunnel protocol | Stargate internal | Stargate custom LB | Bring your own LB/proxy |
| --- | --- | --- | --- |
| `custom` | Valid, but less clean in Kubernetes. This relies on Stargate's built-in peer forwarding when a backend-facing Service lands on the wrong pod, and backend namespaces must be allowed to reach real Stargate pods. | Recommended. `stargate-k8s-router` is built for this path: gRPC registration is routed by HTTP/2 `:authority`, and custom QUIC reverse tunnels are routed by SNI. | Valid only as L4 UDP passthrough with stable QUIC connection routing. Do not use an HTTP/3/WebTransport-aware L7 proxy for `custom`. |
| `webtransport` | Conditionally valid only when pylon connects directly to the selected Stargate pod or an L4 path preserves that pod selection. Do not rely on Stargate's generic peer relay for WebTransport, because WebTransport session ids are scoped to each H3 CONNECT stream. | Invalid. `stargate-k8s-router` is not an HTTP/3/WebTransport-aware proxy. | Recommended when reverse tunnels need standards-based L7 handling. The proxy or load balancer must support HTTP/3 extended CONNECT and WebTransport bidirectional streams. |
| `http3` | Valid only in controlled L4/direct scenarios. Not recommended for reverse tunnels through L7. | Invalid. `stargate-k8s-router` does not implement HTTP/3 L7 routing, and plain H3 is not a good reverse-tunnel contract. | Valid only as L4 UDP passthrough. Avoid plain HTTP/3 L7 for reverse tunnels because Stargate needs to open streams toward pylon. |

The default Kubernetes recommendation is `custom` with `stargate-k8s-router`. If the deployment requires a standards-based L7 proxy or load balancer between pylon and Stargate, use `webtransport` and bring an H3/WebTransport-aware proxy instead of `stargate-k8s-router`.

Use an L4 UDP load balancer for `custom` and for any `http3` reverse-tunnel deployment. L4 balancing preserves the underlying QUIC connection and does not enforce HTTP/3 stream direction rules. For example, if a Kubernetes provider cannot mix TCP and UDP ports in one internal LoadBalancer Service, expose backend-facing gRPC/TCP and custom QUIC/UDP with separate single-protocol Services.

Use `webtransport` when validating or deploying behind an H3/WebTransport L7 hop. The local integration test temporarily rewrites advertised stargate pod hostnames to an in-cluster H3/WebTransport L7 proxy, switches both ends to `webtransport`, and verifies a reverse-tunnel request reaches the expected backend.

## Performance Notes

Use only long networking benchmarks as performance evidence. Short transport runs are useful as smoke tests but are too noisy to compare protocol overhead.

The loopback transport benchmark compares identical one-stream-per-request behavior for:

- `custom-protocol`
- `http3-h3-quinn`
- `webtransport-h3-quinn`

The latest long local run used:

```bash
cargo run --release -p stargate-bench -- transport-bench \
  --requests 20000 \
  --concurrency 256 \
  --warmup-requests 1000 \
  --output-dir .bench-out/transport
```

On that run, `custom` had the highest throughput, `http3` was close behind, and `webtransport` was lower-throughput with higher p95/p99 latency. Treat this as loopback wire-format evidence, not a production capacity model. Validate production choices through the Kubernetes benchmark path with representative load balancers, packet loss, MTU, and endpoint CPU limits.

## References

- HTTP/3: RFC 9114, especially the server-initiated bidirectional stream restriction.
- WebTransport over HTTP/3: IETF WebTransport draft, especially the extended CONNECT session and bidirectional WebTransport stream model.
