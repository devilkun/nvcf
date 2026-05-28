# Agent Architecture Reference

This file is the deeper Stargate reference for agents. Keep `AGENTS.md` compact and put detailed architecture, protocol, and operational notes here.

## What Stargate Does

- `stargate` is the control-plane and routing entrypoint for inference servers.
- Inference servers stream model stats into `RegisterInferenceServer`; stargate stores these snapshots in concurrent routing state.
- A `RegisterInferenceServer` stream is closed when no update arrives within the configured idle window. Heartbeat-aware clients negotiate `max(server floor, 3 * heartbeat)` and then apply the server cap; clients without heartbeat metadata use the server cap as their stale-route fallback.
- HTTP inference requests are proxied to a selected inference server using `(routing_key, model_id)` load-balancing logic.
- The routing key is derived from client authentication, not supplied by the inference server in the registration proto.
- `/v1/chat/completions`, `/v1/responses`, and `/v1/embeddings` proxy over QUIC tunnel transport. The tunnel protocol is selectable with `--tunnel-protocol=custom|http3|webtransport`; both stargate and pylon must use the same value. Direct backends advertise `quic://` inference server URLs; reverse-tunnel backends advertise their upstream HTTP URL and are reached through the pylon-initiated reverse QUIC connection.
- Operational transport guidance lives in `docs/tunnel-transports.md`. In short: use `custom` for the default L4 UDP path, `http3` for direct/client-initiated H3 request-stream experiments or L4-only reverse tests, and `webtransport` when reverse tunnels must work through an H3/WebTransport-aware L7 hop.
- `WatchStargates` exposes a continuous snapshot stream of all known local stargates plus remote-region watch endpoints so clients can diff and react.
- Stargate membership is built from discovery plus self address. Remote-region URLs are separate watch seeds and do not imply routing-state replication.
- Pylons are expected to register directly with every stargate they should be reachable through.

## Routing Identity

- `RoutingTargetKey { routing_key, model_id }` is the composite key for routing state lookups, load-balancer dispatch, and metrics labeling.
- `routing_key` is `Option<String>`. With `OpenAuthenticator`, it is `None`.
- `WorkerAuthenticator` owns registration identity. Current implementations are `GrpcWorkerAuthenticator` and `OpenAuthenticator`.
- HTTP clients may include optional `x-routing-key`; when omitted, the routing key is `None`.
- A single routing key may register multiple models; each `(routing_key, model_id)` pair is an independent routing target.
- The current wire format and server code do not enforce an authenticated allowed-model set.

## HTTP And QUIC Proxy

- Stargate receives OpenAI-compatible inference POSTs (`/v1/chat/completions`, `/v1/responses`, and `/v1/embeddings`) with required `x-request-id`, `x-model`, and `x-input-tokens`; `x-routing-key` is optional.
- Load-balancer object config may additionally require `x-cache-affinity-key`; missing required headers return HTTP `400`.
- Stargate must never parse request bodies in proxy paths. Request bodies are opaque bytes and must be forwarded as-is to upstreams.
- The proxy selects an inference server using the configured load-balancing algorithm from `RegisterInferenceServer` snapshots.
- Proxying uses an already-established QUIC connection set keyed by `inference_server_id`; a fresh request stream is opened per request on one healthy connection from that set. `--direct-quic-connections` controls the set size for direct `quic://` backends and defaults to `1`; reverse tunnels still install the pylon-initiated connection. `custom` uses Stargate's Cap'n Proto tunnel framing on a raw bidirectional QUIC stream. `http3` uses HTTP/3 request and response streams over the selected Quinn connection. `webtransport` establishes one HTTP/3 extended CONNECT session per selected connection and then sends each proxied request on a WebTransport bidirectional stream with a WebTransport-specific HTTP proxy payload: request/response head blocks plus raw body bytes. It must not reuse the custom Cap'n Proto tunnel frame protocol. Plain HTTP/3 is not the right reverse-tunnel contract for H3-aware L7 intermediaries because the reverse path requires Stargate to open streams toward pylon; WebTransport supplies that session-level stream model.
- Direct QUIC backends must register a `quic://` `inference_server_url`.
- Reverse-tunnel backends register their upstream HTTP URL as metadata and set `reverse_tunnel = true`; the proxy still sends requests over the already-established reverse QUIC connection.
- Active backends only enter the routing map after stargate has both an open QUIC path and a successful `/health` RTT sample.
- Backend RTT is measured by a registration-scoped `GET /health` ping loop over the forwarded QUIC path, not by QUIC transport stats.
- Stargate can retry direct QUIC setup races and retry/fail over explicit retryable upstream responses when the body is replayable within the configured replay limit.
- The proxy buffers opaque body bytes only for replay and must not inspect or parse them.
- Before forwarding to pylon, the proxy strips any caller-supplied `x-stargate-expected-queue-ms` and then injects its own routing-time queue estimate when available. Pylon uses that internal value only for local queue-mismatch admission and strips it before forwarding to the upstream HTTP server.
- Stargate applies an optimistic local routing reservation before each upstream attempt. It releases that reservation immediately only for an explicit Pylon `queue_estimate_mismatch` rejection, because that signal guarantees the attempt did not execute upstream; ordinary retryable responses retain their reservation until refreshed by registration state. A queue-mismatch rejection excludes only the rejecting backend first, so an untried sibling in the already selected cluster may serve the request before Stargate fails over to another cluster.
- Stargate adds `x-inference-server-id` and `x-inference-server-url` response headers.

## Pylon Request Contract

A pylon is Stargate's backend sidecar, formerly named `stargate-client`.

- The pylon QUIC tunnel requires `x-request-id`, `x-model`, and `x-input-tokens` on all non-health proxied inference requests.
- `x-routing-key` is optional and participates in the `RoutingTargetKey` when present.
- `x-cache-affinity-key` is optional globally, but required for models configured with `require_cache_affinity_key = true`.
- Missing required tunnel headers are client errors; the tunnel must return HTTP `400` rather than forwarding upstream.
- `x-request-id` is globally unique and is the canonical request identity for pylon-side request observation.
- `x-stargate-retryable`, `x-stargate-retry-reason`, and `x-stargate-retry-after-ms` are tunnel response metadata emitted by pylon, not caller-supplied request headers.
- Pylon may emit retryable `429` with `x-stargate-retry-reason: queue_estimate_mismatch` before upstream forwarding when local queue admission estimates exceed Stargate's routing-time estimate beyond tolerance.
- When pylon has a valid prompt-throughput estimate and its queued prompt work drains, it publishes an explicit empty priority queue map so prior registered queue estimates are cleared rather than preserved as stale state.
- Tunneled `/v1/chat/completions` and `/v1/responses` bodies must be valid JSON with `"stream": true`. Tunneled `/v1/embeddings` bodies must be valid JSON and do not require a `stream` field.
- Pylon-side streaming request observation for chat completions and Responses API assumes streamed responses. Output progress is derived from SSE `data:` events, not non-streaming JSON bodies. Embeddings observations do not parse response bodies; they use `x-input-tokens` for input TPS and retain parsed request `input` cardinality for pylon-local embedding item throughput metrics when available. Generic output-TPS model stats remain streaming generation token/sec fields.
- Request-observer terminal transitions are invariants. Calling terminalization logic from an already terminal state is a bug and should fail loudly.

## Registration Bringup

See `docs/coordinated-calibration-state-machine.md` for the coordinated calibration state machine definition.

- The pylon has a per-model lifecycle: `Connecting/Unavailable`, `AwaitingClusterCalibration`, `Calibrating`, `Recovering`, then `AdvertisingActive`.
- Registration status is per-model after lifecycle gating. A model advertises `Active` only when the caller-provided base status is active and the pylon bringup state is `AdvertisingActive`.
- Bringup calibration runs against the local upstream HTTP server, not the advertised `inference_server_url`.
- When the advertised URL is `quic://...`, callers must also supply the direct local HTTP base URL.
- Pylons with bringup calibration and coordinated calibration enabled set `coordinated_calibration=true` in registration updates. Before the selected calibration router reports cluster calibration complete for all local models, the pylon opens registration streams to one Stargate only; after completion, it fans out to the remaining discovered Stargates. Stargate then assigns exactly one calibration owner for each `(routing_key, cluster_id, model_id)` it sees and returns that decision as `CalibrationState` in `InferenceServerAck.model_calibration_directives`.
- Non-owner pylons advertise local backend activity after Stargate returns `CALIBRATION_STATE_WAITING`; they do not wait for another backend's calibration to finish. Stargate keeps routing gated while cluster calibration is pending. When Stargate accepts a positive completed-calibration `last_mean_input_tps`, later ACK directives report `CALIBRATION_STATE_COMPLETE` and include the cluster capacity value so sibling pylons can advertise the same seed without rerunning calibration.
- Calibration sends `/v1/chat/completions` requests across prompt sizes and concurrent request counts to seed `last_mean_input_tps` before the model is advertised active.
- Pylon also feeds the calibrated `last_mean_input_tps` seed into local queue-mismatch admission before the model becomes routable. Runtime-learned input throughput supersedes that seed, and clearing calibration does not erase a valid runtime value.
- During coordinated calibration, pylon registrations still use `CalibrationState` as proof of completion. Runtime `last_mean_input_tps` data alone is not evidence that coordinated calibration completed.
- Active canaries begin only after the model is advertised active. A canary failure demotes only that backend model into `Recovering`; cluster calibration is not re-entered after the initial cluster calibration has completed.
- The built-in active canary is a deterministic `1+1=` chat request. Completing exactly at `canary_max_generation_threshold` is treated as runaway generation and demotes the model.
- Reverse-tunnel connectivity gates advertisement per stargate. A reverse-tunnel model can be calibrated but still advertise `Inactive` to a specific stargate until that router-local reverse connection is established.

## Load Balancing

- Load balancing is pluggable via the `LoadBalancer` trait in `crates/stargate/src/load_balancer/mod.rs`.
- Built-in algorithms are `power-of-two` (default), `groq-multiregion`, `round-robin`, `random`, and `pulsar`.
- `LoadBalancerRouter` dispatches per-model-id to a configured algorithm or falls back to the default.
- `LoadBalancerRequest` carries request-scoped routing inputs: `RoutingTargetKey`, optional `x-cache-affinity-key`, optional `x-input-tokens`, optional `x-priority`, optional `x-request-slo-ms`, and routing start time.
- `power-of-two` samples 2 random candidates and picks the lower current load score. The score uses queued plus incoming prompt work divided by candidate `ModelStats.last_mean_input_tps`.
- `groq-multiregion` estimates TTFT from backend RTT plus queued and incoming input-token work using registered `ModelStats`; priority-aware queue time is preferred when populated. A non-empty sparse priority map is authoritative: if it contains only lower-urgency work than the request, queued work ahead of that request is known to be zero rather than the aggregate prompt queue.
- `groq-multiregion` can use `x-cache-affinity-key` for stable per-key subset routing before normal TTFT selection.
- `pulsar` uses weighted rendezvous hashing to create a deterministic per-request ranking of all candidates, then walks that ranking until it finds the first feasible backend.
- `pulsar` feasibility is computed from request headers and backend registration snapshots. It does not parse request bodies.
- For `pulsar`, stable capacity weight is candidate `ModelStats.last_mean_input_tps`; candidates without a positive `last_mean_input_tps` do not participate in the ranking. For shared clusters this value is the sum of positive/finite active backend reports.
- Optional `max_input_work_seconds` admission compares pool-wide queued plus incoming prompt work to available `last_mean_input_tps` before ranking. Missing valid capacity or work above the configured limit returns `503`. Unknown or unregistered target misses return `404 no_eligible_candidates`; registered targets with zero eligible active candidates return `503`.
- Accepted limitation: `last_mean_input_tps` does not carry source provenance. A shared cluster can temporarily overstate available input capacity when a calibration-seeded reporter is summed with runtime-observed sibling reporters before traffic has refreshed every pylon's sticky runtime mean.
- PULSAR ranking should follow stable capacity ownership, not transient relative load. Live load belongs in feasibility gates so affinity keys do not flap across backends.
- Configuration is via optional JSON passed with `--lb-config-path`. If omitted, `power-of-two` is used for all models.
- The proxy accepts request-scoped `x-routing-method` algorithm overrides for algorithms preconfigured in `request_algorithms`. Top-level `request_algorithms` applies to every model, and detailed model configs can override or add model-specific request algorithms.
- Canonical config keys are `groq-multiregion`, `power-of-two`, `pulsar`, `random`, and `round-robin`. The request header also accepts underscore aliases derived by replacing hyphens with underscores, such as `groq_multiregion`, `power_of_two`, and `round_robin`, then normalizes internally.
- Missing `x-routing-method` uses the configured or default algorithm. Blank values, unknown values, or known values that are not configured for the target model, return `400 BAD_REQUEST` and are logged with model, routing key, requested algorithm, and rejection reason.
- New algorithms are added by implementing `LoadBalancer` and registering in `create_load_balancer()`.

## Discovery And Kubernetes Connectivity

- Discovery finds stargates on an interval.
- In Kubernetes, `HeadlessDnsDiscovery` resolves the `stargate-headless` Service's named gRPC SRV records via hickory DNS.
- Outside Kubernetes, `DnsDiscovery` resolves `stargate_discovery_dns_name` via hickory DNS and ignores non-self loopback or unspecified DNS aliases so `localhost` does not create fake peers.
- Single-node or manually seeded local deployments should use `--disable-dns-discovery` to publish only the local stargate in `WatchStargates`.
- In K8s mode, `stargate_discovery_dns_name` must point at the headless Service, such as `stargate-headless.<namespace>.svc.cluster.local`. Discovery and peer forwarding both use this headless Service DNS path so Kubernetes EndpointSlice readiness controls which peers are visible.
- In K8s mode, `/healthz` only reports process liveness and `/readyz` reports application readiness. Kubernetes Services should route traffic only to ready pods.
- `WatchStargates` returns snapshots made from discovered peers plus self address in `stargates`, plus configured remote-region `WatchStargates` endpoints in `watch_stargate_urls`.
- Pylons recursively watch seed URLs and `watch_stargate_urls`, wait until every currently discovered watch endpoint has produced a snapshot, then open `RegisterInferenceServer` streams only to concrete entries returned in `stargates`.
- Pylons sort discovered registration targets locally before selecting the single coordinated-calibration router. In StatefulSet deployments this means all pylons choose the same first pod as long as stargate IDs are stable pod names.
- In Kubernetes, stargate advertises per-pod backend gRPC hostnames derived from `--advertised-hostname-template` (default `{pod_name}.stargate.external`). The `{pod_name}` placeholder is the StatefulSet pod hostname, such as `stargate-0`. A DNS rewrite rule maps the wildcard domain to the ClusterIP service.
- In Kubernetes, `WatchStargates.http_advertise_addr` is empty. HTTP proxy traffic is local-only to the selected stargate and must use the load-balanced `stargate-proxy` Service rather than per-pod targets.
- Raw pod IPs are not a supported client contract.
- K8s exposes three client-facing stargate services: `stargate` for backend-client gRPC/QUIC registration traffic, `stargate-model-discovery` for load-balanced `ListModels`, and `stargate-proxy` for load-balanced OpenAI-compatible HTTP proxy traffic.
- Pylons use the `stargate` ClusterIP service for in-cluster backend gRPC registration and, by default, reverse tunnels. For example, a Kubernetes deployment can keep that ClusterIP Service and additionally expose `stargate-grpc-lb` and `stargate-quic-lb` as separate internal L4 LoadBalancer Services when the provider cannot mix TCP and UDP ports in one Service. When `--reverse-tunnel-pylon-dial-addr` is configured, `InferenceServerAck.reverse_tunnel_pylon_dial_addr` is the pylon QUIC dial address and `reverse_tunnel_target` remains the per-pod SNI/routing identity.
- The optional `stargate-k8s-router` Deployment can front only the backend-facing `stargate` Service for the `custom` tunnel transport path. It watches ready EndpointSlices for `stargate-headless` and routes gRPC by HTTP/2 `:authority` and custom QUIC reverse tunnels by SNI directly to the selected pod IP. It is not an HTTP/3/WebTransport-aware L7 proxy; WebTransport deployments should bring an H3/WebTransport-aware load balancer instead. When the router is deployed, backend namespaces should reach real stargate pods only through the router, and the Stargate namespace must carry the NetworkPolicy role label that allows router-to-pod traffic. Router QUIC relay idle timeout and keepalive settings are separate from the peer dial timeout so reverse tunnels can remain open while idle between inference requests. In secure QUIC mode, the router preserves the inbound SNI when dialing the selected pod, so Stargate pod certificates must cover the advertised per-pod hostname template. `stargate-model-discovery` and `stargate-proxy` must continue to select the real stargate pods.
- The LLM gateway/API frontend calls `stargate-model-discovery` and `stargate-proxy` through normal Kubernetes service load balancing. It does not target specific stargate pods.
- `ListModels` is local-only. It is handled by the stargate pod selected by Kubernetes service load balancing and must never forward to another stargate pod.
- If the optional router is not deployed, Stargates transparently relay gRPC registrations and QUIC reverse-tunnel connections to the correct peer pod when a client lands on the wrong pod.
- Relay routing uses gRPC `:authority` and QUIC SNI to extract the advertised pod hostname, then connects to `{pod-name}.<stargate_discovery_dns_name>` while preserving the original advertised hostname as the QUIC TLS server name. QUIC relay endpoints use a long idle timeout and keepalive by default rather than the short connection timeout used for dialing peers.
- If backend-facing gRPC or QUIC traffic arrives for an advertised peer that is not in headless Service DNS, the peer connection fails unavailable instead of being handled locally. This keeps misdelivered registration and reverse-tunnel traffic from corrupting local routing state during startup or pod churn.
- Stargates do not replicate or share routing state. HTTP proxy requests and `ListModels` requests are served from local state only and are not forwarded between peers.
- `ListModels` is served by the dedicated `StargateModelDiscovery` gRPC service, not by the backend-facing `StargateControlPlane` service that owns `WatchStargates` and `RegisterInferenceServer`. It exposes a periodically refreshed local snapshot of active model IDs on the selected stargate instance for the requested `routing_key`; omitted or blank `routing_key` means the unscoped `None` routing key. It supports optional `model_ids` filtering with `x-model`-style trimming, rejects blank model filter entries, and intentionally does not expose backend, cluster, or per-backend routing-key internals in the response. Stale registrations leave routing state when their registration stream closes or exceeds the configured idle timeout, then disappear from `ListModels` after the next snapshot refresh.
- `ListModels` is an eventually consistent discovery hint, not a routing reservation. The LLM gateway/API frontend can identify unknown or unregistered local target misses by `404 NOT_FOUND` with `x-stargate-error-code: no_eligible_candidates`. After a recent positive discovery result, that response may be treated as a transient registration-convergence race and retried after the configured delay; without a recent positive discovery result, it means the model is unavailable to the current proxy request. Registered targets that have no eligible active candidates return `503 SERVICE_UNAVAILABLE`.

## Observability

- Per-request distributed tracing is implemented in `crates/stargate/src/http_proxy.rs` by manually creating the `proxy_openai_request` span, applying the remote parent before entering it, and instrumenting the request future with that span.
- The main proxy span records request path, request routing inputs, selected instance metadata, routing algorithm, candidate count, rank depth, retry data, upstream status, TTFT, failed-backend count, and replay-buffer bytes.
- OpenTelemetry export is opt-in via `--otel-endpoint` and uses OTLP/gRPC; only proxy root spans and children are exported.
- Telemetry initialization lives in `crates/stargate/src/telemetry.rs`; `TelemetryGuard` flushes spans on drop.
- Stargate Prometheus metrics are served on `--metrics-port` (default `9090`) at `GET /metrics`.
- Pylon Prometheus metrics are served on `--metrics-port` (default `9089`) and `--metrics-host` (default `0.0.0.0`) at `GET /metrics`.
- `stargate-k8s-router` serves Prometheus metrics on its health listener at `GET /metrics`; `stargate_k8s_router_quic_connections_total` reports custom-QUIC reverse-tunnel relay outcomes such as accepted, completed, rejected SNI, unavailable target, and relay error.
- Metrics are defined with per-runtime or per-process Prometheus registries so parallel tests do not share collectors.
- A local development stack can include an OTel Collector that receives Stargate traces over OTLP/gRPC on `4317` and forwards its Prometheus exporter for local inspection.
- `stargate-bench run` records a collector baseline containing `stargate_requests_total` and `pylon_requests_total` after readiness probing, waits for both counters to advance after replay, writes both retained scrapes, reports queue-admission and proxy-retry evidence as post-replay counter deltas, and fails if progress cannot be observed.
- Generated benchmark pylons disable bringup/canaries and pin registered plus local queue-admission input TPS to each backend profile's `registration.last_mean_input_tps`; this controls prompt-rate estimation while leaving unknowable output-generation residency outside the admission model.

## Important Files

- `crates/stargate/src/http_proxy.rs`: HTTP proxy, routing invocation, retry/failover, tracing, metrics.
- `crates/stargate/src/load_balancer/`: load-balancer implementations and router.
- `crates/stargate/src/load_balancer_state.rs`: registration snapshots and routable state.
- `crates/stargate/src/metrics.rs`: stargate Prometheus metrics.
- `crates/stargate/src/telemetry.rs`: OpenTelemetry setup.
- `crates/stargate-telemetry/src/lib.rs`: shared OpenTelemetry setup and trace-context propagation helpers.
- `crates/pylon-lib/src/`: sidecar registration, tunnel handling, observation, metrics, bringup/canary behavior.
- `crates/protocol/`: QUIC tunnel framing protocol.
- `crates/proto/`: protobuf-generated API surface.
- `crates/stargate-tls/`: QUIC TLS helper code.

## Terminology

- **Inference server**: a complete inference deployment that registers with stargate via `RegisterInferenceServer` and receives proxied requests. Identified by `inference_server_id` and reachable at `inference_server_url`.
- **Pylon**: Stargate's backend sidecar, formerly named `stargate-client`. A pylon runs next to an inference server and owns registration fanout, QUIC tunnel transport, local upstream forwarding, request observation, metrics, bringup calibration, active canaries, and reverse tunnels.
- **Routing key**: authenticated identity string derived exclusively from `WorkerAuthenticator` during gRPC registration. It is not supplied by the inference server in the registration proto. HTTP clients may include it in optional `x-routing-key`; when omitted, it is `None`.
- **RoutingTargetKey**: named struct `{ routing_key: Option<String>, model_id: String }` used as the composite key for routing state, load-balancer dispatch, and metrics labeling.
- **Cache affinity key**: opaque caller-supplied `x-cache-affinity-key` representing a stable prompt-prefix or KV-cache identity. Stargate does not inspect request bodies to derive it.
