# Stargate

Stargate is a control-plane and HTTP routing service for Inference servers.

It consolidates routing, model-discovery, and gateway concepts into a single control-plane and proxy service.

It does three primary jobs:
- accepts inference server model stats via gRPC (`RegisterInferenceServer`),
- exposes cluster membership via gRPC snapshot stream (`WatchStargates`),
- proxies inference HTTP requests to a selected inference server.

## Workspace layout

- `crates/stargate`: main server binary (`stargate`)
- `crates/stargate-auth`: shared auth token provider (`stargate-auth`)
- `crates/proto`: protobuf definitions + generated Rust types
- `crates/protocol`: QUIC tunnel framing and stream helpers (`stargate-protocol`)
- `crates/stargate-tls`: shared QUIC TLS helpers (`stargate-tls`)
- `crates/stargate-k8s-router`: optional Kubernetes backend-facing gRPC/QUIC router (`stargate-k8s-router`)
- `crates/pylon-lib`: pylon backend sidecar library (`pylon-lib`)
- `crates/mock-dynamo`: mock OpenAI-style HTTP server (`mock-dynamo`) for tests
- `crates/stargate-bench`: Kubernetes benchmark runner (`stargate-bench`)
- `crates/pylon`: QUIC tunnel + registration CLI (`pylon`) for tests and integration
- `docs/diagrams`: PlantUML sources (`.puml`)

## Pylon

A pylon is Stargate's backend sidecar, formerly named `stargate-client`. It runs next to an inference server and owns Stargate registration, QUIC tunnel transport, local upstream forwarding, request observation, metrics, bringup calibration, active canaries, and reverse tunnels.

## Pylon request contract

The pylon tunnel expects proxied requests to include these headers. The tunnel
wire format is selected with `--tunnel-protocol=custom|http3|webtransport`; both
stargate and pylon must use the same value.

- `x-request-id`
- `x-model`
- `x-input-tokens`
- `x-routing-key` (optional)
- `x-priority` (optional, defaults to `0`)
- `x-cache-affinity-key` (required only for models configured with `require_cache_affinity_key=true`)

Current behavior:

- if any required header (`x-request-id`, `x-model`, `x-input-tokens`) is missing from a proxied inference request, the pylon tunnel rejects the request with HTTP `400`
- `x-routing-key` identifies the authenticated routing identity (derived from the `WorkerAuthenticator` during registration); together with `x-model` it forms the `RoutingTargetKey` used for server selection. When omitted, the routing key is `None` internally (matching `OpenAuthenticator`)
- `x-priority` selects priority-specific queue-time estimates for `groq-multiregion` when backends publish `queue_time_estimate_ms_by_priority`
- `x-cache-affinity-key` is an opaque stable prefix / KV-cache identity supplied by the caller. Stargate does not parse request bodies to derive it.
- `x-request-id` is treated as the canonical globally unique request id for pylon-side request observation
- Stargate may add the internal `x-stargate-expected-queue-ms` header when it forwards a selected request to pylon. Stargate strips any caller-supplied value before forwarding, and pylon strips the header before calling the local upstream.
- when pylon's local queue estimate is higher than Stargate's routing-time estimate beyond the configured tolerance, pylon returns a retryable `429` with `x-stargate-retry-reason: queue_estimate_mismatch` so Stargate can try another eligible backend without the request reaching the first upstream.
- tunneled `/v1/chat/completions` and `/v1/responses` bodies must be valid JSON with `"stream": true`; tunneled `/v1/embeddings` bodies must be valid JSON and do not need a `stream` field
- streaming chat-completion and Responses API observation assumes streamed responses and derives output progress from SSE `data:` events

## Pylon bringup behavior

The pylon performs bringup gating before it advertises a model as `Active`.

- it waits for the local upstream HTTP server to respond on `/health`
- during coordinated calibration, it registers with one Stargate until that Stargate assigns or completes cluster calibration for each model
- the assigned pylon runs a `/v1/chat/completions` calibration sweep across prompt sizes and concurrent request levels to seed cluster `last_mean_input_tps`
- a non-owner pylon advertises its backend locally `Active` after Stargate returns `Waiting`, while Stargate keeps the cluster unroutable until calibration completes; after `Complete`, siblings publish the shared `last_mean_input_tps` without rerunning calibration, and later observations can replace that seed locally
- after coordinated calibration completes, it registers with the rest of the discovered Stargates
- if coordinated calibration is disabled, each pylon runs its own local calibration before advertising `Active`
- while active it runs periodic canaries in the background
- a failed active canary demotes the model back out of active advertisement until recovery succeeds

The registration client needs the local upstream HTTP base URL in addition to the advertised
`inference_server_url` when the advertised address is not itself HTTP, for example when direct
mode advertises a `quic://...` listener. In reverse tunnel mode the registered
`inference_server_url` is the local upstream HTTP URL, while stargate still proxies over the
pylon-initiated reverse QUIC connection.

## Pylon production contract

The pylon sidecar's supported responsibilities are:

- register one or more OpenAI-compatible model endpoints with every discovered Stargate
- maintain direct or reverse QUIC tunnel connectivity for proxied HTTP requests
- gate model advertisement through upstream health, coordinated calibration, and active canaries
- forward proxied requests to a local HTTP upstream without changing the caller body
- observe streamed chat-completion and Responses API responses for request lifecycle, token accounting, and retry metadata; optional quality metrics remain chat-completion-specific
- publish Prometheus metrics for registration connectivity, request lifecycle, model load, retry classification, quality checks, and advertised status

Pylon readiness is intentionally tied to advertised model state rather than process liveness. A pylon can be healthy but advertise `Inactive` while its upstream is unavailable, calibration is pending, a reverse tunnel is disconnected, or recovery canaries are still failing. Kubernetes deployments should use process health for container liveness and Stargate-visible advertised status for serving readiness.

The local upstream contract is HTTP today. gRPC upstream forwarding remains future design work because it would require explicit request translation, streaming/error mapping, and observation semantics.

## Shared backend clusters

Multiple pylon registrations can share a logical `cluster_id` when they front the same hardware or scheduler. When `cluster_id` is omitted, pylon and Stargate normalize it to `inference_server_id`, preserving the one-backend-per-cluster behavior.

Stargate load balancers choose from cluster-level candidates. Backend-scoped metrics such as `last_mean_input_tps`, queue size, queued input tokens, and output TPS are aggregated across active backends in the cluster. Cluster-scoped state such as KV-cache capacity and scheduler queue-time estimates is treated as shared hardware state. After a cluster is selected, Stargate chooses a concrete active backend inside that cluster with per-cluster round robin, excluding backends that already failed the current request.

Known limitation: `last_mean_input_tps` intentionally carries both calibration seeds and runtime observations through one sticky field. Stargate does not track source provenance when aggregating a shared cluster, so startup or uneven traffic can temporarily overstate cluster input capacity if a calibration-seeded backend and runtime-observed sibling backends are summed together. The estimate usually settles as round-robin traffic refreshes each pylon's runtime mean, but `max_input_work_seconds`, PULSAR weights, and other capacity-driven decisions should be treated as advisory rather than hard scheduler guarantees during that convergence window.

## Quality monitoring and canaries

Active canaries are serving gates: pylon periodically sends a deterministic chat-completion request, and failed or runaway canary responses demote the model out of active advertisement until recovery succeeds.

Optional quality monitoring is observational by default. When enabled with `--collect-quality-metrics` or threshold flags, pylon evaluates streamed chat-completion output after the response and emits `pylon_quality_*` metrics for clean, matched, or skipped checks. These checks do not currently demote a model; wiring quality threshold matches into active canary demotion is a separate policy decision.

## How Stargate works

- Inference servers open a stream to `RegisterInferenceServer` and periodically send model stats.
- Stargate closes registration streams when no update arrives within the configured idle window. Heartbeat-aware clients negotiate `max(server floor, 3 * heartbeat)` and then apply the server cap; legacy/no-header streams use the server cap as their stale-route fallback.
- Stargate keeps concurrent `(routing_key, model_id)` -> inference server snapshots in memory. The routing key is derived from the `WorkerAuthenticator` during registration authentication, not from the client registration message.
- `/v1/chat/completions`, `/v1/responses`, and `/v1/embeddings` require `x-request-id`, `x-model`, and `x-input-tokens`; `x-routing-key` is optional. The routing key and model pair forms a `RoutingTargetKey` used for inference server selection. When `x-routing-key` is omitted, the routing key is `None` (matching `OpenAuthenticator`). Direct backends advertise `quic://` inference server URLs; reverse-tunnel backends advertise their upstream HTTP URL but are still reached through the already-established reverse QUIC connection.
- Models configured with load-balancer object settings can additionally require `x-cache-affinity-key`. Missing required load-balancer headers return HTTP `400`.
- `WatchStargates` emits full snapshots (not deltas): DNS-discovered stargates + self, plus optional remote-region watch endpoints in `watch_stargate_urls`.
- In Kubernetes, stargate advertises per-pod backend gRPC hostnames derived from `--advertised-hostname-template`. A DNS rewrite rule maps the configured wildcard domain to the ClusterIP service, and server-side forwarding uses the gRPC `:authority` or QUIC SNI target to route connections to the correct pod.

## CI/CD

Recommended validation for feature and PR builds:

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features --no-deps -- -D warnings`
- `cargo check --workspace --all-features`
- `cargo test --workspace`

Expected local validation environment:

- `protoc` and `capnp` available on `PATH` for Rust compile/test steps

## Build

```bash
cargo build -p stargate
```

## Run locally

### 1) Single-node local dev (no DNS discovery)

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --model-discovery-listen-addr 127.0.0.1:50073 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery
```

Notes:
- gRPC control plane: `127.0.0.1:50071`
- gRPC model discovery (`ListModels`): `127.0.0.1:50073`
- HTTP proxy: `127.0.0.1:8000`
- `--disable-dns-discovery` keeps `WatchStargates` self-only for single-node local runs where `localhost` may resolve to multiple loopback aliases.

### 2) Multi-node local test (manual addresses)

Run two stargates on different ports:

```bash
cargo run -p stargate -- \
  --stargate-id local-a \
  --listen-addr 127.0.0.1:50071 \
  --model-discovery-listen-addr 127.0.0.1:50073 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery
```

```bash
cargo run -p stargate -- \
  --stargate-id local-b \
  --listen-addr 127.0.0.1:50072 \
  --model-discovery-listen-addr 127.0.0.1:50074 \
  --http-listen-addr 127.0.0.1:8001 \
  --advertise-addr 127.0.0.1:50072 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery
```

In local mode, seed behavior usually comes from pylon config (`crates/pylon-lib`) pointing at both stargates.

## Run Dynamo with Stargate locally

This is the simplest local integration loop for inference server registration.

1. Start Stargate:

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --model-discovery-listen-addr 127.0.0.1:50073 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery
```

2. Start Dynamo frontend (from the `dynamo` repo) and enable Stargate registration:

(opt: update bindings)
`cd lib/bindings/python; maturin develop --uv; cd ../../..`

```bash
python -m dynamo.frontend \
  --http-port 8001 \
  --stargate-registration \
  --stargate-address 127.0.0.1:50071 \
  --stargate-dynamo-inst-id local-inst-1 \
  --stargate-dynamo-inst-url quic://127.0.0.1:8001 \
  --stargate-heartbeat-ms 1000
```

Notes:
- `--stargate-dynamo-inst-url` must be a `quic://<host>:<port>` URL.
- If running multiple inference servers, give each a unique `--stargate-dynamo-inst-id` and URL.

## Docker

The Dockerfile build stage uses `rust:1.94-bookworm` together with `rust-toolchain.toml` so container builds use the same pinned stable toolchain as local development.

Build image:

```bash
docker build -t stargate:dev .
```

Run container:

```bash
docker run --rm -p 50071:50071 -p 50073:50073 -p 8000:8000 stargate:dev \
  --stargate-id local-1 \
  --listen-addr 0.0.0.0:50071 \
  --model-discovery-listen-addr 0.0.0.0:50073 \
  --http-listen-addr 0.0.0.0:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery
```

## Kubernetes deployment model

Typical setup:
- Deploy Stargate as a StatefulSet with `serviceName: stargate-headless`.
- Expose backend-client gRPC and QUIC ports in the `stargate` ClusterIP Service.
- Expose `ListModels` through the load-balanced `stargate-model-discovery` ClusterIP Service.
- Expose OpenAI-compatible HTTP proxy traffic through the load-balanced `stargate-proxy` ClusterIP Service.
- Add an internal headless `stargate-headless` Service for peer discovery and forwarding. Kubernetes publishes only ready endpoints there unless `publishNotReadyAddresses` is explicitly enabled, which this deployment should not do.
- Set `--advertise-addr` to pod IP + gRPC port and `--pod-name` to the Kubernetes pod hostname.
- Configure `--advertised-hostname-template` and a DNS rewrite rule so backend clients connect to per-pod hostnames through the ClusterIP, with server-side forwarding relaying to `<pod-name>.stargate-headless.<namespace>.svc.cluster.local`.
- Label the Stargate, backend-client, and gateway namespaces with the `stargate.nvidia.com/role` values expected by the base NetworkPolicies, such as `stargate`, `backend`, and `gateway`.

Example container args:

```yaml
args:
  - --stargate-id=$(POD_NAME)
  - --listen-addr=0.0.0.0:50071
  - --model-discovery-listen-addr=0.0.0.0:50073
  - --http-listen-addr=0.0.0.0:8000
  - --advertise-addr=$(POD_IP):50071
  - --stargate-discovery-dns-name=stargate-headless.$(POD_NAMESPACE).svc.cluster.local
  - --advertised-hostname-template={pod_name}.stargate.external
  - --pod-name=$(POD_NAME)
  - --pod-namespace=$(POD_NAMESPACE)
  - --reverse-tunnel-listen-addr=0.0.0.0:50072
  - --dns-poll-ms=1000
  - --dns-resolver-ttl-ms=1000
  - --watch-heartbeat-ms=5000
```

env:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

Health endpoints:
- HTTP liveness/readiness: `GET /healthz`, `GET /readyz`
- Optional gRPC probe binary is included in the Docker image as `/usr/local/bin/grpc_health_probe` when target platform args are available during build.

## Benchmarks

The benchmark crate includes local microbenchmarks plus a Kubernetes benchmark runner. Kubernetes deployment manifests are not part of this mirror surface.

### Transport microbenchmark

`stargate-bench transport-bench` compares the current custom Stargate QUIC framing protocol, HTTP/3 over Quinn, and WebTransport over HTTP/3 using `h3`/`h3-quinn`. It runs locally on loopback without Kubernetes, opens one or more established QUIC connections per transport, and opens one bidirectional stream per request.

```bash
cargo run --release -p stargate-bench -- transport-bench \
  --requests 20000 \
  --concurrency 256 \
  --quic-connections 1 \
  --warmup-requests 1000 \
  --trials 5 \
  --warmup-trials 1 \
  --randomize-order \
  --request-body-bytes 1024 \
  --response-body-bytes 1024 \
  --output-dir .bench-out/transport
```

The command prints comparable throughput, goodput, response-header latency, first-body latency, and completion latency. Repeated trials add aggregate confidence intervals and noise classification to the report. When `--output-dir` is set it writes `run-metadata.json`, `transport-summary.json`, `transport-report.md`, and per-transport sample JSONL files. Single-trial sample files are named `transport-samples-<transport>.jsonl`; repeated trials use `transport-samples-<transport>-trial-<N>.jsonl`. Each JSONL row includes `transport`, `trial_index`, and the request sample fields.

Use `--quic-connections N` to test sharding request streams across multiple QUIC connections to the same loopback server. Use `--disable-quic-send-fairness` to test Quinn's same-priority stream scheduler in write-order mode, and `--disable-http3-grease` to test HTTP/3 without reserved grease settings/frames.

### Load-balancer microbenchmark

`stargate-bench lb-microbench` measures in-process load-balancer `choose` overhead for the `groq-multiregion` and `pulsar` routing paths without Kubernetes, QUIC, or HTTP proxy noise. It prints CSV rows with per-scenario request count, concurrency, elapsed time, nanoseconds per choose, rank-depth summary, and selected-backend distribution.

```bash
cargo run --release -p stargate-bench -- lb-microbench \
  --iterations 100000 \
  --warmup-iterations 10000 \
  --concurrency 8 \
  --candidates 64 \
  --cache-key-count 1024
```

Use repeated `--scenario` flags to narrow a run:

```bash
cargo run --release -p stargate-bench -- lb-microbench \
  --scenario groq-multiregion-affinity \
  --scenario pulsar
```

Supported scenarios are `power-of-two`, `power-of-two-one-excluded`, `groq-multiregion`, `groq-multiregion-one-excluded`, `groq-multiregion-ignore-queue`, `groq-multiregion-ignore-queue-one-excluded`, `groq-multiregion-ignore-queue-multi-excluded`, `groq-multiregion-rtt-only`, `groq-multiregion-rtt-only-one-excluded`, `groq-multiregion-rtt-only-multi-excluded`, `groq-multiregion-affinity`, `groq-multiregion-affinity-one-excluded`, `groq-multiregion-affinity-multi-excluded`, `pulsar`, `pulsar-one-excluded`, `random`, `random-one-excluded`, `round-robin-one-excluded`, and `round-robin-multi-excluded`. Use `--concurrency` above 1 to measure shared-router contention under overlapping `choose` calls, and increase `--cache-key-count` above `--iterations` to approximate cold/high-cardinality affinity-key traffic.

### Header-filter microbenchmark

`stargate-bench header-filter-microbench` measures hot header forwarding predicates without QUIC, HTTP, or routing noise. It compares the old lowercasing matcher shape with the allocation-free static matcher shape used by Stargate proxy forwarding, Stargate HTTP/3 tunnel forwarding, Pylon upstream request forwarding, and Pylon tunnel response forwarding.

```bash
cargo run --release -p stargate-bench -- header-filter-microbench \
  --iterations 1000000 \
  --warmup-iterations 100000 \
  --header-count 128
```

The report prints nanoseconds per header and relative improvement for each forwarding path. Treat it as attribution evidence for predicate cost; keep end-to-end tunnel and proxy tests as the behavior proof for stripped and forwarded headers.

### Body-buffer microbenchmark

`stargate-bench body-buffer-microbench` measures Pylon-style request body accumulation without network or upstream HTTP noise. It compares grow-as-needed `Vec` buffering with preallocated buffering, and compares the previous H3 `copy_to_bytes` loop with copying directly from `Buf::chunk`.

```bash
cargo run --release -p stargate-bench -- body-buffer-microbench \
  --iterations 20000 \
  --warmup-iterations 2000 \
  --body-bytes 65536 \
  --chunk-bytes 1024
```

The report prints nanoseconds per buffered body and relative improvement for each copy path. Treat it as attribution evidence for body copy/allocation cost; keep Pylon tunnel tests as the behavior proof for exact forwarded bytes, max-size enforcement, and JSON validation.

### Run the sample benchmark

Kubernetes benchmark runs require an active `kubectl` context and cluster-visible images for Stargate, Pylon, and mock-dynamo. Set the image contract explicitly in OSS checkouts:

```bash
export STARGATE_BENCH_STARGATE_IMAGE=<registry>/stargate:<tag>
export STARGATE_BENCH_PYLON_IMAGE=<registry>/pylon:<tag>
export STARGATE_BENCH_MOCK_DYNAMO_IMAGE=<registry>/mock-dynamo:<tag>
```

```bash
cargo run -p stargate-bench -- run \
  --scenario hotset-8-backends \
  --output-dir .bench-out/hotset-k8s-smoke
```

Run the pylon queue-estimate mismatch admission A/B diagnostic:

```bash
cargo run -p stargate-bench -- run \
  --scenario queue-mismatch-retry-ab \
  --output-dir .bench-out/queue-mismatch-retry-ab
```

That scenario replays one bursty `groq-multiregion` workload twice with identical topology, traffic, routing config, and tolerance knobs; only `pylon_queue_admission.enabled` changes. Generated benchmark pylons disable bringup/canaries and pin the registered and locally consumed prompt-throughput value to each backend profile's `registration.last_mean_input_tps`; in this scenario it matches the mock's `prefill_tokens_per_s: 2200`. The mock enforces concurrent request slots plus real TTFT, prefill, and decode delays. Queue-mismatch admission intentionally models queued prompt work only: decode continues to occupy a mock slot, but arbitrary output length cannot be known when the request is admitted.

Its `report.md` places `Pylon Rejected`, `Pylon Disabled`, `Queue Mismatch Retries`, and reason-labeled `Retry Exhausted` counters alongside latency. These are post-replay counter deltas, so the pre-replay routability probe is excluded. Nonzero rejected and queue-mismatch retry counts prove the enabled run exercised the mechanism; zero counts mean the run did not trigger it. Local benchmark results are diagnostic evidence, not release-grade performance proof.

List the built-in benchmark scenarios:

```bash
cargo run -p stargate-bench -- list-scenarios
```

Each scenario can include embedded `metadata` with a description, tags, expected runtime, and expected signal. The scenario list and generated report use that metadata to make benchmark intent visible.

For quick algorithm iteration, filter a scenario to one or more algorithms:

```bash
cargo run -p stargate-bench -- run \
  --scenario hotset-8-backends \
  --algorithm pulsar \
  --output-dir .bench-out/hotset-pulsar
```

If `--output-dir` is omitted for `run`, `prepare-run`, or `materialize`, the runner writes to `.bench-out/<benchmark-name>`.

This command:

- generates a deterministic manifest from the benchmark config
- creates one temporary Kubernetes environment per algorithm
- waits for stargate health and for `stargate_active_inference_servers` to reach the configured backend count
- exposes benchmark HTTP traffic through the generated `stargate-http` NodePort service
- replays the same manifest against each load-balancer configuration
- records a pre-replay OTel Collector request-counter baseline, then waits for post-replay stargate and pylon request progress before reporting replay-only queue-admission and proxy-retry counter deltas
- writes `run-metadata.json` with metadata schema version, command, git, host CPU/load/cgroup/affinity, preflight, and Kubernetes context details
- prints progress for the selected scenario and each algorithm
- writes per-run artifacts plus `.bench-out/.../comparison.json` and `report.md`
- collects Kubernetes diagnostics under `run-<algorithm>/logs/` when a run fails or `--keep-resources-on-failure` is set

Useful outputs:

- `.bench-out/<run>/manifest.json`
- `.bench-out/<run>/run-metadata.json`
- `.bench-out/<run>/run-<algorithm>/stargate-external-services.yaml`
- `.bench-out/<run>/run-<algorithm>/requests.jsonl`
- `.bench-out/<run>/run-<algorithm>/summary.json`
- `.bench-out/<run>/run-<algorithm>/collector-metrics.prom`
- `.bench-out/<run>/run-<algorithm>/collector-baseline-metrics.prom`
- `.bench-out/<run>/run-<algorithm>/logs/` when diagnostics are collected
- `.bench-out/<run>/comparison.json`
- `.bench-out/<run>/report.md`

Regenerate a Markdown report from an existing output directory:

```bash
cargo run -p stargate-bench -- report \
  --output-dir .bench-out/hotset-k8s-smoke
```

`summary.json` and `comparison.json` report `max_ttlt_ms` as the largest per-request time-to-last-token observed during that algorithm run. They also report `total_length_ms`, the wall-clock span from the first dispatched benchmark request to the last completed benchmark request for that algorithm.

For heterogeneous backends, `capacity_balance_score` compares actual request share to the expected share from each backend profile's `registration.last_mean_input_tps`; `balance_score` remains the equal-share score. Backends can report per-request KV-cache outcomes with `x-kv-cache-hit`, `x-kv-cache-evicted-entries`, and `x-kv-cache-evicted-tokens`, so `summary.json`, `comparison.json`, and `report.md` include cache hit rate and eviction counts when those headers are present.

The report also includes token-weighted backend shares, per-backend latency/cache summaries, cache-key movement rate, and grouped failure classifications by status, backend, and error string.

Reports may include warnings for suspicious results, such as failures, low capacity-balance scores, or missing KV-cache headers in cache-focused scenarios. These warnings are informational only and do not make the benchmark command fail.

Example benchmark configs:

- `benches/uniform-4-backends.yaml`: homogeneous baseline with evenly shaped traffic
- `benches/hotset-8-backends.yaml`: short cache-affinity hotset sanity check across heterogeneous backends
- `benches/hotset-8-backends-long.yaml`: longer steady-state hotset run with the same heterogeneous topology
- `benches/bursty-8-backends.yaml`: alternating quiet and spike periods against mixed backend capacity
- `benches/mixed-size-pulsar.yaml`: small and large request mix with PULSAR KV-cache metrics enabled
- `benches/stair-step-2-stargates.yaml`: two-stargate service-load-balanced run with gradually increasing request rate
- `benches/overload-6-backends.yaml`: intentional overload run for failure diagnostics
- `benches/cache-thrash-6-backends.yaml`: cache set larger than aggregate KV capacity
- `benches/sticky-hot-prefix.yaml`: small hotset with high reuse to inspect cache locality
- `benches/backend-degradation.yaml`: deletes a backend pod during replay to inspect recovery
- `benches/queue-mismatch-retry-ab.yaml`: enabled-versus-disabled pylon queue-mismatch admission diagnostic under burst load

Notes:

- `stargates.count` controls how many benchmark stargate pods are created; the generated topology always uses a headless service plus StatefulSet, even when the count is `1`
- `backends.profiles` can split the backend pool into counted profile groups; each profile controls mock TTFT, prefill throughput, decode throughput, decode jitter, KV-cache capacity, and max concurrent processing slots
- mock benchmark backends track `x-cache-affinity-key` entries in a simple LRU KV cache; cache hits skip prefill and `pylon` polls `/kv-cache/stats` into registration metrics for PULSAR
- generated benchmark pylons register and use each backend profile's fixed `registration.last_mean_input_tps` value, with bringup calibration and active canaries disabled so scenario capacity is an explicit control input
- benchmark `pylon` pods report registration/stat updates every 100 ms so load balancers can react to in-flight mock backend load during short local runs
- an algorithm may set `pylon_queue_admission` with `enabled`, `min_delta_ms`, `tolerance_factor`, and `retry_after_ms`; when omitted, generated pylons retain their runtime defaults
- each generated benchmark environment includes an OTel Collector with a Prometheus receiver that scrapes stargate metrics and pylon metrics, then exposes the scraped series through its own Prometheus exporter
- the runner stages Kubernetes startup by applying stargate first, creating per-pod advertised external Services, then applying backend deployments so reverse-tunnel DNS is available before registration
- benchmark HTTP traffic uses the generated `stargate-http` NodePort service so requests enter through Kubernetes Service load balancing; set `STARGATE_BENCH_NODE_HOST` if the runner cannot infer a reachable node address
- the local benchmark path uses `OpenAuthenticator`, so benchmark traffic must not rely on non-`None` routing keys unless you also add a matching worker auth setup
- `pulsar` benchmark configs must keep `default` as a string algorithm name and put the detailed object form under `models.<model>`
- the generated output directory is ignored by git via `.bench-out/`

Reliability controls:

- `--reliability-mode smoke|controlled|strict` records locally checkable preflight metadata; `strict` fails before measurement when those checks fail
- `transport-bench` supports `--trials`, `--warmup-trials`, `--cooldown-ms`, `--randomize-order`, `--noise-threshold-cv`, and `--min-effect-size-percent`
- controlled transport measurements should build once with `cargo build --release -p stargate-bench` and run `target/release/stargate-bench` directly instead of measuring through Cargo
- in-cluster drivers, Kubernetes repeated-trial orchestration, privileged network shaping/calibration, host CPU mutation wrappers, and representative multi-node benchmarks are TODOs until they can be validated on appropriate benchmark infrastructure

## Deployment model

Every backend client is expected to register directly with every stargate that should be able to route to it.

`WatchStargates` remains the membership discovery API for that fanout. Stargates publish DNS-discovered peers plus self in `stargates`. They may also publish remote region `WatchStargates` endpoints in `watch_stargate_urls`. Clients recursively watch those remote endpoints, wait until every currently discovered watch endpoint has produced a snapshot, and do not register directly to the remote watch URLs; they register only to concrete stargate entries returned in `stargates`.

Clients sort discovered registration targets locally before choosing the coordinated-calibration router. This preserves the "first pod controls initial cluster calibration" contract even if protobuf or serde ordering changes upstream.

When `--advertised-hostname-template` is configured, the advertised addresses returned by `WatchStargates` and `InferenceServerAck.reverse_tunnel_target` become per-pod backend-facing hostnames rather than raw pod IPs. If `--reverse-tunnel-pylon-dial-addr` is set, pylons dial that address for reverse QUIC but keep `reverse_tunnel_target` as the SNI identity for pod routing. In Kubernetes, `WatchStargates.http_advertise_addr` is empty because OpenAI-compatible HTTP proxy traffic must use the load-balanced `stargate-proxy` Service, not per-pod targets.

In Kubernetes mode, the `{pod_name}` template variable is the StatefulSet pod hostname, such as `stargate-0`.

Stargates transparently relay gRPC registrations and QUIC reverse-tunnel connections to the correct peer pod, but do not replicate or share routing state. HTTP proxy requests are served from local state only and are not forwarded between peers.

The LLM gateway/API frontend uses Kubernetes service DNS for model discovery and proxy traffic. It calls the dedicated `StargateModelDiscovery/ListModels` gRPC service through the load-balanced `stargate-model-discovery` Service on port `50073`; the request is handled by whichever stargate pod Kubernetes selects and is never forwarded to another stargate pod. It sends OpenAI-compatible HTTP requests through the load-balanced `stargate-proxy` Service. `ListModels` accepts an optional `routing_key` and returns only active model IDs owned by that routing key in the selected pod's local snapshot. When `routing_key` is omitted or blank, only unscoped registrations are listed. Optional model filters are trimmed like the `x-model` proxy header, and blank filter entries are rejected. Backend, cluster, and routing-key internals are intentionally not exposed in the response. The list is served from a periodically refreshed local snapshot on the selected stargate pod, so it is eventually consistent with routing state rather than updated synchronously by every registration heartbeat.

`ListModels` is an eventually consistent discovery hint, not a routing reservation. A model can disappear after discovery, and different stargate pods can briefly disagree while backend clients fan out registrations. If the API frontend recently discovered model availability but a proxied request returns `404 NOT_FOUND` with `x-stargate-error-code: no_eligible_candidates`, it may treat that as a transient convergence race and retry after its configured discovery-convergence delay before reporting the model as unavailable. Without a recent positive discovery result, the same response means the model is unknown or unregistered on the current proxy request. If the selected stargate still has a registration for the requested model but no eligible active candidates, the proxy returns `503 SERVICE_UNAVAILABLE` instead.

See [`docs/api-gateway-contract.md`](docs/api-gateway-contract.md) for the full API gateway integration contract.

## Load balancer configuration

Stargate supports pluggable load balancing algorithms. By default it uses `power-of-two` (sample 2 random candidates, pick the lower estimated prompt-work time). You can override the default and set per-model algorithms via a JSON config file:

```json
{
  "default": "power-of-two",
  "models": {
    "my-large-model": "round-robin",
    "my-small-model": "random",
    "my-latency-sensitive-model": "groq-multiregion"
  }
}
```

Available algorithms:
- `power-of-two` -- sample 2 random candidates and pick the lower `(queued + incoming prompt work) / last_mean_input_tps` score (default)
- `groq-multiregion` -- estimate time-to-first-token from backend RTT plus queued and incoming input-token work, preferring priority-specific queue-time estimates when populated; optional `ignore_queue_time` and `ignore_input_processing_time` remove those components from TTFT bucket construction. It filters candidates above the configured queue-time SLO, unlocks later TTFT buckets as request wait time elapses, samples 2 candidates from the unlocked set, then picks by least queue time with least-percent-used as the tie-breaker. It can optionally use `x-cache-affinity-key` to try a stable per-key backend subset before falling back to all candidates.
- `round-robin` -- cycle through candidates sequentially
- `random` -- pick a random candidate uniformly
- `pulsar` -- weighted rendezvous hashing with per-request cache affinity and progressive unlocking across feasible backends

For simple per-model overrides, the existing string syntax still works. Algorithms that need model-specific behavior use the object form:

```json
{
  "default": "power-of-two",
  "models": {
    "llama-70b": {
      "algorithm": "groq-multiregion",
      "seed": "prod-seed-v1",
      "require_cache_affinity_key": true,
      "cache_affinity_virtual_nodes": 150,
      "cache_affinity_backend_selection_count": 1
    }
  }
}
```

`groq-multiregion` cache-affinity fields:
- `seed` -- stable seed mixed into the cache-affinity ring
- `require_cache_affinity_key` -- reject requests missing `x-cache-affinity-key`
- `cache_affinity_virtual_nodes` -- virtual nodes per backend in the stable affinity ring; defaults to `150`
- `cache_affinity_backend_selection_count` -- number of stable per-key backends to try before falling back to all candidates. Omit or set to `0` to disable this path.

`pulsar` also uses the object form:

```json
{
  "default": "power-of-two",
  "models": {
    "llama-70b": {
      "algorithm": "pulsar",
      "seed": "prod-seed-v1",
      "require_cache_affinity_key": true,
      "require_input_tokens": true,
      "require_kv_metrics": true
    }
  }
}
```

PULSAR-specific fields:
- `seed` -- stable seed mixed into the rendezvous hash
- `require_cache_affinity_key` -- reject requests missing `x-cache-affinity-key`
- `require_input_tokens` -- reject requests missing `x-input-tokens`
- `require_kv_metrics` -- only consider backends that advertise KV-capacity metrics

Backends that want to participate fully in `pulsar` should advertise these `ModelStats` fields during registration:
- `kv_cache_capacity_tokens`
- `kv_cache_used_tokens`
- `kv_cache_free_tokens`
PULSAR uses candidate `last_mean_input_tps` as its stable capacity weight. For shared clusters this is the effective cluster capacity: the sum of positive/finite active backend reports. Current load, queueing, and KV pressure act as feasibility gates; if they drive the base ranking instead, the same cache-affinity key will flap between backends and lose cache locality. During shared-cluster convergence, the summed sticky capacity can temporarily include both a calibration-seeded reporter and runtime-observed sibling reporters, so PULSAR may briefly overweight that cluster until runtime observations refresh the seeded backend.

Pass the config file at startup:

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery \
  --lb-config-path lb-config.json
```

If `--lb-config-path` is omitted, `power-of-two` is used for all models.

The HTTP proxy also accepts `x-routing-method` as an optional per-request routing algorithm override. Overrides are selected only from algorithms preconfigured at startup. Top-level `request_algorithms` applies to every model, and a detailed model config can override or add model-specific request algorithms:

```json
{
  "default": "power-of-two",
  "request_algorithms": {
    "round-robin": "round-robin",
    "random": "random"
  },
  "models": {
    "llama-70b": {
      "algorithm": "pulsar",
      "seed": "prod-seed-v1",
      "max_input_work_seconds": 2.0,
      "require_cache_affinity_key": true,
      "require_input_tokens": true,
      "require_kv_metrics": true,
      "request_algorithms": {
        "round-robin": "round-robin"
      }
    }
  }
}
```

Canonical config keys are `groq-multiregion`, `power-of-two`, `pulsar`, `random`, and `round-robin`. The request header accepts those values and underscore aliases derived by replacing hyphens with underscores, such as `groq_multiregion`, `power_of_two`, and `round_robin`; aliases are normalized internally. If the header is absent, Stargate uses the configured or default algorithm. If the header is blank, unknown, or names an algorithm that is not configured for the target model, Stargate rejects the request with `400 BAD_REQUEST` and logs the model, routing key, requested algorithm, and rejection reason.

Accepted overrides reuse the configured or default balancer when the requested algorithm already matches the effective static algorithm. Overrides that switch algorithms use stable per-routing-target/per-algorithm balancer state. The `x-routing-method` header is consumed by Stargate and is not forwarded to the backend.

## Observability

### Prometheus metrics

Stargate serves Prometheus metrics on a separate HTTP port (default `9090`):

```bash
curl http://localhost:9090/metrics
```

Available metrics:
- `stargate_requests_total{routing_key, model, inference_server_id, status}` -- total proxied requests
- `stargate_proxy_attempts_total{routing_key, model, inference_server_id, result}` -- upstream proxy attempts by selected backend and attempt result
- `stargate_proxy_retries_total{routing_key, model, reason}` -- proxy retries by retry reason
- `stargate_proxy_retry_exhausted_total{routing_key, model, reason}` -- requests that exhausted retry options
- `stargate_admission_rejections_total{routing_key, model, reason}` -- requests rejected before ranking by admission control
- `stargate_quic_connection_evictions_total{inference_server_id, reason}` -- QUIC pool evictions
- `stargate_quic_hot_path_reconnect_total{inference_server_id, result}` -- direct QUIC reconnect attempts from the proxy hot path
- `stargate_proxy_replay_buffer_bytes{model}` -- replay buffer size for proxied request bodies
- `stargate_proxy_duration_seconds{routing_key, model, inference_server_id}` -- time-to-first-byte histogram
- `stargate_routing_duration_seconds{routing_key, model}` -- load balancer decision time histogram
- `stargate_active_inference_servers{routing_key, model}` -- currently routable inference servers for a routing target

Change the metrics port with `--metrics-port`. Metric names use the `stargate_`
prefix by default; change it with `--metrics-prefix` for deployments that need
service-scoped names.

`pylon` also serves Prometheus metrics on a separate HTTP port (default `9089`):

```bash
curl http://localhost:9089/metrics
```

Available pylon metrics:
- `target_info{service_version, service_name, commit}` -- process build metadata
- `pylon_requests_inflight{model}` -- current proxied requests in flight
- `pylon_requests_state{model, state}` -- current proxied requests by pylon-side lifecycle state
- `pylon_requests_state_input_tokens{model, state}` -- current input tokens by lifecycle state
- `pylon_requests_total{model, routing_key, status}` -- terminal proxied request counter
- `pylon_request_time_to_response_headers_seconds{model, routing_key}` -- time to upstream response headers histogram
- `pylon_request_time_to_first_output_seconds{model, routing_key}` -- time to first streamed output event histogram
- `pylon_request_time_to_first_token_seconds{model, routing_key}` -- time to first observed output token histogram
- `pylon_request_duration_seconds{model, routing_key, status}` -- total terminal request duration histogram
- `pylon_request_input_tokens_total{model, routing_key, status}` and `pylon_request_output_tokens_total{model, routing_key, status}` -- terminal token counters
- `pylon_request_input_tokens{model, routing_key, status}` and `pylon_request_output_tokens{model, routing_key, status}` -- terminal per-request token histograms
- `pylon_registration_stream_connected{router}` and `pylon_reverse_tunnel_connected{router}` -- per-router connectivity gauges
- `pylon_model_last_mean_input_tps{model}` and `pylon_model_output_tps{model}` -- sticky completed-request mean input throughput and observed output throughput by model. Input throughput is a stable cumulative estimate within the pylon process, not an instantaneous rate; embeddings contribute input samples from `x-input-tokens`. Output TPS remains streaming generation output tokens/sec and does not fold in embeddings item counts.
- `pylon_model_embedding_item_tps{model}` -- observed embeddings item throughput by model, computed from request `input` cardinality and response-body relay duration clamped to the stats duration floor.
- `pylon_model_max_output_tps{model}` and `pylon_model_max_embedding_item_tps{model}` -- observed max output throughput by model with separate token/sec and embedding item/sec surfaces.
- `pylon_model_queue_size{model}` and `pylon_model_queued_input_tokens{model}` -- current queue pressure by model
- `pylon_model_kv_cache_capacity_tokens{model}`, `pylon_model_kv_cache_used_tokens{model}`, and `pylon_model_kv_cache_free_tokens{model}` -- current KV-cache token gauges by model
- `pylon_model_stats_capability{model, capability}` and `pylon_model_stats_source{model, source}` -- observed stats contract labels advertised by model
- `pylon_model_advertised_status{router, model, status}` -- model status advertised on each stargate registration stream
- `pylon_retryable_responses_total{inference_server_id, reason, status}` -- retryable responses emitted or relayed by pylon, including local `queue_estimate_mismatch` admission responses
- `pylon_nonretryable_failures_total{inference_server_id, reason}` -- upstream failures not marked retryable by pylon
- `pylon_queue_admission_decisions_total{inference_server_id, model_id, result}` plus `pylon_queue_admission_expected_ms{inference_server_id, model_id}` and `pylon_queue_admission_actual_ms{inference_server_id, model_id}` -- local queue-estimate admission decisions and estimate histograms
- `pylon_quality_checks_total{model, result}` -- output quality checks by result (`clean`, `matched`, or `skipped`)
- `pylon_quality_threshold_matches_total{model, reason}` -- output quality threshold matches by trigger reason

Change the pylon metrics address with `--metrics-host` and `--metrics-port`.

### OpenTelemetry tracing

Per-request distributed traces can be exported to an OTel collector via OTLP/gRPC:

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery \
  --otel-endpoint http://localhost:4317
```

Each proxied OpenAI-compatible request produces a `proxy_openai_request` span with attributes for request path, request headers, selected instance stats, routing algorithm, and proxy timing. If `--otel-endpoint` is omitted, OTel tracing is disabled and only structured logs are emitted. The OTel `service.name` resource and tracer name default to `stargate`; change them with
`--otel-service-name`.

Stargate uses a custom trace-context-only OTLP/gRPC pipeline and exports only `proxy_openai_request` descendants so process-lifetime loops stay out of trace export.

Pylon can export `pylon_upstream_http_request` spans for local upstream inference requests when started with `--otel-endpoint` or `OTEL_EXPORTER_OTLP_ENDPOINT`. Its OTel `service.name` resource defaults to `pylon`; change it with `--otel-service-name` or `OTEL_SERVICE_NAME`.

## Reverse tunnel mode

In some deployments the inference server sits behind a NAT or firewall and cannot accept inbound connections. Reverse tunnel mode lets the inference server initiate the QUIC connection **to** stargate instead of the other way around.

### 1) Start Stargate with a reverse tunnel listener

Add `--reverse-tunnel-listen-addr` to expose a QUIC endpoint that inference servers can connect to:

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery \
  --reverse-tunnel-listen-addr 0.0.0.0:50072
```

### 2) Start mock inference HTTP and the Pylon CLI in reverse tunnel mode

Run `mock-dynamo` (upstream HTTP only) and `pylon` (tunnel + registration). Point the CLI at the mock HTTP base URL with `--upstream-http-base-url`.
By default, `mock-dynamo` serves streaming `/v1/chat/completions`, streaming `/v1/responses`, non-streaming `/v1/embeddings`, and the pylon test stats stream at `/pylon/v1/stats/stream`. The stats stream emits NDJSON `stats` and `ping` events. Pylon consumes that side channel, keeps private stats out of OpenAI-compatible response bodies, and falls back to chunk JSON token parsing only when the stream is disabled or `auto` mode sees an unsupported stream endpoint before any valid event. Use `--engine-stats-contract off` on `mock-dynamo` and `--engine-stats-stream=off` on `pylon` to exercise fallback/no-stream behavior.

Terminal A (mock inference on port 8090):

```bash
cargo run -p mock-dynamo -- \
  --http-listen-addr 127.0.0.1:8090 \
  --kv-cache-capacity-tokens 150000
```

Terminal B (reverse tunnel client):

```bash
cargo run -p pylon -- \
  --upstream-http-base-url http://127.0.0.1:8090 \
  --stargate-address 127.0.0.1:50071 \
  --reverse-tunnel
```

When `--reverse-tunnel` is set on `pylon`, the pylon:
- registers with HTTP `inference_server_url` (the upstream base URL) and `reverse_tunnel=true`,
- learns stargate reverse listener targets from `InferenceServerAck`,
- opens a QUIC connection to each discovered stargate reverse listener and performs a handshake,
- registers with one Stargate while waiting for cluster calibration assignment or completion,
- runs bringup calibration against the upstream HTTP server only when assigned,
- if it is not the calibration owner, advertises locally `Active` after Stargate returns `Waiting` while Stargate suppresses routing until cluster calibration completes; after completion it publishes the shared seed and registers with the remaining discovered Stargates,
- starts periodic canaries while it is active.

If the reverse tunnel connection drops, the pylon automatically goes `Inactive` and reconnects.

When pylons reach Stargate through separate backend-facing load balancers, configure Stargate with
`--reverse-tunnel-listen-addr` for the local UDP bind socket and
`--reverse-tunnel-pylon-dial-addr` for the QUIC load-balancer address pylons should dial. Stargate
still sends the per-pod reverse tunnel target for QUIC SNI so the backend-facing router can select
the intended Stargate pod.

Tunnel transport selection:

- Use `--tunnel-protocol=custom|http3|webtransport` on both `stargate` and `pylon`.
- `custom` is the default and is the best fit for trusted Stargate-to-pylon paths behind L4 UDP load balancing.
- `http3` uses HTTP/3 request streams and is most appropriate for direct/client-initiated H3 experiments or reverse paths that still remain L4.
- `webtransport` uses an HTTP/3 extended CONNECT session and is the right reverse-tunnel choice when an H3/WebTransport-aware L7 proxy or load balancer sits between Stargate and pylon.
- For Kubernetes reverse tunnels, use `custom` with `stargate-k8s-router`, or use `webtransport` with a bring-your-own H3/WebTransport-aware load balancer. `stargate-k8s-router` is not a WebTransport or plain HTTP/3 L7 proxy.

See [Tunnel Transport Selection](docs/tunnel-transports.md) for requirements, use cases, load-balancer behavior, and benchmark guidance for each protocol.

Useful `pylon` flags:

- `--model-name` (repeatable; register multiple models in one process, e.g. `--model-name a --model-name b`)
- `--cluster-id` (optional logical hardware cluster id; defaults to `--inference-server-id`)
- `--disable-bringup`
- `--disable-coordinated-calibration`
- `--active-canary-interval-ms`
- `--canary-max-generation-threshold`
- `--calibration-requests`
- `--calibration-prompt-units`
- `--calibration-max-concurrency`
- `--bringup-canary-timeout-ms`
- `--bringup-calibration-timeout-ms`
- `--engine-stats-stream` (default `auto`; `auto`, `required`, or `off` for the `/pylon/v1/stats/stream` side-channel listener)
- `--engine-stats-stream-path` (default `/pylon/v1/stats/stream`; upstream HTTP path for the NDJSON stats stream)
- `--engine-stats-contract` (default `true`; enable the mock/test engine stats defaults, including `/kv-cache/stats` polling)
- `--kv-cache-stats-path` (override upstream HTTP path for KV-cache metrics)
- `--min-update-interval-ms` (minimum interval between registration/stat updates to stargate; default `1000`)
- `--tunnel-protocol` (default `custom`; backend tunnel wire format, `custom`, `http3`, or `webtransport`, must match stargate)
- `--metrics-host` (default `0.0.0.0`, Prometheus metrics HTTP host)
- `--metrics-port` (default `9089`, Prometheus metrics HTTP port)
- `--otel-endpoint` (optional, env `OTEL_EXPORTER_OTLP_ENDPOINT`, OTLP/gRPC endpoint for trace export)
- `--otel-service-name` (default `pylon`, env `OTEL_SERVICE_NAME`, OpenTelemetry `service.name` resource)
- `--pylon-retryable-upstream-status-codes` (default `429,503`, env `PYLON_RETRYABLE_UPSTREAM_STATUS_CODES`; comma-separated upstream HTTP statuses that can be marked retryable)
- `--pylon-require-upstream-retry-header` (default `true`, env `PYLON_REQUIRE_UPSTREAM_RETRY_HEADER`; require the upstream retry header before marking retryable statuses retryable)
- `--pylon-upstream-retry-header` (default `x-stargate-upstream-retryable`, env `PYLON_UPSTREAM_RETRY_HEADER`; upstream response header that authorizes retrying retryable status codes)
- `--pylon-propagate-retry-after` (default `true`, env `PYLON_PROPAGATE_RETRY_AFTER`; convert upstream `Retry-After` responses into `x-stargate-retry-after-ms`)
- `--pylon-local-connect-failures-retryable` (default `false`, env `PYLON_LOCAL_CONNECT_FAILURES_RETRYABLE`; mark local upstream connection failures as retryable)
- `--pylon-queue-mismatch-retry-enabled` (default `true`, env `PYLON_QUEUE_MISMATCH_RETRY_ENABLED`; enable local retryable rejection when pylon's queue estimate exceeds Stargate's estimate)
- `--pylon-queue-mismatch-min-delta-ms` (default `25`, env `PYLON_QUEUE_MISMATCH_MIN_DELTA_MS`; additive queue-estimate tolerance)
- `--pylon-queue-mismatch-tolerance-factor` (default `1.25`, env `PYLON_QUEUE_MISMATCH_TOLERANCE_FACTOR`; multiplicative queue-estimate tolerance)
- `--pylon-queue-mismatch-retry-after-ms` (optional, env `PYLON_QUEUE_MISMATCH_RETRY_AFTER_MS`; retry-after hint for local queue-mismatch rejections)
- `--collect-quality-metrics` (default `false`; enable post-stream output quality checks on proxied chat responses)
- `--collect-quality-metrics-min-tokens` (default `20`; minimum observed output tokens before text quality metrics are computed)
- `--quality-output-tokens-threshold-min` (optional; match when observed output tokens exceed threshold)
- `--quality-output-compression-threshold-max` (optional; match when compression ratio is below threshold)
- `--quality-output-degeneracy-threshold-min` (optional; match when degeneracy score exceeds threshold)
- `--quality-output-repetition-1gram-threshold-min` / `--quality-output-repetition-2gram-threshold-min` / `--quality-output-repetition-3gram-threshold-min` (optional; match when repetition score exceeds threshold)
- `--quality-median-logprob-threshold-max` (optional; match when observed median logprob is below threshold when available from upstream metadata)

### 3) Send a request

```bash
curl -X POST http://127.0.0.1:8000/v1/chat/completions \
  -H "x-request-id: req-1" \
  -H "x-model: dummy-model" \
  -H "x-input-tokens: 1" \
  -H "content-type: application/json" \
  -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Responses API requests must set `"stream": true` and use the same routing headers:

```bash
curl -X POST http://127.0.0.1:8000/v1/responses \
  -H "x-request-id: req-2" \
  -H "x-model: dummy-model" \
  -H "x-input-tokens: 1" \
  -H "content-type: application/json" \
  -d '{"input":"hi","max_output_tokens":4,"stream":true}'
```

Embeddings use the same routing headers and do not require a `stream` field:

```bash
curl -X POST http://127.0.0.1:8000/v1/embeddings \
  -H "x-request-id: req-2" \
  -H "x-model: dummy-model" \
  -H "x-input-tokens: 2" \
  -H "content-type: application/json" \
  -d '{"model":"dummy-model","input":["alpha","beta"],"encoding_format":"float"}'
```

The `x-routing-key` header is omitted here because local dev uses `OpenAuthenticator` (no external auth service). When a `GrpcWorkerAuthenticator` is configured, include `x-routing-key` with the value returned by the authentication service.

When routing to a model configured with `pulsar`, also include `x-cache-affinity-key`:

```bash
-H "x-cache-affinity-key: prefix-hash-123"
```

Regular (`quic://`) and reverse-tunnel instances can coexist on the same stargate -- the mode is per-instance.

## Runtime flags

- `--stargate-id` (required)
- `--listen-addr` (default `0.0.0.0:50071`)
- `--model-discovery-listen-addr` (default `0.0.0.0:50073`)
- `--http-listen-addr` (default `0.0.0.0:8000`)
- `--advertise-addr` (required)
- `--stargate-discovery-dns-name` (required)
- `--remote-stargate-url` (repeatable or comma-separated via `STARGATE_REMOTE_WATCH_URLS`; adds remote-region `WatchStargates` endpoints to `watch_stargate_urls`)
- `--advertised-hostname-template` (optional, supports `{pod_name}` and `{namespace}` for client-facing advertised addresses)
- `--pod-name` (optional unless `--advertised-hostname-template` is set)
- `--pod-namespace` (optional unless `--advertised-hostname-template` is set)
- `--disable-dns-discovery` (default `false`; publishes only this stargate in `WatchStargates`)
- `--dns-poll-ms` (default `1000`)
- `--dns-resolver-ttl-ms` (default `1000`)
- `--watch-heartbeat-ms` (default `5000`)
- `--registration-update-idle-timeout-ms` (default `60000`, env `STARGATE_REGISTRATION_UPDATE_IDLE_TIMEOUT_MS`; minimum idle timeout for heartbeat-aware registration streams before the max cap is applied; `0` disables all registration idle enforcement)
- `--registration-update-max-idle-timeout-ms` (default `300000`, env `STARGATE_REGISTRATION_UPDATE_MAX_IDLE_TIMEOUT_MS`; hard maximum for heartbeat-aware registration streams and fallback timeout for legacy/no-header streams; heartbeat-aware clients negotiate `max(min, 3 * heartbeat)` capped by this value; `0` disables all registration idle enforcement)
- `--shutdown-drain-timeout-ms` (default `30000`)
- `--quic-connect-timeout-ms` (default `2000`)
- `--quic-request-timeout-ms` (default `30000`)
- `--direct-quic-connections` (default `1`, env `STARGATE_DIRECT_QUIC_CONNECTIONS`; number of outbound QUIC connections opened per direct `quic://` backend)
- `--tunnel-protocol` (default `custom`; backend tunnel wire format, `custom`, `http3`, or `webtransport`, must match pylon)
- `--proxy-max-connect-retries` (default `2`, env `STARGATE_PROXY_MAX_CONNECT_RETRIES`; maximum direct QUIC reconnect attempts on the proxy hot path)
- `--proxy-max-request-retries` (default `2`, env `STARGATE_PROXY_MAX_REQUEST_RETRIES`; maximum retries for explicit retryable upstream responses)
- `--proxy-max-replay-body-bytes` (default `67108864`, env `STARGATE_PROXY_MAX_REPLAY_BODY_BYTES`; maximum request body bytes buffered for proxy retry replay)
- `--proxy-require-pylon-retry-signal` (default `true`, env `STARGATE_PROXY_REQUIRE_PYLON_RETRY_SIGNAL`; require pylon's explicit retry signal before retrying upstream status responses)
- `--proxy-retry-budget-header` (default `x-stargate-max-wait-ms`, env `STARGATE_PROXY_RETRY_BUDGET_HEADER`; request header carrying the retry budget in milliseconds; empty disables budget headers)
- `--tls-cert-path` (optional, env `STARGATE_TLS_CERT_PATH`)
- `--lb-config-path` (optional, path to load balancer JSON config)
- `--otel-endpoint` (optional, OTLP/gRPC endpoint for trace export)
- `--otel-service-name` (default `stargate`, OpenTelemetry `service.name` resource and tracer name)
- `--metrics-port` (default `9090`, Prometheus metrics HTTP port)
- `--metrics-prefix` (default `stargate_`, prefix prepended to Stargate Prometheus metric names)
- `--reverse-tunnel-listen-addr` (optional, QUIC listen address for client-initiated reverse tunnels)
- `--reverse-tunnel-pylon-dial-addr` (optional, pylon dial address for reverse QUIC tunnels when it differs from the per-pod routing/SNI target, such as a separate UDP load balancer)
- `--reverse-tunnel-connect-timeout-ms` (default `10000`, timeout waiting for a reverse tunnel connection after registration)
- `--worker-auth-endpoint` (optional, gRPC endpoint for worker authentication)
- `--secrets-path` (optional, env `SECRETS_PATH`, path to a JSON secrets file written by vault-agent)
- `--secrets-json-path` (optional, env `SECRETS_JSON_PATH`, dot-separated JSON path to the auth token value inside the secrets file; default `authToken`)

## Authentication

When `--worker-auth-endpoint` is set, stargate authenticates workers via gRPC during registration. To attach a Bearer token to outgoing gRPC calls, configure `--secrets-path` to point at a JSON file containing the token (typically written by a vault-agent sidecar). Use `--secrets-json-path` to specify the dot-separated JSON path to the token value.

Example:

```bash
cargo run -p stargate -- \
  --stargate-id local-1 \
  --listen-addr 127.0.0.1:50071 \
  --http-listen-addr 127.0.0.1:8000 \
  --advertise-addr 127.0.0.1:50071 \
  --stargate-discovery-dns-name localhost \
  --disable-dns-discovery \
  --worker-auth-endpoint http://localhost:50051 \
  --secrets-path /vault/secrets/secrets.json \
  --secrets-json-path nvcfApiToken
```

## Dev checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features --no-deps -- -D warnings
cargo test --workspace
```

Focused behavior checks:

```bash
cargo test -p stargate --test stargate_integration proxy_contract -- --nocapture
cargo test -p stargate --test stargate_integration reverse_tunnel -- --nocapture
cargo test -p stargate --test stargate_integration routing_key -- --nocapture
cargo test -p stargate --test stargate_integration load_balancing -- --nocapture
cargo test -p stargate --test stargate_integration stats_discovery -- --nocapture
cargo test -p pylon-lib responses -- --nocapture
cargo test -p mock-dynamo responses_endpoint_streams_response_events_with_mock_headers -- --nocapture
```

Endpoint contract coverage includes
`crates/stargate/tests/suite/proxy_contract.rs::chat_completions_route_proxies_path_query_and_body_through_quic_tunnel`,
`crates/stargate/tests/suite/proxy_contract.rs::chat_completions_route_forwards_upstream_error_through_quic_tunnel`,
`crates/stargate/tests/suite/proxy_contract.rs::supported_endpoint_required_proxy_headers_are_enforced`,
`crates/stargate/tests/suite/reverse_tunnel.rs::reverse_tunnel_proxies_chat_endpoint_contract`,
`crates/stargate/tests/suite/reverse_tunnel.rs::reverse_tunnel_proxies_responses_response`,
and
`crates/stargate/tests/suite/reverse_tunnel.rs::reverse_tunnel_forwards_endpoint_upstream_errors`.

## PlantUML diagrams

Sources live under `docs/diagrams`. To render them (SVG by default) and re-render on save:

```bash
./scripts/watch_puml.sh
```

From another directory, pass the folder to watch:

```bash
./scripts/watch_puml.sh /path/to/diagrams
```

One-shot render (no file watcher):

```bash
./scripts/watch_puml.sh --once
```

Options:

- `--format png` or `--format pdf` instead of SVG
- `--out <name>`: output subdirectory under the watched folder (default: `out`)
- `--docker`: run PlantUML via Docker (`plantuml/plantuml`); the watch directory must be inside this git repository

Requirements:

- **Renderer**: `plantuml` on your `PATH`, or `java` plus `PLANTUML_JAR` pointing at `plantuml.jar`, or `--docker`
- **Watch mode** (default): Linux `inotifywait` from the `inotify-tools` package

Full usage: `./scripts/watch_puml.sh -h`
