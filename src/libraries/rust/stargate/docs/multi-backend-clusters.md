# Multi-Backend Cluster Routing Design

## Status

Implemented design record. The cluster-keyed routing model, `cluster_id`,
backend selection, and retry distinctions described here are in the codebase.
For concise current operational guidance, use `docs/agent-architecture.md`;
this document retains the design rationale and implementation shape.

## Context

Before this work, Stargate treated one registered backend client as one inference cluster. The same
identifier, `InferenceServerRegistration.inference_server_id`, is used as:

- the registration uniqueness key
- the QUIC connection pool key
- the load-balancer candidate id
- the retry exclusion id
- the metrics label and `x-inference-server-id` response header

That is correct when each backend client owns independent hardware. It is wrong when
two or more backend clients are replicas in front of the same hardware cluster. In
that case Stargate can over-count hardware capacity, over-spread cache-affine traffic,
and fail over between replicas as if they were independent capacity domains.

## Goals

- Route at the hardware cluster level, not the backend-client level.
- Keep QUIC transport, request execution, retry accounting, and response headers tied
  to the concrete backend client that served the request.
- Preserve the current single-backend-per-cluster behavior without requiring config
  changes.
- Make stats aggregation explicit and testable per metric.
- Support multiple active backend clients for the same `(routing_key, model_id,
  cluster_id)`, selecting one backend by round robin after the load balancer chooses
  the cluster.

## Non-Goals

- Cross-Stargate replication of backend state. This design keeps the current local
  registration model.
- Renaming all existing public `inference_server_id` metrics and headers in one step.
  Those remain backend-level compatibility surfaces.
- Solving weighted backend selection inside a cluster. Backend selection inside one
  cluster starts as simple round robin across currently active backends.

## Terminology

- `backend_id`: unique identity for one Pylon registration stream and QUIC
  connection. This is the existing `inference_server_id` identity in the current
  protocol.
- `cluster_id`: logical hardware/capacity domain. Multiple backend clients can share
  one `cluster_id`.
- `single-backend cluster`: compatibility case where `cluster_id == backend_id`.
- `routing target`: existing `(routing_key, model_id)` key.

## Wire Compatibility

The implementation added `cluster_id` to `InferenceServerRegistration`:

```proto
message InferenceServerRegistration {
  string inference_server_id = 1; // backend_id compatibility field
  string inference_server_url = 2;
  map<string, InferenceServerModelRegistration> models = 3;
  bool reverse_tunnel = 4;
  string cluster_id = 5;
}
```

Semantics:

- `inference_server_id` remains required and globally unique per active backend
  registration.
- Stargate normalizes an empty `cluster_id` to `inference_server_id`.
- The `pylon` CLI supports `--cluster-id`, defaulting to
  `--inference-server-id`.
- A future cleanup can introduce a `backend_id` alias in the pylon API, but the first
  implementation should not rename the protobuf field or Prometheus labels.

## State Model

The pre-change state was effectively:

```text
routing target -> inference_server_id -> RoutedInferenceServerSnapshot
registered_inference_servers -> inference_server_id -> RegisteredInferenceServerState
```

The implementation replaces only the routable candidate layer with clusters:

```text
routing target -> cluster_id -> RoutedClusterState
registered_backends -> backend_id -> RegisteredBackendState
```

`RegisteredBackendState`:

- `backend_id`
- `cluster_id`
- `inference_server_url`
- `routing_key`
- `reverse_tunnel`
- per-model latest backend snapshots
- latest RTT for the backend connection

`RoutedClusterState`:

- `cluster_id`
- active backend map: `backend_id -> RoutedBackendSnapshot`
- round-robin counter for backend selection
- stored cluster snapshot
- backend-scoped load aggregated across active backends
- cluster-scoped stats owned at the cluster level and updated from the latest
  registration update
- cluster snapshot update timestamp sourced from the latest cluster-scoped
  registration update

`RoutedBackendSnapshot`:

- `backend_id`
- `inference_server_url`
- backend-scoped model stats
- RTT
- status
- reverse tunnel flag
- delivery target
- snapshot update timestamp

`RoutedClusterSnapshot`, returned to the load balancer:

- `cluster_id`
- aggregated cluster stats
- representative RTT, preferably the minimum active backend RTT
- snapshot update timestamp
- active backend count

The round-robin backend counter stays in `RoutedClusterState`, not in
`RoutedClusterSnapshot`. The load balancer only sees immutable cluster snapshots. After
the load balancer returns a `cluster_id`, the proxy asks state for the next active
backend in that cluster, passing any `failed_backend_ids` for the current request.

The load-balancer trait receives `&[RoutedClusterSnapshot]` instead of
`&[RoutedInferenceServerSnapshot]`. The proxy flow becomes:

```text
request
  -> get active clusters for (routing_key, model_id)
  -> load balancer chooses cluster
  -> state round robins one active backend in that cluster
  -> proxy request over QUIC using backend_id
```

## Stats Aggregation

Each `ModelStats` field needs a scope and merge rule. The table below is the initial
implementation policy, not a permanent API contract. These scopes should stay isolated
behind aggregation helpers because future backend/runtime changes may move individual
metrics between backend and cluster scope.

| Field | Scope | Cluster merge rule |
| --- | --- | --- |
| `output_tps` | Backend | Sum active backends |
| `last_mean_input_tps` | Backend | Sum positive/finite active backends |
| `queue_size` | Backend | Sum active backends |
| `queued_input_size` | Backend | Sum active backends |
| `input_processing_queries` | Backend | Sum active backends |
| `output_generation_queries` | Backend | Sum active backends |
| `stats_observed_at_unix_ms` | Backend | Maximum active backend value |
| `stats_capabilities` | Backend | De-duplicated union across active backends |
| `stats_sources` | Backend | De-duplicated union across active backends |
| `max_output_tps` | Cluster | Latest active backend update wins |
| `kv_cache_capacity_tokens` | Cluster | Latest active backend update wins |
| `kv_cache_used_tokens` | Cluster | Latest active backend update wins |
| `kv_cache_free_tokens` | Cluster | Latest active backend update wins |
| `num_running_queries` | Cluster | Latest active backend update wins |
| `max_engine_concurrency` | Cluster | Latest active backend update wins |
| `total_query_input_size` | Cluster | Latest active backend update wins |
| `queue_time_estimate_ms_by_priority` | Cluster | Latest active backend update wins |

Rationale:

- Live request load and per-client queues are backend-local and must be combined to
  estimate current pressure on the shared cluster.
- `last_mean_input_tps` is backend-local capacity evidence. Runtime observations
  and calibration seeds publish through the same sticky field, and Stargate sums
  positive/finite active backend reports to expose the full cluster input rate.
  Accepted limitation: Stargate does not know whether a backend's sticky value is
  still calibration-seeded or runtime-observed. A shared cluster can therefore
  temporarily overstate capacity when the calibrated backend's seed is summed
  with sibling runtime observations before all pylons have received enough
  representative traffic to refresh their means.
- Hardware capacity and KV/DKVC occupancy describe the shared hardware state. Summing
  them across replicas would overstate cache availability.
- `num_running_queries`, `max_engine_concurrency`, `total_query_input_size`, and
  `queue_time_estimate_ms_by_priority` are also treated as cluster-scoped in the
  first implementation. The intended meaning is shared scheduler state for the
  underlying cluster, not per-client queue fragments.
- `max_output_tps` still needs more design thought. It is treated as cluster latest-wins
  because there is no coordinated output-TPS calibration field today.
- Latest-wins should use Stargate receive time, not client clocks. The chosen source
  backend id should be retained for diagnostics.
- Latest-wins state should only consider active backend snapshots. If the backend that
  supplied the current cluster-level value unregisters or becomes inactive, recompute
  the latest value from the remaining active backend snapshots for that cluster.

Implementation detail: define an internal aggregation policy rather than open-coding
field assignments. For example:

```rust
enum MetricScope {
    BackendSum,
    ClusterLatest,
}
```

The exact enum does not need to be public, but tests should verify the table above.

## Load Balancer Impact

All load balancers evaluate clusters as candidates.

- `power-of-two`: compare cluster load scores using aggregated stats.
- `groq-multiregion`: use aggregated stats and representative cluster RTT.
- `round-robin`: round robin across clusters, not backends.
- `random`: random cluster, not backend.
- `pulsar`: hash key material must use `cluster_id`, not `backend_id`, otherwise
  replicas for the same hardware cluster appear as independent cache destinations.

After a cluster is selected, backend selection inside that cluster is independent of
the load-balancer algorithm and uses per-cluster round robin across active backend
clients.

## Retry Semantics

Retries need two exclusion sets:

- `failed_backend_ids`: concrete backend attempts that should not be tried again for
  this request.
- `failed_cluster_ids`: clusters that should no longer be selected for this request
  after a retryable upstream response or after all of its active backends fail.

Recommended behavior:

1. If backend connection or proxy attempt fails, mark that `backend_id` failed.
2. Re-select a backend from the same chosen cluster if another active backend exists
   and the failure was transport-local.
3. Mark the `cluster_id` failed when:
   - the upstream response is a valid retry signal from the inference service, because
     the shared hardware cluster rejected or could not serve the request, or
   - every active backend in that cluster has failed locally for this request.
4. If pylon returns a retryable local `queue_estimate_mismatch` before upstream
   forwarding, mark only that backend failed first because the comparison reflects
   that pylon's local arrival-time queue; try another active backend in the selected
   cluster before failing over across clusters.
5. Re-run the load balancer with `failed_cluster_ids` excluded after cluster-level
   failure.

This avoids treating one replica's transport or local queue observation as a cluster
failure, while still avoiding repeated attempts against a cluster after a shared
upstream failure.

## Observability

Keep existing backend-level surfaces for compatibility:

- `x-inference-server-id`
- `x-inference-server-url`
- metrics labelled `inference_server_id`

Implemented cluster-level surfaces:

- response header `x-stargate-cluster-id`
- span field `selected_cluster.id`
- the existing concrete-backend span field `selected_inst.id`

The current metrics surface retains
`stargate_active_inference_servers{routing_key, model}` for compatibility.
Separate `stargate_active_clusters` or per-cluster backend gauges remain
possible follow-up observability work; they are not part of the implemented
contract in this document.

## Validation Rules

- Empty `inference_server_id` remains invalid.
- Empty `cluster_id` normalizes to `inference_server_id`.
- Registration uniqueness remains by `backend_id`/`inference_server_id`.
- Multiple active registrations may share `cluster_id`.
- A single registration stream may not change `backend_id`, `cluster_id`, URL, or
  reverse tunnel mode after the first message.
- A backend can be active for a model only if its own model registration is active and
  the backend connection has RTT.
- A cluster is routable for a model only if it has at least one active backend for that
  model.

## Implemented Work Plan

1. Add `cluster_id` to the protobuf and pylon CLI/config, defaulting to
   `inference_server_id`.
2. Extend `RegistrationIdentity` with `cluster_id`; normalize it in
   `start_registration_stream`; reject changes in `validate_running_update`.
3. Split state structs in `load_balancer_state.rs`:
   - keep registration keyed by backend id
   - key routing candidates by cluster id
   - store active backend snapshots under each cluster
4. Add stats aggregation helpers with unit tests for backend-summed and latest-wins
   fields.
5. Change load-balancer candidate type from backend snapshot to cluster snapshot.
   Update PULSAR hash material from backend id to cluster id.
6. Update HTTP proxy:
   - track failed backend ids and failed cluster ids separately
   - choose backend after choosing cluster
   - proxy via backend id
   - emit cluster id in headers and spans
7. Update integration tests:
   - two backend ids sharing one cluster id count as one cluster candidate
   - PULSAR stable hashing maps by cluster id
   - cluster-level stats latest-wins and backend-level stats sum
   - cluster round robin alternates backend ids after a cluster is selected
   - backend-local failure can try another backend in the same cluster
8. Update benchmark/Kubernetes helpers to optionally set `cluster_id`. Existing manifests
   should keep one cluster per backend by default.

## Main Risk

The highest-risk part is retry semantics. A backend-local transport failure and a
cluster-level rejection need different exclusions. If the implementation keeps a single
failed id set, it will either skip healthy replicas too aggressively or retry the same
overloaded hardware cluster too much.

The second risk is observability compatibility. Existing dashboards and benchmark code
consume `x-inference-server-id` and metrics labelled by inference server id. The first
implementation should add cluster observability without changing those existing
meanings.
