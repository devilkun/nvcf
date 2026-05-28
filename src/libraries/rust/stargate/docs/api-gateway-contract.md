# API Gateway Integration Contract

This document describes the contract an API gateway or LLM frontend must follow
when using Stargate for model discovery and OpenAI-compatible request routing.

The gateway owns end-user authentication, authorization, model aliasing, quota,
and public API shape. Stargate owns backend registration state, local routing,
load balancing, and the selected backend tunnel transport over QUIC.

## Responsibilities

The API gateway is responsible for:

- authenticating the external caller and enforcing product-level authorization;
- mapping the caller, function, tenant, or deployment to a Stargate
  `routing_key`;
- resolving the public model name to the exact backend model id sent in
  `x-model`;
- enforcing any caller allowed-model list before forwarding to Stargate;
- generating request metadata such as `x-request-id`, `x-input-tokens`, and
  optional affinity or SLO headers;
- calling `ListModels` for discovery hints when needed;
- forwarding only trusted, gateway-owned routing headers to Stargate.

Stargate is responsible for:

- serving `ListModels` from the selected Stargate pod's local active-model
  snapshot;
- routing OpenAI-compatible inference POSTs (`/v1/chat/completions`,
  `/v1/responses`, and `/v1/embeddings`) by
  `RoutingTargetKey { routing_key, model_id }`;
- selecting an active backend with an open tunnel connection and successful
  forwarded `/health` RTT sample;
- proxying the request body as opaque bytes on the Stargate HTTP proxy path;
- retrying or failing over only within Stargate's configured proxy retry
  policy.

Stargate does not authenticate public HTTP callers, enforce per-caller allowed
models, parse HTTP proxy request bodies, or replicate routing state between
Stargate pods.

## Kubernetes Endpoints

In Kubernetes, the gateway should use only the load-balanced frontend services:

| Purpose | Service | Protocol | Default port |
| --- | --- | --- | --- |
| Model discovery | `stargate-model-discovery` | gRPC `StargateModelDiscovery/ListModels` | `50073` |
| Inference proxy | `stargate-proxy` | HTTP `POST /v1/chat/completions`, `POST /v1/responses`, `POST /v1/embeddings` | `8000` |

The gateway must not target raw pod IPs or per-pod Stargate addresses for
frontend traffic. `ListModels` and HTTP proxy requests are both local to the
Stargate pod selected by Kubernetes service load balancing and are never
forwarded to another Stargate pod.

Pylons use `WatchStargates`, backend-facing gRPC, and QUIC tunnel services.
Those are not gateway-facing discovery or proxy APIs.

## Backend Tunnel Protocol

The gateway-facing contract is unchanged by Stargate's backend tunnel protocol.
The gateway always calls the HTTP proxy service for inference traffic.

Operators may configure Stargate and pylon with
`--tunnel-protocol=custom`, `--tunnel-protocol=http3`, or
`--tunnel-protocol=webtransport`. Both sides of a backend tunnel must use the
same value:

- `custom`: Stargate's Cap'n Proto tunnel framing on a raw bidirectional QUIC
  stream.
- `http3`: HTTP/3 request and response streams over the same Quinn-based QUIC
  connection.
- `webtransport`: one HTTP/3 extended CONNECT session, then WebTransport
  bidirectional streams carrying HTTP proxy heads and raw body bytes.

Do not add a gateway request header or public API option to select this per
request. It is a deployment-level backend transport setting.

## Routing Identity

Stargate routing state is keyed by:

```text
RoutingTargetKey {
  routing_key: Option<String>,
  model_id: String,
}
```

The gateway should derive `routing_key` from its own authentication or control
plane lookup. It must not trust a public client's `x-routing-key` header. When a
request is intentionally unscoped, omit `x-routing-key`; blank values are
treated as omitted.

The gateway should derive `model_id` from its own model resolution logic and
send it in `x-model`. Stargate routes only from `x-model`; it does not inspect
or validate a `model` field inside the JSON body. If the public API body also
contains a model field, the gateway should keep it aligned with `x-model` or
rewrite it before forwarding.

The current Stargate wire contract does not enforce an authenticated allowed
model set. The gateway must do that check before forwarding.

## Model Discovery

Use `StargateModelDiscovery/ListModels` through the
`stargate-model-discovery` service.

Request fields:

- `routing_key`: optional. Omitted or blank means the unscoped `None` routing
  key.
- `model_ids`: optional filters. Each value is trimmed like the `x-model` proxy
  header. Blank filter entries are invalid.

Response fields:

- `model_ids`: active model ids visible in the selected Stargate pod's local
  snapshot for the requested routing key and optional filters.

`ListModels` is an eventually consistent discovery hint, not a routing
reservation. A model returned by `ListModels` can disappear before a later proxy
request, and a separate proxy request may land on a different Stargate pod with
a slightly different local snapshot.

Recommended gateway behavior:

- Cache results only briefly, scoped by `routing_key` and model filter.
- Treat a positive result as "worth attempting", not as guaranteed capacity.
- Treat a negative result as unavailable for the selected local snapshot; avoid
  presenting it as global proof if backend registrations may still be
  converging.
- If a recent positive `ListModels` result is followed by a proxy
  `404 NOT_FOUND` with `x-stargate-error-code: no_eligible_candidates`, retry
  after the gateway's configured discovery-convergence delay before reporting
  the model unavailable.
- Treat `503 SERVICE_UNAVAILABLE` for a registered model with no eligible
  candidates as transient serving unavailability, not as a discovery miss.

## Proxy Request Contract

The currently supported gateway-facing inference endpoints are:

```text
POST /v1/chat/completions
POST /v1/responses
POST /v1/embeddings
```

Minimum headers for successful end-to-end inference routing:

| Header | Required | Owner | Semantics |
| --- | --- | --- | --- |
| `x-request-id` | Yes | Gateway | Globally unique request id and pylon-side observation key. |
| `x-model` | Yes | Gateway | Exact Stargate model id used for routing. |
| `x-input-tokens` | Yes | Gateway | Unsigned integer input-token estimate. Required by the pylon tunnel and by some load balancers. |
| `x-routing-key` | Tenant-scoped requests only | Gateway | Authenticated routing scope. Omit for unscoped routing. |
| `content-type` | Yes for JSON bodies | Gateway | Usually `application/json`. |

Optional routing headers:

| Header | Semantics |
| --- | --- |
| `x-cache-affinity-key` | Opaque stable prompt-prefix or KV-cache identity. Required for models whose load-balancer config sets `require_cache_affinity_key=true`. |
| `x-priority` | Unsigned integer priority. Defaults to `0`; used by priority-aware queue-time routing when backends report priority estimates. |
| `x-request-slo-ms` | Per-request SLO hint for load balancers that use latency objectives. |
| `x-max-wait-ms` | Maximum time Stargate should spend waiting for a feasible routing candidate when candidates exist but are temporarily ineligible. Stargate caps this internally. |
| `x-stargate-max-wait-ms` | Proxy retry budget in milliseconds for Stargate's internal retries. Invalid values return `400`. Set this from the gateway's remaining request budget. |

Internal proxy headers:

| Header | Owner | Semantics |
| --- | --- | --- |
| `x-stargate-expected-queue-ms` | Stargate | Routing-time queue estimate sent only from Stargate to pylon. Caller-supplied values are stripped by Stargate before backend forwarding, and pylon strips the header before forwarding to the local upstream. |

For chat completions and Responses API requests routed through the pylon
tunnel, the request body must be valid JSON and must set `"stream": true`.
The HTTP proxy in Stargate does not parse the body, but the backend-side
pylon validates the tunneled request and rejects non-streaming
`/v1/chat/completions` or `/v1/responses` requests with `400`. Streaming
Responses API responses use semantic SSE events such as `response.created`,
`response.output_text.delta`, and `response.completed`.

For embeddings routed through the pylon tunnel, the request body must be valid
JSON and does not need a `stream` field. The HTTP proxy in Stargate does not
parse these bodies, but the backend-side pylon checks JSON validity before
forwarding upstream; embeddings payload semantics remain the upstream's
responsibility.

The gateway should synthesize or overwrite all Stargate routing headers. Do not
forward public caller-supplied values for `x-routing-key`, `x-model`,
`x-input-tokens`, `x-cache-affinity-key`, `x-priority`, `x-request-slo-ms`,
`x-max-wait-ms`, or `x-stargate-max-wait-ms` unless the public API explicitly
defines those fields and validates them first.

Example:

```bash
curl -X POST http://stargate-proxy:8000/v1/chat/completions \
  -H "content-type: application/json" \
  -H "x-request-id: req-01HY..." \
  -H "x-routing-key: tenant-or-function-routing-key" \
  -H "x-model: llama-3.1-8b" \
  -H "x-input-tokens: 128" \
  -H "x-cache-affinity-key: prompt-prefix-hash" \
  -H "x-stargate-max-wait-ms: 1500" \
  -d '{"model":"llama-3.1-8b","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Responses API example:

```bash
curl -X POST http://stargate-proxy:8000/v1/responses \
  -H "content-type: application/json" \
  -H "x-request-id: req-01HY-resp" \
  -H "x-routing-key: tenant-or-function-routing-key" \
  -H "x-model: llama-3.1-8b" \
  -H "x-input-tokens: 128" \
  -H "x-cache-affinity-key: prompt-prefix-hash" \
  -H "x-stargate-max-wait-ms: 1500" \
  -d '{"model":"llama-3.1-8b","input":"hi","max_output_tokens":16,"stream":true}'
```

Embeddings example:

```bash
curl -X POST http://stargate-proxy:8000/v1/embeddings \
  -H "content-type: application/json" \
  -H "x-request-id: req-01HZ..." \
  -H "x-routing-key: tenant-or-function-routing-key" \
  -H "x-model: text-embedding-3-small" \
  -H "x-input-tokens: 8" \
  -d '{"model":"text-embedding-3-small","input":["alpha","beta"],"encoding_format":"float"}'
```

## Response Contract

On a proxied upstream response, Stargate returns the upstream status, body, and
forwardable upstream headers, then adds:

| Header | Semantics |
| --- | --- |
| `x-inference-server-id` | Concrete backend client that served the final response. |
| `x-inference-server-url` | Registered backend URL for that backend. |
| `x-stargate-cluster-id` | Logical capacity cluster selected by Stargate. |

These headers are useful for internal debugging and metrics correlation. A
public API gateway should decide explicitly whether to expose or strip them from
the public response.

Important error cases:

| Status | Signal | Meaning | Gateway action |
| --- | --- | --- | --- |
| `400` | Missing or invalid required header/body | Gateway contract violation or invalid caller input after gateway validation. | Do not retry unchanged. Fix or reject at the gateway. |
| `404` | `x-stargate-error-code: no_eligible_candidates` and JSON body code `no_eligible_candidates` | The requested `(routing_key, model_id)` is unknown or no longer registered on the selected Stargate pod. | If a recent positive discovery result exists, retry after convergence delay; otherwise report unavailable. |
| `413` | Payload too large | Request body exceeds Stargate's configured replay buffer limit. | Do not retry unchanged. |
| `502`, `503`, `504` | No `no_eligible_candidates` error code | Proxy transport failure, retry exhaustion, or upstream/service failure. | Treat as a serving failure. External retry policy must account for duplicate generation risk. |

Stargate uses internal retry metadata such as `x-stargate-retryable`,
`x-stargate-retry-reason`, and `x-stargate-retry-after-ms` between pylon and
the Stargate proxy. Those are not caller-supplied request headers and are
stripped from downstream responses.

## Retry And Timeout Guidance

Stargate may internally retry direct QUIC setup races, retry explicit retryable
upstream responses, and fail over to another eligible cluster or backend when
the request body is replayable within the configured replay limit. Those retry
rules apply regardless of whether the backend tunnel is using `custom` framing
or HTTP/3.

Pylon can also return retryable `429` responses with
`x-stargate-retry-reason: queue_estimate_mismatch` when its local queue estimate
has grown beyond the queue estimate Stargate used to choose that backend. This
signal is internal: the first upstream was not called, and Stargate can replay
the opaque request body to another eligible backend if retry budget remains.

The gateway should:

- set `x-stargate-max-wait-ms` from the remaining end-to-end request budget if
  the public API has a deadline;
- use `x-max-wait-ms` only when it wants Stargate to wait briefly for currently
  infeasible candidates instead of returning immediately;
- avoid blind external retries after a streaming inference request may have
  reached an upstream backend;
- keep the same `x-request-id` when retrying a no-candidate convergence race
  that did not reach an upstream backend;
- use normal client and server deadlines in addition to Stargate's request
  headers.

## Security Requirements

The gateway should be the only public entrypoint for Stargate proxy traffic.
NetworkPolicy, service exposure, or ingress configuration should prevent
untrusted callers from reaching `stargate-proxy` or
`stargate-model-discovery` directly.

Before forwarding, the gateway should:

- authenticate the public caller;
- resolve and authorize the target function, tenant, deployment, and model;
- derive the trusted `routing_key`;
- strip or overwrite untrusted Stargate headers;
- enforce public rate limits and quotas.

A wrong or unauthorized routing key usually appears to Stargate as an unknown
local target. That is not an authorization decision; it is a routing miss. The
gateway must produce the public authorization behavior.

## Observability

Use `x-request-id` as the common request correlation key across gateway logs,
Stargate proxy logs, pylon observations, and upstream inference logs.

The gateway may also propagate standard trace context headers. Stargate records
proxy spans with routing inputs, chosen backend metadata, retry information,
upstream status, and timing when OpenTelemetry export is enabled.

For internal debugging, capture the added response headers
`x-inference-server-id`, `x-inference-server-url`, and
`x-stargate-cluster-id` before stripping them from any public response.

## Minimal Gateway Checklist

1. Route only through `stargate-model-discovery` and `stargate-proxy`.
2. Authenticate the public caller and derive a trusted `routing_key`.
3. Resolve and authorize the model, then set `x-model` from that result.
4. Generate a globally unique `x-request-id`.
5. Estimate input tokens and set `x-input-tokens`.
6. Set `"stream": true` for both streaming endpoints, `/v1/chat/completions`
   and `/v1/responses`; keep `/v1/embeddings` exempt from any `stream`
   requirement.
7. Optionally call `ListModels` as a short-lived availability hint.
8. Handle `404` plus `x-stargate-error-code: no_eligible_candidates` as the
   Stargate unknown-or-unregistered local target signal.
9. Treat Stargate response backend headers as internal unless the public API
   intentionally exposes them.
