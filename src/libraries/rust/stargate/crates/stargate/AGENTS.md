<coding_guidelines>
# crates/stargate

Server-side stargate code owns registration state, load-balancer routing, HTTP proxying, QUIC connection use, peer forwarding, metrics, and tracing.

## Local Invariants

- Keep routing state keyed by `RoutingTargetKey { routing_key, model_id }`.
- `routing_key` comes from `WorkerAuthenticator`; never trust a registration proto field for routing identity.
- Do not parse HTTP proxy request bodies. Treat bodies as opaque bytes and buffer only for replay.
- Direct backends use a QUIC connection set keyed by `inference_server_id`; `--direct-quic-connections` controls the set size and defaults to `1`.
- Only route to backends with an open QUIC path and successful forwarded `/health` RTT sample.
- Keep HTTP proxy requests local to the serving stargate. Do not forward HTTP proxy requests between peer stargates.
- Preserve retry/failover accounting so final request metrics emit once and attempt metrics emit per upstream attempt.
- Keep tracing fields on `proxy_openai_request` useful for backend selection, retry, replay, upstream status, and TTFT debugging.

## Load Balancing

- New algorithms implement `LoadBalancer` in `src/load_balancer/` and are registered in `create_load_balancer()`.
- Pass request-specific inputs through `LoadBalancerRequest`; do not grow trait methods with positional arguments.
- Candidate lookup is scoped by `RoutingTargetKey`.
- `pulsar` ranking must use effective candidate capacity (`ModelStats.last_mean_input_tps` after cluster aggregation) and keep transient live load in feasibility gates.
- Any algorithm that needs request headers must define missing-header behavior explicitly and return client errors where configured.

## Concurrency

- Use approved `scc` single-shot APIs. Avoid entry-style APIs and `contains_*` followed by dependent mutation.
- In async code, prefer `*_async` operations and never hold bucket-level locks across external `.await`s.
- Keep lock closures short, non-blocking, and side-effect free.
</coding_guidelines>
