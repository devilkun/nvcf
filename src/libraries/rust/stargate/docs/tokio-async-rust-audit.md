# Tokio Async Rust Audit

Date: 2026-05-19

This report reviews Stargate's Tokio/async Rust usage across the Rust workspace, using the `tokio-async-rust` review checklist: task ownership, shutdown, cancellation, blocking work, channels/backpressure, lock lifetimes, and tests.

## Executive Summary

No clear async-lock deadlock or unbounded load-bearing queue stood out in the production server paths. The codebase already uses several good patterns: `TaskTracker` for the core Stargate runtime and pylon tunnel handles, bounded `flume`/`mpsc` channels for registration and observation paths, `CancellationToken` for top-level shutdown, short `parking_lot`/`std::sync::Mutex` critical sections, and `scc` async APIs for routing maps.

The main risk is task ownership. A few important background tasks are spawned with raw `tokio::spawn` outside the owning runtime tracker, or are stopped by `abort()` without an awaitable cooperative shutdown. Those choices make it harder to prove graceful shutdown, observe panics, and prevent short-lived task leaks during router churn or reverse-tunnel reconnects.

## Remediation Status

This report captures the pre-remediation audit baseline. The accompanying implementation branch addresses the high and medium findings by tracking reverse listener tasks under Stargate runtime shutdown, adding cooperative pylon registration shutdown, retaining and finalizing request-body sender tasks, adding cancellable health-check shutdown, and replacing propagation sleeps in Stargate integration tests with observable polling helpers.

## Findings

### 1. Reverse tunnel listener tasks are outside `StargateRuntime` shutdown tracking

Severity: High

`StargateRuntime::start` creates a `TaskTracker` and waits on it in `StargateHandle::wait_for_shutdown`, but `QuicHttpProxy::start_reverse_listener` starts its accept loop with raw `tokio::spawn`, and each accepted reverse connection is also dispatched with raw `tokio::spawn`.

Relevant code:

- `crates/stargate/src/runtime.rs:176` creates the runtime `TaskTracker`.
- `crates/stargate/src/runtime.rs:210` starts the reverse listener.
- `crates/stargate/src/runtime.rs:510` waits only for `task_tracker.wait()`.
- `crates/stargate/src/quic_tunnel.rs:1188` spawns the reverse listener loop.
- `crates/stargate/src/quic_tunnel.rs:1200` spawns per-connection dispatch tasks.
- `crates/stargate/src/quic_tunnel.rs:1376` and `crates/stargate/src/quic_tunnel.rs:1552` spawn pool-cleanup tasks that wait for reverse connections to close.

Impact:

`begin_shutdown()` cancels the listener token, so the accept loop is likely to exit, but `wait_for_shutdown()` can report completion without having joined the reverse listener, in-flight reverse handshakes, peer relays, or pool-cleanup tasks. Panics in those detached tasks are also unobserved. This is most visible in Kubernetes/reverse-tunnel deployments where the QUIC listener is part of the serving contract.

Recommendation:

Make the reverse listener return a handle or accept a `TaskTracker` from `StargateRuntime`. Track the listener, per-connection dispatch, and connection cleanup tasks under the runtime shutdown tree. Add a focused test that starts a runtime with `reverse_tunnel_listen_addr`, calls `begin_shutdown()`, waits, and asserts the listener and dispatch tasks have terminated or been cancelled.

### 2. Pylon registration shutdown is mostly abort-driven and can detach child reverse-tunnel work

Severity: Medium

`InferenceServerRegistrationClient::stop` sends cancellation signals, then immediately aborts the watch, bringup, and registration supervisor tasks. The registration supervisor also aborts per-router tasks when routers are removed. Those per-router tasks own reverse-tunnel child `JoinHandle`s locally; aborting the parent drops those handles rather than joining them. The reverse tunnel loop does observe shared stop signals, but several retry/backoff sleeps are not cancellation-aware.

Relevant code:

- `crates/pylon-lib/src/lib.rs:335` cancels and aborts registration client tasks.
- `crates/pylon-lib/src/lib.rs:454` and `crates/pylon-lib/src/lib.rs:479` abort per-router registration tasks.
- `crates/pylon-lib/src/lib.rs:1201` spawns each router's reverse tunnel loop.
- `crates/pylon-lib/src/lib.rs:1337` aborts the reverse task only on normal loop exit.
- `crates/pylon-lib/src/lib.rs:1707` and `crates/pylon-lib/src/lib.rs:1723` sleep during reverse-tunnel backoff without selecting on stop/cancel.
- `crates/pylon-lib/src/quic_http_tunnel.rs:2412` cancels reverse tunnel handles on drop, which mitigates leaked QUIC sessions.

Impact:

The handle-drop cancellation added in `quic_http_tunnel.rs` prevents the worst duplicate-connection leak, and there is regression coverage for that in `crates/stargate/tests/suite/reverse_tunnel.rs:1111`. Still, shutdown is not fully joinable: `stop()` returns before child tasks have actually finished, and a detached reverse-tunnel loop can continue until its current connect timeout or backoff sleep completes.

Recommendation:

Replace the registration client's raw handle set with a `TaskTracker` plus `CancellationToken`, or add an async `shutdown()` that cancels, closes the tracker, and awaits task completion. Change backoff sleeps to a helper that selects on parent stop, local stop, and cancellation. Keep `stop()` as a best-effort sync shim if the public API needs it.

### 3. Per-request tunnel body-sender tasks detach after successful response headers

Severity: Medium

For custom QUIC, HTTP/3, and WebTransport proxy requests, Stargate spawns a task to send the request body while the main future waits for response headers. If response headers arrive first and the status is successful, the function returns a `StreamingResponse` without retaining or awaiting the body sender's `JoinHandle`.

Relevant code:

- `crates/stargate/src/quic_tunnel.rs:219` spawns the custom request-body sender.
- `crates/stargate/src/quic_tunnel.rs:295` spawns the WebTransport request-body sender.
- `crates/stargate/src/quic_tunnel.rs:343` spawns the HTTP/3 request-body sender.
- `crates/stargate/src/quic_tunnel.rs:227`, `crates/stargate/src/quic_tunnel.rs:303`, and `crates/stargate/src/quic_tunnel.rs:351` race body send against response headers.
- `crates/stargate/src/quic_tunnel.rs:270`, `crates/stargate/src/quic_tunnel.rs:325`, and `crates/stargate/src/quic_tunnel.rs:371` abort only on non-success statuses.

Impact:

Dropping a Tokio `JoinHandle` detaches the task. Late body-send errors are logged inside the task, but they are no longer connected to the request outcome, metrics, or shutdown ownership. This may be acceptable for a deliberate duplex-streaming contract, but OpenAI-compatible request bodies are normally finite request payloads, so detached send work is a cancellation and observability blind spot.

Recommendation:

Make ownership explicit. One option is to store the sender handle in `StreamingBody` so it is aborted or awaited when the response body is dropped or fully drained. Another is to avoid spawning for non-duplex transports and finish sending the request body before returning response headers. Add tests for "successful early headers then request-body send fails" and "client drops response while upload is still in progress".

### 4. Some producer loops use fixed sleeps instead of cancellation-aware waits

Severity: Low

Most long-running loops use `select!` with stop/cancel branches, but a few retry paths still use plain sleeps inside task loops.

Relevant code:

- `crates/pylon-lib/src/lib.rs:1194` sleeps for one second after registration stream open failure.
- `crates/pylon-lib/src/lib.rs:1243` sleeps after an initial registration send failure.
- `crates/pylon-lib/src/lib.rs:1707` and `crates/pylon-lib/src/lib.rs:1723` sleep during reverse tunnel reconnect backoff.
- `crates/stargate/src/control_plane.rs:982` sleeps in the registration health-check loop, which is stopped by abort rather than cooperative cancellation.

Impact:

These are bounded delays, so they are not catastrophic, but they make shutdown latency depend on where the task happens to be suspended. They also encourage callers to use `abort()` to get crisp shutdown, which hides panics and cleanup errors.

Recommendation:

Consolidate pylon retry sleeps behind `sleep_or_stop` or a new helper that also accepts a `CancellationToken`. Give the Stargate health-check loop a cancellation token and await it as part of registration cleanup, or document that abort is the intended owner action.

### 5. Test coverage still relies on time sleeps in many async integration tests

Severity: Low

The repo guidance says not to use bare `tokio::time::sleep` in tests to wait for async state propagation, but there are still many occurrences in integration tests.

Examples:

- `crates/stargate/tests/suite/reverse_tunnel.rs:1077` waits three seconds for heartbeat/reconnect behavior.
- `crates/stargate/tests/suite/proxy_contract.rs:1371` and nearby tests sleep before assertions.
- `crates/stargate/tests/suite/load_balancing.rs:439` and nearby tests sleep before checking routing/load-balancing effects.

Impact:

These tests can hide scheduler races and slow down the suite. They also make it harder to add sharper cancellation and shutdown assertions, because a passing sleep-based test does not prove the desired event happened promptly.

Recommendation:

Gradually replace sleeps with existing observable polling helpers, watch receivers, metrics checks, or deterministic seams. The reverse-tunnel and proxy retry areas would benefit most from this because they are exactly where shutdown and reconnection timing matter.

## Positive Notes

- The core Stargate runtime has a clear shutdown model for the gRPC, model-discovery, HTTP, discovery, and active-model snapshot tasks through `TaskTracker` and `CancellationToken` (`crates/stargate/src/runtime.rs:176`, `crates/stargate/src/runtime.rs:322`, `crates/stargate/src/runtime.rs:348`, `crates/stargate/src/runtime.rs:377`, `crates/stargate/src/runtime.rs:449`).
- Routing state uses `scc` async APIs and keeps lock closures short. The `parking_lot` locks in `load_balancer_state.rs` protect plain data and are not held across external `.await`s in the inspected paths.
- Load-bearing channels are generally bounded: registration ACKs, pylon status/stats updates, watch endpoint updates, request observations, and registration update streams all use bounded `flume` or bounded `mpsc`.
- The Kubernetes router has the cleanest service-level task ownership: it tracks watcher, gRPC router, QUIC router, and health server tasks under one `TaskTracker`, reports unexpected critical exits, cancels, closes, and awaits the tracker (`crates/stargate-k8s-router/src/main.rs:117`, `crates/stargate-k8s-router/src/main.rs:204`, `crates/stargate-k8s-router/src/main.rs:214`).
- Pylon tunnel handles use `TaskTracker` and cancellation tokens, and their `Drop` implementations cancel as a defensive fallback (`crates/pylon-lib/src/quic_http_tunnel.rs:129`, `crates/pylon-lib/src/quic_http_tunnel.rs:148`, `crates/pylon-lib/src/quic_http_tunnel.rs:2400`).

## Verification Performed

This was a static audit. I inspected the repo startup instructions, architecture docs, tunnel transport docs, scoped Stargate/Pylon agent guidance, and the main async Rust modules. I did not run the full test suite because the task was to write an analysis report and no behavior was changed.

Recommended focused checks before acting on fixes:

- `cargo test -p stargate reverse_tunnel`
- `cargo test -p stargate quic_forwarding`
- `cargo test -p pylon-lib`
- `cargo test -p stargate-k8s-router`
- For runtime shutdown changes: `cargo test -p stargate runtime::tests::shutdown_cancels_in_flight_discovery_poll`
