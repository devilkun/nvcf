# Coordinated Calibration State Machine

This document defines the state machines involved in Stargate-coordinated
cluster calibration. The important separation is:

- Stargate owns cluster calibration state.
- The client owns local backend bringup and health state.
- Registration fanout is a client-side transport latch that protects the
  Stargate-owned calibration state from being initialized independently by
  multiple routers.

## Stargate Cluster Calibration State

Scope: `(routing_key, cluster_id, model_id)`.

```text
Missing
  -> Assigned(owner_backend)
       first coordinated backend registers this model without a valid
       completed calibration state

  -> Complete(last_mean_input_tps)
       backend registers this model Active with calibration_state=Complete
       and valid last_mean_input_tps

Assigned(owner_backend)
  -> Assigned(owner_backend)
       owner is still running calibration or still reporting inactive

  -> Complete(last_mean_input_tps)
       owner reports Active with calibration_state=Complete and valid
       last_mean_input_tps

  -> Missing
       owner disconnects or removes the model before completion

  -> Waiting directive
       non-owner backend registers while owner is assigned; this is a
       cluster-calibration ownership directive, not a backend lifecycle state

Complete(last_mean_input_tps)
  -> Complete(last_mean_input_tps)
       at least one coordinated backend in the cluster still serves this model

  -> Missing
       no coordinated backend in the cluster still serves this model
```

Stargate may receive `Active` from non-owner backends while calibration is
pending. That is expected: those backends are not doing calibration and do not
need to block their local lifecycle on another backend's calibration work.
While the cluster calibration state is `Assigned`, Stargate treats the model as
calibration-pending for routing and does not add it as an active routing target.

The `Complete(last_mean_input_tps)` value is cluster/model calibration state.
It is not a backend health signal. The pylon publishes the calibration seed
through the same sticky `last_mean_input_tps` field later used by runtime mean
observations, while `calibration_state=Complete` remains the separate proof that
coordinated calibration finished.

Stargate only accepts a backend registration as proof of cluster calibration
completion when the model is `Active`, has a valid `last_mean_input_tps`, and
reports `calibration_state=Complete`. `Active` plus a positive runtime
`last_mean_input_tps` is not enough, because non-owner backends can advertise
local activity while calibration is pending.

## Client Backend Bringup State

Scope: one client process, one model.

```text
ConnectingUnavailable
  -> AwaitingClusterCalibration
       upstream health is OK and coordinated calibration is enabled for initial
       bringup

  -> Calibrating
       upstream health is OK and local calibration is required

  -> AdvertisingActive
       upstream health is OK and no calibration is required

AwaitingClusterCalibration
  -> Calibrating
       Stargate returns Run; this backend is the calibration owner

  -> AdvertisingActive and keep observing directives
       Stargate returns Waiting; another backend owns calibration, so this
       backend does not run local calibration now

  -> AdvertisingActive
       Stargate returns Complete; cluster calibration already exists

Calibrating
  -> AdvertisingActive
       local calibration succeeds

  -> Calibrating
       calibration fails while upstream health remains OK

  -> ConnectingUnavailable
       upstream health fails

AdvertisingActive
  -> AdvertisingActive
       active canary succeeds

  -> Recovering
       active canary fails while upstream health is OK

  -> ConnectingUnavailable
       active canary fails and upstream health is down

Recovering
  -> AdvertisingActive
       recovery canary succeeds

  -> Recovering
       recovery canary fails while upstream health remains OK

  -> ConnectingUnavailable
       upstream health fails
```

`AwaitingClusterCalibration` means the client is waiting for its assignment from
Stargate. A `Waiting` directive means "not the calibration owner right now." It
does not make initial bringup terminal: the client advertises the backend as
locally active, clears any stale local `Complete` state, and keeps observing
directives until Stargate returns either `Run` for reassignment or `Complete`
for the cluster result. Routing suppression while calibration is pending remains
Stargate's responsibility.

## Registration Fanout Latch

Scope: one client registration process.

```text
coordinated_calibration=false
  FullFanout

coordinated_calibration=true
  SingleRouterUntilCalibrated
    -> FullFanout
         every local model is AdvertisingActive and the single calibration
         router has reported Complete for every local model

FullFanout
  -> FullFanout
       terminal latch state; later backend health changes do not collapse
       registration back to one router
```

This latch prevents multiple Stargates from independently assigning different
calibration owners during initial bringup. Non-owner backends may become locally
active before cluster calibration is complete, but the client still stays
connected only to the selected calibration router until that router reports
completion for all local models.

For global discovery, `WatchStargates.watch_stargate_urls` are recursive watch
seeds only. They are not registration targets. The client waits for every
currently discovered watch endpoint to produce a snapshot, then sorts concrete
registration targets returned from all watched `stargates` snapshots before
selecting the single calibration router. This preserves the same one-router
latch until cluster calibration completes.

## Current Recovery Boundary

The current implementation treats recovery as backend-local health recovery:
after initial bringup, a canary failure moves only that backend/model to
`Recovering`, runs a local recovery canary, and returns to `AdvertisingActive`
when it succeeds.

Cluster calibration is not re-entered for backend-local recovery. If recovery
needs to become Stargate-coordinated, that should be a separate Stargate-owned
cluster recovery state machine rather than overloading
`Complete(last_mean_input_tps)`.
