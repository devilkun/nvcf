# Historical Inference Progress SSE Comment Contract

This document is retained only as historical context for the removed
`inference-progress.v1` experiment.

The active pylon runtime stats contract is the NDJSON engine stats stream
documented in [runtime-stats-interface.md](runtime-stats-interface.md). New
inference containers should implement:

```text
GET /pylon/v1/stats/stream
Accept: application/x-ndjson
```

Pylon no longer requires, consumes, or strips `inference-progress.v1` SSE
comments. If an upstream response contains those comments, pylon treats them as
ordinary response bytes and relies on the engine stats stream, or the configured
minimal OpenAI fallback mode, for model stats.

The historical contract carried request progress in private SSE comments such
as:

```text
: inference-progress.v1 v=1 req=req-123 seq=1 ph=prefill ip=1024
```

That design was replaced because pylon stats should not be coupled to
customer-visible OpenAI response streams, and engines should publish
high-frequency cumulative request counters and ping events on a separate
operational side channel.
