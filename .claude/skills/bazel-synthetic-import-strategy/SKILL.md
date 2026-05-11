---
name: bazel-synthetic-import-strategy
description: >-
  NVCF-specific guidance for adding Bazel to umbrella-monorepo subtrees
  whose source of truth lives in separate upstream GitLab repos
  (synthetic imports, marked `authoritative_source: upstream` in
  `imports.yaml`). Covers the constraint that `BUILD.bazel` files
  committed only to the umbrella get overwritten on the next sync, three
  rollout strategies (native-only Phase 1, scaffold-and-accept-loss,
  scaffold-with-companion-upstream-MRs), the companion-MR pattern (land
  BUILD files upstream first, then bump the commit pin in
  `imports.yaml`), a coverage-tracker template, per-service ordering
  recommendations, and the `gazelle:prefix` pitfall when umbrella and
  upstream paths differ. Triggers on synthetic import Bazel,
  imports.yaml authoritative_source upstream, BUILD files overwritten by
  sync, NVCF subtree Bazel, or adding Bazel to a service whose code
  lives in another repo.
version: "1.0.0"
author: NVCF Platform Team
tags:
  - bazel
  - nvcf
  - synthetic-imports
  - monorepo
tools:
  - Read
  - Shell
  - Write
---

# Bazel Synthetic Import Strategy (NVCF-specific)

The NVCF umbrella monorepo (`nvcf/nvcf`) presents most services as
ordinary directories, but those directories are synthetic imports: their
content is mirrored from separate upstream GitLab repos and overwritten
on the next sync. This skill explains how to add Bazel to those services
without losing the work on the next sync cycle.

## The Constraint

`imports.yaml` at the umbrella root declares each subtree's provenance:

```yaml
imports:
  - path: src/libraries/go/lib
    repo: https://github.com/NVIDIA/egx/intelligent-infra/nvcf-go.git
    commit: eb6ccfe97a47ac0049c113054f266cceff668aa8
    authoritative_source: native
  - path: src/invocation-plane-services/http-invocation
    repo: https://github.com/NVIDIA/nvcf/nvcf-invocation-service.git
    commit: 716fac3630c2eac1b10dc81a13adab881d479819
    authoritative_source: upstream
  - path: src/clis/nvcf-cli
    repo: https://github.com/NVIDIA/ncp/nvcf/cli.git
    commit: f9da2689d75dc399685539701cca6991cb083dae
    authoritative_source: native
```

Two values for `authoritative_source`:

- `native`: edits in the umbrella win. Sync brings history forward but
  does not overwrite local files.
- `upstream`: the upstream repo is the source of truth. The next sync
  replays the upstream tree under the umbrella path, blowing away any
  files that exist only in the umbrella.

A `BUILD.bazel` added to a synthetic-import subtree, in the umbrella
only, has a useful lifetime measured in days. Any plan to "just add
BUILD files to all services" without touching upstream will silently
revert on the next sync.

## Workflow

For any synthetic-import subtree the agent (or a human) should touch
with Bazel:

1. Look up the subtree in the umbrella's `imports.yaml` and read its
   `authoritative_source` value.
2. If `native`, treat it like a normal Bazel subtree: add BUILD files,
   run Gazelle, commit. The synthetic-import constraint does not apply.
3. If `upstream`, pick a strategy from the next section based on how
   long the work needs to survive:
   - Strategy A (recommended) when the goal is incremental Phase 1
     coverage limited to native subtrees.
   - Strategy B for short-lived experiments (results will vanish on
     the next sync).
   - Strategy C when the BUILD files must persist across syncs.
4. For Strategy C, follow the Companion-MR Pattern below: land BUILD
   files in the upstream repo first, get them merged, bump the commit
   pin in `imports.yaml`, run the sync tool, and validate
   `bazel build` from the umbrella.
5. Track the subtree's status in the coverage tracker (see template
   below) so other agents and humans can see what is in flight, what
   landed upstream, and what is bazel-buildable in the umbrella.

## Three Strategies

### Strategy A: Native-only Phase 1 (recommended start)

Add Bazel files only to subtrees with `authoritative_source: native`,
plus the umbrella root. This gives an honest, incremental rollout that
cannot be silently reverted.

In the current NVCF umbrella, that scope is:

- Root infra: `MODULE.bazel`, `.bazelrc`, `BUILD.bazel`, `rules/`,
  `platforms/`, `tools/`, `ci/`.
- `src/clis/nvcf-cli` (Go CLI, native).
- `src/libraries/go/lib` (Go library, native).
- `ai-tooling` (native, but currently no buildable code; skip Bazel
  targets, just stay aware of it).

Out of scope for Phase 1 (every other src/ subtree is `upstream`).

This is the strategy laid out in the `bazel-monorepo-bootstrap` and
`bazel-go-gazelle` skills.

### Strategy B: Scaffold and Accept Loss

Add BUILD files everywhere, accept that they will be lost on the next
sync of each upstream repo, and re-add them as needed. Useful for a
short-lived experiment ("can Bazel build all of NVCF end-to-end?") but
not viable as a maintained state.

Document explicitly: "BUILD files in `src/<svc>/...` for synthetic
imports are research only and will be lost on next sync."

### Strategy C: Scaffold with Companion Upstream MRs

The only way to keep BUILD files in synthetic-import subtrees alive
across syncs. Requires a paired MR in the upstream service repo for
every subtree. The upstream MR lands first; then the umbrella's
`imports.yaml` commit pin is bumped to the new upstream commit, and the
sync brings the BUILD files in.

The flow per service:

1. Open an MR in the upstream repo (e.g.
   `nvcf/nvcf-invocation-service`) that adds the BUILD files.
2. Verify the upstream MR builds and tests cleanly with Bazel under the
   upstream's local layout. The upstream tree's root path is different
   from the umbrella's path; accommodate this with `gazelle:prefix`
   (see "Pitfalls" below).
3. Merge the upstream MR.
4. In the umbrella, bump `commit:` in `imports.yaml` for that service to
   point at the merged upstream SHA.
5. Run the umbrella's import-sync tool (typically a script in
   `tools/scripts/`) to replay upstream into the subtree.
6. Run `bazel build` against the synced subtree from the umbrella to
   confirm path translation worked end-to-end.

This is the only option that produces a maintained state. It is also the
slowest because each upstream repo has its own review cycle, CI, and
maintainers.

## Companion-MR Pattern

Concrete steps for one service, using `nvcf/nvcf-invocation-service` as
the example:

### 1. Clone and branch the upstream

```bash
git clone https://github.com/NVIDIA/nvcf/nvcf-invocation-service.git
cd nvcf-invocation-service
git checkout -b feat/bazel-build
```

### 2. Add the umbrella's Bazel scaffolding to the upstream

The upstream repo will need `BUILD.bazel` files at its own root layout.
For a single-crate Rust service, that is one `BUILD.bazel` plus
`crates/server/BUILD.bazel`. The umbrella's `MODULE.bazel`,
`rules/oci/`, and `tools/workspace_status.sh` do NOT live in the
upstream repo (they belong to the umbrella). The upstream BUILD files
load from `@//rules/oci:defs.bzl`, but those references only resolve
when the subtree is consumed from the umbrella.

Two options:

(a) Make the upstream Bazel-buildable standalone by adding a thin local
   `MODULE.bazel`, `rules/`, `platforms/`, etc. mirroring the umbrella.
   Upstream CI can verify Bazel works without the umbrella. Adds
   maintenance overhead in the upstream repo.

(b) Make the upstream Bazel-buildable only when consumed from the
   umbrella. Keep BUILD files referencing umbrella-only labels
   (`//rules/oci:defs.bzl`). Skip Bazel CI in the upstream; rely on the
   umbrella's CI to validate. Less work upfront, but the upstream MR
   cannot prove the BUILD files work.

Recommended: (a) for high-traffic services (invocation-service,
http-invocation, llm-gateway). (b) for low-traffic services where the
overhead is not worth it.

### 3. Pre-flight upstream BUILD files

If you went with option (a), `bazel build //...` in the upstream root
should pass. If (b), at least confirm the BUILD files parse:

```bash
bazel query //... 2>&1 | head
```

(Errors about missing labels in `@//rules/oci:defs.bzl` are expected for
option (b); errors about Starlark syntax or missing `srcs` are bugs.)

### 4. Open and merge the upstream MR

Standard MR. Reference the umbrella issue/MR in the description so
maintainers understand why these BUILD files appeared. Coordinate with
the upstream maintainer; they may push back on adding Bazel to a repo
that does not currently use it.

### 5. Bump `imports.yaml` in the umbrella

Once the upstream MR merges, in the umbrella:

```yaml
- path: src/invocation-plane-services/http-invocation
  repo: https://github.com/NVIDIA/nvcf/nvcf-invocation-service.git
  commit: <NEW_SHA_FROM_MERGED_UPSTREAM_MR>
  authoritative_source: upstream
```

### 6. Run the import sync and validate

```bash
# Whatever the umbrella uses for sync; commonly:
python3 tools/scripts/sync-imports.py --path src/invocation-plane-services/http-invocation
git add -A
git commit -m "chore(sync): pull http-invocation BUILD files from upstream"
bazel build //src/invocation-plane-services/http-invocation/...
```

If `bazel build` fails, the most common cause is a `gazelle:prefix`
mismatch (see Pitfalls).

## Coverage Tracker Template

A single markdown checklist per language scope helps make progress
visible across the many upstream repos. Recommended location:
`docs/bazel-rollout.md` in the umbrella.

```markdown
# NVCF Bazel Rollout Coverage

Per-service status of the Bazel migration. "Companion MR" is the upstream
MR that adds BUILD files; "imports.yaml pin" is the umbrella commit
that brought them in.

## Native subtrees (Phase 1)

| Subtree | Path | Status | Notes |
|---------|------|--------|-------|
| nvcf-cli | src/clis/nvcf-cli | done | Phase 1 |
| nvcf-go-lib | src/libraries/go/lib | done | Phase 1 |
| ai-tooling | ai-tooling | n/a | no buildable code |

## Synthetic-import subtrees (Phase 2+)

### Go services

| Service | Upstream | Companion MR | imports.yaml pin | Status |
|---------|----------|--------------|------------------|--------|
| nvca | egx/intelligent-infra/nvca | <link> | <sha> | not started |
| ess-agent | nvcf/ess-agent | | | not started |
| image-credential-helper | egx/.../nvcf-image-credential-helper | | | not started |
| dns-cache | nvcf/nvcf-cache/dns-cache | | | not started |
| helm-reval | nvcf/.../nvcf-helm-reval-api | | | not started |
| nats-auth-callout | nvcf/nvcf-nats-auth-callout-service | | | not started |

### Rust services

| Service | Upstream | Companion MR | imports.yaml pin | Status |
|---------|----------|--------------|------------------|--------|
| http-invocation | nvcf/nvcf-invocation-service | | | not started |
| ratelimiter | nvcf/nvcf-ratelimiter | | | not started |
| grpc-proxy | nvcf/nvcf-grpc-proxy | | | not started |
| llm-gateway | nvcf/llm-api-gateway | | | not started |
| function-autoscaler | nvcf/optimization/.../rs-autoscaler | | | not started |

### Other

| Service | Upstream | Companion MR | imports.yaml pin | Status |
|---------|----------|--------------|------------------|--------|
| byoo-otel-collector | nvcf/monitoring/byoo-otel-collector | | | not started (Go, OpenTelemetry collector custom build) |
```

Update on every milestone. The "Status" column values: `not started`,
`upstream MR open`, `upstream MR merged`, `synced to umbrella`,
`bazel build passing`.

## Suggested Service Ordering

Three considerations: complexity (simpler languages first), upstream
maintainer responsiveness, and dependency graph (libraries before
consumers).

1. Go workers and CLIs first. Gazelle handles most of the work; failure
   modes are well understood.
   - `nvca`, `ess-agent`, `image-credential-helper`, `dns-cache`,
     `helm-reval`, `nats-auth-callout`.

2. Rust services next. crate_universe and hermetic_cc_toolchain are
   more involved per service but the pattern from
   `bazel-rust-crate-universe` is known-good.
   - `http-invocation`, `ratelimiter`, `grpc-proxy`, `llm-gateway`,
     `function-autoscaler`.

3. Specials last. byoo-otel-collector uses an OpenTelemetry custom
   collector builder that synthesizes Go source at build time;
   Bazel/Gazelle integration needs additional thought.

## Pitfalls

### `gazelle:prefix` umbrella vs upstream

In the upstream repo's tree, a Go file might live at:

```
nvcf-invocation-service/cmd/server/main.go
```

with `go.mod` `module github.com/NVIDIA/nvcf-invocation-service` at the
upstream root. Gazelle's `prefix` matches the import path:

```python
# gazelle:prefix github.com/NVIDIA/nvcf-invocation-service
```

After sync, the same file lives in the umbrella at:

```
src/invocation-plane-services/http-invocation/cmd/server/main.go
```

The Go import path stays the same (`github.com/NVIDIA/...`); the Bazel
package path changes. Gazelle's prefix directive should still be the Go
import path (not the Bazel package path), so the same directive works in
both places without modification. If you accidentally put the Bazel
package path in the prefix (`src/invocation-plane-services/...`), the
file works in the umbrella and breaks in the upstream.

The fix: always set `gazelle:prefix` to the Go import path, never to a
Bazel label path. Upstream and umbrella agree.

### Cross-subtree deps

In the umbrella, a service might naturally want to depend on
`//src/libraries/go/lib/pkg/foo`. In the upstream, that dep does not
exist as a Bazel label; the upstream's `go.mod` pulls
`github.com/NVIDIA/nvcf/src/libraries/go/lib` as a regular Go module.

Two ways to keep both working:

(a) Always express the dep through Go's import path, let Gazelle resolve
   it through `go_deps` in the umbrella. Adds an indirection but works
   in both repos.

(b) Keep BUILD files identical in upstream and umbrella by always using
   `@io_bazel_rules_go//proto/wkt:go_default_library`-style fully
   qualified labels. Less ergonomic, more robust to refactors.

(a) is the default; switch to (b) only if drift becomes a chronic
problem.

### Sync tool may not preserve `BUILD.bazel`

Some import-sync scripts have a hard-coded include list. Verify that
`BUILD.bazel`, `*.bzl`, and `MODULE.bazel.lock` are in the sync's
allowlist. If not, add them; otherwise the sync drops the BUILD files
even when upstream commits them.

The sync script lives under `tools/` in the umbrella. Look for a list of
file patterns and confirm Bazel files are included.

### Versioning churn between upstream and umbrella

If the upstream service's `MODULE.bazel` (option (a)) drifts from the
umbrella's `MODULE.bazel`, builds work in one place and break in the
other. Two mitigations:

- Pin both to the same `bazel_dep` versions, manually kept in sync.
- Use a shared `MODULE.bazel.shared.bazel` snippet that both repos
  `include` (Bazel does not support module-level includes natively, but
  scripts can copy a versions block on sync).

For Phase 2, accept the manual sync cost; revisit if churn becomes a
problem.

## When to Skip a Service

Some services are not worth converting:

- byoo-otel-collector uses an upstream Go-source-generating builder. The
  Bazel pattern for this exists (`go_proto_library` plus custom genrule)
  but adds complexity disproportionate to the value if no other Bazel
  consumer needs it.
- Services that are about to be deprecated or rewritten. Confirm the
  service is on the medium-term roadmap before opening an upstream MR.
- Services owned by teams that explicitly do not want Bazel. Coordinate
  before forcing it.

## Additional Resources

- Companion skills: `bazel-monorepo-bootstrap`, `bazel-go-gazelle`,
  `bazel-rust-crate-universe`, `bazel-java-maven`, `bazel-oci-images`,
  `bazel-gitlab-child-pipelines`.
- NVCF imports.yaml format and sync flow: see the umbrella's
  `tools/AGENTS.md` and `imports.yaml` itself.
- The nvidia-oss-prep skill for the parallel cleanup work each upstream
  repo benefits from on its way to Bazel readiness.
