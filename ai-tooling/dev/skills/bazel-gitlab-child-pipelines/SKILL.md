---
name: bazel-gitlab-child-pipelines
description: >-
  Sets up additive per-service Bazel CI lanes in GitLab using parent-child
  pipelines. Each service owns its own `service/.gitlab-ci.yml`; the root
  `.gitlab-ci.yml` fans out to them with `trigger.include` and
  `strategy: depend`. Only the service that actually changed gets built,
  via path-based `rules.changes:` filters that also key on shared infra
  (`MODULE.bazel`, `.bazelrc`). Includes a manual `SERVICE=name` dispatch
  variable for retrying a single service from `glab ci run` or the GitLab
  UI, schedule and web triggers for nightly all-services health checks,
  the pre-warmed CI Dockerfile pattern (Ubuntu noble plus Bazelisk plus
  lld plus docker), and explains why `--config=ci` was removed from job
  scripts when the remote cache was a placeholder. Triggers on Bazel
  GitLab CI, parent-child pipeline Bazel, per-service trigger Bazel,
  GitLab change rules Bazel monorepo, or wiring Bazel into GitLab CI
  without breaking existing pipelines.
license: Apache-2.0
compatibility: Requires a local checkout of an NVCF or Bazel monorepo
author: "nvcf-core-eng <nvcf-core-eng@exchange.nvidia.com>"
version: "1.0.0"
tags:
  - nvcf
  - bazel
  - gitlab-ci
  - parent-child-pipelines
  - monorepo
tools:
  - Read
  - Shell
  - Write
metadata:
  internal: false
  author: "nvcf-core-eng <nvcf-core-eng@exchange.nvidia.com>"
  version: "1.0.0"
  tags:
    - nvcf
    - bazel
    - gitlab-ci
    - parent-child-pipelines
    - monorepo
  languages:
    - yaml
    - starlark
  frameworks:
    - bazel
  domain: build-systems
---

# Bazel on GitLab CI: Parent-Child Pipelines

Add Bazel CI for a multi-service monorepo without breaking the existing
pipelines. Each service gets its own pipeline file, the root pipeline
triggers them, and only the service that changed gets built.

## Quick Reference

| File | Owns |
|------|------|
| `.gitlab-ci.yml` (root) | Stages and one `trigger:` job per service |
| `<service>/.gitlab-ci.yml` | The actual `bazel build`/`bazel test`/`bazel run :image_push` for that service |
| `ci/Dockerfile` | The pre-warmed Bazel CI image referenced by every child pipeline |

## Workflow

```
Bazel GitLab CI Setup Progress:
- [ ] Step 1:  Build and push the CI image (ci/Dockerfile)
- [ ] Step 2:  Write a per-service .gitlab-ci.yml for each service
- [ ] Step 3:  Add trigger jobs to root .gitlab-ci.yml (additive, not replacing existing jobs)
- [ ] Step 4:  Set path-based rules.changes for each trigger
- [ ] Step 5:  Add SERVICE manual-dispatch variable filtering
- [ ] Step 6:  Add schedule and web triggers for all-services health checks
- [ ] Step 7:  Validate: open an MR with a single-service change, confirm only that pipeline runs
```

### Step 1: CI image

The pre-warmed Dockerfile lives in `ci/Dockerfile`. Full template in
`bazel-monorepo-bootstrap` reference. The critical bits CI needs that the
default `bazel/bazel` image lacks:

- `lld` (for Rust linking under `rules_rust`)
- `docker.io` and `docker-compose-v2` (for Testcontainers / Spring Boot
  test setups)
- `gcc`, `g++`, `cmake`, `pkg-config` (for any cgo or build-script
  compilation)
- Bazelisk pre-installed and Bazel pre-warmed

Build and push:

```bash
docker build -t myregistry.example.com:5005/myorg/myrepo/ci:1 ci/
docker push myregistry.example.com:5005/myorg/myrepo/ci:1
```

Reference the image in every child pipeline. Bump the tag (`ci:2`,
`ci:3`, ...) on every Dockerfile change so existing pipelines keep using
the version they expect.

### Step 2: Per-service `<svc>/.gitlab-ci.yml`

Three patterns covering most cases. Full versions in
[reference.md](reference.md).

Go service:

```yaml
stages:
  - build

default:
  tags: [os/linux, perflab, type/docker]
  image: myregistry.example.com:5005/myorg/myrepo/ci:1

build-and-test:
  stage: build
  script:
    - bazel build //myservice/...
    - bazel test //myservice/...
    - export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
    - mkdir -p "$DOCKER_CONFIG"
    - |
      printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
        "$CI_REGISTRY" \
        "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
        > "$DOCKER_CONFIG/config.json"
    - bazel run --stamp //myservice:image_push
```

Rust service: add `CARGO_BAZEL_ISOLATED: "false"` under `variables` so
crate_universe can read the private cargo registry credentials baked into
the runner.

Java service with Testcontainers: spin up a `docker:27-dind` service,
pre-start the test dependencies (Cassandra, LocalStack, NATS) before
`bazel test`, and pass the Docker host env vars through `env_inherit` on
the `java_test` rule. The full failure-mode-by-failure-mode rationale
for every knob in the dind setup (`TESTCONTAINERS_HOST_OVERRIDE`,
`TESTCONTAINERS_RYUK_DISABLED`, `CI_PRESTARTED_CONTAINERS`,
`--local_test_jobs=1`, the wait-loop pattern using
`docker compose exec`, etc.) is in `bazel-java-maven` SKILL Step 7a.

### Step 3: Root `.gitlab-ci.yml` triggers

```yaml
default:
  tags: [os/linux, perflab, type/docker]

stages:
  - trigger

myservice:
  stage: trigger
  trigger:
    include: myservice/.gitlab-ci.yml
    strategy: depend
  rules:
    - if: $SERVICE && $SERVICE != "myservice"
      when: never
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes: [myservice/**, MODULE.bazel, .bazelrc]
    - if: $CI_PIPELINE_SOURCE == "schedule"
    - if: $CI_PIPELINE_SOURCE == "web"
```

`strategy: depend` means the parent pipeline status reflects each child
pipeline status. Without it, the parent goes green even when a child
fails.

The `SERVICE` env-var filter is what lets you retry a single service
from the CLI:

```bash
glab ci run -b main --variables "SERVICE:myservice"
```

When `SERVICE` is set, every other trigger evaluates `if: $SERVICE && $SERVICE != "<other>"` first and short-circuits to `never`. When it is
unset, all triggers fall through to the next rule.

### Step 4: Path-based `rules.changes`

Three categories of changes that should trigger a service pipeline:

1. The service's own files: `myservice/**`.
2. Shared infra: `MODULE.bazel`, `.bazelrc`. A bump to `rules_go` or
   `rules_rust` should rebuild every service that uses them.
3. Build rules layer: `rules/**`. If you change `rules/oci/private/go.bzl`
   the OCI image for every Go service can break.

```yaml
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes:
        - myservice/**
        - MODULE.bazel
        - MODULE.bazel.lock
        - .bazelrc
        - rules/**
        - tools/workspace_status.sh
```

`MODULE.bazel.lock` matters if you commit it (which you should). A
lockfile change without an unlocked-graph change still affects build
outputs.

### Step 5: `SERVICE` manual dispatch

The full filter pattern repeats in every trigger:

```yaml
rules:
  - if: $SERVICE && $SERVICE != "<this-service>"
    when: never
  - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
  - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    changes: [<this-service>/**, MODULE.bazel, .bazelrc]
  - if: $CI_PIPELINE_SOURCE == "schedule"
  - if: $CI_PIPELINE_SOURCE == "web"
```

Order matters. The `SERVICE` short-circuit must come first, otherwise the
default-branch or change-rules clauses match and the unrelated services
all run anyway.

### Step 6: Schedule and web triggers

`if: $CI_PIPELINE_SOURCE == "schedule"` plus the matching web rule give
you two extra ways to run all services:

- Set up a nightly schedule in `CI/CD > Schedules`: every service
  pipeline runs unconditionally, catching bit-rot from infra changes
  that did not touch any individual service path.
- Manual run from the GitLab UI (`Run Pipeline`) is `web`. Without
  variables, every service runs. Add `SERVICE=<name>` to scope to one.

### Step 7: Validate

Open an MR that touches only one service. Confirm:

1. Only that service's child pipeline runs.
2. Touching `MODULE.bazel` triggers all service pipelines.
3. Touching `docs/**` triggers no Bazel pipelines (because no rule
   matches).

If a docs-only change runs all pipelines, you have a missing or
overly-broad path filter somewhere.

## Patterns

### Adding a Bazel lane to an existing CI

The skill is "additive". The repo probably already has a working
`.gitlab-ci.yml` with non-Bazel jobs. To add Bazel without breaking
anything:

1. Keep the existing stages and jobs unchanged.
2. Add a new stage for Bazel, e.g. `Bazel`, after `Prerequisites`.
3. Add the new trigger jobs only in the new stage.

```yaml
stages:
  - Prerequisites    # existing
  - Snapshot         # existing
  - Bazel            # new

myservice-bazel:
  stage: Bazel
  trigger:
    include: myservice/.gitlab-ci.yml
    strategy: depend
  rules: ...
```

The Bazel pipeline runs alongside the existing CI; existing developers
keep their current commands and workflows.

### Per-service vs single-job

For repos with one or two services, a single Bazel job in the root
pipeline is fine. The parent-child pattern pays off at three or more
services where:

- Build times become long enough that parallelism matters.
- Service teams want isolated CI ownership.
- You want each service to declare its own Docker-side dependencies
  (Testcontainers, dind, etc.) without polluting unrelated pipelines.

### Why `--config=ci` is omitted from CI scripts

In an early iteration of the reference repo, `.bazelrc` had:

```
build:ci --remote_cache=grpc://CACHE_HOST:9092
build:ci --remote_upload_local_results=true
build:ci --jobs=32
```

with `CACHE_HOST` as a placeholder. Adding `--config=ci` to CI scripts
caused the build to fail trying to reach a non-existent gRPC service. Two
ways to handle:

1. Comment out `build:ci --remote_cache=...` (default in the
   `bazel-monorepo-bootstrap` reference).
2. Drop `--config=ci` from CI scripts entirely.

Both are equivalent. When a real remote cache exists, uncomment the line
and re-add `--config=ci`. Until then, leave it out.

## Common Failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| All services run on every push | Missing `rules.changes` clause | Add `changes: [<svc>/**, MODULE.bazel, .bazelrc]` to each MR rule |
| Unrelated services run when only docs change | A trigger has no `changes` filter on the MR rule | Add the filter |
| `SERVICE=foo` still runs other services | `SERVICE` short-circuit not first in `rules` | Move `if: $SERVICE && $SERVICE != "<svc>": never` to top |
| Parent pipeline stays green when child fails | `strategy: depend` missing | Add it to every `trigger:` job |
| CI fails with `lld: command not found` | CI image missing `lld` | Rebuild `ci/Dockerfile` with `apt install lld` and bump tag |
| `--config=ci` failure: cannot reach remote cache | Placeholder `--remote_cache` URL | Comment out the line in `.bazelrc` until cache is real |
| Image push fails with `denied: requested access to the resource is denied` | Registry auth file (under `$DOCKER_CONFIG`) not written before push | Set `DOCKER_CONFIG=$CI_PROJECT_DIR/.docker` and write `$DOCKER_CONFIG/config.json` before `bazel run :image_push` (see `bazel-oci-images` Step 8) |
| Spring Boot test cannot reach Docker daemon | Missing `services: [docker:27-dind]` plus env vars | Use the Java service template in [reference.md](reference.md) |
| Schedule runs nothing | Missing `if: $CI_PIPELINE_SOURCE == "schedule"` rule | Add it to every trigger |

## Local Reproduction

CI failures should be reproducible locally with the same image:

```bash
docker run --rm -it -v "$PWD:/work" -w /work \
  myregistry.example.com:5005/myorg/myrepo/ci:1 \
  bash -c 'bazel build //myservice/...'
```

If the build passes locally inside the image but fails in CI, the
difference is almost always:

- A different registry auth file (whatever `$DOCKER_CONFIG` points at)
- Different cargo registry config under `$CARGO_HOME` for Rust private
  registries
- Missing env vars (especially `CARGO_BAZEL_ISOLATED=false`)
- Network-side: `urm.nvidia.com` reachable in one place but not the
  other.

## Additional Resources

- Full child-pipeline templates: [reference.md](reference.md)
- GitLab parent-child pipelines:
  https://docs.gitlab.com/ee/ci/pipelines/parent_child_pipelines.html
- Path-based rules: https://docs.gitlab.com/ee/ci/yaml/#ruleschanges
