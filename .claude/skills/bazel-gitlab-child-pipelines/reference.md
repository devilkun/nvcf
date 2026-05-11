# Bazel GitLab Child Pipelines: Templates

## Root `.gitlab-ci.yml` (parent fan-out)

```yaml
# Root pipeline - dispatches to per-service child pipelines.
# Each service owns its own CI definition under <service>/.gitlab-ci.yml.
# Changes to shared infra (MODULE.bazel, .bazelrc) trigger all services.
#
# Manual single-service run:
#   glab ci run -b main --variables "SERVICE:myservice"

default:
  tags: [os/linux, perflab, type/docker]

stages:
  - trigger

# ----- Service pipelines (parent-child) -----
# Default branch: always trigger all services.
# MRs: path-filtered (only build + test what changed).

myservice-go:
  stage: trigger
  trigger:
    include: services/myservice-go/.gitlab-ci.yml
    strategy: depend
  rules:
    - if: $SERVICE && $SERVICE != "myservice-go"
      when: never
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes:
        - services/myservice-go/**
        - MODULE.bazel
        - MODULE.bazel.lock
        - .bazelrc
        - rules/**
    - if: $CI_PIPELINE_SOURCE == "schedule"
    - if: $CI_PIPELINE_SOURCE == "web"

myservice-rust:
  stage: trigger
  trigger:
    include: services/myservice-rust/.gitlab-ci.yml
    strategy: depend
  rules:
    - if: $SERVICE && $SERVICE != "myservice-rust"
      when: never
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes:
        - services/myservice-rust/**
        - MODULE.bazel
        - MODULE.bazel.lock
        - .bazelrc
        - rules/**
    - if: $CI_PIPELINE_SOURCE == "schedule"
    - if: $CI_PIPELINE_SOURCE == "web"

myservice-java:
  stage: trigger
  trigger:
    include: services/myservice-java/.gitlab-ci.yml
    strategy: depend
  rules:
    - if: $SERVICE && $SERVICE != "myservice-java"
      when: never
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes:
        - services/myservice-java/**
        - MODULE.bazel
        - MODULE.bazel.lock
        - .bazelrc
        - rules/**
    - if: $CI_PIPELINE_SOURCE == "schedule"
    - if: $CI_PIPELINE_SOURCE == "web"
```

## Per-service Go pipeline

`services/myservice-go/.gitlab-ci.yml`:

```yaml
stages:
  - build

default:
  tags: [os/linux, perflab, type/docker]
  image: myregistry.example.com:5005/myorg/myrepo/ci:1

build-and-test:
  stage: build
  script:
    - bazel build //services/myservice-go/...
    - bazel test //services/myservice-go/...
    - export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
    - mkdir -p "$DOCKER_CONFIG"
    - |
      printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
        "$CI_REGISTRY" \
        "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
        > "$DOCKER_CONFIG/config.json"
    - bazel run --stamp //services/myservice-go:image_push
```

For a service that produces multiple binaries (init, task, niclls, etc.):

```yaml
build-and-test:
  stage: build
  script:
    - bazel build //services/myservice-go/...
    - bazel test //services/myservice-go/...
    - export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
    - mkdir -p "$DOCKER_CONFIG"
    - |
      printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
        "$CI_REGISTRY" \
        "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
        > "$DOCKER_CONFIG/config.json"
    - bazel run --stamp //services/myservice-go/cmd/init:image_push
    - bazel run --stamp //services/myservice-go/cmd/task:image_push
    - bazel run --stamp //services/myservice-go/cmd/utils:image_push
    - bazel run --stamp //services/myservice-go/cmd/niclls:image_push
```

## Per-service Rust pipeline

`services/myservice-rust/.gitlab-ci.yml`:

```yaml
stages:
  - build

default:
  tags: [os/linux, perflab, type/docker]
  image: myregistry.example.com:5005/myorg/myrepo/ci:1

build-and-test:
  stage: build
  variables:
    CARGO_BAZEL_ISOLATED: "false"
  script:
    - bazel build //services/myservice-rust
    - bazel test //services/myservice-rust/...
    - export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
    - mkdir -p "$DOCKER_CONFIG"
    - |
      printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
        "$CI_REGISTRY" \
        "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
        > "$DOCKER_CONFIG/config.json"
    - bazel run --stamp //services/myservice-rust/crates/server:image_push
```

`CARGO_BAZEL_ISOLATED: "false"` is what lets crate_universe read the
runner's pre-configured cargo credentials for the private registry.

## Per-service Java pipeline (Spring Boot with Testcontainers)

`services/myservice-java/.gitlab-ci.yml`:

```yaml
stages:
  - build

default:
  tags: [os/linux, perflab, type/docker]
  image: myregistry.example.com:5005/myorg/myrepo/ci:1

build-and-test:
  stage: build
  services:
    - name: docker:27-dind
      alias: docker
      command: ["--tls=false"]
  variables:
    DOCKER_HOST: tcp://docker:2375
    DOCKER_DRIVER: overlay2
    DOCKER_TLS_CERTDIR: ""
    DOCKER_TLS_VERIFY: ""
    DOCKER_CERT_PATH: ""
    TESTCONTAINERS_HOST_OVERRIDE: "docker"
    TESTCONTAINERS_RYUK_DISABLED: "true"
  script:
    - docker info
    # Pre-start dependencies so Testcontainers does not have to bring them up
    # under sandbox.
    - |
      cd services/myservice-java/local_env
      docker compose up -d
      echo "Waiting for LocalStack..."
      for i in $(seq 1 60); do
        if docker compose exec -T aws curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; then
          echo "LocalStack ready"; break
        fi
        sleep 2
      done
      echo "Waiting for Cassandra schema init..."
      for i in $(seq 1 90); do
        if docker compose logs cassandra 2>&1 | grep -q "Cassandra init scripts executed"; then
          echo "Cassandra ready"; break
        fi
        sleep 2
      done
      echo "Waiting for NATS..."
      for i in $(seq 1 30); do
        if docker compose exec -T nats nats-server --help >/dev/null 2>&1; then
          echo "NATS ready"; break
        fi
        sleep 1
      done
      docker compose ps -a
      cd "$CI_PROJECT_DIR"
    - export CI_PRESTARTED_CONTAINERS=true
    - bazel build //services/myservice-java
    - bazel test //services/myservice-java/... \
        --test_output=errors \
        --test_summary=detailed \
        --local_test_jobs=1 \
        --test_timeout=1800
    - mkdir -p test-results
    - cp bazel-testlogs/services/myservice-java/myservice-java_test/test.log test-results/junit-output.log 2>/dev/null || true
    - export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
    - mkdir -p "$DOCKER_CONFIG"
    - |
      printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
        "$CI_REGISTRY" \
        "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
        > "$DOCKER_CONFIG/config.json"
    - bazel run --stamp //services/myservice-java:image_push
  artifacts:
    when: always
    paths:
      - test-results/
    expire_in: 7 days
```

## Adding Bazel additively to an existing CI

Existing repo's `.gitlab-ci.yml` already has Prerequisites, Snapshot,
etc. Add Bazel as a new top-level stage and one trigger per service.
Existing jobs untouched.

```yaml
include:
  # ... existing includes ...

default:
  # ... existing default config ...

stages:
  - Prerequisites      # existing
  - Docs-Preview       # existing
  - Docs-Publish       # existing
  - Snapshot           # existing
  - Bazel              # new

# ... existing Prerequisites and Snapshot jobs unchanged ...

bazel-myservice:
  stage: Bazel
  trigger:
    include: services/myservice/.gitlab-ci.yml
    strategy: depend
  rules:
    - if: $SERVICE && $SERVICE != "myservice"
      when: never
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
      changes:
        - services/myservice/**
        - MODULE.bazel
        - MODULE.bazel.lock
        - .bazelrc
        - rules/**
```

## Manual dispatch invocations

GitLab UI:

1. **CI/CD > Pipelines > Run pipeline**
2. Select branch
3. Add variable: key `SERVICE`, value `myservice` (omit to run all)

CLI:

```bash
# All services
glab ci run -b main

# Single service
glab ci run -b main --variables "SERVICE:myservice"
```

REST API (for scripted retries):

```bash
curl --request POST \
  --header "PRIVATE-TOKEN: $GITLAB_TOKEN" \
  --form "ref=main" \
  --form "variables[][key]=SERVICE" \
  --form "variables[][value]=myservice" \
  "https://gitlab.example.com/api/v4/projects/$PROJECT_ID/pipeline"
```

## Scheduled pipelines

Set up under **CI/CD > Schedules**:

- Branch: `main` (or whichever default branch)
- Cron: `0 6 * * *` (daily at 06:00 UTC)
- Variables: leave empty (runs all services)

The schedule rule (`if: $CI_PIPELINE_SOURCE == "schedule"`) on every
trigger ensures all service pipelines fire.

## Cache strategy notes

Bazel's local action cache lives at `~/.cache/bazel`. GitLab CI runners
typically have ephemeral storage, so:

- Inside a single job: builds re-use the cache from earlier `bazel build`
  invocations.
- Across jobs: cache is lost. The pre-warmed CI image (with Bazel
  pre-downloaded) and a future remote cache are how you mitigate this.

If you need to share state across jobs in the same pipeline (e.g.
testing then pushing), keep both steps in the same `script:` so they
share the local cache. Splitting into separate jobs forces a re-fetch.

For a real remote cache, the simplest options are:

- bazel-remote (https://github.com/buchgr/bazel-remote): self-hosted gRPC
  server.
- Buildbarn: full Bazel remote execution + caching.
- Cloud-managed: BuildBuddy, EngFlow, etc.

Once a cache exists, uncomment in `.bazelrc`:

```
build:ci --remote_cache=grpc://CACHE_HOST:9092
build:ci --remote_upload_local_results=true
build:ci --remote_timeout=60
```

and re-add `--config=ci` to CI scripts.
