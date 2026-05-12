---
name: bazel-oci-images
description: >-
  Builds multi-arch OCI container images from Bazel binaries (Go, Rust,
  Java) using `rules_oci`, `rules_pkg`, and `aspect_bazel_lib`. Provides a
  per-language macro pattern (`go_oci_image`, `java_oci_image`,
  `rust_oci_image`) that wraps the underlying `oci_image` plus
  `oci_image_index` plus platform transitions, supports both `oci_load`
  (local docker daemon) and `oci_push` (registry), and stamps Git commit
  and version into image tags via `tools/workspace_status.sh`. Covers
  `oci.pull` with multi-arch base images, `use_repo` for the per-arch
  child repos, crane-compatible registry auth via `$DOCKER_CONFIG` for
  `oci_push` to GitLab Container Registry, the `latest` plus stamped-tag
  pattern, and the `--stamp` build flag. Triggers on rules_oci, Bazel
  container image, multi-arch container Bazel, oci_push GitLab, stamp git
  commit OCI tag, go_oci_image, java_oci_image, rust_oci_image, or
  oci.pull.
license: Apache-2.0
compatibility: Requires a local checkout of an NVCF or Bazel monorepo
author: "nvcf-core-eng <nvcf-core-eng@exchange.nvidia.com>"
version: "1.0.0"
tags:
  - nvcf
  - bazel
  - oci
  - container
  - rules_oci
  - hermetic_cc
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
    - oci
    - container
    - rules_oci
    - hermetic_cc
  languages:
    - starlark
    - go
    - rust
    - java
  frameworks:
    - bazel
  domain: build-systems
---

# Bazel OCI Images

Package compiled binaries into multi-arch (amd64 + arm64) container images
without Docker on the build host, then push them to a registry with
git-stamped tags. Assumes the bootstrap and at least one language-specific
skill (`bazel-go-gazelle`, `bazel-java-maven`, or
`bazel-rust-crate-universe`) are in place.

## Quick Reference

| Concept | Where it lives |
|---------|----------------|
| Base images | `MODULE.bazel`: `oci.pull(...)` plus `use_repo(oci, ...)` |
| Per-arch repos | `<base_name>_linux_amd64`, `<base_name>_linux_arm64_v8` |
| Image macros | `rules/oci/defs.bzl` (your repo) loading from `private/{go,java,rust}.bzl` |
| Common helpers | `rules/oci/private/common.bzl` (multi-arch transition + push) |
| Multi-arch transition | `rules/oci/transition.bzl` |
| Tag stamping | `tools/workspace_status.sh` plus `--stamp` |
| Push auth | `$DOCKER_CONFIG/config.json` (crane reads this; defaults to `$HOME/.docker`) |

## Workflow

```
Bazel OCI Setup Progress:
- [ ] Step 1:  Add rules_oci, rules_pkg, aspect_bazel_lib, hermetic_cc to MODULE.bazel
- [ ] Step 2:  Pull base images via oci.pull (multi-arch)
- [ ] Step 3:  Add platforms/BUILD.bazel (linux_arm64 and linux_x86_64)
- [ ] Step 4:  Create rules/oci/ with defs.bzl + private/{common,go,java,rust}.bzl + transition.bzl
- [ ] Step 5:  Wire workspace_status.sh for tag stamping
- [ ] Step 6:  Add a <lang>_oci_image macro call to each binary BUILD.bazel
- [ ] Step 7:  Build and load locally: bazel run //path:image_load
- [ ] Step 8:  Push from CI: bazel run --stamp //path:image_push
```

### Step 1: `MODULE.bazel` deps

```python
bazel_dep(name = "platforms", version = "1.0.0")
bazel_dep(name = "rules_oci", version = "2.2.7")
bazel_dep(name = "rules_pkg", version = "1.2.0")
bazel_dep(name = "aspect_bazel_lib", version = "2.19.3")
bazel_dep(name = "hermetic_cc_toolchain", version = "4.1.0")
```

`hermetic_cc_toolchain` is needed for cross-arch C/C++ compilation (Rust
binaries linking to system libs, multi-arch protoc, etc.). Even if you
only ship Go binaries, leave it in for protoc.

### Step 2: Pull base images

```python
oci = use_extension("@rules_oci//oci:extensions.bzl", "oci")

oci.pull(
    name = "ubuntu_noble",
    digest = "sha256:ef59d9e82939bbce08973bdffb8761b025f75369fb7d2882cdc4938b5a9e992e",
    image = "public.ecr.aws/ubuntu/ubuntu",
    platforms = ["linux/arm64/v8", "linux/amd64"],
)

oci.pull(
    name = "eclipse_temurin_21_jre",
    image = "public.ecr.aws/docker/library/eclipse-temurin",
    tag = "21-jre-noble",
    platforms = ["linux/arm64/v8", "linux/amd64"],
)

use_repo(
    oci,
    "ubuntu_noble",
    "ubuntu_noble_linux_amd64",
    "ubuntu_noble_linux_arm64_v8",
    "eclipse_temurin_21_jre",
    "eclipse_temurin_21_jre_linux_amd64",
    "eclipse_temurin_21_jre_linux_arm64_v8",
)
```

Always pin by digest. If you must use a tag, accept that builds will
become non-reproducible the moment upstream re-publishes that tag.

The per-arch child repos (`<name>_linux_amd64`, `<name>_linux_arm64_v8`)
must each appear in `use_repo` even though `<name>` itself does too.
Without them the multi-arch index assembly fails with a confusing
"unknown repository" error.

For mirror availability, `public.ecr.aws/ubuntu/ubuntu` and
`public.ecr.aws/docker/library/...` are AWS public mirrors that do not
rate-limit. Avoid `docker.io` for CI; you will hit anonymous pull
throttling.

### Step 3: Platforms

See `bazel-monorepo-bootstrap` reference.md. The two definitions
(`linux_arm64`, `linux_x86_64`) are required by the multi-arch transition.

### Step 4: Create the macro layer

The macro layer (`rules/oci/`) is a small abstraction so service BUILD
files do not have to repeat the multi-arch boilerplate. Five files:

```
rules/oci/
|-- BUILD.bazel               # comment-only marker
|-- defs.bzl                  # public API: go_oci_image, java_oci_image, rust_oci_image
|-- transition.bzl            # multi_arch transition rule
`-- private/
    |-- common.bzl            # create_oci_image: shared image+push helper
    |-- go.bzl                # go_oci_image macro
    |-- java.bzl              # java_oci_image macro
    `-- rust.bzl              # rust_oci_image macro
```

Full bodies in [reference.md](reference.md). The high-level flow each
macro implements:

1. Wrap the binary in a `pkg_tar` layer.
2. Build an `oci_image` with that layer on top of the base image.
3. Use `platform_transition_filegroup` to build for the host platform
   (so `oci_load` works locally on whatever the contributor is running).
4. Use a custom `multi_arch` transition to build the same image for both
   `linux_arm64` and `linux_x86_64`, then assemble them into an
   `oci_image_index` (multi-arch manifest list).
5. Generate `<name>_load` (`docker load`-compatible) and `<name>.tar`
   targets for local use.
6. If `registry` is set, generate a `<name>_push` target that uploads the
   index to the registry with stamped tags.

### Step 5: Tag stamping

In `.bazelrc`:

```
build --workspace_status_command=./tools/workspace_status.sh
build --nostamp
```

`--nostamp` is the default for normal builds (so cache hit rates stay
high). CI opts in per-build with `--stamp`:

```bash
bazel run --stamp //myservice:image_push
```

The `expand_template` rule inside `create_oci_image` reads the
`STABLE_VERSION`, `STABLE_OCI_TAG`, and `STABLE_GIT_COMMIT` keys produced
by `workspace_status.sh` and substitutes them into the `remote_tags`
list. With `--stamp`, the image is pushed with four tags:
`latest`, `{VERSION}` (e.g. `0.0.1`), `{OCI_TAG}` (e.g. `0.0.1-abc1234`),
and `{COMMIT}` (e.g. `abc1234`). Without `--stamp`, the literal
placeholder strings are used (which is what you want for rebuilding the
target without bumping anything).

### Step 6: Use the macro from a service BUILD

Go:

```python
load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("//rules/oci:defs.bzl", "go_oci_image")

go_binary(
    name = "myservice",
    embed = [":myservice_lib"],
)

go_oci_image(
    name = "image",
    binary = ":myservice",
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

Rust:

```python
load("@rules_rust//rust:defs.bzl", "rust_binary")
load("//rules/oci:defs.bzl", "rust_oci_image")

rust_binary(name = "server", ...)

rust_oci_image(
    name = "image",
    binary = ":server",
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

Java (Spring Boot fat jar):

```python
load("@rules_java//java:defs.bzl", "java_binary")
load("//rules/oci:defs.bzl", "java_oci_image")

java_binary(
    name = "myservice",
    main_class = "com.myorg.myservice.App",
    runtime_deps = [":myservice_lib"],
)

java_oci_image(
    name = "image",
    binary = ":myservice",
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

The Java macro packages `<binary>_deploy.jar` (Bazel's standard
fat-jar output) under `/app/` and uses `java -jar /app/<name>_deploy.jar`
as the entrypoint. Override `entrypoint` or `jvm_flags` on the macro
call when you need to.

### Step 7: Local validation

```bash
bazel build //myservice:image           # build (produces tarball)
bazel run //myservice:image_load        # load into local docker
docker run --rm myservice:latest        # smoke test
```

`image_load` uses the host platform's image (built by
`platform_transition_filegroup`). The multi-arch index is only assembled
when you build `:image_index` or run `:image_push`.

### Step 8: Push from CI

Set `DOCKER_CONFIG` to a job-scoped directory and write the registry auth
file there (avoids touching `$HOME` and works in read-only home runners):

```bash
export DOCKER_CONFIG="$CI_PROJECT_DIR/.docker"
mkdir -p "$DOCKER_CONFIG"
printf '{"auths":{"%s":{"auth":"%s"}}}\n' \
  "$CI_REGISTRY" \
  "$(printf '%s:%s' "$CI_REGISTRY_USER" "$CI_REGISTRY_PASSWORD" | base64 -w0)" \
  > "$DOCKER_CONFIG/config.json"
bazel run --stamp //myservice:image_push
```

Why `$DOCKER_CONFIG` and not `bazel run` flags:

- `oci_push` invokes `crane` under the hood, which reads
  `$DOCKER_CONFIG/config.json` for registry auth (or the Docker default
  location when `DOCKER_CONFIG` is unset). There is no equivalent CLI
  flag.
- The `auth` field is `base64(username:password)`, the same format
  `docker login` writes. It is a per-registry token, not a JWT.
- GitLab provides `$CI_REGISTRY_USER` and `$CI_REGISTRY_PASSWORD` to the
  runner. For other registries, substitute the equivalent CI variables.
- Pointing `DOCKER_CONFIG` at `$CI_PROJECT_DIR/.docker` keeps the auth
  file inside the job workspace and outside `$HOME`, which makes cleanup
  automatic and avoids interactions with shared-home runners.

## Multi-arch Notes

### Why the per-arch repos must be in `use_repo`

`oci.pull` with multiple `platforms` creates one parent repo and one repo
per platform. The macro layer references all three (parent for image
metadata, per-arch for the actual layer data). If you forget the per-arch
entries in `use_repo`, the build error is:

```
no such repository '@ubuntu_noble_linux_arm64_v8'
```

### Why `latest` is in the tag list

GitLab Container Registry hides repositories from the UI listing if they
have never had a tag named `latest`. Pushing only `0.0.1-abc1234` makes
the repo invisible until the user clicks deep into "Untagged manifests".
Always push `latest` alongside the stamped tags so the UI is usable.

### Why `--config=ci` was removed from CI scripts

In an earlier reference iteration, `--config=ci` was set on CI jobs but
the `build:ci --remote_cache=...` line was a placeholder. Bazel treated
the missing cache as a hard failure. Two options:

1. Comment out `build:ci --remote_cache=...` until a real cache exists
   (the current default in [reference.md](reference.md)).
2. Drop `--config=ci` from CI scripts entirely until the cache is wired.

Both are equivalent in effect.

## Common Failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `no such repository '@<base>_linux_amd64'` | Per-arch repos missing from `use_repo` | Add `<base>_linux_amd64` and `<base>_linux_arm64_v8` |
| Push fails with `401 Unauthorized` | Registry auth file (under `$DOCKER_CONFIG`) not written | Run the `printf '{"auths":...}' > "$DOCKER_CONFIG/config.json"` snippet from Step 8 before `bazel run ...:image_push` |
| Pushed image not visible in GitLab UI | No `latest` tag pushed | Add `"latest"` to the macro's tag template (already there in reference) |
| Multi-arch image assembled but each arch is the same | `multi_arch` transition not used | Use `oci_image_index` over the `multi_arch`-transitioned target, not directly over `oci_image` |
| `bazel run :image_load` produces wrong-arch image | Host arch is amd64 but `select` falls through to arm64 | Confirm `platform_transition_filegroup` `select` covers `@platforms//cpu:x86_64` |
| Stamped tags show literal `{VERSION}` strings | `--stamp` flag missing | Add `--stamp` to the `bazel run` invocation (or set `build:ci --stamp` for CI) |
| Build cache rebuilds everything on every commit | Non-stable status keys (no `STABLE_` prefix) | Use `STABLE_VERSION`, `STABLE_GIT_COMMIT`, `STABLE_OCI_TAG` in `workspace_status.sh` |
| Cross-arch builds fail with linker errors | `hermetic_cc_toolchain` not registered or wrong arch | Re-check `register_toolchains("@zig_sdk//toolchain:linux_arm64_gnu.2.38")` |

## Optional: Distroless or Scratch Bases

The reference layer uses Ubuntu and Eclipse Temurin bases. To switch to
distroless or scratch:

1. Add `bazel_dep(name = "rules_distroless", version = "0.6.1")`.
2. Add a CA certificate layer (since scratch has no `ca-certificates`):
   ```python
   http_archive = use_repo_rule("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
   http_archive(
       name = "ca-certificates",
       build_file_content = """exports_files(["data.tar.zst"])""",
       sha256 = "<verify upstream>",
       type = ".deb",
       urls = ["https://launchpad.net/ubuntu/+archive/primary/+files/ca-certificates_20240203_all.deb"],
   )
   ```
3. Reference the cert layer in `COMMON_LAYERS` inside `common.bzl`.
4. Switch `DEFAULT_BASE` to your distroless image.

Most service teams stick with Ubuntu noble for ease of debugging; switch
only if image size or attack surface is a hard requirement.

## Additional Resources

- Full source for `rules/oci/`: [reference.md](reference.md)
- rules_oci: https://github.com/bazel-contrib/rules_oci
- crane (the underlying push tool): https://github.com/google/go-containerregistry
- aspect_bazel_lib `expand_template`: https://github.com/aspect-build/bazel-lib
