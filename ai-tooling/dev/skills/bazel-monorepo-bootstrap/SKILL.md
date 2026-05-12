---
name: bazel-monorepo-bootstrap
description: >-
  Bootstraps Bazel in an existing polyglot monorepo from zero to a working
  `bazel build //...`. Generates the root files Bazel needs:
  `MODULE.bazel` (Bzlmod), `WORKSPACE.bzlmod`, `.bazelrc`, `.bazelversion`,
  root `BUILD.bazel` with Gazelle, `setup.sh` (Bazelisk installer),
  `tools/workspace_status.sh` (git-stamp for OCI tags), `platforms/BUILD.bazel`
  (linux x86_64 + arm64), and a `ci/Dockerfile`. Triggers on adding Bazel
  to an existing repo, bootstrapping Bazel, MODULE.bazel from scratch,
  Bazelisk install, polyglot monorepo Bazel, .bazelrc profiles
  (debug/release/ci), Bzlmod migration, or initial Bazel scaffold for
  a polyglot codebase.
license: Apache-2.0
compatibility: Requires a local checkout of an NVCF or Bazel monorepo
author: "nvcf-core-eng <nvcf-core-eng@exchange.nvidia.com>"
version: "1.0.0"
tags:
  - nvcf
  - bazel
  - bootstrap
  - bzlmod
  - gazelle
  - polyglot-monorepo
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
    - bootstrap
    - bzlmod
    - gazelle
    - polyglot-monorepo
  languages:
    - starlark
    - shell
  frameworks:
    - bazel
  domain: build-systems
---

# Bazel Monorepo Bootstrap

Add Bazel to an existing repository in a deterministic, additive way. After
this skill runs, the repo has a working `MODULE.bazel`, `bazel build //...`
succeeds with at least one target, and existing per-language build systems
(Make, cargo, mvn, go) are untouched.

This skill is the entry point for adding Bazel. Once the root scaffold is in
place, follow the language-specific skills:

- Go: `bazel-go-gazelle`
- Rust: `bazel-rust-crate-universe`
- Java: `bazel-java-maven`
- OCI images: `bazel-oci-images`
- GitLab CI: `bazel-gitlab-child-pipelines`
- NVCF synthetic-import subtrees: `bazel-synthetic-import-strategy`

## Quick Reference

| Artifact | Purpose |
|----------|---------|
| `.bazelversion` | Pins the Bazel version (managed by Bazelisk) |
| `WORKSPACE.bzlmod` | Empty file; signals "use Bzlmod only" |
| `MODULE.bazel` | All third-party dependencies and toolchain registration |
| `.bazelrc` | Build flags and profiles (`debug`, `release`, `ci`) |
| `BUILD.bazel` (root) | Gazelle target plus repo-wide directives |
| `setup.sh` | One-shot Bazelisk installer for new contributors |
| `tools/workspace_status.sh` | Stamps git commit and version into OCI tags |
| `platforms/BUILD.bazel` | `linux_arm64` and `linux_x86_64` platform definitions |
| `ci/Dockerfile` | Pre-warmed Bazel image used by CI |

## Workflow

```
Bazel Bootstrap Progress:
- [ ] Step 1:  Choose Bazel version and pin .bazelversion
- [ ] Step 2:  Create empty WORKSPACE.bzlmod
- [ ] Step 3:  Write MODULE.bazel with the bazel_dep blocks you need
- [ ] Step 4:  Write .bazelrc with debug/release/ci profiles
- [ ] Step 5:  Add root BUILD.bazel with Gazelle target
- [ ] Step 6:  Add platforms/BUILD.bazel
- [ ] Step 7:  Add tools/workspace_status.sh (only if building OCI images)
- [ ] Step 8:  Add setup.sh (Bazelisk installer)
- [ ] Step 9:  Add ci/Dockerfile (pre-warmed CI image)
- [ ] Step 10: Validate: bazel info, bazel mod graph, build a hello target
```

### Step 1: Choose Bazel Version

Pin to a recent Bazel 8.x release. Bzlmod is on by default in Bazel 8 and the
ecosystem (rules_go, rules_rust, rules_jvm_external, rules_oci) all have
known-good versions for it.

```bash
echo "8.6.0" > .bazelversion
```

Bazelisk reads this file and downloads the matching Bazel automatically. Do
not commit a Bazel binary.

### Step 2: Create `WORKSPACE.bzlmod`

```bash
touch WORKSPACE.bzlmod
```

This file stays empty. Its presence tells Bazel "I have migrated to Bzlmod
and the legacy WORKSPACE file should be ignored." Do not create a `WORKSPACE`
or `WORKSPACE.bazel` file unless you have a hard requirement for legacy rules.

### Step 3: Write `MODULE.bazel`

Decide which language stacks the repo needs. Use the matrix below as a
starting point and only include the blocks for languages you actually build.

| Stack | `bazel_dep` block | Companion skill |
|-------|-------------------|-----------------|
| Go | `rules_go`, `gazelle` | `bazel-go-gazelle` |
| Rust | `rules_rust`, `hermetic_cc_toolchain`, `protobuf` (often) | `bazel-rust-crate-universe` |
| Java | `rules_java`, `rules_jvm_external`, `contrib_rules_jvm` | `bazel-java-maven` |
| OCI images | `platforms`, `rules_oci`, `rules_pkg`, `aspect_bazel_lib`, `hermetic_cc_toolchain` | `bazel-oci-images` |
| Python | `rules_python` | (transitive; keep `ignore_root_user_error = True` for CI containers) |

The full annotated `MODULE.bazel` template lives in [reference.md](reference.md).
A minimal Go-only starter is:

```python
module(name = "myrepo", version = "0.0.1")

bazel_dep(name = "rules_go", version = "0.60.0")
bazel_dep(name = "gazelle", version = "0.48.0")

go_sdk = use_extension("@rules_go//go:extensions.bzl", "go_sdk")
go_sdk.download(version = "1.24.12")

go_deps = use_extension("@gazelle//:extensions.bzl", "go_deps")
go_deps.from_file(go_mod = "//:go.mod")
```

Verify versions against the [Bazel Central Registry](https://registry.bazel.build/)
before committing. Outdated module versions are the most common first-build
failure.

### Step 4: Write `.bazelrc`

The starter `.bazelrc` defines four things: default compilation mode, Bzlmod
on, platform-aware config selection, and three named profiles
(`debug`, `release`, `ci`). Full template in [reference.md](reference.md).

Critical settings that catch new bootstrappers:

- `common --enable_bzlmod` is redundant on Bazel 8 but harmless; keep it for
  cross-version safety.
- `common --enable_platform_specific_config` lets you write `build:linux ...`
  and `build:macos ...` blocks that auto-apply.
- `build --incompatible_strict_action_env` improves remote-cache hit rates.
- `startup --host_jvm_args=-Xmx4g` prevents OOM on large analysis phases.
- For OCI builds: `build --workspace_status_command=./tools/workspace_status.sh`
  and `build --nostamp` (let CI opt in with `--stamp` per-build).

Per-language flags belong in `.bazelrc`, not in BUILD files. For example:

```
build --@rules_go//go/config:pure
build --java_language_version=21
build --java_runtime_version=remotejdk_21
build:linux --@rules_rust//:extra_rustc_flags=-Clink-arg=-fuse-ld=lld
```

### Step 5: Root `BUILD.bazel` with Gazelle

```python
load("@gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/myorg/myrepo
gazelle(name = "gazelle")
```

The `# gazelle:prefix` directive must match your `go.mod` `module` line. If
the repo has multiple Go modules, set `prefix` per-module in each module's
root `BUILD.bazel` and leave the root one as a sane default. See
`bazel-go-gazelle` for the multi-module pattern.

### Step 6: `platforms/BUILD.bazel`

Even if you do not build OCI images yet, define platforms early so later
multi-arch work is friction-free:

```python
package(default_visibility = ["//visibility:public"])

platform(
    name = "linux_arm64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:aarch64",
    ],
)

platform(
    name = "linux_x86_64",
    constraint_values = [
        "@platforms//os:linux",
        "@platforms//cpu:x86_64",
    ],
)
```

### Step 7: `tools/workspace_status.sh`

Skip this step if you are not building OCI images yet. When you are, add the
script and wire it via `build --workspace_status_command=./tools/workspace_status.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DIRTY=""
if [ "$COMMIT" != "unknown" ] && [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    DIRTY="-dirty"
fi
VERSION="0.0.1"
echo "STABLE_VERSION ${VERSION}"
echo "STABLE_GIT_COMMIT ${COMMIT}${DIRTY}"
echo "STABLE_OCI_TAG ${VERSION}-${COMMIT}${DIRTY}"
```

`STABLE_*` keys propagate to `--stamp`-aware rules. Non-stable keys force a
full rebuild on every commit, which is almost never what you want.

### Step 8: `setup.sh` (Bazelisk installer)

A one-command bootstrap for new contributors. The full script lives in
[reference.md](reference.md). It:

1. Detects host OS and arch.
2. Downloads `bazelisk` from the GitHub release page.
3. Installs it as `bazel` into `$INSTALL_DIR` (default `~/.local/bin` on
   Linux, `/usr/local/bin` on macOS).
4. Verifies `bazel --version` works.

After running `./setup.sh`, the next `bazel build //...` will auto-download
the version pinned in `.bazelversion`.

### Step 9: `ci/Dockerfile`

A pre-warmed CI image saves several minutes per pipeline by avoiding the
"download Bazel" step on every job. The full Dockerfile lives in
[reference.md](reference.md). Key elements:

- Base on a stable distro (`ubuntu:24.04`).
- Install `gcc`, `g++`, `lld`, `cmake`, `pkg-config`, `perl`, `ca-certificates`,
  `docker.io`, `docker-compose-v2`, `python3`. The `lld` install is required
  for Rust linking under `rules_rust`.
- `update-alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 100` so
  builds that invoke the system `ld` get the lld implementation.
- Install Bazelisk as `bazel`.
- Pre-warm: create a throwaway workspace, run `bazel version`, then delete it.
  This downloads the pinned Bazel into the image cache so the first real
  build does not pay the cost.
- For Java/Spring repos that use Testcontainers `withLocalCompose(true)`,
  add the `docker-compose` shim (`docker compose "$@"`).

### Step 10: Validate

```bash
./setup.sh
bazel info release
bazel mod graph
```

`bazel info release` confirms the version. `bazel mod graph` validates that
the module dependency tree resolves cleanly. If either fails, fix the
error before adding language-specific rules.

## Decision Matrix: What to Defer

Do not add every `bazel_dep` you might ever need. Add language blocks only
when you are about to wire a real target in that language. A common mistake
is including `rules_jvm_external` with an empty `maven.install` early; it
adds a slow first-build cost for nothing.

| Block | Add when |
|-------|----------|
| `rules_go` + `gazelle` | First Go binary or library is being wired |
| `rules_rust` + `crate_universe` | First Rust crate is being wired |
| `rules_java` + `rules_jvm_external` | First Java target is being wired |
| `rules_oci` + `rules_pkg` | First container image is being built |
| `hermetic_cc_toolchain` | Multi-arch builds (Rust or C/C++) start |
| `protobuf` | Source `.proto` files are being compiled |

## Common First-Build Failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `JAVA_HOME` warning in red text | Bazel using its own embedded JDK; the warning is informational | Ignore it; pin via `--java_runtime_version=remotejdk_21` if you want to silence it |
| `Module not found in registry` | Outdated module version | Check [registry.bazel.build](https://registry.bazel.build/) and bump |
| `error: missing input file` for proto deps | `protobuf` module not declared | Add `bazel_dep(name = "protobuf", version = "31.1")` |
| Wrong arch in build outputs | Missing `enable_platform_specific_config` | Add `common --enable_platform_specific_config` to `.bazelrc` |
| `-fuse-ld=lld` not found at link time | CI image missing `lld` | Add `lld` to `apt install` in `ci/Dockerfile` |
| `SIGILL` in Rust cross-compile | Both native and `zig` toolchains registered for the same arch | Only register `register_toolchains("@zig_sdk//toolchain:linux_arm64_gnu.2.38")` (cross only); see `bazel-rust-crate-universe` |

## Additional Resources

- Full file templates: [reference.md](reference.md)
- Per-language wiring: see the companion skills listed at the top of this file
- Bazel Central Registry: https://registry.bazel.build/
- Bzlmod migration guide: https://bazel.build/external/migration
