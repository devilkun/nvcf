# Bazel Monorepo Bootstrap: File Templates

Full templates for every file the bootstrap skill creates. Copy-paste, then
adapt the marked placeholders.

## `.bazelversion`

```
8.6.0
```

## `WORKSPACE.bzlmod`

Empty file. Its presence enables Bzlmod-only mode and disables the legacy
`WORKSPACE` resolution path.

## `MODULE.bazel` (annotated polyglot starter)

```python
"""<Repo Name> — Bazel module definition.

All Java dependency versions are sourced from:
  JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 mvn dependency:list -f <svc>/pom.xml
"""

module(
    name = "myrepo",
    version = "0.0.1",
)

# rules_python is pulled transitively; allow running as root in CI containers
bazel_dep(name = "rules_python", version = "1.4.1")

# ============================================================================
# OCI containers (multi-arch). Drop this block if not building images yet.
# ============================================================================
bazel_dep(name = "platforms", version = "1.0.0")
bazel_dep(name = "rules_oci", version = "2.2.7")
bazel_dep(name = "rules_pkg", version = "1.2.0")
bazel_dep(name = "aspect_bazel_lib", version = "2.19.3")
bazel_dep(name = "hermetic_cc_toolchain", version = "4.1.0")

python = use_extension("@rules_python//python/extensions:python.bzl", "python")
python.toolchain(
    ignore_root_user_error = True,
    python_version = "3.11",
)

# ============================================================================
# Go. See bazel-go-gazelle for full setup.
# ============================================================================
bazel_dep(name = "rules_go", version = "0.60.0")
bazel_dep(name = "gazelle", version = "0.48.0")

go_sdk = use_extension("@rules_go//go:extensions.bzl", "go_sdk")
go_sdk.download(version = "1.24.12")

go_deps = use_extension("@gazelle//:extensions.bzl", "go_deps")
go_deps.from_file(go_mod = "//path/to/your/go.mod:go.mod")
# use_repo(go_deps, "com_github_xxx_yyy", ...)  # filled in after first run

# ============================================================================
# C++ cross-compilation (needed for multi-arch protoc / Rust cross builds).
# Only register the cross toolchain; native arch uses the host compiler.
# ============================================================================
zig = use_extension("@hermetic_cc_toolchain//toolchain:ext.bzl", "toolchains")
use_repo(zig, "zig_sdk")

register_toolchains(
    "@zig_sdk//toolchain:linux_arm64_gnu.2.38",
)

# ============================================================================
# Rust. See bazel-rust-crate-universe for full setup.
# ============================================================================
bazel_dep(name = "rules_rust", version = "0.69.0")

rust = use_extension("@rules_rust//rust:extensions.bzl", "rust")
rust.toolchain(
    edition = "2021",
    versions = ["1.91.1"],
)
use_repo(rust, "rust_toolchains")
register_toolchains("@rust_toolchains//:all")

# ============================================================================
# Java. See bazel-java-maven for full setup.
# ============================================================================
bazel_dep(name = "rules_java", version = "8.14.0")
bazel_dep(name = "contrib_rules_jvm", version = "0.32.0")
bazel_dep(name = "rules_jvm_external", version = "6.10")
bazel_dep(name = "protobuf", version = "31.1")

# ============================================================================
# OCI base images
# ============================================================================
oci = use_extension("@rules_oci//oci:extensions.bzl", "oci")

oci.pull(
    name = "ubuntu_noble",
    digest = "sha256:ef59d9e82939bbce08973bdffb8761b025f75369fb7d2882cdc4938b5a9e992e",
    image = "public.ecr.aws/ubuntu/ubuntu",
    platforms = ["linux/arm64/v8", "linux/amd64"],
)

use_repo(
    oci,
    "ubuntu_noble",
    "ubuntu_noble_linux_amd64",
    "ubuntu_noble_linux_arm64_v8",
)
```

Always pin OCI base images by digest. Tag-based pulls break reproducibility
when upstream re-publishes the same tag.

## `.bazelrc`

```
# ============================================================================
# <Repo> Bazel Configuration
# ============================================================================

build --compilation_mode=fastbuild
common --enable_bzlmod
build --incompatible_strict_action_env
common --enable_platform_specific_config

# Stamping for OCI image tags. Default off; CI opts in with --stamp.
build --workspace_status_command=./tools/workspace_status.sh
build --nostamp

test --test_output=errors

startup --host_jvm_args=-Xmx4g

# ---- Go ----
build --@rules_go//go/config:pure

# ---- Java ----
build --java_language_version=21
build --java_runtime_version=remotejdk_21
build --nojava_header_compilation
build --javacopt="-XepDisableAllChecks"

# ---- Rust ----
# Use LLD instead of the deprecated gold linker (Linux only).
build:linux --@rules_rust//:extra_rustc_flags=-Clink-arg=-fuse-ld=lld
# macOS sandbox blocks cargo_build_script subdirectory creation.
build:macos --strategy_regexp=CargoBuildScriptRun=local

# ---- CI profile ----
# Remote cache: uncomment after deploying a real cache.
# build:ci --remote_cache=grpc://CACHE_HOST:9092
# build:ci --remote_upload_local_results=true
# build:ci --remote_timeout=60
build:ci --jobs=32
build:ci --stamp

# ---- Debug profile ----
build:debug --compilation_mode=dbg
build:debug -s

# ---- Release profile ----
build:release --compilation_mode=opt
build:release --strip=always
```

Do not add `--config=ci` to your CI job scripts until the remote cache is
real; with the placeholder commented out it is a no-op, but other flags
(like `--jobs=32`) can hide misconfiguration. Add it back when there is a
working cache to talk to.

## Root `BUILD.bazel`

```python
load("@gazelle//:def.bzl", "gazelle")

# Gazelle: auto-generate BUILD files for Go packages.
# Usage:
#   bazel run //:gazelle                     - generate or update BUILD files
#   bazel run //:gazelle -- update-repos     - sync go.mod deps
gazelle(
    name = "gazelle",
    prefix = "github.com/myorg/myrepo",
)
```

## `platforms/BUILD.bazel`

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

## `tools/workspace_status.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DIRTY=""
if [ "$COMMIT" != "unknown" ] && [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    DIRTY="-dirty"
fi

# Base version - update on releases.
VERSION="0.0.1"

echo "STABLE_VERSION ${VERSION}"
echo "STABLE_GIT_COMMIT ${COMMIT}${DIRTY}"
echo "STABLE_OCI_TAG ${VERSION}-${COMMIT}${DIRTY}"
```

Make it executable: `chmod +x tools/workspace_status.sh`.

## `setup.sh` (Bazelisk installer)

```bash
#!/usr/bin/env bash
set -euo pipefail

BAZELISK_VERSION="1.25.0"
if [[ "$(uname -s)" == "Darwin" ]]; then
  DEFAULT_INSTALL_DIR="/usr/local/bin"
else
  DEFAULT_INSTALL_DIR="$HOME/.local/bin"
fi
INSTALL_DIR="${INSTALL_DIR:-$DEFAULT_INSTALL_DIR}"

info()  { echo "[setup] $*"; }
error() { echo "[setup] ERROR: $*" >&2; }
check_command() { command -v "$1" >/dev/null 2>&1; }

install_bazelisk() {
  if check_command bazel; then
    local current
    current="$(bazel --version 2>/dev/null || true)"
    if echo "$current" | grep -qi bazelisk; then
      info "Bazelisk already installed: $current"
      return 0
    fi
  fi

  info "Installing Bazelisk ${BAZELISK_VERSION} to ${INSTALL_DIR}..."
  mkdir -p "$INSTALL_DIR"

  local os arch url
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m)"
  case "$arch" in
    x86_64)  arch="amd64" ;;
    aarch64|arm64) arch="arm64" ;;
    *) error "Unsupported architecture: $arch"; exit 1 ;;
  esac

  url="https://github.com/bazelbuild/bazelisk/releases/download/v${BAZELISK_VERSION}/bazelisk-${os}-${arch}"
  info "Downloading from ${url}"

  if check_command curl; then
    curl -fSL -o "${INSTALL_DIR}/bazel" "$url"
  elif check_command wget; then
    wget -q -O "${INSTALL_DIR}/bazel" "$url"
  else
    error "Neither curl nor wget found"; exit 1
  fi

  chmod +x "${INSTALL_DIR}/bazel"
  info "Installed bazelisk as ${INSTALL_DIR}/bazel"
}

verify_path() {
  if ! echo "$PATH" | tr ':' '\n' | grep -qx "$INSTALL_DIR"; then
    info ""
    info "Add this to your shell profile (~/.bashrc or ~/.zshrc):"
    info "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    info ""
    export PATH="${INSTALL_DIR}:$PATH"
  fi
}

verify_install() {
  info "Verifying installation..."
  if ! check_command bazel; then
    error "bazel not found in PATH after install"
    exit 1
  fi
  bazel --version
  info ""
  info "Bazel will auto-download version $(cat "$(dirname "$0")/.bazelversion") on first build."
}

info "=== <Repo Name> Bazel Setup ==="
install_bazelisk
verify_path
verify_install
info ""
info "Ready! Try:"
info "  bazel build //..."
```

Make it executable: `chmod +x setup.sh`.

## `ci/Dockerfile`

```dockerfile
FROM ubuntu:24.04

ARG BAZELISK_VERSION=1.25.0
ARG BAZEL_VERSION=8.6.0

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -qq && apt-get install -y -qq --no-install-recommends \
    curl git gcc g++ python3 \
    docker.io docker-compose-v2 \
    build-essential cmake pkg-config perl lld \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/* \
  && update-alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 100

# docker-compose shim: Testcontainers withLocalCompose(true) looks for `docker-compose`.
RUN printf '#!/usr/bin/env bash\ndocker compose "$@"\n' > /usr/local/bin/docker-compose \
  && chmod +x /usr/local/bin/docker-compose

# Bazelisk (installed as `bazel`).
RUN arch="$(uname -m)" && case "$arch" in x86_64) arch=amd64;; aarch64|arm64) arch=arm64;; esac \
  && curl -fSL -o /usr/local/bin/bazel \
     "https://github.com/bazelbuild/bazelisk/releases/download/v${BAZELISK_VERSION}/bazelisk-linux-${arch}" \
  && chmod +x /usr/local/bin/bazel

# Pre-warm: download Bazel itself so the first real build does not pay the cost.
RUN mkdir /tmp/warmup && cd /tmp/warmup \
  && echo "${BAZEL_VERSION}" > .bazelversion \
  && touch MODULE.bazel WORKSPACE \
  && bazel version \
  && rm -rf /tmp/warmup

LABEL description="<Repo> CI image - Ubuntu 24.04 + Bazel ${BAZEL_VERSION}"
```

Build and push:

```bash
docker build -t myregistry.example.com/myrepo/ci:1 ci/
docker push myregistry.example.com/myrepo/ci:1
```

Increment the tag (`ci:2`, `ci:3`, ...) on every change so existing
pipelines keep using the version they expect.

## `.gitignore` additions

```
# Bazel symlinks
bazel-*
```

The five `bazel-*` symlinks (`bazel-bin`, `bazel-out`, `bazel-testlogs`,
`bazel-<repo>`, sometimes `bazel-source`) point into `~/.cache/bazel`. They
must not be committed.

## `MODULE.bazel.lock`

Commit this file. It is the lockfile for the entire module graph and is what
makes Bazel builds reproducible across machines and CI. Treat it like
`go.sum`, `Cargo.lock`, or `package-lock.json`.
