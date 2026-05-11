---
name: bazel-rust-crate-universe
description: >-
  Wire a Rust service into a Bazel monorepo using `rules_rust` and the
  `crate_universe` extension. Covers `crate.from_cargo` setup with multiple
  `supported_platform_triples`, working with private cargo registries
  (`.cargo/config.toml` plus `CARGO_BAZEL_ISOLATED=false`), the
  `CARGO_BAZEL_REPIN=1 bazel sync` workflow, the `cargo_build_script`
  pattern with `protoc` for protobuf-generating `build.rs` files,
  hermetic_cc_toolchain cross-compilation (and the SIGILL pitfall when
  registering native and zig toolchains together), the `lld` linker flag,
  jemalloc feature placement (binary only, never library), and the macOS
  sandbox workaround for `cargo_build_script`. Use when the user mentions
  rules_rust, crate_universe, Bazel Rust, private cargo registry Bazel,
  hermetic_cc Rust, CARGO_BAZEL_REPIN, build.rs Bazel, or jemalloc Bazel.
version: "1.0.0"
author: NVCF Platform Team
tags:
  - bazel
  - rust
  - rules_rust
  - crate_universe
  - hermetic_cc
tools:
  - Read
  - Shell
  - Write
---

# Bazel Rust and crate_universe

Build a Rust service under Bazel from an existing Cargo workspace. This
skill assumes the root scaffold from `bazel-monorepo-bootstrap` is in place
and that `hermetic_cc_toolchain` is registered for cross-compilation.

## Quick Reference

| Concept | Where it lives |
|---------|----------------|
| Rust toolchain | `MODULE.bazel`: `rust.toolchain(...)` |
| Crate metadata source of truth | `Cargo.toml` + `Cargo.lock` |
| Crate resolution | `MODULE.bazel`: `crate.from_cargo(...)` |
| Private registry config | `<service>/.cargo/config.toml` |
| Per-target crate selection | `aliases()`, `all_crate_deps(...)` from generated `defs.bzl` |
| `build.rs` files | `cargo_build_script` rule with explicit `tools` and `build_script_env` |
| OCI image | `rust_oci_image` macro from `bazel-oci-images` |

## Workflow

```
Bazel Rust Wiring Progress:
- [ ] Step 1:  Confirm rules_rust + hermetic_cc_toolchain in MODULE.bazel
- [ ] Step 2:  Add crate.from_cargo block in MODULE.bazel
- [ ] Step 3:  Verify .cargo/config.toml is wired into the extension
- [ ] Step 4:  Hand-write the BUILD.bazel for the crate (rust_library + rust_binary + rust_test)
- [ ] Step 5:  Add cargo_build_script if the crate has a build.rs
- [ ] Step 6:  Add jemalloc feature on the binary target only
- [ ] Step 7:  Repin and validate
- [ ] Step 8:  (Optional) Wire OCI image via rust_oci_image
```

### Step 1: Confirm `rules_rust` and `hermetic_cc_toolchain`

In `MODULE.bazel`:

```python
bazel_dep(name = "rules_rust", version = "0.69.0")
bazel_dep(name = "hermetic_cc_toolchain", version = "4.1.0")

rust = use_extension("@rules_rust//rust:extensions.bzl", "rust")
rust.toolchain(
    edition = "2021",
    versions = ["1.91.1"],
)
use_repo(rust, "rust_toolchains")
register_toolchains("@rust_toolchains//:all")

zig = use_extension("@hermetic_cc_toolchain//toolchain:ext.bzl", "toolchains")
use_repo(zig, "zig_sdk")

# IMPORTANT: register only the cross toolchain, not native.
# Registering both causes SIGILL on x86_64 hosts because zig_sdk lies about
# being able to handle the native arch but the produced binaries crash.
register_toolchains(
    "@zig_sdk//toolchain:linux_arm64_gnu.2.38",
)
```

Pin `rules_rust` to a recent release that supports your `rust.toolchain`
edition. The crate_universe API has changed across versions; use one
consistent version across the module.

### Step 2: `crate.from_cargo` block

```python
crate = use_extension("@rules_rust//crate_universe:extension.bzl", "crate")
crate.from_cargo(
    name = "myservice_crates",
    cargo_config = "//myservice:.cargo/config.toml",
    cargo_lockfile = "//myservice:Cargo.lock",
    manifests = ["//myservice:Cargo.toml"],
    supported_platform_triples = [
        "x86_64-unknown-linux-gnu",
        "aarch64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
    ],
)
use_repo(crate, "myservice_crates")
```

Why each field matters:

- `name`: the workspace name you reference from BUILD files
  (`@myservice_crates//:defs.bzl`). Pick something unique per service so
  multiple Cargo workspaces in one Bazel module do not collide.
- `cargo_config`: required when the workspace pulls from a private registry.
  Without it, `crate.from_cargo` cannot resolve the crates and the build
  fails at sync time with a confusing "registry not found" error.
- `cargo_lockfile`: must be present and committed. crate_universe is
  deterministic only when the lockfile is.
- `manifests`: list of `Cargo.toml` files. For a single-crate workspace,
  one entry. For a multi-crate Cargo workspace, list each member's
  `Cargo.toml`.
- `supported_platform_triples`: the crate set is resolved per-triple; if
  you skip a triple here you cannot build for it later. Always list both
  `linux-gnu` triples for arm64 and amd64 if you ship containers.

### Step 3: `.cargo/config.toml` for private registries

Place at `<service>/.cargo/config.toml` and reference it from `cargo_config`
above:

```toml
[registries]
nvcf-internal = { index = "sparse+https://urm.nvidia.com/artifactory/api/cargo/sw-gpu-nvcf-cargo-local/" }

[source.crates-io]
replace-with = "nvcf-internal"
```

For Bazel to see this file, also add an `exports_files` line at the
service's top-level `BUILD.bazel`:

```python
exports_files([".cargo/config.toml"])
```

CI must set:

```bash
export CARGO_BAZEL_ISOLATED=false
```

This lets `crate_universe` read the contributor's or CI runner's existing
cargo registry config (the file under `$CARGO_HOME` or `CARGO_REGISTRIES_*`
environment variables). With isolation on, `crate_universe` runs in a
sandbox that cannot see that config and the private fetch fails.

### Step 4: Hand-written `BUILD.bazel` for the crate

crate_universe does not generate Rust BUILD files for your own code; you
write them. Use `aliases()` and `all_crate_deps(...)` from the generated
`defs.bzl`:

```python
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_test")
load("@myservice_crates//:defs.bzl", "aliases", "all_crate_deps")

rust_library(
    name = "myservice_lib",
    srcs = glob(["src/**/*.rs"], exclude = ["src/server.rs"]),
    aliases = aliases(),
    crate_features = ["default"],
    crate_name = "myservice",
    edition = "2021",
    proc_macro_deps = all_crate_deps(proc_macro = True),
    visibility = ["//visibility:public"],
    deps = all_crate_deps(normal = True),
)

rust_binary(
    name = "server",
    srcs = ["src/server.rs"],
    aliases = aliases(),
    crate_features = ["default", "jemalloc"],
    edition = "2021",
    proc_macro_deps = all_crate_deps(proc_macro = True),
    visibility = ["//visibility:public"],
    deps = [":myservice_lib"] + all_crate_deps(normal = True),
)

rust_test(
    name = "myservice_test",
    crate = ":myservice_lib",
    crate_features = ["default"],
    deps = all_crate_deps(normal_dev = True),
)
```

The `aliases()` and `all_crate_deps(...)` calls expand at analysis time to
the right set of `@myservice_crates//:foo` labels based on the platform.
Do not list crate deps by hand.

### Step 5: `cargo_build_script` for `build.rs`

If the crate has a `build.rs` (common for protobuf codegen), declare it
explicitly. crate_universe does not auto-handle your own build scripts.

```python
load("@rules_rust//cargo:defs.bzl", "cargo_build_script")

cargo_build_script(
    name = "build_script",
    srcs = ["build.rs"],
    build_script_env = {
        "PROTOC": "$(execpath @protobuf//:protoc)",
    },
    data = glob(["proto/**/*.proto"]) + [
        "@protobuf//:well_known_type_protos",
    ],
    tools = [
        "@protobuf//:protoc",
    ],
    deps = all_crate_deps(build = True),
)
```

Then add `:build_script` to the `deps` of the `rust_library` that imports
the generated code.

Critical:

- `protoc` must be in `tools`, not `data`. `tools` builds protoc for the
  exec platform (the host); `data` builds it for the target platform,
  which on cross-compile is a different arch and the binary cannot run on
  the host doing the build.
- The `$(execpath ...)` expansion in `build_script_env` is what makes
  `protoc` resolvable from inside `build.rs`.
- `data` carries the actual `.proto` files plus any well-known types
  needed at codegen time.

### Step 6: jemalloc placement

Put the `jemalloc` feature on the binary target only, never on the
library. Both `tikv-jemallocator` and `jemallocator` register a global
allocator at `extern crate` time; if a library declares the feature, every
downstream consumer (including tests) inherits it and you get linker
errors complaining about "multiple definitions of `__rdl_alloc`".

```python
rust_library(
    name = "myservice_lib",
    crate_features = ["default"],          # no jemalloc here
    # ...
)

rust_binary(
    name = "server",
    crate_features = ["default", "jemalloc"],   # only here
    # ...
)
```

In `Cargo.toml`, the feature gate stays:

```toml
[features]
default = []
jemalloc = ["dep:tikv-jemallocator"]
```

And in source, gate the global allocator behind the feature:

```rust
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
```

### Step 7: Repin and validate

After editing `Cargo.toml` or `Cargo.lock`:

```bash
CARGO_BAZEL_REPIN=1 CARGO_BAZEL_ISOLATED=false bazel sync --only=myservice_crates
```

This rewrites the lockfile and the cached crate metadata. Commit any diff
to `Cargo.lock` and `MODULE.bazel.lock`.

Then:

```bash
bazel build //myservice/...
bazel test //myservice/...
```

### Step 8: OCI image

See `bazel-oci-images` for the full pattern. Short version:

```python
load("//rules/oci:defs.bzl", "rust_oci_image")

rust_oci_image(
    name = "image",
    binary = ":server",
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

## Linker and Cross-Compile Notes

### LLD over gold

Add to `.bazelrc`:

```
build:linux --@rules_rust//:extra_rustc_flags=-Clink-arg=-fuse-ld=lld
```

The gold linker is deprecated and the default `binutils` ld is slow on
large Rust binaries. lld must be installed in the CI image (`apt install
lld`). See `bazel-monorepo-bootstrap` reference.md for the Dockerfile.

### macOS sandbox and `cargo_build_script`

```
build:macos --strategy_regexp=CargoBuildScriptRun=local
```

The default Bazel sandbox on macOS blocks the subdirectory creation that
`cargo_build_script` does internally. Running `local` strategy works
around it without exposing host filesystem to other actions.

### SIGILL on x86_64 with arm64 cross-compile

If you `register_toolchains("@zig_sdk//toolchain:linux_amd64_gnu.2.38")`
alongside the arm64 one on an x86_64 host, the host build will pick the
zig amd64 toolchain over the system gcc. zig produces SIGILL-prone
binaries on some host CPUs in this configuration. Only register the
non-host arch:

```python
register_toolchains(
    "@zig_sdk//toolchain:linux_arm64_gnu.2.38",   # cross only
)
```

The native compiler picks up the amd64 builds.

## Common Failures

| Symptom | Cause | Fix |
|---------|-------|-----|
| `registry 'foo' not found` at sync | Missing or unreferenced `cargo_config` | Add `cargo_config = "//myservice:.cargo/config.toml"` to `crate.from_cargo` and `exports_files` to the BUILD |
| `401 Unauthorized` against private registry in CI | `CARGO_BAZEL_ISOLATED` is true (default) | Set `CARGO_BAZEL_ISOLATED=false` in CI env |
| Lockfile mismatch | Cargo.lock edited without repinning | `CARGO_BAZEL_REPIN=1 bazel sync --only=<name>` |
| `multiple definitions of __rdl_alloc` | jemalloc feature on library target | Move feature to binary only |
| `build.rs` cannot find protoc | `protoc` in `data` instead of `tools` | Move to `tools = ["@protobuf//:protoc"]` |
| Subdirectory create fails on macOS | Sandbox blocking build script | Add `build:macos --strategy_regexp=CargoBuildScriptRun=local` |
| `SIGILL` running tests on Linux x86_64 | Both native and zig amd64 toolchains registered | Register only the cross toolchain |
| Slow link times | Default ld linker in use | Add `--@rules_rust//:extra_rustc_flags=-Clink-arg=-fuse-ld=lld` |

## Performance Note: protoc

`bazel build` will, by default, compile `protobuf` from source on first
run (this is roughly 2800 C++ actions). On CI it is fine because the image
caches the result. On laptops it is painful. Two options:

1. Accept the one-time cost; subsequent builds use the cache.
2. Pre-build a `protoc` binary into the CI image and reference it via
   `tools = ["@host_protoc//:protoc"]` instead of `@protobuf//:protoc`.
   Reverted in the NVCF reference repo because it broke cross-compile;
   only do this if you do not need cross-arch protoc execution.

## Additional Resources

- Full templates: [reference.md](reference.md)
- rules_rust docs: https://github.com/bazelbuild/rules_rust
- crate_universe docs: https://bazelbuild.github.io/rules_rust/crate_universe.html
- hermetic_cc_toolchain: https://github.com/uber/hermetic_cc_toolchain
