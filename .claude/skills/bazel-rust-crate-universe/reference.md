# Bazel Rust and crate_universe: Templates

## `MODULE.bazel` Rust section

```python
bazel_dep(name = "rules_rust", version = "0.69.0")
bazel_dep(name = "hermetic_cc_toolchain", version = "4.1.0")
bazel_dep(name = "protobuf", version = "31.1")

rust = use_extension("@rules_rust//rust:extensions.bzl", "rust")
rust.toolchain(
    edition = "2021",
    versions = ["1.91.1"],
)
use_repo(rust, "rust_toolchains")
register_toolchains("@rust_toolchains//:all")

# C++ cross-compile (needed for multi-arch builds and proto codegen).
zig = use_extension("@hermetic_cc_toolchain//toolchain:ext.bzl", "toolchains")
use_repo(zig, "zig_sdk")

# Register only the cross arch; native uses the host gcc.
register_toolchains(
    "@zig_sdk//toolchain:linux_arm64_gnu.2.38",
)

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

## `.bazelrc` Rust block

```
# Use LLD instead of the deprecated gold linker (Linux only).
build:linux --@rules_rust//:extra_rustc_flags=-Clink-arg=-fuse-ld=lld

# macOS sandbox blocks cargo_build_script subdirectory creation.
build:macos --strategy_regexp=CargoBuildScriptRun=local
```

## Service top-level `BUILD.bazel`

```python
exports_files([".cargo/config.toml"])

# Convenience alias so `bazel build //myservice` works.
alias(
    name = "myservice",
    actual = "//myservice/crates/server:server",
    visibility = ["//visibility:public"],
)
```

## `crates/server/BUILD.bazel` (full pattern)

```python
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_test")
load("@rules_rust//cargo:defs.bzl", "cargo_build_script")
load("@myservice_crates//:defs.bzl", "aliases", "all_crate_deps")
load("//rules/oci:defs.bzl", "rust_oci_image")

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

rust_library(
    name = "myservice_lib",
    srcs = glob(["src/**/*.rs"], exclude = ["src/server.rs"]),
    aliases = aliases(),
    crate_features = ["default", "profiling"],
    crate_name = "myservice",
    edition = "2021",
    proc_macro_deps = all_crate_deps(proc_macro = True),
    visibility = ["//visibility:public"],
    deps = [":build_script"] + all_crate_deps(normal = True),
)

rust_binary(
    name = "server",
    srcs = ["src/server.rs"],
    aliases = aliases(),
    crate_features = ["default", "jemalloc", "profiling"],
    edition = "2021",
    proc_macro_deps = all_crate_deps(proc_macro = True),
    visibility = ["//visibility:public"],
    deps = [
        ":myservice_lib",
    ] + all_crate_deps(normal = True),
)

rust_test(
    name = "myservice_test",
    crate = ":myservice_lib",
    crate_features = ["default", "profiling"],
    deps = all_crate_deps(normal_dev = True),
)

rust_oci_image(
    name = "image",
    binary = ":server",
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

Note: `crate_features` differs between library/test (no `jemalloc`) and
binary (`jemalloc` included).

## `.cargo/config.toml` for a private registry

```toml
[registries]
nvcf-internal = { index = "sparse+https://urm.nvidia.com/artifactory/api/cargo/sw-gpu-nvcf-cargo-local/" }

[source.crates-io]
replace-with = "nvcf-internal"

[net]
git-fetch-with-cli = true
```

`git-fetch-with-cli = true` is needed when the registry has any git-based
crates and you want to share git auth with the system git client.

## `Cargo.toml` jemalloc gate

```toml
[features]
default = []
profiling = ["dep:pprof"]
jemalloc = ["dep:tikv-jemallocator"]

[dependencies]
tikv-jemallocator = { version = "0.6", optional = true }
pprof = { version = "0.14", optional = true }
```

## `src/server.rs` jemalloc registration

```rust
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    // ...
}
```

## `build.rs` for protobuf

Works with both Cargo and Bazel:

```rust
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = vec![
        "proto/myservice/v1/api.proto",
        "proto/myservice/v1/types.proto",
    ];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(&proto_files, &["proto/"])?;

    Ok(())
}
```

`PROTOC` is read by `prost-build` / `tonic-build` automatically. Bazel
provides it via `build_script_env` in the `cargo_build_script` rule.

## Repinning workflow

```bash
# After editing Cargo.toml or Cargo.lock:
CARGO_BAZEL_REPIN=1 CARGO_BAZEL_ISOLATED=false bazel sync --only=myservice_crates
git diff Cargo.lock MODULE.bazel.lock        # review
git add Cargo.lock MODULE.bazel.lock
git commit -m "chore(bazel): repin myservice crates"
```

`CARGO_BAZEL_ISOLATED=false` is required for any sync that fetches from a
private registry. Set it locally for one-shot syncs:

```bash
CARGO_BAZEL_ISOLATED=false bazel sync --only=myservice_crates
```

In CI, set it as a job-level env variable.

## CI snippet

```yaml
build-and-test:
  stage: build
  variables:
    CARGO_BAZEL_ISOLATED: "false"
  script:
    - bazel build //myservice
    - bazel test //myservice/...
```

## Common Cargo features that need attention

| Feature | Notes |
|---------|-------|
| `jemalloc` / `tikv-jemallocator` | Binary only; library will linker-conflict. |
| `default-tls` (reqwest, hyper) | Pick `rustls-tls` for hermetic builds; `native-tls` pulls in OpenSSL. |
| `serde_derive` | Use `proc_macro_deps`, not `deps`, in the BUILD file. crate_universe sorts this for you via `all_crate_deps(proc_macro = True)`. |
| Async runtimes | `tokio` is fine; pin a single runtime version across the workspace via `Cargo.toml` workspace.dependencies to avoid double-link errors. |
