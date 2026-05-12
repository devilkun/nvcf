# Bazel OCI Images: Templates

Full source for the `rules/oci/` macro layer used to build multi-arch
container images.

## `MODULE.bazel` OCI section

```python
bazel_dep(name = "platforms", version = "1.0.0")
bazel_dep(name = "rules_oci", version = "2.2.7")
bazel_dep(name = "rules_pkg", version = "1.2.0")
bazel_dep(name = "aspect_bazel_lib", version = "2.19.3")
bazel_dep(name = "hermetic_cc_toolchain", version = "4.1.0")

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

## `rules/oci/BUILD.bazel`

```python
# OCI image helpers.
# A cacerts target can be added here later if using scratch/distroless bases.
```

(Comment-only file; Bazel still needs it to recognize the package.)

## `rules/oci/transition.bzl`

```python
"Multi-arch transition rule for OCI images."

def _multiarch_transition(settings, attr):
    return [
        {"//command_line_option:platforms": str(platform)}
        for platform in attr.platforms
    ]

multiarch_transition = transition(
    implementation = _multiarch_transition,
    inputs = [],
    outputs = ["//command_line_option:platforms"],
)

def _multi_arch_impl(ctx):
    return DefaultInfo(files = depset(ctx.files.image))

multi_arch = rule(
    implementation = _multi_arch_impl,
    attrs = {
        "image": attr.label(cfg = multiarch_transition),
        "platforms": attr.label_list(),
        "_allowlist_function_transition": attr.label(
            default = "@bazel_tools//tools/allowlists/function_transition_allowlist",
        ),
    },
)
```

## `rules/oci/defs.bzl`

```python
"OCI image rules for packaging binaries into multi-arch containers."

load("//rules/oci/private:go.bzl", _go_oci_image = "go_oci_image")
load("//rules/oci/private:java.bzl", _java_oci_image = "java_oci_image")
load("//rules/oci/private:rust.bzl", _rust_oci_image = "rust_oci_image")

go_oci_image = _go_oci_image
java_oci_image = _java_oci_image
rust_oci_image = _rust_oci_image
```

## `rules/oci/private/common.bzl`

```python
"Shared helpers for OCI image rules."

load("@aspect_bazel_lib//lib:expand_template.bzl", "expand_template")
load("@aspect_bazel_lib//lib:transitions.bzl", "platform_transition_filegroup")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_image_index", "oci_load", "oci_push")
load("//rules/oci:transition.bzl", "multi_arch")

DEFAULT_BASE = "@ubuntu_noble"
DEFAULT_JAVA_BASE = "@eclipse_temurin_21_jre"
DEFAULT_REGISTRY = "myregistry.example.com:5005/myorg/myrepo"
DEFAULT_PLATFORMS = [
    "//platforms:linux_arm64",
    "//platforms:linux_x86_64",
]

COMMON_LAYERS = []

def create_oci_image(
        name,
        tars,
        base,
        entrypoint,
        visibility,
        registry = None,
        tags = None):
    """Creates OCI image targets with platform transitions and tarball output.

    Generates:
      - {name}: Platform-transitioned OCI image (for local builds)
      - {name}_index: Multi-arch image index (amd64 + arm64)
      - {name}_load: Local docker load target
      - {name}.tar: Tarball filegroup
      - {name}_push: Push to registry (if registry is set)
    """
    all_tags = ["manual"] + (tags or [])

    pre_transitioned = name + "_pre_transitioned"
    oci_image(
        name = pre_transitioned,
        base = base,
        tars = tars + COMMON_LAYERS,
        entrypoint = entrypoint,
        visibility = ["//visibility:private"],
        tags = all_tags,
    )

    platform_transition_filegroup(
        name = name,
        srcs = [pre_transitioned],
        target_platform = select({
            "@platforms//cpu:arm64": "//platforms:linux_arm64",
            "@platforms//cpu:x86_64": "//platforms:linux_x86_64",
        }),
        visibility = visibility,
        tags = all_tags,
    )

    multi_arch_name = name + "_multi_arch"
    multi_arch(
        name = multi_arch_name,
        image = pre_transitioned,
        platforms = DEFAULT_PLATFORMS,
        visibility = ["//visibility:private"],
        tags = all_tags,
    )

    oci_image_index(
        name = name + "_index",
        images = [multi_arch_name],
        visibility = visibility,
        tags = all_tags,
    )

    load_name = name + "_load"
    oci_load(
        name = load_name,
        image = name,
        repo_tags = [native.package_name() + ":latest"],
        visibility = visibility,
        tags = all_tags,
    )

    native.filegroup(
        name = name + ".tar",
        srcs = [load_name],
        output_group = "tarball",
        visibility = visibility,
        tags = all_tags,
    )

    if registry:
        stamped_tags = name + "_stamped_tags"
        expand_template(
            name = stamped_tags,
            out = name + "_tags.txt",
            stamp_substitutions = {
                "{VERSION}": "{{STABLE_VERSION}}",
                "{OCI_TAG}": "{{STABLE_OCI_TAG}}",
                "{COMMIT}": "{{STABLE_GIT_COMMIT}}",
            },
            template = [
                "latest",
                "{VERSION}",
                "{OCI_TAG}",
                "{COMMIT}",
            ],
            visibility = ["//visibility:private"],
        )

        oci_push(
            name = name + "_push",
            image = name + "_index",
            remote_tags = stamped_tags,
            repository = registry,
            visibility = visibility,
            tags = all_tags,
        )
```

## `rules/oci/private/go.bzl`

```python
"OCI image rules for Go binaries."

load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//rules/oci/private:common.bzl", "DEFAULT_BASE", "DEFAULT_REGISTRY", "create_oci_image")

def _go_oci_image_impl(name, visibility, binary, base, entrypoint, registry, tags):
    layer_name = name + "_layer"
    pkg_tar(
        name = layer_name,
        srcs = [binary],
        visibility = ["//visibility:private"],
    )

    entry = entrypoint
    if not entry:
        entry = ["/" + native.package_relative_label(binary).name]

    create_oci_image(
        name = name,
        tars = [layer_name],
        base = base,
        entrypoint = entry,
        visibility = visibility,
        registry = registry,
        tags = tags,
    )

go_oci_image = macro(
    doc = "Packages a go_binary into a multi-arch OCI image with Linux platform transition.",
    implementation = _go_oci_image_impl,
    attrs = {
        "binary": attr.label(
            doc = "The go_binary target to package.",
            mandatory = True,
            configurable = False,
        ),
        "base": attr.label(
            doc = "Base OCI image.",
            default = DEFAULT_BASE,
            configurable = False,
        ),
        "entrypoint": attr.string_list(
            doc = "Container entrypoint. Defaults to /{binary_name}.",
            configurable = False,
        ),
        "registry": attr.string(
            doc = "Registry to push to. If not set, push target is not created.",
            configurable = False,
        ),
        "tags": attr.string_list(
            doc = "Tags for generated targets. 'manual' is always added.",
            configurable = False,
        ),
    },
)
```

## `rules/oci/private/rust.bzl`

```python
"OCI image rules for Rust binaries."

load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//rules/oci/private:common.bzl", "DEFAULT_BASE", "DEFAULT_REGISTRY", "create_oci_image")

def _rust_oci_image_impl(name, visibility, binary, base, entrypoint, registry, tags):
    layer_name = name + "_layer"
    pkg_tar(
        name = layer_name,
        srcs = [binary],
        visibility = ["//visibility:private"],
    )

    entry = entrypoint
    if not entry:
        entry = ["/" + native.package_relative_label(binary).name]

    create_oci_image(
        name = name,
        tars = [layer_name],
        base = base,
        entrypoint = entry,
        visibility = visibility,
        registry = registry,
        tags = tags,
    )

rust_oci_image = macro(
    doc = "Packages a rust_binary into a multi-arch OCI image with Linux platform transition.",
    implementation = _rust_oci_image_impl,
    attrs = {
        "binary": attr.label(mandatory = True, configurable = False),
        "base": attr.label(default = DEFAULT_BASE, configurable = False),
        "entrypoint": attr.string_list(configurable = False),
        "registry": attr.string(configurable = False),
        "tags": attr.string_list(configurable = False),
    },
)
```

## `rules/oci/private/java.bzl`

```python
"OCI image rules for Java applications (Spring Boot deploy jars)."

load("@rules_pkg//pkg:tar.bzl", "pkg_tar")
load("//rules/oci/private:common.bzl", "DEFAULT_JAVA_BASE", "DEFAULT_REGISTRY", "create_oci_image")

def _java_oci_image_impl(name, visibility, binary, base, entrypoint, registry, jvm_flags, tags):
    bin_name = native.package_relative_label(binary).name
    deploy_jar = str(binary) + "_deploy.jar"

    layer_name = name + "_layer"
    pkg_tar(
        name = layer_name,
        srcs = [deploy_jar],
        package_dir = "/app",
        visibility = ["//visibility:private"],
    )

    entry = entrypoint
    if not entry:
        entry = ["java"] + (jvm_flags or []) + ["-jar", "/app/" + bin_name + "_deploy.jar"]

    create_oci_image(
        name = name,
        tars = [layer_name],
        base = base,
        entrypoint = entry,
        visibility = visibility,
        registry = registry,
        tags = tags,
    )

java_oci_image = macro(
    doc = "Packages a java_binary deploy jar into a multi-arch OCI image with JRE base.",
    implementation = _java_oci_image_impl,
    attrs = {
        "binary": attr.label(
            doc = "The java_binary target. Its _deploy.jar output is packaged.",
            mandatory = True,
            configurable = False,
        ),
        "base": attr.label(
            doc = "Base OCI image with JRE. Defaults to eclipse-temurin:21-jre-noble.",
            default = DEFAULT_JAVA_BASE,
            configurable = False,
        ),
        "entrypoint": attr.string_list(
            doc = "Container entrypoint. Defaults to java -jar /app/<name>_deploy.jar.",
            configurable = False,
        ),
        "jvm_flags": attr.string_list(
            doc = "JVM flags before -jar. Only used when entrypoint is not set.",
            configurable = False,
        ),
        "registry": attr.string(configurable = False),
        "tags": attr.string_list(configurable = False),
    },
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

VERSION="0.0.1"

echo "STABLE_VERSION ${VERSION}"
echo "STABLE_GIT_COMMIT ${COMMIT}${DIRTY}"
echo "STABLE_OCI_TAG ${VERSION}-${COMMIT}${DIRTY}"
```

Wired in `.bazelrc`:

```
build --workspace_status_command=./tools/workspace_status.sh
build --nostamp
build:ci --stamp
```

## CI snippet for image push (GitLab)

```yaml
build-and-test:
  stage: build
  image: myregistry.example.com:5005/myorg/myrepo/ci:1
  script:
    - bazel build //myservice
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

For multiple image targets in one service, run `image_push` for each:

```yaml
    - bazel run --stamp //myservice/cmd/init:image_push
    - bazel run --stamp //myservice/cmd/task:image_push
    - bazel run --stamp //myservice/cmd/utils:image_push
```

## Sample service usage

```python
# Go
load("//rules/oci:defs.bzl", "go_oci_image")
go_oci_image(
    name = "image",
    binary = ":myservice",
    registry = "myregistry.example.com:5005/myorg/myservice",
)

# Rust
load("//rules/oci:defs.bzl", "rust_oci_image")
rust_oci_image(
    name = "image",
    binary = ":server",
    registry = "myregistry.example.com:5005/myorg/myservice",
)

# Java (defaults to java -jar /app/<name>_deploy.jar)
load("//rules/oci:defs.bzl", "java_oci_image")
java_oci_image(
    name = "image",
    binary = ":myservice",
    jvm_flags = ["-Xmx2g", "-XX:MaxRAMPercentage=75"],
    registry = "myregistry.example.com:5005/myorg/myservice",
)
```

## Targets generated

For a `<lang>_oci_image(name = "image", ...)` call, the macro generates:

| Target | Purpose |
|--------|---------|
| `:image` | Single-arch image for the host platform; used by `image_load` |
| `:image_index` | Multi-arch (`linux/amd64` + `linux/arm64`) manifest list |
| `:image_load` | `bazel run` to load `:image` into the local docker daemon |
| `:image.tar` | `filegroup` with the load tarball (for `docker load < tarball`) |
| `:image_push` | `bazel run` to push `:image_index` to the registry |

All except `:image_load` and `:image.tar` are tagged `manual` so they do
not build under `bazel build //...`. Build them explicitly:

```bash
bazel build //myservice:image
bazel build //myservice:image_index
bazel run //myservice:image_load
bazel run --stamp //myservice:image_push
```
