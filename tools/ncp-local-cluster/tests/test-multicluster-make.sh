#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local label="$3"

  if [ "$actual" != "$expected" ]; then
    fail "${label}: expected '${expected}', got '${actual}'"
  fi
}

run_make() {
  make -s -C "$ROOT_DIR" "$@"
}

default_clusters="$(run_make print-compute-clusters)"
assert_eq "ncp-local-compute-1" "$default_clusters" "default compute cluster"

three_clusters="$(run_make print-compute-clusters COMPUTE_CLUSTER_COUNT=3)"
assert_eq "ncp-local-compute-1 ncp-local-compute-2 ncp-local-compute-3" "$three_clusters" "count-derived compute clusters"

explicit_clusters="$(run_make print-compute-clusters COMPUTE_CLUSTER_COUNT=3 COMPUTE_CLUSTERS="ncp-east ncp-west")"
assert_eq "ncp-east ncp-west" "$explicit_clusters" "explicit compute clusters override count"

for invalid_count in 0 abc ""; do
  tmp_output="$(mktemp)"
  if run_make print-compute-clusters COMPUTE_CLUSTER_COUNT="$invalid_count" >"$tmp_output" 2>&1; then
    cat "$tmp_output" >&2
    rm -f "$tmp_output"
    fail "invalid count '${invalid_count}' should fail"
  fi
  if ! grep -q "COMPUTE_CLUSTER_COUNT must be a positive integer" "$tmp_output"; then
    cat "$tmp_output" >&2
    rm -f "$tmp_output"
    fail "invalid count '${invalid_count}' did not print validation message"
  fi
  rm -f "$tmp_output"
done

help_output="$(run_make help)"
for target in \
  build-and-deploy-control-plane-cluster \
  build-and-deploy-compute-plane-cluster \
  build-and-deploy-multicluster \
  configure-compute-control-plane-dns \
  deploy-compute-control-plane-endpoints \
  deploy-control-plane-endpoints \
  destroy-control-plane \
  destroy-compute-plane \
  destroy-multicluster; do
  if ! grep -q "$target" <<<"$help_output"; then
    fail "make help missing target '${target}'"
  fi
done

if ! grep -q '\${CONTROL_PLANE_NATS_PORT}:4222' "$ROOT_DIR/k3d-config-control-plane.yaml"; then
  fail "control-plane k3d config must expose CONTROL_PLANE_NATS_PORT to Gateway port 4222"
fi

if ! grep -q 'name: nats' "$ROOT_DIR/apps/envoy-gateway/gateway.yaml"; then
  fail "control-plane Gateway must define a nats TCP listener"
fi

if grep -R -q 'type: ExternalName' "$ROOT_DIR/apps/compute-control-plane-endpoints"; then
  fail "compute control-plane endpoint aliases must support port translation for custom control-plane host ports"
fi

if ! grep -q 'CONTROL_PLANE_HTTP_PORT' "$ROOT_DIR/scripts/configure-control-plane-endpoints.sh"; then
  fail "compute control-plane endpoint configuration must honor CONTROL_PLANE_HTTP_PORT"
fi

for unsupported_alias in 'api.${domain}' 'api-keys.${domain}' 'invocation.${domain}'; do
  if grep -q "$unsupported_alias" "$ROOT_DIR/scripts/configure-control-plane-dns.sh"; then
    fail "compute DNS must not advertise unsupported control-plane alias '${unsupported_alias}'"
  fi
done

custom_domain="control-plane.dev.test"

rendered_routes="$(CONTROL_PLANE_DOMAIN="$custom_domain" "$ROOT_DIR/scripts/render-control-plane-endpoints.sh")"
if ! grep -q "sis.${custom_domain}" <<<"$rendered_routes"; then
  fail "control-plane SIS route must use CONTROL_PLANE_DOMAIN"
fi
if ! grep -q "reval.${custom_domain}" <<<"$rendered_routes"; then
  fail "control-plane ReVal route must use CONTROL_PLANE_DOMAIN"
fi
if grep -q "sis.nvcf-control-plane.test" <<<"$rendered_routes"; then
  fail "custom control-plane routes must not keep the default domain"
fi

dns_yaml="$(CONTROL_PLANE_DOMAIN="$custom_domain" CONTROL_PLANE_GATEWAY_IP=172.18.0.1 CLUSTER_NAME=ncp-local-compute-1 "$ROOT_DIR/scripts/configure-control-plane-dns.sh" --dry-run)"
for hostname in "sis.${custom_domain}" "reval.${custom_domain}" "nats.${custom_domain}"; do
  if ! grep -q "$hostname" <<<"$dns_yaml"; then
    fail "compute DNS dry-run missing '${hostname}'"
  fi
done
for unsupported_alias in "api.${custom_domain}" "api-keys.${custom_domain}" "invocation.${custom_domain}"; do
  if grep -q "$unsupported_alias" <<<"$dns_yaml"; then
    fail "compute DNS must not advertise unsupported alias '${unsupported_alias}'"
  fi
done

echo "PASS: multicluster Makefile dry tests"
