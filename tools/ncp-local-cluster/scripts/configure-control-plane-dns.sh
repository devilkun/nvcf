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

dry_run=false
if [ "${1:-}" = "--dry-run" ]; then
  dry_run=true
fi

cluster_name="${CLUSTER_NAME:-ncp-local-compute-1}"
domain="${CONTROL_PLANE_DOMAIN:-nvcf-control-plane.test}"
network_name="k3d-${cluster_name}"

gateway_ip="${CONTROL_PLANE_GATEWAY_IP:-}"
if [ -z "$gateway_ip" ]; then
  gateway_ip="$(docker network inspect "$network_name" --format '{{ (index .IPAM.Config 0).Gateway }}')"
fi

if [ -z "$gateway_ip" ] || [ "$gateway_ip" = "<no value>" ]; then
  echo "ERROR: unable to determine Docker gateway IP for ${network_name}" >&2
  exit 1
fi

echo "Configuring CoreDNS ${domain} zone to ${gateway_ip}"

yaml="$(cat <<YAML
apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns-custom
  namespace: kube-system
data:
  nvcf-control-plane.server: |
    ${domain}:53 {
        errors
        cache 30
        hosts {
            ${gateway_ip} sis.${domain}
            ${gateway_ip} reval.${domain}
            ${gateway_ip} nats.${domain}
            fallthrough
        }
    }
YAML
)"

if [ "$dry_run" = true ]; then
  printf '%s\n' "$yaml"
  exit 0
fi

printf '%s\n' "$yaml" | kubectl apply -f -

kubectl -n kube-system rollout restart deployment/coredns >/dev/null
kubectl -n kube-system rollout status deployment/coredns --timeout=120s
