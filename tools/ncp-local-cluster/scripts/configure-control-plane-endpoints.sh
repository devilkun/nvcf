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

cluster_name="${CLUSTER_NAME:-ncp-local-compute-1}"
domain="${CONTROL_PLANE_DOMAIN:-nvcf-control-plane.test}"
http_port="${CONTROL_PLANE_HTTP_PORT:-8080}"
nats_port="${CONTROL_PLANE_NATS_PORT:-4222}"
network_name="k3d-${cluster_name}"

gateway_ip="${CONTROL_PLANE_GATEWAY_IP:-}"
if [ -z "$gateway_ip" ]; then
  gateway_ip="$(docker network inspect "$network_name" --format '{{ (index .IPAM.Config 0).Gateway }}')"
fi

if [ -z "$gateway_ip" ] || [ "$gateway_ip" = "<no value>" ]; then
  echo "ERROR: unable to determine Docker gateway IP for ${network_name}" >&2
  exit 1
fi

echo "Configuring compute aliases for ${domain} via ${gateway_ip}"
echo "  HTTP service port 8080 -> control-plane host port ${http_port}"
echo "  NATS service port 4222 -> control-plane host port ${nats_port}"

kubectl apply -f - <<YAML
apiVersion: v1
kind: Endpoints
metadata:
  name: api
  namespace: sis
subsets:
  - addresses:
      - ip: ${gateway_ip}
    ports:
      - name: http
        port: ${http_port}
        protocol: TCP
---
apiVersion: v1
kind: Endpoints
metadata:
  name: reval
  namespace: nvcf
subsets:
  - addresses:
      - ip: ${gateway_ip}
    ports:
      - name: http
        port: ${http_port}
        protocol: TCP
---
apiVersion: v1
kind: Endpoints
metadata:
  name: nats
  namespace: nats-system
subsets:
  - addresses:
      - ip: ${gateway_ip}
    ports:
      - name: client
        port: ${nats_port}
        protocol: TCP
YAML
