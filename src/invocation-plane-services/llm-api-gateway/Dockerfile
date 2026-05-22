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
FROM golang:1.25-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOFLAGS=-mod=mod \
    go build -o /out/llm-api-gateway ./cmd/llm-api-gateway

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOFLAGS=-mod=mod \
    go build -o /out/llm-api-gateway-rate-limit-sync-worker ./cmd/llm-api-gateway-rate-limit-sync-worker

FROM debian:bookworm-slim

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin app

USER 1000
WORKDIR /home/app

COPY --from=builder /out/llm-api-gateway ./llm-api-gateway
COPY --from=builder /out/llm-api-gateway-rate-limit-sync-worker ./llm-api-gateway-rate-limit-sync-worker

EXPOSE 8080 9464

ENTRYPOINT ["./llm-api-gateway"]
