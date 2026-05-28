// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::common::{SelfDiscovery, base_config, bind_ephemeral, ephemeral_addr, init_crypto};
use stargate::runtime::StargateRuntime;

/// When the gRPC port is already occupied, `start()` must return an error
/// rather than silently spawning a dead gRPC task.
#[tokio::test]
async fn start_fails_when_grpc_port_is_occupied() {
    init_crypto();

    // Hold the listener open so the port stays occupied for the entire test;
    // dropping it would let concurrent tests (or stargate itself) re-bind it.
    let (grpc_addr, grpc_std) = bind_ephemeral();
    grpc_std
        .set_nonblocking(true)
        .expect("set_nonblocking on grpc blocker");
    let _grpc_blocker = tokio::net::TcpListener::from_std(grpc_std).expect("grpc blocker from_std");

    let http_addr = ephemeral_addr();

    let discovery = SelfDiscovery::new("test-occupied", grpc_addr, http_addr);
    let config = base_config("test-occupied", grpc_addr, http_addr);
    let runtime = StargateRuntime::new(config, Box::new(discovery));

    let result = runtime.start().await;
    assert!(
        result.is_err(),
        "start() must fail when gRPC port is occupied"
    );
}

/// When the model-discovery gRPC port is already occupied, `start()` must
/// return an error rather than serving ListModels on the control-plane port.
#[tokio::test]
async fn start_fails_when_model_discovery_port_is_occupied() {
    init_crypto();

    let (model_discovery_addr, model_discovery_std) = bind_ephemeral();
    model_discovery_std
        .set_nonblocking(true)
        .expect("set_nonblocking on model-discovery blocker");
    let _model_discovery_blocker = tokio::net::TcpListener::from_std(model_discovery_std)
        .expect("model-discovery blocker from_std");

    let grpc_addr = ephemeral_addr();
    let http_addr = ephemeral_addr();

    let discovery = SelfDiscovery::new("test-model-discovery-occupied", grpc_addr, http_addr);
    let mut config = base_config("test-model-discovery-occupied", grpc_addr, http_addr);
    config.model_discovery_listen_addr = model_discovery_addr;
    let runtime = StargateRuntime::new(config, Box::new(discovery));

    let result = runtime.start().await;
    assert!(
        result.is_err(),
        "start() must fail when model-discovery gRPC port is occupied"
    );
}
