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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::common::{
    TokenMapAuthenticator, init_crypto, make_stargate_runtime,
    make_stargate_runtime_with_auth_and_model_discovery,
    make_stargate_runtime_with_model_discovery, make_stargate_runtime_with_shared_discovery,
    make_stargate_runtime_with_shared_discovery_and_remote_watch_urls,
    make_stargate_runtime_with_watch_intervals, start_dummy_inst, wait_for_all_probes_routed_to,
    wait_for_routing, wait_for_routing_with_rk, with_proxy_headers,
};
use pylon_lib::{
    AuthTokenProvider, BringupConfig, CurrentModelStats, InferenceServerRegistrationClient,
    InferenceServerRegistrationConfig, OutputTokenParserFactory,
};
use stargate_proto::pb::stargate_control_plane_client::StargateControlPlaneClient;
use stargate_proto::pb::stargate_model_discovery_client::StargateModelDiscoveryClient;
use stargate_proto::pb::{InferenceServerStatus, ListModelsRequest, WatchStargatesRequest};
use tonic::transport::Channel;

#[tokio::test]
async fn stats_update_via_channel_propagates() {
    init_crypto();

    let (grpc_addr, http_addr, runtime) = make_stargate_runtime("test-sg-stats");
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr_a, quic_url_a, _tunnel_a) = start_dummy_inst("stats-model").await;
    let (inst_addr_b, quic_url_b, _tunnel_b) = start_dummy_inst("stats-model").await;

    let mut reg_a = InferenceServerRegistrationClient::default();
    let channels_a = reg_a
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "stats-inst-a".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_a,
                upstream_http_base_url: Some(format!("http://{inst_addr_a}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
                // Skip calibration so both backends advertise Active immediately;
                // this test only exercises stats propagation, not bringup.
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                output_token_parser_factory: OutputTokenParserFactory::vllm(),
                request_observation_tx: None,
                request_quality_monitor: pylon_lib::RequestQualityMonitorConfig::default(),
                metrics: None,
                retry: pylon_lib::PylonRetryConfig::default(),
                queue_mismatch_retry: pylon_lib::PylonQueueMismatchRetryConfig::default(),
                queue_tracker: pylon_lib::QueueAdmissionTracker::default(),
                auth_token_provider: None,
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["stats-model".to_string()],
        )
        .expect("registration failed");

    let mut reg_b = InferenceServerRegistrationClient::default();
    let channels_b = reg_b
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "stats-inst-b".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_b,
                upstream_http_base_url: Some(format!("http://{inst_addr_b}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
                // Skip calibration so both backends advertise Active immediately;
                // this test only exercises stats propagation, not bringup.
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                output_token_parser_factory: OutputTokenParserFactory::vllm(),
                request_observation_tx: None,
                request_quality_monitor: pylon_lib::RequestQualityMonitorConfig::default(),
                metrics: None,
                retry: pylon_lib::PylonRetryConfig::default(),
                queue_mismatch_retry: pylon_lib::PylonQueueMismatchRetryConfig::default(),
                queue_tracker: pylon_lib::QueueAdmissionTracker::default(),
                auth_token_provider: None,
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["stats-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing(http_addr, "stats-model", Duration::from_secs(5)).await;

    // Give instance A more pending prompt work so p2c prefers B.
    channels_a
        .model_stats
        .send_async((
            "stats-model".to_string(),
            CurrentModelStats {
                output_tps: 50.0,
                last_mean_input_tps: 1000.0,
                max_output_tps: 100.0,
                queue_size: 0,
                queued_input_size: 1_000,
                kv_cache_capacity_tokens: 0,
                kv_cache_used_tokens: 0,
                kv_cache_free_tokens: 0,
                ..CurrentModelStats::default()
            },
        ))
        .await
        .expect("send stats failed");

    channels_b
        .model_stats
        .send_async((
            "stats-model".to_string(),
            CurrentModelStats {
                output_tps: 50.0,
                last_mean_input_tps: 1000.0,
                max_output_tps: 100.0,
                queue_size: 0,
                queued_input_size: 0,
                kv_cache_capacity_tokens: 0,
                kv_cache_used_tokens: 0,
                kv_cache_free_tokens: 0,
                ..CurrentModelStats::default()
            },
        ))
        .await
        .expect("send stats for B failed");

    wait_for_all_probes_routed_to(
        http_addr,
        "stats-model",
        "req-stats-wait",
        "stats-inst-b",
        20,
        Duration::from_secs(15),
    )
    .await;

    reg_a.stop();
    reg_b.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn watch_stargates_returns_self() {
    init_crypto();

    let (grpc_addr, _http_addr, runtime) = make_stargate_runtime("test-sg-watch");
    let handle = runtime.start().await.expect("stargate failed to start");

    let mut client = connect_control_plane(grpc_addr).await;

    let resp = client
        .watch_stargates(WatchStargatesRequest {})
        .await
        .expect("WatchStargates RPC failed");
    let mut stream = resp.into_inner();

    let msg = stream
        .message()
        .await
        .expect("stream error")
        .expect("stream ended without a message");

    let addrs: Vec<&str> = msg
        .stargates
        .iter()
        .map(|s| s.advertise_addr.as_str())
        .collect();
    let expected = grpc_addr.to_string();
    assert!(
        addrs.contains(&expected.as_str()),
        "WatchStargates should contain the stargate's own advertise_addr ({expected}), got: {addrs:?}"
    );

    // Close gRPC streams before shutdown so tonic's graceful shutdown
    // doesn't block waiting for in-flight RPCs to finish.
    drop(stream);
    drop(client);

    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn watch_stargates_returns_remote_watch_urls_without_remote_registration_targets() {
    init_crypto();

    let peers = Arc::new(Mutex::new(Vec::new()));
    let (grpc_addr, _http_addr, runtime) =
        make_stargate_runtime_with_shared_discovery_and_remote_watch_urls(
            "test-sg-watch-remote",
            peers,
            vec![
                " remote-b:50071 ".to_string(),
                "remote-a:50071".to_string(),
                "remote-b:50071".to_string(),
                String::new(),
            ],
        );
    let handle = runtime.start().await.expect("stargate failed to start");

    let mut client = connect_control_plane(grpc_addr).await;
    let resp = client
        .watch_stargates(WatchStargatesRequest {})
        .await
        .expect("WatchStargates RPC failed");
    let mut stream = resp.into_inner();

    let msg = stream
        .message()
        .await
        .expect("stream error")
        .expect("stream ended without a message");

    assert_eq!(msg.stargates.len(), 1);
    assert_eq!(msg.stargates[0].stargate_id, "test-sg-watch-remote");
    assert_eq!(
        msg.watch_stargate_urls,
        vec!["remote-a:50071", "remote-b:50071"]
    );

    drop(stream);
    drop(client);
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn watch_stargates_first_message_uses_discovery_snapshot_not_self_only_initial() {
    init_crypto();

    let peers = Arc::new(Mutex::new(Vec::new()));
    let (grpc_addr_1, _http_addr_1, runtime_1) =
        make_stargate_runtime_with_shared_discovery("test-sg-watch-full-1", peers.clone());
    let (_grpc_addr_2, _http_addr_2, runtime_2) =
        make_stargate_runtime_with_shared_discovery("test-sg-watch-full-2", peers);
    let handle_1 = runtime_1.start().await.expect("stargate 1 failed");
    let handle_2 = runtime_2.start().await.expect("stargate 2 failed");

    let endpoint = format!("http://{grpc_addr_1}");
    let channel = Channel::from_shared(endpoint)
        .expect("invalid endpoint")
        .connect()
        .await
        .expect("failed to connect to stargate gRPC");
    let mut client = StargateControlPlaneClient::new(channel);

    let resp = client
        .watch_stargates(WatchStargatesRequest {})
        .await
        .expect("WatchStargates RPC failed");
    let mut stream = resp.into_inner();

    let msg = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("timed out waiting for WatchStargates")
        .expect("stream error")
        .expect("stream ended without a message");

    let mut ids: Vec<&str> = msg
        .stargates
        .iter()
        .map(|s| s.stargate_id.as_str())
        .collect();
    ids.sort();
    assert_eq!(
        ids,
        vec!["test-sg-watch-full-1", "test-sg-watch-full-2"],
        "first WatchStargates message should come from discovery, not self-only initial state"
    );

    drop(stream);
    drop(client);
    handle_1.begin_shutdown();
    handle_2.begin_shutdown();
    handle_1.wait_for_shutdown(Duration::from_secs(5)).await;
    handle_2.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn watch_stargates_emits_heartbeat_snapshots_without_membership_change() {
    init_crypto();

    let (grpc_addr, _http_addr, runtime) = make_stargate_runtime_with_watch_intervals(
        "test-sg-watch-heartbeat",
        Duration::from_millis(50),
        Duration::from_millis(100),
    );
    let handle = runtime.start().await.expect("stargate failed to start");

    let mut client = connect_control_plane(grpc_addr).await;
    let resp = client
        .watch_stargates(WatchStargatesRequest {})
        .await
        .expect("WatchStargates RPC failed");
    let mut stream = resp.into_inner();

    let first = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("timed out waiting for initial WatchStargates")
        .expect("stream error")
        .expect("stream ended without an initial message");
    let second = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .expect("timed out waiting for heartbeat WatchStargates")
        .expect("stream error")
        .expect("stream ended without a heartbeat message");

    assert_eq!(
        first, second,
        "heartbeat should republish the same snapshot when discovery is unchanged"
    );

    drop(stream);
    drop(client);
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

async fn connect_control_plane(
    grpc_addr: std::net::SocketAddr,
) -> StargateControlPlaneClient<Channel> {
    let endpoint = Channel::from_shared(format!("http://{grpc_addr}")).expect("invalid endpoint");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return StargateControlPlaneClient::new(channel),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("failed to connect to stargate gRPC after retries: {error:?}");
                }
            }
        }

        interval.tick().await;
    }
}

async fn connect_model_discovery(
    model_discovery_addr: std::net::SocketAddr,
) -> StargateModelDiscoveryClient<Channel> {
    let endpoint =
        Channel::from_shared(format!("http://{model_discovery_addr}")).expect("invalid endpoint");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        match endpoint.clone().connect().await {
            Ok(channel) => return StargateModelDiscoveryClient::new(channel),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("failed to connect to model discovery gRPC after retries: {error:?}");
                }
            }
        }

        interval.tick().await;
    }
}

async fn wait_for_list_models(
    client: &mut StargateModelDiscoveryClient<Channel>,
    routing_key: Option<&str>,
    model_ids: &[&str],
    expected_ids: &[&str],
    timeout: Duration,
) -> Vec<String> {
    let mut expected_ids = expected_ids.to_vec();
    expected_ids.sort_unstable();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let response = match client
            .list_models(ListModelsRequest {
                routing_key: routing_key.map(ToOwned::to_owned),
                model_ids: model_ids.iter().map(|id| (*id).to_string()).collect(),
            })
            .await
        {
            Ok(response) => response.into_inner(),
            Err(error) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!(
                        "ListModels for filters {model_ids:?} failed before matching {expected_ids:?} within {}s: {error:?}",
                        timeout.as_secs(),
                    );
                }
                interval.tick().await;
                continue;
            }
        };

        let mut actual_ids = response
            .model_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        actual_ids.sort_unstable();
        if actual_ids == expected_ids {
            return response.model_ids;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "ListModels for filters {model_ids:?} returned ids {actual_ids:?}; expected {expected_ids:?} within {}s",
                timeout.as_secs(),
            );
        }
        interval.tick().await;
    }
}

#[tokio::test]
async fn list_models_empty_when_no_models_are_routable() {
    init_crypto();

    let (grpc_addr, model_discovery_addr, _http_addr, runtime) =
        make_stargate_runtime_with_model_discovery("test-sg-list-empty");
    let handle = runtime.start().await.expect("stargate failed to start");
    let mut wrong_port_client = connect_model_discovery(grpc_addr).await;
    let mut client = connect_model_discovery(model_discovery_addr).await;

    let control_plane_list_models = wrong_port_client
        .list_models(ListModelsRequest {
            routing_key: None,
            model_ids: vec![],
        })
        .await
        .expect_err("control-plane port should not serve ListModels");
    assert_eq!(
        control_plane_list_models.code(),
        tonic::Code::Unimplemented,
        "ListModels must only be served on the model-discovery port"
    );

    let response = client
        .list_models(ListModelsRequest {
            routing_key: None,
            model_ids: vec![],
        })
        .await
        .expect("ListModels RPC failed")
        .into_inner();

    assert!(
        response.model_ids.is_empty(),
        "got: {:?}",
        response.model_ids
    );

    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn list_models_returns_filtered_active_models() {
    init_crypto();

    let (grpc_addr, model_discovery_addr, http_addr, runtime) =
        make_stargate_runtime_with_model_discovery("test-sg-list-active");
    let handle = runtime.start().await.expect("stargate failed to start");

    let seeds = vec![grpc_addr.to_string()];
    let mut alpha = crate::common::start_and_register_backend(
        &seeds,
        "list-backend-alpha",
        "list-alpha",
        false,
    )
    .await;
    let mut beta =
        crate::common::start_and_register_backend(&seeds, "list-backend-beta", "list-beta", false)
            .await;

    wait_for_routing(http_addr, "list-alpha", Duration::from_secs(5)).await;
    wait_for_routing(http_addr, "list-beta", Duration::from_secs(5)).await;

    let mut client = connect_model_discovery(model_discovery_addr).await;
    let models = wait_for_list_models(
        &mut client,
        None,
        &["list-alpha"],
        &["list-alpha"],
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(models.len(), 1);
    assert_eq!(models[0], "list-alpha");

    let all = wait_for_list_models(
        &mut client,
        None,
        &[],
        &["list-alpha", "list-beta"],
        Duration::from_secs(5),
    )
    .await;
    let mut all_ids = all.iter().map(String::as_str).collect::<Vec<_>>();
    all_ids.sort_unstable();
    assert_eq!(all_ids, vec!["list-alpha", "list-beta"]);

    alpha.stop();
    let after_alpha_stop = wait_for_list_models(
        &mut client,
        None,
        &[],
        &["list-beta"],
        Duration::from_secs(5),
    )
    .await;
    let after_alpha_stop_ids = after_alpha_stop
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(after_alpha_stop_ids, vec!["list-beta"]);

    beta.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn recent_list_models_hit_can_be_followed_by_no_candidates_404() {
    init_crypto();

    let (grpc_addr, model_discovery_addr, http_addr, runtime) =
        make_stargate_runtime_with_model_discovery("test-sg-list-404-race");
    let handle = runtime.start().await.expect("stargate failed to start");

    let seeds = vec![grpc_addr.to_string()];
    let mut backend = crate::common::start_and_register_backend(
        &seeds,
        "list-404-backend",
        "list-404-model",
        false,
    )
    .await;
    wait_for_routing(http_addr, "list-404-model", Duration::from_secs(5)).await;

    let mut client = connect_model_discovery(model_discovery_addr).await;
    let listed = wait_for_list_models(
        &mut client,
        None,
        &["list-404-model"],
        &["list-404-model"],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(listed, vec!["list-404-model"]);

    backend.stop();

    let http_client = reqwest::Client::new();
    let stargate_url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "list-404-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut interval = tokio::time::interval(Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let resp = with_proxy_headers(
            http_client.post(&stargate_url),
            "list-404-model",
            "req-list-404-race",
        )
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("request failed");

        let status = resp.status();
        if status == 404 {
            assert_eq!(
                resp.headers()
                    .get("x-stargate-error-code")
                    .and_then(|value| value.to_str().ok()),
                Some("no_eligible_candidates"),
                "recent ListModels hit followed by local model disappearance should return the no-candidates contract"
            );
            let body: serde_json::Value = resp
                .json()
                .await
                .expect("no-candidates response body should be json");
            assert_eq!(body["code"], "no_eligible_candidates");
            break;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!(
                "proxy did not return no-candidates 404 after recent ListModels hit; last_status={status}"
            );
        }
        interval.tick().await;
    }

    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

#[tokio::test]
async fn list_models_filters_by_routing_key() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([
        ("list-token-a", "rk-list-a"),
        ("list-token-b", "rk-list-b"),
    ]));
    let (grpc_addr, model_discovery_addr, http_addr, runtime) =
        make_stargate_runtime_with_auth_and_model_discovery("test-sg-list-rk", auth);
    let handle = runtime.start().await.expect("stargate failed to start");
    let seeds = vec![grpc_addr.to_string()];

    let (inst_addr_a, quic_url_a, _tunnel_a) = start_dummy_inst("list-rk-model-a").await;
    let mut reg_a = InferenceServerRegistrationClient::default();
    let _channels_a = reg_a
        .start(
            InferenceServerRegistrationConfig {
                seeds: seeds.clone(),
                inference_server_id: "list-rk-backend-a".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_a,
                upstream_http_base_url: Some(format!("http://{inst_addr_a}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
                // Skip calibration so the test only exercises discovery scope.
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                output_token_parser_factory: OutputTokenParserFactory::vllm(),
                request_observation_tx: None,
                request_quality_monitor: pylon_lib::RequestQualityMonitorConfig::default(),
                metrics: None,
                retry: pylon_lib::PylonRetryConfig::default(),
                queue_mismatch_retry: pylon_lib::PylonQueueMismatchRetryConfig::default(),
                queue_tracker: pylon_lib::QueueAdmissionTracker::default(),
                auth_token_provider: Some(Arc::new(AuthTokenProvider::Static(
                    "list-token-a".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["list-rk-model-a".to_string()],
        )
        .expect("registration failed");

    let (inst_addr_b, quic_url_b, _tunnel_b) = start_dummy_inst("list-rk-model-b").await;
    let mut reg_b = InferenceServerRegistrationClient::default();
    let _channels_b = reg_b
        .start(
            InferenceServerRegistrationConfig {
                seeds,
                inference_server_id: "list-rk-backend-b".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_b,
                upstream_http_base_url: Some(format!("http://{inst_addr_b}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
                // Skip calibration so the test only exercises discovery scope.
                bringup: BringupConfig {
                    enabled: false,
                    ..BringupConfig::default()
                },
                output_token_parser_factory: OutputTokenParserFactory::vllm(),
                request_observation_tx: None,
                request_quality_monitor: pylon_lib::RequestQualityMonitorConfig::default(),
                metrics: None,
                retry: pylon_lib::PylonRetryConfig::default(),
                queue_mismatch_retry: pylon_lib::PylonQueueMismatchRetryConfig::default(),
                queue_tracker: pylon_lib::QueueAdmissionTracker::default(),
                auth_token_provider: Some(Arc::new(AuthTokenProvider::Static(
                    "list-token-b".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["list-rk-model-b".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-list-a"),
        "list-rk-model-a",
        Duration::from_secs(5),
    )
    .await;
    wait_for_routing_with_rk(
        http_addr,
        Some("rk-list-b"),
        "list-rk-model-b",
        Duration::from_secs(5),
    )
    .await;

    let mut client = connect_model_discovery(model_discovery_addr).await;
    let unscoped = wait_for_list_models(&mut client, None, &[], &[], Duration::from_secs(5)).await;
    assert!(
        unscoped.is_empty(),
        "unscoped ListModels must not include keyed registrations: {unscoped:?}"
    );

    let listed_a = wait_for_list_models(
        &mut client,
        Some("rk-list-a"),
        &[],
        &["list-rk-model-a"],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(listed_a, vec!["list-rk-model-a"]);

    let listed_b = wait_for_list_models(
        &mut client,
        Some("rk-list-b"),
        &[],
        &["list-rk-model-b"],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(listed_b, vec!["list-rk-model-b"]);

    let wrong_key = wait_for_list_models(
        &mut client,
        Some("rk-list-c"),
        &[],
        &[],
        Duration::from_secs(5),
    )
    .await;
    assert!(
        wrong_key.is_empty(),
        "ListModels must not leak models across routing keys: {wrong_key:?}"
    );

    let space_padded = wait_for_list_models(
        &mut client,
        Some(" rk-list-a "),
        &[" list-rk-model-a "],
        &["list-rk-model-a"],
        Duration::from_secs(5),
    )
    .await;
    assert_eq!(
        space_padded.len(),
        1,
        "ListModels should trim model filters like proxy headers: {:?}",
        space_padded
    );
    assert_eq!(space_padded[0], "list-rk-model-a");

    let blank_model_filter = client
        .list_models(ListModelsRequest {
            routing_key: Some("rk-list-a".to_string()),
            model_ids: vec![" ".to_string()],
        })
        .await
        .expect_err("blank model filters should be rejected");
    assert_eq!(
        blank_model_filter.code(),
        tonic::Code::InvalidArgument,
        "blank model filters should be caller errors"
    );

    reg_a.stop();
    reg_b.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}
