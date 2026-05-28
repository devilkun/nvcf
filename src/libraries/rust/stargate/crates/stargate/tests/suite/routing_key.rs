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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    TokenMapAuthenticator, bind_ephemeral_udp, init_crypto, make_stargate_runtime,
    make_stargate_runtime_with_auth, make_stargate_runtime_with_reverse_and_auth,
    start_dummy_backend, start_dummy_inst, wait_for_routing, wait_for_routing_with_rk,
    with_proxy_headers, with_proxy_headers_rk,
};
use pylon_lib::{
    AuthTokenProvider, BringupConfig, InferenceServerRegistrationClient,
    InferenceServerRegistrationConfig, OutputTokenParserFactory,
};
use stargate_proto::pb::InferenceServerStatus;

fn reverse_registration_config(
    grpc_addr: SocketAddr,
    inference_server_id: &str,
    backend_addr: SocketAddr,
    auth_token: &str,
) -> InferenceServerRegistrationConfig {
    InferenceServerRegistrationConfig {
        seeds: vec![grpc_addr.to_string()],
        inference_server_id: inference_server_id.to_string(),
        cluster_id: String::new(),
        inference_server_url: format!("http://{backend_addr}"),
        upstream_http_base_url: Some(format!("http://{backend_addr}")),
        min_update_interval: Duration::from_millis(100),
        status: InferenceServerStatus::Active,
        reverse_tunnel: true,
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
        auth_token_provider: Some(Arc::new(AuthTokenProvider::Static(auth_token.to_string()))),
        quic_insecure: true,
        tunnel_protocol: Default::default(),
    }
}

async fn assert_no_eligible_candidates(resp: reqwest::Response, context: &str) {
    assert_eq!(
        resp.status(),
        404,
        "{context} should get 404 because no routing target matches"
    );
    assert_eq!(
        resp.headers()
            .get("x-stargate-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("no_eligible_candidates"),
        "{context} should return the no-candidates error code"
    );
    let _ = resp.bytes().await;
}

/// With OpenAuthenticator (default), requests without `x-routing-key` route
/// successfully through the None routing key path.
#[tokio::test]
async fn no_routing_key_header_routes_via_open_authenticator() {
    init_crypto();

    let (grpc_addr, http_addr, runtime) = make_stargate_runtime("test-sg-no-rk");
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr, quic_url, _tunnel) = start_dummy_inst("no-rk-model").await;

    let mut reg = InferenceServerRegistrationClient::default();
    let _channels = reg
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "no-rk-inst".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url,
                upstream_http_base_url: Some(format!("http://{inst_addr}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
            vec!["no-rk-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing(http_addr, "no-rk-model", Duration::from_secs(5)).await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "no-rk-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    let resp = with_proxy_headers(http_client.post(&url), "no-rk-model", "req-no-rk")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("request failed");
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-inference-server-id")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-rk-inst"
    );

    reg.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// Reverse-tunnel registrations are scoped by the authenticated routing key,
/// just like direct tunnel registrations.
#[tokio::test]
async fn reverse_tunnel_routing_keys_isolate_same_model_backends() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([
        ("token-rt-a", "rk-rt-a"),
        ("token-rt-b", "rk-rt-b"),
    ]));
    let (reverse_addr, reverse_socket) = bind_ephemeral_udp();
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_reverse_and_auth(
        "test-sg-rt-rk-iso",
        reverse_addr,
        Some(reverse_socket),
        auth,
    );
    let handle = runtime.start().await.expect("stargate failed to start");

    let backend_addr_a = start_dummy_backend("rt-rk-iso-model").await;
    let backend_addr_b = start_dummy_backend("rt-rk-iso-model").await;

    let mut reg_a = InferenceServerRegistrationClient::default();
    let _channels_a = reg_a
        .start(
            reverse_registration_config(grpc_addr, "rt-rk-iso-a", backend_addr_a, "token-rt-a"),
            vec!["rt-rk-iso-model".to_string()],
        )
        .expect("registration a failed");

    let mut reg_b = InferenceServerRegistrationClient::default();
    let _channels_b = reg_b
        .start(
            reverse_registration_config(grpc_addr, "rt-rk-iso-b", backend_addr_b, "token-rt-b"),
            vec!["rt-rk-iso-model".to_string()],
        )
        .expect("registration b failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-rt-a"),
        "rt-rk-iso-model",
        Duration::from_secs(10),
    )
    .await;
    wait_for_routing_with_rk(
        http_addr,
        Some("rk-rt-b"),
        "rt-rk-iso-model",
        Duration::from_secs(10),
    )
    .await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "rt-rk-iso-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    for (routing_key, expected_inst, request_prefix) in [
        ("rk-rt-a", "rt-rk-iso-a", "req-rt-rk-a"),
        ("rk-rt-b", "rt-rk-iso-b", "req-rt-rk-b"),
    ] {
        for i in 0..4 {
            let resp = with_proxy_headers_rk(
                http_client.post(&url),
                routing_key,
                "rt-rk-iso-model",
                &format!("{request_prefix}-{i}"),
            )
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .expect("request failed");
            assert_eq!(resp.status(), 200);
            assert_eq!(
                resp.headers()
                    .get("x-inference-server-id")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                expected_inst,
                "{routing_key} should only route to its authenticated reverse-tunnel backend"
            );
            let _ = resp.bytes().await;
        }
    }

    reg_a.stop();
    reg_b.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// Wrong and omitted routing keys must fail before they can reach a reverse
/// tunnel backend registered under another routing identity.
#[tokio::test]
async fn reverse_tunnel_wrong_or_omitted_routing_key_returns_404_no_eligible_candidates() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([("token-rt-x", "rk-rt-x")]));
    let (reverse_addr, reverse_socket) = bind_ephemeral_udp();
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_reverse_and_auth(
        "test-sg-rt-rk-miss",
        reverse_addr,
        Some(reverse_socket),
        auth,
    );
    let handle = runtime.start().await.expect("stargate failed to start");

    let backend_addr = start_dummy_backend("rt-rk-miss-model").await;

    let mut reg = InferenceServerRegistrationClient::default();
    let _channels = reg
        .start(
            reverse_registration_config(grpc_addr, "rt-rk-miss-inst", backend_addr, "token-rt-x"),
            vec!["rt-rk-miss-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-rt-x"),
        "rt-rk-miss-model",
        Duration::from_secs(10),
    )
    .await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "rt-rk-miss-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    let wrong_key_resp = with_proxy_headers_rk(
        http_client.post(&url),
        "rk-rt-wrong",
        "rt-rk-miss-model",
        "req-rt-rk-wrong",
    )
    .header("content-type", "application/json")
    .json(&body)
    .send()
    .await
    .expect("wrong routing-key request failed");
    assert_no_eligible_candidates(wrong_key_resp, "wrong routing key").await;

    let omitted_key_resp = with_proxy_headers(
        http_client.post(&url),
        "rt-rk-miss-model",
        "req-rt-rk-omitted",
    )
    .header("content-type", "application/json")
    .json(&body)
    .send()
    .await
    .expect("omitted routing-key request failed");
    assert_no_eligible_candidates(omitted_key_resp, "omitted routing key").await;

    reg.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// With a TokenMapAuthenticator, a request with the correct `x-routing-key`
/// header reaches the backend registered under that routing key.
#[tokio::test]
async fn routing_key_header_routes_to_correct_backend() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([("token-alpha", "rk-alpha")]));
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_auth("test-sg-rk-match", auth);
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr, quic_url, _tunnel) = start_dummy_inst("rk-model").await;

    let mut reg = InferenceServerRegistrationClient::default();
    let _channels = reg
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "rk-match-inst".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url,
                upstream_http_base_url: Some(format!("http://{inst_addr}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
                    "token-alpha".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["rk-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-alpha"),
        "rk-model",
        Duration::from_secs(5),
    )
    .await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "rk-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    let resp = with_proxy_headers_rk(
        http_client.post(&url),
        "rk-alpha",
        "rk-model",
        "req-rk-match",
    )
    .header("content-type", "application/json")
    .json(&body)
    .send()
    .await
    .expect("request failed");
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("x-inference-server-id")
            .unwrap()
            .to_str()
            .unwrap(),
        "rk-match-inst"
    );

    reg.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// A request with a routing key that has no registered backends returns 404.
#[tokio::test]
async fn wrong_routing_key_returns_404_no_eligible_candidates() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([("token-a", "rk-a")]));
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_auth("test-sg-wrong-rk", auth);
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr, quic_url, _tunnel) = start_dummy_inst("rk404-model").await;

    let mut reg = InferenceServerRegistrationClient::default();
    let _channels = reg
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "rk404-inst".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url,
                upstream_http_base_url: Some(format!("http://{inst_addr}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
                    "token-a".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["rk404-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-a"),
        "rk404-model",
        Duration::from_secs(5),
    )
    .await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "rk404-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    let resp = with_proxy_headers_rk(
        http_client.post(&url),
        "rk-wrong",
        "rk404-model",
        "req-wrong-rk",
    )
    .header("content-type", "application/json")
    .json(&body)
    .send()
    .await
    .expect("request failed");
    assert_eq!(
        resp.status(),
        404,
        "request with wrong routing key should get 404 (no matching candidates)"
    );
    assert_eq!(
        resp.headers()
            .get("x-stargate-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("no_eligible_candidates"),
        "no-candidates proxy errors should be distinguishable from upstream errors"
    );

    reg.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// Two backends registered under different routing keys for the same model are
/// isolated: each routing key only reaches its own backend.
#[tokio::test]
async fn different_routing_keys_isolate_backends() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([
        ("token-a", "rk-a"),
        ("token-b", "rk-b"),
    ]));
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_auth("test-sg-rk-iso", auth);
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr_a, quic_url_a, _tunnel_a) = start_dummy_inst("iso-model").await;
    let (inst_addr_b, quic_url_b, _tunnel_b) = start_dummy_inst("iso-model").await;

    let mut reg_a = InferenceServerRegistrationClient::default();
    let _ch_a = reg_a
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "iso-inst-a".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_a,
                upstream_http_base_url: Some(format!("http://{inst_addr_a}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
                    "token-a".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["iso-model".to_string()],
        )
        .expect("registration failed");

    let mut reg_b = InferenceServerRegistrationClient::default();
    let _ch_b = reg_b
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "iso-inst-b".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url_b,
                upstream_http_base_url: Some(format!("http://{inst_addr_b}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
                    "token-b".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["iso-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(http_addr, Some("rk-a"), "iso-model", Duration::from_secs(5)).await;
    wait_for_routing_with_rk(http_addr, Some("rk-b"), "iso-model", Duration::from_secs(5)).await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "iso-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    for i in 0..5 {
        let resp = with_proxy_headers_rk(
            http_client.post(&url),
            "rk-a",
            "iso-model",
            &format!("req-iso-a-{i}"),
        )
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("request failed");
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("x-inference-server-id")
                .unwrap()
                .to_str()
                .unwrap(),
            "iso-inst-a",
            "rk-a should always route to iso-inst-a"
        );
        let _ = resp.bytes().await;
    }

    for i in 0..5 {
        let resp = with_proxy_headers_rk(
            http_client.post(&url),
            "rk-b",
            "iso-model",
            &format!("req-iso-b-{i}"),
        )
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("request failed");
        assert_eq!(resp.status(), 200);
        assert_eq!(
            resp.headers()
                .get("x-inference-server-id")
                .unwrap()
                .to_str()
                .unwrap(),
            "iso-inst-b",
            "rk-b should always route to iso-inst-b"
        );
        let _ = resp.bytes().await;
    }

    reg_a.stop();
    reg_b.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}

/// A request without `x-routing-key` does NOT match backends registered under
/// a non-None routing key, returning 404.
#[tokio::test]
async fn omitted_routing_key_does_not_match_keyed_backend() {
    init_crypto();

    let auth = Arc::new(TokenMapAuthenticator::new([("token-x", "rk-x")]));
    let (grpc_addr, http_addr, runtime) = make_stargate_runtime_with_auth("test-sg-rk-omit", auth);
    let handle = runtime.start().await.expect("stargate failed to start");

    let (inst_addr, quic_url, _tunnel) = start_dummy_inst("omit-model").await;

    let mut reg = InferenceServerRegistrationClient::default();
    let _channels = reg
        .start(
            InferenceServerRegistrationConfig {
                seeds: vec![grpc_addr.to_string()],
                inference_server_id: "omit-inst".to_string(),
                cluster_id: String::new(),
                inference_server_url: quic_url,
                upstream_http_base_url: Some(format!("http://{inst_addr}")),
                min_update_interval: Duration::from_millis(100),
                status: InferenceServerStatus::Active,
                reverse_tunnel: false,
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
                    "token-x".to_string(),
                ))),
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            vec!["omit-model".to_string()],
        )
        .expect("registration failed");

    wait_for_routing_with_rk(
        http_addr,
        Some("rk-x"),
        "omit-model",
        Duration::from_secs(5),
    )
    .await;

    let http_client = reqwest::Client::new();
    let url = format!("http://{http_addr}/v1/chat/completions");
    let body = serde_json::json!({
        "model": "omit-model",
        "messages": [{"role": "user", "content": "hi"}],
        "stream": true,
    });

    let resp = with_proxy_headers(http_client.post(&url), "omit-model", "req-omit-rk")
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
        .expect("request failed");
    assert_eq!(
        resp.status(),
        404,
        "request without x-routing-key should not match backend registered with rk-x"
    );
    assert_eq!(
        resp.headers()
            .get("x-stargate-error-code")
            .and_then(|value| value.to_str().ok()),
        Some("no_eligible_candidates"),
        "no-candidates proxy errors should be distinguishable from upstream errors"
    );

    reg.stop();
    handle.begin_shutdown();
    handle.wait_for_shutdown(Duration::from_secs(5)).await;
}
