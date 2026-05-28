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
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail, ensure};
use bytes::Buf;
use futures::TryStreamExt;
use quinn::Endpoint;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use sonic_rs::JsonValueTrait;
use stargate_telemetry::{
    inject_trace_context, parent_context_from_headers, traceparent_from_headers,
};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, Span, field};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use stargate_protocol::TunnelTransportProtocol;
use stargate_protocol::{RecvStream, SendStream};

use crate::PylonMetrics;
use crate::output_token_parser::{
    OutputTokenParser, OutputTokenParserFactory, OutputTokenProgress,
};
use crate::queue_admission::{
    HEADER_STARGATE_EXPECTED_QUEUE_MS, PylonQueueMismatchRetryConfig, QueueAdmissionDecision,
    QueueAdmissionTracker, QueueTrackedRequestGuard, RETRY_REASON_QUEUE_ESTIMATE_MISMATCH,
};
use crate::request_observer::{
    EmbeddingsRequestObserver, MissingRequiredHeaderError, RequestObservation,
    RequestObservationEndpoint, RequestObserver, RequiredTunnelHeaders,
    embedding_items_from_request_body, validate_required_tunnel_headers,
};
use crate::request_quality_monitor::{
    RequestOutputTokenProgress, RequestQualityMonitorConfig, RequestQualityRecorder,
};
use crate::sse_message_stream::{
    ParsedSseMessage, SseMessage, SseReadTimeoutPhase, UpstreamSseMessageStream,
    UpstreamSseReadError, upstream_sse_message_stream,
};

const DEFAULT_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_FIRST_OUTPUT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OUTPUT_CHUNK_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_SPECULATIVE_REQUEST_BODY_PREALLOC_BYTES: usize = 64 * 1024;
const HEADER_STARGATE_UPSTREAM_RETRYABLE: &str = "x-stargate-upstream-retryable";
const HEADER_STARGATE_RETRYABLE: &str = "x-stargate-retryable";
const HEADER_STARGATE_RETRY_REASON: &str = "x-stargate-retry-reason";
const HEADER_STARGATE_RETRY_AFTER_MS: &str = "x-stargate-retry-after-ms";
const RETRY_REASON_UPSTREAM_ADMISSION_REJECTED: &str = "upstream_admission_rejected";
const RETRY_REASON_LOCAL_CONNECT_FAILURE: &str = "local_connect_failure";
const WEBTRANSPORT_TUNNEL_PATH: &str = "/_stargate/webtransport";
const HEADER_INFERENCE_SERVER_ID: &str = "x-inference-server-id";
const HEADER_REVERSE_AUTH_TOKEN: &str = "x-stargate-auth-token";
const WEBTRANSPORT_STREAM_HEADER_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct PylonRetryConfig {
    pub retryable_upstream_status_codes: Vec<reqwest::StatusCode>,
    pub require_upstream_retry_header: bool,
    pub upstream_retry_header: HeaderName,
    pub propagate_retry_after: bool,
    pub local_connect_failures_retryable: bool,
}

impl Default for PylonRetryConfig {
    fn default() -> Self {
        Self {
            retryable_upstream_status_codes: vec![
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                reqwest::StatusCode::SERVICE_UNAVAILABLE,
            ],
            require_upstream_retry_header: true,
            upstream_retry_header: HeaderName::from_static(HEADER_STARGATE_UPSTREAM_RETRYABLE),
            propagate_retry_after: true,
            local_connect_failures_retryable: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TunnelError {
    #[error("TLS configuration failed: {0}")]
    Tls(String),
    #[error("failed to bind QUIC endpoint: {0}")]
    Bind(#[source] std::io::Error),
    #[error("reverse tunnel handshake failed: {0}")]
    Handshake(String),
    #[error("reverse tunnel connection failed: {0}")]
    Connect(String),
}

#[derive(Clone, Debug)]
pub struct QuicHttpTunnelConfig {
    pub listen_addr: SocketAddr,
    pub inference_server_id: Option<String>,
    pub upstream_http_base_url: String,
    pub max_request_body_bytes: usize,
    pub first_output_timeout: Duration,
    pub output_chunk_timeout: Duration,
    pub output_token_parser_factory: OutputTokenParserFactory,
    pub tls_cert_pem: Option<Vec<u8>>,
    pub tls_key_pem: Option<Vec<u8>>,
    pub quic_insecure: bool,
    pub tunnel_protocol: TunnelTransportProtocol,
    pub request_observation_tx: Option<flume::Sender<RequestObservation>>,
    pub request_quality_monitor: RequestQualityMonitorConfig,
    pub retry: PylonRetryConfig,
    pub queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    pub queue_tracker: QueueAdmissionTracker,
    pub metrics: Option<Arc<PylonMetrics>>,
}

impl QuicHttpTunnelConfig {
    pub fn new(listen_addr: SocketAddr, upstream_http_base_url: String) -> Self {
        Self {
            listen_addr,
            inference_server_id: None,
            upstream_http_base_url,
            max_request_body_bytes: DEFAULT_MAX_BODY_BYTES,
            first_output_timeout: DEFAULT_FIRST_OUTPUT_TIMEOUT,
            output_chunk_timeout: DEFAULT_OUTPUT_CHUNK_TIMEOUT,
            output_token_parser_factory: OutputTokenParserFactory,
            tls_cert_pem: None,
            tls_key_pem: None,
            quic_insecure: false,
            tunnel_protocol: TunnelTransportProtocol::Custom,
            request_observation_tx: None,
            request_quality_monitor: RequestQualityMonitorConfig::default(),
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            metrics: None,
        }
    }
}

/// Both tunnel handles (`QuicHttpTunnelHandle`, `ReverseQuicTunnelHandle`)
/// cancel their `CancellationToken` on drop so the spawned accept loop exits
/// and the QUIC connection/endpoint closes even if the caller never awaits
/// `shutdown()` (e.g. the handle is dropped by `JoinHandle::abort()`).
/// Without this, leaked connections cause the server to NACK reconnections
/// as duplicates.
#[derive(Debug)]
pub struct QuicHttpTunnelHandle {
    listen_addr: SocketAddr,
    endpoint: Endpoint,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
}

impl QuicHttpTunnelHandle {
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.endpoint.close(0u32.into(), b"shutdown");
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }
}

impl Drop for QuicHttpTunnelHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone)]
struct TunnelServerApp {
    http_client: reqwest::Client,
    inference_server_id: String,
    upstream_http_base_url: String,
    max_request_body_bytes: usize,
    first_output_timeout: Duration,
    output_chunk_timeout: Duration,
    output_token_parser_factory: OutputTokenParserFactory,
    request_observation_tx: Option<flume::Sender<RequestObservation>>,
    request_quality_monitor: RequestQualityMonitorConfig,
    retry: PylonRetryConfig,
    queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    queue_tracker: QueueAdmissionTracker,
    metrics: Option<Arc<PylonMetrics>>,
}

pub async fn start_quic_http_tunnel(
    config: QuicHttpTunnelConfig,
) -> Result<QuicHttpTunnelHandle, TunnelError> {
    ensure_rustls_provider();
    let server_config = make_server_config(
        config.tls_cert_pem.as_deref(),
        config.tls_key_pem.as_deref(),
        config.tunnel_protocol,
    )
    .map_err(|e| TunnelError::Tls(e.to_string()))?;
    let endpoint =
        Endpoint::server(server_config, config.listen_addr).map_err(TunnelError::Bind)?;
    let listen_addr = endpoint
        .local_addr()
        .map_err(|e| TunnelError::Bind(std::io::Error::other(e)))?;

    let shutdown = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    let endpoint_for_task = endpoint.clone();
    let shutdown_for_task = shutdown.clone();
    let tunnel_protocol = config.tunnel_protocol;
    let app = TunnelServerApp {
        http_client: reqwest::Client::new(),
        inference_server_id: config.inference_server_id.unwrap_or_default(),
        upstream_http_base_url: config.upstream_http_base_url.clone(),
        max_request_body_bytes: config.max_request_body_bytes,
        first_output_timeout: config.first_output_timeout,
        output_chunk_timeout: config.output_chunk_timeout,
        output_token_parser_factory: config.output_token_parser_factory.clone(),
        request_observation_tx: config.request_observation_tx.clone(),
        request_quality_monitor: config.request_quality_monitor.clone(),
        retry: config.retry.clone(),
        queue_mismatch_retry: config.queue_mismatch_retry.clone(),
        queue_tracker: config.queue_tracker.clone(),
        metrics: config.metrics.clone(),
    };
    let task_tracker_for_accept = task_tracker.clone();
    let task_tracker_for_streams = task_tracker.clone();

    task_tracker.spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_for_task.cancelled() => break,
                incoming = endpoint_for_task.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };
                    let shutdown_for_conn = shutdown_for_task.clone();
                    let app = app.clone();
                    let tracker = task_tracker_for_streams.clone();
                    task_tracker_for_accept.spawn(async move {
                        if let Err(error) = handle_connection(
                            incoming,
                            shutdown_for_conn,
                            tracker,
                            app,
                            tunnel_protocol,
                        ).await {
                            tracing::warn!(error = %error, "quic tunnel connection failed");
                        }
                    });
                }
            }
        }
    });

    Ok(QuicHttpTunnelHandle {
        listen_addr,
        endpoint,
        shutdown,
        task_tracker,
    })
}

fn ensure_rustls_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Extracts the hostname from a `host:port` target address for use as TLS SNI.
/// Falls back to `"stargate"` if the host is an IP address or localhost.
fn derive_sni(target_addr: &str) -> String {
    let host = target_addr
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(target_addr);
    if host.parse::<std::net::IpAddr>().is_ok() || host == "localhost" {
        "stargate".to_string()
    } else {
        host.to_string()
    }
}

fn target_authority(target_addr: &str) -> String {
    if target_addr.starts_with('[') {
        return target_addr.to_string();
    }
    let Some((host, port)) = target_addr.rsplit_once(':') else {
        return target_addr.to_string();
    };
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        target_addr.to_string()
    }
}

async fn handle_connection(
    incoming: quinn::Incoming,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<()> {
    match tunnel_protocol {
        TunnelTransportProtocol::Custom => {
            handle_custom_connection(incoming, shutdown, task_tracker, app).await
        }
        TunnelTransportProtocol::Http3 => {
            handle_h3_connection(incoming, shutdown, task_tracker, app).await
        }
        TunnelTransportProtocol::WebTransport => {
            handle_webtransport_connection(incoming, shutdown, task_tracker, app).await
        }
    }
}

async fn handle_custom_connection(
    incoming: quinn::Incoming,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
) -> Result<()> {
    let connection = incoming.await.context("accept quic connection failed")?;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            stream = connection.accept_bi() => {
                let Ok((quinn_send, quinn_recv)) = stream else {
                    break;
                };
                let app = app.clone();
                task_tracker.spawn(async move {
                    if let Err(error) = handle_stream(quinn_send, quinn_recv, &app).await {
                        tracing::warn!(error = %error, "quic tunnel stream failed");
                    }
                });
            }
        }
    }
    Ok(())
}

/// RAII guard that ensures `observer.fail()` is called if the observer has not
/// reached a terminal state by the time this guard is dropped.
struct ObserverGuard(RequestObserver);

impl std::ops::Deref for ObserverGuard {
    type Target = RequestObserver;
    fn deref(&self) -> &RequestObserver {
        &self.0
    }
}

impl std::ops::DerefMut for ObserverGuard {
    fn deref_mut(&mut self) -> &mut RequestObserver {
        &mut self.0
    }
}

impl Drop for ObserverGuard {
    fn drop(&mut self) {
        if !self.0.is_terminal() {
            self.0.fail();
        }
    }
}

struct EmbeddingsObserverGuard(EmbeddingsRequestObserver);

impl std::ops::Deref for EmbeddingsObserverGuard {
    type Target = EmbeddingsRequestObserver;
    fn deref(&self) -> &EmbeddingsRequestObserver {
        &self.0
    }
}

impl std::ops::DerefMut for EmbeddingsObserverGuard {
    fn deref_mut(&mut self) -> &mut EmbeddingsRequestObserver {
        &mut self.0
    }
}

impl Drop for EmbeddingsObserverGuard {
    fn drop(&mut self) {
        if !self.0.is_terminal() {
            self.0.fail();
        }
    }
}

fn embeddings_observer_for_request(
    method: &reqwest::Method,
    path_and_query: &str,
    required_tunnel_headers: Option<RequiredTunnelHeaders>,
    observation_tx: Option<flume::Sender<RequestObservation>>,
) -> Result<Option<EmbeddingsObserverGuard>> {
    if !is_embeddings_request(method, path_and_query) {
        return Ok(None);
    }

    let required = required_tunnel_headers
        .ok_or_else(|| anyhow::anyhow!("required tunnel headers missing for embeddings request"))?;
    Ok(Some(EmbeddingsObserverGuard(
        EmbeddingsRequestObserver::accepted(required, observation_tx),
    )))
}

fn update_embeddings_observer_items(
    embeddings_observer: &mut Option<EmbeddingsObserverGuard>,
    body_bytes: &[u8],
) {
    if let Some(obs) = embeddings_observer.as_deref_mut() {
        obs.update_embedding_items(embedding_items_from_request_body(body_bytes));
    }
}

fn fail_tunnel_observers(
    observer: &mut Option<ObserverGuard>,
    embeddings_observer: &mut Option<EmbeddingsObserverGuard>,
) {
    if let Some(obs) = observer.as_deref_mut() {
        obs.fail();
    }
    if let Some(obs) = embeddings_observer.as_deref_mut() {
        obs.fail();
    }
}

fn evaluate_queue_admission(
    app: &TunnelServerApp,
    required_tunnel_headers: &RequiredTunnelHeaders,
    request_headers: &HeaderMap,
) -> QueueAdmissionDecision {
    let decision = app.queue_tracker.evaluate(
        &app.queue_mismatch_retry,
        required_tunnel_headers,
        request_headers,
    );
    if let Some(metrics) = app.metrics.as_deref() {
        metrics.observe_queue_admission_decision(
            &app.inference_server_id,
            &required_tunnel_headers.model_id,
            decision.result_label(),
            decision.expected_ms(),
            decision.actual_ms(),
        );
    }
    tracing::info!(
        queue.expected_ms = decision.expected_ms().unwrap_or_default(),
        queue.expected_present = decision.expected_ms().is_some(),
        queue.actual_ms = decision.actual_ms().unwrap_or_default(),
        queue.actual_present = decision.actual_ms().is_some(),
        queue.admission_result = decision.result_label(),
        queue.mismatch_threshold_ms = decision.threshold_ms().unwrap_or_default(),
        queue.mismatch_threshold_present = decision.threshold_ms().is_some(),
        "evaluated local queue mismatch admission"
    );
    decision
}

fn tracked_queue_request_for_required_headers(
    app: &TunnelServerApp,
    required_tunnel_headers: Option<&RequiredTunnelHeaders>,
) -> Option<QueueTrackedRequestGuard> {
    required_tunnel_headers.map(|required| app.queue_tracker.track_request(required))
}

fn observe_queue_output(queue_request: &mut Option<QueueTrackedRequestGuard>) {
    if let Some(queue_request) = queue_request.as_mut() {
        queue_request.observe_output();
    }
}

fn cleanup_rejected_queue_request(app: &TunnelServerApp, required: &RequiredTunnelHeaders) {
    // Observers are created before admission so body validation and terminal
    // accounting keep their existing order. Remove synchronously before sending
    // the rejection so an observation that won the race cannot self-count or
    // briefly leak in the tracker.
    app.queue_tracker.remove_request_id(&required.request_id);
}

fn tunnel_observers_on_upstream_response_headers(
    observer: &mut Option<ObserverGuard>,
    embeddings_observer: &mut Option<EmbeddingsObserverGuard>,
    queue_request: &mut Option<QueueTrackedRequestGuard>,
    response_headers: &HeaderMap,
    status: reqwest::StatusCode,
) {
    if let Some(queue_request) = queue_request.as_mut() {
        queue_request.on_upstream_response_headers(response_headers);
    }
    if let Some(obs) = observer.as_deref_mut() {
        obs.on_upstream_response_headers(response_headers, status.as_u16());
    }
    if let Some(obs) = embeddings_observer.as_deref_mut() {
        obs.on_upstream_response_headers(status.as_u16());
    }
}

fn finish_tunnel_observers(
    observer: &mut Option<ObserverGuard>,
    embeddings_observer: &mut Option<EmbeddingsObserverGuard>,
    queue_request: &mut Option<QueueTrackedRequestGuard>,
) {
    if let Some(obs) = observer.as_deref_mut()
        && !obs.is_terminal()
    {
        obs.finish();
    }
    if let Some(obs) = embeddings_observer.as_deref_mut() {
        obs.finish();
    }
    if let Some(queue_request) = queue_request.as_mut() {
        queue_request.finish();
    }
}

async fn handle_h3_connection(
    incoming: quinn::Incoming,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
) -> Result<()> {
    let connection = incoming.await.context("accept quic connection failed")?;
    handle_h3_established_connection(connection, shutdown, task_tracker, app).await
}

async fn handle_h3_established_connection(
    connection: quinn::Connection,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
) -> Result<()> {
    let mut h3_connection = h3::server::builder()
        .build(h3_quinn::Connection::new(connection))
        .await
        .map_err(|error| anyhow::anyhow!("create h3 server connection: {error:?}"))?;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = h3_connection.accept() => {
                match accepted {
                    Ok(Some(resolver)) => {
                        let app = app.clone();
                        task_tracker.spawn(async move {
                            if let Err(error) = handle_h3_request(resolver, &app).await {
                                tracing::warn!(error = %error, "h3 tunnel request failed");
                            }
                        });
                    }
                    Ok(None) => break,
                    Err(error) if error.is_h3_no_error() => break,
                    Err(error) => return Err(anyhow::anyhow!("h3 accept failed: {error:?}")),
                }
            }
        }
    }
    Ok(())
}

async fn handle_webtransport_connection(
    incoming: quinn::Incoming,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
) -> Result<()> {
    let connection = incoming.await.context("accept quic connection failed")?;
    handle_webtransport_established_connection(connection, shutdown, task_tracker, app).await
}

async fn handle_webtransport_established_connection(
    connection: quinn::Connection,
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
    app: TunnelServerApp,
) -> Result<()> {
    let mut builder = h3::server::builder();
    builder
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1);
    let mut h3_connection: h3::server::Connection<h3_quinn::Connection, bytes::Bytes> = builder
        .build(h3_quinn::Connection::new(connection.clone()))
        .await
        .map_err(|error| anyhow::anyhow!("create WebTransport h3 server: {error:?}"))?;
    let Some(resolver) = h3_connection
        .accept()
        .await
        .map_err(|error| anyhow::anyhow!("accept WebTransport CONNECT: {error:?}"))?
    else {
        return Ok(());
    };
    let (request, mut connect_stream) = resolver
        .resolve_request()
        .await
        .map_err(|error| anyhow::anyhow!("resolve WebTransport CONNECT: {error:?}"))?;
    let is_webtransport = request
        .extensions()
        .get::<h3::ext::Protocol>()
        .is_some_and(|protocol| *protocol == h3::ext::Protocol::WEB_TRANSPORT);
    if request.method() != reqwest::Method::CONNECT
        || request.uri().path() != WEBTRANSPORT_TUNNEL_PATH
        || !is_webtransport
    {
        let response = http::Response::builder()
            .status(reqwest::StatusCode::BAD_REQUEST.as_u16())
            .body(())
            .context("build WebTransport rejection")?;
        connect_stream
            .send_response(response)
            .await
            .map_err(|error| anyhow::anyhow!("send WebTransport rejection: {error:?}"))?;
        connect_stream
            .finish()
            .await
            .map_err(|error| anyhow::anyhow!("finish WebTransport rejection: {error:?}"))?;
        bail!("invalid WebTransport CONNECT request");
    }
    let session_id = connect_stream.id().into_inner();
    let response = http::Response::builder()
        .status(reqwest::StatusCode::OK.as_u16())
        .body(())
        .context("build WebTransport CONNECT response")?;
    connect_stream
        .send_response(response)
        .await
        .map_err(|error| anyhow::anyhow!("send WebTransport CONNECT response: {error:?}"))?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            stream = connection.accept_bi() => {
                let Ok((quinn_send, quinn_recv)) = stream else {
                    break;
                };
                let app = app.clone();
                task_tracker.spawn(async move {
                    if let Err(error) =
                        handle_webtransport_stream(quinn_send, quinn_recv, session_id, app).await
                    {
                        tracing::warn!(error = %error, "WebTransport tunnel stream failed");
                    }
                });
            }
        }
    }
    // Keep the CONNECT stream alive for the duration of the WebTransport loop.
    drop(connect_stream);
    Ok(())
}

async fn handle_h3_request(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, bytes::Bytes>,
    app: &TunnelServerApp,
) -> Result<()> {
    let (request, mut stream) = resolver
        .resolve_request()
        .await
        .map_err(|error| anyhow::anyhow!("resolve h3 request: {error:?}"))?;
    let method: reqwest::Method = request
        .method()
        .as_str()
        .parse()
        .context("invalid h3 method")?;
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/")
        .to_string();
    let request_headers = request.headers().clone();
    let streaming_endpoint = stream_request_observation_endpoint(&method, &path_and_query);
    let is_chat_completion_request =
        streaming_endpoint == Some(RequestObservationEndpoint::ChatCompletions);
    let required_tunnel_headers = if is_health_request_path(&path_and_query) {
        None
    } else {
        match validate_required_tunnel_headers(&request_headers) {
            Ok(headers) => Some(headers),
            Err(MissingRequiredHeaderError { header_name }) => {
                send_h3_error_response(
                    &mut stream,
                    reqwest::StatusCode::BAD_REQUEST,
                    format!("missing required {header_name} header"),
                )
                .await?;
                return Ok(());
            }
        }
    };
    let mut observer = if let Some(endpoint) = streaming_endpoint {
        let required = required_tunnel_headers.clone().ok_or_else(|| {
            anyhow::anyhow!("required tunnel headers missing for streaming request")
        })?;
        Some(ObserverGuard(RequestObserver::from_required(
            endpoint,
            required,
            app.request_observation_tx.clone(),
        )))
    } else {
        None
    };
    let mut quality_recorder =
        if is_chat_completion_request && app.request_quality_monitor.enabled() {
            Some(RequestQualityRecorder::new())
        } else {
            None
        };
    let mut embeddings_observer = embeddings_observer_for_request(
        &method,
        &path_and_query,
        required_tunnel_headers.clone(),
        app.request_observation_tx.clone(),
    )?;

    let body_bytes =
        read_h3_request_body(&mut stream, &request_headers, app.max_request_body_bytes).await?;
    if is_health_request_path(&path_and_query) {
        return handle_h3_health_stream(
            app,
            &mut stream,
            method,
            &path_and_query,
            &request_headers,
            body_bytes,
        )
        .await;
    }
    update_embeddings_observer_items(&mut embeddings_observer, &body_bytes);
    if let Err(error) = validate_request_body(&method, &path_and_query, &body_bytes) {
        fail_tunnel_observers(&mut observer, &mut embeddings_observer);
        send_h3_error_response(
            &mut stream,
            reqwest::StatusCode::BAD_REQUEST,
            error.to_string(),
        )
        .await?;
        return Ok(());
    }
    if let Some(required) = required_tunnel_headers.as_ref() {
        let decision = evaluate_queue_admission(app, required, &request_headers);
        if matches!(decision, QueueAdmissionDecision::Rejected { .. }) {
            cleanup_rejected_queue_request(app, required);
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_h3_queue_mismatch_response(&mut stream, app, &decision).await?;
            return Ok(());
        }
    }
    let mut queue_request =
        tracked_queue_request_for_required_headers(app, required_tunnel_headers.as_ref());

    let response = match send_traced_upstream_request(
        app,
        method,
        &path_and_query,
        &request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_h3_local_connect_failure_response(&mut stream, app, &error, true).await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_h3_local_connect_failure_response(&mut stream, app, &error, false).await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    let status = response.status();
    let response_headers = response.headers().clone();
    send_h3_success_headers(
        &mut stream,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;
    tunnel_observers_on_upstream_response_headers(
        &mut observer,
        &mut embeddings_observer,
        &mut queue_request,
        &response_headers,
        status,
    );

    if streaming_endpoint.is_some() && is_sse_response(&response_headers) {
        let mut upstream_messages = upstream_sse_message_stream(
            response.bytes_stream(),
            app.first_output_timeout,
            app.output_chunk_timeout,
        );
        let mut output_token_parser = app.output_token_parser_factory.create();
        let obs = observer
            .as_deref_mut()
            .ok_or_else(|| anyhow::anyhow!("observer missing for observed streaming request"))?;
        let mut response_body_sink = H3ResponseBodyEventSink {
            stream: &mut stream,
        };
        relay_remaining_output(
            &mut upstream_messages,
            &mut output_token_parser,
            obs,
            quality_recorder.as_mut(),
            &mut queue_request,
            &mut response_body_sink,
        )
        .await?;
    } else {
        if status.is_success() {
            observe_queue_output(&mut queue_request);
        }
        if let Some(obs) = observer.as_deref_mut()
            && status.is_success()
        {
            obs.observe_output_message();
        }
        relay_response_body_raw_h3(response.bytes_stream(), &mut stream).await?;
    }

    stream
        .finish()
        .await
        .map_err(|error| anyhow::anyhow!("failed to finish h3 response stream: {error:?}"))?;
    finish_tunnel_observers(&mut observer, &mut embeddings_observer, &mut queue_request);
    finalize_quality_check(
        &request_headers,
        quality_recorder.as_ref(),
        &app.request_quality_monitor,
        app.metrics.as_deref(),
    );
    Ok(())
}

async fn handle_stream(
    quinn_send: quinn::SendStream,
    quinn_recv: quinn::RecvStream,
    app: &TunnelServerApp,
) -> Result<()> {
    let mut recv_stream = RecvStream::new(quinn_recv);
    let mut send_stream = SendStream::new(quinn_send);

    let request_headers = recv_stream
        .recv_header()
        .await
        .context("failed to read request headers")?;

    let method: reqwest::Method = request_headers
        .get("x-method")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("missing required x-method header"))?
        .parse()
        .context("invalid method")?;

    let path_and_query = request_headers
        .get("x-path")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("missing required x-path header"))?
        .to_string();
    let streaming_endpoint = stream_request_observation_endpoint(&method, &path_and_query);
    let is_chat_completion_request =
        streaming_endpoint == Some(RequestObservationEndpoint::ChatCompletions);
    let required_tunnel_headers = if is_health_request_path(&path_and_query) {
        None
    } else {
        match validate_required_tunnel_headers(&request_headers) {
            Ok(headers) => Some(headers),
            Err(MissingRequiredHeaderError { header_name }) => {
                send_error_response(
                    &mut send_stream,
                    reqwest::StatusCode::BAD_REQUEST,
                    format!("missing required {header_name} header"),
                )
                .await?;
                return Ok(());
            }
        }
    };
    let mut observer = if let Some(endpoint) = streaming_endpoint {
        let required = required_tunnel_headers.clone().ok_or_else(|| {
            anyhow::anyhow!("required tunnel headers missing for streaming request")
        })?;
        Some(ObserverGuard(RequestObserver::from_required(
            endpoint,
            required,
            app.request_observation_tx.clone(),
        )))
    } else {
        None
    };
    let mut quality_recorder =
        if is_chat_completion_request && app.request_quality_monitor.enabled() {
            Some(RequestQualityRecorder::new())
        } else {
            None
        };
    let mut embeddings_observer = embeddings_observer_for_request(
        &method,
        &path_and_query,
        required_tunnel_headers.clone(),
        app.request_observation_tx.clone(),
    )?;

    let mut body_bytes = request_body_buffer(&request_headers, app.max_request_body_bytes)?;
    let mut total_body = 0usize;
    while let Some(chunk) = recv_stream.recv_body().await? {
        total_body = next_body_len(total_body, chunk.len(), app.max_request_body_bytes)?;
        body_bytes.extend_from_slice(&chunk);
    }
    if is_health_request_path(&path_and_query) {
        return handle_health_stream(
            app,
            &mut send_stream,
            method,
            &path_and_query,
            &request_headers,
            body_bytes,
        )
        .await;
    }
    update_embeddings_observer_items(&mut embeddings_observer, &body_bytes);
    if let Err(error) = validate_request_body(&method, &path_and_query, &body_bytes) {
        fail_tunnel_observers(&mut observer, &mut embeddings_observer);
        send_error_response(
            &mut send_stream,
            reqwest::StatusCode::BAD_REQUEST,
            error.to_string(),
        )
        .await?;
        return Ok(());
    }
    if let Some(required) = required_tunnel_headers.as_ref() {
        let decision = evaluate_queue_admission(app, required, &request_headers);
        if matches!(decision, QueueAdmissionDecision::Rejected { .. }) {
            cleanup_rejected_queue_request(app, required);
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_queue_mismatch_response(&mut send_stream, app, &decision).await?;
            return Ok(());
        }
    }
    let mut queue_request =
        tracked_queue_request_for_required_headers(app, required_tunnel_headers.as_ref());

    let response = match send_traced_upstream_request(
        app,
        method,
        &path_and_query,
        &request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_local_connect_failure_response(&mut send_stream, app, &error, true).await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_local_connect_failure_response(&mut send_stream, app, &error, false).await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    let status = response.status();
    let response_headers = response.headers().clone();
    send_success_headers(
        &mut send_stream,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;
    tunnel_observers_on_upstream_response_headers(
        &mut observer,
        &mut embeddings_observer,
        &mut queue_request,
        &response_headers,
        status,
    );

    if streaming_endpoint.is_some() && is_sse_response(&response_headers) {
        let mut upstream_messages = upstream_sse_message_stream(
            response.bytes_stream(),
            app.first_output_timeout,
            app.output_chunk_timeout,
        );
        let mut output_token_parser = app.output_token_parser_factory.create();
        let obs = observer
            .as_deref_mut()
            .ok_or_else(|| anyhow::anyhow!("observer missing for observed streaming request"))?;
        relay_remaining_output(
            &mut upstream_messages,
            &mut output_token_parser,
            obs,
            quality_recorder.as_mut(),
            &mut queue_request,
            &mut send_stream,
        )
        .await?;
    } else {
        if status.is_success() {
            observe_queue_output(&mut queue_request);
        }
        if let Some(obs) = observer.as_deref_mut()
            && status.is_success()
        {
            obs.observe_output_message();
        }
        relay_response_body_raw(response.bytes_stream(), &mut send_stream).await?;
    }

    send_stream
        .finish()
        .context("failed to finish send stream")?;
    finish_tunnel_observers(&mut observer, &mut embeddings_observer, &mut queue_request);
    finalize_quality_check(
        &request_headers,
        quality_recorder.as_ref(),
        &app.request_quality_monitor,
        app.metrics.as_deref(),
    );

    Ok(())
}

async fn handle_webtransport_http_stream(
    mut quinn_send: quinn::SendStream,
    mut quinn_recv: quinn::RecvStream,
    app: &TunnelServerApp,
) -> Result<()> {
    let request_head = stargate_protocol::read_webtransport_http_request_head(&mut quinn_recv)
        .await
        .context("failed to read WebTransport request head")?;
    let method: reqwest::Method = request_head
        .method
        .as_str()
        .parse()
        .context("invalid WebTransport request method")?;
    let path_and_query = request_head.path_and_query;
    let request_headers = request_head.headers;
    let streaming_endpoint = stream_request_observation_endpoint(&method, &path_and_query);
    let is_chat_completion_request =
        streaming_endpoint == Some(RequestObservationEndpoint::ChatCompletions);
    let required_tunnel_headers = if is_health_request_path(&path_and_query) {
        None
    } else {
        match validate_required_tunnel_headers(&request_headers) {
            Ok(headers) => Some(headers),
            Err(MissingRequiredHeaderError { header_name }) => {
                send_webtransport_error_response(
                    &mut quinn_send,
                    reqwest::StatusCode::BAD_REQUEST,
                    format!("missing required {header_name} header"),
                )
                .await?;
                return Ok(());
            }
        }
    };
    let mut observer = if let Some(endpoint) = streaming_endpoint {
        let required = required_tunnel_headers.clone().ok_or_else(|| {
            anyhow::anyhow!("required tunnel headers missing for streaming request")
        })?;
        Some(ObserverGuard(RequestObserver::from_required(
            endpoint,
            required,
            app.request_observation_tx.clone(),
        )))
    } else {
        None
    };
    let mut quality_recorder =
        if is_chat_completion_request && app.request_quality_monitor.enabled() {
            Some(RequestQualityRecorder::new())
        } else {
            None
        };
    let mut embeddings_observer = embeddings_observer_for_request(
        &method,
        &path_and_query,
        required_tunnel_headers.clone(),
        app.request_observation_tx.clone(),
    )?;

    let body_bytes = read_webtransport_request_body(
        &mut quinn_recv,
        &request_headers,
        app.max_request_body_bytes,
    )
    .await?;
    if is_health_request_path(&path_and_query) {
        return handle_webtransport_health_stream(
            app,
            &mut quinn_send,
            method,
            &path_and_query,
            &request_headers,
            body_bytes,
        )
        .await;
    }
    update_embeddings_observer_items(&mut embeddings_observer, &body_bytes);
    if let Err(error) = validate_request_body(&method, &path_and_query, &body_bytes) {
        fail_tunnel_observers(&mut observer, &mut embeddings_observer);
        send_webtransport_error_response(
            &mut quinn_send,
            reqwest::StatusCode::BAD_REQUEST,
            error.to_string(),
        )
        .await?;
        return Ok(());
    }
    if let Some(required) = required_tunnel_headers.as_ref() {
        let decision = evaluate_queue_admission(app, required, &request_headers);
        if matches!(decision, QueueAdmissionDecision::Rejected { .. }) {
            cleanup_rejected_queue_request(app, required);
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_webtransport_queue_mismatch_response(&mut quinn_send, app, &decision).await?;
            return Ok(());
        }
    }
    let mut queue_request =
        tracked_queue_request_for_required_headers(app, required_tunnel_headers.as_ref());

    let response = match send_traced_upstream_request(
        app,
        method,
        &path_and_query,
        &request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_webtransport_local_connect_failure_response(&mut quinn_send, app, &error, true)
                .await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            fail_tunnel_observers(&mut observer, &mut embeddings_observer);
            send_webtransport_local_connect_failure_response(&mut quinn_send, app, &error, false)
                .await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    let status = response.status();
    let response_headers = response.headers().clone();
    send_webtransport_success_headers(
        &mut quinn_send,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;
    tunnel_observers_on_upstream_response_headers(
        &mut observer,
        &mut embeddings_observer,
        &mut queue_request,
        &response_headers,
        status,
    );

    if streaming_endpoint.is_some() && is_sse_response(&response_headers) {
        let mut upstream_messages = upstream_sse_message_stream(
            response.bytes_stream(),
            app.first_output_timeout,
            app.output_chunk_timeout,
        );
        let mut output_token_parser = app.output_token_parser_factory.create();
        let obs = observer
            .as_deref_mut()
            .ok_or_else(|| anyhow::anyhow!("observer missing for observed streaming request"))?;
        let mut response_body_sink = WebTransportResponseBodyEventSink {
            send_stream: &mut quinn_send,
        };
        relay_remaining_output(
            &mut upstream_messages,
            &mut output_token_parser,
            obs,
            quality_recorder.as_mut(),
            &mut queue_request,
            &mut response_body_sink,
        )
        .await?;
    } else {
        if status.is_success() {
            observe_queue_output(&mut queue_request);
        }
        if let Some(obs) = observer.as_deref_mut()
            && status.is_success()
        {
            obs.observe_output_message();
        }
        relay_response_body_raw_webtransport(response.bytes_stream(), &mut quinn_send).await?;
    }

    stargate_protocol::finish_webtransport_http_stream(&mut quinn_send)
        .context("failed to finish WebTransport response stream")?;
    finish_tunnel_observers(&mut observer, &mut embeddings_observer, &mut queue_request);
    finalize_quality_check(
        &request_headers,
        quality_recorder.as_ref(),
        &app.request_quality_monitor,
        app.metrics.as_deref(),
    );

    Ok(())
}

async fn handle_health_stream(
    app: &TunnelServerApp,
    send_stream: &mut SendStream,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> Result<()> {
    let response = match send_untraced_upstream_request(
        app,
        method,
        path_and_query,
        request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            send_local_connect_failure_response(send_stream, app, &error, true).await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            send_local_connect_failure_response(send_stream, app, &error, false).await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let status = response.status();
    let response_headers = response.headers().clone();
    send_success_headers(
        send_stream,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;

    let mut body_stream = response.bytes_stream();
    while let Some(chunk) = body_stream.try_next().await? {
        send_stream
            .send_body(chunk)
            .await
            .context("failed to send health response body")?;
    }
    send_stream
        .finish()
        .context("failed to finish health response stream")?;
    Ok(())
}

async fn handle_h3_health_stream<S>(
    app: &TunnelServerApp,
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> Result<()>
where
    S: h3::quic::SendStream<bytes::Bytes>,
{
    let response = match send_untraced_upstream_request(
        app,
        method,
        path_and_query,
        request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            send_h3_local_connect_failure_response(stream, app, &error, true).await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            send_h3_local_connect_failure_response(stream, app, &error, false).await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let status = response.status();
    let response_headers = response.headers().clone();
    send_h3_success_headers(
        stream,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;
    relay_response_body_raw_h3(response.bytes_stream(), stream).await?;
    stream
        .finish()
        .await
        .map_err(|error| anyhow::anyhow!("failed to finish h3 health response: {error:?}"))?;
    Ok(())
}

async fn handle_webtransport_health_stream(
    app: &TunnelServerApp,
    send_stream: &mut quinn::SendStream,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> Result<()> {
    let response = match send_untraced_upstream_request(
        app,
        method,
        path_and_query,
        request_headers,
        body_bytes,
    )
    .await
    {
        Ok(response) => response,
        Err(error) if app.retry.local_connect_failures_retryable && error.is_connect_failure() => {
            send_webtransport_local_connect_failure_response(send_stream, app, &error, true)
                .await?;
            return Ok(());
        }
        Err(error) if error.is_connect_failure() => {
            send_webtransport_local_connect_failure_response(send_stream, app, &error, false)
                .await?;
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    let status = response.status();
    let response_headers = response.headers().clone();
    send_webtransport_success_headers(
        send_stream,
        status,
        &response_headers,
        &app.retry,
        app.metrics.as_deref(),
        &app.inference_server_id,
    )
    .await?;
    relay_response_body_raw_webtransport(response.bytes_stream(), send_stream).await?;
    stargate_protocol::finish_webtransport_http_stream(send_stream)
        .context("failed to finish WebTransport health response")?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum RequestBodyValidationError {
    #[error("request body must be valid JSON")]
    InvalidJson,
    #[error("{endpoint} requests must set stream=true")]
    StreamingEndpointMustStream { endpoint: &'static str },
}

async fn send_traced_upstream_request(
    app: &TunnelServerApp,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> std::result::Result<reqwest::Response, UpstreamRequestError> {
    let span = pylon_upstream_http_span(app, &method, path_and_query, request_headers);
    let upstream_headers = headers_for_traced_upstream_request(request_headers, &span);
    let result =
        send_upstream_request_inner(app, method, path_and_query, &upstream_headers, body_bytes)
            .instrument(span.clone())
            .await;
    record_pylon_upstream_result_to_span(&span, &result);
    result
}

async fn send_untraced_upstream_request(
    app: &TunnelServerApp,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> std::result::Result<reqwest::Response, UpstreamRequestError> {
    send_upstream_request_inner(app, method, path_and_query, request_headers, body_bytes).await
}

async fn send_upstream_request_inner(
    app: &TunnelServerApp,
    method: reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
    body_bytes: Vec<u8>,
) -> std::result::Result<reqwest::Response, UpstreamRequestError> {
    let request_url = join_base_path(&app.upstream_http_base_url, path_and_query)
        .map_err(UpstreamRequestError::Build)?;
    let mut request = app
        .http_client
        .request(method, request_url)
        .body(body_bytes);
    for (name, value) in request_headers {
        if should_forward_header(name) {
            request = request.header(name, value);
        }
    }
    request.send().await.map_err(UpstreamRequestError::Send)
}

fn pylon_upstream_http_span(
    app: &TunnelServerApp,
    method: &reqwest::Method,
    path_and_query: &str,
    request_headers: &HeaderMap,
) -> Span {
    let span = tracing::info_span!(
        "pylon_upstream_http_request",
        otel_parent = field::Empty,
        http.method = %method,
        http.path = %path_and_query,
        inference_server.id = %app.inference_server_id,
        upstream.status = field::Empty,
        upstream.error = field::Empty,
    );
    span.set_parent(pylon_upstream_parent_context(request_headers));
    if let Some(otel_parent) = otel_parent_from_headers(request_headers) {
        span.record("otel_parent", otel_parent);
    }
    span
}

fn headers_for_traced_upstream_request(request_headers: &HeaderMap, span: &Span) -> HeaderMap {
    let mut upstream_headers = request_headers.clone();
    inject_trace_context(&mut upstream_headers, &span.context());
    upstream_headers
}

fn pylon_upstream_parent_context(headers: &HeaderMap) -> opentelemetry::Context {
    parent_context_from_headers(headers)
}

fn otel_parent_from_headers(headers: &HeaderMap) -> Option<&str> {
    traceparent_from_headers(headers)
}

fn record_pylon_upstream_result_to_span(
    span: &Span,
    result: &std::result::Result<reqwest::Response, UpstreamRequestError>,
) {
    match result {
        Ok(response) => {
            span.record("upstream.status", response.status().as_u16());
        }
        Err(error) => {
            let error = error.to_string();
            span.record("upstream.error", error.as_str());
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum UpstreamRequestError {
    #[error("failed to build upstream request: {0}")]
    Build(#[source] anyhow::Error),
    #[error("upstream http request failed: {0}")]
    Send(#[source] reqwest::Error),
}

impl UpstreamRequestError {
    fn is_connect_failure(&self) -> bool {
        matches!(self, Self::Send(error) if error.is_connect())
    }
}

fn validate_request_body(
    method: &reqwest::Method,
    path_and_query: &str,
    body_bytes: &[u8],
) -> Result<(), RequestBodyValidationError> {
    if sonic_rs::get(body_bytes, &[] as &[&str]).is_err() {
        return Err(RequestBodyValidationError::InvalidJson);
    }

    if let Some(endpoint) = stream_required_endpoint(method, path_and_query)
        && !sonic_rs::get(body_bytes, &["stream"])
            .ok()
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    {
        return Err(RequestBodyValidationError::StreamingEndpointMustStream { endpoint });
    }

    Ok(())
}

fn is_health_request_path(path_and_query: &str) -> bool {
    path_and_query
        .split('?')
        .next()
        .is_some_and(|path| path == "/health")
}

fn stream_request_observation_endpoint(
    method: &reqwest::Method,
    path_and_query: &str,
) -> Option<RequestObservationEndpoint> {
    if method != reqwest::Method::POST {
        return None;
    }

    match path_and_query.split('?').next() {
        Some("/v1/chat/completions") => Some(RequestObservationEndpoint::ChatCompletions),
        Some("/v1/responses") => Some(RequestObservationEndpoint::Responses),
        _ => None,
    }
}

fn stream_required_endpoint(
    method: &reqwest::Method,
    path_and_query: &str,
) -> Option<&'static str> {
    if method != reqwest::Method::POST {
        return None;
    }

    match path_and_query.split('?').next() {
        Some("/v1/chat/completions") => Some("/v1/chat/completions"),
        Some("/v1/responses") => Some("/v1/responses"),
        _ => None,
    }
}

fn is_embeddings_request(method: &reqwest::Method, path_and_query: &str) -> bool {
    method == reqwest::Method::POST
        && path_and_query
            .split('?')
            .next()
            .is_some_and(|path| path == "/v1/embeddings")
}

fn is_sse_response(headers: &HeaderMap) -> bool {
    headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"))
}

trait ResponseBodyEventSink {
    async fn send_body_event(&mut self, event: bytes::Bytes) -> Result<()>;
}

impl ResponseBodyEventSink for SendStream {
    async fn send_body_event(&mut self, event: bytes::Bytes) -> Result<()> {
        self.send_body(event)
            .await
            .context("failed to send response body event")
    }
}

struct H3ResponseBodyEventSink<'a, S>
where
    S: h3::quic::SendStream<bytes::Bytes> + Send,
{
    stream: &'a mut h3::server::RequestStream<S, bytes::Bytes>,
}

impl<S> ResponseBodyEventSink for H3ResponseBodyEventSink<'_, S>
where
    S: h3::quic::SendStream<bytes::Bytes> + Send,
{
    async fn send_body_event(&mut self, event: bytes::Bytes) -> Result<()> {
        self.stream
            .send_data(event)
            .await
            .map_err(|error| anyhow::anyhow!("failed to send h3 response body event: {error:?}"))
    }
}

struct WebTransportResponseBodyEventSink<'a> {
    send_stream: &'a mut quinn::SendStream,
}

impl ResponseBodyEventSink for WebTransportResponseBodyEventSink<'_> {
    async fn send_body_event(&mut self, event: bytes::Bytes) -> Result<()> {
        stargate_protocol::write_webtransport_http_body(self.send_stream, event)
            .await
            .context("failed to send WebTransport response body event")
    }
}

async fn relay_remaining_output<Sink>(
    upstream_messages: &mut UpstreamSseMessageStream,
    output_token_parser: &mut OutputTokenParser,
    observer: &mut RequestObserver,
    quality_recorder: Option<&mut RequestQualityRecorder>,
    queue_request: &mut Option<QueueTrackedRequestGuard>,
    body_sink: &mut Sink,
) -> Result<()>
where
    Sink: ResponseBodyEventSink,
{
    let Some(first_message) =
        read_next_upstream_sse_message(upstream_messages, observer, false).await?
    else {
        return Ok(());
    };

    relay_chunk_stats_fallback_output(
        first_message,
        upstream_messages,
        output_token_parser,
        observer,
        quality_recorder,
        queue_request,
        body_sink,
    )
    .await
}

async fn relay_chunk_stats_fallback_output<Sink>(
    first_message: ParsedSseMessage,
    upstream_messages: &mut UpstreamSseMessageStream,
    output_token_parser: &mut OutputTokenParser,
    observer: &mut RequestObserver,
    quality_recorder: Option<&mut RequestQualityRecorder>,
    queue_request: &mut Option<QueueTrackedRequestGuard>,
    body_sink: &mut Sink,
) -> Result<()>
where
    Sink: ResponseBodyEventSink,
{
    let mut saw_output = false;
    let mut next_message = Some(first_message);
    let mut quality_recorder = quality_recorder;
    loop {
        let parsed_message = match next_message.take() {
            Some(parsed_message) => parsed_message,
            None => {
                let Some(parsed_message) =
                    read_next_upstream_sse_message(upstream_messages, observer, saw_output).await?
                else {
                    return Ok(());
                };
                parsed_message
            }
        };

        let forward_event = Some(parsed_message.raw_event.clone());
        observe_output_message_if_needed(&parsed_message, observer, queue_request, &mut saw_output);
        if let SseMessage::ChatCompletionChunk { raw_data } = &parsed_message.message {
            let output_progress = if !observer.is_terminal() {
                output_token_parser.parse_output_token_progress(raw_data)
            } else {
                None
            };
            if let Some(progress) = output_progress {
                observe_output_token_progress(observer, progress);
            }
            if let Some(recorder) = quality_recorder.as_deref_mut() {
                recorder.observe_sse_chunk_with_token_progress(
                    raw_data,
                    output_progress.map(request_quality_output_token_progress),
                );
            }
        }

        if let Some(event) = forward_event {
            body_sink.send_body_event(event).await?;
        }
    }
}

async fn read_next_upstream_sse_message(
    upstream_messages: &mut UpstreamSseMessageStream,
    observer: &mut RequestObserver,
    saw_output: bool,
) -> Result<Option<ParsedSseMessage>> {
    match upstream_messages.try_next().await {
        Ok(Some(parsed_message)) => Ok(Some(parsed_message)),
        Ok(None) if saw_output => Ok(None),
        Ok(None) => {
            observer.fail();
            bail!("upstream stream ended before first output event");
        }
        Err(UpstreamSseReadError::Timeout(SseReadTimeoutPhase::SubsequentOutput)) => {
            observer.fail();
            bail!("timed out waiting for subsequent output event from upstream");
        }
        Err(UpstreamSseReadError::Timeout(SseReadTimeoutPhase::FirstOutput)) => {
            observer.fail();
            bail!("timed out waiting for first output event from upstream");
        }
        Err(UpstreamSseReadError::Upstream(error)) => {
            observer.fail();
            Err(error.context("failed to read upstream response message"))
        }
    }
}

fn observe_output_message_if_needed(
    parsed_message: &ParsedSseMessage,
    observer: &mut RequestObserver,
    queue_request: &mut Option<QueueTrackedRequestGuard>,
    saw_output: &mut bool,
) {
    if parsed_message.message.counts_as_output() && !observer.is_terminal() {
        *saw_output = true;
        observe_queue_output(queue_request);
        observer.observe_output_message();
    }
}

fn observe_output_token_progress(observer: &mut RequestObserver, progress: OutputTokenProgress) {
    match progress {
        OutputTokenProgress::ExplicitCumulative { tokens, .. } => {
            observer.observe_output_tokens_generated_so_far(tokens);
        }
        OutputTokenProgress::EstimatedDelta { delta } => {
            observer.observe_output_tokens(delta);
        }
    }
}

fn request_quality_output_token_progress(
    progress: OutputTokenProgress,
) -> RequestOutputTokenProgress {
    match progress {
        OutputTokenProgress::ExplicitCumulative { tokens, delta } => {
            RequestOutputTokenProgress::Cumulative { tokens, delta }
        }
        OutputTokenProgress::EstimatedDelta { delta } => RequestOutputTokenProgress::Delta(delta),
    }
}

fn finalize_quality_check(
    request_headers: &HeaderMap,
    quality_recorder: Option<&RequestQualityRecorder>,
    quality_config: &RequestQualityMonitorConfig,
    metrics: Option<&PylonMetrics>,
) {
    let Some(recorder) = quality_recorder else {
        return;
    };
    if !recorder.has_observed_stream_output() {
        return;
    }
    let model_id = request_headers
        .get("x-model")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let (_quality_metrics, quality_result) = recorder.evaluate(quality_config);
    if let Some(metrics) = metrics {
        let result_label = if !quality_result.evaluated {
            "skipped"
        } else if quality_result.threshold_match_reason.is_some() {
            "matched"
        } else {
            "clean"
        };
        metrics.observe_quality_check_result(model_id, result_label);
        if let Some(reason) = quality_result.threshold_match_reason {
            metrics.observe_quality_threshold_match(model_id, reason);
        }
    }
}

async fn relay_response_body_raw<S>(mut body_stream: S, send_stream: &mut SendStream) -> Result<()>
where
    S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
{
    while let Some(chunk) = body_stream
        .try_next()
        .await
        .context("failed to read upstream response body")?
    {
        send_stream
            .send_body(chunk)
            .await
            .context("failed to send response body chunk")?;
    }
    Ok(())
}

async fn relay_response_body_raw_h3<BodyStream, H3Stream>(
    mut body_stream: BodyStream,
    stream: &mut h3::server::RequestStream<H3Stream, bytes::Bytes>,
) -> Result<()>
where
    BodyStream: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
    H3Stream: h3::quic::SendStream<bytes::Bytes>,
{
    while let Some(chunk) = body_stream
        .try_next()
        .await
        .context("failed to read upstream response body")?
    {
        stream
            .send_data(chunk)
            .await
            .map_err(|error| anyhow::anyhow!("failed to send h3 response body: {error:?}"))?;
    }
    Ok(())
}

async fn relay_response_body_raw_webtransport<S>(
    mut body_stream: S,
    send_stream: &mut quinn::SendStream,
) -> Result<()>
where
    S: futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin,
{
    while let Some(chunk) = body_stream
        .try_next()
        .await
        .context("failed to read upstream response body")?
    {
        stargate_protocol::write_webtransport_http_body(send_stream, chunk)
            .await
            .context("failed to send WebTransport response body")?;
    }
    Ok(())
}

async fn read_h3_request_body<S>(
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    request_headers: &HeaderMap,
    max_request_body_bytes: usize,
) -> Result<Vec<u8>>
where
    S: h3::quic::RecvStream,
{
    let mut body_bytes = request_body_buffer(request_headers, max_request_body_bytes)?;
    let mut total_body = 0usize;
    while let Some(mut chunk) = stream
        .recv_data()
        .await
        .map_err(|error| anyhow::anyhow!("failed to read h3 request body: {error:?}"))?
    {
        total_body = next_body_len(total_body, chunk.remaining(), max_request_body_bytes)?;
        extend_body_from_buf(&mut body_bytes, &mut chunk);
    }
    Ok(body_bytes)
}

async fn read_webtransport_request_body(
    recv_stream: &mut quinn::RecvStream,
    request_headers: &HeaderMap,
    max_request_body_bytes: usize,
) -> Result<Vec<u8>> {
    let mut body_bytes = request_body_buffer(request_headers, max_request_body_bytes)?;
    let mut total_body = 0usize;
    while let Some(chunk) = stargate_protocol::read_webtransport_http_body_chunk(recv_stream)
        .await
        .context("failed to read WebTransport request body")?
    {
        total_body = next_body_len(total_body, chunk.len(), max_request_body_bytes)?;
        body_bytes.extend_from_slice(&chunk);
    }
    Ok(body_bytes)
}

fn request_body_buffer(
    request_headers: &HeaderMap,
    max_request_body_bytes: usize,
) -> Result<Vec<u8>> {
    let capacity = request_body_capacity(request_headers, max_request_body_bytes)?;
    Ok(Vec::with_capacity(capacity.unwrap_or(0)))
}

fn request_body_capacity(
    request_headers: &HeaderMap,
    max_request_body_bytes: usize,
) -> Result<Option<usize>> {
    let Some(value) = request_headers.get(reqwest::header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let Ok(value) = value.to_str() else {
        return Ok(None);
    };
    let Ok(content_length) = value.trim().parse::<usize>() else {
        return Ok(None);
    };
    ensure!(
        content_length <= max_request_body_bytes,
        "request body too large"
    );
    // Preallocate for honest small Content-Length values, but cap speculative
    // allocation so a legal large body cannot reserve tens of MiB up front.
    Ok(Some(
        content_length.min(MAX_SPECULATIVE_REQUEST_BODY_PREALLOC_BYTES),
    ))
}

fn next_body_len(current: usize, chunk_len: usize, max_request_body_bytes: usize) -> Result<usize> {
    let next = current
        .checked_add(chunk_len)
        .context("request body length overflowed")?;
    ensure!(next <= max_request_body_bytes, "request body too large");
    Ok(next)
}

fn extend_body_from_buf<B>(body_bytes: &mut Vec<u8>, chunk: &mut B)
where
    B: Buf,
{
    while chunk.has_remaining() {
        // Copy each contiguous slice directly out of the Buf; this avoids
        // materializing another Bytes value while still handling segmented Buf
        // implementations.
        let bytes = chunk.chunk();
        body_bytes.extend_from_slice(bytes);
        chunk.advance(bytes.len());
    }
}

async fn send_success_headers(
    send_stream: &mut SendStream,
    status: reqwest::StatusCode,
    response_headers: &HeaderMap,
    retry: &PylonRetryConfig,
    metrics: Option<&PylonMetrics>,
    inference_server_id: &str,
) -> Result<()> {
    let mut header_frame = build_response_headers(
        status,
        response_headers,
        retry,
        metrics,
        inference_server_id,
        false,
    )?;
    header_frame.insert(
        HeaderName::from_static("x-status"),
        HeaderValue::from_str(&status.as_u16().to_string()).context("invalid status code")?,
    );
    send_stream
        .send_header(header_frame)
        .await
        .context("failed to send response headers")
}

async fn send_h3_success_headers<S>(
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    status: reqwest::StatusCode,
    response_headers: &HeaderMap,
    retry: &PylonRetryConfig,
    metrics: Option<&PylonMetrics>,
    inference_server_id: &str,
) -> Result<()>
where
    S: h3::quic::SendStream<bytes::Bytes>,
{
    let headers = build_response_headers(
        status,
        response_headers,
        retry,
        metrics,
        inference_server_id,
        true,
    )?;
    let mut response = http::Response::builder()
        .status(status.as_u16())
        .body(())
        .context("build h3 response")?;
    for (name, value) in &headers {
        response.headers_mut().append(name, value.clone());
    }
    stream
        .send_response(response)
        .await
        .map_err(|error| anyhow::anyhow!("failed to send h3 response headers: {error:?}"))
}

async fn send_webtransport_success_headers(
    send_stream: &mut quinn::SendStream,
    status: reqwest::StatusCode,
    response_headers: &HeaderMap,
    retry: &PylonRetryConfig,
    metrics: Option<&PylonMetrics>,
    inference_server_id: &str,
) -> Result<()> {
    let headers = build_response_headers(
        status,
        response_headers,
        retry,
        metrics,
        inference_server_id,
        false,
    )?;
    let head = stargate_protocol::WebTransportHttpResponseHead { status, headers };
    stargate_protocol::write_webtransport_http_response_head(send_stream, &head)
        .await
        .context("failed to send WebTransport response head")
}

fn build_response_headers(
    status: reqwest::StatusCode,
    response_headers: &HeaderMap,
    retry: &PylonRetryConfig,
    metrics: Option<&PylonMetrics>,
    inference_server_id: &str,
    omit_content_length: bool,
) -> Result<HeaderMap> {
    let mut header_frame = HeaderMap::new();
    let classification = classify_upstream_response(status, response_headers, retry);
    tracing::info!(
        upstream.status = status.as_u16(),
        tunnel.retryable = classification.retryable,
        tunnel.retry_reason = classification.reason,
        upstream.retry_header_present = classification.upstream_retry_header_present,
        "classified upstream response"
    );
    if let Some(metrics) = metrics
        && !status.is_success()
    {
        if classification.retryable {
            metrics
                .retryable_responses_total(
                    inference_server_id,
                    classification.reason,
                    &status.as_u16().to_string(),
                )
                .inc();
        } else {
            metrics
                .nonretryable_failures_total(inference_server_id, classification.reason)
                .inc();
        }
    }

    if classification.retryable {
        header_frame.insert(
            HeaderName::from_static(HEADER_STARGATE_RETRYABLE),
            HeaderValue::from_static("true"),
        );
        header_frame.insert(
            HeaderName::from_static(HEADER_STARGATE_RETRY_REASON),
            HeaderValue::from_static(RETRY_REASON_UPSTREAM_ADMISSION_REJECTED),
        );
        if retry.propagate_retry_after
            && let Some(retry_after_ms) = retry_after_millis(response_headers)
        {
            header_frame.insert(
                HeaderName::from_static(HEADER_STARGATE_RETRY_AFTER_MS),
                HeaderValue::from_str(&retry_after_ms.to_string())
                    .context("invalid retry-after millis")?,
            );
        }
    }
    for (name, value) in response_headers {
        if should_forward_response_header(name, retry)
            && !(omit_content_length && name == reqwest::header::CONTENT_LENGTH)
        {
            header_frame.append(name, value.clone());
        }
    }
    Ok(header_frame)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UpstreamRetryClassification {
    retryable: bool,
    reason: &'static str,
    upstream_retry_header_present: bool,
}

fn classify_upstream_response(
    status: reqwest::StatusCode,
    response_headers: &HeaderMap,
    retry: &PylonRetryConfig,
) -> UpstreamRetryClassification {
    let upstream_retry_header_present = response_headers
        .get(&retry.upstream_retry_header)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let status_retryable = retry.retryable_upstream_status_codes.contains(&status);
    let retryable =
        status_retryable && (!retry.require_upstream_retry_header || upstream_retry_header_present);
    let reason = if retryable {
        RETRY_REASON_UPSTREAM_ADMISSION_REJECTED
    } else if status_retryable
        && retry.require_upstream_retry_header
        && !upstream_retry_header_present
    {
        "missing_upstream_retry_header"
    } else if !status.is_success() {
        "upstream_nonretryable_status"
    } else {
        ""
    };

    UpstreamRetryClassification {
        retryable,
        reason,
        upstream_retry_header_present,
    }
}

fn retry_after_millis(response_headers: &HeaderMap) -> Option<u64> {
    let value = response_headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return seconds.checked_mul(1000);
    }
    let retry_at = httpdate::parse_http_date(value).ok()?;
    let duration = retry_at
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    u64::try_from(duration.as_millis()).ok()
}

async fn send_error_response(
    send_stream: &mut SendStream,
    status: reqwest::StatusCode,
    message: String,
) -> Result<()> {
    let mut header_frame = HeaderMap::new();
    header_frame.insert(
        HeaderName::from_static("x-status"),
        HeaderValue::from_str(&status.as_u16().to_string()).context("invalid status code")?,
    );
    header_frame.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    send_stream
        .send_header(header_frame)
        .await
        .context("failed to send error response headers")?;
    let body = problem_details_body(status, message);
    send_stream
        .send_body(body.into_bytes().into())
        .await
        .context("failed to send error response body")?;
    send_stream
        .finish()
        .context("failed to finish error response stream")?;
    Ok(())
}

async fn send_h3_error_response<S>(
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    status: reqwest::StatusCode,
    message: String,
) -> Result<()>
where
    S: h3::quic::SendStream<bytes::Bytes>,
{
    let response = http::Response::builder()
        .status(status.as_u16())
        .header(
            reqwest::header::CONTENT_TYPE.as_str(),
            "application/problem+json",
        )
        .body(())
        .context("build h3 error response")?;
    stream
        .send_response(response)
        .await
        .map_err(|error| anyhow::anyhow!("failed to send h3 error response headers: {error:?}"))?;
    let body = problem_details_body(status, message);
    stream
        .send_data(bytes::Bytes::from(body))
        .await
        .map_err(|error| anyhow::anyhow!("failed to send h3 error response body: {error:?}"))?;
    stream
        .finish()
        .await
        .map_err(|error| anyhow::anyhow!("failed to finish h3 error response stream: {error:?}"))
}

async fn send_webtransport_error_response(
    send_stream: &mut quinn::SendStream,
    status: reqwest::StatusCode,
    message: String,
) -> Result<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    let head = stargate_protocol::WebTransportHttpResponseHead { status, headers };
    stargate_protocol::write_webtransport_http_response_head(send_stream, &head)
        .await
        .context("failed to send WebTransport error response head")?;
    let body = problem_details_body(status, message);
    stargate_protocol::write_webtransport_http_body(send_stream, bytes::Bytes::from(body))
        .await
        .context("failed to send WebTransport error response body")?;
    stargate_protocol::finish_webtransport_http_stream(send_stream)
        .context("failed to finish WebTransport error response stream")?;
    Ok(())
}

fn queue_mismatch_response_headers(
    app: &TunnelServerApp,
    decision: &QueueAdmissionDecision,
    include_custom_status: bool,
) -> Result<HeaderMap> {
    let status = reqwest::StatusCode::TOO_MANY_REQUESTS;
    if let Some(metrics) = app.metrics.as_deref() {
        metrics
            .retryable_responses_total(
                &app.inference_server_id,
                RETRY_REASON_QUEUE_ESTIMATE_MISMATCH,
                &status.as_u16().to_string(),
            )
            .inc();
    }

    let mut headers = HeaderMap::new();
    if include_custom_status {
        headers.insert(
            HeaderName::from_static("x-status"),
            HeaderValue::from_static("429"),
        );
    }
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    headers.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRYABLE),
        HeaderValue::from_static("true"),
    );
    headers.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRY_REASON),
        HeaderValue::from_static(RETRY_REASON_QUEUE_ESTIMATE_MISMATCH),
    );
    if let QueueAdmissionDecision::Rejected {
        retry_after_ms: Some(retry_after_ms),
        ..
    } = decision
    {
        headers.insert(
            HeaderName::from_static(HEADER_STARGATE_RETRY_AFTER_MS),
            HeaderValue::from_str(&retry_after_ms.to_string())
                .context("invalid queue mismatch retry-after millis")?,
        );
    }
    Ok(headers)
}

fn queue_mismatch_body(decision: &QueueAdmissionDecision) -> String {
    let (expected_ms, actual_ms, threshold_ms) = match decision {
        QueueAdmissionDecision::Rejected {
            expected_ms,
            actual_ms,
            threshold_ms,
            ..
        } => (*expected_ms, *actual_ms, *threshold_ms),
        _ => (0, 0, 0),
    };
    serde_json::json!({
        "type": "about:blank",
        "title": "Too Many Requests",
        "status": reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16(),
        "detail": "local queue estimate exceeded Stargate routing estimate",
        "reason": RETRY_REASON_QUEUE_ESTIMATE_MISMATCH,
        "expected_queue_ms": expected_ms,
        "actual_queue_ms": actual_ms,
        "threshold_ms": threshold_ms,
    })
    .to_string()
}

async fn send_queue_mismatch_response(
    send_stream: &mut SendStream,
    app: &TunnelServerApp,
    decision: &QueueAdmissionDecision,
) -> Result<()> {
    let headers = queue_mismatch_response_headers(app, decision, true)?;
    send_stream
        .send_header(headers)
        .await
        .context("failed to send queue mismatch response headers")?;
    send_stream
        .send_body(queue_mismatch_body(decision).into_bytes().into())
        .await
        .context("failed to send queue mismatch response body")?;
    send_stream
        .finish()
        .context("failed to finish queue mismatch response")?;
    Ok(())
}

async fn send_h3_queue_mismatch_response<S>(
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    app: &TunnelServerApp,
    decision: &QueueAdmissionDecision,
) -> Result<()>
where
    S: h3::quic::SendStream<bytes::Bytes>,
{
    let headers = queue_mismatch_response_headers(app, decision, false)?;
    let mut response = http::Response::builder()
        .status(reqwest::StatusCode::TOO_MANY_REQUESTS.as_u16())
        .body(())
        .context("build h3 queue mismatch response")?;
    for (name, value) in &headers {
        response.headers_mut().append(name, value.clone());
    }
    stream
        .send_response(response)
        .await
        .map_err(|error| anyhow::anyhow!("failed to send h3 queue mismatch headers: {error:?}"))?;
    stream
        .send_data(bytes::Bytes::from(queue_mismatch_body(decision)))
        .await
        .map_err(|error| anyhow::anyhow!("failed to send h3 queue mismatch body: {error:?}"))?;
    stream
        .finish()
        .await
        .map_err(|error| anyhow::anyhow!("failed to finish h3 queue mismatch response: {error:?}"))
}

async fn send_webtransport_queue_mismatch_response(
    send_stream: &mut quinn::SendStream,
    app: &TunnelServerApp,
    decision: &QueueAdmissionDecision,
) -> Result<()> {
    let headers = queue_mismatch_response_headers(app, decision, false)?;
    let head = stargate_protocol::WebTransportHttpResponseHead {
        status: reqwest::StatusCode::TOO_MANY_REQUESTS,
        headers,
    };
    stargate_protocol::write_webtransport_http_response_head(send_stream, &head)
        .await
        .context("failed to send WebTransport queue mismatch response head")?;
    stargate_protocol::write_webtransport_http_body(
        send_stream,
        bytes::Bytes::from(queue_mismatch_body(decision)),
    )
    .await
    .context("failed to send WebTransport queue mismatch response body")?;
    stargate_protocol::finish_webtransport_http_stream(send_stream)
        .context("failed to finish WebTransport queue mismatch response")?;
    Ok(())
}

async fn send_local_connect_failure_response(
    send_stream: &mut SendStream,
    app: &TunnelServerApp,
    error: &UpstreamRequestError,
    retryable: bool,
) -> Result<()> {
    tracing::warn!(
        inference_server_id = %app.inference_server_id,
        error = %error,
        retryable,
        "local upstream connection failed"
    );

    let status = reqwest::StatusCode::SERVICE_UNAVAILABLE;
    if let Some(metrics) = app.metrics.as_deref() {
        if retryable {
            metrics
                .retryable_responses_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                    &status.as_u16().to_string(),
                )
                .inc();
        } else {
            metrics
                .nonretryable_failures_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                )
                .inc();
        }
    }

    let mut header_frame = HeaderMap::new();
    header_frame.insert(
        HeaderName::from_static("x-status"),
        HeaderValue::from_static("503"),
    );
    header_frame.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    header_frame.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRYABLE),
        HeaderValue::from_static(if retryable { "true" } else { "false" }),
    );
    header_frame.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRY_REASON),
        HeaderValue::from_static(RETRY_REASON_LOCAL_CONNECT_FAILURE),
    );
    send_stream
        .send_header(header_frame)
        .await
        .context("failed to send local connect failure response headers")?;
    let body = problem_details_body(status, "local upstream connection failed");
    send_stream
        .send_body(body.into_bytes().into())
        .await
        .context("failed to send local connect failure response body")?;
    send_stream
        .finish()
        .context("failed to finish local connect failure response stream")?;
    Ok(())
}

async fn send_webtransport_local_connect_failure_response(
    send_stream: &mut quinn::SendStream,
    app: &TunnelServerApp,
    error: &UpstreamRequestError,
    retryable: bool,
) -> Result<()> {
    tracing::warn!(
        inference_server_id = %app.inference_server_id,
        error = %error,
        retryable,
        "local upstream connection failed"
    );

    let status = reqwest::StatusCode::SERVICE_UNAVAILABLE;
    if let Some(metrics) = app.metrics.as_deref() {
        if retryable {
            metrics
                .retryable_responses_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                    &status.as_u16().to_string(),
                )
                .inc();
        } else {
            metrics
                .nonretryable_failures_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                )
                .inc();
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        HeaderValue::from_static("application/problem+json"),
    );
    headers.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRYABLE),
        HeaderValue::from_static(if retryable { "true" } else { "false" }),
    );
    headers.insert(
        HeaderName::from_static(HEADER_STARGATE_RETRY_REASON),
        HeaderValue::from_static(RETRY_REASON_LOCAL_CONNECT_FAILURE),
    );
    let head = stargate_protocol::WebTransportHttpResponseHead { status, headers };
    stargate_protocol::write_webtransport_http_response_head(send_stream, &head)
        .await
        .context("failed to send WebTransport local connect failure response head")?;
    let body = problem_details_body(status, "local upstream connection failed");
    stargate_protocol::write_webtransport_http_body(send_stream, bytes::Bytes::from(body))
        .await
        .context("failed to send WebTransport local connect failure response body")?;
    stargate_protocol::finish_webtransport_http_stream(send_stream)
        .context("failed to finish WebTransport local connect failure response stream")?;
    Ok(())
}

async fn send_h3_local_connect_failure_response<S>(
    stream: &mut h3::server::RequestStream<S, bytes::Bytes>,
    app: &TunnelServerApp,
    error: &UpstreamRequestError,
    retryable: bool,
) -> Result<()>
where
    S: h3::quic::SendStream<bytes::Bytes>,
{
    tracing::warn!(
        inference_server_id = %app.inference_server_id,
        error = %error,
        retryable,
        "local upstream connection failed"
    );

    let status = reqwest::StatusCode::SERVICE_UNAVAILABLE;
    if let Some(metrics) = app.metrics.as_deref() {
        if retryable {
            metrics
                .retryable_responses_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                    &status.as_u16().to_string(),
                )
                .inc();
        } else {
            metrics
                .nonretryable_failures_total(
                    &app.inference_server_id,
                    RETRY_REASON_LOCAL_CONNECT_FAILURE,
                )
                .inc();
        }
    }

    let mut response = http::Response::builder()
        .status(status.as_u16())
        .header(
            reqwest::header::CONTENT_TYPE.as_str(),
            "application/problem+json",
        )
        .body(())
        .context("build h3 local connect failure response")?;
    response.headers_mut().insert(
        HeaderName::from_static(HEADER_STARGATE_RETRYABLE),
        HeaderValue::from_static(if retryable { "true" } else { "false" }),
    );
    response.headers_mut().insert(
        HeaderName::from_static(HEADER_STARGATE_RETRY_REASON),
        HeaderValue::from_static(RETRY_REASON_LOCAL_CONNECT_FAILURE),
    );
    stream.send_response(response).await.map_err(|error| {
        anyhow::anyhow!("failed to send h3 local connect failure response headers: {error:?}")
    })?;
    let body = problem_details_body(status, "local upstream connection failed");
    stream
        .send_data(bytes::Bytes::from(body))
        .await
        .map_err(|error| {
            anyhow::anyhow!("failed to send h3 local connect failure response body: {error:?}")
        })?;
    stream.finish().await.map_err(|error| {
        anyhow::anyhow!("failed to finish h3 local connect failure response stream: {error:?}")
    })
}

fn problem_details_body(status: reqwest::StatusCode, detail: impl Into<String>) -> String {
    serde_json::json!({
        "type": "about:blank",
        "title": status.canonical_reason().unwrap_or("Error"),
        "status": status.as_u16(),
        "detail": detail.into(),
    })
    .to_string()
}

fn make_server_config(
    cert_pem: Option<&[u8]>,
    key_pem: Option<&[u8]>,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<quinn::ServerConfig> {
    let (cert_owned, key_owned);
    let (cert_data, key_data) = match (cert_pem, key_pem) {
        (Some(c), Some(k)) => (c, k),
        _ => {
            tracing::info!("no TLS cert/key provided, generating self-signed certificate");
            let (c, k) = stargate_tls::generate_self_signed_cert()?;
            cert_owned = c;
            key_owned = k;
            (cert_owned.as_slice(), key_owned.as_slice())
        }
    };
    let cert_chain: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut &*cert_data)
            .collect::<std::result::Result<_, _>>()
            .context("failed to parse cert PEM")?;
    let key = rustls_pemfile::private_key(&mut &*key_data)
        .context("failed to parse key PEM")?
        .context("no private key found in PEM")?;
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build quic TLS server config failed")?;
    tls_config.alpn_protocols = tunnel_protocol.alpn_protocols();
    Ok(quinn::ServerConfig::with_crypto(std::sync::Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .context("build quic server config failed")?,
    )))
}

fn join_base_path(base: &str, path_and_query: &str) -> Result<url::Url> {
    let base = url::Url::parse(base).context("invalid upstream_http_base_url")?;
    let pq = if path_and_query.starts_with('/') {
        path_and_query.to_string()
    } else {
        format!("/{path_and_query}")
    };
    let joined = base.join(&pq).context("join upstream path failed")?;
    Ok(joined)
}

fn should_forward_header(name: &HeaderName) -> bool {
    // `HeaderName` is normalized by http/reqwest, so `as_str()` gives a stable
    // lowercase key without allocating on the header-forwarding hot path.
    if name.as_str() == HEADER_STARGATE_EXPECTED_QUEUE_MS {
        return false;
    }
    !matches!(
        name.as_str(),
        "connection"
            | "proxy-connection"
            | "keep-alive"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "host"
            | "x-method"
            | "x-path"
    )
}

fn should_forward_response_header(name: &HeaderName, retry: &PylonRetryConfig) -> bool {
    if name == retry.upstream_retry_header {
        return false;
    }
    // Keep response filtering allocation-free; this runs for every upstream
    // response header before the frame is written back through the tunnel.
    let name = name.as_str();
    if name.starts_with(crate::request_observer::ENGINE_STAT_HEADER_PREFIX) {
        return false;
    }
    !matches!(
        name,
        "connection"
            | "proxy-connection"
            | "keep-alive"
            | "transfer-encoding"
            | "te"
            | "trailer"
            | "upgrade"
            | "content-length"
            | HEADER_STARGATE_UPSTREAM_RETRYABLE
            | HEADER_STARGATE_RETRYABLE
            | HEADER_STARGATE_RETRY_REASON
            | HEADER_STARGATE_RETRY_AFTER_MS
    )
}

#[derive(Clone, Debug)]
pub struct ReverseQuicTunnelConfig {
    pub target_addr: String,
    pub inference_server_id: String,
    pub upstream_http_base_url: String,
    pub max_request_body_bytes: usize,
    pub first_output_timeout: Duration,
    pub output_chunk_timeout: Duration,
    pub output_token_parser_factory: OutputTokenParserFactory,
    pub tls_cert_pem: Option<Vec<u8>>,
    pub quic_insecure: bool,
    pub tunnel_protocol: TunnelTransportProtocol,
    pub request_observation_tx: Option<flume::Sender<RequestObservation>>,
    pub request_quality_monitor: RequestQualityMonitorConfig,
    pub sni_override: Option<String>,
    pub auth_token_provider: Option<std::sync::Arc<crate::AuthTokenProvider>>,
    pub retry: PylonRetryConfig,
    pub queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    pub queue_tracker: QueueAdmissionTracker,
    pub metrics: Option<Arc<PylonMetrics>>,
}

impl ReverseQuicTunnelConfig {
    pub fn new(
        target_addr: String,
        inference_server_id: String,
        upstream_http_base_url: String,
    ) -> Self {
        Self {
            target_addr,
            inference_server_id,
            upstream_http_base_url,
            max_request_body_bytes: DEFAULT_MAX_BODY_BYTES,
            first_output_timeout: DEFAULT_FIRST_OUTPUT_TIMEOUT,
            output_chunk_timeout: DEFAULT_OUTPUT_CHUNK_TIMEOUT,
            output_token_parser_factory: OutputTokenParserFactory,
            tls_cert_pem: None,
            quic_insecure: false,
            tunnel_protocol: TunnelTransportProtocol::Custom,
            request_observation_tx: None,
            request_quality_monitor: RequestQualityMonitorConfig::default(),
            sni_override: None,
            auth_token_provider: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            metrics: None,
        }
    }
}

pub struct ReverseQuicTunnelHandle {
    shutdown: CancellationToken,
    task_tracker: TaskTracker,
}

impl ReverseQuicTunnelHandle {
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    pub async fn closed(&self) {
        self.task_tracker.wait().await;
    }
}

impl Drop for ReverseQuicTunnelHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

pub async fn start_reverse_quic_tunnel(
    config: ReverseQuicTunnelConfig,
) -> Result<ReverseQuicTunnelHandle, TunnelError> {
    ensure_rustls_provider();
    if config.tunnel_protocol == TunnelTransportProtocol::WebTransport {
        return start_reverse_webtransport_tunnel(config).await;
    }
    let client_config = build_trusted_client_config(
        config.tls_cert_pem.as_deref(),
        config.quic_insecure,
        config.tunnel_protocol,
    )
    .map_err(|e| TunnelError::Tls(e.to_string()))?;
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(TunnelError::Bind)?;
    endpoint.set_default_client_config(client_config);

    let resolved_addrs: Vec<_> = tokio::net::lookup_host(config.target_addr.as_str())
        .await
        .map_err(|e| TunnelError::Connect(e.to_string()))?
        .collect();
    let resolved_target = resolved_addrs
        .iter()
        .find(|addr| addr.is_ipv4())
        .copied()
        .or_else(|| resolved_addrs.first().copied())
        .ok_or_else(|| TunnelError::Connect("no resolved reverse tunnel address".to_string()))?;

    let sni = config
        .sni_override
        .as_deref()
        .map(String::from)
        .unwrap_or_else(|| derive_sni(&config.target_addr));
    let connection = endpoint
        .connect(resolved_target, &sni)
        .map_err(|e| TunnelError::Connect(e.to_string()))?
        .await
        .map_err(|e| TunnelError::Connect(e.to_string()))?;

    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|e| TunnelError::Handshake(e.to_string()))?;

    let auth_token = if let Some(provider) = &config.auth_token_provider {
        Some(
            provider
                .get_token()
                .await
                .map_err(|e| TunnelError::Handshake(format!("failed to read auth token: {e}")))?,
        )
    } else {
        None
    };

    let handshake_request = stargate_protocol::HandshakeRequest {
        inference_server_id: config.inference_server_id.clone(),
        auth_token,
    };
    stargate_protocol::write_handshake(&mut send, &handshake_request)
        .await
        .map_err(|e| TunnelError::Handshake(e.to_string()))?;
    send.finish()
        .map_err(|e| TunnelError::Handshake(e.to_string()))?;

    let ack = stargate_protocol::read_handshake_ack(&mut recv)
        .await
        .map_err(|e| TunnelError::Handshake(e.to_string()))?;
    if !ack.accepted {
        return Err(TunnelError::Handshake(format!(
            "stargate rejected reverse tunnel handshake: {}",
            ack.reason
        )));
    }

    let tunnel_protocol = config.tunnel_protocol;
    let shutdown = CancellationToken::new();
    let task_tracker = TaskTracker::new();

    let app = TunnelServerApp {
        http_client: reqwest::Client::new(),
        inference_server_id: config.inference_server_id,
        upstream_http_base_url: config.upstream_http_base_url,
        max_request_body_bytes: config.max_request_body_bytes,
        first_output_timeout: config.first_output_timeout,
        output_chunk_timeout: config.output_chunk_timeout,
        output_token_parser_factory: config.output_token_parser_factory,
        request_observation_tx: config.request_observation_tx,
        request_quality_monitor: config.request_quality_monitor,
        retry: config.retry,
        queue_mismatch_retry: config.queue_mismatch_retry,
        queue_tracker: config.queue_tracker,
        metrics: config.metrics,
    };

    let shutdown_for_task = shutdown.clone();
    let stream_tracker = task_tracker.clone();
    task_tracker.spawn(async move {
        let _endpoint = endpoint;
        match tunnel_protocol {
            TunnelTransportProtocol::Custom => loop {
                tokio::select! {
                    _ = shutdown_for_task.cancelled() => break,
                    stream = connection.accept_bi() => {
                        let Ok((quinn_send, quinn_recv)) = stream else { break };
                        let app = app.clone();
                        stream_tracker.spawn(async move {
                            if let Err(error) = handle_stream(quinn_send, quinn_recv, &app).await {
                                tracing::warn!(error = %error, "reverse tunnel stream failed");
                            }
                        });
                    }
                }
            },
            TunnelTransportProtocol::Http3 => {
                if let Err(error) = handle_h3_established_connection(
                    connection,
                    shutdown_for_task,
                    stream_tracker,
                    app,
                )
                .await
                {
                    tracing::warn!(error = %error, "reverse h3 tunnel connection failed");
                }
            }
            TunnelTransportProtocol::WebTransport => unreachable!(
                "WebTransport reverse tunnels are handled before the legacy handshake path"
            ),
        }
    });
    task_tracker.close();

    Ok(ReverseQuicTunnelHandle {
        shutdown,
        task_tracker,
    })
}

async fn start_reverse_webtransport_tunnel(
    config: ReverseQuicTunnelConfig,
) -> Result<ReverseQuicTunnelHandle, TunnelError> {
    let client_config = build_trusted_client_config(
        config.tls_cert_pem.as_deref(),
        config.quic_insecure,
        config.tunnel_protocol,
    )
    .map_err(|e| TunnelError::Tls(e.to_string()))?;
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).map_err(TunnelError::Bind)?;
    endpoint.set_default_client_config(client_config);

    let resolved_addrs: Vec<_> = tokio::net::lookup_host(config.target_addr.as_str())
        .await
        .map_err(|e| TunnelError::Connect(e.to_string()))?
        .collect();
    let resolved_target = resolved_addrs
        .iter()
        .find(|addr| addr.is_ipv4())
        .copied()
        .or_else(|| resolved_addrs.first().copied())
        .ok_or_else(|| TunnelError::Connect("no resolved reverse tunnel address".to_string()))?;

    let sni = config
        .sni_override
        .as_deref()
        .map(String::from)
        .unwrap_or_else(|| derive_sni(&config.target_addr));
    let connection = endpoint
        .connect(resolved_target, &sni)
        .map_err(|e| TunnelError::Connect(e.to_string()))?
        .await
        .map_err(|e| TunnelError::Connect(e.to_string()))?;

    let auth_token = if let Some(provider) = &config.auth_token_provider {
        Some(
            provider
                .get_token()
                .await
                .map_err(|e| TunnelError::Handshake(format!("failed to read auth token: {e}")))?,
        )
    } else {
        None
    };

    let mut builder = h3::client::builder();
    builder.enable_extended_connect(true).enable_datagram(true);
    let (h3_connection, mut send_request): (
        h3::client::Connection<h3_quinn::Connection, bytes::Bytes>,
        h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    ) = builder
        .build(h3_quinn::Connection::new(connection.clone()))
        .await
        .map_err(|error| TunnelError::Handshake(format!("create h3 client: {error:?}")))?;
    let mut request: http::Request<()> = http::Request::builder()
        .method(reqwest::Method::CONNECT.as_str())
        .uri(format!(
            "https://{}{WEBTRANSPORT_TUNNEL_PATH}",
            target_authority(&config.target_addr)
        ))
        .header(
            HEADER_INFERENCE_SERVER_ID,
            config.inference_server_id.as_str(),
        )
        .body(())
        .map_err(|error| TunnelError::Handshake(format!("build CONNECT request: {error}")))?;
    if let Some(token) = &auth_token {
        request.headers_mut().insert(
            HeaderName::from_static(HEADER_REVERSE_AUTH_TOKEN),
            HeaderValue::from_str(token)
                .map_err(|error| TunnelError::Handshake(format!("invalid auth token: {error}")))?,
        );
    }
    request
        .extensions_mut()
        .insert(h3::ext::Protocol::WEB_TRANSPORT);
    let mut connect_stream = send_request
        .send_request(request)
        .await
        .map_err(|error| TunnelError::Handshake(format!("send CONNECT request: {error:?}")))?;
    let session_id = connect_stream.id().into_inner();
    connect_stream
        .finish()
        .await
        .map_err(|error| TunnelError::Handshake(format!("finish CONNECT request: {error:?}")))?;
    let response = connect_stream
        .recv_response()
        .await
        .map_err(|error| TunnelError::Handshake(format!("receive CONNECT response: {error:?}")))?;
    if !response.status().is_success() {
        return Err(TunnelError::Handshake(format!(
            "stargate rejected reverse WebTransport CONNECT with status {}",
            response.status()
        )));
    }

    let shutdown = CancellationToken::new();
    let task_tracker = TaskTracker::new();
    let app = TunnelServerApp {
        http_client: reqwest::Client::new(),
        inference_server_id: config.inference_server_id,
        upstream_http_base_url: config.upstream_http_base_url,
        max_request_body_bytes: config.max_request_body_bytes,
        first_output_timeout: config.first_output_timeout,
        output_chunk_timeout: config.output_chunk_timeout,
        output_token_parser_factory: config.output_token_parser_factory,
        request_observation_tx: config.request_observation_tx,
        request_quality_monitor: config.request_quality_monitor,
        retry: config.retry,
        queue_mismatch_retry: config.queue_mismatch_retry,
        queue_tracker: config.queue_tracker,
        metrics: config.metrics,
    };

    let shutdown_for_task = shutdown.clone();
    let stream_tracker = task_tracker.clone();
    task_tracker.spawn(async move {
        let _endpoint = endpoint;
        let _h3_connection = h3_connection;
        let _connect_stream = connect_stream;
        loop {
            tokio::select! {
                _ = shutdown_for_task.cancelled() => break,
                stream = connection.accept_bi() => {
                    let Ok((quinn_send, quinn_recv)) = stream else { break };
                    let app = app.clone();
                    stream_tracker.spawn(async move {
                        if let Err(error) =
                            handle_webtransport_stream(
                                quinn_send,
                                quinn_recv,
                                session_id,
                                app,
                            )
                            .await
                        {
                            tracing::warn!(error = %error, "reverse WebTransport stream failed");
                        }
                    });
                }
            }
        }
    });
    task_tracker.close();

    Ok(ReverseQuicTunnelHandle {
        shutdown,
        task_tracker,
    })
}

async fn handle_webtransport_stream(
    mut quinn_send: quinn::SendStream,
    mut quinn_recv: quinn::RecvStream,
    expected_session_id: u64,
    app: TunnelServerApp,
) -> Result<()> {
    let stream_session_id = match tokio::time::timeout(
        WEBTRANSPORT_STREAM_HEADER_TIMEOUT,
        stargate_protocol::read_webtransport_bidi_header(&mut quinn_recv),
    )
    .await
    {
        Ok(Ok(session_id)) => session_id,
        Ok(Err(error)) => {
            reset_webtransport_stream(&mut quinn_send, &mut quinn_recv);
            return Err(error).context("invalid WebTransport stream header");
        }
        Err(_) => {
            reset_webtransport_stream(&mut quinn_send, &mut quinn_recv);
            bail!("timed out waiting for WebTransport stream header");
        }
    };
    if stream_session_id != expected_session_id {
        reset_webtransport_stream(&mut quinn_send, &mut quinn_recv);
        bail!(
            "WebTransport stream session id mismatch: got {stream_session_id}, expected {expected_session_id}"
        );
    }

    handle_webtransport_http_stream(quinn_send, quinn_recv, &app).await
}

fn reset_webtransport_stream(
    quinn_send: &mut quinn::SendStream,
    quinn_recv: &mut quinn::RecvStream,
) {
    let _ = quinn_send.reset(0u32.into());
    let _ = quinn_recv.stop(0u32.into());
}

fn build_trusted_client_config(
    cert_pem: Option<&[u8]>,
    insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<quinn::ClientConfig> {
    if insecure {
        return stargate_tls::build_insecure_quic_client_config_with_alpn(
            tunnel_protocol.alpn_protocols(),
        );
    }
    let cert_data = cert_pem.context("TLS cert required when --quic-insecure is not set")?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &*cert_data) {
        roots
            .add(cert.context("failed to parse cert PEM")?)
            .context("failed to add cert to root store")?;
    }
    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = tunnel_protocol.alpn_protocols();
    Ok(quinn::ClientConfig::new(std::sync::Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::extract::Request;
    use axum::http::{HeaderName, HeaderValue, StatusCode};
    use axum::response::sse::Event;
    use axum::response::{IntoResponse, Response};
    use axum::routing::post;
    use axum::{Json, Router, body::Body};
    use bytes::{Buf, Bytes};
    use futures::future;
    use opentelemetry::trace::TraceContextExt;
    use prometheus::{Encoder, TextEncoder};
    use quinn::ClientConfig;
    use tokio::net::TcpListener;

    use crate::{StatsCollectorConfig, request_observation_channel, start_stats_collector};

    type TestWebTransportConnectStream = h3::client::RequestStream<
        <h3_quinn::OpenStreams as h3::quic::OpenStreams<Bytes>>::BidiStream,
        Bytes,
    >;

    struct DirectWebTransportSession {
        _endpoint: Endpoint,
        connection: quinn::Connection,
        _h3_connection: h3::client::Connection<h3_quinn::Connection, Bytes>,
        _connect_stream: TestWebTransportConnectStream,
        session_id: u64,
    }

    #[test]
    fn engine_stat_response_headers_are_never_forwarded() {
        let retry = PylonRetryConfig::default();

        assert!(!should_forward_response_header(
            &HeaderName::from_static("x-pylon-engine-stat-input-tokens-processed"),
            &retry,
        ));
        assert!(!should_forward_response_header(
            &HeaderName::from_static("x-pylon-engine-stat-output-tokens-generated"),
            &retry,
        ));
        assert!(should_forward_response_header(
            &HeaderName::from_static("x-kv-cache-hit"),
            &retry,
        ));
    }

    #[test]
    fn pylon_request_header_filter_strips_tunnel_headers_case_insensitively()
    -> std::result::Result<(), reqwest::header::InvalidHeaderName> {
        assert!(!should_forward_header(&HeaderName::from_bytes(
            b"Connection"
        )?));
        assert!(!should_forward_header(&HeaderName::from_bytes(
            b"Proxy-Connection"
        )?));
        assert!(!should_forward_header(&HeaderName::from_bytes(b"Host")?));
        assert!(!should_forward_header(&HeaderName::from_bytes(
            b"X-Method"
        )?));
        assert!(!should_forward_header(&HeaderName::from_bytes(b"X-Path")?));
        assert!(!should_forward_header(&HeaderName::from_bytes(
            b"X-Stargate-Expected-Queue-Ms"
        )?));
        assert!(should_forward_header(&HeaderName::from_bytes(
            b"X-Request-Id"
        )?));
        Ok(())
    }

    #[test]
    fn pylon_trace_context_extracts_remote_parent() -> Result<()> {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        let span_context = pylon_upstream_parent_context(&headers)
            .span()
            .span_context()
            .clone();

        assert!(span_context.is_valid());
        assert!(span_context.is_remote());
        assert_eq!(
            span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span_context.span_id().to_string(), "00f067aa0ba902b7");
        Ok(())
    }

    #[test]
    fn pylon_otel_parent_attribute_uses_traceparent_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("traceparent"),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );

        assert_eq!(
            otel_parent_from_headers(&headers),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }

    #[test]
    fn pylon_response_header_filter_strips_internal_headers_case_insensitively()
    -> std::result::Result<(), reqwest::header::InvalidHeaderName> {
        let retry = PylonRetryConfig::default();

        assert!(!should_forward_response_header(
            &HeaderName::from_bytes(b"Connection")?,
            &retry,
        ));
        assert!(!should_forward_response_header(
            &HeaderName::from_bytes(b"X-Pylon-Engine-Stat-Input-Tokens-Processed")?,
            &retry,
        ));
        assert!(!should_forward_response_header(
            &HeaderName::from_bytes(b"X-Stargate-Retryable")?,
            &retry,
        ));
        assert!(should_forward_response_header(
            &HeaderName::from_bytes(b"X-Kv-Cache-Hit")?,
            &retry,
        ));
        Ok(())
    }

    #[test]
    fn request_body_buffer_uses_valid_declared_content_length() -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_LENGTH, "4096".parse()?);

        let body = request_body_buffer(&headers, 8192)?;

        assert_eq!(body.len(), 0);
        assert!(body.capacity() >= 4096);
        Ok(())
    }

    #[test]
    fn request_body_buffer_caps_large_valid_declared_content_length() -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_LENGTH, "1048576".parse()?);

        let capacity = request_body_capacity(&headers, 2 * 1024 * 1024)?;

        assert_eq!(capacity, Some(MAX_SPECULATIVE_REQUEST_BODY_PREALLOC_BYTES));
        Ok(())
    }

    #[test]
    fn request_body_buffer_rejects_declared_length_above_limit() -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_LENGTH, "4097".parse()?);

        let Err(error) = request_body_buffer(&headers, 4096) else {
            panic!("oversized content-length should fail");
        };

        assert!(error.to_string().contains("request body too large"));
        Ok(())
    }

    #[test]
    fn request_body_buffer_ignores_invalid_content_length() -> Result<()> {
        let mut headers = HeaderMap::new();
        headers.insert(reqwest::header::CONTENT_LENGTH, "not-a-number".parse()?);

        let body = request_body_buffer(&headers, 4096)?;

        assert_eq!(body.len(), 0);
        assert_eq!(body.capacity(), 0);
        Ok(())
    }

    #[test]
    fn extend_body_from_buf_copies_and_consumes_buffer() {
        let mut body = Vec::with_capacity(5);
        let mut chunk = Bytes::from_static(b"hello");

        extend_body_from_buf(&mut body, &mut chunk);

        assert_eq!(body, b"hello");
        assert!(!chunk.has_remaining());
    }

    fn metrics_text(metrics: &PylonMetrics) -> String {
        let metric_families = metrics.registry().gather();
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .expect("encode metrics");
        String::from_utf8(buffer).expect("metrics should be utf8")
    }

    async fn read_response_text(recv: &mut stargate_protocol::RecvStream) -> String {
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        String::from_utf8(response_body).unwrap()
    }

    fn queue_mismatch_request_headers(request_id: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", request_id.parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("x-stargate-expected-queue-ms", "0".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        headers
    }

    async fn start_queue_mismatch_test_tunnel(
        tunnel_protocol: TunnelTransportProtocol,
        enabled: bool,
    ) -> (
        QuicHttpTunnelHandle,
        Arc<AtomicUsize>,
        QueueAdmissionTracker,
        QueueTrackedRequestGuard,
        Arc<PylonMetrics>,
    ) {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let upstream_hits = Arc::new(AtomicUsize::new(0));
        let upstream_hits_for_app = upstream_hits.clone();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| {
                let upstream_hits = upstream_hits_for_app.clone();
                async move {
                    upstream_hits.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::OK, "forwarded")
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.tunnel_protocol = tunnel_protocol;
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.queue_mismatch_retry.enabled = enabled;
        config.queue_mismatch_retry.retry_after_ms = Some(125);
        config
            .queue_tracker
            .update_model_throughput("model-a", 100.0);
        let queue_tracker = config.queue_tracker.clone();
        let queued_request = config.queue_tracker.track_request(&RequiredTunnelHeaders {
            request_id: "req-already-queued".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            accepted_at: std::time::Instant::now(),
        });

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        (
            tunnel,
            upstream_hits,
            queue_tracker,
            queued_request,
            metrics,
        )
    }

    async fn send_custom_quic_json_request(
        tunnel_addr: SocketAddr,
        headers: HeaderMap,
        body: &'static [u8],
    ) -> (HeaderMap, Vec<u8>) {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(body)).await.unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        connection.close(0u32.into(), b"test complete");
        (response_headers, response_body)
    }

    fn assert_problem_response(
        response_headers: &HeaderMap,
        response_text: &str,
        status: u16,
        title: &str,
        detail: &str,
    ) {
        assert_eq!(
            response_headers
                .get(reqwest::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/problem+json"
        );
        let problem: serde_json::Value = serde_json::from_str(response_text).unwrap();
        assert_eq!(problem["type"], "about:blank");
        assert_eq!(problem["title"], title);
        assert_eq!(problem["status"], status);
        assert_eq!(problem["detail"], detail);
    }

    #[test]
    fn health_request_path_accepts_query_string() {
        assert!(is_health_request_path("/health"));
        assert!(is_health_request_path("/health?probe=1"));
        assert!(!is_health_request_path("/healthz"));
    }

    async fn open_test_tunnel_stream(
        tunnel_addr: SocketAddr,
    ) -> (
        Endpoint,
        stargate_protocol::SendStream,
        stargate_protocol::RecvStream,
    ) {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();
        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        (
            endpoint,
            stargate_protocol::SendStream::new(quinn_send),
            stargate_protocol::RecvStream::new(quinn_recv),
        )
    }

    async fn negotiate_alpn(
        client_config: ClientConfig,
        server_config: quinn::ServerConfig,
    ) -> Option<Vec<u8>> {
        let server_endpoint =
            Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server_endpoint.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let incoming = server_endpoint.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let protocol = connection
                .handshake_data()
                .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
                .and_then(|data| data.protocol);
            connection.close(0u32.into(), b"test complete");
            protocol
        });

        let mut client_endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client_endpoint.set_default_client_config(client_config);
        let connection = client_endpoint
            .connect(server_addr, "stargate")
            .unwrap()
            .await
            .unwrap();
        connection.close(0u32.into(), b"test complete");
        server_task.await.unwrap()
    }

    #[tokio::test]
    async fn custom_tunnel_tls_configs_do_not_negotiate_alpn() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let client_config =
            build_trusted_client_config(None, true, TunnelTransportProtocol::Custom)
                .expect("client config");
        let server_config =
            make_server_config(None, None, TunnelTransportProtocol::Custom).expect("server config");

        assert_eq!(negotiate_alpn(client_config, server_config).await, None);
    }

    #[tokio::test]
    async fn http3_tunnel_tls_configs_negotiate_h3_alpn() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let client_config = build_trusted_client_config(None, true, TunnelTransportProtocol::Http3)
            .expect("client config");
        let server_config =
            make_server_config(None, None, TunnelTransportProtocol::Http3).expect("server config");

        assert_eq!(
            negotiate_alpn(client_config, server_config).await,
            Some(b"h3".to_vec())
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http3_direct_tunnel_accepts_responses_request_to_upstream() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/responses",
            post(|req: Request| async move {
                let model = req
                    .headers()
                    .get("x-model")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("missing")
                    .to_string();
                let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                (
                    StatusCode::OK,
                    [(reqwest::header::CONTENT_TYPE.as_str(), "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{upstream_addr}"),
        );
        config.tunnel_protocol = TunnelTransportProtocol::Http3;
        let tunnel = start_quic_http_tunnel(config).await.unwrap();

        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(
                stargate_tls::build_insecure_quic_client_config_with_alpn(
                    TunnelTransportProtocol::Http3.alpn_protocols(),
                )
                .unwrap(),
            );
            let connection = endpoint
                .connect(tunnel.listen_addr(), "stargate")
                .unwrap()
                .await
                .unwrap();
            let (mut driver, mut send_request) = h3::client::builder()
                .build(h3_quinn::Connection::new(connection.clone()))
                .await
                .unwrap();
            let mut driver_task =
                tokio::spawn(async move { future::poll_fn(|cx| driver.poll_close(cx)).await });

            let uri: http::Uri = format!(
                "https://stargate:{}/v1/responses?source=http3",
                tunnel.listen_addr().port()
            )
            .parse()
            .unwrap();
            let request = http::Request::builder()
                .method(http::Method::POST)
                .uri(uri)
                .header("x-request-id", "req-h3-direct")
                .header("x-model", "model-h3")
                .header("x-input-tokens", "7")
                .header(reqwest::header::CONTENT_TYPE.as_str(), "application/json")
                .body(())
                .unwrap();
            let mut stream = send_request.send_request(request).await.unwrap();
            stream
                .send_data(Bytes::from_static(br#"{"input":"hi","stream":true}"#))
                .await
                .unwrap();
            stream.finish().await.unwrap();

            let response = stream.recv_response().await.unwrap();
            let mut body = Vec::new();
            while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                while chunk.has_remaining() {
                    let len = chunk.remaining();
                    body.extend_from_slice(&chunk.copy_to_bytes(len));
                }
            }

            connection.close(0u32.into(), b"test complete");
            if tokio::time::timeout(Duration::from_secs(1), &mut driver_task)
                .await
                .is_err()
            {
                driver_task.abort();
            }
            (response.status(), String::from_utf8(body).unwrap())
        })
        .await
        .expect("h3 request timed out");

        assert_eq!(result.0, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_str(&result.1).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("model-h3")
        );
        assert_eq!(
            payload.get("body_len").and_then(serde_json::Value::as_u64),
            Some(28)
        );

        tunnel.shutdown().await;
    }

    async fn open_direct_webtransport_session(
        tunnel_addr: SocketAddr,
    ) -> DirectWebTransportSession {
        let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(
            stargate_tls::build_insecure_quic_client_config_with_alpn(
                TunnelTransportProtocol::WebTransport.alpn_protocols(),
            )
            .unwrap(),
        );
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();
        let mut builder = h3::client::builder();
        builder.enable_extended_connect(true).enable_datagram(true);
        let (h3_connection, mut send_request): (
            h3::client::Connection<h3_quinn::Connection, Bytes>,
            h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
        ) = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .unwrap();
        let mut request: http::Request<()> = http::Request::builder()
            .method(http::Method::CONNECT)
            .uri(format!("https://stargate{WEBTRANSPORT_TUNNEL_PATH}"))
            .body(())
            .unwrap();
        request
            .extensions_mut()
            .insert(h3::ext::Protocol::WEB_TRANSPORT);
        let mut connect_stream = send_request.send_request(request).await.unwrap();
        let session_id = connect_stream.id().into_inner();
        connect_stream.finish().await.unwrap();
        let response = connect_stream.recv_response().await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        DirectWebTransportSession {
            _endpoint: endpoint,
            connection,
            _h3_connection: h3_connection,
            _connect_stream: connect_stream,
            session_id,
        }
    }

    async fn send_direct_webtransport_json_request(
        session: &DirectWebTransportSession,
        path: &str,
        model: &str,
        request_id: &str,
        body: &'static [u8],
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        let mut headers = HeaderMap::new();
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", model.parse().unwrap());
        headers.insert("x-request-id", request_id.parse().unwrap());
        headers.insert("x-input-tokens", "2".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send_direct_webtransport_request_with_headers(session, path, headers, body).await
    }

    async fn send_direct_webtransport_request_with_headers(
        session: &DirectWebTransportSession,
        path: &str,
        headers: HeaderMap,
        body: &'static [u8],
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        let (mut quinn_send, quinn_recv) = session.connection.open_bi().await.unwrap();
        let request_head = stargate_protocol::WebTransportHttpRequestHead {
            method: reqwest::Method::POST,
            path_and_query: path.to_string(),
            headers,
        };
        let bidi_header = stargate_protocol::WebTransportBidiHeader::new(session.session_id)
            .unwrap()
            .to_bytes();
        stargate_protocol::write_webtransport_http_request_head_after_prefix(
            &mut quinn_send,
            bidi_header,
            &request_head,
        )
        .await
        .unwrap();
        stargate_protocol::write_webtransport_http_body(
            &mut quinn_send,
            bytes::Bytes::from_static(body),
        )
        .await
        .unwrap();
        stargate_protocol::finish_webtransport_http_stream(&mut quinn_send).unwrap();

        let mut quinn_recv = quinn_recv;
        let response_head =
            stargate_protocol::read_webtransport_http_response_head(&mut quinn_recv)
                .await
                .unwrap();
        let mut response_body = Vec::new();
        while let Some(chunk) =
            stargate_protocol::read_webtransport_http_body_chunk(&mut quinn_recv)
                .await
                .unwrap()
        {
            response_body.extend_from_slice(&chunk);
        }
        (response_head.status, response_head.headers, response_body)
    }

    async fn send_direct_http3_json_request(
        tunnel_addr: SocketAddr,
        path: &str,
        headers: HeaderMap,
        body: &'static [u8],
    ) -> (StatusCode, HeaderMap, Vec<u8>) {
        tokio::time::timeout(Duration::from_secs(2), async move {
            let mut endpoint = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(
                stargate_tls::build_insecure_quic_client_config_with_alpn(
                    TunnelTransportProtocol::Http3.alpn_protocols(),
                )
                .unwrap(),
            );
            let connection = endpoint
                .connect(tunnel_addr, "stargate")
                .unwrap()
                .await
                .unwrap();
            let (mut driver, mut send_request) = h3::client::builder()
                .build(h3_quinn::Connection::new(connection.clone()))
                .await
                .unwrap();
            let mut driver_task =
                tokio::spawn(async move { future::poll_fn(|cx| driver.poll_close(cx)).await });

            let uri: http::Uri = format!("https://stargate:{}{path}", tunnel_addr.port())
                .parse()
                .unwrap();
            let mut request = http::Request::builder()
                .method(http::Method::POST)
                .uri(uri)
                .body(())
                .unwrap();
            *request.headers_mut() = headers;
            let mut stream = send_request.send_request(request).await.unwrap();
            stream.send_data(Bytes::from_static(body)).await.unwrap();
            stream.finish().await.unwrap();

            let response = stream.recv_response().await.unwrap();
            let status = response.status();
            let headers = response.headers().clone();
            let mut response_body = Vec::new();
            while let Some(mut chunk) = stream.recv_data().await.unwrap() {
                while chunk.has_remaining() {
                    let len = chunk.remaining();
                    response_body.extend_from_slice(&chunk.copy_to_bytes(len));
                }
            }

            connection.close(0u32.into(), b"test complete");
            if tokio::time::timeout(Duration::from_secs(1), &mut driver_task)
                .await
                .is_err()
            {
                driver_task.abort();
            }
            (status, headers, response_body)
        })
        .await
        .expect("direct HTTP/3 request timed out")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_webtransport_stalled_stream_header_does_not_block_later_responses_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = listener.local_addr().unwrap();
        let app = Router::new().route(
            "/v1/responses",
            post(|req: Request| async move {
                let request_id = req
                    .headers()
                    .get("x-request-id")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("missing")
                    .to_string();
                (
                    StatusCode::OK,
                    [(reqwest::header::CONTENT_TYPE.as_str(), "application/json")],
                    format!(r#"{{"request_id":"{request_id}"}}"#),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{upstream_addr}"),
        );
        config.tunnel_protocol = TunnelTransportProtocol::WebTransport;
        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let session = open_direct_webtransport_session(tunnel.listen_addr()).await;

        let (_stalled_send, _stalled_recv) = session.connection.open_bi().await.unwrap();
        let (response_status, _response_headers, response_body) = tokio::time::timeout(
            Duration::from_secs(2),
            send_direct_webtransport_json_request(
                &session,
                "/v1/responses",
                "model-webtransport",
                "req-after-stalled-direct-wt",
                br#"{"input":"hi","stream":true}"#,
            ),
        )
        .await
        .expect("direct WebTransport request after stalled stream timed out");

        assert_eq!(response_status, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(
            payload
                .get("request_id")
                .and_then(serde_json::Value::as_str),
            Some("req-after-stalled-direct-wt")
        );

        tunnel.shutdown().await;
    }

    async fn send_json_proxy_request(
        send: &mut stargate_protocol::SendStream,
        path: &str,
        model: &str,
        request_id: &str,
        body: &'static [u8],
    ) {
        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", path.parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", model.parse().unwrap());
        headers.insert("x-request-id", request_id.parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(body)).await.unwrap();
        send.finish().unwrap();
    }

    async fn send_proxy_request_with_headers(
        send: &mut stargate_protocol::SendStream,
        headers: HeaderMap,
        body: &'static [u8],
    ) {
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(body)).await.unwrap();
        send.finish().unwrap();
    }

    fn embeddings_tunnel_headers(request_id: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/embeddings".parse().unwrap());
        headers.insert("x-request-id", request_id.parse().unwrap());
        headers.insert("x-model", "model-embed".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        headers
    }

    fn assert_quality_metrics_absent(metrics: &str) {
        assert!(
            !metrics.contains("pylon_quality_checks_total"),
            "quality checks should be absent:\n{metrics}"
        );
        assert!(
            !metrics.contains("pylon_quality_threshold_matches_total"),
            "quality threshold matches should be absent:\n{metrics}"
        );
    }

    #[tokio::test]
    async fn quic_tunnel_forwards_to_http_backend() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|req: Request| async move {
                let model = req
                    .headers()
                    .get("x-model")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("none");
                let saw_expected_queue_header =
                    req.headers().contains_key("x-stargate-expected-queue-ms");
                let mut sse = axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"ok"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
                .into_response();
                sse.headers_mut().insert(
                    HeaderName::from_static("x-echo-model"),
                    HeaderValue::from_str(model).unwrap(),
                );
                sse.headers_mut().insert(
                    HeaderName::from_static("x-saw-expected-queue"),
                    HeaderValue::from_str(&saw_expected_queue_header.to_string()).unwrap(),
                );
                *sse.status_mut() = StatusCode::OK;
                sse
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-tunnel-1".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("x-stargate-expected-queue-ms", "5".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();

        let body = b"{\"messages\":[],\"stream\":true}";
        send.send_body(Bytes::from(&body[..])).await.unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        let status = response_headers.get("x-status").unwrap().to_str().unwrap();
        assert_eq!(status, "200");
        assert_eq!(
            response_headers
                .get("x-echo-model")
                .unwrap()
                .to_str()
                .unwrap(),
            "model-a"
        );
        assert_eq!(
            response_headers
                .get("x-saw-expected-queue")
                .unwrap()
                .to_str()
                .unwrap(),
            "false"
        );

        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_text = String::from_utf8(response_body).unwrap();
        assert!(response_text.contains("chat.completion.chunk"));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_marks_explicit_retryable_upstream_rejection() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                let mut response = Response::new(Body::from(r#"{"error":"queue full"}"#));
                *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                response.headers_mut().insert(
                    HeaderName::from_static("x-stargate-upstream-retryable"),
                    HeaderValue::from_static("true"),
                );
                response
                    .headers_mut()
                    .insert(reqwest::header::RETRY_AFTER, HeaderValue::from_static("2"));
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-retryable-1".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "429"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "upstream_admission_rejected"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-after-ms")
                .unwrap()
                .to_str()
                .unwrap(),
            "2000"
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="upstream_admission_rejected",status="429"} 1"#
            ),
            "missing retryable response metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_rejects_queue_estimate_mismatch_before_upstream() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let upstream_hits = Arc::new(AtomicUsize::new(0));
        let upstream_hits_for_app = upstream_hits.clone();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| {
                let upstream_hits = upstream_hits_for_app.clone();
                async move {
                    upstream_hits.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::OK, "unexpected")
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.queue_mismatch_retry.retry_after_ms = Some(125);
        config
            .queue_tracker
            .update_model_throughput("model-a", 100.0);
        let queue_tracker = config.queue_tracker.clone();
        let _queued_request = config.queue_tracker.track_request(&RequiredTunnelHeaders {
            request_id: "req-already-queued".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            accepted_at: std::time::Instant::now(),
        });

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-queue-mismatch".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("x-stargate-expected-queue-ms", "0".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "429"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "queue_estimate_mismatch"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-after-ms")
                .unwrap()
                .to_str()
                .unwrap(),
            "125"
        );

        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_text = String::from_utf8(response_body).unwrap();
        assert!(response_text.contains("queue_estimate_mismatch"));
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue_tracker.tracked_request_count(),
            1,
            "queue mismatch rejection should not leak the rejected request"
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                "# HELP pylon_retryable_responses_total Total number of retryable responses emitted or relayed by pylon"
            ),
            "retryable response HELP text should cover local admission responses:\n{metrics}"
        );
        assert!(
            metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="queue_estimate_mismatch",status="429"} 1"#
            ),
            "missing queue mismatch retry metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_queue_mismatch_retry_disabled_forwards_to_upstream() {
        let (tunnel, upstream_hits, queue_tracker, _queued_request, metrics) =
            start_queue_mismatch_test_tunnel(TunnelTransportProtocol::Custom, false).await;

        let mut headers = queue_mismatch_request_headers("req-queue-mismatch-disabled");
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        let (response_headers, response_body) = send_custom_quic_json_request(
            tunnel.listen_addr(),
            headers,
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        assert!(response_headers.get("x-stargate-retryable").is_none());
        assert_eq!(String::from_utf8(response_body).unwrap(), "forwarded");
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            queue_tracker.tracked_request_count(),
            1,
            "disabled queue mismatch admission should still finish the proxied request"
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_queue_admission_decisions_total{inference_server_id="inst-a",model_id="model-a",result="disabled"} 1"#
            ),
            "missing disabled queue admission metric:\n{metrics}"
        );
        assert!(
            !metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="queue_estimate_mismatch",status="429"}"#
            ),
            "disabled queue mismatch admission should not emit retryable rejection metrics:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http3_tunnel_rejects_queue_estimate_mismatch_before_upstream() {
        let (tunnel, upstream_hits, queue_tracker, _queued_request, metrics) =
            start_queue_mismatch_test_tunnel(TunnelTransportProtocol::Http3, true).await;

        let (status, response_headers, response_body) = send_direct_http3_json_request(
            tunnel.listen_addr(),
            "/v1/chat/completions",
            queue_mismatch_request_headers("req-h3-queue-mismatch"),
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "queue_estimate_mismatch"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-after-ms")
                .unwrap()
                .to_str()
                .unwrap(),
            "125"
        );
        assert!(response_headers.get("x-status").is_none());
        assert!(
            String::from_utf8(response_body)
                .unwrap()
                .contains("queue_estimate_mismatch")
        );
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue_tracker.tracked_request_count(),
            1,
            "HTTP/3 queue mismatch rejection should not leak the rejected request"
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="queue_estimate_mismatch",status="429"} 1"#
            ),
            "missing HTTP/3 queue mismatch retry metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webtransport_tunnel_rejects_queue_estimate_mismatch_before_upstream() {
        let (tunnel, upstream_hits, queue_tracker, _queued_request, metrics) =
            start_queue_mismatch_test_tunnel(TunnelTransportProtocol::WebTransport, true).await;
        let session = open_direct_webtransport_session(tunnel.listen_addr()).await;

        let (status, response_headers, response_body) =
            send_direct_webtransport_request_with_headers(
                &session,
                "/v1/chat/completions",
                queue_mismatch_request_headers("req-webtransport-queue-mismatch"),
                br#"{"messages":[],"stream":true}"#,
            )
            .await;

        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "queue_estimate_mismatch"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-after-ms")
                .unwrap()
                .to_str()
                .unwrap(),
            "125"
        );
        assert!(response_headers.get("x-status").is_none());
        assert!(
            String::from_utf8(response_body)
                .unwrap()
                .contains("queue_estimate_mismatch")
        );
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue_tracker.tracked_request_count(),
            1,
            "WebTransport queue mismatch rejection should not leak the rejected request"
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="queue_estimate_mismatch",status="429"} 1"#
            ),
            "missing WebTransport queue mismatch retry metric:\n{metrics}"
        );

        session.connection.close(0u32.into(), b"test complete");
        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_strips_spoofed_retry_headers_without_upstream_signal() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                let mut response = Response::new(Body::from(r#"{"error":"too many"}"#));
                *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
                response.headers_mut().insert(
                    HeaderName::from_static("x-stargate-retryable"),
                    HeaderValue::from_static("true"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("x-stargate-retry-reason"),
                    HeaderValue::from_static("spoofed"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("x-stargate-retry-after-ms"),
                    HeaderValue::from_static("1"),
                );
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-bare-429".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "429"
        );
        assert!(response_headers.get("x-stargate-retryable").is_none());
        assert!(response_headers.get("x-stargate-retry-reason").is_none());
        assert!(response_headers.get("x-stargate-retry-after-ms").is_none());

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_nonretryable_failures_total{inference_server_id="inst-a",reason="missing_upstream_retry_header"} 1"#
            ),
            "missing nonretryable failure metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_marks_local_connect_failure_retryable_when_configured() {
        let closed_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{closed_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.retry.local_connect_failures_retryable = true;
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-local-connect-failure".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "503"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "true"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "local_connect_failure"
        );
        let response_text = read_response_text(&mut recv).await;
        assert_problem_response(
            &response_headers,
            &response_text,
            503,
            "Service Unavailable",
            "local upstream connection failed",
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_retryable_responses_total{inference_server_id="inst-a",reason="local_connect_failure",status="503"} 1"#
            ),
            "missing local connect failure retryable metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_marks_local_connect_failure_nonretryable_by_default() {
        let closed_addr = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{closed_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert(
            "x-request-id",
            "req-local-connect-nonretry".parse().unwrap(),
        );
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "503"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retryable")
                .unwrap()
                .to_str()
                .unwrap(),
            "false"
        );
        assert_eq!(
            response_headers
                .get("x-stargate-retry-reason")
                .unwrap()
                .to_str()
                .unwrap(),
            "local_connect_failure"
        );
        let response_text = read_response_text(&mut recv).await;
        assert_problem_response(
            &response_headers,
            &response_text,
            503,
            "Service Unavailable",
            "local upstream connection failed",
        );

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_nonretryable_failures_total{inference_server_id="inst-a",reason="local_connect_failure"} 1"#
            ),
            "missing local connect failure nonretryable metric:\n{metrics}"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_request_observation_for_streaming_response() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                let mut response = axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":1}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":" world"}}],"usage":{"completion_tokens":2}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
                .into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-total"),
                    HeaderValue::from_static("17"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-processed"),
                    HeaderValue::from_static("17"),
                );
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-stream".parse().unwrap());
        headers.insert("x-request-id", "req-stream-1".parse().unwrap());
        headers.insert("x-input-tokens", "17".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        assert!(
            response_headers
                .get("x-pylon-engine-stat-input-tokens-total")
                .is_none()
        );
        assert!(
            response_headers
                .get("x-pylon-engine-stat-input-tokens-processed")
                .is_none()
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.request_id, "req-stream-1");
        assert_eq!(observation.model_id, "model-stream");
        assert_eq!(observation.input_tokens, 17);
        assert_eq!(observation.input_tokens_processed, 17);
        assert_eq!(observation.output_messages, 2);
        assert_eq!(observation.output_tokens, 2);
        assert!(observation.output_tokens_explicit);
        assert!(observation.output_tokens_from_chunk_usage);
        assert!(observation.has_engine_request_stats);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_request_observation_for_streaming_responses() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/responses",
            post(|_req: Request| async move {
                let mut response = axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .event("response.created")
                            .data(r#"{"type":"response.created","response":{"status":"in_progress"}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .event("response.output_text.delta")
                            .data(r#"{"type":"response.output_text.delta","delta":"Hello"}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .event("response.completed")
                            .data(r#"{"type":"response.completed","response":{"usage":{"input_tokens":11,"output_tokens":2,"total_tokens":13}}}"#)
                    );
                })
                .into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-total"),
                    HeaderValue::from_static("11"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-processed"),
                    HeaderValue::from_static("11"),
                );
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/responses",
            "model-responses",
            "req-responses-observed",
            br#"{"input":"hello","stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        assert!(
            response_headers
                .get("x-pylon-engine-stat-input-tokens-total")
                .is_none()
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_text = String::from_utf8(response_body).unwrap();
        assert!(response_text.contains("event: response.created"));
        assert!(response_text.contains("event: response.output_text.delta"));
        assert!(response_text.contains("event: response.completed"));

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            observation.endpoint,
            crate::request_observer::RequestObservationEndpoint::Responses
        );
        assert_eq!(observation.request_id, "req-responses-observed");
        assert_eq!(observation.model_id, "model-responses");
        assert_eq!(observation.input_tokens, 11);
        assert_eq!(observation.input_tokens_processed, 11);
        assert_eq!(observation.output_messages, 2);
        assert_eq!(observation.output_tokens, 2);
        assert!(observation.output_tokens_explicit);
        assert!(observation.output_tokens_from_chunk_usage);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_times_out_when_responses_stream_stalls_before_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/responses",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .event("response.created")
                            .data(r#"{"type":"response.created","response":{"status":"in_progress"}}"#)
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default()
                            .event("response.completed")
                            .data(r#"{"type":"response.completed","response":{"status":"completed"}}"#)
                    );
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.first_output_timeout = Duration::from_millis(10);
        config.output_chunk_timeout = Duration::from_millis(100);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/responses",
            "model-responses",
            "req-responses-timeout",
            br#"{"input":"hello","stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let first_chunk = recv
            .recv_body()
            .await
            .unwrap()
            .expect("response.created event should be forwarded");
        assert!(
            String::from_utf8(first_chunk.to_vec())
                .unwrap()
                .contains("response.created")
        );
        assert!(recv.recv_body().await.is_err());

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_feeds_engine_stats_into_stats_collector() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                let mut response = axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":1}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":" world"}}],"usage":{"completion_tokens":2}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
                .into_response();
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-total"),
                    HeaderValue::from_static("17"),
                );
                response.headers_mut().insert(
                    HeaderName::from_static("x-pylon-engine-stat-input-tokens-processed"),
                    HeaderValue::from_static("17"),
                );
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let stats_config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&stats_config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let stats_handle =
            start_stats_collector(stats_config, observation_rx, model_stats_tx, stop_rx);

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(observation_tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-stream".parse().unwrap());
        headers.insert("x-request-id", "req-stream-stats".parse().unwrap());
        headers.insert("x-input-tokens", "17".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let stats = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let (model_id, stats) = model_stats_rx.recv_async().await.unwrap();
                if model_id == "model-stream"
                    && stats
                        .stats_capabilities
                        .contains(&"request.final_headers".to_string())
                    && stats
                        .stats_capabilities
                        .contains(&"request.output.chunk_usage".to_string())
                {
                    break stats;
                }
            }
        })
        .await
        .unwrap();
        assert!(stats.stats_observed_at_unix_ms > 0);
        assert_eq!(
            stats.stats_sources,
            vec!["request_metadata".to_string(), "chunk_usage".to_string()]
        );

        let _ = stop_tx.send(true);
        stats_handle.shutdown().await;
        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_counts_terminal_only_usage_tokens() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":null}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":" world"}}],"usage":null}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[],"usage":{"completion_tokens":7}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-terminal-usage".parse().unwrap());
        headers.insert("x-request-id", "req-terminal-usage".parse().unwrap());
        headers.insert("x-input-tokens", "13".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.request_id, "req-terminal-usage");
        assert_eq!(observation.model_id, "model-terminal-usage");
        assert_eq!(observation.input_tokens, 13);
        assert_eq!(observation.output_messages, 3);
        assert_eq!(observation.output_tokens, 7);
        assert!(observation.output_tokens_explicit);
        assert!(observation.output_tokens_from_chunk_usage);
        assert!(!observation.has_engine_request_stats);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_forwards_legacy_progress_comments_without_observing_them() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let data_chunk = r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":100}}"#;
        let sse_body = format!(
            ": keepalive\n\n\
: inference-progress.v1 v=1 req=req-progress m=model-progress seq=1 ph=prefill it=17 ip=9 extra=ignored\n\n\
data: {data_chunk}\n\n\
: inference-progress.v1 v=1 req=req-progress m=model-progress seq=2 ph=decode it=17 ip=17 og=2 ts=1760000000000\n\n\
: inference-progress.v1 v=1 req=req-progress m=model-progress seq=3 ph=decode og=1\n\n\
: inference-progress.v1 v=1 req=req-progress m=wrong-model seq=4 ph=decode og=99\n\n\
: inference-progress.v1 v=1 req=req-progress seq=bad og=9\n\n\
data: [DONE]\n\n"
        );

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| {
                let sse_body = sse_body.clone();
                async move {
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from(sse_body))
                        .unwrap()
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(32);
        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                collect_quality_metrics: true,
                collect_quality_metrics_min_tokens: 1,
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-progress",
            "req-progress",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_body = String::from_utf8(response_body).unwrap();
        assert!(response_body.contains(": keepalive"));
        assert!(response_body.contains("inference-progress.v1"));
        assert!(response_body.contains(&format!("data: {data_chunk}")));
        assert!(response_body.contains("data: [DONE]"));

        let observations = tokio::time::timeout(Duration::from_secs(1), async {
            let mut observations = Vec::new();
            loop {
                let observation = rx.recv_async().await.unwrap();
                let terminal = observation.is_terminal();
                observations.push(observation);
                if terminal {
                    break observations;
                }
            }
        })
        .await
        .unwrap();

        let terminal = observations.last().unwrap();
        assert_eq!(terminal.request_id, "req-progress");
        assert_eq!(terminal.model_id, "model-progress");
        assert_eq!(terminal.input_tokens_processed, 0);
        assert_eq!(terminal.output_messages, 1);
        assert_eq!(terminal.output_tokens, 100);
        assert!(terminal.output_tokens_explicit);
        assert!(terminal.output_tokens_from_chunk_usage);
        assert!(!terminal.has_inference_progress_stats);
        assert_eq!(
            terminal.state,
            crate::request_observer::RequestObservationState::Complete
        );
        let metrics = metrics_text(&metrics);
        assert!(
            metrics
                .contains(r#"pylon_quality_checks_total{model="model-progress",result="clean"} 1"#)
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_uses_chunk_stats_fallback_when_progress_contract_is_absent() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":9}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-fallback",
            "req-fallback",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.output_messages, 1);
        assert_eq!(observation.output_tokens, 9);
        assert!(observation.output_tokens_explicit);
        assert!(observation.output_tokens_from_chunk_usage);
        assert!(!observation.has_inference_progress_stats);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_forwards_late_legacy_progress_comments_after_fallback_starts() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let data_chunk = r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":3}}"#;
        let sse_body = format!(
            "data: {data_chunk}\n\n\
: inference-progress.v1 v=1 req=req-late-progress m=model-late-progress seq=1 ph=decode it=11 ip=11 og=99\n\n\
data: [DONE]\n\n"
        );

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| {
                let sse_body = sse_body.clone();
                async move {
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from(sse_body))
                        .unwrap()
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-late-progress",
            "req-late-progress",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_body = String::from_utf8(response_body).unwrap();
        assert!(response_body.contains(&format!("data: {data_chunk}")));
        assert!(response_body.contains("inference-progress.v1"));
        assert!(response_body.contains("data: [DONE]"));

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.output_tokens, 3);
        assert!(observation.output_tokens_from_chunk_usage);
        assert!(!observation.has_inference_progress_stats);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_ignores_malformed_legacy_progress_comments_for_stats() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let data_chunk = r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello"}}],"usage":{"completion_tokens":9}}"#;
        let sse_body = format!(
            ": inference-progress.v1 v=1 req=req-malformed-progress seq=bad og=9\n\n\
data: {data_chunk}\n\n\
data: [DONE]\n\n"
        );

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| {
                let sse_body = sse_body.clone();
                async move {
                    Response::builder()
                        .header("content-type", "text/event-stream")
                        .body(Body::from(sse_body))
                        .unwrap()
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-malformed-progress",
            "req-malformed-progress",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_body = String::from_utf8(response_body).unwrap();
        assert!(response_body.contains("inference-progress.v1"));
        assert!(response_body.contains(&format!("data: {data_chunk}")));

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(observation.output_messages, 1);
        assert_eq!(observation.output_tokens, 9);
        assert!(observation.output_tokens_explicit);
        assert!(observation.output_tokens_from_chunk_usage);
        assert!(!observation.has_inference_progress_stats);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_quality_token_threshold_uses_chunk_usage_counts() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let sse_body = "\
: inference-progress.v1 v=1 req=req-progress-quality m=model-progress-quality seq=1 ph=prefill it=11 ip=11\n\n\
data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"alpha beta\"}}],\"usage\":{\"completion_tokens\":12}}\n\n\
: inference-progress.v1 v=1 req=req-progress-quality m=model-progress-quality seq=2 ph=decode it=11 ip=11 og=12\n\n\
data: [DONE]\n\n";

        let app = Router::new().route(
            "/v1/chat/completions",
            post(move |_req: Request| async move {
                Response::builder()
                    .header("content-type", "text/event-stream")
                    .body(Body::from(sse_body))
                    .unwrap()
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(10),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-progress-quality",
            "req-progress-quality",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let response_body = String::from_utf8(response_body).unwrap();
        assert!(response_body.contains("inference-progress.v1"));

        let metrics = metrics_text(&metrics);
        assert!(metrics.contains(
            r#"pylon_quality_checks_total{model="model-progress-quality",result="matched"} 1"#
        ));
        assert!(metrics.contains(
            r#"pylon_quality_threshold_matches_total{model="model-progress-quality",reason="output_tokens"} 1"#
        ));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_quality_metrics_for_repetitive_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"loop loop loop loop loop loop"}}],"usage":{"completion_tokens":6}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                collect_quality_metrics: true,
                collect_quality_metrics_min_tokens: 1,
                output_repetition_1gram_threshold_min: Some(0.2),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-quality".parse().unwrap());
        headers.insert("x-request-id", "req-quality-1".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_quality_checks_total{model="model-quality",result="matched"} 1"#
            )
        );
        assert!(metrics.contains(
            r#"pylon_quality_threshold_matches_total{model="model-quality",reason="repetition_1gram"} 1"#
        ));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_scores_all_choices_in_streamed_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"alpha beta gamma delta"}}],"usage":{"completion_tokens":4}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"index":1,"delta":{"content":"loop loop loop loop"}}],"usage":{"completion_tokens":8}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                collect_quality_metrics: true,
                collect_quality_metrics_min_tokens: 1,
                output_repetition_1gram_threshold_min: Some(0.3),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-multi-choice",
            br#"{"messages":[],"stream":true,"n":2}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_quality_checks_total{model="model-quality",result="matched"} 1"#
            )
        );
        assert!(metrics.contains(
            r#"pylon_quality_threshold_matches_total{model="model-quality",reason="repetition_1gram"} 1"#
        ));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_skips_quality_metrics_for_non_sse_chat_error_response() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                let mut response = Response::new(Body::from(r#"{"error":"backend overloaded"}"#));
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                response
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(1),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-quality".parse().unwrap());
        headers.insert("x-request-id", "req-quality-error".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "500"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert_quality_metrics_absent(&metrics);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_clean_quality_metrics_once_for_clean_sse_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"alpha beta gamma delta"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                collect_quality_metrics: true,
                collect_quality_metrics_min_tokens: 1,
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-clean",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="clean"} 1"#)
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="matched"}"#)
        );
        assert!(!metrics.contains("pylon_quality_threshold_matches_total"));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_skipped_quality_metrics_for_unevaluated_streamed_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"alpha beta gamma"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                collect_quality_metrics: true,
                output_repetition_1gram_threshold_min: Some(0.3),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-skipped",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_quality_checks_total{model="model-quality",result="skipped"} 1"#
            )
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="clean"}"#)
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="matched"}"#)
        );
        assert!(!metrics.contains("pylon_quality_threshold_matches_total"));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_skipped_quality_metrics_for_role_only_stream_with_token_threshold() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant"}}],"usage":{"completion_tokens":3}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(10),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-role-only",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_quality_checks_total{model="model-quality",result="skipped"} 1"#
            )
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="clean"}"#)
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="matched"}"#)
        );
        assert!(!metrics.contains("pylon_quality_threshold_matches_total"));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_emits_clean_quality_metrics_for_below_threshold_text_stream() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"alpha beta gamma"}}],"usage":{"completion_tokens":3}}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(10),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-token-clean",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="clean"} 1"#)
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="skipped"}"#)
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="matched"}"#)
        );
        assert!(!metrics.contains("pylon_quality_threshold_matches_total"));

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_never_emits_quality_metrics_when_monitor_is_disabled() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"alpha beta gamma delta"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-disabled",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert_quality_metrics_absent(&metrics);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_never_emits_quality_metrics_for_non_chat_requests() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app =
            Router::new().route(
                "/v1/embeddings",
                post(|_req: Request| async move {
                    Response::new(Body::from(r#"{"embedding":[1,2,3]}"#))
                }),
            );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(1),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/embeddings",
            "model-quality",
            "req-quality-non-chat",
            br#"{"input":"hello"}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert_quality_metrics_absent(&metrics);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn embeddings_tunnel_forwards_json_without_stream() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/embeddings",
            post(|req: Request| async move {
                let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                    .await
                    .unwrap();
                assert_eq!(
                    body,
                    Bytes::from_static(
                        br#"{"model":"model-embed","input":["alpha","beta"]}"#
                    )
                );
                Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"object":"list","data":[{"object":"embedding","embedding":[0.1,0.2],"index":0},{"object":"embedding","embedding":[0.3,0.4],"index":1}],"model":"model-embed","usage":{"prompt_tokens":11,"total_tokens":11}}"#,
                    ))
                    .unwrap()
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/embeddings",
            "model-embed",
            "req-embed-forward",
            br#"{"model":"model-embed","input":["alpha","beta"]}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(payload["object"], "list");
        assert_eq!(payload["data"].as_array().unwrap().len(), 2);

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            observation.endpoint,
            crate::request_observer::RequestObservationEndpoint::Embeddings
        );
        assert_eq!(observation.request_id, "req-embed-forward");
        assert_eq!(observation.model_id, "model-embed");
        assert_eq!(observation.input_tokens, 11);
        assert_eq!(observation.embedding_items, 2);
        assert!(observation.embedding_items_observed);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn embeddings_tunnel_rejects_missing_request_id_model_or_input_tokens() {
        for (missing_header, expected_message) in [
            ("x-request-id", "missing required x-request-id header"),
            ("x-model", "missing required x-model header"),
            ("x-input-tokens", "missing required x-input-tokens header"),
        ] {
            let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let http_addr = http_listener.local_addr().unwrap();
            let hits = Arc::new(AtomicUsize::new(0));
            let hits_for_app = hits.clone();
            let app = Router::new().route(
                "/v1/embeddings",
                post(move |_req: Request| {
                    let hits = hits_for_app.clone();
                    async move {
                        hits.fetch_add(1, Ordering::Relaxed);
                        Response::new(Body::from(r#"{"unexpected":true}"#))
                    }
                }),
            );
            tokio::spawn(async move {
                let _ = axum::serve(http_listener, app).await;
            });

            let config = QuicHttpTunnelConfig::new(
                "127.0.0.1:0".parse().unwrap(),
                format!("http://{http_addr}"),
            );
            let tunnel = start_quic_http_tunnel(config).await.unwrap();
            let tunnel_addr = tunnel.listen_addr();
            let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

            let mut headers = embeddings_tunnel_headers("req-embed-missing");
            headers.remove(missing_header);

            send_proxy_request_with_headers(
                &mut send,
                headers,
                br#"{"model":"model-embed","input":"hello"}"#,
            )
            .await;

            let response_headers = recv.recv_header().await.unwrap();
            assert_eq!(
                response_headers.get("x-status").unwrap().to_str().unwrap(),
                "400"
            );
            let mut response_body = Vec::new();
            while let Some(chunk) = recv.recv_body().await.unwrap() {
                response_body.extend_from_slice(&chunk);
            }
            let body = String::from_utf8(response_body).unwrap();
            assert!(
                body.contains(expected_message),
                "expected body to contain {expected_message:?}, got {body:?}"
            );
            assert_eq!(
                hits.load(Ordering::Relaxed),
                0,
                "upstream must not be called when {missing_header} is missing"
            );

            tunnel.shutdown().await;
        }
    }

    #[tokio::test]
    async fn embeddings_tunnel_rejects_malformed_json_before_upstream() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_app = hits.clone();
        let app = Router::new().route(
            "/v1/embeddings",
            post(move |_req: Request| {
                let hits = hits_for_app.clone();
                async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    Response::new(Body::from(r#"{"unexpected":true}"#))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.request_observation_tx = Some(tx);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_proxy_request_with_headers(
            &mut send,
            embeddings_tunnel_headers("req-embed-bad-json"),
            br#"{"model":"model-embed","input":"unterminated"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "400"
        );
        let mut response_body = Vec::new();
        while let Some(chunk) = recv.recv_body().await.unwrap() {
            response_body.extend_from_slice(&chunk);
        }
        let body = String::from_utf8(response_body).unwrap();
        assert!(
            body.contains("request body must be valid JSON"),
            "expected invalid JSON error, got {body:?}"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 0);

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            observation.endpoint,
            crate::request_observer::RequestObservationEndpoint::Embeddings
        );
        assert_eq!(observation.request_id, "req-embed-bad-json");
        assert!(!observation.embedding_items_observed);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Failed
        );

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http3_embeddings_tunnel_forwards_json_and_validates_required_headers() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_app = hits.clone();
        let app = Router::new().route(
            "/v1/embeddings",
            post(move |req: Request| {
                let hits = hits_for_app.clone();
                async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    let path = req
                        .uri()
                        .path_and_query()
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| req.uri().path().to_string());
                    let model = req
                        .headers()
                        .get("x-model")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing")
                        .to_string();
                    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                        .await
                        .unwrap();
                    Json(serde_json::json!({
                        "path": path,
                        "model": model,
                        "body": String::from_utf8(body.to_vec()).unwrap(),
                        "object": "list",
                        "data": [
                            {"object": "embedding", "embedding": "AAAA", "index": 0}
                        ],
                        "usage": {"prompt_tokens": 11, "total_tokens": 11}
                    }))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.tunnel_protocol = TunnelTransportProtocol::Http3;
        config.request_observation_tx = Some(tx);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-embeddings".parse().unwrap());
        headers.insert("x-model", "model-embed".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let (status, _response_headers, response_body) = send_direct_http3_json_request(
            tunnel.listen_addr(),
            "/v1/embeddings?encoding=base64",
            headers,
            br#"{"model":"model-embed","input":"alpha","encoding_format":"base64"}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(payload["path"], "/v1/embeddings?encoding=base64");
        assert_eq!(payload["model"], "model-embed");
        assert_eq!(
            payload["body"],
            r#"{"model":"model-embed","input":"alpha","encoding_format":"base64"}"#
        );
        assert_eq!(payload["data"][0]["embedding"], "AAAA");
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            observation.endpoint,
            crate::request_observer::RequestObservationEndpoint::Embeddings
        );
        assert_eq!(observation.request_id, "req-h3-embeddings");
        assert_eq!(observation.embedding_items, 1);
        assert!(observation.embedding_items_observed);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-embeddings-missing".parse().unwrap());
        headers.insert("x-model", "model-embed".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let (status, _response_headers, response_body) = send_direct_http3_json_request(
            tunnel.listen_addr(),
            "/v1/embeddings",
            headers,
            br#"{"model":"model-embed","input":"alpha"}"#,
        )
        .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response_body).unwrap();
        assert!(
            body.contains("missing required x-input-tokens header"),
            "expected missing input-token error, got {body:?}"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webtransport_embeddings_tunnel_forwards_json_and_validates_required_headers() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let hits = Arc::new(AtomicUsize::new(0));
        let hits_for_app = hits.clone();
        let app = Router::new().route(
            "/v1/embeddings",
            post(move |req: Request| {
                let hits = hits_for_app.clone();
                async move {
                    hits.fetch_add(1, Ordering::Relaxed);
                    let path = req
                        .uri()
                        .path_and_query()
                        .map(|value| value.as_str().to_string())
                        .unwrap_or_else(|| req.uri().path().to_string());
                    let model = req
                        .headers()
                        .get("x-model")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("missing")
                        .to_string();
                    let body = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                        .await
                        .unwrap();
                    Json(serde_json::json!({
                        "path": path,
                        "model": model,
                        "body": String::from_utf8(body.to_vec()).unwrap(),
                        "object": "list",
                        "data": [
                            {"object": "embedding", "embedding": [0.1, 0.2], "index": 0},
                            {"object": "embedding", "embedding": [0.3, 0.4], "index": 1}
                        ],
                        "usage": {"prompt_tokens": 11, "total_tokens": 11}
                    }))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let (tx, rx) = flume::bounded(16);
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.tunnel_protocol = TunnelTransportProtocol::WebTransport;
        config.request_observation_tx = Some(tx);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let session = open_direct_webtransport_session(tunnel.listen_addr()).await;

        let (status, _response_headers, response_body) = send_direct_webtransport_json_request(
            &session,
            "/v1/embeddings?source=webtransport",
            "model-embed",
            "req-wt-embeddings",
            br#"{"model":"model-embed","input":["alpha","beta"]}"#,
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        let payload: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(payload["path"], "/v1/embeddings?source=webtransport");
        assert_eq!(payload["model"], "model-embed");
        assert_eq!(
            payload["body"],
            r#"{"model":"model-embed","input":["alpha","beta"]}"#
        );
        assert_eq!(payload["data"].as_array().unwrap().len(), 2);
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        let observation = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let observation = rx.recv_async().await.unwrap();
                if observation.is_terminal() {
                    break observation;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            observation.endpoint,
            crate::request_observer::RequestObservationEndpoint::Embeddings
        );
        assert_eq!(observation.request_id, "req-wt-embeddings");
        assert_eq!(observation.embedding_items, 2);
        assert!(observation.embedding_items_observed);
        assert_eq!(
            observation.state,
            crate::request_observer::RequestObservationState::Complete
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-wt-embeddings-missing".parse().unwrap());
        headers.insert("x-model", "model-embed".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let (status, _response_headers, response_body) =
            send_direct_webtransport_request_with_headers(
                &session,
                "/v1/embeddings",
                headers,
                br#"{"model":"model-embed","input":"alpha"}"#,
            )
            .await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8(response_body).unwrap();
        assert!(
            body.contains("missing required x-input-tokens header"),
            "expected missing input-token error, got {body:?}"
        );
        assert_eq!(hits.load(Ordering::Relaxed), 1);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_skips_quality_metrics_when_stream_times_out_before_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.first_output_timeout = Duration::from_millis(10);
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(1),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-timeout",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        assert!(recv.recv_body().await.is_err());

        let metrics = metrics_text(&metrics);
        assert_quality_metrics_absent(&metrics);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_skips_quality_metrics_when_stream_ends_before_output() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(1),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-eof",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        if let Ok(response_headers) = recv.recv_header().await {
            assert_eq!(
                response_headers.get("x-status").unwrap().to_str().unwrap(),
                "200"
            );
            let _ = recv.recv_body().await;
        }

        let metrics = metrics_text(&metrics);
        assert_quality_metrics_absent(&metrics);

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_records_one_quality_check_for_multi_chunk_stream() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"alpha"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":" beta"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":" gamma"}}]}"#)
                    );
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let metrics = PylonMetrics::new().unwrap();
        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.inference_server_id = Some("inst-a".to_string());
        config.metrics = Some(metrics.clone());
        config.request_quality_monitor =
            crate::request_quality_monitor::RequestQualityMonitorConfig {
                output_tokens_threshold_min: Some(2),
                ..crate::request_quality_monitor::RequestQualityMonitorConfig::default()
            };

        let tunnel = start_quic_http_tunnel(config).await.unwrap();
        let tunnel_addr = tunnel.listen_addr();
        let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;

        send_json_proxy_request(
            &mut send,
            "/v1/chat/completions",
            "model-quality",
            "req-quality-multi-chunk",
            br#"{"messages":[],"stream":true}"#,
        )
        .await;

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        while recv.recv_body().await.unwrap().is_some() {}

        let metrics = metrics_text(&metrics);
        assert!(
            metrics.contains(
                r#"pylon_quality_checks_total{model="model-quality",result="matched"} 1"#
            )
        );
        assert!(
            !metrics
                .contains(r#"pylon_quality_checks_total{model="model-quality",result="clean"}"#)
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_rejects_missing_request_id() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move { Response::new(Body::from("{\"ok\":true}")) }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        ))
        .await
        .unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[]}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "400"
        );
        let response_text = read_response_text(&mut recv).await;
        assert_problem_response(
            &response_headers,
            &response_text,
            400,
            "Bad Request",
            "missing required x-request-id header",
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_rejects_non_streaming_request_body() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move { Response::new(Body::from("{\"ok\":true}")) }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        ))
        .await
        .unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-non-stream-1".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":false}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "400"
        );
        let response_text = read_response_text(&mut recv).await;
        assert_problem_response(
            &response_headers,
            &response_text,
            400,
            "Bad Request",
            "/v1/chat/completions requests must set stream=true",
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_rejects_non_streaming_responses_request_body() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let upstream_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let app_hits = Arc::clone(&upstream_hits);
        let app = Router::new().route(
            "/v1/responses",
            post(move |_req: Request| {
                let app_hits = Arc::clone(&app_hits);
                async move {
                    app_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Response::new(Body::from("{\"ok\":true}"))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        ))
        .await
        .unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/responses".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-non-stream-responses".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"input":"hello","stream":false}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "400"
        );

        let response_text = read_response_text(&mut recv).await;
        assert_problem_response(
            &response_headers,
            &response_text,
            400,
            "Bad Request",
            "/v1/responses requests must set stream=true",
        );
        assert_eq!(
            upstream_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "non-streaming responses requests should not reach upstream"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_rejects_missing_required_headers_for_responses() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();
        let upstream_hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let app_hits = Arc::clone(&upstream_hits);
        let app = Router::new().route(
            "/v1/responses",
            post(move |_req: Request| {
                let app_hits = Arc::clone(&app_hits);
                async move {
                    app_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Response::new(Body::from("{\"ok\":true}"))
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        ))
        .await
        .unwrap();

        let tunnel_addr = tunnel.listen_addr();
        let required_headers = [
            ("x-request-id", "req-responses-required"),
            ("x-model", "model-a"),
            ("x-input-tokens", "11"),
        ];

        for (missing_header, expected_body_fragment) in [
            ("x-request-id", "x-request-id"),
            ("x-model", "x-model"),
            ("x-input-tokens", "x-input-tokens"),
        ] {
            let (_endpoint, mut send, mut recv) = open_test_tunnel_stream(tunnel_addr).await;
            let mut headers = HeaderMap::new();
            headers.insert("x-method", "POST".parse().unwrap());
            headers.insert("x-path", "/v1/responses".parse().unwrap());
            headers.insert("x-routing-key", "rk-1".parse().unwrap());
            headers.insert("content-type", "application/json".parse().unwrap());
            for (name, value) in required_headers {
                if name != missing_header {
                    headers.insert(name, value.parse().unwrap());
                }
            }
            send.send_header(headers).await.unwrap();
            send.send_body(Bytes::from_static(br#"{"input":"hello","stream":true}"#))
                .await
                .unwrap();
            send.finish().unwrap();

            let response_headers = recv.recv_header().await.unwrap();
            assert_eq!(
                response_headers.get("x-status").unwrap().to_str().unwrap(),
                "400",
                "missing {missing_header} should be rejected"
            );
            let response_text = read_response_text(&mut recv).await;
            assert_problem_response(
                &response_headers,
                &response_text,
                400,
                "Bad Request",
                &format!("missing required {expected_body_fragment} header"),
            );
        }

        assert_eq!(
            upstream_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "requests missing required headers should not reach upstream"
        );

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_times_out_when_no_output_event_arrives() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    yield Ok::<_, std::convert::Infallible>(Event::default().data("[DONE]"));
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.first_output_timeout = Duration::from_millis(10);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-timeout-1".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );
        assert!(recv.recv_body().await.is_err());

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn quic_tunnel_times_out_when_subsequent_output_event_arrives_too_late() {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http_addr = http_listener.local_addr().unwrap();

        let app = Router::new().route(
            "/v1/chat/completions",
            post(|_req: Request| async move {
                axum::response::Sse::new(async_stream::stream! {
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"first"}}]}"#)
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    yield Ok::<_, std::convert::Infallible>(
                        Event::default().data(r#"{"object":"chat.completion.chunk","choices":[{"delta":{"content":"second"}}]}"#)
                    );
                })
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(http_listener, app).await;
        });

        let mut config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            format!("http://{http_addr}"),
        );
        config.first_output_timeout = Duration::from_millis(100);
        config.output_chunk_timeout = Duration::from_millis(10);
        let tunnel = start_quic_http_tunnel(config).await.unwrap();

        let tunnel_addr = tunnel.listen_addr();

        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).unwrap();
        endpoint.set_default_client_config(trusted_client_config().unwrap());
        let connection = endpoint
            .connect(tunnel_addr, "stargate")
            .unwrap()
            .await
            .unwrap();

        let (quinn_send, quinn_recv) = connection.open_bi().await.unwrap();
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert("x-method", "POST".parse().unwrap());
        headers.insert("x-path", "/v1/chat/completions".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-request-id", "req-timeout-2".parse().unwrap());
        headers.insert("x-input-tokens", "11".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        send.send_header(headers).await.unwrap();
        send.send_body(Bytes::from_static(br#"{"messages":[],"stream":true}"#))
            .await
            .unwrap();
        send.finish().unwrap();

        let response_headers = recv.recv_header().await.unwrap();
        assert_eq!(
            response_headers.get("x-status").unwrap().to_str().unwrap(),
            "200"
        );

        let first_chunk = recv.recv_body().await.unwrap().expect("first chunk");
        let first_text = std::str::from_utf8(&first_chunk).unwrap();
        assert!(first_text.contains("first"));

        let next_chunk = recv.recv_body().await;
        assert!(
            next_chunk.is_err(),
            "expected stream read error after output timeout"
        );

        tunnel.shutdown().await;
    }

    fn trusted_client_config() -> Result<ClientConfig> {
        stargate_tls::build_insecure_quic_client_config()
    }

    #[test]
    fn derive_sni_extracts_hostname() {
        assert_eq!(
            derive_sni("pod-a.stargate.external:50072"),
            "pod-a.stargate.external"
        );
    }

    #[test]
    fn derive_sni_falls_back_for_ip() {
        assert_eq!(derive_sni("10.0.0.1:50072"), "stargate");
    }

    #[test]
    fn derive_sni_falls_back_for_localhost() {
        assert_eq!(derive_sni("localhost:50072"), "stargate");
    }

    #[test]
    fn derive_sni_falls_back_for_ipv6() {
        assert_eq!(derive_sni("::1:50072"), "stargate");
    }

    #[test]
    fn derive_sni_handles_bare_hostname() {
        assert_eq!(
            derive_sni("pod-a.stargate.external"),
            "pod-a.stargate.external"
        );
    }

    #[test]
    fn target_authority_preserves_advertised_hostname() {
        assert_eq!(
            target_authority("pod-a.stargate.external:50072"),
            "pod-a.stargate.external:50072"
        );
    }

    #[test]
    fn target_authority_brackets_ipv6_address() {
        assert_eq!(target_authority("::1:50072"), "[::1]:50072");
    }

    #[test]
    fn observer_guard_calls_fail_on_drop() {
        use crate::request_observer::{RequestObservation, RequestObserver};

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-request-id", "req-guard".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-input-tokens", "10".parse().unwrap());
        let (tx, rx) = flume::bounded::<RequestObservation>(8);
        let observer = RequestObserver::new(&headers, Some(tx)).unwrap();
        {
            let _guard = ObserverGuard(observer);
        }
        let mut last = None;
        while let Ok(obs) = rx.try_recv() {
            last = Some(obs);
        }
        let last = last.expect("expected at least one observation from guard drop");
        assert_eq!(
            last.state,
            crate::request_observer::RequestObservationState::Failed
        );
    }

    #[test]
    fn observer_guard_noop_when_already_terminal() {
        use crate::request_observer::{RequestObservation, RequestObserver};

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-request-id", "req-noop".parse().unwrap());
        headers.insert("x-routing-key", "rk-1".parse().unwrap());
        headers.insert("x-model", "model-a".parse().unwrap());
        headers.insert("x-input-tokens", "10".parse().unwrap());
        let (tx, rx) = flume::bounded::<RequestObservation>(16);
        let mut observer = RequestObserver::new(&headers, Some(tx)).unwrap();
        observer.on_upstream_response_headers(&reqwest::header::HeaderMap::new(), 200);
        observer.observe_output_message();
        observer.finish();
        assert!(observer.is_terminal());

        let pre_drop_count = rx.len();
        {
            let _guard = ObserverGuard(observer);
        }
        assert_eq!(
            rx.len(),
            pre_drop_count,
            "guard should not emit extra observations when already terminal"
        );
    }

    #[test]
    fn build_trusted_client_config_insecure_succeeds_without_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = build_trusted_client_config(None, true, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn build_trusted_client_config_secure_fails_without_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = build_trusted_client_config(None, false, TunnelTransportProtocol::Custom);
        assert!(result.is_err());
    }

    #[test]
    fn build_trusted_client_config_secure_succeeds_with_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert_pem, _key_pem) = stargate_tls::generate_self_signed_cert().unwrap();
        let result =
            build_trusted_client_config(Some(&cert_pem), false, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn make_server_config_self_signed_when_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = make_server_config(None, None, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn make_server_config_uses_provided_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert_pem, key_pem) = stargate_tls::generate_self_signed_cert().unwrap();
        let result = make_server_config(
            Some(&cert_pem),
            Some(&key_pem),
            TunnelTransportProtocol::Custom,
        );
        assert!(result.is_ok());
    }
}
