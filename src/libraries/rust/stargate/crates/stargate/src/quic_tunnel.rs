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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail, ensure};
use axum::body::Body;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use bytes::{Buf, Bytes};
use futures::{StreamExt, future};
use quinn::{ClientConfig, Connection, Endpoint, EndpointConfig};
use rustls::RootCertStore;
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{Instrument, error, info, info_span, warn};
use url::Url;

use stargate_protocol::{RecvStream, SendStream};
use stargate_protocol::{
    TunnelTransportProtocol, WebTransportHttpRequestHead, WebTransportHttpResponseHead,
};

use crate::auth::WorkerAuthenticator;
use crate::forwarding::{self, ForwardingResolver, PeerResolution};
use crate::load_balancer_state::StargateState;

const WEBTRANSPORT_TUNNEL_PATH: &str = "/_stargate/webtransport";
const HEADER_INFERENCE_SERVER_ID: &str = "x-inference-server-id";
const HEADER_REVERSE_AUTH_TOKEN: &str = "x-stargate-auth-token";

#[derive(Clone, Debug)]
pub struct QuicTunnelConfig {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub direct_quic_connections: usize,
    pub tls_cert_pem: Option<Vec<u8>>,
    pub tls_key_pem: Option<Vec<u8>>,
    pub quic_insecure: bool,
    pub tunnel_protocol: TunnelTransportProtocol,
}

pub struct StreamingResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body_stream: StreamingBody,
}

type H3ClientBidiStream =
    <h3_quinn::OpenStreams as h3::quic::OpenStreams<bytes::Bytes>>::BidiStream;
type H3ClientConnection = h3::client::Connection<h3_quinn::Connection, bytes::Bytes>;
type H3ServerConnection = h3::server::Connection<h3_quinn::Connection, bytes::Bytes>;
type H3ClientRequestStream = h3::client::RequestStream<H3ClientBidiStream, bytes::Bytes>;
type H3ClientRequestSendStream = h3::client::RequestStream<
    <H3ClientBidiStream as h3::quic::BidiStream<bytes::Bytes>>::SendStream,
    bytes::Bytes,
>;
type H3ClientRequestRecvStream = h3::client::RequestStream<
    <H3ClientBidiStream as h3::quic::BidiStream<bytes::Bytes>>::RecvStream,
    bytes::Bytes,
>;
type H3ServerRequestStream = h3::server::RequestStream<H3ClientBidiStream, bytes::Bytes>;

pub struct StreamingBody {
    inner: StreamingBodyInner,
    request_body_send_task: Option<RequestBodySendTask>,
}

enum StreamingBodyInner {
    Custom {
        recv_stream: RecvStream,
    },
    Http3 {
        stream: Box<H3ClientRequestRecvStream>,
        _connection_handle: Http3ConnectionHandle,
    },
    WebTransport {
        recv_stream: quinn::RecvStream,
        _connection_handle: WebTransportConnectionHandle,
    },
}

struct RequestBodySendTask {
    label: &'static str,
    completion_timeout: Duration,
    handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl RequestBodySendTask {
    fn new(
        label: &'static str,
        completion_timeout: Duration,
        handle: tokio::task::JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            label,
            completion_timeout,
            handle: Some(handle),
        }
    }

    async fn finish(mut self) -> Result<()> {
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        let mut handle = AbortOnDropRequestBodySendHandle::new(handle);

        match tokio::time::timeout(self.completion_timeout, handle.join()).await {
            Ok(result) => {
                handle.disarm();
                finish_request_body_send_result(self.label, result)
            }
            Err(_) => {
                warn!(
                    task = self.label,
                    timeout_ms = self.completion_timeout.as_millis(),
                    "request body send task did not finish before response EOF timeout"
                );
                // The upstream response is already complete; abort the upload
                // producer so response finalization cannot stall forever.
                handle.abort();
                let result = handle.join().await;
                handle.disarm();
                finish_timed_out_request_body_send(self.label, result)
            }
        }
    }

    fn abort(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

struct AbortOnDropRequestBodySendHandle {
    handle: Option<tokio::task::JoinHandle<Result<()>>>,
}

impl AbortOnDropRequestBodySendHandle {
    fn new(handle: tokio::task::JoinHandle<Result<()>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    async fn join(&mut self) -> std::result::Result<Result<()>, tokio::task::JoinError> {
        self.handle
            .as_mut()
            .expect("request body send handle should not be disarmed before join")
            .await
    }

    fn disarm(&mut self) {
        let _completed = self.handle.take();
    }
}

impl Drop for AbortOnDropRequestBodySendHandle {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            // Response EOF finalization can be cancelled by downstream disconnect;
            // abort before dropping the handle so the upload task is not detached.
            handle.abort();
        }
    }
}

fn finish_request_body_send_result(
    label: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result.with_context(|| format!("failed to join {label} send task"))? {
        Ok(()) => Ok(()),
        Err(error) => Err(error.context(format!("failed to send {label}"))),
    }
}

fn finish_timed_out_request_body_send(
    label: &'static str,
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
) -> Result<()> {
    match result {
        Ok(result) => match result {
            Ok(()) => Ok(()),
            Err(error) => Err(error.context(format!("failed to send {label}"))),
        },
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(error).with_context(|| format!("failed to join {label} send task")),
    }
}

impl Drop for RequestBodySendTask {
    fn drop(&mut self) {
        // If callers drop the response before EOF, stop the producer so it
        // cannot keep reading user body bytes after the response is abandoned.
        self.abort();
    }
}

impl StreamingBody {
    fn custom(
        recv_stream: RecvStream,
        request_body_send_task: Option<RequestBodySendTask>,
    ) -> Self {
        Self {
            inner: StreamingBodyInner::Custom { recv_stream },
            request_body_send_task,
        }
    }

    fn http3(
        stream: Box<H3ClientRequestRecvStream>,
        connection_handle: Http3ConnectionHandle,
        request_body_send_task: Option<RequestBodySendTask>,
    ) -> Self {
        Self {
            inner: StreamingBodyInner::Http3 {
                stream,
                _connection_handle: connection_handle,
            },
            request_body_send_task,
        }
    }

    fn webtransport(
        recv_stream: quinn::RecvStream,
        connection_handle: WebTransportConnectionHandle,
        request_body_send_task: Option<RequestBodySendTask>,
    ) -> Self {
        Self {
            inner: StreamingBodyInner::WebTransport {
                recv_stream,
                _connection_handle: connection_handle,
            },
            request_body_send_task,
        }
    }

    pub async fn recv_body(&mut self) -> Result<Option<bytes::Bytes>> {
        let next_chunk = match &mut self.inner {
            StreamingBodyInner::Custom { recv_stream } => recv_stream
                .recv_body()
                .await
                .context("failed to receive custom tunnel response body"),
            StreamingBodyInner::Http3 { stream, .. } => {
                match stream
                    .recv_data()
                    .await
                    .map_err(|error| anyhow!("failed to receive h3 response body: {error:?}"))?
                {
                    Some(mut chunk) => {
                        let len = chunk.remaining();
                        Ok(Some(chunk.copy_to_bytes(len)))
                    }
                    None => Ok(None),
                }
            }
            StreamingBodyInner::WebTransport { recv_stream, .. } => {
                stargate_protocol::read_webtransport_http_body_chunk(recv_stream)
                    .await
                    .context("failed to receive WebTransport response body")
            }
        };

        match next_chunk {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.finish_request_body_send().await?;
                Ok(None)
            }
            Err(error) => {
                self.abort_request_body_send();
                Err(error)
            }
        }
    }

    async fn finish_request_body_send(&mut self) -> Result<()> {
        if let Some(task) = self.request_body_send_task.take() {
            task.finish().await?;
        }
        Ok(())
    }

    fn abort_request_body_send(&mut self) {
        if let Some(mut task) = self.request_body_send_task.take() {
            task.abort();
        }
    }
}

pub struct OpenStreamingRequest {
    inner: OpenStreamingRequestInner,
    response_header_timeout: Duration,
}

enum OpenStreamingRequestInner {
    Custom {
        send_stream: SendStream,
        recv_stream: RecvStream,
    },
    Http3 {
        stream: Box<H3ClientRequestStream>,
        connection_handle: Http3ConnectionHandle,
    },
    WebTransport {
        send_stream: quinn::SendStream,
        recv_stream: quinn::RecvStream,
        connection_handle: WebTransportConnectionHandle,
    },
}

impl OpenStreamingRequest {
    pub async fn send_body_and_recv_response(self, body: Body) -> Result<StreamingResponse> {
        let Self {
            inner,
            response_header_timeout,
        } = self;

        match inner {
            OpenStreamingRequestInner::Custom {
                send_stream,
                recv_stream,
            } => {
                Self::send_custom_body_and_recv_response(
                    send_stream,
                    recv_stream,
                    response_header_timeout,
                    body,
                )
                .await
            }
            OpenStreamingRequestInner::Http3 {
                stream,
                connection_handle,
            } => {
                Self::send_h3_body_and_recv_response(
                    stream,
                    response_header_timeout,
                    body,
                    connection_handle,
                )
                .await
            }
            OpenStreamingRequestInner::WebTransport {
                send_stream,
                recv_stream,
                connection_handle,
            } => {
                Self::send_webtransport_body_and_recv_response(
                    send_stream,
                    recv_stream,
                    response_header_timeout,
                    body,
                    connection_handle,
                )
                .await
            }
        }
    }

    async fn send_custom_body_and_recv_response(
        send_stream: SendStream,
        mut recv_stream: RecvStream,
        response_header_timeout: Duration,
        body: Body,
    ) -> Result<StreamingResponse> {
        let response_header_deadline = tokio::time::Instant::now() + response_header_timeout;
        let mut send_task = tokio::spawn(async move {
            let result = send_custom_request_body(send_stream, body).await;
            if let Err(error) = &result {
                error!(error = %error, "failed to send custom request body");
            }
            result
        });
        let mut send_done = false;

        let response_headers = tokio::select! {
            // If a peer reset makes both branches ready, preserve the local
            // body producer error instead of returning a less actionable
            // response-header read error. Early server responses still win
            // while the body task is pending.
            biased;
            send_result = &mut send_task => {
                send_done = true;
                match send_result.context("custom request body send task panicked")? {
                    Ok(()) => {
                        recv_custom_response_headers_until(response_header_deadline, &mut recv_stream)
                            .await?
                    }
                    Err(error) => return Err(error.context("failed to send custom request body")),
                }
            }
            response_headers =
                recv_custom_response_headers_until(response_header_deadline, &mut recv_stream) => {
                    match response_headers {
                        Ok(headers) => headers,
                        Err(error) => {
                            send_task.abort();
                            return Err(error);
                        }
                    }
                }
        };

        let status_code = match response_headers
            .get("x-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u16>().ok())
        {
            Some(status_code) => status_code,
            None => {
                warn!(
                    response_headers = ?response_headers,
                    "custom tunnel response missing or invalid x-status header"
                );
                502
            }
        };
        let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::BAD_GATEWAY);
        let request_body_send_task = if status.is_success() && !send_done {
            Some(RequestBodySendTask::new(
                "custom request body",
                response_header_timeout,
                send_task,
            ))
        } else {
            if !send_done {
                send_task.abort();
            }
            None
        };

        let mut clean_headers = HeaderMap::new();
        for (name, value) in &response_headers {
            if name.as_str() != "x-status" {
                clean_headers.append(name, value.clone());
            }
        }

        Ok(StreamingResponse {
            status,
            headers: clean_headers,
            body_stream: StreamingBody::custom(recv_stream, request_body_send_task),
        })
    }

    async fn send_webtransport_body_and_recv_response(
        send_stream: quinn::SendStream,
        mut recv_stream: quinn::RecvStream,
        response_header_timeout: Duration,
        body: Body,
        connection_handle: WebTransportConnectionHandle,
    ) -> Result<StreamingResponse> {
        let response_header_deadline = tokio::time::Instant::now() + response_header_timeout;
        let mut send_task = tokio::spawn(async move {
            let result = send_webtransport_request_body(send_stream, body).await;
            if let Err(error) = &result {
                error!(error = %error, "failed to send WebTransport request body");
            }
            result
        });
        let mut send_done = false;

        let response_head = tokio::select! {
            biased;
            // If the peer rejects the request and closes the upload side in the
            // same tick, preserve the response already sent by the peer.
            response_head =
                recv_webtransport_response_head_until(response_header_deadline, &mut recv_stream) => {
                    match response_head {
                        Ok(response_head) => response_head,
                        Err(error) => {
                            send_task.abort();
                            return Err(error);
                        }
                    }
            }
            send_result = &mut send_task => {
                send_done = true;
                match send_result.context("WebTransport request body send task panicked")? {
                    Ok(()) => {
                        recv_webtransport_response_head_until(response_header_deadline, &mut recv_stream)
                            .await?
                    }
                    Err(error) => return Err(error.context("failed to send WebTransport request body")),
                }
            }
        };
        let request_body_send_task = if response_head.status.is_success() && !send_done {
            Some(RequestBodySendTask::new(
                "WebTransport request body",
                response_header_timeout,
                send_task,
            ))
        } else {
            if !send_done {
                send_task.abort();
            }
            None
        };

        Ok(StreamingResponse {
            status: response_head.status,
            headers: response_head.headers,
            body_stream: StreamingBody::webtransport(
                recv_stream,
                connection_handle,
                request_body_send_task,
            ),
        })
    }

    async fn send_h3_body_and_recv_response(
        stream: Box<H3ClientRequestStream>,
        response_header_timeout: Duration,
        body: Body,
        connection_handle: Http3ConnectionHandle,
    ) -> Result<StreamingResponse> {
        let (mut send_stream, mut recv_stream) = stream.split();
        let response_header_deadline = tokio::time::Instant::now() + response_header_timeout;
        let mut send_task = tokio::spawn(async move {
            let result = send_h3_request_body(&mut send_stream, body).await;
            if let Err(error) = &result {
                error!(error = %error, "failed to send h3 request body");
            }
            result
        });
        let mut send_done = false;

        let response = tokio::select! {
            biased;
            // If the peer rejects the request and closes the upload side in the
            // same tick, preserve the response already sent by the peer.
            response = recv_h3_response_until(response_header_deadline, &mut recv_stream) => {
                match response {
                    Ok(response) => response,
                    Err(error) => {
                        send_task.abort();
                        return Err(error);
                    }
                }
            }
            send_result = &mut send_task => {
                send_done = true;
                match send_result.context("h3 request body send task panicked")? {
                    Ok(()) => recv_h3_response_until(response_header_deadline, &mut recv_stream).await?,
                    Err(error) => return Err(error.context("failed to send h3 request body")),
                }
            }
        };
        let status =
            StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
        let request_body_send_task = if status.is_success() && !send_done {
            Some(RequestBodySendTask::new(
                "h3 request body",
                response_header_timeout,
                send_task,
            ))
        } else {
            if !send_done {
                send_task.abort();
            }
            None
        };
        Ok(StreamingResponse {
            status,
            headers: response.headers().clone(),
            body_stream: StreamingBody::http3(
                Box::new(recv_stream),
                connection_handle,
                request_body_send_task,
            ),
        })
    }
}

async fn send_custom_request_body(mut send_stream: SendStream, body: Body) -> Result<()> {
    let mut body_stream = body.into_data_stream();
    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.context("failed to read request body chunk")?;
        send_stream
            .send_body(chunk)
            .await
            .context("failed to send request body chunk")?;
    }
    send_stream.finish().context("failed to finish send stream")
}

async fn send_webtransport_request_body(
    mut send_stream: quinn::SendStream,
    body: Body,
) -> Result<()> {
    let mut body_stream = body.into_data_stream();
    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.context("failed to read request body chunk")?;
        stargate_protocol::write_webtransport_http_body(&mut send_stream, chunk)
            .await
            .context("failed to send WebTransport request body chunk")?;
    }
    stargate_protocol::finish_webtransport_http_stream(&mut send_stream)
        .context("failed to finish WebTransport request stream")
}

async fn recv_custom_response_headers_until(
    deadline: tokio::time::Instant,
    recv_stream: &mut RecvStream,
) -> Result<HeaderMap> {
    tokio::time::timeout_at(deadline, recv_stream.recv_header())
        .await
        .map_err(|_| anyhow!("quic request timed out"))?
        .context("failed to receive response headers")
}

async fn recv_webtransport_response_head_until(
    deadline: tokio::time::Instant,
    recv_stream: &mut quinn::RecvStream,
) -> Result<WebTransportHttpResponseHead> {
    tokio::time::timeout_at(
        deadline,
        stargate_protocol::read_webtransport_http_response_head(recv_stream),
    )
    .await
    .map_err(|_| anyhow!("quic request timed out"))?
    .context("failed to receive WebTransport response head")
}

async fn send_h3_request_body(
    send_stream: &mut H3ClientRequestSendStream,
    body: Body,
) -> Result<()> {
    let mut body_stream = body.into_data_stream();
    while let Some(chunk_result) = body_stream.next().await {
        let chunk = chunk_result.context("failed to read request body chunk")?;
        send_stream
            .send_data(chunk)
            .await
            .map_err(|error| anyhow!("failed to send h3 request body chunk: {error:?}"))?;
    }
    send_stream
        .finish()
        .await
        .map_err(|error| anyhow!("failed to finish h3 request stream: {error:?}"))
}

async fn recv_h3_response_until(
    deadline: tokio::time::Instant,
    recv_stream: &mut H3ClientRequestRecvStream,
) -> Result<http::Response<()>> {
    tokio::time::timeout_at(deadline, recv_stream.recv_response())
        .await
        .map_err(|_| anyhow!("quic request timed out"))?
        .map_err(|error| anyhow!("failed to receive h3 response headers: {error:?}"))
}

fn remaining_request_timeout(started_at: Instant, request_timeout: Duration) -> Duration {
    request_timeout
        .checked_sub(started_at.elapsed())
        .unwrap_or(Duration::ZERO)
}

pub struct QuicHttpProxy {
    config: QuicTunnelConfig,
    endpoint_v4: Arc<Endpoint>,
    endpoint_v6: Arc<Endpoint>,
    pool: Arc<RwLock<HashMap<String, TunnelConnectionSet>>>,
    reverse_connection_events: ReverseConnectionEvents,
    pending_reverse_connections: Arc<Mutex<HashSet<String>>>,
    authenticator: Arc<dyn WorkerAuthenticator>,
}

#[derive(Clone)]
struct ReverseConnectionEvents {
    updates: watch::Sender<u64>,
}

struct ReverseConnectionEventReceiver {
    updates: watch::Receiver<u64>,
}

impl ReverseConnectionEvents {
    fn new() -> Self {
        let (updates, _) = watch::channel(0);
        Self { updates }
    }

    fn subscribe(&self) -> ReverseConnectionEventReceiver {
        ReverseConnectionEventReceiver {
            updates: self.updates.subscribe(),
        }
    }

    fn notify_changed(&self) {
        self.updates
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    async fn wait_until<Check, Fut>(&self, timeout: Duration, mut is_ready: Check) -> bool
    where
        Check: FnMut() -> Fut,
        Fut: Future<Output = bool>,
    {
        let mut connection_events = self.subscribe();
        tokio::time::timeout(timeout, async {
            loop {
                if is_ready().await {
                    return true;
                }
                if !connection_events.changed().await {
                    return false;
                }
            }
        })
        .await
        .unwrap_or(false)
    }
}

impl ReverseConnectionEventReceiver {
    async fn changed(&mut self) -> bool {
        self.updates.changed().await.is_ok()
    }
}

#[derive(Clone)]
enum TunnelConnection {
    Custom(Connection),
    Http3(Http3ConnectionHandle),
    WebTransport(WebTransportConnectionHandle),
}

impl TunnelConnection {
    fn is_healthy(&self) -> bool {
        match self {
            Self::Custom(connection) => connection.close_reason().is_none(),
            Self::Http3(handle) => {
                connection_is_healthy(&handle.connection)
                    && !handle.driver_closed.load(Ordering::Acquire)
            }
            Self::WebTransport(handle) => connection_is_healthy(&handle.connection),
        }
    }

    fn stable_id(&self) -> usize {
        match self {
            Self::Custom(connection) => connection.stable_id(),
            Self::Http3(handle) => handle.connection.stable_id(),
            Self::WebTransport(handle) => handle.connection.stable_id(),
        }
    }
}

#[derive(Clone)]
struct TunnelConnectionSet {
    inner: Arc<TunnelConnectionSetInner>,
}

struct TunnelConnectionSetInner {
    connections: Vec<TunnelConnection>,
    cursor: AtomicUsize,
}

impl TunnelConnectionSet {
    fn new(connections: Vec<TunnelConnection>) -> Result<Self> {
        ensure!(!connections.is_empty(), "tunnel connection set is empty");
        Ok(Self {
            inner: Arc::new(TunnelConnectionSetInner {
                connections,
                cursor: AtomicUsize::new(0),
            }),
        })
    }

    fn single(connection: TunnelConnection) -> Self {
        Self {
            inner: Arc::new(TunnelConnectionSetInner {
                connections: vec![connection],
                cursor: AtomicUsize::new(0),
            }),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.connections.len()
    }

    fn is_healthy(&self) -> bool {
        self.inner
            .connections
            .iter()
            .any(TunnelConnection::is_healthy)
    }

    fn needs_replenishment(&self) -> bool {
        !self
            .inner
            .connections
            .iter()
            .all(TunnelConnection::is_healthy)
    }

    fn choose_healthy(&self) -> Option<TunnelConnection> {
        let len = self.inner.connections.len();
        // The cursor only spreads load across equivalent live connections, so
        // relaxed ordering is enough; the health check below owns correctness.
        let start = self.inner.cursor.fetch_add(1, Ordering::Relaxed) % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let connection = &self.inner.connections[index];
            if connection.is_healthy() {
                return Some(connection.clone());
            }
        }
        None
    }

    fn contains_stable_id(&self, stable_id: usize) -> bool {
        self.inner
            .connections
            .iter()
            .any(|connection| connection.stable_id() == stable_id)
    }
}

#[derive(Clone)]
struct Http3ConnectionHandle {
    connection: Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
    driver_closed: Arc<AtomicBool>,
    _driver_task: Arc<Http3DriverTask>,
}

struct Http3DriverTask {
    connection: Connection,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Http3DriverTask {
    fn drop(&mut self) {
        // The driver task is Arc-held by live H3 handles. When the last handle
        // drops, close QUIC first; abort is only a leak-prevention fallback.
        self.connection.close(0u32.into(), b"h3 driver dropped");
        self.task.abort();
    }
}

#[derive(Clone)]
struct WebTransportConnectionHandle {
    connection: Connection,
    bidi_header: Bytes,
    _lifetime: Arc<WebTransportConnectionLifetime>,
}

enum WebTransportH3Connection {
    Client {
        _connection: Box<H3ClientConnection>,
    },
    Server {
        _connection: Box<H3ServerConnection>,
    },
}

enum WebTransportConnectStream {
    Client { _stream: Box<H3ClientRequestStream> },
    Server { _stream: Box<H3ServerRequestStream> },
}

struct WebTransportConnectionLifetime {
    connection: Connection,
    _h3_connection: tokio::sync::Mutex<Option<WebTransportH3Connection>>,
    _connect_stream: tokio::sync::Mutex<Option<WebTransportConnectStream>>,
}

impl Drop for WebTransportConnectionLifetime {
    fn drop(&mut self) {
        // The WebTransport session is valid only while its CONNECT stream is
        // alive. Closing QUIC here makes the lifetime boundary explicit when
        // the last pooled handle drops.
        self.connection.close(0u32.into(), b"webtransport dropped");
    }
}

fn connection_is_healthy(connection: &Connection) -> bool {
    connection.close_reason().is_none()
}

struct PendingReverseConnectionGuard {
    pending: Arc<Mutex<HashSet<String>>>,
    inference_server_id: String,
}

impl Drop for PendingReverseConnectionGuard {
    fn drop(&mut self) {
        lock_pending_reverse_connections(&self.pending).remove(&self.inference_server_id);
    }
}

fn lock_pending_reverse_connections(
    pending: &Mutex<HashSet<String>>,
) -> std::sync::MutexGuard<'_, HashSet<String>> {
    match pending.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl QuicHttpProxy {
    pub fn new(
        config: QuicTunnelConfig,
        authenticator: Arc<dyn WorkerAuthenticator>,
    ) -> Result<Self> {
        ensure!(
            config.direct_quic_connections > 0,
            "direct_quic_connections must be > 0"
        );
        let client_config = build_client_config(
            config.tls_cert_pem.as_deref(),
            config.quic_insecure,
            config.tunnel_protocol,
        )?;
        let mut endpoint_v4 = Endpoint::client("0.0.0.0:0".parse()?)?;
        let mut endpoint_v6 = Endpoint::client("[::]:0".parse()?)?;
        endpoint_v4.set_default_client_config(client_config.clone());
        endpoint_v6.set_default_client_config(client_config);

        Ok(Self {
            config,
            endpoint_v4: Arc::new(endpoint_v4),
            endpoint_v6: Arc::new(endpoint_v6),
            pool: Arc::new(RwLock::new(HashMap::new())),
            reverse_connection_events: ReverseConnectionEvents::new(),
            pending_reverse_connections: Arc::new(Mutex::new(HashSet::new())),
            authenticator,
        })
    }

    pub async fn preconnect(&self, inference_server_id: &str, target_url: &str) -> Result<()> {
        let connection = self.connect_direct_set(target_url).await?;
        self.pool
            .write()
            .await
            .insert(inference_server_id.to_string(), connection);
        Ok(())
    }

    async fn connect_direct_set(&self, target_url: &str) -> Result<TunnelConnectionSet> {
        let mut connections = Vec::with_capacity(self.config.direct_quic_connections);
        // Opening the configured set up front lets hot-path requests distribute
        // stream creation across QUIC connections instead of piling onto one.
        for _ in 0..self.config.direct_quic_connections {
            connections.push(self.connect_direct(target_url).await?);
        }
        TunnelConnectionSet::new(connections)
    }

    async fn connect_direct(&self, target_url: &str) -> Result<TunnelConnection> {
        let addr = parse_quic_addr(target_url)?;
        let endpoint = if addr.is_ipv6() {
            self.endpoint_v6.as_ref()
        } else {
            self.endpoint_v4.as_ref()
        };
        let connect = endpoint
            .connect(addr, "stargate")
            .context("initiate quic connect failed")?;
        let connection = match tokio::time::timeout(self.config.connect_timeout, connect).await {
            Ok(result) => result.context("quic connect failed")?,
            Err(_) => bail!("quic connect timed out"),
        };
        match tokio::time::timeout(
            self.config.connect_timeout,
            self.build_direct_tunnel_connection(connection),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => bail!("direct tunnel setup timed out"),
        }
    }

    pub async fn reconnect_direct(
        &self,
        inference_server_id: &str,
        target_url: &str,
    ) -> Result<()> {
        let connection = self.connect_direct_set(target_url).await?;
        self.pool
            .write()
            .await
            .insert(inference_server_id.to_string(), connection);
        Ok(())
    }

    pub async fn has_healthy_connection(&self, inference_server_id: &str) -> bool {
        self.pool
            .read()
            .await
            .get(inference_server_id)
            .is_some_and(TunnelConnectionSet::is_healthy)
    }

    pub async fn connection_set_needs_replenishment(&self, inference_server_id: &str) -> bool {
        self.pool
            .read()
            .await
            .get(inference_server_id)
            .is_none_or(TunnelConnectionSet::needs_replenishment)
    }

    pub async fn health_check_rtt(&self, inference_server_id: &str) -> Result<Duration> {
        let start = std::time::Instant::now();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&format!("stargate-health-{inference_server_id}"))
                .context("invalid health check request id")?,
        );
        headers.insert(
            HeaderName::from_static("x-model"),
            HeaderValue::from_static("stargate-health"),
        );
        headers.insert(
            HeaderName::from_static("x-input-tokens"),
            HeaderValue::from_static("0"),
        );
        let response = self
            .proxy_request_streaming(
                inference_server_id,
                Method::GET,
                "/health",
                headers,
                Body::empty(),
            )
            .await?;
        if !response.status.is_success() {
            bail!("health check returned status {}", response.status);
        }

        let mut body_stream = response.body_stream;
        while body_stream.recv_body().await?.is_some() {}

        Ok(start.elapsed())
    }

    pub async fn await_reverse_connection(
        &self,
        inference_server_id: &str,
        timeout: Duration,
    ) -> bool {
        let pool = self.pool.clone();
        let inference_server_id = inference_server_id.to_string();
        self.reverse_connection_events
            .wait_until(timeout, move || {
                let pool = pool.clone();
                let inference_server_id = inference_server_id.clone();
                async move {
                    pool.read()
                        .await
                        .get(&inference_server_id)
                        .is_some_and(TunnelConnectionSet::is_healthy)
                }
            })
            .await
    }

    pub async fn store_reverse_connection(
        &self,
        inference_server_id: &str,
        connection: Connection,
    ) -> bool {
        if self.has_healthy_connection(inference_server_id).await {
            return false;
        }
        let Some(_pending_guard) = self.try_mark_pending_reverse_connection(inference_server_id)
        else {
            return false;
        };

        let tunnel_connection = match self.build_reverse_tunnel_connection(connection).await {
            Ok(connection) => connection,
            Err(error) => {
                warn!(
                    inference_server_id = %inference_server_id,
                    error = %error,
                    "failed to initialize tunnel connection"
                );
                return false;
            }
        };
        self.store_built_reverse_connection(inference_server_id, tunnel_connection)
            .await
    }

    async fn store_built_reverse_connection(
        &self,
        inference_server_id: &str,
        tunnel_connection: TunnelConnection,
    ) -> bool {
        let mut pool = self.pool.write().await;
        if let Some(existing) = pool.get(inference_server_id)
            && existing.is_healthy()
        {
            return false;
        }
        pool.insert(
            inference_server_id.to_string(),
            TunnelConnectionSet::single(tunnel_connection),
        );
        self.reverse_connection_events.notify_changed();
        true
    }

    fn try_mark_pending_reverse_connection(
        &self,
        inference_server_id: &str,
    ) -> Option<PendingReverseConnectionGuard> {
        let mut pending = lock_pending_reverse_connections(&self.pending_reverse_connections);
        if !pending.insert(inference_server_id.to_string()) {
            return None;
        }
        Some(PendingReverseConnectionGuard {
            pending: self.pending_reverse_connections.clone(),
            inference_server_id: inference_server_id.to_string(),
        })
    }

    async fn build_direct_tunnel_connection(
        &self,
        connection: Connection,
    ) -> Result<TunnelConnection> {
        match self.config.tunnel_protocol {
            TunnelTransportProtocol::Custom => Ok(TunnelConnection::Custom(connection)),
            TunnelTransportProtocol::Http3 => self.build_h3_client_connection(connection).await,
            TunnelTransportProtocol::WebTransport => {
                self.build_webtransport_client_connection(connection).await
            }
        }
    }

    async fn build_reverse_tunnel_connection(
        &self,
        connection: Connection,
    ) -> Result<TunnelConnection> {
        match self.config.tunnel_protocol {
            TunnelTransportProtocol::Custom => Ok(TunnelConnection::Custom(connection)),
            TunnelTransportProtocol::Http3 => self.build_h3_client_connection(connection).await,
            TunnelTransportProtocol::WebTransport => {
                bail!("reverse WebTransport connections are established by CONNECT handshake")
            }
        }
    }

    async fn build_h3_client_connection(&self, connection: Connection) -> Result<TunnelConnection> {
        let driver_closed = Arc::new(AtomicBool::new(false));
        let driver_closed_for_task = driver_closed.clone();
        let (mut driver, send_request) = h3::client::builder()
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .map_err(|error| anyhow!("create h3 client connection: {error:?}"))?;
        let driver_task = tokio::spawn(async move {
            let error = future::poll_fn(|cx| driver.poll_close(cx)).await;
            driver_closed_for_task.store(true, Ordering::Release);
            if !error.is_h3_no_error() {
                warn!(error = ?error, "h3 client connection closed with error");
            }
        });
        Ok(TunnelConnection::Http3(Http3ConnectionHandle {
            connection: connection.clone(),
            send_request,
            driver_closed,
            _driver_task: Arc::new(Http3DriverTask {
                connection,
                task: driver_task,
            }),
        }))
    }

    async fn build_webtransport_client_connection(
        &self,
        connection: Connection,
    ) -> Result<TunnelConnection> {
        let mut builder = h3::client::builder();
        builder.enable_extended_connect(true).enable_datagram(true);
        let (h3_connection, mut send_request): (
            H3ClientConnection,
            h3::client::SendRequest<h3_quinn::OpenStreams, bytes::Bytes>,
        ) = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .map_err(|error| anyhow!("create WebTransport h3 client connection: {error:?}"))?;

        let mut request: http::Request<()> = http::Request::builder()
            .method(Method::CONNECT)
            .uri(format!("https://stargate{WEBTRANSPORT_TUNNEL_PATH}"))
            .body(())
            .context("build WebTransport CONNECT request")?;
        request
            .extensions_mut()
            .insert(h3::ext::Protocol::WEB_TRANSPORT);

        let mut connect_stream = send_request
            .send_request(request)
            .await
            .map_err(|error| anyhow!("send WebTransport CONNECT request: {error:?}"))?;
        let session_id = connect_stream.id().into_inner();
        connect_stream
            .finish()
            .await
            .map_err(|error| anyhow!("finish WebTransport CONNECT request: {error:?}"))?;
        let response = connect_stream
            .recv_response()
            .await
            .map_err(|error| anyhow!("receive WebTransport CONNECT response: {error:?}"))?;
        if !response.status().is_success() {
            bail!(
                "WebTransport CONNECT rejected with status {}",
                response.status()
            );
        }

        Ok(TunnelConnection::WebTransport(
            WebTransportConnectionHandle {
                connection: connection.clone(),
                bidi_header: stargate_protocol::WebTransportBidiHeader::new(session_id)
                    .context("precompute WebTransport bidi stream header")?
                    .to_bytes(),
                _lifetime: Arc::new(WebTransportConnectionLifetime {
                    connection,
                    _h3_connection: tokio::sync::Mutex::new(Some(
                        WebTransportH3Connection::Client {
                            _connection: Box::new(h3_connection),
                        },
                    )),
                    _connect_stream: tokio::sync::Mutex::new(Some(
                        WebTransportConnectStream::Client {
                            _stream: Box::new(connect_stream),
                        },
                    )),
                }),
            },
        ))
    }

    async fn build_webtransport_server_connection(
        &self,
        connection: Connection,
        h3_connection: H3ServerConnection,
        mut connect_stream: H3ServerRequestStream,
    ) -> Result<TunnelConnection> {
        let session_id = connect_stream.id().into_inner();
        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(())
            .context("build WebTransport CONNECT response")?;
        connect_stream
            .send_response(response)
            .await
            .map_err(|error| anyhow!("send WebTransport CONNECT response: {error:?}"))?;

        Ok(TunnelConnection::WebTransport(
            WebTransportConnectionHandle {
                connection: connection.clone(),
                bidi_header: stargate_protocol::WebTransportBidiHeader::new(session_id)
                    .context("precompute WebTransport bidi stream header")?
                    .to_bytes(),
                _lifetime: Arc::new(WebTransportConnectionLifetime {
                    connection,
                    _h3_connection: tokio::sync::Mutex::new(Some(
                        WebTransportH3Connection::Server {
                            _connection: Box::new(h3_connection),
                        },
                    )),
                    _connect_stream: tokio::sync::Mutex::new(Some(
                        WebTransportConnectStream::Server {
                            _stream: Box::new(connect_stream),
                        },
                    )),
                }),
            },
        ))
    }

    pub async fn proxy_request_streaming(
        &self,
        inference_server_id: &str,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Body,
    ) -> Result<StreamingResponse> {
        let request = self
            .open_streaming_request(inference_server_id, method, path_and_query, headers)
            .await?;
        request.send_body_and_recv_response(body).await
    }

    pub async fn open_streaming_request(
        &self,
        inference_server_id: &str,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
    ) -> Result<OpenStreamingRequest> {
        let _span = info_span!("quic_http_proxy");
        let started_at = Instant::now();

        let connection = {
            let pool = self.pool.read().await;
            let connection_set = pool.get(inference_server_id).ok_or_else(|| {
                anyhow!("no connection for inference server '{inference_server_id}'")
            })?;
            connection_set.choose_healthy().ok_or_else(|| {
                anyhow!("connection to inference server '{inference_server_id}' is closed")
            })?
        };

        let req = async {
            match connection {
                TunnelConnection::Custom(connection) => {
                    let (quinn_send, quinn_recv) = connection
                        .open_bi()
                        .await
                        .context("open bi stream failed")?;

                    let mut send_stream = SendStream::new(quinn_send);
                    let recv_stream = RecvStream::new(quinn_recv);

                    let mut request_headers = HeaderMap::new();
                    request_headers.insert(
                        HeaderName::from_static("x-method"),
                        HeaderValue::from_str(method.as_str()).context("invalid method")?,
                    );
                    request_headers.insert(
                        HeaderName::from_static("x-path"),
                        HeaderValue::from_str(path_and_query).context("invalid path")?,
                    );
                    for (name, value) in &headers {
                        request_headers.append(name, value.clone());
                    }

                    send_stream
                        .send_header(request_headers)
                        .await
                        .context("failed to send request headers")?;

                    let response_header_timeout =
                        remaining_request_timeout(started_at, self.config.request_timeout);
                    Ok(OpenStreamingRequest {
                        inner: OpenStreamingRequestInner::Custom {
                            send_stream,
                            recv_stream,
                        },
                        response_header_timeout,
                    })
                }
                TunnelConnection::Http3(handle) => {
                    let uri: http::Uri = format!("https://stargate{path_and_query}")
                        .parse()
                        .context("invalid h3 request uri")?;
                    let mut request = http::Request::builder()
                        .method(method.as_str())
                        .uri(uri)
                        .body(())
                        .context("build h3 request")?;
                    for (name, value) in &headers {
                        if should_forward_h3_tunnel_request_header(name) {
                            request.headers_mut().append(name, value.clone());
                        }
                    }
                    let mut send_request = handle.send_request.clone();
                    let stream = send_request
                        .send_request(request)
                        .await
                        .map_err(|error| anyhow!("send h3 request headers: {error:?}"))?;
                    let response_header_timeout =
                        remaining_request_timeout(started_at, self.config.request_timeout);
                    Ok(OpenStreamingRequest {
                        inner: OpenStreamingRequestInner::Http3 {
                            stream: Box::new(stream),
                            connection_handle: handle,
                        },
                        response_header_timeout,
                    })
                }
                TunnelConnection::WebTransport(handle) => {
                    let (quinn_send, quinn_recv) = handle
                        .connection
                        .open_bi()
                        .await
                        .context("open WebTransport bi stream failed")?;

                    let mut request_headers = HeaderMap::new();
                    for (name, value) in &headers {
                        request_headers.append(name, value.clone());
                    }

                    let mut quinn_send = quinn_send;
                    let request_head = WebTransportHttpRequestHead {
                        method: method.clone(),
                        path_and_query: path_and_query.to_string(),
                        headers: request_headers,
                    };
                    stargate_protocol::write_webtransport_http_request_head_after_prefix(
                        &mut quinn_send,
                        handle.bidi_header.clone(),
                        &request_head,
                    )
                    .await
                    .context("failed to send WebTransport request head")?;

                    let response_header_timeout =
                        remaining_request_timeout(started_at, self.config.request_timeout);
                    Ok(OpenStreamingRequest {
                        inner: OpenStreamingRequestInner::WebTransport {
                            send_stream: quinn_send,
                            recv_stream: quinn_recv,
                            connection_handle: handle,
                        },
                        response_header_timeout,
                    })
                }
            }
        };

        match tokio::time::timeout(self.config.request_timeout, req).await {
            Ok(inner) => inner,
            Err(_) => bail!("quic request timed out"),
        }
    }

    pub async fn start_reverse_listener(
        self: &Arc<Self>,
        listen_addr: SocketAddr,
        state: Arc<StargateState>,
        shutdown: CancellationToken,
        task_tracker: TaskTracker,
        forwarding: Option<Arc<dyn ForwardingResolver>>,
        pre_bound_socket: Option<std::net::UdpSocket>,
    ) -> Result<SocketAddr> {
        let server_config = build_server_config(
            self.config.tls_cert_pem.as_deref(),
            self.config.tls_key_pem.as_deref(),
            self.config.tunnel_protocol,
        )?;
        let endpoint = match pre_bound_socket {
            Some(socket) => {
                socket
                    .set_nonblocking(true)
                    .context("set reverse listener socket to non-blocking")?;
                let runtime =
                    quinn::default_runtime().context("no async runtime for quinn endpoint")?;
                Endpoint::new(
                    EndpointConfig::default(),
                    Some(server_config),
                    socket,
                    runtime,
                )
                .context("create reverse listener from pre-bound socket")?
            }
            None => {
                Endpoint::server(server_config, listen_addr).context("bind reverse listener")?
            }
        };
        let bound_addr = endpoint
            .local_addr()
            .context("reverse listener local addr")?;

        let relay_client_config = build_client_config(
            self.config.tls_cert_pem.as_deref(),
            self.config.quic_insecure,
            self.config.tunnel_protocol,
        )?;
        let relay_endpoints = Arc::new(
            forwarding::build_relay_endpoints(
                forwarding::RelayEndpointConfig::default(),
                relay_client_config,
            )
            .context("build relay endpoints")?,
        );

        let proxy = self.clone();
        let listener_tasks = task_tracker.clone();
        let listener_span = info_span!("reverse_tunnel_listener", addr = %bound_addr);
        task_tracker.spawn(
            async move {
                loop {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        incoming = endpoint.accept() => {
                            let Some(incoming) = incoming else { break };
                            let proxy = proxy.clone();
                            let state = state.clone();
                            let forwarding = forwarding.clone();
                            let relay_endpoints = relay_endpoints.clone();
                            let port = bound_addr.port();
                            let peer_connect_timeout = proxy.config.connect_timeout;
                            let connection_tasks = listener_tasks.clone();
                            let connection_span = info_span!("reverse_tunnel_connection", port);
                            listener_tasks.spawn(async move {
                                let dispatch = ReverseDispatchContext {
                                    proxy: &proxy,
                                    state: &state,
                                    forwarding: forwarding.as_deref(),
                                    relay_endpoints: &relay_endpoints,
                                    listen_port: port,
                                    peer_connect_timeout,
                                    task_tracker: &connection_tasks,
                                };
                                if let Err(e) = dispatch_incoming(incoming, dispatch).await {
                                    warn!(error = %e, "reverse tunnel connection failed");
                                }
                            }.instrument(connection_span));
                        }
                    }
                }
                endpoint.close(0u32.into(), b"shutdown");
            }
            .instrument(listener_span),
        );

        info!(addr = %bound_addr, "reverse tunnel listener started");
        Ok(bound_addr)
    }
}

struct ReverseDispatchContext<'a> {
    proxy: &'a QuicHttpProxy,
    state: &'a StargateState,
    forwarding: Option<&'a dyn ForwardingResolver>,
    relay_endpoints: &'a forwarding::RelayEndpoints,
    listen_port: u16,
    peer_connect_timeout: Duration,
    task_tracker: &'a TaskTracker,
}

async fn dispatch_incoming(
    incoming: quinn::Incoming,
    dispatch: ReverseDispatchContext<'_>,
) -> Result<()> {
    let connection = incoming.await.context("accept reverse connection")?;

    if let Some(fwd) = dispatch.forwarding {
        let sni = connection
            .handshake_data()
            .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
            .and_then(|hd| hd.server_name);

        if let Some(sni) = sni {
            match fwd.resolve_peer(&sni, dispatch.listen_port) {
                PeerResolution::Peer(peer) => {
                    info!(
                        peer = %peer.dial_addr,
                        server_name = %peer.server_name,
                        sni = %sni,
                        "relaying QUIC connection to peer"
                    );
                    return forwarding::forward_quic_connection(
                        connection,
                        &peer,
                        dispatch.relay_endpoints,
                        dispatch.peer_connect_timeout,
                    )
                    .await;
                }
                PeerResolution::Local | PeerResolution::NotPeer => {}
            }
        }
    }

    match dispatch.proxy.config.tunnel_protocol {
        TunnelTransportProtocol::WebTransport => {
            handle_reverse_webtransport_connect(
                connection,
                dispatch.proxy,
                dispatch.state,
                dispatch.task_tracker,
            )
            .await
        }
        TunnelTransportProtocol::Custom | TunnelTransportProtocol::Http3 => {
            handle_reverse_handshake(
                connection,
                dispatch.proxy,
                dispatch.state,
                dispatch.task_tracker,
            )
            .await
        }
    }
}

async fn handle_reverse_webtransport_connect(
    connection: Connection,
    proxy: &QuicHttpProxy,
    state: &StargateState,
    task_tracker: &TaskTracker,
) -> Result<()> {
    let mut h3_connection = h3::server::builder()
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1)
        .build(h3_quinn::Connection::new(connection.clone()))
        .await
        .map_err(|error| anyhow!("create reverse WebTransport h3 server: {error:?}"))?;
    let Some(resolver) = h3_connection
        .accept()
        .await
        .map_err(|error| anyhow!("accept reverse WebTransport CONNECT: {error:?}"))?
    else {
        bail!("reverse WebTransport connection closed before CONNECT");
    };
    let (request, mut stream) = resolver
        .resolve_request()
        .await
        .map_err(|error| anyhow!("resolve reverse WebTransport CONNECT: {error:?}"))?;

    let is_webtransport = request
        .extensions()
        .get::<h3::ext::Protocol>()
        .is_some_and(|protocol| *protocol == h3::ext::Protocol::WEB_TRANSPORT);
    if request.method() != Method::CONNECT
        || request.uri().path() != WEBTRANSPORT_TUNNEL_PATH
        || !is_webtransport
    {
        send_webtransport_connect_response(&mut stream, StatusCode::BAD_REQUEST).await?;
        bail!("invalid reverse WebTransport CONNECT request");
    }

    let Some(inference_server_id) = request
        .headers()
        .get(HEADER_INFERENCE_SERVER_ID)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
    else {
        send_webtransport_connect_response(&mut stream, StatusCode::BAD_REQUEST).await?;
        bail!("reverse WebTransport CONNECT missing {HEADER_INFERENCE_SERVER_ID}");
    };
    let auth_token = request
        .headers()
        .get(HEADER_REVERSE_AUTH_TOKEN)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);

    let result = match proxy
        .authenticator
        .authenticate(auth_token.as_deref())
        .await
    {
        Ok(result) => result,
        Err(error) => {
            warn!(
                inference_server_id = %inference_server_id,
                error = %error,
                "reverse WebTransport authentication failed"
            );
            send_webtransport_connect_response(&mut stream, StatusCode::UNAUTHORIZED).await?;
            bail!("authentication failed for reverse WebTransport: {inference_server_id}");
        }
    };

    let Some(registration) = state.registered_reverse_tunnel(&inference_server_id).await else {
        send_webtransport_connect_response(&mut stream, StatusCode::NOT_FOUND).await?;
        bail!("unauthorized inference_server_id in reverse WebTransport: {inference_server_id}");
    };

    if result.routing_key != registration.routing_key {
        send_webtransport_connect_response(&mut stream, StatusCode::FORBIDDEN).await?;
        bail!("QUIC routing_key does not match gRPC registration: {inference_server_id}");
    }

    if proxy.has_healthy_connection(&inference_server_id).await {
        send_webtransport_connect_response(&mut stream, StatusCode::CONFLICT).await?;
        bail!("duplicate reverse WebTransport connection for: {inference_server_id}");
    }
    let Some(_pending_guard) = proxy.try_mark_pending_reverse_connection(&inference_server_id)
    else {
        send_webtransport_connect_response(&mut stream, StatusCode::CONFLICT).await?;
        bail!("pending duplicate reverse WebTransport connection for: {inference_server_id}");
    };

    let tunnel_connection = proxy
        .build_webtransport_server_connection(connection.clone(), h3_connection, stream)
        .await?;
    if !proxy
        .store_built_reverse_connection(&inference_server_id, tunnel_connection)
        .await
    {
        bail!("duplicate reverse WebTransport connection for: {inference_server_id}");
    }

    info!(inference_server_id = %inference_server_id, "reverse WebTransport tunnel established");
    let pool = proxy.pool.clone();
    let closed_id = connection.stable_id();
    let cleanup_span = info_span!(
        "reverse_webtransport_connection_cleanup",
        inference_server_id = %inference_server_id,
        stable_id = closed_id,
    );
    task_tracker.spawn(async move {
        connection.closed().await;
        let mut guard = pool.write().await;
        let is_current = guard
            .get(&inference_server_id)
            .is_some_and(|conn| conn.contains_stable_id(closed_id));
        if is_current {
            guard.remove(&inference_server_id);
            warn!(inference_server_id = %inference_server_id, "reverse WebTransport connection closed, removed from pool");
        }
    }.instrument(cleanup_span));

    Ok(())
}

async fn send_webtransport_connect_response(
    stream: &mut H3ServerRequestStream,
    status: StatusCode,
) -> Result<()> {
    let response = http::Response::builder()
        .status(status)
        .body(())
        .context("build WebTransport CONNECT rejection")?;
    stream
        .send_response(response)
        .await
        .map_err(|error| anyhow!("send WebTransport CONNECT response: {error:?}"))?;
    stream
        .finish()
        .await
        .map_err(|error| anyhow!("finish WebTransport CONNECT response: {error:?}"))
}

async fn send_handshake_nack(
    send: &mut quinn::SendStream,
    reason: &str,
    delivery_timeout: Duration,
) -> Result<()> {
    let ack = stargate_protocol::HandshakeAck {
        accepted: false,
        reason: reason.to_string(),
    };
    stargate_protocol::write_handshake_ack(send, &ack)
        .await
        .context("send handshake NACK")?;
    send.finish().context("finish NACK stream")?;
    // finish() marks the stream as done but does not wait for QUIC to
    // deliver the bytes. The caller bail!s after this function returns,
    // which drops the Connection and tears down the transport before the
    // NACK reaches the client. stopped() blocks until the peer has
    // consumed the stream, ensuring the rejection reason is delivered.
    match tokio::time::timeout(delivery_timeout, send.stopped()).await {
        Ok(result) => {
            result?;
        }
        Err(_) => {
            warn!(
                timeout_ms = delivery_timeout.as_millis(),
                "timed out waiting for reverse handshake NACK delivery"
            );
        }
    }
    Ok(())
}

async fn handle_reverse_handshake(
    connection: Connection,
    proxy: &QuicHttpProxy,
    state: &StargateState,
    task_tracker: &TaskTracker,
) -> Result<()> {
    let (mut quinn_send, mut quinn_recv) = connection
        .accept_bi()
        .await
        .context("accept handshake stream")?;

    let handshake = stargate_protocol::read_handshake(&mut quinn_recv)
        .await
        .context("read handshake message")?;
    let inference_server_id = handshake.inference_server_id;

    if inference_server_id.is_empty() {
        send_handshake_nack(
            &mut quinn_send,
            "empty inference_server_id",
            proxy.config.connect_timeout,
        )
        .await?;
        bail!("empty inference_server_id in reverse handshake");
    }

    let result = match proxy
        .authenticator
        .authenticate(handshake.auth_token.as_deref())
        .await
    {
        Ok(result) => result,
        Err(e) => {
            warn!(
                inference_server_id = %inference_server_id,
                error = %e,
                "reverse handshake authentication failed"
            );
            send_handshake_nack(
                &mut quinn_send,
                "authentication failed",
                proxy.config.connect_timeout,
            )
            .await?;
            bail!("authentication failed for reverse handshake: {inference_server_id}");
        }
    };

    info!(
        inference_server_id = %inference_server_id,
        routing_key = ?result.routing_key,
        "reverse handshake authenticated"
    );

    let Some(registration) = state.registered_reverse_tunnel(&inference_server_id).await else {
        warn!(
            inference_server_id = %inference_server_id,
            "reverse handshake NACK: unauthorized inference_server_id"
        );
        send_handshake_nack(
            &mut quinn_send,
            "unauthorized inference_server_id",
            proxy.config.connect_timeout,
        )
        .await?;
        bail!("unauthorized inference_server_id in reverse handshake: {inference_server_id}");
    };

    if result.routing_key != registration.routing_key {
        warn!(
            inference_server_id = %inference_server_id,
            quic_routing_key = ?result.routing_key,
            stored_routing_key = ?registration.routing_key,
            "reverse handshake NACK: routing key mismatch"
        );
        send_handshake_nack(
            &mut quinn_send,
            "routing key mismatch",
            proxy.config.connect_timeout,
        )
        .await?;
        bail!("QUIC routing_key does not match gRPC registration: {inference_server_id}");
    }

    if !proxy
        .store_reverse_connection(&inference_server_id, connection.clone())
        .await
    {
        warn!(
            inference_server_id = %inference_server_id,
            "reverse handshake NACK: duplicate connection"
        );
        send_handshake_nack(
            &mut quinn_send,
            "duplicate connection",
            proxy.config.connect_timeout,
        )
        .await?;
        bail!("duplicate reverse tunnel connection for: {inference_server_id}");
    }
    let ack = stargate_protocol::HandshakeAck {
        accepted: true,
        reason: String::new(),
    };
    stargate_protocol::write_handshake_ack(&mut quinn_send, &ack)
        .await
        .context("send ACK")?;
    quinn_send.finish().context("finish ACK stream")?;

    info!(inference_server_id = %inference_server_id, "reverse tunnel connection established");
    let pool = proxy.pool.clone();
    let closed_id = connection.stable_id();
    let cleanup_span = info_span!(
        "reverse_tunnel_connection_cleanup",
        inference_server_id = %inference_server_id,
        stable_id = closed_id,
    );
    task_tracker.spawn(async move {
        connection.closed().await;
        let mut guard = pool.write().await;
        // Only the connection instance that installed this cleanup task may
        // remove the pool entry; newer reconnects should remain active.
        let is_current = guard
            .get(&inference_server_id)
            .is_some_and(|conn| conn.contains_stable_id(closed_id));
        if is_current {
            guard.remove(&inference_server_id);
            warn!(inference_server_id = %inference_server_id, "reverse tunnel connection closed, removed from pool");
        }
    }.instrument(cleanup_span));

    Ok(())
}

enum ConnState {
    Connecting { url: String },
    Connected { url: String },
}

pub struct ConnectionWatcher {
    proxy: Arc<QuicHttpProxy>,
    states: HashMap<String, ConnState>,
    reverse_tunnel_connect_timeout: Duration,
}

pub enum EnsureConnectedResult {
    Connected,
    ReverseDisconnected,
    Unavailable,
}

impl ConnectionWatcher {
    pub fn new(proxy: Arc<QuicHttpProxy>, reverse_tunnel_connect_timeout: Duration) -> Self {
        Self {
            proxy,
            states: HashMap::new(),
            reverse_tunnel_connect_timeout,
        }
    }

    pub async fn ensure_connected(
        &mut self,
        inference_server_id: &str,
        inference_server_url: &str,
        reverse_tunnel: bool,
    ) -> EnsureConnectedResult {
        match self.states.get(inference_server_id) {
            None => {
                self.states.insert(
                    inference_server_id.to_string(),
                    ConnState::Connecting {
                        url: inference_server_url.to_string(),
                    },
                );
                if reverse_tunnel {
                    EnsureConnectedResult::ReverseDisconnected
                } else {
                    self.try_preconnect(inference_server_id, inference_server_url)
                        .await
                }
            }
            Some(ConnState::Connected { url }) => {
                if url != inference_server_url {
                    // Evict the stale connection so await_reverse_connection
                    // doesn't match the old entry and short-circuit.
                    self.proxy.pool.write().await.remove(inference_server_id);
                    self.states.insert(
                        inference_server_id.to_string(),
                        ConnState::Connecting {
                            url: inference_server_url.to_string(),
                        },
                    );
                    if reverse_tunnel {
                        self.try_reverse_connect(inference_server_id, inference_server_url)
                            .await
                    } else {
                        self.try_preconnect(inference_server_id, inference_server_url)
                            .await
                    }
                } else if self.proxy.has_healthy_connection(inference_server_id).await {
                    if reverse_tunnel {
                        EnsureConnectedResult::Connected
                    } else {
                        self.try_replenish_direct_connection_set(
                            inference_server_id,
                            inference_server_url,
                        )
                        .await
                    }
                } else {
                    warn!(inference_server_id = %inference_server_id, "connection lost, reconnecting");
                    self.states.insert(
                        inference_server_id.to_string(),
                        ConnState::Connecting {
                            url: inference_server_url.to_string(),
                        },
                    );
                    if reverse_tunnel {
                        self.try_reverse_connect(inference_server_id, inference_server_url)
                            .await
                    } else {
                        self.try_preconnect(inference_server_id, inference_server_url)
                            .await
                    }
                }
            }
            Some(ConnState::Connecting { url }) => {
                if reverse_tunnel && self.proxy.has_healthy_connection(inference_server_id).await {
                    self.states.insert(
                        inference_server_id.to_string(),
                        ConnState::Connected {
                            url: url.to_string(),
                        },
                    );
                    return EnsureConnectedResult::Connected;
                }
                if reverse_tunnel {
                    self.try_reverse_connect(inference_server_id, inference_server_url)
                        .await
                } else {
                    self.try_preconnect(inference_server_id, inference_server_url)
                        .await
                }
            }
        }
    }

    async fn try_preconnect(
        &mut self,
        inference_server_id: &str,
        inference_server_url: &str,
    ) -> EnsureConnectedResult {
        match self
            .proxy
            .preconnect(inference_server_id, inference_server_url)
            .await
        {
            Ok(()) => {
                self.states.insert(
                    inference_server_id.to_string(),
                    ConnState::Connected {
                        url: inference_server_url.to_string(),
                    },
                );
                EnsureConnectedResult::Connected
            }
            Err(error) => {
                warn!(
                    inference_server_id = %inference_server_id,
                    inference_server_url = %inference_server_url,
                    error = %error,
                    "quic preconnect failed"
                );
                EnsureConnectedResult::Unavailable
            }
        }
    }

    async fn try_replenish_direct_connection_set(
        &mut self,
        inference_server_id: &str,
        inference_server_url: &str,
    ) -> EnsureConnectedResult {
        if !self
            .proxy
            .connection_set_needs_replenishment(inference_server_id)
            .await
        {
            return EnsureConnectedResult::Connected;
        }

        match self
            .try_preconnect(inference_server_id, inference_server_url)
            .await
        {
            EnsureConnectedResult::Connected => EnsureConnectedResult::Connected,
            EnsureConnectedResult::Unavailable | EnsureConnectedResult::ReverseDisconnected => {
                // A failed replenish attempt should not demote a backend that
                // still has at least one usable direct connection; the next
                // update will retry.
                EnsureConnectedResult::Connected
            }
        }
    }

    async fn try_reverse_connect(
        &mut self,
        inference_server_id: &str,
        inference_server_url: &str,
    ) -> EnsureConnectedResult {
        if self
            .proxy
            .await_reverse_connection(inference_server_id, self.reverse_tunnel_connect_timeout)
            .await
        {
            self.states.insert(
                inference_server_id.to_string(),
                ConnState::Connected {
                    url: inference_server_url.to_string(),
                },
            );
            EnsureConnectedResult::Connected
        } else {
            error!(
                inference_server_id = %inference_server_id,
                timeout_secs = self.reverse_tunnel_connect_timeout.as_secs(),
                "reverse tunnel connection not received within timeout"
            );
            EnsureConnectedResult::ReverseDisconnected
        }
    }
}

fn build_client_config(
    cert_pem: Option<&[u8]>,
    insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<ClientConfig> {
    if insecure {
        return stargate_tls::build_insecure_quic_client_config_with_alpn(
            tunnel_protocol.alpn_protocols(),
        );
    }
    let cert_data = cert_pem.context("TLS cert required when --quic-insecure is not set")?;
    let mut roots = RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut &*cert_data) {
        roots
            .add(cert.context("failed to parse tunnel cert PEM")?)
            .context("failed to add tunnel cert to root store")?;
    }

    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = tunnel_protocol.alpn_protocols();

    Ok(ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    )))
}

fn build_server_config(
    cert_pem: Option<&[u8]>,
    key_pem: Option<&[u8]>,
    tunnel_protocol: TunnelTransportProtocol,
) -> Result<quinn::ServerConfig> {
    let (cert_owned, key_owned);
    let (cert_data, key_data) = match (cert_pem, key_pem) {
        (Some(c), Some(k)) => (c, k),
        _ => {
            info!("no TLS cert/key provided, generating self-signed certificate");
            let (c, k) = stargate_tls::generate_self_signed_cert()?;
            cert_owned = c;
            key_owned = k;
            (cert_owned.as_slice(), key_owned.as_slice())
        }
    };
    let cert_chain: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut &*cert_data)
            .collect::<std::result::Result<_, _>>()
            .context("failed to parse reverse tunnel cert PEM")?;
    let key = rustls_pemfile::private_key(&mut &*key_data)
        .context("failed to parse reverse tunnel key PEM")?
        .context("no private key found in reverse tunnel PEM")?;
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build reverse tunnel TLS server config failed")?;
    tls_config.alpn_protocols = tunnel_protocol.alpn_protocols();
    Ok(quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)
            .context("build reverse tunnel QUIC server config failed")?,
    )))
}

fn should_forward_h3_tunnel_request_header(name: &HeaderName) -> bool {
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
    )
}

fn parse_quic_addr(target_url: &str) -> Result<SocketAddr> {
    let parsed_url = Url::parse(target_url).context("invalid quic target url")?;
    if parsed_url.scheme() != "quic" {
        bail!("target url is not quic scheme");
    }
    let port = parsed_url
        .port_or_known_default()
        .ok_or_else(|| anyhow!("missing port in quic url"))?;
    let ip = parsed_url
        .host_str()
        .and_then(|h| h.parse().ok())
        .ok_or_else(|| anyhow!("quic inference_server_url host must be an IP address"))?;
    Ok(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::load_balancer_state::{RegistrationIdentity, StargateState};
    use axum::extract::Request;
    use axum::routing::{get, post};
    use axum::{Router, body::Body};
    use pylon_lib::{
        QuicHttpTunnelConfig, ReverseQuicTunnelConfig, start_quic_http_tunnel,
        start_reverse_quic_tunnel,
    };
    use tokio::net::TcpListener;

    const INFERENCE_SERVER_ID: &str = "test-backend";

    struct DropNotifier(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    async fn setup_mock_backend() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, format!("http://{addr}"))
    }

    async fn register_backend(state: &StargateState, id: &str, reverse_tunnel: bool) {
        let identity = RegistrationIdentity {
            inference_server_id: id.to_string(),
            cluster_id: id.to_string(),
            inference_server_url: "http://127.0.0.1:1".to_string(),
            routing_key: None,
            reverse_tunnel,
            coordinated_calibration: false,
        };
        state.begin_registration(&identity).await.unwrap();
    }

    async fn start_tunnel_server(
        state: Arc<StargateState>,
    ) -> (Arc<QuicHttpProxy>, SocketAddr, CancellationToken) {
        start_tunnel_server_with_insecure(state, true, TunnelTransportProtocol::Custom).await
    }

    async fn start_tunnel_server_with_insecure(
        state: Arc<StargateState>,
        quic_insecure: bool,
        tunnel_protocol: TunnelTransportProtocol,
    ) -> (Arc<QuicHttpProxy>, SocketAddr, CancellationToken) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let proxy = Arc::new(
            QuicHttpProxy::new(
                QuicTunnelConfig {
                    connect_timeout: Duration::from_secs(5),
                    request_timeout: Duration::from_secs(5),
                    direct_quic_connections: 1,
                    tls_cert_pem: None,
                    tls_key_pem: None,
                    quic_insecure,
                    tunnel_protocol,
                },
                Arc::new(crate::auth::OpenAuthenticator),
            )
            .expect("test QUIC proxy should initialize"),
        );
        let shutdown = CancellationToken::new();
        let addr = proxy
            .start_reverse_listener(
                "127.0.0.1:0".parse().expect("valid test listen address"),
                state,
                shutdown.clone(),
                TaskTracker::new(),
                None,
                None,
            )
            .await
            .expect("reverse listener should start");
        (proxy, addr, shutdown)
    }

    #[tokio::test]
    async fn reverse_listener_shutdown_waits_for_stalled_dispatch_task() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let state = Arc::new(StargateState::new());
        let proxy = Arc::new(
            QuicHttpProxy::new(
                QuicTunnelConfig {
                    connect_timeout: Duration::from_secs(5),
                    request_timeout: Duration::from_secs(5),
                    tls_cert_pem: None,
                    tls_key_pem: None,
                    quic_insecure: true,
                    tunnel_protocol: TunnelTransportProtocol::Custom,
                    direct_quic_connections: 1,
                },
                Arc::new(crate::auth::OpenAuthenticator),
            )
            .expect("test QUIC proxy should initialize"),
        );
        let shutdown = CancellationToken::new();
        let task_tracker = TaskTracker::new();
        let addr = proxy
            .start_reverse_listener(
                "127.0.0.1:0".parse().expect("valid test listen address"),
                state,
                shutdown.clone(),
                task_tracker.clone(),
                None,
                None,
            )
            .await
            .expect("reverse listener should start");

        let mut client_endpoint = Endpoint::client(
            "127.0.0.1:0"
                .parse()
                .expect("valid test client bind address"),
        )
        .expect("client endpoint should start");
        client_endpoint.set_default_client_config(
            build_client_config(None, true, TunnelTransportProtocol::Custom)
                .expect("client config"),
        );
        let connection = client_endpoint
            .connect(addr, "stargate")
            .expect("connect")
            .await
            .expect("reverse listener accepts connection");

        shutdown.cancel();
        task_tracker.close();
        tokio::time::timeout(Duration::from_secs(2), task_tracker.wait())
            .await
            .expect("tracked reverse listener tasks should exit on shutdown");
        tokio::time::timeout(Duration::from_secs(2), connection.closed())
            .await
            .expect("client connection should observe listener shutdown");
        client_endpoint.close(0u32.into(), b"test complete");
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
        let client_config = build_client_config(None, true, TunnelTransportProtocol::Custom)
            .expect("client config");
        let server_config = build_server_config(None, None, TunnelTransportProtocol::Custom)
            .expect("server config");

        assert_eq!(negotiate_alpn(client_config, server_config).await, None);
    }

    #[tokio::test]
    async fn http3_tunnel_tls_configs_negotiate_h3_alpn() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let client_config =
            build_client_config(None, true, TunnelTransportProtocol::Http3).expect("client config");
        let server_config =
            build_server_config(None, None, TunnelTransportProtocol::Http3).expect("server config");

        assert_eq!(
            negotiate_alpn(client_config, server_config).await,
            Some(b"h3".to_vec())
        );
    }

    async fn connect_reverse_tunnel(
        listener_addr: SocketAddr,
        backend_url: &str,
    ) -> pylon_lib::ReverseQuicTunnelHandle {
        connect_reverse_tunnel_insecure(listener_addr, backend_url, INFERENCE_SERVER_ID).await
    }

    async fn connect_reverse_tunnel_insecure(
        listener_addr: SocketAddr,
        backend_url: &str,
        server_id: &str,
    ) -> pylon_lib::ReverseQuicTunnelHandle {
        connect_reverse_tunnel_insecure_with_protocol(
            listener_addr,
            backend_url,
            server_id,
            TunnelTransportProtocol::Custom,
        )
        .await
    }

    async fn connect_reverse_tunnel_insecure_with_protocol(
        listener_addr: SocketAddr,
        backend_url: &str,
        server_id: &str,
        tunnel_protocol: TunnelTransportProtocol,
    ) -> pylon_lib::ReverseQuicTunnelHandle {
        let mut config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", listener_addr.port()),
            server_id.to_string(),
            backend_url.to_string(),
        );
        config.quic_insecure = true;
        config.tunnel_protocol = tunnel_protocol;
        start_reverse_quic_tunnel(config).await.unwrap()
    }

    #[tokio::test]
    async fn reverse_connection_events_wait_until_wakes_existing_waiters() {
        let events = ReverseConnectionEvents::new();
        let connected = Arc::new(AtomicBool::new(false));

        let wait = tokio::spawn({
            let events = events.clone();
            let connected = connected.clone();
            async move {
                events
                    .wait_until(Duration::from_secs(1), move || {
                        let connected = connected.clone();
                        async move { connected.load(Ordering::SeqCst) }
                    })
                    .await
            }
        });
        tokio::task::yield_now().await;
        connected.store(true, Ordering::SeqCst);
        events.notify_changed();

        let woke = tokio::time::timeout(Duration::from_millis(50), wait)
            .await
            .expect("reverse connection event should wake promptly")
            .expect("wait task should complete");
        assert!(woke);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reverse_tunnel_proxies_request_to_upstream() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
            get(|req: Request| async move {
                let echo = req
                    .headers()
                    .get("x-model")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("none")
                    .to_string();
                (StatusCode::OK, format!(r#"{{"model":"{echo}"}}"#))
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) = start_tunnel_server(state).await;

        let handle = connect_reverse_tunnel(addr, &backend_url).await;
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-1".parse().unwrap());
        headers.insert("x-model", "test-model".parse().unwrap());
        headers.insert("x-input-tokens", "5".parse().unwrap());

        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::GET,
                "/v1/models",
                headers,
                Body::from("{}"),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("test-model")
        );

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reverse_http3_tunnel_proxies_request_to_upstream() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
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
                    [(http::header::CONTENT_TYPE, "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) =
            start_tunnel_server_with_insecure(state, true, TunnelTransportProtocol::Http3).await;

        let handle = tokio::time::timeout(
            Duration::from_secs(3),
            connect_reverse_tunnel_insecure_with_protocol(
                addr,
                &backend_url,
                INFERENCE_SERVER_ID,
                TunnelTransportProtocol::Http3,
            ),
        )
        .await
        .expect("http3 reverse tunnel handshake timed out");
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-reverse".parse().unwrap());
        headers.insert("x-model", "model-h3".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/models?source=reverse-http3",
                headers,
                Body::from(r#"{"ping":true}"#),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("model-h3")
        );
        assert_eq!(
            payload.get("body_len").and_then(serde_json::Value::as_u64),
            Some(13)
        );

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reverse_webtransport_tunnel_proxies_request_to_upstream() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
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
                    [(http::header::CONTENT_TYPE, "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) =
            start_tunnel_server_with_insecure(state, true, TunnelTransportProtocol::WebTransport)
                .await;

        let handle = tokio::time::timeout(
            Duration::from_secs(3),
            connect_reverse_tunnel_insecure_with_protocol(
                addr,
                &backend_url,
                INFERENCE_SERVER_ID,
                TunnelTransportProtocol::WebTransport,
            ),
        )
        .await
        .expect("webtransport reverse tunnel handshake timed out");
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-wt-reverse".parse().unwrap());
        headers.insert("x-model", "model-wt".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/models?source=reverse-webtransport",
                headers,
                Body::from(r#"{"ping":true}"#),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("model-wt")
        );
        assert_eq!(
            payload.get("body_len").and_then(serde_json::Value::as_u64),
            Some(13)
        );

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reverse_webtransport_stalled_stream_header_does_not_block_later_requests() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
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
                    [(http::header::CONTENT_TYPE, "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) =
            start_tunnel_server_with_insecure(state, true, TunnelTransportProtocol::WebTransport)
                .await;

        let handle = tokio::time::timeout(
            Duration::from_secs(3),
            connect_reverse_tunnel_insecure_with_protocol(
                addr,
                &backend_url,
                INFERENCE_SERVER_ID,
                TunnelTransportProtocol::WebTransport,
            ),
        )
        .await
        .expect("webtransport reverse tunnel handshake timed out");
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        {
            let webtransport = {
                let pool = proxy.pool.read().await;
                match pool
                    .get(INFERENCE_SERVER_ID)
                    .expect("pooled connection")
                    .choose_healthy()
                    .expect("healthy connection")
                {
                    TunnelConnection::WebTransport(handle) => handle.clone(),
                    TunnelConnection::Custom(_) | TunnelConnection::Http3(_) => {
                        panic!("expected WebTransport tunnel connection")
                    }
                }
            };
            let (_stalled_send, _stalled_recv) = webtransport.connection.open_bi().await.unwrap();

            let mut headers = HeaderMap::new();
            headers.insert("x-request-id", "req-wt-after-stalled".parse().unwrap());
            headers.insert("x-model", "model-wt".parse().unwrap());
            headers.insert("x-input-tokens", "7".parse().unwrap());
            headers.insert("content-type", "application/json".parse().unwrap());

            let response = tokio::time::timeout(
                Duration::from_secs(3),
                proxy.proxy_request_streaming(
                    INFERENCE_SERVER_ID,
                    Method::POST,
                    "/v1/models?source=reverse-webtransport-stalled",
                    headers,
                    Body::from(r#"{"ping":true}"#),
                ),
            )
            .await
            .expect("request after stalled WebTransport stream timed out")
            .unwrap();

            assert_eq!(response.status, StatusCode::OK);
            let mut body = Vec::new();
            let mut stream = response.body_stream;
            while let Some(chunk) = stream.recv_body().await.unwrap() {
                body.extend_from_slice(&chunk);
            }
            let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                payload.get("model").and_then(serde_json::Value::as_str),
                Some("model-wt")
            );
            assert_eq!(
                payload.get("body_len").and_then(serde_json::Value::as_u64),
                Some(13)
            );
        }

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_preconnect_installs_configured_connection_set() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            backend_url,
        ))
        .await
        .unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 3,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Custom,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();

        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let connection_count = {
            let pool = proxy.pool.read().await;
            let connection_set = pool.get(INFERENCE_SERVER_ID).expect("pooled connection");
            assert!(connection_set.is_healthy());
            connection_set.len()
        };
        assert_eq!(connection_count, 3);

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-direct-set".parse().unwrap());
        headers.insert("x-model", "model-direct-set".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::GET,
                "/health",
                headers,
                Body::empty(),
            )
            .await
            .unwrap();

        assert_eq!(response.status, StatusCode::OK);

        let first_connection = {
            let pool = proxy.pool.read().await;
            let connection_set = pool.get(INFERENCE_SERVER_ID).expect("pooled connection");
            match &connection_set.inner.connections[0] {
                TunnelConnection::Custom(connection) => connection.clone(),
                TunnelConnection::Http3(_) | TunnelConnection::WebTransport(_) => {
                    panic!("expected custom tunnel connection")
                }
            }
        };
        first_connection.close(0u32.into(), b"test partial direct set close");
        assert!(
            proxy.has_healthy_connection(INFERENCE_SERVER_ID).await,
            "partial direct connection set should remain usable"
        );
        assert!(
            proxy
                .connection_set_needs_replenishment(INFERENCE_SERVER_ID)
                .await,
            "partial direct connection set should request replenishment"
        );

        proxy
            .reconnect_direct(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();
        assert!(proxy.has_healthy_connection(INFERENCE_SERVER_ID).await);
        assert!(
            !proxy
                .connection_set_needs_replenishment(INFERENCE_SERVER_ID)
                .await
        );
        tunnel.shutdown().await;
    }

    #[test]
    fn h3_tunnel_request_filter_strips_hop_headers_case_insensitively()
    -> std::result::Result<(), axum::http::header::InvalidHeaderName> {
        assert!(!should_forward_h3_tunnel_request_header(
            &HeaderName::from_bytes(b"Connection")?
        ));
        assert!(!should_forward_h3_tunnel_request_header(
            &HeaderName::from_bytes(b"Proxy-Connection")?
        ));
        assert!(!should_forward_h3_tunnel_request_header(
            &HeaderName::from_bytes(b"Host")?
        ));
        assert!(should_forward_h3_tunnel_request_header(
            &HeaderName::from_bytes(b"X-Request-Id")?
        ));
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_http3_tunnel_proxies_request_to_upstream() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
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
                    [(http::header::CONTENT_TYPE, "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut tunnel_config =
            QuicHttpTunnelConfig::new("127.0.0.1:0".parse().unwrap(), backend_url);
        tunnel_config.tunnel_protocol = TunnelTransportProtocol::Http3;
        let tunnel = start_quic_http_tunnel(tunnel_config).await.unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Http3,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-direct".parse().unwrap());
        headers.insert("x-model", "model-h3".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = tokio::time::timeout(
            Duration::from_secs(3),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/models?source=http3",
                headers,
                Body::from(r#"{"ping":true}"#),
            ),
        )
        .await
        .expect("http3 proxy request timed out")
        .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("model-h3")
        );
        assert_eq!(
            payload.get("body_len").and_then(serde_json::Value::as_u64),
            Some(13)
        );

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_webtransport_tunnel_proxies_request_to_upstream() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route(
            "/v1/models",
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
                    [(http::header::CONTENT_TYPE, "application/json")],
                    format!(r#"{{"model":"{model}","body_len":{}}}"#, body.len()),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut tunnel_config =
            QuicHttpTunnelConfig::new("127.0.0.1:0".parse().unwrap(), backend_url);
        tunnel_config.tunnel_protocol = TunnelTransportProtocol::WebTransport;
        let tunnel = start_quic_http_tunnel(tunnel_config).await.unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::WebTransport,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-wt-direct".parse().unwrap());
        headers.insert("x-model", "model-wt".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = tokio::time::timeout(
            Duration::from_secs(3),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/models?source=webtransport",
                headers,
                Body::from(r#"{"ping":true}"#),
            ),
        )
        .await
        .expect("webtransport proxy request timed out")
        .unwrap();

        assert_eq!(response.status, StatusCode::OK);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload.get("model").and_then(serde_json::Value::as_str),
            Some("model-wt")
        );
        assert_eq!(
            payload.get("body_len").and_then(serde_json::Value::as_u64),
            Some(13)
        );

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_webtransport_connect_response_uses_connect_timeout() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server_config =
            build_server_config(None, None, TunnelTransportProtocol::WebTransport).unwrap();
        let server = Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let incoming = server.accept().await.expect("server should accept");
            let connection = incoming.await.expect("server connection should complete");
            let mut builder = h3::server::builder();
            builder
                .enable_webtransport(true)
                .enable_extended_connect(true)
                .enable_datagram(true)
                .max_webtransport_sessions(1);
            let mut h3_connection: H3ServerConnection = builder
                .build(h3_quinn::Connection::new(connection))
                .await
                .expect("h3 server connection");
            let resolver = h3_connection
                .accept()
                .await
                .expect("accept CONNECT")
                .expect("CONNECT request");
            let (_request, _stream) = resolver.resolve_request().await.expect("resolve CONNECT");
            futures::future::pending::<()>().await;
        });

        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_millis(50),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::WebTransport,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            proxy.preconnect(INFERENCE_SERVER_ID, &format!("quic://{server_addr}")),
        )
        .await
        .expect("preconnect should return after the connect timeout");
        let error = result.expect_err("WebTransport CONNECT response should time out");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("direct tunnel setup timed out"),
            "unexpected error chain: {error_chain}"
        );

        server_task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_http3_response_body_survives_pool_eviction() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let release_body = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/v1/stream",
            post({
                let release_body = release_body.clone();
                move |_req: Request| {
                    let release_body = release_body.clone();
                    async move {
                        let body_stream = futures::stream::once(future::ready(Ok::<
                            _,
                            std::convert::Infallible,
                        >(
                            bytes::Bytes::from_static(b"first-"),
                        )))
                        .chain(futures::stream::once(async move {
                            release_body.notified().await;
                            Ok::<_, std::convert::Infallible>(bytes::Bytes::from_static(b"second"))
                        }));
                        http::Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from_stream(body_stream))
                            .unwrap()
                    }
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut tunnel_config =
            QuicHttpTunnelConfig::new("127.0.0.1:0".parse().unwrap(), backend_url);
        tunnel_config.tunnel_protocol = TunnelTransportProtocol::Http3;
        let tunnel = start_quic_http_tunnel(tunnel_config).await.unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Http3,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-evict-body".parse().unwrap());
        headers.insert("x-model", "model-h3".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/stream",
                headers,
                Body::from(r#"{"stream":true}"#),
            )
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);

        proxy.pool.write().await.remove(INFERENCE_SERVER_ID);
        release_body.notify_waiters();

        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        assert_eq!(body, b"first-second");

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_webtransport_response_body_survives_pool_eviction() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let release_body = Arc::new(tokio::sync::Notify::new());
        let app = Router::new().route(
            "/v1/stream",
            post({
                let release_body = release_body.clone();
                move |_req: Request| {
                    let release_body = release_body.clone();
                    async move {
                        let body_stream = futures::stream::once(future::ready(Ok::<
                            _,
                            std::convert::Infallible,
                        >(
                            bytes::Bytes::from_static(b"first-"),
                        )))
                        .chain(futures::stream::once(async move {
                            release_body.notified().await;
                            Ok::<_, std::convert::Infallible>(bytes::Bytes::from_static(b"second"))
                        }));
                        http::Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from_stream(body_stream))
                            .unwrap()
                    }
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut tunnel_config =
            QuicHttpTunnelConfig::new("127.0.0.1:0".parse().unwrap(), backend_url);
        tunnel_config.tunnel_protocol = TunnelTransportProtocol::WebTransport;
        let tunnel = start_quic_http_tunnel(tunnel_config).await.unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::WebTransport,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-wt-evict-body".parse().unwrap());
        headers.insert("x-model", "model-wt".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());
        let response = proxy
            .proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/stream",
                headers,
                Body::from(r#"{"stream":true}"#),
            )
            .await
            .unwrap();
        assert_eq!(response.status, StatusCode::OK);

        proxy.pool.write().await.remove(INFERENCE_SERVER_ID);
        release_body.notify_waiters();

        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        assert_eq!(body, b"first-second");

        tunnel.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_http3_tunnel_returns_header_errors_before_request_body_finishes() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut tunnel_config = QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:1".to_string(),
        );
        tunnel_config.tunnel_protocol = TunnelTransportProtocol::Http3;
        let tunnel = start_quic_http_tunnel(tunnel_config).await.unwrap();
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(2),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Http3,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-h3-early-error".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let (release_body_tx, release_body_rx) = tokio::sync::oneshot::channel();
        let body_stream = futures::stream::once(future::ready(Ok::<_, std::convert::Infallible>(
            bytes::Bytes::from_static(br#"{"stream":true"#),
        )))
        .chain(futures::stream::once(async move {
            let _ = release_body_rx.await;
            Ok::<_, std::convert::Infallible>(bytes::Bytes::new())
        }));

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/chat/completions",
                headers,
                Body::from_stream(body_stream),
            ),
        )
        .await
        .expect("http3 proxy should return response headers before request body finishes")
        .unwrap();
        let _ = release_body_tx.send(());

        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let mut body = Vec::new();
        let mut stream = response.body_stream;
        while let Some(chunk) = stream.recv_body().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["type"], "about:blank");
        assert_eq!(payload["title"], "Bad Request");
        assert_eq!(payload["status"], 400);
        assert_eq!(payload["detail"], "missing required x-model header");

        tunnel.shutdown().await;
    }

    #[tokio::test]
    async fn cancelled_request_body_send_finish_aborts_upload_task() {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let upload_task = tokio::spawn(async move {
            let _drop_notifier = DropNotifier(Some(dropped_tx));
            let _ = entered_tx.send(());
            std::future::pending::<Result<()>>().await
        });

        entered_rx.await.expect("upload task should start");

        {
            let finish = RequestBodySendTask::new(
                "cancelled request body",
                Duration::from_secs(30),
                upload_task,
            )
            .finish();
            tokio::pin!(finish);
            tokio::select! {
                biased;
                _ = &mut finish => panic!("pending upload should not finish before cancellation"),
                _ = tokio::task::yield_now() => {}
            }
        }

        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("cancelled EOF finalization should abort the upload task")
            .expect("upload drop notifier should send");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_tunnel_returns_body_send_error_before_header_timeout() {
        body_send_error_is_returned_before_header_timeout(TunnelTransportProtocol::Custom).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http3_tunnel_does_not_wait_for_header_timeout_after_body_send_error() {
        body_send_error_is_returned_before_header_timeout(TunnelTransportProtocol::Http3).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webtransport_tunnel_returns_body_send_error_before_header_timeout() {
        body_send_error_is_returned_before_header_timeout(TunnelTransportProtocol::WebTransport)
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_tunnel_reports_body_send_error_at_response_eof() {
        body_send_error_is_returned_at_response_eof(TunnelTransportProtocol::Custom).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http3_tunnel_reports_body_send_error_at_response_eof() {
        body_send_error_is_returned_at_response_eof(TunnelTransportProtocol::Http3).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn webtransport_tunnel_reports_body_send_error_at_response_eof() {
        body_send_error_is_returned_at_response_eof(TunnelTransportProtocol::WebTransport).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_tunnel_does_not_wait_forever_for_stalled_request_body_at_response_eof() {
        stalled_body_send_does_not_block_response_eof(TunnelTransportProtocol::Custom).await;
    }

    struct EarlySuccessBodySendErrorPeer {
        addr: SocketAddr,
        release_tx: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    }

    fn start_early_success_body_send_error_peer(
        tunnel_protocol: TunnelTransportProtocol,
    ) -> EarlySuccessBodySendErrorPeer {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server_config = build_server_config(None, None, tunnel_protocol)
            .expect("test server config should build");
        let server = Endpoint::server(
            server_config,
            "127.0.0.1:0"
                .parse()
                .expect("valid test server bind address"),
        )
        .expect("test server endpoint should start");
        let addr = server
            .local_addr()
            .expect("test server endpoint should expose local address");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let incoming = server.accept().await.expect("server should accept");
            let connection = incoming.await.expect("server connection should complete");
            match tunnel_protocol {
                TunnelTransportProtocol::Custom => {
                    accept_early_success_custom_request(connection, release_rx).await
                }
                TunnelTransportProtocol::Http3 => {
                    accept_early_success_h3_request(connection, release_rx).await
                }
                TunnelTransportProtocol::WebTransport => {
                    accept_early_success_webtransport_request(connection, release_rx).await
                }
            }
        });
        EarlySuccessBodySendErrorPeer {
            addr,
            release_tx,
            task,
        }
    }

    async fn accept_early_success_custom_request(
        connection: Connection,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let (server_send, quinn_recv) = connection.accept_bi().await.expect("request stream");
        let mut recv_stream = RecvStream::new(quinn_recv);
        recv_stream.recv_header().await.expect("request headers");
        let mut send_stream = SendStream::new(server_send);
        let mut response_headers = HeaderMap::new();
        response_headers.insert("x-status", HeaderValue::from_static("200"));
        send_stream
            .send_header(response_headers)
            .await
            .expect("send response headers");
        send_stream.finish().expect("finish response");
        let _ = release_rx.await;
    }

    async fn accept_early_success_h3_request(
        connection: Connection,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut h3_connection: H3ServerConnection = h3::server::builder()
            .build(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 server connection");
        let resolver = h3_connection
            .accept()
            .await
            .expect("accept h3 request")
            .expect("h3 request");
        let (_request, mut stream) = resolver
            .resolve_request()
            .await
            .expect("resolve h3 request");
        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("build h3 response");
        stream
            .send_response(response)
            .await
            .expect("send h3 response headers");
        stream.finish().await.expect("finish h3 response");
        let _ = (&h3_connection, &stream);
        let _ = release_rx.await;
    }

    async fn accept_early_success_webtransport_request(
        connection: Connection,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut builder = h3::server::builder();
        builder
            .enable_webtransport(true)
            .enable_extended_connect(true)
            .enable_datagram(true)
            .max_webtransport_sessions(1);
        let mut h3_connection: H3ServerConnection = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .expect("WebTransport h3 server connection");
        let resolver = h3_connection
            .accept()
            .await
            .expect("accept WebTransport CONNECT")
            .expect("WebTransport CONNECT request");
        let (_request, mut connect_stream) = resolver
            .resolve_request()
            .await
            .expect("resolve WebTransport CONNECT");
        let session_id = connect_stream.id().into_inner();
        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("build WebTransport CONNECT response");
        connect_stream
            .send_response(response)
            .await
            .expect("send WebTransport CONNECT response");

        let (mut quinn_send, mut quinn_recv) = connection
            .accept_bi()
            .await
            .expect("WebTransport request stream");
        let stream_session_id = stargate_protocol::read_webtransport_bidi_header(&mut quinn_recv)
            .await
            .expect("WebTransport bidi header");
        assert_eq!(stream_session_id, session_id);
        stargate_protocol::read_webtransport_http_request_head(&mut quinn_recv)
            .await
            .expect("WebTransport request head");
        let response_head = WebTransportHttpResponseHead {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
        };
        stargate_protocol::write_webtransport_http_response_head(&mut quinn_send, &response_head)
            .await
            .expect("send WebTransport response head");
        stargate_protocol::finish_webtransport_http_stream(&mut quinn_send)
            .expect("finish WebTransport response");
        let _ = (&h3_connection, &connect_stream, &quinn_send);
        let _ = release_rx.await;
    }

    struct InertBodySendErrorPeer {
        addr: SocketAddr,
        headers_seen_rx: tokio::sync::oneshot::Receiver<()>,
        release_tx: tokio::sync::oneshot::Sender<()>,
        task: tokio::task::JoinHandle<()>,
    }

    fn start_inert_body_send_error_peer(
        tunnel_protocol: TunnelTransportProtocol,
    ) -> InertBodySendErrorPeer {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let server_config = build_server_config(None, None, tunnel_protocol)
            .expect("test server config should build");
        let server = Endpoint::server(
            server_config,
            "127.0.0.1:0"
                .parse()
                .expect("valid test server bind address"),
        )
        .expect("test server endpoint should start");
        let addr = server
            .local_addr()
            .expect("test server endpoint should expose local address");
        let (headers_seen_tx, headers_seen_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let incoming = server.accept().await.expect("server should accept");
            let connection = incoming.await.expect("server connection should complete");
            match tunnel_protocol {
                TunnelTransportProtocol::Custom => {
                    accept_inert_custom_request(connection, headers_seen_tx, release_rx).await
                }
                TunnelTransportProtocol::Http3 => {
                    accept_inert_h3_request(connection, headers_seen_tx, release_rx).await
                }
                TunnelTransportProtocol::WebTransport => {
                    accept_inert_webtransport_request(connection, headers_seen_tx, release_rx).await
                }
            }
        });
        InertBodySendErrorPeer {
            addr,
            headers_seen_rx,
            release_tx,
            task,
        }
    }

    async fn accept_inert_custom_request(
        connection: Connection,
        headers_seen_tx: tokio::sync::oneshot::Sender<()>,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let (server_send, quinn_recv) = connection.accept_bi().await.expect("request stream");
        let keep_response_send_open = server_send;
        let mut recv_stream = RecvStream::new(quinn_recv);
        recv_stream.recv_header().await.expect("request headers");
        let _ = headers_seen_tx.send(());
        let _ = release_rx.await;
        // Keep the server-side response stream in the task state until the
        // synthetic request-body failure has been observed.
        let _ = &keep_response_send_open;
    }

    async fn accept_inert_h3_request(
        connection: Connection,
        headers_seen_tx: tokio::sync::oneshot::Sender<()>,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut h3_connection: H3ServerConnection = h3::server::builder()
            .build(h3_quinn::Connection::new(connection))
            .await
            .expect("h3 server connection");
        let resolver = h3_connection
            .accept()
            .await
            .expect("accept h3 request")
            .expect("h3 request");
        let (_request, stream) = resolver
            .resolve_request()
            .await
            .expect("resolve h3 request");
        let keep_request_stream_open = stream;
        let _ = headers_seen_tx.send(());
        let _ = release_rx.await;
        // Keep H3 state alive so the peer does not synthesize a response or
        // reset before the local body producer error wins.
        let _ = (&h3_connection, &keep_request_stream_open);
    }

    async fn accept_inert_webtransport_request(
        connection: Connection,
        headers_seen_tx: tokio::sync::oneshot::Sender<()>,
        release_rx: tokio::sync::oneshot::Receiver<()>,
    ) {
        let mut builder = h3::server::builder();
        builder
            .enable_webtransport(true)
            .enable_extended_connect(true)
            .enable_datagram(true)
            .max_webtransport_sessions(1);
        let mut h3_connection: H3ServerConnection = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .expect("WebTransport h3 server connection");
        let resolver = h3_connection
            .accept()
            .await
            .expect("accept WebTransport CONNECT")
            .expect("WebTransport CONNECT request");
        let (_request, mut connect_stream) = resolver
            .resolve_request()
            .await
            .expect("resolve WebTransport CONNECT");
        let session_id = connect_stream.id().into_inner();
        let response = http::Response::builder()
            .status(StatusCode::OK)
            .body(())
            .expect("build WebTransport CONNECT response");
        connect_stream
            .send_response(response)
            .await
            .expect("send WebTransport CONNECT response");

        let (quinn_send, mut quinn_recv) = connection
            .accept_bi()
            .await
            .expect("WebTransport request stream");
        let keep_response_send_open = quinn_send;
        let stream_session_id = stargate_protocol::read_webtransport_bidi_header(&mut quinn_recv)
            .await
            .expect("WebTransport bidi header");
        assert_eq!(stream_session_id, session_id);
        stargate_protocol::read_webtransport_http_request_head(&mut quinn_recv)
            .await
            .expect("WebTransport request head");
        let _ = headers_seen_tx.send(());
        let _ = release_rx.await;
        // Keep the CONNECT session and server-side response stream open until
        // the synthetic request-body failure has been observed.
        let _ = (&h3_connection, &connect_stream, &keep_response_send_open);
    }

    async fn body_send_error_is_returned_before_header_timeout(
        tunnel_protocol: TunnelTransportProtocol,
    ) {
        let peer = start_inert_body_send_error_peer(tunnel_protocol);
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(5),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .expect("test QUIC proxy should initialize");
        proxy
            .preconnect(INFERENCE_SERVER_ID, &format!("quic://{}", peer.addr))
            .await
            .expect("proxy should preconnect to inert test peer");

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("req-body-error"));
        headers.insert("x-model", HeaderValue::from_static("model-body-error"));
        headers.insert("x-input-tokens", HeaderValue::from_static("7"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let body_stream = futures::stream::once(async move {
            let _ = peer.headers_seen_rx.await;
            Err::<bytes::Bytes, std::io::Error>(std::io::Error::other("synthetic body failure"))
        });

        let result = tokio::time::timeout(
            Duration::from_millis(500),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/chat/completions",
                headers,
                Body::from_stream(body_stream),
            ),
        )
        .await
        .expect("body send error should be returned before the header timeout");
        let error = match result {
            Ok(_) => panic!("body send should fail before response headers arrive"),
            Err(error) => error,
        };
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("failed to send")
                && error_chain.contains("synthetic body failure"),
            "unexpected error chain: {error_chain}"
        );

        let _ = peer.release_tx.send(());
        peer.task
            .await
            .expect("inert body-send-error peer task should finish");
    }

    async fn body_send_error_is_returned_at_response_eof(tunnel_protocol: TunnelTransportProtocol) {
        let peer = start_early_success_body_send_error_peer(tunnel_protocol);
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(5),
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol,
                direct_quic_connections: 1,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .expect("test QUIC proxy should initialize");
        proxy
            .preconnect(INFERENCE_SERVER_ID, &format!("quic://{}", peer.addr))
            .await
            .expect("proxy should preconnect to early-success test peer");

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("req-body-error-after-headers"),
        );
        headers.insert("x-model", HeaderValue::from_static("model-body-error"));
        headers.insert("x-input-tokens", HeaderValue::from_static("7"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let (release_body_tx, release_body_rx) = tokio::sync::oneshot::channel();
        let body_stream = futures::stream::once(future::ready(Ok::<_, std::io::Error>(
            bytes::Bytes::from_static(b"partial request body"),
        )))
        .chain(futures::stream::once(async move {
            let _ = release_body_rx.await;
            Err::<bytes::Bytes, std::io::Error>(std::io::Error::other("synthetic body failure"))
        }));

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/chat/completions",
                headers,
                Body::from_stream(body_stream),
            ),
        )
        .await
        .expect("early success response should arrive before request body finishes")
        .expect("proxy request should return early success response");

        let _ = release_body_tx.send(());
        let mut stream = response.body_stream;
        let error = tokio::time::timeout(Duration::from_secs(1), stream.recv_body())
            .await
            .expect("response EOF should wait for request body send result")
            .expect_err("request body failure should surface at response EOF");
        let error_chain = format!("{error:#}");
        assert!(
            error_chain.contains("failed to send")
                && error_chain.contains("synthetic body failure"),
            "unexpected error chain: {error_chain}"
        );

        let _ = peer.release_tx.send(());
        peer.task
            .await
            .expect("early-success body-send-error peer task should finish");
    }

    async fn stalled_body_send_does_not_block_response_eof(
        tunnel_protocol: TunnelTransportProtocol,
    ) {
        let peer = start_early_success_body_send_error_peer(tunnel_protocol);
        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_millis(50),
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol,
                direct_quic_connections: 1,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .expect("test QUIC proxy should initialize");
        proxy
            .preconnect(INFERENCE_SERVER_ID, &format!("quic://{}", peer.addr))
            .await
            .expect("proxy should preconnect to early-success test peer");

        let mut headers = HeaderMap::new();
        headers.insert(
            "x-request-id",
            HeaderValue::from_static("req-body-stalled-after-headers"),
        );
        headers.insert("x-model", HeaderValue::from_static("model-body-stalled"));
        headers.insert("x-input-tokens", HeaderValue::from_static("7"));
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        let body_stream = futures::stream::once(future::ready(Ok::<_, std::io::Error>(
            bytes::Bytes::from_static(b"partial request body"),
        )))
        .chain(futures::stream::pending::<
            std::result::Result<bytes::Bytes, std::io::Error>,
        >());

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            proxy.proxy_request_streaming(
                INFERENCE_SERVER_ID,
                Method::POST,
                "/v1/chat/completions",
                headers,
                Body::from_stream(body_stream),
            ),
        )
        .await
        .expect("early success response should arrive before stalled body finishes")
        .expect("proxy request should return early success response");

        let mut stream = response.body_stream;
        let eof = tokio::time::timeout(Duration::from_millis(500), stream.recv_body())
            .await
            .expect("response EOF should not wait forever for a stalled request body")
            .expect("response EOF should not fail for a merely stalled request body");
        assert!(eof.is_none());

        let _ = peer.release_tx.send(());
        peer.task
            .await
            .expect("early-success stalled-body peer task should finish");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn response_header_timeout_uses_remaining_budget_after_stream_setup() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let mut server_config =
            build_server_config(None, None, TunnelTransportProtocol::Custom).unwrap();
        let mut transport = quinn::TransportConfig::default();
        // Limit the server to one open request stream so the second request
        // spends part of its timeout budget waiting for stream capacity.
        transport.max_concurrent_bidi_streams(1_u8.into());
        server_config.transport_config(Arc::new(transport));
        let server = Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let server_addr = server.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let incoming = server.accept().await.expect("server should accept");
            let connection = incoming.await.expect("server connection should complete");
            let (_first_send, mut first_recv) = connection.accept_bi().await.expect("first stream");
            let first_task = tokio::spawn(async move {
                let _ = first_recv.read_to_end(1024).await;
            });

            let (second_send, second_recv) = connection.accept_bi().await.expect("second stream");
            first_task.await.unwrap();
            let mut recv_stream = RecvStream::new(second_recv);
            let mut send_stream = SendStream::new(second_send);
            recv_stream.recv_header().await.expect("request headers");
            tokio::time::sleep(Duration::from_millis(120)).await;
            let mut response_headers = HeaderMap::new();
            response_headers.insert("x-status", HeaderValue::from_static("200"));
            send_stream
                .send_header(response_headers)
                .await
                .expect("send response headers");
            send_stream.finish().expect("finish response");
        });

        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_millis(250),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Custom,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        proxy
            .preconnect(INFERENCE_SERVER_ID, &format!("quic://{server_addr}"))
            .await
            .unwrap();

        let connection = {
            let pool = proxy.pool.read().await;
            match pool
                .get(INFERENCE_SERVER_ID)
                .expect("pooled connection")
                .choose_healthy()
                .expect("healthy connection")
            {
                TunnelConnection::Custom(connection) => connection.clone(),
                TunnelConnection::Http3(_) | TunnelConnection::WebTransport(_) => {
                    panic!("expected custom tunnel connection")
                }
            }
        };
        let first_stream = connection.open_bi().await.expect("open first stream");

        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req-header-budget".parse().unwrap());
        headers.insert("x-model", "model-budget".parse().unwrap());
        headers.insert("x-input-tokens", "7".parse().unwrap());
        let request = proxy.proxy_request_streaming(
            INFERENCE_SERVER_ID,
            Method::POST,
            "/v1/chat/completions",
            headers,
            Body::empty(),
        );
        tokio::pin!(request);
        tokio::time::sleep(Duration::from_millis(180)).await;
        // Free the only server-side request stream after setup has consumed
        // most of the request timeout, so the response-header phase should
        // inherit only the remaining budget.
        drop(first_stream);

        let result = tokio::time::timeout(Duration::from_secs(1), &mut request)
            .await
            .expect("request should complete before outer test timeout");
        let error = match result {
            Ok(_) => panic!("response header wait should use only remaining budget"),
            Err(error) => error,
        };
        assert!(
            format!("{error:#}").contains("quic request timed out"),
            "unexpected error: {error:#}"
        );

        server_task.abort();
    }

    #[tokio::test]
    async fn health_check_succeeds_through_reverse_tunnel() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { (StatusCode::OK, "ok") }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) = start_tunnel_server(state).await;

        let handle = connect_reverse_tunnel(addr, &backend_url).await;
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        let rtt = proxy.health_check_rtt(INFERENCE_SERVER_ID).await.unwrap();
        assert!(rtt.as_millis() < 1000);

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test]
    async fn handshake_nack_for_unregistered_server() {
        let state = Arc::new(StargateState::new());
        let (_proxy, addr, shutdown) = start_tunnel_server(state).await;

        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let mut config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", addr.port()),
            "unregistered-backend".to_string(),
            backend_url,
        );
        config.quic_insecure = true;
        let result = start_reverse_quic_tunnel(config).await;

        assert!(result.is_err(), "expected handshake to be rejected");

        shutdown.cancel();
    }

    #[tokio::test]
    async fn url_change_evicts_stale_pool_entry() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) = start_tunnel_server(state).await;

        let handle = connect_reverse_tunnel(addr, &backend_url).await;
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await,
        );
        assert!(proxy.has_healthy_connection(INFERENCE_SERVER_ID).await);

        let mut watcher = ConnectionWatcher::new(proxy.clone(), Duration::from_millis(100));
        watcher.states.insert(
            INFERENCE_SERVER_ID.to_string(),
            ConnState::Connected {
                url: "quic://1.2.3.4:9999".to_string(),
            },
        );

        let result = watcher
            .ensure_connected(INFERENCE_SERVER_ID, "quic://5.6.7.8:1111", true)
            .await;
        assert!(matches!(result, EnsureConnectedResult::ReverseDisconnected));
        assert!(
            !proxy.has_healthy_connection(INFERENCE_SERVER_ID).await,
            "stale pool entry should have been evicted on URL change"
        );

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test]
    async fn duplicate_reverse_connection_rejected() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (proxy, addr, shutdown) = start_tunnel_server(state).await;

        let handle1 = connect_reverse_tunnel(addr, &backend_url).await;
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await,
            "first reverse connection should be accepted"
        );

        let (listener2, backend_url2) = setup_mock_backend().await;
        let app2 = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener2, app2).await;
        });

        let mut dup_config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", addr.port()),
            INFERENCE_SERVER_ID.to_string(),
            backend_url2,
        );
        dup_config.quic_insecure = true;
        let result = start_reverse_quic_tunnel(dup_config).await;
        assert!(
            result.is_err(),
            "second connection with same id should be rejected while first is active"
        );

        handle1.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test]
    async fn await_reverse_connection_ignores_closed_pool_entry() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Duration::from_secs(5),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: TunnelTransportProtocol::Custom,
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            backend_url,
        ))
        .await
        .unwrap();

        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();
        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(1))
                .await
        );

        tunnel.shutdown().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while proxy.has_healthy_connection(INFERENCE_SERVER_ID).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pooled connection should close");

        assert!(
            !proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_millis(50))
                .await,
            "closed pool entry must not satisfy reverse connection wait"
        );
    }

    #[test]
    fn build_client_config_insecure_succeeds_without_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = build_client_config(None, true, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn build_client_config_secure_fails_without_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = build_client_config(None, false, TunnelTransportProtocol::Custom);
        assert!(result.is_err());
    }

    #[test]
    fn build_client_config_secure_succeeds_with_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert_pem, _key_pem) = stargate_tls::generate_self_signed_cert().unwrap();
        let result = build_client_config(Some(&cert_pem), false, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn build_server_config_self_signed_when_none() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let result = build_server_config(None, None, TunnelTransportProtocol::Custom);
        assert!(result.is_ok());
    }

    #[test]
    fn build_server_config_uses_provided_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert_pem, key_pem) = stargate_tls::generate_self_signed_cert().unwrap();
        let result = build_server_config(
            Some(&cert_pem),
            Some(&key_pem),
            TunnelTransportProtocol::Custom,
        );
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reverse_tunnel_works_with_secure_client_and_provided_cert() {
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { (StatusCode::OK, "ok") }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (cert_pem, key_pem) = stargate_tls::generate_self_signed_cert().unwrap();

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;

        let proxy = Arc::new(
            QuicHttpProxy::new(
                QuicTunnelConfig {
                    connect_timeout: Duration::from_secs(5),
                    request_timeout: Duration::from_secs(5),
                    direct_quic_connections: 1,
                    tls_cert_pem: Some(cert_pem.clone()),
                    tls_key_pem: Some(key_pem),
                    quic_insecure: false,
                    tunnel_protocol: Default::default(),
                },
                Arc::new(crate::auth::OpenAuthenticator),
            )
            .unwrap(),
        );
        let shutdown = CancellationToken::new();
        let addr = proxy
            .start_reverse_listener(
                "127.0.0.1:0".parse().unwrap(),
                state,
                shutdown.clone(),
                TaskTracker::new(),
                None,
                None,
            )
            .await
            .unwrap();

        let mut config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", addr.port()),
            INFERENCE_SERVER_ID.to_string(),
            backend_url,
        );
        config.tls_cert_pem = Some(cert_pem);
        config.quic_insecure = false;
        let handle = start_reverse_quic_tunnel(config).await.unwrap();

        assert!(
            proxy
                .await_reverse_connection(INFERENCE_SERVER_ID, Duration::from_secs(2))
                .await
        );

        let rtt = proxy.health_check_rtt(INFERENCE_SERVER_ID).await.unwrap();
        assert!(rtt.as_millis() < 1000);

        handle.shutdown().await;
        shutdown.cancel();
    }

    #[tokio::test]
    async fn reverse_tunnel_secure_client_rejects_unknown_server_cert() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let state = Arc::new(StargateState::new());
        register_backend(&state, INFERENCE_SERVER_ID, true).await;
        let (_proxy, addr, shutdown) = start_tunnel_server(state).await;

        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let (different_cert, _) = stargate_tls::generate_self_signed_cert().unwrap();
        let mut config = ReverseQuicTunnelConfig::new(
            format!("127.0.0.1:{}", addr.port()),
            INFERENCE_SERVER_ID.to_string(),
            backend_url,
        );
        config.tls_cert_pem = Some(different_cert);
        config.quic_insecure = false;
        let result = start_reverse_quic_tunnel(config).await;

        assert!(
            result.is_err(),
            "secure client with different CA should reject server cert"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    async fn failed_reconnect_direct_preserves_existing_connection() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let (listener, backend_url) = setup_mock_backend().await;
        let app = Router::new().route("/health", get(|| async { "ok" }));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let proxy = QuicHttpProxy::new(
            QuicTunnelConfig {
                connect_timeout: Duration::from_millis(50),
                request_timeout: Duration::from_secs(5),
                direct_quic_connections: 1,
                tls_cert_pem: None,
                tls_key_pem: None,
                quic_insecure: true,
                tunnel_protocol: Default::default(),
            },
            Arc::new(crate::auth::OpenAuthenticator),
        )
        .unwrap();
        let tunnel = start_quic_http_tunnel(QuicHttpTunnelConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            backend_url,
        ))
        .await
        .unwrap();

        proxy
            .preconnect(
                INFERENCE_SERVER_ID,
                &format!("quic://{}", tunnel.listen_addr()),
            )
            .await
            .unwrap();
        assert!(proxy.has_healthy_connection(INFERENCE_SERVER_ID).await);

        let result = proxy
            .reconnect_direct(INFERENCE_SERVER_ID, "quic://not-an-ip:50072")
            .await;

        assert!(result.is_err());
        assert!(
            proxy.has_healthy_connection(INFERENCE_SERVER_ID).await,
            "failed reconnect must not remove the existing healthy connection"
        );

        tunnel.shutdown().await;
    }
}
