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

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

use stargate_proto::REGISTRATION_HEARTBEAT_MS_METADATA;
use stargate_proto::pb::stargate_control_plane_client::StargateControlPlaneClient;
use stargate_proto::pb::{
    CalibrationState, InferenceServerAck, InferenceServerModelRegistration,
    InferenceServerRegistration, InferenceServerStatus, ModelCalibrationDirective, ModelStats,
    WatchStargatesRequest, WatchStargatesResponse,
};
pub use stargate_protocol::TunnelTransportProtocol;

pub use stargate_auth::AuthTokenProvider;

mod bringup;
mod engine_stats_stream;
mod metrics;
mod output_token_parser;
mod queue_admission;
mod quic_http_tunnel;
mod request_observer;
mod request_quality_monitor;
mod sse_message_stream;
mod stats_collector;
mod token_metrics;
pub use bringup::BringupConfig;
use bringup::{
    BringupCalibrationUpdate, BringupModelUpdate, ClusterCalibrationDirective,
    ClusterCalibrationDirectiveState, ModelBringupState, start_bringup_supervisor,
};
pub use engine_stats_stream::{
    EngineStatsStreamConfig, EngineStatsStreamHandle, EngineStatsStreamMode,
    parse_engine_stats_line_for_benchmark, start_engine_stats_stream,
};
pub use metrics::{PylonMetrics, start_metrics_server};
pub use output_token_parser::OutputTokenParserFactory;
pub use queue_admission::{PylonQueueMismatchRetryConfig, QueueAdmissionTracker};
pub use quic_http_tunnel::{
    PylonRetryConfig, QuicHttpTunnelConfig, QuicHttpTunnelHandle, ReverseQuicTunnelConfig,
    ReverseQuicTunnelHandle, TunnelError, start_quic_http_tunnel, start_reverse_quic_tunnel,
};
pub use request_observer::{
    RequestObservation, RequestObservationEndpoint, RequestObservationState,
};
pub use request_quality_monitor::RequestQualityMonitorConfig;
pub use stats_collector::{
    RequestCounterUpdate, StatsAggregatorUpdate, StatsCollectorConfig, StatsCollectorHandle,
    StatsUpdateSource, request_observation_channel, start_stats_collector,
    start_stats_collector_with_engine_stats, stats_aggregator_update_channel,
};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("QUIC tunnel failed: {0}")]
    Tunnel(#[from] TunnelError),
}

#[derive(Debug, Clone)]
pub struct InferenceServerRegistrationConfig {
    pub seeds: Vec<String>,
    pub inference_server_id: String,
    pub cluster_id: String,
    pub inference_server_url: String,
    pub upstream_http_base_url: Option<String>,
    pub min_update_interval: Duration,
    pub status: InferenceServerStatus,
    pub reverse_tunnel: bool,
    pub quic_insecure: bool,
    pub tunnel_protocol: TunnelTransportProtocol,
    pub bringup: BringupConfig,
    pub output_token_parser_factory: OutputTokenParserFactory,
    pub request_observation_tx: Option<flume::Sender<RequestObservation>>,
    pub request_quality_monitor: RequestQualityMonitorConfig,
    pub metrics: Option<Arc<PylonMetrics>>,
    pub retry: PylonRetryConfig,
    pub queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    pub queue_tracker: QueueAdmissionTracker,
    pub auth_token_provider: Option<Arc<AuthTokenProvider>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RegistrationStartPlan {
    watch_seeds: Vec<String>,
    cluster_id: String,
    upstream_http_base_url: String,
}

impl RegistrationStartPlan {
    fn from_config(config: &InferenceServerRegistrationConfig) -> Result<Self, ClientError> {
        if config.seeds.is_empty() {
            return Err(ClientError::Config("stargate seeds are empty".to_string()));
        }
        if !config.reverse_tunnel && !is_direct_inference_server_url(&config.inference_server_url) {
            return Err(ClientError::Config(
                "direct registration inference_server_url must be quic://".to_string(),
            ));
        }

        let upstream_http_base_url = config
            .upstream_http_base_url
            .clone()
            .or_else(|| {
                config
                    .reverse_tunnel
                    .then(|| infer_upstream_http_base_url(&config.inference_server_url))
                    .flatten()
            })
            .ok_or_else(|| {
                ClientError::Config(
                    "upstream_http_base_url is required when inference_server_url is not http(s)"
                        .to_string(),
                )
            })?;

        Ok(Self {
            watch_seeds: config.seeds.clone(),
            cluster_id: effective_cluster_id(&config.cluster_id, &config.inference_server_id),
            upstream_http_base_url,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct CurrentModelStats {
    // Sticky last valid mean input TPS for this backend observation. Stargate
    // sums positive active backend reports when building a shared-cluster view.
    pub last_mean_input_tps: f64,
    // Token/sec output rate for streaming generation endpoints. Embeddings item
    // cardinality is observed separately and is not exported through this field.
    pub output_tps: f64,
    pub embedding_item_tps: f64,
    pub queue_size: u64,
    pub queued_input_size: u64,

    // Cluster-scoped shared-hardware/scheduler state. Stargate currently keeps
    // the latest active backend snapshot for these fields when multiple
    // backends share a cluster.
    // Same token/sec unit as `output_tps`; embeddings item rates are not folded in.
    pub max_output_tps: f64,
    pub max_embedding_item_tps: f64,
    pub kv_cache_capacity_tokens: u64,
    pub kv_cache_used_tokens: u64,
    pub kv_cache_free_tokens: u64,
    pub num_running_queries: u64,
    pub max_engine_concurrency: Option<u64>,
    pub total_query_input_size: u64,
    pub queue_time_estimate_ms_by_priority: Option<HashMap<u32, u64>>,
    pub input_processing_queries: u64,
    pub output_generation_queries: u64,
    pub stats_observed_at_unix_ms: u64,
    pub stats_capabilities: Vec<String>,
    pub stats_sources: Vec<String>,
}

pub struct InferenceServerUpdateChannels {
    pub status: flume::Sender<InferenceServerStatus>,
    pub model_stats: flume::Sender<(String, CurrentModelStats)>,
}

#[derive(Debug, Clone)]
struct RouterRegistrationTaskConfig {
    router_addr: String,
    inference_server_id: String,
    cluster_id: String,
    inference_server_url: String,
    min_update_interval: Duration,
    reverse_tunnel: bool,
    coordinated_calibration: bool,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
    upstream_http_base_url: String,
    cluster_calibration_directive_tx: flume::Sender<ClusterCalibrationDirective>,
    output_token_parser_factory: OutputTokenParserFactory,
    request_observation_tx: Option<flume::Sender<RequestObservation>>,
    request_quality_monitor: RequestQualityMonitorConfig,
    metrics: Option<Arc<PylonMetrics>>,
    retry: PylonRetryConfig,
    queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    queue_tracker: QueueAdmissionTracker,
    cancel_token: CancellationToken,
    auth_token_provider: Option<Arc<AuthTokenProvider>>,
}

#[derive(Debug, Clone)]
struct RouterRegistrationTaskTemplate {
    inference_server_id: String,
    cluster_id: String,
    inference_server_url: String,
    min_update_interval: Duration,
    reverse_tunnel: bool,
    coordinated_calibration: bool,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
    upstream_http_base_url: String,
    cluster_calibration_directive_tx: flume::Sender<ClusterCalibrationDirective>,
    output_token_parser_factory: OutputTokenParserFactory,
    request_observation_tx: Option<flume::Sender<RequestObservation>>,
    request_quality_monitor: RequestQualityMonitorConfig,
    metrics: Option<Arc<PylonMetrics>>,
    retry: PylonRetryConfig,
    queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    queue_tracker: QueueAdmissionTracker,
    cancel_token: CancellationToken,
    auth_token_provider: Option<Arc<AuthTokenProvider>>,
}

impl RouterRegistrationTaskTemplate {
    fn from_registration_config(
        register_config: &InferenceServerRegistrationConfig,
        cluster_id: &str,
        upstream_http_base_url: &str,
        cluster_calibration_directive_tx: flume::Sender<ClusterCalibrationDirective>,
        cancel_token: &CancellationToken,
    ) -> Self {
        let inference_server_url = if register_config.reverse_tunnel {
            upstream_http_base_url.to_string()
        } else {
            register_config.inference_server_url.clone()
        };
        Self {
            inference_server_id: register_config.inference_server_id.clone(),
            cluster_id: cluster_id.to_string(),
            inference_server_url,
            min_update_interval: register_config.min_update_interval,
            reverse_tunnel: register_config.reverse_tunnel,
            coordinated_calibration: register_config.bringup.coordinated_calibration
                && register_config.bringup.enabled
                && register_config.bringup.calibration_requests > 0,
            quic_insecure: register_config.quic_insecure,
            tunnel_protocol: register_config.tunnel_protocol,
            upstream_http_base_url: upstream_http_base_url.to_string(),
            cluster_calibration_directive_tx,
            output_token_parser_factory: register_config.output_token_parser_factory.clone(),
            request_observation_tx: register_config.request_observation_tx.clone(),
            request_quality_monitor: register_config.request_quality_monitor.clone(),
            metrics: register_config.metrics.clone(),
            retry: register_config.retry.clone(),
            queue_mismatch_retry: register_config.queue_mismatch_retry.clone(),
            queue_tracker: register_config.queue_tracker.clone(),
            cancel_token: cancel_token.clone(),
            auth_token_provider: register_config.auth_token_provider.clone(),
        }
    }

    fn build_for_router(&self, router_addr: String) -> RouterRegistrationTaskConfig {
        RouterRegistrationTaskConfig {
            router_addr,
            inference_server_id: self.inference_server_id.clone(),
            cluster_id: self.cluster_id.clone(),
            inference_server_url: self.inference_server_url.clone(),
            min_update_interval: self.min_update_interval,
            reverse_tunnel: self.reverse_tunnel,
            coordinated_calibration: self.coordinated_calibration,
            quic_insecure: self.quic_insecure,
            tunnel_protocol: self.tunnel_protocol,
            upstream_http_base_url: self.upstream_http_base_url.clone(),
            cluster_calibration_directive_tx: self.cluster_calibration_directive_tx.clone(),
            output_token_parser_factory: self.output_token_parser_factory.clone(),
            request_observation_tx: self.request_observation_tx.clone(),
            request_quality_monitor: self.request_quality_monitor.clone(),
            metrics: self.metrics.clone(),
            retry: self.retry.clone(),
            queue_mismatch_retry: self.queue_mismatch_retry.clone(),
            queue_tracker: self.queue_tracker.clone(),
            cancel_token: self.cancel_token.clone(),
            auth_token_provider: self.auth_token_provider.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct ReverseTunnelLoopConfig {
    router_addr: String,
    inference_server_id: String,
    upstream_http_base_url: String,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
    output_token_parser_factory: OutputTokenParserFactory,
    request_observation_tx: Option<flume::Sender<RequestObservation>>,
    request_quality_monitor: RequestQualityMonitorConfig,
    metrics: Option<Arc<PylonMetrics>>,
    retry: PylonRetryConfig,
    queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    queue_tracker: QueueAdmissionTracker,
    auth_token_provider: Option<Arc<AuthTokenProvider>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReverseTunnelEndpoint {
    routing_target_addr: String,
    pylon_dial_addr: String,
    sni_override: Option<String>,
}

struct ReverseQuicTunnelConfigParams {
    dial_addr: String,
    sni_override: Option<String>,
    inference_server_id: String,
    upstream_http_base_url: String,
    quic_insecure: bool,
    tunnel_protocol: TunnelTransportProtocol,
    output_token_parser_factory: OutputTokenParserFactory,
    request_observation_tx: Option<flume::Sender<RequestObservation>>,
    request_quality_monitor: RequestQualityMonitorConfig,
    auth_token_provider: Option<Arc<AuthTokenProvider>>,
    retry: PylonRetryConfig,
    queue_mismatch_retry: PylonQueueMismatchRetryConfig,
    queue_tracker: QueueAdmissionTracker,
    metrics: Option<Arc<PylonMetrics>>,
}

fn build_reverse_quic_tunnel_config(
    params: ReverseQuicTunnelConfigParams,
) -> ReverseQuicTunnelConfig {
    let mut tunnel_config = ReverseQuicTunnelConfig::new(
        params.dial_addr,
        params.inference_server_id,
        params.upstream_http_base_url,
    );
    tunnel_config.quic_insecure = params.quic_insecure;
    tunnel_config.tunnel_protocol = params.tunnel_protocol;
    tunnel_config.sni_override = params.sni_override;
    tunnel_config.output_token_parser_factory = params.output_token_parser_factory;
    tunnel_config.request_observation_tx = params.request_observation_tx;
    tunnel_config.request_quality_monitor = params.request_quality_monitor;
    tunnel_config.auth_token_provider = params.auth_token_provider;
    tunnel_config.retry = params.retry;
    tunnel_config.queue_mismatch_retry = params.queue_mismatch_retry;
    tunnel_config.queue_tracker = params.queue_tracker;
    tunnel_config.metrics = params.metrics;
    tunnel_config
}

#[derive(Default)]
pub struct InferenceServerRegistrationClient {
    watch_task: Option<JoinHandle<()>>,
    bringup_task: Option<JoinHandle<()>>,
    register_task: Option<JoinHandle<()>>,
    stop_tx: Option<watch::Sender<bool>>,
    cancel_token: CancellationToken,
}

impl InferenceServerRegistrationClient {
    pub fn stop(&mut self) {
        self.request_stop();
        self.abort_owned_tasks();
    }

    pub async fn shutdown(&mut self) {
        self.request_stop();
        await_named_join_handles(self.take_owned_tasks(), REGISTRATION_TASK_SHUTDOWN_TIMEOUT).await;
    }

    fn request_stop(&mut self) {
        self.cancel_token.cancel();
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(true);
        }
    }

    fn abort_owned_tasks(&mut self) {
        for task in self.take_owned_tasks() {
            // The synchronous stop API cannot wait for cooperative shutdown, so
            // abort after sending stop signals to avoid detached background work.
            abort_named_join_handle(task);
        }
    }

    fn take_owned_tasks(&mut self) -> Vec<NamedJoinHandle> {
        let mut tasks = Vec::new();
        if let Some(task) = self.watch_task.take() {
            tasks.push(NamedJoinHandle::new("watch stargate discovery", task));
        }
        if let Some(task) = self.bringup_task.take() {
            tasks.push(NamedJoinHandle::new("bringup supervisor", task));
        }
        if let Some(task) = self.register_task.take() {
            tasks.push(NamedJoinHandle::new("registration supervisor", task));
        }
        tasks
    }

    pub fn start(
        &mut self,
        config: InferenceServerRegistrationConfig,
        model_ids: Vec<String>,
    ) -> Result<InferenceServerUpdateChannels, ClientError> {
        self.stop();
        let start_plan = RegistrationStartPlan::from_config(&config)?;

        let (stop_tx, stop_rx) = watch::channel(false);
        self.stop_tx = Some(stop_tx);
        let cancel_token = CancellationToken::new();
        self.cancel_token = cancel_token.clone();

        let (status_tx, status_rx) = flume::bounded::<InferenceServerStatus>(64);
        let (stats_tx, stats_rx) = flume::bounded::<(String, CurrentModelStats)>(256);
        let (bringup_state_tx, bringup_state_rx) = flume::bounded::<BringupModelUpdate>(256);
        let (bringup_calibration_tx, bringup_calibration_rx) =
            flume::bounded::<BringupCalibrationUpdate>(256);
        let (cluster_calibration_directive_tx, cluster_calibration_directive_rx) =
            flume::bounded::<ClusterCalibrationDirective>(256);

        let (stargate_updates_tx, mut stargate_updates_rx) = mpsc::channel::<BTreeSet<String>>(8);
        let watch_seeds = start_plan.watch_seeds.clone();
        let watch_stop_rx = stop_rx.clone();
        self.watch_task = Some(tokio::spawn(run_watch_stargate_discovery(
            watch_seeds,
            stargate_updates_tx,
            watch_stop_rx,
        )));

        let register_config = config.clone();
        let task_template = RouterRegistrationTaskTemplate::from_registration_config(
            &register_config,
            &start_plan.cluster_id,
            &start_plan.upstream_http_base_url,
            cluster_calibration_directive_tx,
            &cancel_token,
        );

        self.bringup_task = Some(start_bringup_supervisor(
            model_ids
                .iter()
                .cloned()
                .map(|model_id| bringup::BringupTaskConfig {
                    upstream_http_base_url: start_plan.upstream_http_base_url.clone(),
                    model_id,
                    config: register_config.bringup.clone(),
                    metrics: register_config.metrics.clone(),
                })
                .collect(),
            bringup_state_tx,
            bringup_calibration_tx,
            cluster_calibration_directive_rx,
            stop_rx.clone(),
        ));

        let mut register_stop_rx = stop_rx.clone();
        self.register_task = Some(tokio::spawn(async move {
            let mut active_routers = BTreeSet::<String>::new();
            let mut per_router_tasks: HashMap<String, RouterRegistrationWorker> = HashMap::new();
            let mut calibration_router: Option<String> = None;
            let mut fanout_phase =
                initial_registration_fanout_phase(task_template.coordinated_calibration);

            let shared_state = SharedInstState::new(
                register_config.status,
                &model_ids,
                SharedInstStateChannels {
                    status_rx,
                    stats_rx,
                    bringup_state_rx,
                    bringup_calibration_rx,
                },
                register_config.bringup.enabled,
                register_config.queue_tracker.clone(),
            );

            loop {
                if *register_stop_rx.borrow() {
                    break;
                }

                while let Ok(new_set) = stargate_updates_rx.try_recv() {
                    active_routers = new_set;
                }
                shared_state.drain_updates();

                fanout_phase = advance_registration_fanout_phase(
                    fanout_phase,
                    &shared_state,
                    task_template.coordinated_calibration,
                );
                let desired_routers = desired_registration_routers(
                    &active_routers,
                    fanout_phase,
                    &mut calibration_router,
                );

                let current_routers: Vec<String> = per_router_tasks.keys().cloned().collect();
                for router in current_routers {
                    if desired_routers.contains(&router) {
                        continue;
                    }
                    if let Some(worker) = per_router_tasks.remove(&router) {
                        stop_router_registration_worker(worker).await;
                    }
                }

                for router in &desired_routers {
                    if per_router_tasks.contains_key(router) {
                        continue;
                    }

                    let (worker_stop_tx, worker_stop_rx) = watch::channel(false);
                    let task_config = task_template.build_for_router(router.clone());
                    let task = tokio::spawn(run_router_registration_stream(
                        task_config,
                        shared_state.clone(),
                        register_stop_rx.clone(),
                        worker_stop_rx,
                    ));
                    per_router_tasks.insert(
                        router.clone(),
                        RouterRegistrationWorker {
                            stop_tx: worker_stop_tx,
                            task,
                        },
                    );
                }

                tokio::select! {
                    changed = register_stop_rx.changed() => {
                        if stop_channel_changed(changed, &register_stop_rx) {
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            }

            for (_, worker) in per_router_tasks {
                stop_router_registration_worker(worker).await;
            }
        }));

        Ok(InferenceServerUpdateChannels {
            status: status_tx,
            model_stats: stats_tx,
        })
    }
}

struct NamedJoinHandle {
    name: &'static str,
    task: JoinHandle<()>,
}

impl NamedJoinHandle {
    fn new(name: &'static str, task: JoinHandle<()>) -> Self {
        Self { name, task }
    }
}

fn abort_named_join_handle(task: NamedJoinHandle) {
    task.task.abort();
}

struct AbortOnDropJoinHandle {
    handle: Option<JoinHandle<()>>,
}

impl AbortOnDropJoinHandle {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn abort(&self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }

    async fn join(&mut self) -> Result<(), tokio::task::JoinError> {
        self.handle
            .as_mut()
            .expect("join handle should not be disarmed before join")
            .await
    }

    fn disarm(&mut self) {
        let _completed = self.handle.take();
    }
}

impl Drop for AbortOnDropJoinHandle {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            // A parent shutdown deadline can cancel the join wait; abort before
            // dropping the handle so the child task is not detached.
            handle.abort();
        }
    }
}

async fn await_named_join_handles(tasks: Vec<NamedJoinHandle>, timeout: Duration) {
    for task in tasks {
        await_named_join_handle(task, timeout).await;
    }
}

async fn await_named_join_handle(task: NamedJoinHandle, timeout: Duration) {
    await_named_join_handle_until(task, tokio::time::Instant::now() + timeout).await;
}

async fn await_named_join_handle_until(task: NamedJoinHandle, deadline: tokio::time::Instant) {
    let name = task.name;
    let mut handle = AbortOnDropJoinHandle::new(task.task);
    let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
        Some(duration) if !duration.is_zero() => duration,
        _ => {
            tracing::warn!(task = name, "task did not stop before shutdown deadline");
            // Cooperative shutdown missed the shared deadline; abort is the final fallback.
            handle.abort();
            let result = handle.join().await;
            handle.disarm();
            finish_joined_task(name, result);
            return;
        }
    };

    match tokio::time::timeout(remaining, handle.join()).await {
        Ok(result) => {
            handle.disarm();
            finish_joined_task(name, result);
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                timeout_ms = remaining.as_millis(),
                "task did not stop before shutdown timeout"
            );
            // Cooperative shutdown missed the timeout; abort is the final fallback.
            handle.abort();
            let result = handle.join().await;
            handle.disarm();
            finish_joined_task(name, result);
        }
    }
}

fn finish_joined_task(name: &'static str, result: Result<(), tokio::task::JoinError>) {
    match result {
        Ok(()) => {}
        Err(error) if error.is_cancelled() => {}
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => {
            tracing::warn!(task = name, error = %error, "task join failed");
        }
    }
}

struct RouterRegistrationWorker {
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
}

async fn stop_router_registration_worker(worker: RouterRegistrationWorker) {
    let _ = worker.stop_tx.send(true);
    await_named_join_handle(
        NamedJoinHandle::new("router registration stream", worker.task),
        REGISTRATION_TASK_SHUTDOWN_TIMEOUT,
    )
    .await;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct WatchEndpointSnapshot {
    registration_routers: BTreeSet<String>,
    watch_urls: BTreeSet<String>,
}

#[derive(Debug)]
enum WatchEndpointEvent {
    Snapshot(WatchEndpointSnapshot),
    Disconnected,
}

#[derive(Debug)]
struct WatchEndpointUpdate {
    watch_url: String,
    generation: u64,
    event: WatchEndpointEvent,
}

#[derive(Debug)]
enum WatchEndpointState {
    Connecting,
    Live(WatchEndpointSnapshot),
    Disconnected,
}

impl WatchEndpointState {
    fn snapshot(&self) -> Option<&WatchEndpointSnapshot> {
        match self {
            WatchEndpointState::Live(snapshot) => Some(snapshot),
            WatchEndpointState::Connecting | WatchEndpointState::Disconnected => None,
        }
    }

    fn has_snapshot(&self) -> bool {
        matches!(self, WatchEndpointState::Live(_))
    }
}

struct WatchedEndpoint {
    generation: u64,
    stop_tx: watch::Sender<bool>,
    task: JoinHandle<()>,
    state: WatchEndpointState,
}

const INITIAL_WATCH_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);
const REVERSE_TUNNEL_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REGISTRATION_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

async fn run_watch_stargate_discovery(
    seeds: Vec<String>,
    stargate_updates_tx: mpsc::Sender<BTreeSet<String>>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let seeds = normalize_string_set(seeds);
    let (endpoint_updates_tx, mut endpoint_updates_rx) = mpsc::channel::<WatchEndpointUpdate>(32);
    let mut watched: HashMap<String, WatchedEndpoint> = HashMap::new();
    let mut next_generation = 0_u64;
    let mut last_published = BTreeSet::new();
    let mut has_published = false;
    let initial_discovery_started_at = Instant::now();

    loop {
        if *stop_rx.borrow() {
            break;
        }

        let desired_watch_urls = desired_watch_urls(&seeds, &watched);
        let current_watch_urls: Vec<String> = watched.keys().cloned().collect();
        for watch_url in current_watch_urls {
            if desired_watch_urls.contains(&watch_url) {
                continue;
            }
            if let Some(endpoint) = watched.remove(&watch_url) {
                stop_watched_endpoint(endpoint).await;
            }
        }

        for watch_url in &desired_watch_urls {
            if watched.contains_key(watch_url) {
                continue;
            }
            let generation = next_generation;
            next_generation = next_generation
                .checked_add(1)
                .expect("watch endpoint generation counter overflowed");
            let (endpoint_stop_tx, endpoint_stop_rx) = watch::channel(false);
            let task = tokio::spawn(watch_stargate_endpoint(
                watch_url.clone(),
                generation,
                endpoint_updates_tx.clone(),
                stop_rx.clone(),
                endpoint_stop_rx,
            ));
            watched.insert(
                watch_url.clone(),
                WatchedEndpoint {
                    generation,
                    stop_tx: endpoint_stop_tx,
                    task,
                    state: WatchEndpointState::Connecting,
                },
            );
        }

        let active_routers = active_registration_routers(watched_endpoint_snapshots(&watched));
        let snapshots_complete =
            all_desired_watch_urls_have_snapshots(&desired_watch_urls, |watch_url| {
                watched
                    .get(watch_url)
                    .is_some_and(|endpoint| endpoint.state.has_snapshot())
            });
        if should_publish_watch_routers(
            &active_routers,
            &last_published,
            snapshots_complete,
            initial_discovery_started_at.elapsed() >= INITIAL_WATCH_DISCOVERY_TIMEOUT,
            has_published,
        ) {
            if stargate_updates_tx
                .send(active_routers.clone())
                .await
                .is_err()
            {
                break;
            }
            last_published = active_routers;
            has_published = true;
        }

        tokio::select! {
            maybe_update = endpoint_updates_rx.recv() => {
                match maybe_update {
                    Some(update) => {
                        apply_watch_endpoint_update(&mut watched, update);
                    }
                    None => break,
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    for (_, endpoint) in watched {
        stop_watched_endpoint(endpoint).await;
    }
}

async fn stop_watched_endpoint(endpoint: WatchedEndpoint) {
    let _ = endpoint.stop_tx.send(true);
    await_named_join_handle(
        NamedJoinHandle::new("watch stargate endpoint", endpoint.task),
        REGISTRATION_TASK_SHUTDOWN_TIMEOUT,
    )
    .await;
}

async fn watch_stargate_endpoint(
    watch_url: String,
    generation: u64,
    endpoint_updates_tx: mpsc::Sender<WatchEndpointUpdate>,
    mut stop_rx: watch::Receiver<bool>,
    mut endpoint_stop_rx: watch::Receiver<bool>,
) {
    loop {
        if should_stop(&stop_rx, &endpoint_stop_rx) {
            return;
        }

        let endpoint = normalize_addr(&watch_url);
        let channel = Channel::from_shared(endpoint)
            .context("invalid watch endpoint")
            .map(|endpoint| endpoint.connect_lazy());
        let Ok(channel) = channel else {
            if watch_endpoint_sleep_or_stop(
                &mut stop_rx,
                &mut endpoint_stop_rx,
                Duration::from_secs(1),
            )
            .await
            {
                return;
            }
            continue;
        };
        let mut client = StargateControlPlaneClient::new(channel);
        let response = tokio::select! {
            response = client.watch_stargates(WatchStargatesRequest {}) => response,
            changed = stop_rx.changed() => {
                if stop_channel_changed(changed, &stop_rx) || should_stop(&stop_rx, &endpoint_stop_rx) {
                    return;
                }
                continue;
            }
            changed = endpoint_stop_rx.changed() => {
                if stop_channel_changed(changed, &endpoint_stop_rx) || should_stop(&stop_rx, &endpoint_stop_rx) {
                    return;
                }
                continue;
            }
        };
        let Ok(response) = response else {
            if watch_endpoint_sleep_or_stop(
                &mut stop_rx,
                &mut endpoint_stop_rx,
                Duration::from_secs(1),
            )
            .await
            {
                return;
            }
            continue;
        };
        let mut stream = response.into_inner();

        loop {
            tokio::select! {
                message = stream.message() => {
                    match message {
                        Ok(Some(event)) => {
                            let update = WatchEndpointUpdate {
                                watch_url: watch_url.clone(),
                                generation,
                                event: WatchEndpointEvent::Snapshot(
                                    watch_endpoint_snapshot_from_response(&watch_url, event),
                                ),
                            };
                            if !send_watch_endpoint_update(
                                &endpoint_updates_tx,
                                update,
                                &mut stop_rx,
                                &mut endpoint_stop_rx,
                            )
                            .await
                            {
                                return;
                            }
                        }
                        Ok(None) | Err(_) => {
                            let update = WatchEndpointUpdate {
                                watch_url: watch_url.clone(),
                                generation,
                                event: WatchEndpointEvent::Disconnected,
                            };
                            if !send_watch_endpoint_update(
                                &endpoint_updates_tx,
                                update,
                                &mut stop_rx,
                                &mut endpoint_stop_rx,
                            )
                            .await
                            {
                                return;
                            }
                            break;
                        }
                    }
                }
                changed = stop_rx.changed() => {
                    if stop_channel_changed(changed, &stop_rx)
                        || should_stop(&stop_rx, &endpoint_stop_rx)
                    {
                        return;
                    }
                }
                changed = endpoint_stop_rx.changed() => {
                    if stop_channel_changed(changed, &endpoint_stop_rx)
                        || should_stop(&stop_rx, &endpoint_stop_rx)
                    {
                        return;
                    }
                }
            }
        }

        if watch_endpoint_sleep_or_stop(&mut stop_rx, &mut endpoint_stop_rx, Duration::from_secs(1))
            .await
        {
            return;
        }
    }
}

async fn send_watch_endpoint_update(
    endpoint_updates_tx: &mpsc::Sender<WatchEndpointUpdate>,
    update: WatchEndpointUpdate,
    parent_stop_rx: &mut watch::Receiver<bool>,
    endpoint_stop_rx: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        let permit = tokio::select! {
            permit = endpoint_updates_tx.reserve() => match permit {
                Ok(permit) => permit,
                Err(_) => return false,
            },
            changed = parent_stop_rx.changed() => {
                if stop_channel_changed(changed, parent_stop_rx)
                    || should_stop(parent_stop_rx, endpoint_stop_rx)
                {
                    return false;
                }
                continue;
            }
            changed = endpoint_stop_rx.changed() => {
                if stop_channel_changed(changed, endpoint_stop_rx)
                    || should_stop(parent_stop_rx, endpoint_stop_rx)
                {
                    return false;
                }
                continue;
            }
        };
        permit.send(update);
        return true;
    }
}

fn apply_watch_endpoint_update(
    watched: &mut HashMap<String, WatchedEndpoint>,
    update: WatchEndpointUpdate,
) -> bool {
    let Some(endpoint) = watched.get_mut(&update.watch_url) else {
        return false;
    };
    if endpoint.generation != update.generation {
        return false;
    }
    endpoint.state = match update.event {
        WatchEndpointEvent::Snapshot(snapshot) => WatchEndpointState::Live(snapshot),
        WatchEndpointEvent::Disconnected => WatchEndpointState::Disconnected,
    };
    true
}

fn should_publish_watch_routers(
    active_routers: &BTreeSet<String>,
    last_published: &BTreeSet<String>,
    snapshots_complete: bool,
    initial_discovery_timed_out: bool,
    has_published: bool,
) -> bool {
    // The normal initial publish waits for recursive discovery to complete, but
    // a bad redundant seed must not block registration to already discovered routers.
    let initial_publish_ready =
        snapshots_complete || (initial_discovery_timed_out && !active_routers.is_empty());
    // After the first publish, losing a watch stream is itself a router-removal update.
    (initial_publish_ready || has_published) && active_routers != last_published
}

fn watch_endpoint_snapshot_from_response(
    _watch_url: &str,
    response: WatchStargatesResponse,
) -> WatchEndpointSnapshot {
    WatchEndpointSnapshot {
        registration_routers: response
            .stargates
            .into_iter()
            .filter_map(|info| {
                if !info.advertise_addr.is_empty() {
                    Some(info.advertise_addr)
                } else if !info.stargate_id.is_empty() {
                    Some(info.stargate_id)
                } else {
                    None
                }
            })
            .collect(),
        watch_urls: normalize_string_set(response.watch_stargate_urls),
    }
}

fn desired_watch_urls(
    seeds: &BTreeSet<String>,
    watched: &HashMap<String, WatchedEndpoint>,
) -> BTreeSet<String> {
    desired_watch_urls_from_snapshot_lookup(seeds, |watch_url| {
        watched
            .get(watch_url)
            .and_then(|endpoint| endpoint.state.snapshot())
    })
}

#[cfg(test)]
fn desired_watch_urls_from_snapshots(
    seeds: &BTreeSet<String>,
    snapshots: &HashMap<String, WatchEndpointSnapshot>,
) -> BTreeSet<String> {
    desired_watch_urls_from_snapshot_lookup(seeds, |watch_url| snapshots.get(watch_url))
}

fn desired_watch_urls_from_snapshot_lookup<'a>(
    seeds: &BTreeSet<String>,
    mut snapshot_for_watch_url: impl FnMut(&str) -> Option<&'a WatchEndpointSnapshot>,
) -> BTreeSet<String> {
    let mut desired = seeds.clone();
    let mut pending: Vec<String> = seeds.iter().cloned().collect();
    while let Some(watch_url) = pending.pop() {
        let Some(snapshot) = snapshot_for_watch_url(&watch_url) else {
            continue;
        };
        for next_watch_url in &snapshot.watch_urls {
            if desired.insert(next_watch_url.clone()) {
                pending.push(next_watch_url.clone());
            }
        }
    }
    desired
}

fn active_registration_routers<'a>(
    snapshots: impl IntoIterator<Item = &'a WatchEndpointSnapshot>,
) -> BTreeSet<String> {
    snapshots
        .into_iter()
        .flat_map(|snapshot| snapshot.registration_routers.iter().cloned())
        .collect()
}

fn all_desired_watch_urls_have_snapshots(
    desired_watch_urls: &BTreeSet<String>,
    has_snapshot: impl Fn(&str) -> bool,
) -> bool {
    desired_watch_urls
        .iter()
        .all(|watch_url| has_snapshot(watch_url))
}

fn watched_endpoint_snapshots(
    watched: &HashMap<String, WatchedEndpoint>,
) -> impl Iterator<Item = &WatchEndpointSnapshot> {
    watched
        .values()
        .filter_map(|endpoint| endpoint.state.snapshot())
}

fn normalize_string_set(values: Vec<String>) -> BTreeSet<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

async fn watch_endpoint_sleep_or_stop(
    parent_stop_rx: &mut watch::Receiver<bool>,
    endpoint_stop_rx: &mut watch::Receiver<bool>,
    duration: Duration,
) -> bool {
    tokio::select! {
        changed = parent_stop_rx.changed() => {
            stop_channel_changed(changed, parent_stop_rx)
                || should_stop(parent_stop_rx, endpoint_stop_rx)
        }
        changed = endpoint_stop_rx.changed() => {
            stop_channel_changed(changed, endpoint_stop_rx)
                || should_stop(parent_stop_rx, endpoint_stop_rx)
        }
        _ = tokio::time::sleep(duration) => should_stop(parent_stop_rx, endpoint_stop_rx),
    }
}

fn stop_channel_changed(
    changed: std::result::Result<(), watch::error::RecvError>,
    stop_rx: &watch::Receiver<bool>,
) -> bool {
    changed.is_err() || *stop_rx.borrow()
}

async fn sleep_until_registration_stop(
    parent_stop_rx: &mut watch::Receiver<bool>,
    local_stop_rx: &mut watch::Receiver<bool>,
    cancel_token: &CancellationToken,
    duration: Duration,
) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => true,
        changed = parent_stop_rx.changed() => changed.is_err() || registration_should_stop(parent_stop_rx, local_stop_rx, cancel_token),
        changed = local_stop_rx.changed() => changed.is_err() || registration_should_stop(parent_stop_rx, local_stop_rx, cancel_token),
        _ = tokio::time::sleep(duration) => registration_should_stop(parent_stop_rx, local_stop_rx, cancel_token),
    }
}

#[derive(Clone)]
struct SharedInstStateChannels {
    status_rx: flume::Receiver<InferenceServerStatus>,
    stats_rx: flume::Receiver<(String, CurrentModelStats)>,
    bringup_state_rx: flume::Receiver<BringupModelUpdate>,
    bringup_calibration_rx: flume::Receiver<BringupCalibrationUpdate>,
}

#[derive(Clone)]
struct SharedInstState {
    status_rx: flume::Receiver<InferenceServerStatus>,
    stats_rx: flume::Receiver<(String, CurrentModelStats)>,
    bringup_state_rx: flume::Receiver<BringupModelUpdate>,
    bringup_calibration_rx: flume::Receiver<BringupCalibrationUpdate>,
    current_status: watch::Sender<InferenceServerStatus>,
    current_status_rx: watch::Receiver<InferenceServerStatus>,
    current_stats: watch::Sender<HashMap<String, CurrentModelStats>>,
    current_stats_rx: watch::Receiver<HashMap<String, CurrentModelStats>>,
    current_bringup_state: watch::Sender<HashMap<String, ModelBringupState>>,
    current_bringup_state_rx: watch::Receiver<HashMap<String, ModelBringupState>>,
    current_last_mean_input_tps: watch::Sender<HashMap<String, f64>>,
    current_last_mean_input_tps_rx: watch::Receiver<HashMap<String, f64>>,
    queue_tracker: QueueAdmissionTracker,
    snapshot_update_lock: Arc<Mutex<()>>,
}

impl SharedInstState {
    fn new(
        initial_status: InferenceServerStatus,
        model_ids: &[String],
        channels: SharedInstStateChannels,
        bringup_enabled: bool,
        queue_tracker: QueueAdmissionTracker,
    ) -> Self {
        let mut initial_stats = HashMap::new();
        let mut initial_bringup_state = HashMap::new();
        for model_id in model_ids {
            initial_stats.insert(model_id.clone(), CurrentModelStats::default());
            initial_bringup_state.insert(
                model_id.clone(),
                if bringup_enabled {
                    ModelBringupState::ConnectingUnavailable
                } else {
                    ModelBringupState::AdvertisingActive
                },
            );
        }
        let (current_status, current_status_rx) = watch::channel(initial_status);
        let (current_stats, current_stats_rx) = watch::channel(initial_stats);
        let (current_bringup_state, current_bringup_state_rx) =
            watch::channel(initial_bringup_state);
        let (current_last_mean_input_tps, current_last_mean_input_tps_rx) =
            watch::channel(HashMap::new());
        Self {
            status_rx: channels.status_rx,
            stats_rx: channels.stats_rx,
            bringup_state_rx: channels.bringup_state_rx,
            bringup_calibration_rx: channels.bringup_calibration_rx,
            current_status,
            current_status_rx,
            current_stats,
            current_stats_rx,
            current_bringup_state,
            current_bringup_state_rx,
            current_last_mean_input_tps,
            current_last_mean_input_tps_rx,
            queue_tracker,
            snapshot_update_lock: Arc::new(Mutex::new(())),
        }
    }

    fn drain_updates(&self) -> bool {
        self.drain_updates_with_calibration_barrier(|| {})
    }

    fn drain_updates_with_calibration_barrier(&self, after_calibration: impl FnOnce()) -> bool {
        let _snapshot_update_guard = self.snapshot_update_lock.lock();
        let mut changed = false;

        while let Ok(new_status) = self.status_rx.try_recv() {
            let _ = self.current_status.send(new_status);
            changed = true;
        }

        let mut stats_updated = false;
        while let Ok((model_id, stats)) = self.stats_rx.try_recv() {
            self.current_stats.send_modify(|map| {
                let merged = map
                    .get(&model_id)
                    .map(|existing| merge_current_model_stats(existing, &stats))
                    .unwrap_or(stats);
                map.insert(model_id, merged);
            });
            stats_updated = true;
        }
        if stats_updated {
            changed = true;
        }

        let mut calibration_updated = false;
        while let Ok(update) = self.bringup_calibration_rx.try_recv() {
            match update {
                BringupCalibrationUpdate::Complete {
                    model_id,
                    last_mean_input_tps,
                } => {
                    self.current_last_mean_input_tps.send_modify(|map| {
                        map.insert(model_id.clone(), last_mean_input_tps);
                    });
                    self.queue_tracker
                        .update_calibrated_model_throughput(&model_id, last_mean_input_tps);
                }
                BringupCalibrationUpdate::Clear { model_id } => {
                    self.current_last_mean_input_tps.send_modify(|map| {
                        map.remove(&model_id);
                    });
                    self.queue_tracker
                        .update_calibrated_model_throughput(&model_id, 0.0);
                }
            }
            calibration_updated = true;
        }
        if calibration_updated {
            changed = true;
        }

        after_calibration();

        let mut bringup_updated = false;
        while let Ok(update) = self.bringup_state_rx.try_recv() {
            self.current_bringup_state.send_modify(|map| {
                map.insert(update.model_id, update.state);
            });
            bringup_updated = true;
        }
        if bringup_updated {
            changed = true;
        }

        changed
    }

    fn snapshot(
        &self,
        _coordinated_calibration: bool,
    ) -> HashMap<String, InferenceServerModelRegistration> {
        let _snapshot_update_guard = self.snapshot_update_lock.lock();
        let status = *self.current_status_rx.borrow();
        let current = self.current_stats_rx.borrow().clone();
        let bringup_state = self.current_bringup_state_rx.borrow().clone();
        let last_mean_input_tps = self.current_last_mean_input_tps_rx.borrow().clone();
        current
            .into_iter()
            .map(|(model_id, cur)| {
                let last_mean_input_tps = last_mean_input_tps
                    .get(&model_id)
                    .copied()
                    .unwrap_or_default();
                let has_valid_calibration = valid_last_mean_input_tps(last_mean_input_tps);
                let last_mean_input_tps = if valid_last_mean_input_tps(cur.last_mean_input_tps) {
                    cur.last_mean_input_tps
                } else {
                    last_mean_input_tps
                };
                let calibration_state = if has_valid_calibration {
                    CalibrationState::Complete
                } else {
                    CalibrationState::Unknown
                };
                let model_status = gated_model_status(
                    status,
                    bringup_state
                        .get(&model_id)
                        .copied()
                        .unwrap_or(ModelBringupState::ConnectingUnavailable),
                );
                let proto = InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps,
                        output_tps: cur.output_tps,
                        max_output_tps: cur.max_output_tps,
                        queue_size: cur.queue_size,
                        queued_input_size: cur.queued_input_size,
                        kv_cache_capacity_tokens: cur.kv_cache_capacity_tokens,
                        kv_cache_used_tokens: cur.kv_cache_used_tokens,
                        kv_cache_free_tokens: cur.kv_cache_free_tokens,
                        num_running_queries: cur.num_running_queries,
                        max_engine_concurrency: cur.max_engine_concurrency.unwrap_or_default(),
                        total_query_input_size: cur.total_query_input_size,
                        queue_time_estimate_ms_by_priority: cur
                            .queue_time_estimate_ms_by_priority
                            .clone()
                            .unwrap_or_default(),
                        input_processing_queries: cur.input_processing_queries,
                        output_generation_queries: cur.output_generation_queries,
                        stats_observed_at_unix_ms: cur.stats_observed_at_unix_ms,
                        stats_capabilities: cur.stats_capabilities.clone(),
                        stats_sources: cur.stats_sources.clone(),
                    }),
                    status: model_status.into(),
                    calibration_state: calibration_state.into(),
                };
                (model_id, proto)
            })
            .collect()
    }

    fn all_models_advertising_active(&self) -> bool {
        self.current_bringup_state_rx
            .borrow()
            .values()
            .all(|state| *state == ModelBringupState::AdvertisingActive)
    }

    fn all_models_cluster_calibration_complete(&self) -> bool {
        let current = self.current_stats_rx.borrow();
        let last_mean_input_tps = self.current_last_mean_input_tps_rx.borrow();
        !current.is_empty()
            && current.keys().all(|model_id| {
                last_mean_input_tps
                    .get(model_id)
                    .is_some_and(|value| valid_last_mean_input_tps(*value))
            })
    }
}

fn merge_current_model_stats(
    existing: &CurrentModelStats,
    incoming: &CurrentModelStats,
) -> CurrentModelStats {
    let mut merged = incoming.clone();
    if incoming.kv_cache_capacity_tokens == 0
        && incoming.kv_cache_used_tokens == 0
        && incoming.kv_cache_free_tokens == 0
        && has_any_kv_metrics(existing)
    {
        merged.kv_cache_capacity_tokens = existing.kv_cache_capacity_tokens;
        merged.kv_cache_used_tokens = existing.kv_cache_used_tokens;
        merged.kv_cache_free_tokens = existing.kv_cache_free_tokens;
    }
    if incoming.max_engine_concurrency.is_none() && existing.max_engine_concurrency.is_some() {
        merged.max_engine_concurrency = existing.max_engine_concurrency;
    }
    if incoming.queue_time_estimate_ms_by_priority.is_none()
        && existing.queue_time_estimate_ms_by_priority.is_some()
    {
        merged.queue_time_estimate_ms_by_priority =
            existing.queue_time_estimate_ms_by_priority.clone();
    }
    if incoming.stats_observed_at_unix_ms == 0 && existing.stats_observed_at_unix_ms != 0 {
        merged.stats_observed_at_unix_ms = existing.stats_observed_at_unix_ms;
    }
    if incoming.stats_capabilities.is_empty() && !existing.stats_capabilities.is_empty() {
        merged.stats_capabilities = existing.stats_capabilities.clone();
    }
    if incoming.stats_sources.is_empty() && !existing.stats_sources.is_empty() {
        merged.stats_sources = existing.stats_sources.clone();
    }
    merged
}

fn has_any_kv_metrics(stats: &CurrentModelStats) -> bool {
    stats.kv_cache_capacity_tokens > 0
        || stats.kv_cache_used_tokens > 0
        || stats.kv_cache_free_tokens > 0
}

fn valid_last_mean_input_tps(last_mean_input_tps: f64) -> bool {
    last_mean_input_tps > 0.0 && last_mean_input_tps.is_finite()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationFanoutPhase {
    SingleRouterUntilCalibrated,
    FullFanout,
}

fn initial_registration_fanout_phase(coordinated_calibration: bool) -> RegistrationFanoutPhase {
    if coordinated_calibration {
        RegistrationFanoutPhase::SingleRouterUntilCalibrated
    } else {
        RegistrationFanoutPhase::FullFanout
    }
}

fn advance_registration_fanout_phase(
    current: RegistrationFanoutPhase,
    shared_state: &SharedInstState,
    coordinated_calibration: bool,
) -> RegistrationFanoutPhase {
    match current {
        RegistrationFanoutPhase::FullFanout => RegistrationFanoutPhase::FullFanout,
        RegistrationFanoutPhase::SingleRouterUntilCalibrated
            if shared_state.all_models_advertising_active()
                && (!coordinated_calibration
                    || shared_state.all_models_cluster_calibration_complete()) =>
        {
            RegistrationFanoutPhase::FullFanout
        }
        RegistrationFanoutPhase::SingleRouterUntilCalibrated => {
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        }
    }
}

fn desired_registration_routers(
    active_routers: &BTreeSet<String>,
    phase: RegistrationFanoutPhase,
    calibration_router: &mut Option<String>,
) -> BTreeSet<String> {
    match phase {
        RegistrationFanoutPhase::FullFanout => {
            *calibration_router = None;
            active_routers.clone()
        }
        RegistrationFanoutPhase::SingleRouterUntilCalibrated => {
            if let Some(router) = calibration_router
                .as_ref()
                .filter(|router| active_routers.contains(*router))
            {
                return BTreeSet::from([router.clone()]);
            }
            *calibration_router = active_routers.iter().next().cloned();
            calibration_router.iter().cloned().collect()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistrationStreamExit {
    Stop,
    Retry,
}

async fn stop_reverse_tunnel_task(cancel_token: CancellationToken, task: JoinHandle<()>) {
    cancel_token.cancel();
    await_named_join_handle(
        NamedJoinHandle::new("reverse tunnel registration worker", task),
        REGISTRATION_TASK_SHUTDOWN_TIMEOUT,
    )
    .await;
}

async fn run_router_registration_stream(
    task_config: RouterRegistrationTaskConfig,
    shared_state: SharedInstState,
    parent_stop_rx: watch::Receiver<bool>,
    local_stop_rx: watch::Receiver<bool>,
) {
    let RouterRegistrationTaskConfig {
        router_addr,
        inference_server_id,
        cluster_id,
        inference_server_url,
        min_update_interval,
        reverse_tunnel,
        coordinated_calibration,
        quic_insecure,
        tunnel_protocol,
        upstream_http_base_url,
        cluster_calibration_directive_tx,
        output_token_parser_factory,
        request_observation_tx,
        request_quality_monitor,
        metrics,
        retry,
        queue_mismatch_retry,
        queue_tracker,
        cancel_token,
        auth_token_provider,
    } = task_config;
    let mut parent_stop_rx = parent_stop_rx;
    let mut local_stop_rx = local_stop_rx;

    loop {
        if registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token) {
            return;
        }

        let connection = tokio::select! {
            _ = cancel_token.cancelled() => return,
            changed = parent_stop_rx.changed() => {
                if changed.is_err()
                    || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                {
                    return;
                }
                continue;
            }
            changed = local_stop_rx.changed() => {
                if changed.is_err()
                    || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                {
                    return;
                }
                continue;
            }
            connection = open_registration_stream(
                &router_addr,
                auth_token_provider.as_deref(),
                min_update_interval,
            ) => connection,
        };
        let Ok((mut ack_stream, update_tx)) = connection else {
            if sleep_until_registration_stop(
                &mut parent_stop_rx,
                &mut local_stop_rx,
                &cancel_token,
                Duration::from_secs(1),
            )
            .await
            {
                return;
            }
            continue;
        };

        let (endpoint_tx, endpoint_rx) = watch::channel(None);
        let (connected_tx, mut connected_rx) = watch::channel(false);
        let reverse_cancel_token = cancel_token.child_token();
        let reverse_task = if reverse_tunnel {
            Some(tokio::spawn(run_reverse_tunnel_loop(
                ReverseTunnelLoopConfig {
                    router_addr: router_addr.clone(),
                    inference_server_id: inference_server_id.clone(),
                    upstream_http_base_url: upstream_http_base_url.clone(),
                    quic_insecure,
                    tunnel_protocol,
                    output_token_parser_factory: output_token_parser_factory.clone(),
                    request_observation_tx: request_observation_tx.clone(),
                    request_quality_monitor: request_quality_monitor.clone(),
                    metrics: metrics.clone(),
                    retry: retry.clone(),
                    queue_mismatch_retry: queue_mismatch_retry.clone(),
                    queue_tracker: queue_tracker.clone(),
                    auth_token_provider: auth_token_provider.clone(),
                },
                endpoint_rx,
                connected_tx,
                parent_stop_rx.clone(),
                local_stop_rx.clone(),
                reverse_cancel_token.clone(),
            )))
        } else {
            None
        };

        shared_state.drain_updates();
        let initial = build_update(
            &inference_server_id,
            &cluster_id,
            &inference_server_url,
            &shared_state.snapshot(coordinated_calibration),
            reverse_tunnel,
            coordinated_calibration,
            *connected_rx.borrow(),
        );
        let mut advertised_status =
            RouterAdvertisedStatusTracker::new(metrics.as_deref(), &router_addr);
        advertised_status.record_reverse_tunnel_connected(false);
        let advertised = advertised_model_statuses(&initial);
        if update_tx.send(initial).await.is_err() {
            if let Some(task) = reverse_task {
                stop_reverse_tunnel_task(reverse_cancel_token, task).await;
            }
            if sleep_until_registration_stop(
                &mut parent_stop_rx,
                &mut local_stop_rx,
                &cancel_token,
                Duration::from_millis(200),
            )
            .await
            {
                return;
            }
            continue;
        }
        advertised_status.record_successful_advertisement(advertised);
        let mut last_send = Instant::now();

        let mut tick_interval = tokio::time::interval(min_update_interval);
        tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let stream_exit = loop {
            tokio::select! {
                _ = cancel_token.cancelled() => break RegistrationStreamExit::Stop,
                changed = parent_stop_rx.changed() => {
                    if changed.is_err()
                        || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                    {
                        break RegistrationStreamExit::Stop;
                    }
                }
                changed = local_stop_rx.changed() => {
                    if changed.is_err()
                        || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                    {
                        break RegistrationStreamExit::Stop;
                    }
                }
                _ = tick_interval.tick() => {
                    let changed = shared_state.drain_updates();
                    let heartbeat_due = last_send.elapsed() >= min_update_interval;

                    if changed || heartbeat_due {
                        // Heartbeats resend the full current snapshot; identical model
                        // stats across sends are normal liveness traffic.
                        let update = build_update(
                            &inference_server_id,
                            &cluster_id,
                            &inference_server_url,
                            &shared_state.snapshot(coordinated_calibration),
                            reverse_tunnel,
                            coordinated_calibration,
                            *connected_rx.borrow(),
                        );
                        let advertised = advertised_model_statuses(&update);
                        if update_tx.send(update).await.is_err() {
                            break RegistrationStreamExit::Retry;
                        }
                        advertised_status.record_successful_advertisement(advertised);
                        last_send = Instant::now();
                    }
                }
                connected_changed = connected_rx.changed(), if reverse_tunnel => {
                    if connected_changed.is_ok() {
                        let reverse_connected = *connected_rx.borrow();
                        advertised_status.record_reverse_tunnel_connected(reverse_connected);
                        let update = build_update(
                            &inference_server_id,
                            &cluster_id,
                            &inference_server_url,
                            &shared_state.snapshot(coordinated_calibration),
                            reverse_tunnel,
                            coordinated_calibration,
                            reverse_connected,
                        );
                        let advertised = advertised_model_statuses(&update);
                        if update_tx.send(update).await.is_err() {
                            break RegistrationStreamExit::Retry;
                        }
                        advertised_status.record_successful_advertisement(advertised);
                        last_send = Instant::now();
                    } else {
                        break RegistrationStreamExit::Retry;
                    }
                }
                maybe_ack = ack_stream.message() => {
                    match maybe_ack {
                        Ok(Some(ack)) => {
                            publish_cluster_calibration_directives(
                                &cluster_calibration_directive_tx,
                                ack.model_calibration_directives.clone(),
                            )
                            .await;
                            if reverse_tunnel {
                                let endpoint = reverse_tunnel_endpoint_from_ack(&ack);
                                endpoint_tx.send_if_modified(move |current| {
                                    if *current == endpoint {
                                        return false;
                                    }
                                    *current = endpoint;
                                    true
                                });
                            }
                        }
                        Ok(None) => break RegistrationStreamExit::Retry,
                        Err(_) => break RegistrationStreamExit::Retry,
                    }
                }
            }
        };

        if let Some(task) = reverse_task {
            stop_reverse_tunnel_task(reverse_cancel_token, task).await;
        }
        if stream_exit == RegistrationStreamExit::Stop {
            return;
        }
    }
}

#[derive(Debug, Clone)]
struct AdvertisedModelStatus {
    model_id: String,
    status: InferenceServerStatus,
}

#[derive(Debug)]
struct RouterAdvertisedStatusTracker<'a> {
    metrics: Option<&'a PylonMetrics>,
    router_addr: &'a str,
    last_advertised: Vec<AdvertisedModelStatus>,
}

impl<'a> RouterAdvertisedStatusTracker<'a> {
    fn new(metrics: Option<&'a PylonMetrics>, router_addr: &'a str) -> Self {
        Self {
            metrics,
            router_addr,
            last_advertised: Vec::new(),
        }
    }

    fn record_successful_advertisement(&mut self, advertised: Vec<AdvertisedModelStatus>) {
        if let Some(metrics) = self.metrics {
            metrics.observe_registration_stream_connected(self.router_addr, true);
        }
        observe_advertised_statuses(self.metrics, self.router_addr, &advertised);
        self.last_advertised = advertised;
    }

    fn record_reverse_tunnel_connected(&self, connected: bool) {
        if let Some(metrics) = self.metrics {
            metrics.observe_reverse_tunnel_connected(self.router_addr, connected);
        }
    }

    fn clear(&mut self) {
        let Some(metrics) = self.metrics else {
            return;
        };

        metrics.observe_registration_stream_connected(self.router_addr, false);
        metrics.observe_reverse_tunnel_connected(self.router_addr, false);
        for advertised in self.last_advertised.drain(..) {
            metrics.observe_model_advertised_status(
                self.router_addr,
                &advertised.model_id,
                InferenceServerStatus::Inactive,
            );
        }
    }
}

impl Drop for RouterAdvertisedStatusTracker<'_> {
    fn drop(&mut self) {
        self.clear();
    }
}

fn router_advertised_status(
    model_status: InferenceServerStatus,
    reverse_tunnel: bool,
    reverse_connected: bool,
) -> InferenceServerStatus {
    if reverse_tunnel && model_status == InferenceServerStatus::Active && !reverse_connected {
        InferenceServerStatus::Inactive
    } else {
        model_status
    }
}

fn observe_advertised_statuses(
    metrics: Option<&PylonMetrics>,
    router_addr: &str,
    advertised: &[AdvertisedModelStatus],
) {
    let Some(metrics) = metrics else {
        return;
    };

    for advertised in advertised {
        metrics.observe_model_advertised_status(
            router_addr,
            &advertised.model_id,
            advertised.status,
        );
    }
}

fn advertised_model_statuses(update: &InferenceServerRegistration) -> Vec<AdvertisedModelStatus> {
    update
        .models
        .iter()
        .map(|(model_id, registration)| AdvertisedModelStatus {
            model_id: model_id.clone(),
            status: InferenceServerStatus::try_from(registration.status)
                .unwrap_or(InferenceServerStatus::Unknown),
        })
        .collect()
}

async fn open_registration_stream(
    router_addr: &str,
    auth_token_provider: Option<&AuthTokenProvider>,
    min_update_interval: Duration,
) -> anyhow::Result<(
    tonic::Streaming<InferenceServerAck>,
    mpsc::Sender<InferenceServerRegistration>,
)> {
    let endpoint = normalize_addr(router_addr);
    let channel = Channel::from_shared(endpoint)?.connect().await?;
    let mut client = StargateControlPlaneClient::new(channel);

    let (update_tx, update_rx) = mpsc::channel(32);
    let stream = ReceiverStream::new(update_rx);

    let mut request = tonic::Request::new(stream);
    let heartbeat_ms: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        min_update_interval.as_millis().to_string().parse()?;
    request
        .metadata_mut()
        .insert(REGISTRATION_HEARTBEAT_MS_METADATA, heartbeat_ms);
    if let Some(provider) = auth_token_provider {
        let token = provider.get_token().await?;
        let header_value: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            format!("Bearer {token}")
                .parse()
                .context("invalid auth token")?;
        request.metadata_mut().insert("authorization", header_value);
    }

    let ack_stream = client
        .register_inference_server(request)
        .await?
        .into_inner();
    Ok((ack_stream, update_tx))
}

fn reverse_tunnel_endpoint_from_ack(ack: &InferenceServerAck) -> Option<ReverseTunnelEndpoint> {
    let routing_target_addr = ack.reverse_tunnel_target.trim();
    if routing_target_addr.is_empty() {
        return None;
    }
    let pylon_dial_addr = ack.reverse_tunnel_pylon_dial_addr.trim();
    if pylon_dial_addr.is_empty() || pylon_dial_addr == routing_target_addr {
        return Some(ReverseTunnelEndpoint {
            routing_target_addr: routing_target_addr.to_string(),
            pylon_dial_addr: routing_target_addr.to_string(),
            sni_override: None,
        });
    }

    Some(ReverseTunnelEndpoint {
        routing_target_addr: routing_target_addr.to_string(),
        pylon_dial_addr: pylon_dial_addr.to_string(),
        sni_override: Some(reverse_tunnel_sni_from_routing_target(routing_target_addr)),
    })
}

fn reverse_tunnel_sni_from_routing_target(routing_target_addr: &str) -> String {
    let host = routing_target_addr
        .strip_prefix('[')
        .and_then(|rest| rest.split_once(']').map(|(host, _)| host))
        .or_else(|| routing_target_addr.rsplit_once(':').map(|(host, _)| host))
        .unwrap_or(routing_target_addr);
    if host == "localhost" || host.parse::<IpAddr>().is_ok() {
        "stargate".to_string()
    } else {
        host.to_string()
    }
}

async fn publish_cluster_calibration_directives(
    tx: &flume::Sender<ClusterCalibrationDirective>,
    directives: Vec<ModelCalibrationDirective>,
) {
    for directive in directives {
        let Some(state) = cluster_calibration_directive_state(directive.state) else {
            continue;
        };
        let _ = tx
            .send_async(ClusterCalibrationDirective {
                model_id: directive.model_id,
                state,
                last_mean_input_tps: directive.last_mean_input_tps,
            })
            .await;
    }
}

fn cluster_calibration_directive_state(state: i32) -> Option<ClusterCalibrationDirectiveState> {
    match CalibrationState::try_from(state).unwrap_or(CalibrationState::Unknown) {
        CalibrationState::Waiting => Some(ClusterCalibrationDirectiveState::Waiting),
        CalibrationState::Run => Some(ClusterCalibrationDirectiveState::Run),
        CalibrationState::Complete => Some(ClusterCalibrationDirectiveState::Complete),
        CalibrationState::Unknown => None,
    }
}

/// Maintains a single reverse QUIC tunnel connection to a stargate router.
///
/// Lifecycle:
///
///   1. **Wait for endpoint** -- `endpoint_rx` starts as `None`. The
///      registration loop populates it from the first ACK that includes a
///      reverse tunnel routing target. Until an endpoint arrives we park,
///      signalling `connected_tx = false`.
///
///   2. **Connect** -- open a reverse QUIC tunnel to the pylon dial address,
///      using an SNI override when the dial address differs from the routing
///      target address.
///      On success, signal `connected_tx = true` so the registration loop
///      can advertise the model as active.
///
///   3. **Hold** -- select waiting for the connection to break.
///      Three things can end the hold:
///        - *Stop signal*: clean shutdown, return immediately.
///        - *Endpoint changed*: the router changed the dial address or routing
///          target;
///          tear down the old tunnel and reconnect (outer loop).
///        - *Connection lost*: the QUIC session closed; back off and retry.
///
///      The sender deduplicates via `send_if_modified`, so heartbeat ACKs
///      with the same endpoint do not wake the receiver.
///
///   4. **Backoff** -- exponential 1s..30s on failures. Reset to 1s after
///      a connection that survived > 60 seconds, or on an endpoint change.
async fn reverse_tunnel_connect_with_timeout<F>(
    connect_timeout: Duration,
    connect_attempt: F,
) -> Result<ReverseQuicTunnelHandle, TunnelError>
where
    F: Future<Output = Result<ReverseQuicTunnelHandle, TunnelError>>,
{
    tokio::time::timeout(connect_timeout, connect_attempt)
        .await
        .map_err(|_| {
            TunnelError::Connect(format!(
                "connect attempt timed out after {}ms",
                connect_timeout.as_millis()
            ))
        })?
}

async fn run_reverse_tunnel_loop(
    config: ReverseTunnelLoopConfig,
    endpoint_rx: watch::Receiver<Option<ReverseTunnelEndpoint>>,
    connected_tx: watch::Sender<bool>,
    parent_stop_rx: watch::Receiver<bool>,
    local_stop_rx: watch::Receiver<bool>,
    cancel_token: CancellationToken,
) {
    let ReverseTunnelLoopConfig {
        router_addr,
        inference_server_id,
        upstream_http_base_url,
        quic_insecure,
        tunnel_protocol,
        output_token_parser_factory,
        request_observation_tx,
        request_quality_monitor,
        metrics,
        retry,
        queue_mismatch_retry,
        queue_tracker,
        auth_token_provider,
    } = config;
    let reverse_tunnel_connect_timeout = REVERSE_TUNNEL_CONNECT_TIMEOUT;
    let mut endpoint_rx = endpoint_rx;
    let mut parent_stop_rx = parent_stop_rx;
    let mut local_stop_rx = local_stop_rx;
    let mut backoff = Duration::from_secs(1);
    const BACKOFF_MAX: Duration = Duration::from_secs(30);

    // -- outer loop: each iteration is one connect attempt ----------------
    loop {
        if registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token) {
            let _ = connected_tx.send(false);
            return;
        }

        // Phase 1: wait for a reverse tunnel endpoint from the registration ACK stream.
        let endpoint = endpoint_rx.borrow().clone();
        let Some(endpoint) = endpoint else {
            let _ = connected_tx.send(false);
            tokio::select! {
                _ = cancel_token.cancelled() => return,
                changed = parent_stop_rx.changed() => {
                    if changed.is_err()
                        || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                    {
                        return;
                    }
                }
                changed = local_stop_rx.changed() => {
                    if changed.is_err()
                        || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                    {
                        return;
                    }
                }
                changed = endpoint_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            continue;
        };

        // Phase 2: open a reverse QUIC tunnel to the pylon dial address.
        let tunnel_config = build_reverse_quic_tunnel_config(ReverseQuicTunnelConfigParams {
            dial_addr: endpoint.pylon_dial_addr.clone(),
            sni_override: endpoint.sni_override.clone(),
            inference_server_id: inference_server_id.clone(),
            upstream_http_base_url: upstream_http_base_url.clone(),
            quic_insecure,
            tunnel_protocol,
            output_token_parser_factory: output_token_parser_factory.clone(),
            request_observation_tx: request_observation_tx.clone(),
            request_quality_monitor: request_quality_monitor.clone(),
            auth_token_provider: auth_token_provider.clone(),
            retry: retry.clone(),
            queue_mismatch_retry: queue_mismatch_retry.clone(),
            queue_tracker: queue_tracker.clone(),
            metrics: metrics.clone(),
        });
        let connect_result = tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = connected_tx.send(false);
                return;
            }
            changed = parent_stop_rx.changed() => {
                if changed.is_err()
                    || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                {
                    let _ = connected_tx.send(false);
                    return;
                }
                continue;
            }
            changed = local_stop_rx.changed() => {
                if changed.is_err()
                    || registration_should_stop(&parent_stop_rx, &local_stop_rx, &cancel_token)
                {
                    let _ = connected_tx.send(false);
                    return;
                }
                continue;
            }
            result = reverse_tunnel_connect_with_timeout(
                reverse_tunnel_connect_timeout,
                start_reverse_quic_tunnel(tunnel_config),
            ) => result,
        };
        match connect_result {
            Ok(handle) => {
                tracing::info!(
                    router_addr = %router_addr,
                    dial_addr = %endpoint.pylon_dial_addr,
                    routing_target_addr = %endpoint.routing_target_addr,
                    inference_server_id = %inference_server_id,
                    "reverse tunnel connected"
                );
                let _ = connected_tx.send(true);
                let connected_at = tokio::time::Instant::now();

                // Phase 3: hold the connection until stop / target change / drop.
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        handle.shutdown().await;
                        let _ = connected_tx.send(false);
                        return;
                    }
                    _ = parent_stop_rx.changed() => {
                        handle.shutdown().await;
                        let _ = connected_tx.send(false);
                        return;
                    }
                    _ = local_stop_rx.changed() => {
                        handle.shutdown().await;
                        let _ = connected_tx.send(false);
                        return;
                    }
                    _ = endpoint_rx.changed() => {
                        // endpoint_rx carries the reverse tunnel endpoint from
                        // InferenceServerAck. The sender deduplicates via
                        // send_if_modified, so changed() only fires on a
                        // genuine routing-target or pylon-dial-address change.
                        handle.shutdown().await;
                        let _ = connected_tx.send(false);
                        backoff = Duration::from_secs(1);
                    }
                    _ = handle.closed() => {
                        tracing::warn!(
                            router_addr = %router_addr,
                            dial_addr = %endpoint.pylon_dial_addr,
                            routing_target_addr = %endpoint.routing_target_addr,
                            inference_server_id = %inference_server_id,
                            backoff_ms = backoff.as_millis(),
                            "reverse tunnel connection dropped, reconnecting"
                        );
                        let _ = connected_tx.send(false);
                        if connected_at.elapsed() > Duration::from_secs(60) {
                            backoff = Duration::from_secs(1);
                        }
                        if sleep_until_registration_stop(
                            &mut parent_stop_rx,
                            &mut local_stop_rx,
                            &cancel_token,
                            backoff,
                        )
                        .await
                        {
                            return;
                        }
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                    }
                }
            }
            Err(error) => {
                tracing::warn!(
                    router_addr = %router_addr,
                    dial_addr = %endpoint.pylon_dial_addr,
                    routing_target_addr = %endpoint.routing_target_addr,
                    inference_server_id = %inference_server_id,
                    error = %error,
                    backoff_ms = backoff.as_millis(),
                    "reverse tunnel connect failed, retrying"
                );
                let _ = connected_tx.send(false);
                if sleep_until_registration_stop(
                    &mut parent_stop_rx,
                    &mut local_stop_rx,
                    &cancel_token,
                    backoff,
                )
                .await
                {
                    return;
                }
                backoff = (backoff * 2).min(BACKOFF_MAX);
            }
        }
    }
}

fn build_update(
    inference_server_id: &str,
    cluster_id: &str,
    inference_server_url: &str,
    models: &HashMap<String, InferenceServerModelRegistration>,
    reverse_tunnel: bool,
    coordinated_calibration: bool,
    reverse_connected: bool,
) -> InferenceServerRegistration {
    let models = models
        .iter()
        .map(|(model_id, model)| {
            let mut model = model.clone();
            let model_status = InferenceServerStatus::try_from(model.status)
                .unwrap_or(InferenceServerStatus::Unknown);
            model.status =
                router_advertised_status(model_status, reverse_tunnel, reverse_connected).into();
            (model_id.clone(), model)
        })
        .collect();
    InferenceServerRegistration {
        inference_server_id: inference_server_id.to_string(),
        cluster_id: cluster_id.to_string(),
        inference_server_url: inference_server_url.to_string(),
        models,
        reverse_tunnel,
        coordinated_calibration,
    }
}

fn effective_cluster_id(cluster_id: &str, inference_server_id: &str) -> String {
    if cluster_id.is_empty() {
        inference_server_id.to_string()
    } else {
        cluster_id.to_string()
    }
}

fn should_stop(
    parent_stop_rx: &watch::Receiver<bool>,
    local_stop_rx: &watch::Receiver<bool>,
) -> bool {
    *parent_stop_rx.borrow() || *local_stop_rx.borrow()
}

fn registration_should_stop(
    parent_stop_rx: &watch::Receiver<bool>,
    local_stop_rx: &watch::Receiver<bool>,
    cancel_token: &CancellationToken,
) -> bool {
    should_stop(parent_stop_rx, local_stop_rx) || cancel_token.is_cancelled()
}

fn normalize_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

fn infer_upstream_http_base_url(inference_server_url: &str) -> Option<String> {
    if inference_server_url.starts_with("http://") || inference_server_url.starts_with("https://") {
        Some(inference_server_url.to_string())
    } else {
        None
    }
}

fn is_direct_inference_server_url(inference_server_url: &str) -> bool {
    url::Url::parse(inference_server_url).is_ok_and(|url| url.scheme() == "quic")
}

fn gated_model_status(
    base_status: InferenceServerStatus,
    bringup_state: ModelBringupState,
) -> InferenceServerStatus {
    if base_status != InferenceServerStatus::Active {
        return base_status;
    }
    match bringup_state {
        ModelBringupState::AdvertisingActive => InferenceServerStatus::Active,
        ModelBringupState::ConnectingUnavailable
        | ModelBringupState::AwaitingClusterCalibration
        | ModelBringupState::Calibrating
        | ModelBringupState::Recovering => InferenceServerStatus::Inactive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DropNotifier(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotifier {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    #[test]
    fn reverse_tunnel_connectivity_only_overrides_router_local_advertisement() {
        assert_eq!(
            router_advertised_status(InferenceServerStatus::Active, true, false),
            InferenceServerStatus::Inactive
        );
        assert_eq!(
            router_advertised_status(InferenceServerStatus::Active, true, true),
            InferenceServerStatus::Active
        );
        assert_eq!(
            router_advertised_status(InferenceServerStatus::Inactive, true, false),
            InferenceServerStatus::Inactive
        );
    }

    #[test]
    fn bringup_gates_active_status_until_model_is_advertising() {
        assert_eq!(
            gated_model_status(
                InferenceServerStatus::Active,
                ModelBringupState::ConnectingUnavailable
            ),
            InferenceServerStatus::Inactive
        );
        assert_eq!(
            gated_model_status(
                InferenceServerStatus::Active,
                ModelBringupState::Calibrating
            ),
            InferenceServerStatus::Inactive
        );
        assert_eq!(
            gated_model_status(
                InferenceServerStatus::Active,
                ModelBringupState::AwaitingClusterCalibration
            ),
            InferenceServerStatus::Inactive
        );
        assert_eq!(
            gated_model_status(InferenceServerStatus::Active, ModelBringupState::Recovering),
            InferenceServerStatus::Inactive
        );
        assert_eq!(
            gated_model_status(
                InferenceServerStatus::Active,
                ModelBringupState::AdvertisingActive
            ),
            InferenceServerStatus::Active
        );
    }

    #[test]
    fn observes_router_advertised_status_from_registration_update() {
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let mut models = HashMap::new();
        models.insert(
            "model-a".to_string(),
            InferenceServerModelRegistration {
                stats: None,
                status: InferenceServerStatus::Active.into(),
                calibration_state: CalibrationState::Unknown as i32,
            },
        );

        let update = build_update(
            "client-a",
            "cluster-a",
            "quic://127.0.0.1:9000",
            &models,
            true,
            false,
            false,
        );
        let advertised = advertised_model_statuses(&update);
        observe_advertised_statuses(Some(metrics.as_ref()), "router-a", &advertised);

        let body = metrics.gather_text().expect("metrics should encode");
        assert!(body.contains(
            r#"pylon_model_advertised_status{model="model-a",router="router-a",status="inactive"} 1"#
        ));
        assert!(body.contains(
            r#"pylon_model_advertised_status{model="model-a",router="router-a",status="active"} 0"#
        ));
    }

    #[test]
    fn clears_router_advertised_status_when_tracker_drops() {
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let mut models = HashMap::new();
        models.insert(
            "model-a".to_string(),
            InferenceServerModelRegistration {
                stats: None,
                status: InferenceServerStatus::Active.into(),
                calibration_state: CalibrationState::Unknown as i32,
            },
        );
        let update = build_update(
            "client-a",
            "cluster-a",
            "quic://127.0.0.1:9000",
            &models,
            false,
            false,
            false,
        );

        {
            let mut tracker =
                RouterAdvertisedStatusTracker::new(Some(metrics.as_ref()), "router-a");
            tracker.record_successful_advertisement(advertised_model_statuses(&update));
            let body = metrics.gather_text().expect("metrics should encode");
            assert!(body.contains(
                r#"pylon_model_advertised_status{model="model-a",router="router-a",status="active"} 1"#
            ));
            assert!(body.contains(r#"pylon_registration_stream_connected{router="router-a"} 1"#));
            tracker.record_reverse_tunnel_connected(true);
            let body = metrics.gather_text().expect("metrics should encode");
            assert!(body.contains(r#"pylon_reverse_tunnel_connected{router="router-a"} 1"#));
        }

        let body = metrics.gather_text().expect("metrics should encode");
        assert!(body.contains(
            r#"pylon_model_advertised_status{model="model-a",router="router-a",status="active"} 0"#
        ));
        assert!(body.contains(
            r#"pylon_model_advertised_status{model="model-a",router="router-a",status="inactive"} 1"#
        ));
        assert!(body.contains(r#"pylon_registration_stream_connected{router="router-a"} 0"#));
        assert!(body.contains(r#"pylon_reverse_tunnel_connected{router="router-a"} 0"#));
    }

    #[test]
    fn infers_http_upstream_base_url_from_http_registration_url() {
        assert_eq!(
            infer_upstream_http_base_url("http://127.0.0.1:8000"),
            Some("http://127.0.0.1:8000".to_string())
        );
        assert_eq!(infer_upstream_http_base_url("quic://127.0.0.1:8000"), None);
    }

    fn test_registration_config() -> InferenceServerRegistrationConfig {
        InferenceServerRegistrationConfig {
            seeds: vec!["router-a".to_string()],
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            inference_server_url: "quic://127.0.0.1:8443".to_string(),
            upstream_http_base_url: Some("http://127.0.0.1:8090".to_string()),
            min_update_interval: Duration::from_secs(2),
            status: InferenceServerStatus::Active,
            reverse_tunnel: false,
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::Custom,
            bringup: BringupConfig::default(),
            output_token_parser_factory: OutputTokenParserFactory,
            request_observation_tx: None,
            request_quality_monitor: RequestQualityMonitorConfig::default(),
            metrics: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            auth_token_provider: None,
        }
    }

    #[test]
    fn registration_start_plan_normalizes_config_before_orchestration() {
        let mut config = test_registration_config();
        config.cluster_id = String::new();
        config.inference_server_url = "http://127.0.0.1:8090".to_string();
        config.upstream_http_base_url = None;
        config.reverse_tunnel = true;

        let plan = RegistrationStartPlan::from_config(&config).expect("plan should build");

        assert_eq!(plan.watch_seeds, vec!["router-a".to_string()]);
        assert_eq!(plan.cluster_id, "inst-a");
        assert_eq!(plan.upstream_http_base_url, "http://127.0.0.1:8090");
    }

    #[test]
    fn registration_start_plan_rejects_invalid_startup_config() {
        let mut empty_seeds = test_registration_config();
        empty_seeds.seeds.clear();
        assert!(matches!(
            RegistrationStartPlan::from_config(&empty_seeds),
            Err(ClientError::Config(message)) if message == "stargate seeds are empty"
        ));

        let mut missing_upstream = test_registration_config();
        missing_upstream.inference_server_url = "quic://127.0.0.1:8443".to_string();
        missing_upstream.upstream_http_base_url = None;
        assert!(matches!(
            RegistrationStartPlan::from_config(&missing_upstream),
            Err(ClientError::Config(message))
                if message == "upstream_http_base_url is required when inference_server_url is not http(s)"
        ));

        let mut direct_http_url = test_registration_config();
        direct_http_url.inference_server_url = "http://127.0.0.1:8090".to_string();
        direct_http_url.upstream_http_base_url = None;
        direct_http_url.reverse_tunnel = false;
        assert!(matches!(
            RegistrationStartPlan::from_config(&direct_http_url),
            Err(ClientError::Config(message))
                if message == "direct registration inference_server_url must be quic://"
        ));
    }

    #[tokio::test]
    async fn cancelled_registration_join_wait_aborts_child_task() {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let child = tokio::spawn(async move {
            let _drop_notifier = DropNotifier(Some(dropped_tx));
            let _ = entered_tx.send(());
            std::future::pending::<()>().await;
        });

        entered_rx.await.expect("child should start");

        {
            let wait = await_named_join_handle(
                NamedJoinHandle::new("pending registration child", child),
                Duration::from_secs(30),
            );
            tokio::pin!(wait);
            tokio::select! {
                biased;
                _ = &mut wait => panic!("pending child should not finish before cancellation"),
                _ = tokio::task::yield_now() => {}
            }
        }

        tokio::time::timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("cancelled join wait should abort the child")
            .expect("child drop notifier should send");
    }

    #[tokio::test]
    async fn registration_retry_sleep_wakes_on_parent_stop() {
        let (parent_tx, parent_rx) = watch::channel(false);
        let (_local_tx, local_rx) = watch::channel(false);
        let cancel_token = CancellationToken::new();
        let task_cancel_token = cancel_token.clone();

        let task = tokio::spawn(async move {
            let mut parent_rx = parent_rx;
            let mut local_rx = local_rx;
            sleep_until_registration_stop(
                &mut parent_rx,
                &mut local_rx,
                &task_cancel_token,
                Duration::from_secs(30),
            )
            .await
        });

        tokio::task::yield_now().await;
        parent_tx
            .send(true)
            .expect("parent stop signal should send");

        let stopped = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("retry sleep should wake on parent stop")
            .expect("retry task should not panic");
        assert!(stopped);
    }

    #[tokio::test]
    async fn registration_retry_sleep_wakes_when_parent_stop_channel_closes() {
        let (parent_tx, parent_rx) = watch::channel(false);
        let (_local_tx, local_rx) = watch::channel(false);
        let cancel_token = CancellationToken::new();
        let task_cancel_token = cancel_token.clone();

        let task = tokio::spawn(async move {
            let mut parent_rx = parent_rx;
            let mut local_rx = local_rx;
            sleep_until_registration_stop(
                &mut parent_rx,
                &mut local_rx,
                &task_cancel_token,
                Duration::from_secs(30),
            )
            .await
        });

        tokio::task::yield_now().await;
        // Close the parent stop channel to verify closed watch senders stop retry sleeps.
        drop(parent_tx);

        let stopped = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("retry sleep should wake when parent stop channel closes")
            .expect("retry task should not panic");
        assert!(stopped);
    }

    #[tokio::test]
    async fn registration_retry_sleep_wakes_on_cancel_token() {
        let (_parent_tx, parent_rx) = watch::channel(false);
        let (_local_tx, local_rx) = watch::channel(false);
        let cancel_token = CancellationToken::new();
        let task_cancel_token = cancel_token.clone();

        let task = tokio::spawn(async move {
            let mut parent_rx = parent_rx;
            let mut local_rx = local_rx;
            sleep_until_registration_stop(
                &mut parent_rx,
                &mut local_rx,
                &task_cancel_token,
                Duration::from_secs(30),
            )
            .await
        });

        tokio::task::yield_now().await;
        cancel_token.cancel();

        let stopped = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("retry sleep should wake on cancel token")
            .expect("retry task should not panic");
        assert!(stopped);
    }

    #[test]
    fn reverse_tunnel_registration_advertises_upstream_http_url() {
        let register_config = InferenceServerRegistrationConfig {
            seeds: vec!["router-a".to_string()],
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            inference_server_url: "quic://127.0.0.1:8443".to_string(),
            upstream_http_base_url: Some("http://127.0.0.1:8090".to_string()),
            min_update_interval: Duration::from_secs(2),
            status: InferenceServerStatus::Active,
            reverse_tunnel: true,
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::Custom,
            bringup: BringupConfig::default(),
            output_token_parser_factory: OutputTokenParserFactory,
            request_observation_tx: None,
            request_quality_monitor: RequestQualityMonitorConfig::default(),
            metrics: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            auth_token_provider: None,
        };
        let cancel_token = CancellationToken::new();
        let (cluster_calibration_directive_tx, _cluster_calibration_directive_rx) =
            flume::bounded(1);

        let task_template = RouterRegistrationTaskTemplate::from_registration_config(
            &register_config,
            &register_config.cluster_id,
            register_config
                .upstream_http_base_url
                .as_deref()
                .expect("test config includes upstream HTTP base URL"),
            cluster_calibration_directive_tx,
            &cancel_token,
        );

        let task_config = task_template.build_for_router("router-a".to_string());
        assert_eq!(task_config.inference_server_url, "http://127.0.0.1:8090");
    }

    #[test]
    fn build_update_includes_cluster_id() {
        let models = HashMap::new();

        let update = build_update(
            "client-a",
            "cluster-shared",
            "quic://127.0.0.1:9000",
            &models,
            false,
            true,
            false,
        );

        assert_eq!(update.inference_server_id, "client-a");
        assert_eq!(update.cluster_id, "cluster-shared");
        assert!(update.coordinated_calibration);
    }

    #[test]
    fn coordinated_calibration_uses_one_stable_router_until_active() {
        let mut calibration_router = None;
        let active_routers = BTreeSet::from([
            "http://router-b".to_string(),
            "http://router-a".to_string(),
            "http://router-c".to_string(),
        ]);

        let desired = desired_registration_routers(
            &active_routers,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated,
            &mut calibration_router,
        );

        assert_eq!(desired, BTreeSet::from(["http://router-a".to_string()]));
        assert_eq!(calibration_router, Some("http://router-a".to_string()));

        let active_routers = BTreeSet::from([
            "http://router-0".to_string(),
            "http://router-a".to_string(),
            "http://router-b".to_string(),
        ]);
        let desired = desired_registration_routers(
            &active_routers,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated,
            &mut calibration_router,
        );
        assert_eq!(desired, BTreeSet::from(["http://router-a".to_string()]));

        let active_routers =
            BTreeSet::from(["http://router-0".to_string(), "http://router-b".to_string()]);
        let desired = desired_registration_routers(
            &active_routers,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated,
            &mut calibration_router,
        );
        assert_eq!(desired, BTreeSet::from(["http://router-0".to_string()]));
        assert_eq!(calibration_router, Some("http://router-0".to_string()));

        let desired = desired_registration_routers(
            &active_routers,
            RegistrationFanoutPhase::FullFanout,
            &mut calibration_router,
        );
        assert_eq!(desired, active_routers);
        assert_eq!(calibration_router, None);
    }

    #[test]
    fn watch_stargate_urls_are_discovery_seeds_not_registration_routers() {
        let snapshot = watch_endpoint_snapshot_from_response(
            "seed-a",
            stargate_proto::pb::WatchStargatesResponse {
                stargates: vec![stargate_proto::pb::StargateInfo {
                    stargate_id: "stargate-0".to_string(),
                    advertise_addr: "stargate-0.region-a:50071".to_string(),
                    http_advertise_addr: "stargate-0.region-a:8000".to_string(),
                }],
                watch_stargate_urls: vec!["stargate.region-b:50071".to_string()],
            },
        );

        assert_eq!(
            snapshot.registration_routers,
            BTreeSet::from(["stargate-0.region-a:50071".to_string()])
        );
        assert_eq!(
            snapshot.watch_urls,
            BTreeSet::from(["stargate.region-b:50071".to_string()])
        );
    }

    #[test]
    fn recursive_watch_discovery_waits_for_remote_snapshots_before_registration_publish() {
        let seeds = BTreeSet::from(["stargate.region-a:50071".to_string()]);
        let mut snapshots = HashMap::from([(
            "stargate.region-a:50071".to_string(),
            WatchEndpointSnapshot {
                registration_routers: BTreeSet::from([
                    "stargate-0.region-a:50071".to_string(),
                    "stargate-1.region-a:50071".to_string(),
                ]),
                watch_urls: BTreeSet::from(["stargate.region-b:50071".to_string()]),
            },
        )]);

        let desired_urls = desired_watch_urls_from_snapshots(&seeds, &snapshots);
        assert_eq!(
            desired_urls,
            BTreeSet::from([
                "stargate.region-a:50071".to_string(),
                "stargate.region-b:50071".to_string(),
            ])
        );
        assert!(!all_desired_watch_urls_have_snapshots(
            &desired_urls,
            |watch_url| snapshots.contains_key(watch_url)
        ));

        snapshots.insert(
            "stargate.region-b:50071".to_string(),
            WatchEndpointSnapshot {
                registration_routers: BTreeSet::from([
                    "stargate-0.region-b:50071".to_string(),
                    "stargate-1.region-b:50071".to_string(),
                ]),
                watch_urls: BTreeSet::from(["stargate.region-a:50071".to_string()]),
            },
        );
        let desired_urls = desired_watch_urls_from_snapshots(&seeds, &snapshots);
        assert!(all_desired_watch_urls_have_snapshots(
            &desired_urls,
            |watch_url| snapshots.contains_key(watch_url)
        ));
        assert_eq!(
            active_registration_routers(snapshots.values()),
            BTreeSet::from([
                "stargate-0.region-a:50071".to_string(),
                "stargate-0.region-b:50071".to_string(),
                "stargate-1.region-a:50071".to_string(),
                "stargate-1.region-b:50071".to_string(),
            ])
        );
    }

    #[test]
    fn recursive_watch_discovery_ignores_disconnected_snapshot_cycles() {
        let seeds = BTreeSet::from(["stargate.region-a:50071".to_string()]);
        let snapshots = HashMap::from([
            (
                "stargate.region-a:50071".to_string(),
                WatchEndpointSnapshot {
                    registration_routers: BTreeSet::from(["stargate-0.region-a:50071".to_string()]),
                    watch_urls: BTreeSet::new(),
                },
            ),
            (
                "stargate.region-b:50071".to_string(),
                WatchEndpointSnapshot {
                    registration_routers: BTreeSet::from(["stargate-0.region-b:50071".to_string()]),
                    watch_urls: BTreeSet::from(["stargate.region-c:50071".to_string()]),
                },
            ),
            (
                "stargate.region-c:50071".to_string(),
                WatchEndpointSnapshot {
                    registration_routers: BTreeSet::from(["stargate-0.region-c:50071".to_string()]),
                    watch_urls: BTreeSet::from(["stargate.region-b:50071".to_string()]),
                },
            ),
        ]);

        assert_eq!(
            desired_watch_urls_from_snapshots(&seeds, &snapshots),
            BTreeSet::from(["stargate.region-a:50071".to_string()])
        );
    }

    #[tokio::test]
    async fn stop_watched_endpoint_signals_and_awaits_task() {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let (exited_tx, exited_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                if *stop_rx.borrow() {
                    break;
                }
                if stop_rx.changed().await.is_err() {
                    break;
                }
            }
            let _ = exited_tx.send(());
        });
        let endpoint = WatchedEndpoint {
            generation: 0,
            stop_tx,
            task,
            state: WatchEndpointState::Connecting,
        };

        tokio::time::timeout(Duration::from_secs(1), stop_watched_endpoint(endpoint))
            .await
            .expect("watched endpoint should stop cooperatively");
        exited_rx
            .await
            .expect("watched endpoint task should publish exit");
    }

    #[tokio::test]
    async fn watch_endpoint_update_send_wakes_on_local_stop_when_channel_is_full() {
        let update = |generation| WatchEndpointUpdate {
            watch_url: "stargate.region-b:50071".to_string(),
            generation,
            event: WatchEndpointEvent::Disconnected,
        };
        let (updates_tx, mut updates_rx) = mpsc::channel(1);
        updates_tx
            .send(update(1))
            .await
            .expect("seed update should fill channel");
        let (_parent_tx, mut parent_rx) = watch::channel(false);
        let (local_tx, mut local_rx) = watch::channel(false);

        let task = tokio::spawn(async move {
            send_watch_endpoint_update(&updates_tx, update(2), &mut parent_rx, &mut local_rx).await
        });
        tokio::task::yield_now().await;
        local_tx.send(true).expect("local stop should send");

        let sent = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("send wait should wake on local stop")
            .expect("send task should not panic");
        assert!(!sent, "stopped endpoint should not enqueue an update");
        assert_eq!(
            updates_rx
                .recv()
                .await
                .expect("first update should still be queued")
                .generation,
            1
        );
        assert!(updates_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn watch_endpoint_updates_ignore_removed_or_replaced_generations() {
        let snapshot = |router: &str| WatchEndpointSnapshot {
            registration_routers: BTreeSet::from([router.to_string()]),
            watch_urls: BTreeSet::new(),
        };
        let watch_url = "stargate.region-b:50071".to_string();
        let mut watched = HashMap::<String, WatchedEndpoint>::new();

        assert!(!apply_watch_endpoint_update(
            &mut watched,
            WatchEndpointUpdate {
                watch_url: watch_url.clone(),
                generation: 0,
                event: WatchEndpointEvent::Snapshot(snapshot("stale-router")),
            }
        ));
        assert!(active_registration_routers(watched_endpoint_snapshots(&watched)).is_empty());

        let (stop_tx, _stop_rx) = watch::channel(false);
        let task = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        watched.insert(
            watch_url.clone(),
            WatchedEndpoint {
                generation: 1,
                stop_tx,
                task,
                state: WatchEndpointState::Connecting,
            },
        );

        assert!(!apply_watch_endpoint_update(
            &mut watched,
            WatchEndpointUpdate {
                watch_url: watch_url.clone(),
                generation: 0,
                event: WatchEndpointEvent::Snapshot(snapshot("stale-router")),
            }
        ));
        assert!(!all_desired_watch_urls_have_snapshots(
            &BTreeSet::from([watch_url.clone()]),
            |watch_url| watched
                .get(watch_url)
                .is_some_and(|endpoint| endpoint.state.has_snapshot())
        ));
        assert!(active_registration_routers(watched_endpoint_snapshots(&watched)).is_empty());

        assert!(apply_watch_endpoint_update(
            &mut watched,
            WatchEndpointUpdate {
                watch_url: watch_url.clone(),
                generation: 1,
                event: WatchEndpointEvent::Snapshot(snapshot("current-router")),
            }
        ));
        assert_eq!(
            active_registration_routers(watched_endpoint_snapshots(&watched)),
            BTreeSet::from(["current-router".to_string()])
        );

        assert!(apply_watch_endpoint_update(
            &mut watched,
            WatchEndpointUpdate {
                watch_url: watch_url.clone(),
                generation: 1,
                event: WatchEndpointEvent::Disconnected,
            }
        ));
        assert!(matches!(
            watched.get(&watch_url).map(|endpoint| &endpoint.state),
            Some(WatchEndpointState::Disconnected)
        ));
        assert!(active_registration_routers(watched_endpoint_snapshots(&watched)).is_empty());

        for endpoint in watched.into_values() {
            endpoint.task.abort();
        }
    }

    #[test]
    fn watch_router_publish_gate_waits_for_initial_complete_or_timeout_then_allows_removal() {
        let empty = BTreeSet::new();
        let seed_router = BTreeSet::from(["stargate-0.region-a:50071".to_string()]);
        let global_routers = BTreeSet::from([
            "stargate-0.region-a:50071".to_string(),
            "stargate-0.region-b:50071".to_string(),
        ]);

        assert!(!should_publish_watch_routers(
            &seed_router,
            &empty,
            false,
            false,
            false
        ));
        assert!(should_publish_watch_routers(
            &seed_router,
            &empty,
            false,
            true,
            false
        ));
        assert!(!should_publish_watch_routers(
            &empty, &empty, false, true, false
        ));
        assert!(should_publish_watch_routers(
            &global_routers,
            &empty,
            true,
            false,
            false
        ));
        assert!(should_publish_watch_routers(
            &seed_router,
            &global_routers,
            false,
            false,
            true
        ));
        assert!(!should_publish_watch_routers(
            &seed_router,
            &seed_router,
            false,
            false,
            true
        ));
    }

    #[test]
    fn coordinated_calibration_fans_out_after_cluster_calibration_completes() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (_stats_tx, stats_rx) = flume::bounded(1);
        let (bringup_state_tx, bringup_state_rx) = flume::bounded(4);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(4);
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string(), "model-b".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            true,
            QueueAdmissionTracker::default(),
        );

        let mut fanout_phase = initial_registration_fanout_phase(true);
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(
            fanout_phase,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        );

        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-a".to_string(),
                state: ModelBringupState::AdvertisingActive,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(
            fanout_phase,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        );

        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-b".to_string(),
                state: ModelBringupState::AdvertisingActive,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(
            fanout_phase,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(
            fanout_phase,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-b".to_string(),
                last_mean_input_tps: 456.0,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(fanout_phase, RegistrationFanoutPhase::FullFanout);

        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-a".to_string(),
                state: ModelBringupState::Recovering,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(fanout_phase, RegistrationFanoutPhase::FullFanout);
    }

    #[test]
    fn coordinated_calibration_fanout_requires_valid_calibrated_capacity() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (_stats_tx, stats_rx) = flume::bounded(1);
        let (bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(2);
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            true,
            QueueAdmissionTracker::default(),
        );
        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-a".to_string(),
                state: ModelBringupState::AdvertisingActive,
            })
            .unwrap();
        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 0.0,
            })
            .unwrap();
        shared_state.drain_updates();

        let mut fanout_phase = initial_registration_fanout_phase(true);
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(
            fanout_phase,
            RegistrationFanoutPhase::SingleRouterUntilCalibrated
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        shared_state.drain_updates();
        fanout_phase = advance_registration_fanout_phase(fanout_phase, &shared_state, true);
        assert_eq!(fanout_phase, RegistrationFanoutPhase::FullFanout);
    }

    #[test]
    fn calibrated_model_snapshot_seeds_last_mean_input_tps_and_queue_admission() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (_stats_tx, stats_rx) = flume::bounded(1);
        let (_bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(1);
        let queue_tracker = QueueAdmissionTracker::default();
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            false,
            queue_tracker.clone(),
        );

        let initial = shared_state.snapshot(false);
        assert_eq!(
            initial["model-a"].calibration_state,
            CalibrationState::Unknown as i32
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        shared_state.drain_updates();

        let calibrated = shared_state.snapshot(false);
        assert_eq!(
            calibrated["model-a"].calibration_state,
            CalibrationState::Complete as i32
        );
        let stats = calibrated["model-a"]
            .stats
            .as_ref()
            .expect("stats should be present");
        assert_eq!(stats.last_mean_input_tps, 123.0);
        assert_eq!(
            queue_tracker
                .snapshot_model("model-a")
                .queue_time_estimate_ms_by_priority,
            Some(HashMap::new())
        );
    }

    #[test]
    fn calibrated_admission_is_seeded_before_model_advertises_active() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (_stats_tx, stats_rx) = flume::bounded(1);
        let (bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(1);
        let queue_tracker = QueueAdmissionTracker::default();
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            true,
            queue_tracker.clone(),
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-a".to_string(),
                state: ModelBringupState::AdvertisingActive,
            })
            .unwrap();

        shared_state.drain_updates_with_calibration_barrier(|| {
            assert!(shared_state.snapshot_update_lock.try_lock().is_none());
            assert_eq!(
                shared_state.current_bringup_state_rx.borrow()["model-a"],
                ModelBringupState::ConnectingUnavailable
            );
            assert_eq!(
                queue_tracker
                    .snapshot_model("model-a")
                    .queue_time_estimate_ms_by_priority,
                Some(HashMap::new())
            );
        });

        let active = shared_state.snapshot(false);
        assert_eq!(
            active["model-a"].status,
            InferenceServerStatus::Active as i32
        );
    }

    #[test]
    fn learned_calibration_seed_survives_insufficient_runtime_stats() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (stats_tx, stats_rx) = flume::bounded(1);
        let (_bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(1);
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            false,
            QueueAdmissionTracker::default(),
        );

        stats_tx
            .send((
                "model-a".to_string(),
                CurrentModelStats {
                    last_mean_input_tps: 0.0,
                    ..CurrentModelStats::default()
                },
            ))
            .unwrap();
        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        shared_state.drain_updates();

        let calibrated = shared_state.snapshot(true);
        let model = &calibrated["model-a"];
        assert_eq!(model.calibration_state, CalibrationState::Complete as i32);
        let stats = model.stats.as_ref().expect("stats should be present");
        assert_eq!(stats.last_mean_input_tps, 123.0);
    }

    #[test]
    fn snapshot_forwards_collected_model_stats_exactly() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (stats_tx, stats_rx) = flume::bounded(1);
        let (bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (_bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(1);
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            false,
            QueueAdmissionTracker::default(),
        );

        let queue_time_estimate_ms_by_priority = HashMap::from([(0, 11), (2, 7)]);
        stats_tx
            .send((
                "model-a".to_string(),
                CurrentModelStats {
                    output_tps: 2.5,
                    embedding_item_tps: 0.0,
                    last_mean_input_tps: 3.5,
                    queue_size: 4,
                    queued_input_size: 5,
                    max_output_tps: 6.5,
                    max_embedding_item_tps: 0.0,
                    kv_cache_capacity_tokens: 7,
                    kv_cache_used_tokens: 8,
                    kv_cache_free_tokens: 9,
                    num_running_queries: 10,
                    max_engine_concurrency: Some(11),
                    total_query_input_size: 12,
                    input_processing_queries: 13,
                    output_generation_queries: 14,
                    stats_observed_at_unix_ms: 15,
                    stats_capabilities: vec!["request.final_headers".to_string()],
                    stats_sources: vec!["request_metadata".to_string()],
                    queue_time_estimate_ms_by_priority: Some(
                        queue_time_estimate_ms_by_priority.clone(),
                    ),
                },
            ))
            .unwrap();
        bringup_state_tx
            .send(BringupModelUpdate {
                model_id: "model-a".to_string(),
                state: ModelBringupState::AdvertisingActive,
            })
            .unwrap();
        shared_state.drain_updates();

        let snapshot = shared_state.snapshot(false);
        let model = &snapshot["model-a"];
        assert_eq!(model.status, InferenceServerStatus::Active as i32);
        let stats = model.stats.as_ref().expect("stats should be present");
        assert_eq!(stats.output_tps, 2.5);
        assert_eq!(stats.last_mean_input_tps, 3.5);
        assert_eq!(stats.queue_size, 4);
        assert_eq!(stats.queued_input_size, 5);
        assert_eq!(stats.max_output_tps, 6.5);
        assert_eq!(stats.kv_cache_capacity_tokens, 7);
        assert_eq!(stats.kv_cache_used_tokens, 8);
        assert_eq!(stats.kv_cache_free_tokens, 9);
        assert_eq!(stats.num_running_queries, 10);
        assert_eq!(stats.max_engine_concurrency, 11);
        assert_eq!(stats.total_query_input_size, 12);
        assert_eq!(
            stats.queue_time_estimate_ms_by_priority,
            queue_time_estimate_ms_by_priority
        );
        assert_eq!(stats.input_processing_queries, 13);
        assert_eq!(stats.output_generation_queries, 14);
        assert_eq!(stats.stats_observed_at_unix_ms, 15);
        assert_eq!(
            stats.stats_capabilities,
            vec!["request.final_headers".to_string()]
        );
        assert_eq!(stats.stats_sources, vec!["request_metadata".to_string()]);
    }

    #[test]
    fn calibrated_model_snapshot_clears_calibration_complete_state() {
        let (_status_tx, status_rx) = flume::bounded(1);
        let (_stats_tx, stats_rx) = flume::bounded(1);
        let (_bringup_state_tx, bringup_state_rx) = flume::bounded(1);
        let (bringup_calibration_tx, bringup_calibration_rx) = flume::bounded(2);
        let queue_tracker = QueueAdmissionTracker::default();
        let shared_state = SharedInstState::new(
            InferenceServerStatus::Active,
            &["model-a".to_string()],
            SharedInstStateChannels {
                status_rx,
                stats_rx,
                bringup_state_rx,
                bringup_calibration_rx,
            },
            false,
            queue_tracker.clone(),
        );

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Complete {
                model_id: "model-a".to_string(),
                last_mean_input_tps: 123.0,
            })
            .unwrap();
        shared_state.drain_updates();

        let calibrated = shared_state.snapshot(false);
        assert_eq!(
            calibrated["model-a"].calibration_state,
            CalibrationState::Complete as i32
        );
        assert!(shared_state.all_models_cluster_calibration_complete());

        bringup_calibration_tx
            .send(BringupCalibrationUpdate::Clear {
                model_id: "model-a".to_string(),
            })
            .unwrap();
        shared_state.drain_updates();

        let reset = shared_state.snapshot(false);
        assert_eq!(
            reset["model-a"].calibration_state,
            CalibrationState::Unknown as i32
        );
        assert_eq!(
            reset["model-a"]
                .stats
                .as_ref()
                .expect("stats should be present")
                .last_mean_input_tps,
            0.0
        );
        assert_eq!(
            queue_tracker
                .snapshot_model("model-a")
                .queue_time_estimate_ms_by_priority,
            None
        );
        assert!(!shared_state.all_models_cluster_calibration_complete());
    }

    #[test]
    fn merge_current_model_stats_preserves_existing_kv_metrics_when_incoming_has_none() {
        let existing = CurrentModelStats {
            kv_cache_capacity_tokens: 4096,
            kv_cache_used_tokens: 1024,
            kv_cache_free_tokens: 3072,
            ..CurrentModelStats::default()
        };
        let incoming = CurrentModelStats {
            output_tps: 20.0,
            last_mean_input_tps: 30.0,
            max_output_tps: 40.0,
            queue_size: 5,
            queued_input_size: 6,
            ..CurrentModelStats::default()
        };

        let merged = merge_current_model_stats(&existing, &incoming);
        assert_eq!(merged.last_mean_input_tps, 30.0);
        assert_eq!(merged.queue_size, 5);
        assert_eq!(merged.kv_cache_capacity_tokens, 4096);
        assert_eq!(merged.kv_cache_used_tokens, 1024);
        assert_eq!(merged.kv_cache_free_tokens, 3072);
    }

    #[test]
    fn merge_current_model_stats_preserves_existing_backend_only_metrics_when_incoming_has_none() {
        let existing = CurrentModelStats {
            max_engine_concurrency: Some(8),
            queue_time_estimate_ms_by_priority: Some(HashMap::from([(4, 120)])),
            ..CurrentModelStats::default()
        };
        let incoming = CurrentModelStats {
            output_tps: 20.0,
            last_mean_input_tps: 30.0,
            max_output_tps: 40.0,
            queue_size: 5,
            queued_input_size: 6,
            ..CurrentModelStats::default()
        };

        let merged = merge_current_model_stats(&existing, &incoming);
        assert_eq!(merged.last_mean_input_tps, 30.0);
        assert_eq!(merged.queue_size, 5);
        assert_eq!(merged.max_engine_concurrency, Some(8));
        assert_eq!(
            merged.queue_time_estimate_ms_by_priority,
            Some(HashMap::from([(4, 120)]))
        );
    }

    #[test]
    fn merge_current_model_stats_accepts_explicit_backend_only_metric_clears() {
        let existing = CurrentModelStats {
            max_engine_concurrency: Some(8),
            queue_time_estimate_ms_by_priority: Some(HashMap::from([(4, 120)])),
            ..CurrentModelStats::default()
        };
        let incoming = CurrentModelStats {
            max_engine_concurrency: Some(0),
            queue_time_estimate_ms_by_priority: Some(HashMap::new()),
            ..CurrentModelStats::default()
        };

        let merged = merge_current_model_stats(&existing, &incoming);
        assert_eq!(merged.max_engine_concurrency, Some(0));
        assert_eq!(
            merged.queue_time_estimate_ms_by_priority,
            Some(HashMap::new())
        );
    }

    #[test]
    fn merge_current_model_stats_accepts_non_zero_incoming_kv_metrics() {
        let existing = CurrentModelStats {
            kv_cache_capacity_tokens: 4096,
            kv_cache_used_tokens: 1024,
            kv_cache_free_tokens: 3072,
            ..CurrentModelStats::default()
        };
        let incoming = CurrentModelStats {
            kv_cache_capacity_tokens: 8192,
            kv_cache_used_tokens: 2048,
            kv_cache_free_tokens: 6144,
            ..CurrentModelStats::default()
        };

        let merged = merge_current_model_stats(&existing, &incoming);
        assert_eq!(merged.kv_cache_capacity_tokens, 8192);
        assert_eq!(merged.kv_cache_used_tokens, 2048);
        assert_eq!(merged.kv_cache_free_tokens, 6144);
    }

    #[test]
    fn reverse_tunnel_config_propagates_metrics() {
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let config = build_reverse_quic_tunnel_config(ReverseQuicTunnelConfigParams {
            dial_addr: "127.0.0.1:12345".to_string(),
            sni_override: None,
            inference_server_id: "inst-a".to_string(),
            upstream_http_base_url: "http://127.0.0.1:8090/".to_string(),
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::Http3,
            output_token_parser_factory: OutputTokenParserFactory,
            request_observation_tx: None,
            request_quality_monitor: RequestQualityMonitorConfig::default(),
            auth_token_provider: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            metrics: Some(metrics.clone()),
        });

        assert!(
            Arc::ptr_eq(config.metrics.as_ref().unwrap(), &metrics),
            "reverse tunnel config should carry pylon metrics"
        );
        assert_eq!(config.tunnel_protocol, TunnelTransportProtocol::Http3);
    }

    #[test]
    fn reverse_tunnel_endpoint_from_ack_uses_pylon_dial_addr_and_preserves_routing_sni() {
        let endpoint = reverse_tunnel_endpoint_from_ack(&InferenceServerAck {
            reverse_tunnel_target: "stargate-0.stargate-headless.stargate.svc.cluster.local:50072"
                .to_string(),
            reverse_tunnel_pylon_dial_addr: "stargate-quic-lb.stargate.svc.cluster.local:50072"
                .to_string(),
            model_calibration_directives: Vec::new(),
        })
        .expect("ack should contain reverse tunnel endpoint");

        assert_eq!(
            endpoint.pylon_dial_addr,
            "stargate-quic-lb.stargate.svc.cluster.local:50072"
        );
        assert_eq!(
            endpoint.routing_target_addr,
            "stargate-0.stargate-headless.stargate.svc.cluster.local:50072"
        );
        assert_eq!(
            endpoint.sni_override.as_deref(),
            Some("stargate-0.stargate-headless.stargate.svc.cluster.local")
        );
    }

    #[test]
    fn reverse_tunnel_endpoint_from_ack_falls_back_to_target_for_legacy_ack() {
        let endpoint = reverse_tunnel_endpoint_from_ack(&InferenceServerAck {
            reverse_tunnel_target: "stargate-0.stargate-headless.stargate.svc.cluster.local:50072"
                .to_string(),
            reverse_tunnel_pylon_dial_addr: String::new(),
            model_calibration_directives: Vec::new(),
        })
        .expect("legacy ack should contain reverse tunnel endpoint");

        assert_eq!(
            endpoint.pylon_dial_addr,
            "stargate-0.stargate-headless.stargate.svc.cluster.local:50072"
        );
        assert_eq!(endpoint.sni_override, None);
    }

    #[tokio::test]
    async fn reverse_tunnel_connect_attempt_times_out() {
        let result = reverse_tunnel_connect_with_timeout(
            Duration::from_millis(1),
            std::future::pending::<Result<ReverseQuicTunnelHandle, TunnelError>>(),
        )
        .await;

        assert!(
            matches!(result, Err(TunnelError::Connect(message)) if message.contains("timed out")),
            "expected timeout connect error"
        );
    }

    #[test]
    fn reverse_tunnel_config_propagates_request_quality_monitor() {
        let request_quality_monitor = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 7,
            output_tokens_threshold_min: Some(9),
            output_compression_threshold_max: Some(0.4),
            output_degeneracy_threshold_min: Some(0.5),
            output_repetition_1gram_threshold_min: Some(0.6),
            output_repetition_2gram_threshold_min: Some(0.7),
            output_repetition_3gram_threshold_min: Some(0.8),
            median_logprob_threshold_max: Some(-6.5),
        };

        let config = build_reverse_quic_tunnel_config(ReverseQuicTunnelConfigParams {
            dial_addr: "127.0.0.1:12345".to_string(),
            sni_override: None,
            inference_server_id: "inst-a".to_string(),
            upstream_http_base_url: "http://127.0.0.1:8090/".to_string(),
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::Custom,
            output_token_parser_factory: OutputTokenParserFactory,
            request_observation_tx: None,
            request_quality_monitor: request_quality_monitor.clone(),
            auth_token_provider: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            metrics: None,
        });

        assert!(config.request_quality_monitor.collect_quality_metrics);
        assert_eq!(
            config
                .request_quality_monitor
                .collect_quality_metrics_min_tokens,
            7
        );
        assert_eq!(
            config.request_quality_monitor.output_tokens_threshold_min,
            Some(9)
        );
        assert_eq!(
            config
                .request_quality_monitor
                .output_compression_threshold_max,
            Some(0.4)
        );
        assert_eq!(
            config
                .request_quality_monitor
                .output_degeneracy_threshold_min,
            Some(0.5)
        );
        assert_eq!(
            config
                .request_quality_monitor
                .output_repetition_1gram_threshold_min,
            Some(0.6)
        );
        assert_eq!(
            config
                .request_quality_monitor
                .output_repetition_2gram_threshold_min,
            Some(0.7)
        );
        assert_eq!(
            config
                .request_quality_monitor
                .output_repetition_3gram_threshold_min,
            Some(0.8)
        );
        assert_eq!(
            config.request_quality_monitor.median_logprob_threshold_max,
            Some(-6.5)
        );
    }

    #[test]
    fn router_registration_task_harness_propagates_request_quality_monitor_to_each_router() {
        let request_quality_monitor = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 7,
            output_tokens_threshold_min: Some(9),
            output_compression_threshold_max: Some(0.4),
            output_degeneracy_threshold_min: Some(0.5),
            output_repetition_1gram_threshold_min: Some(0.6),
            output_repetition_2gram_threshold_min: Some(0.7),
            output_repetition_3gram_threshold_min: Some(0.8),
            median_logprob_threshold_max: Some(-6.5),
        };
        let register_config = InferenceServerRegistrationConfig {
            seeds: vec!["router-a".to_string(), "router-b".to_string()],
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-a".to_string(),
            inference_server_url: "quic://127.0.0.1:8443".to_string(),
            upstream_http_base_url: Some("http://127.0.0.1:8090".to_string()),
            min_update_interval: Duration::from_secs(2),
            status: InferenceServerStatus::Active,
            reverse_tunnel: true,
            quic_insecure: true,
            tunnel_protocol: TunnelTransportProtocol::Http3,
            bringup: BringupConfig::default(),
            output_token_parser_factory: OutputTokenParserFactory,
            request_observation_tx: None,
            request_quality_monitor: request_quality_monitor.clone(),
            metrics: None,
            retry: PylonRetryConfig::default(),
            queue_mismatch_retry: PylonQueueMismatchRetryConfig::default(),
            queue_tracker: QueueAdmissionTracker::default(),
            auth_token_provider: None,
        };
        let cancel_token = CancellationToken::new();
        let (cluster_calibration_directive_tx, _cluster_calibration_directive_rx) =
            flume::bounded(1);
        let task_template = RouterRegistrationTaskTemplate::from_registration_config(
            &register_config,
            &register_config.cluster_id,
            register_config.upstream_http_base_url.as_deref().unwrap(),
            cluster_calibration_directive_tx,
            &cancel_token,
        );

        for router in ["router-a", "router-b"] {
            let task_config = task_template.build_for_router(router.to_string());
            assert_eq!(task_config.router_addr, router);
            assert_eq!(task_config.inference_server_id, "inst-a");
            assert_eq!(task_config.cluster_id, "cluster-a");
            assert_eq!(task_config.inference_server_url, "http://127.0.0.1:8090");
            assert_eq!(task_config.min_update_interval, Duration::from_secs(2));
            assert!(task_config.reverse_tunnel);
            assert!(task_config.coordinated_calibration);
            assert!(task_config.quic_insecure);
            assert_eq!(task_config.tunnel_protocol, TunnelTransportProtocol::Http3);
            assert_eq!(task_config.upstream_http_base_url, "http://127.0.0.1:8090");
            assert!(task_config.request_quality_monitor.collect_quality_metrics);
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .collect_quality_metrics_min_tokens,
                7
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_tokens_threshold_min,
                Some(9)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_compression_threshold_max,
                Some(0.4)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_degeneracy_threshold_min,
                Some(0.5)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_repetition_1gram_threshold_min,
                Some(0.6)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_repetition_2gram_threshold_min,
                Some(0.7)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .output_repetition_3gram_threshold_min,
                Some(0.8)
            );
            assert_eq!(
                task_config
                    .request_quality_monitor
                    .median_logprob_threshold_max,
                Some(-6.5)
            );
        }
    }
}
