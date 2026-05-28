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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant as TokioInstant;

use crate::queue_admission::QueueAdmissionTracker;
use crate::request_observer::{RequestObservationEndpoint, RequestObservationState};
use crate::token_metrics::{EventAggregatorConfig, TpsDistribution};
use crate::{CurrentModelStats, PylonMetrics, RequestObservation};

const DEFAULT_OBSERVATION_CHANNEL_CAPACITY: usize = 1024;
const DEFAULT_SMOOTHING_WINDOW_SIZE: usize = 8;
const DEFAULT_MIN_INPUT_TOKENS: u64 = 1;
const DEFAULT_MIN_OUTPUT_TOKENS: u64 = 1;
const DEFAULT_DURATION_FLOOR: Duration = Duration::from_millis(10);
const DEFAULT_KV_CACHE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_KV_CACHE_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_ENGINE_STATS_REQUEST_TTL: Duration = Duration::from_secs(300);
const DEFAULT_ENGINE_STATS_MODEL_TTL: Duration = Duration::from_secs(30);
const DEFAULT_ENGINE_STATS_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct StatsCollectorConfig {
    pub observation_channel_capacity: usize,
    pub smoothing_window_size: usize,
    pub min_input_tokens: u64,
    pub min_output_tokens: u64,
    pub duration_floor: Duration,
    pub configured_model_ids: Vec<String>,
    /// Pins input throughput for deterministic benchmarks instead of publishing learned samples.
    pub fixed_last_mean_input_tps: Option<f64>,
    pub kv_cache_stats_url: Option<String>,
    pub kv_cache_poll_interval: Duration,
    pub kv_cache_request_timeout: Duration,
    pub engine_stats_request_ttl: Duration,
    pub engine_stats_model_ttl: Duration,
    pub engine_stats_sweep_interval: Duration,
    pub openai_fallback_stats_enabled: bool,
    pub queue_tracker: QueueAdmissionTracker,
    pub metrics: Option<Arc<PylonMetrics>>,
}

impl Default for StatsCollectorConfig {
    fn default() -> Self {
        Self {
            observation_channel_capacity: DEFAULT_OBSERVATION_CHANNEL_CAPACITY,
            smoothing_window_size: DEFAULT_SMOOTHING_WINDOW_SIZE,
            min_input_tokens: DEFAULT_MIN_INPUT_TOKENS,
            min_output_tokens: DEFAULT_MIN_OUTPUT_TOKENS,
            duration_floor: DEFAULT_DURATION_FLOOR,
            configured_model_ids: Vec::new(),
            fixed_last_mean_input_tps: None,
            kv_cache_stats_url: None,
            kv_cache_poll_interval: DEFAULT_KV_CACHE_POLL_INTERVAL,
            kv_cache_request_timeout: DEFAULT_KV_CACHE_REQUEST_TIMEOUT,
            engine_stats_request_ttl: DEFAULT_ENGINE_STATS_REQUEST_TTL,
            engine_stats_model_ttl: DEFAULT_ENGINE_STATS_MODEL_TTL,
            engine_stats_sweep_interval: DEFAULT_ENGINE_STATS_SWEEP_INTERVAL,
            openai_fallback_stats_enabled: true,
            queue_tracker: QueueAdmissionTracker::default(),
            metrics: None,
        }
    }
}

pub fn request_observation_channel(
    config: &StatsCollectorConfig,
) -> (
    flume::Sender<RequestObservation>,
    flume::Receiver<RequestObservation>,
) {
    flume::bounded(config.observation_channel_capacity)
}

pub fn stats_aggregator_update_channel(
    config: &StatsCollectorConfig,
) -> (
    flume::Sender<StatsAggregatorUpdate>,
    flume::Receiver<StatsAggregatorUpdate>,
) {
    flume::bounded(config.observation_channel_capacity)
}

pub struct StatsCollectorHandle {
    task: JoinHandle<()>,
}

impl StatsCollectorHandle {
    pub async fn shutdown(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

#[derive(Debug, Default)]
struct ModelMetricsState {
    last_mean_input_tps: f64,
    chat_output_tps_samples: VecDeque<f64>,
    chat_output_tps_sum: f64,
    embedding_item_tps_samples: VecDeque<f64>,
    embedding_item_tps_sum: f64,
    max_chat_output_tps: f64,
    max_embedding_item_tps: f64,
    kv_cache: KvCacheStatsSnapshot,
    stream_input_tps_distribution: TpsDistribution,
    request_metadata_stats_observed: bool,
    chunk_usage_stats_observed: bool,
    kv_cache_stats_observed: bool,
    engine_stream_stats_observed: bool,
    last_stats_event_at: Option<TokioInstant>,
    stats_observed_at_unix_ms: u64,
}

#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
struct KvCacheStatsSnapshot {
    model: String,
    kv_cache_capacity_tokens: u64,
    kv_cache_used_tokens: u64,
    kv_cache_free_tokens: u64,
}

struct InFlightRequestState {
    endpoint: RequestObservationEndpoint,
    model_id: String,
    output_tokens: u64,
    time_to_first_output: Option<Duration>,
    time_to_first_token: Option<Duration>,
    total_duration: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsUpdateSource {
    EngineStatsStream,
    OpenAiFallback,
}

#[derive(Debug, Clone)]
pub enum StatsAggregatorUpdate {
    RequestCounters(RequestCounterUpdate),
    RequestObservation(RequestObservationStatsUpdate),
    FinalizeRequest(FinalizeRequestUpdate),
    EnableOpenAiFallback,
}

#[derive(Debug, Clone)]
pub struct RequestCounterUpdate {
    pub(crate) source: StatsUpdateSource,
    pub(crate) request_id: String,
    pub(crate) model_id: String,
    pub(crate) tokens_processed: Option<u64>,
    pub(crate) tokens_generated: Option<u64>,
    pub(crate) finished: bool,
    pub(crate) observed_at: TokioInstant,
}

impl RequestCounterUpdate {
    pub fn new(
        source: StatsUpdateSource,
        request_id: impl Into<String>,
        model_id: impl Into<String>,
        tokens_processed: Option<u64>,
        tokens_generated: Option<u64>,
        finished: bool,
        observed_at: TokioInstant,
    ) -> Self {
        Self {
            source,
            request_id: request_id.into(),
            model_id: model_id.into(),
            tokens_processed,
            tokens_generated,
            finished,
            observed_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestObservationStatsUpdate {
    pub(crate) model_id: String,
    pub(crate) input_tokens: Option<u64>,
    pub(crate) input_duration: Option<Duration>,
    pub(crate) clamp_input_duration_to_floor: bool,
    pub(crate) embedding_items: Option<u64>,
    pub(crate) embedding_duration: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct FinalizeRequestUpdate {
    pub(crate) source: StatsUpdateSource,
    pub(crate) request_id: String,
    pub(crate) observed_at: TokioInstant,
}

impl FinalizeRequestUpdate {
    pub fn new(
        source: StatsUpdateSource,
        request_id: impl Into<String>,
        observed_at: TokioInstant,
    ) -> Self {
        Self {
            source,
            request_id: request_id.into(),
            observed_at,
        }
    }
}

#[derive(Debug)]
struct RequestCounterState {
    model_id: String,
    input: CounterSampleState,
    output: CounterSampleState,
    last_seen_at: TokioInstant,
}

impl RequestCounterState {
    fn from_observed(
        model_id: String,
        tokens_processed: u64,
        tokens_generated: u64,
        observed_at: TokioInstant,
    ) -> Self {
        Self {
            model_id,
            input: CounterSampleState::from_observed(tokens_processed, observed_at),
            output: CounterSampleState::from_observed(tokens_generated, observed_at),
            last_seen_at: observed_at,
        }
    }

    fn from_zero_baseline(model_id: String, observed_at: TokioInstant) -> Self {
        Self {
            model_id,
            input: CounterSampleState::from_observed(0, observed_at),
            output: CounterSampleState::from_observed(0, observed_at),
            last_seen_at: observed_at,
        }
    }
}

#[derive(Debug)]
struct CounterSampleState {
    observed: u64,
    sampled: u64,
    sampled_at: TokioInstant,
}

impl CounterSampleState {
    fn from_observed(observed: u64, observed_at: TokioInstant) -> Self {
        Self {
            observed,
            sampled: observed,
            sampled_at: observed_at,
        }
    }

    fn is_regression(&self, next: u64) -> bool {
        next < self.observed
    }

    fn observe(
        &mut self,
        next: u64,
        observed_at: TokioInstant,
        min_units: u64,
        duration_floor: Duration,
    ) -> Option<CounterSample> {
        let prior_observed = self.observed;
        self.observed = next;

        let units = next.saturating_sub(self.sampled);
        if units == 0 || units < min_units {
            return None;
        }

        let mut duration = observed_at
            .checked_duration_since(self.sampled_at)
            .unwrap_or(Duration::ZERO);
        if duration < duration_floor && self.sampled == 0 && prior_observed == 0 {
            duration = duration_floor;
        }
        if duration < duration_floor {
            return None;
        }

        self.sampled = next;
        self.sampled_at = observed_at;
        Some(CounterSample { units, duration })
    }
}

#[derive(Debug)]
struct CounterSample {
    units: u64,
    duration: Duration,
}

struct SharedStatsAggregator {
    config: StatsCollectorConfig,
    per_model: HashMap<String, ModelMetricsState>,
    per_request: HashMap<String, RequestCounterState>,
    finalized_requests: HashMap<String, TokioInstant>,
    unix_ms_anchor: u64,
    instant_anchor: TokioInstant,
}

impl SharedStatsAggregator {
    fn new(config: StatsCollectorConfig) -> Self {
        Self {
            config,
            per_model: HashMap::new(),
            per_request: HashMap::new(),
            finalized_requests: HashMap::new(),
            unix_ms_anchor: current_unix_millis(),
            instant_anchor: TokioInstant::now(),
        }
    }

    fn apply_update(&mut self, update: StatsAggregatorUpdate) -> Vec<(String, CurrentModelStats)> {
        let mut updated_models = Vec::new();
        self.apply_update_into(update, &mut updated_models);
        updated_models
    }

    fn apply_update_into(
        &mut self,
        update: StatsAggregatorUpdate,
        updated_models: &mut Vec<(String, CurrentModelStats)>,
    ) {
        match update {
            StatsAggregatorUpdate::RequestCounters(update) => {
                self.apply_request_counters_into(update, updated_models);
            }
            StatsAggregatorUpdate::RequestObservation(update) => {
                updated_models.extend(self.apply_request_observation(update));
            }
            StatsAggregatorUpdate::FinalizeRequest(update) => {
                updated_models.extend(self.finalize_request(update));
            }
            StatsAggregatorUpdate::EnableOpenAiFallback => {}
        }
    }

    fn live_request_count(&self) -> usize {
        self.per_request.len()
    }

    fn model_state_count(&self) -> usize {
        self.per_model.len()
    }

    fn unix_millis_at(&self, observed_at: TokioInstant) -> u64 {
        if let Some(elapsed) = observed_at.checked_duration_since(self.instant_anchor) {
            return self
                .unix_ms_anchor
                .saturating_add(duration_millis_u64(elapsed));
        }
        let elapsed = self
            .instant_anchor
            .checked_duration_since(observed_at)
            .unwrap_or(Duration::ZERO);
        self.unix_ms_anchor
            .saturating_sub(duration_millis_u64(elapsed))
    }

    fn sweep_stale(&mut self, now: TokioInstant) -> Vec<(String, CurrentModelStats)> {
        let mut dirty_models = Vec::new();
        let request_ttl = self.config.engine_stats_request_ttl;
        if !request_ttl.is_zero() {
            let mut stale_requests = Vec::new();
            for (request_id, state) in &self.per_request {
                if elapsed_since(now, state.last_seen_at) >= request_ttl {
                    stale_requests.push((request_id.clone(), state.model_id.clone()));
                }
            }
            for (request_id, model_id) in stale_requests {
                if self.per_request.remove(&request_id).is_some() {
                    self.finalized_requests.insert(request_id.clone(), now);
                    tracing::warn!(
                        request_id,
                        model_id,
                        ttl_ms = request_ttl.as_millis(),
                        "removing stale engine stats request entry"
                    );
                    if let Some(metrics) = &self.config.metrics {
                        metrics
                            .observe_engine_stats_stale_cleanup("request", "engine_stats_stream");
                    }
                    push_dirty_model(&mut dirty_models, model_id);
                }
            }

            self.finalized_requests
                .retain(|_, finalized_at| elapsed_since(now, *finalized_at) < request_ttl);
        }

        let model_ttl = self.config.engine_stats_model_ttl;
        if !model_ttl.is_zero() {
            for (model_id, state) in &mut self.per_model {
                if state
                    .last_stats_event_at
                    .is_some_and(|observed_at| elapsed_since(now, observed_at) >= model_ttl)
                    && state.clear_live_output_tps()
                {
                    state.stats_observed_at_unix_ms = current_unix_millis();
                    tracing::warn!(
                        model_id,
                        ttl_ms = model_ttl.as_millis(),
                        "clearing stale engine stats output TPS"
                    );
                    if let Some(metrics) = &self.config.metrics {
                        metrics.observe_engine_stats_stale_cleanup("stats", "engine_stats_stream");
                    }
                    push_dirty_model(&mut dirty_models, model_id.clone());
                }
            }
        }

        if let Some(metrics) = &self.config.metrics {
            metrics
                .observe_engine_stats_model_states("engine_stats_stream", self.model_state_count());
            for _ in &dirty_models {
                metrics.observe_engine_stats_dirty_snapshot("engine_stats_stream", "stale");
            }
        }

        dirty_models
            .into_iter()
            .map(|model_id| {
                let stats = self.snapshot(&model_id);
                (model_id, stats)
            })
            .collect()
    }

    fn apply_request_counters_into(
        &mut self,
        update: RequestCounterUpdate,
        updated_models: &mut Vec<(String, CurrentModelStats)>,
    ) {
        let RequestCounterUpdate {
            source,
            request_id,
            model_id,
            tokens_processed,
            tokens_generated,
            finished,
            observed_at,
        } = update;

        if self.finalized_requests.contains_key(&request_id) {
            tracing::warn!(
                request_id,
                source = ?source,
                "ignoring stats event after request finalization"
            );
            if let Some(metrics) = &self.config.metrics {
                metrics.observe_engine_stats_invalid_event("post_finalize");
            }
            return;
        }

        if !self.configured_model_allowed(&model_id) {
            tracing::warn!(
                model_id,
                configured_models = ?self.config.configured_model_ids,
                "dropping stats event for unconfigured model"
            );
            if let Some(metrics) = &self.config.metrics {
                metrics.observe_engine_stats_invalid_event("unconfigured_model");
            }
            return;
        }

        if self
            .per_request
            .get(&request_id)
            .is_some_and(|state| state.model_id != model_id)
        {
            let prior_model = self
                .per_request
                .remove(&request_id)
                .map(|state| state.model_id)
                .unwrap_or_default();
            tracing::warn!(
                request_id,
                prior_model,
                model_id,
                "resetting request stats after model changed"
            );
        }

        let duration_floor = self.config.duration_floor;
        let min_input_tokens = self.config.min_input_tokens;
        let min_output_tokens = self.config.min_output_tokens;
        let mut new_request_state = None;
        let mut remove_finished_request = false;
        let mut input_sample = None;
        let mut output_sample = None;

        if let Some(state) = self.per_request.get_mut(&request_id) {
            if tokens_processed.is_some_and(|next| state.input.is_regression(next))
                || tokens_generated.is_some_and(|next| state.output.is_regression(next))
            {
                tracing::warn!(
                    request_id,
                    model_id,
                    prior_tokens_processed = state.input.observed,
                    tokens_processed = tokens_processed.unwrap_or(state.input.observed),
                    prior_tokens_generated = state.output.observed,
                    tokens_generated = tokens_generated.unwrap_or(state.output.observed),
                    source = ?source,
                    "ignoring regressing request stats counters"
                );
                if let Some(metrics) = &self.config.metrics {
                    metrics.observe_engine_stats_invalid_event("regressing_counters");
                }
                return;
            }

            if let Some(next_tokens_processed) = tokens_processed {
                input_sample = state.input.observe(
                    next_tokens_processed,
                    observed_at,
                    min_input_tokens,
                    duration_floor,
                );
            }
            if let Some(next_tokens_generated) = tokens_generated {
                output_sample = state.output.observe(
                    next_tokens_generated,
                    observed_at,
                    min_output_tokens,
                    duration_floor,
                );
            }
            state.last_seen_at = observed_at;
            remove_finished_request = finished;
        } else {
            let next_tokens_processed = tokens_processed.unwrap_or(0);
            let next_tokens_generated = tokens_generated.unwrap_or(0);
            let mut state = if source == StatsUpdateSource::EngineStatsStream {
                RequestCounterState::from_zero_baseline(model_id.clone(), observed_at)
            } else {
                RequestCounterState::from_observed(
                    model_id.clone(),
                    next_tokens_processed,
                    next_tokens_generated,
                    observed_at,
                )
            };

            if source == StatsUpdateSource::EngineStatsStream {
                if let Some(next_tokens_processed) = tokens_processed {
                    input_sample = state.input.observe(
                        next_tokens_processed,
                        observed_at,
                        min_input_tokens,
                        duration_floor,
                    );
                }
                if let Some(next_tokens_generated) = tokens_generated {
                    output_sample = state.output.observe(
                        next_tokens_generated,
                        observed_at,
                        min_output_tokens,
                        duration_floor,
                    );
                }
            }

            if !finished {
                new_request_state = Some(state);
            }
        }
        let stats_observed_at_unix_ms = self.unix_millis_at(observed_at);

        if finished {
            if remove_finished_request {
                self.per_request.remove(&request_id);
            }
            self.finalized_requests.insert(request_id, observed_at);
        } else if let Some(state) = new_request_state {
            self.per_request.insert(request_id, state);
        }

        let smoothing_window_size = self.config.smoothing_window_size;
        let (dirty, mut stats) = {
            let mut dirty = false;
            let model_state = self.per_model.entry(model_id.clone()).or_default();
            if source == StatsUpdateSource::EngineStatsStream
                && !model_state.engine_stream_stats_observed
            {
                model_state.engine_stream_stats_observed = true;
                dirty = true;
            }
            model_state.last_stats_event_at = Some(observed_at);
            model_state.stats_observed_at_unix_ms = stats_observed_at_unix_ms;

            if let Some(sample) = input_sample
                && let Some(input_tps) =
                    tps_for_units(sample.units, sample.duration, duration_floor)
            {
                model_state.stream_input_tps_distribution.update(input_tps);
                if model_state
                    .stream_input_tps_distribution
                    .has_sufficient_data()
                    && valid_last_mean_input_tps(model_state.stream_input_tps_distribution.mean)
                {
                    let last_mean_input_tps = effective_last_mean_input_tps(
                        &self.config,
                        model_state.stream_input_tps_distribution.mean,
                    );
                    if model_state.last_mean_input_tps != last_mean_input_tps {
                        model_state.last_mean_input_tps = last_mean_input_tps;
                        self.config
                            .queue_tracker
                            .update_model_throughput(&model_id, last_mean_input_tps);
                        dirty = true;
                    }
                }
            }
            if let Some(sample) = output_sample
                && let Some(output_tps) =
                    tps_for_units(sample.units, sample.duration, duration_floor)
            {
                if output_tps > model_state.max_chat_output_tps {
                    model_state.max_chat_output_tps = output_tps;
                }
                dirty = true;
                push_sample(
                    &mut model_state.chat_output_tps_samples,
                    &mut model_state.chat_output_tps_sum,
                    output_tps,
                    smoothing_window_size,
                );
            }

            (
                dirty,
                model_state.current_stats(ModelStatsSnapshotInputs::default()),
            )
        };
        apply_fixed_last_mean_input_tps(&self.config, &mut stats);
        if dirty {
            updated_models.push((model_id, stats));
        }
    }

    fn apply_request_observation(
        &mut self,
        update: RequestObservationStatsUpdate,
    ) -> Vec<(String, CurrentModelStats)> {
        if !self.configured_model_allowed(&update.model_id) {
            return Vec::new();
        }
        let min_input_tokens = self.config.min_input_tokens;
        let duration_floor = self.config.duration_floor;
        let smoothing_window_size = self.config.smoothing_window_size;
        let model_state = self.per_model.entry(update.model_id.clone()).or_default();
        model_state.stats_observed_at_unix_ms = current_unix_millis();
        let mut dirty = false;

        if let (Some(input_tokens), Some(duration)) = (update.input_tokens, update.input_duration)
            && input_tokens >= min_input_tokens
        {
            let duration = if update.clamp_input_duration_to_floor {
                duration.max(duration_floor)
            } else {
                duration
            };
            if let Some(input_tps) = tps_for_units(input_tokens, duration, duration_floor) {
                model_state.stream_input_tps_distribution.update(input_tps);
                if model_state
                    .stream_input_tps_distribution
                    .has_sufficient_data()
                    && valid_last_mean_input_tps(model_state.stream_input_tps_distribution.mean)
                {
                    let last_mean_input_tps = effective_last_mean_input_tps(
                        &self.config,
                        model_state.stream_input_tps_distribution.mean,
                    );
                    model_state.last_mean_input_tps = last_mean_input_tps;
                    self.config
                        .queue_tracker
                        .update_model_throughput(&update.model_id, last_mean_input_tps);
                }
                dirty = true;
            }
        }

        if let (Some(embedding_items), Some(duration)) =
            (update.embedding_items, update.embedding_duration)
        {
            let duration = duration.max(duration_floor);
            if let Some(embedding_item_tps) =
                tps_for_units(embedding_items, duration, duration_floor)
            {
                if embedding_item_tps > model_state.max_embedding_item_tps {
                    model_state.max_embedding_item_tps = embedding_item_tps;
                }
                push_sample(
                    &mut model_state.embedding_item_tps_samples,
                    &mut model_state.embedding_item_tps_sum,
                    embedding_item_tps,
                    smoothing_window_size,
                );
                dirty = true;
            }
        }

        dirty
            .then(|| (update.model_id.clone(), self.snapshot(&update.model_id)))
            .into_iter()
            .collect()
    }

    fn finalize_request(
        &mut self,
        update: FinalizeRequestUpdate,
    ) -> Vec<(String, CurrentModelStats)> {
        self.finalized_requests
            .insert(update.request_id.clone(), update.observed_at);
        let Some(state) = self.per_request.remove(&update.request_id) else {
            return Vec::new();
        };
        let stats_observed_at_unix_ms = self.unix_millis_at(update.observed_at);
        if let Some(model_state) = self.per_model.get_mut(&state.model_id) {
            model_state.stats_observed_at_unix_ms = stats_observed_at_unix_ms;
        }
        tracing::debug!(
            request_id = update.request_id,
            source = ?update.source,
            "finalized request stats"
        );
        vec![(state.model_id.clone(), self.snapshot(&state.model_id))]
    }

    fn snapshot(&self, model_id: &str) -> CurrentModelStats {
        let mut stats = self
            .per_model
            .get(model_id)
            .map(|state| {
                state.current_stats(ModelStatsSnapshotInputs {
                    active_chat_output_tps: 0.0,
                    queue_size: 0,
                    queued_input_size: 0,
                    num_running_queries: 0,
                    total_query_input_size: 0,
                    input_processing_queries: 0,
                    output_generation_queries: 0,
                })
            })
            .unwrap_or_default();
        apply_fixed_last_mean_input_tps(&self.config, &mut stats);
        stats
    }

    fn configured_model_allowed(&self, model_id: &str) -> bool {
        self.config.configured_model_ids.is_empty()
            || self
                .config
                .configured_model_ids
                .iter()
                .any(|configured| configured == model_id)
    }
}

fn elapsed_since(now: TokioInstant, then: TokioInstant) -> Duration {
    now.checked_duration_since(then).unwrap_or(Duration::ZERO)
}

fn push_dirty_model(models: &mut Vec<String>, model_id: String) {
    if !models.iter().any(|existing| existing == &model_id) {
        models.push(model_id);
    }
}

#[derive(Debug, Clone)]
struct MeanInputTpsAggregatorConfig {
    min_input_tokens: u64,
    duration_floor: Duration,
    event_config: EventAggregatorConfig,
}

impl From<&StatsCollectorConfig> for MeanInputTpsAggregatorConfig {
    fn from(config: &StatsCollectorConfig) -> Self {
        Self {
            min_input_tokens: config.min_input_tokens,
            duration_floor: config.duration_floor,
            event_config: EventAggregatorConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct MeanInputTpsUpdate {
    model_id: String,
    last_mean_input_tps: f64,
}

#[derive(Debug, Clone)]
struct MeanInputTpsObservation {
    request_id: String,
    model_id: String,
    input_tokens: u64,
    input_tokens_processed: u64,
    direct_sample: Option<MeanInputDirectInputSample>,
    state: RequestObservationState,
}

impl MeanInputTpsObservation {
    fn from_request_observation(observation: &RequestObservation) -> Self {
        Self {
            request_id: observation.request_id.clone(),
            model_id: observation.model_id.clone(),
            input_tokens: observation.input_tokens,
            input_tokens_processed: observed_progress_input_tokens_processed(observation),
            direct_sample: observed_direct_mean_input_sample(observation),
            state: observation.state,
        }
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            RequestObservationState::Complete
                | RequestObservationState::Failed
                | RequestObservationState::Cancelled
        )
    }
}

#[derive(Debug)]
struct MeanInputRequestState {
    model_id: String,
    last_input_tokens_processed: u64,
    saw_incremental_progress: bool,
    input_processing: bool,
}

#[derive(Debug, Clone, Copy)]
struct MeanInputDirectInputSample {
    input_tokens: u64,
    duration: Duration,
    clamp_duration_to_floor: bool,
}

struct MeanInputModelState {
    pending_input_tokens: u64,
    elapsed_since_last_input_sample: Duration,
    active_input_requests: usize,
    distribution: TpsDistribution,
}

impl MeanInputModelState {
    fn new() -> Self {
        Self {
            pending_input_tokens: 0,
            elapsed_since_last_input_sample: Duration::ZERO,
            active_input_requests: 0,
            distribution: TpsDistribution::default(),
        }
    }
}

struct MeanInputTpsAggregator {
    config: MeanInputTpsAggregatorConfig,
    per_model: HashMap<String, MeanInputModelState>,
    per_request: HashMap<String, MeanInputRequestState>,
}

impl MeanInputTpsAggregator {
    fn new(config: MeanInputTpsAggregatorConfig) -> Self {
        Self {
            config,
            per_model: HashMap::new(),
            per_request: HashMap::new(),
        }
    }

    #[cfg(test)]
    fn record_request_observation(
        &mut self,
        observation: &RequestObservation,
    ) -> Vec<MeanInputTpsUpdate> {
        self.record_observation(MeanInputTpsObservation::from_request_observation(
            observation,
        ))
    }

    fn record_observation(
        &mut self,
        observation: MeanInputTpsObservation,
    ) -> Vec<MeanInputTpsUpdate> {
        let mut state = self
            .per_request
            .remove(&observation.request_id)
            .unwrap_or_else(|| MeanInputRequestState {
                model_id: observation.model_id.clone(),
                last_input_tokens_processed: 0,
                saw_incremental_progress: false,
                input_processing: false,
            });
        if state.model_id != observation.model_id {
            if state.input_processing {
                self.decrement_active_input_request(&state.model_id);
            }
            state = MeanInputRequestState {
                model_id: observation.model_id.clone(),
                last_input_tokens_processed: 0,
                saw_incremental_progress: false,
                input_processing: false,
            };
        }

        let track_input_tps = observation.input_tokens >= self.config.min_input_tokens;
        let terminal_direct_sample = observation.is_terminal()
            && !state.saw_incremental_progress
            && observation.direct_sample.is_some();
        if track_input_tps && observation.input_tokens_processed > state.last_input_tokens_processed
        {
            let delta = observation.input_tokens_processed - state.last_input_tokens_processed;
            state.last_input_tokens_processed = observation.input_tokens_processed;
            if !terminal_direct_sample {
                state.saw_incremental_progress = true;
                let model = self.model_state(&observation.model_id);
                model.pending_input_tokens = model
                    .pending_input_tokens
                    .checked_add(delta)
                    .expect("pending input tokens cannot overflow before request metadata storage");
            }
        }

        let input_processing =
            track_input_tps && observation.state == RequestObservationState::InputProcessing;
        if state.input_processing != input_processing {
            if input_processing {
                self.increment_active_input_request(&observation.model_id);
            } else {
                self.decrement_active_input_request(&observation.model_id);
            }
            state.input_processing = input_processing;
        }

        if !track_input_tps {
            return Vec::new();
        }

        let mut updates = Vec::new();
        if observation.state == RequestObservationState::Complete
            && !state.saw_incremental_progress
            && let Some(sample) = observation.direct_sample
            && let Some(update) = self.record_direct_sample(&observation.model_id, sample)
        {
            updates.push(update);
        }

        if !observation.is_terminal() {
            self.per_request
                .insert(observation.request_id.clone(), state);
        }

        updates
    }

    fn tick(&mut self, elapsed: Duration) -> Vec<MeanInputTpsUpdate> {
        if elapsed.is_zero() {
            return Vec::new();
        }

        let model_ids = self.per_model.keys().cloned().collect::<Vec<_>>();
        let mut updates = Vec::new();

        for model_id in model_ids {
            let Some(model) = self.per_model.get_mut(&model_id) else {
                continue;
            };
            if model.active_input_requests == 0 && model.pending_input_tokens == 0 {
                continue;
            }

            model.elapsed_since_last_input_sample += elapsed;
            if model.pending_input_tokens > 0 {
                let elapsed = model.elapsed_since_last_input_sample;
                let pending_input_tokens = std::mem::take(&mut model.pending_input_tokens);
                model.elapsed_since_last_input_sample = Duration::ZERO;
                let rate = pending_input_tokens as f64 / elapsed.as_secs_f64();
                if let Some(update) = self.record_sample(&model_id, rate) {
                    updates.push(update);
                }
            }
        }

        updates
    }

    fn increment_active_input_request(&mut self, model_id: &str) {
        let model = self.model_state(model_id);
        model.active_input_requests = model
            .active_input_requests
            .checked_add(1)
            .expect("active input request count cannot overflow before request metadata storage");
    }

    fn decrement_active_input_request(&mut self, model_id: &str) {
        let model = self
            .per_model
            .get_mut(model_id)
            .unwrap_or_else(|| panic!("missing active input request state for model {model_id}"));
        assert!(
            model.active_input_requests > 0,
            "active input request count underflow for model {model_id}"
        );
        model.active_input_requests -= 1;
        if model.active_input_requests == 0 && model.pending_input_tokens == 0 {
            model.elapsed_since_last_input_sample = Duration::ZERO;
        }
    }

    fn record_sample(&mut self, model_id: &str, sample: f64) -> Option<MeanInputTpsUpdate> {
        if !valid_last_mean_input_tps(sample) {
            return None;
        }
        let model = self.model_state(model_id);
        model.distribution.update(sample);
        model
            .distribution
            .has_sufficient_data()
            .then(|| MeanInputTpsUpdate {
                model_id: model_id.to_string(),
                last_mean_input_tps: model.distribution.mean,
            })
            .filter(|update| valid_last_mean_input_tps(update.last_mean_input_tps))
    }

    fn record_direct_sample(
        &mut self,
        model_id: &str,
        sample: MeanInputDirectInputSample,
    ) -> Option<MeanInputTpsUpdate> {
        let duration = if sample.clamp_duration_to_floor {
            sample.duration.max(self.config.duration_floor)
        } else {
            sample.duration
        };
        let sample = tps_for_units(sample.input_tokens, duration, self.config.duration_floor)?;
        self.record_sample(model_id, sample)
    }

    fn model_state(&mut self, model_id: &str) -> &mut MeanInputModelState {
        self.per_model
            .entry(model_id.to_string())
            .or_insert_with(MeanInputModelState::new)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct ModelStatsSnapshotInputs {
    active_chat_output_tps: f64,
    queue_size: u64,
    queued_input_size: u64,
    num_running_queries: u64,
    total_query_input_size: u64,
    input_processing_queries: u64,
    output_generation_queries: u64,
}

impl ModelMetricsState {
    fn clear_live_output_tps(&mut self) -> bool {
        self.last_stats_event_at = None;
        if self.chat_output_tps_samples.is_empty() {
            return false;
        }
        self.chat_output_tps_samples.clear();
        self.chat_output_tps_sum = 0.0;
        true
    }

    fn current_stats(&self, inputs: ModelStatsSnapshotInputs) -> CurrentModelStats {
        let (stats_capabilities, stats_sources) = self.stats_labels();
        CurrentModelStats {
            last_mean_input_tps: self.last_mean_input_tps,
            output_tps: inputs.active_chat_output_tps.max(average_with_sum(
                &self.chat_output_tps_samples,
                self.chat_output_tps_sum,
            )),
            embedding_item_tps: average_with_sum(
                &self.embedding_item_tps_samples,
                self.embedding_item_tps_sum,
            ),
            max_output_tps: self.max_chat_output_tps,
            max_embedding_item_tps: self.max_embedding_item_tps,
            queue_size: inputs.queue_size,
            queued_input_size: inputs.queued_input_size,
            kv_cache_capacity_tokens: self.kv_cache.kv_cache_capacity_tokens,
            kv_cache_used_tokens: self.kv_cache.kv_cache_used_tokens,
            kv_cache_free_tokens: self.kv_cache.kv_cache_free_tokens,
            num_running_queries: inputs.num_running_queries,
            max_engine_concurrency: None,
            total_query_input_size: inputs.total_query_input_size,
            queue_time_estimate_ms_by_priority: None,
            input_processing_queries: inputs.input_processing_queries,
            output_generation_queries: inputs.output_generation_queries,
            stats_observed_at_unix_ms: self.stats_observed_at_unix_ms,
            stats_capabilities,
            stats_sources,
        }
    }

    fn stats_labels(&self) -> (Vec<String>, Vec<String>) {
        // These labels are sticky per model metrics state. They describe
        // contract surfaces pylon has observed from this backend, not just the
        // surfaces exercised by the most recent request.
        let capability_count = usize::from(self.request_metadata_stats_observed)
            + usize::from(self.chunk_usage_stats_observed)
            + usize::from(self.kv_cache_stats_observed)
            + usize::from(self.engine_stream_stats_observed);
        let source_count = usize::from(self.request_metadata_stats_observed)
            + usize::from(self.chunk_usage_stats_observed)
            + usize::from(self.kv_cache_stats_observed)
            + usize::from(self.engine_stream_stats_observed);
        let mut capabilities = Vec::with_capacity(capability_count);
        let mut sources = Vec::with_capacity(source_count);
        if self.request_metadata_stats_observed {
            capabilities.push("request.final_headers".to_string());
            sources.push("request_metadata".to_string());
        }
        if self.chunk_usage_stats_observed {
            capabilities.push("request.output.chunk_usage".to_string());
            sources.push("chunk_usage".to_string());
        }
        if self.kv_cache_stats_observed {
            capabilities.push("machine.kv_cache.http".to_string());
            sources.push("kv_cache_stats".to_string());
        }
        if self.engine_stream_stats_observed {
            capabilities.push("model.throughput.engine_stream".to_string());
            sources.push("engine_stats_stream".to_string());
        }
        (capabilities, sources)
    }
}

pub fn start_stats_collector(
    config: StatsCollectorConfig,
    observation_rx: flume::Receiver<RequestObservation>,
    model_stats_tx: flume::Sender<(String, CurrentModelStats)>,
    stop_rx: watch::Receiver<bool>,
) -> StatsCollectorHandle {
    start_stats_collector_with_engine_stats(config, observation_rx, None, model_stats_tx, stop_rx)
}

pub fn start_stats_collector_with_engine_stats(
    mut config: StatsCollectorConfig,
    observation_rx: flume::Receiver<RequestObservation>,
    stats_update_rx: Option<flume::Receiver<StatsAggregatorUpdate>>,
    model_stats_tx: flume::Sender<(String, CurrentModelStats)>,
    stop_rx: watch::Receiver<bool>,
) -> StatsCollectorHandle {
    if stats_update_rx.is_some() {
        // A wired engine stats stream is the throughput source of truth. Auto
        // mode falls back only after the stream task sends EnableOpenAiFallback.
        config.openai_fallback_stats_enabled = false;
    }
    let task = tokio::spawn(async move {
        run_stats_collector(
            config,
            observation_rx,
            stats_update_rx,
            model_stats_tx,
            stop_rx,
        )
        .await;
    });
    StatsCollectorHandle { task }
}

async fn run_stats_collector(
    config: StatsCollectorConfig,
    observation_rx: flume::Receiver<RequestObservation>,
    mut stats_update_rx: Option<flume::Receiver<StatsAggregatorUpdate>>,
    model_stats_tx: flume::Sender<(String, CurrentModelStats)>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut per_model = HashMap::<String, ModelMetricsState>::new();
    let mut in_flight = HashMap::<String, InFlightRequestState>::new();
    if let Some(last_mean_input_tps) = fixed_last_mean_input_tps(&config) {
        for model_id in &config.configured_model_ids {
            config
                .queue_tracker
                .update_model_throughput(model_id, last_mean_input_tps);
            let stats = snapshot_model_stats(&config, &mut per_model, &in_flight, model_id);
            observe_model_metric(&config, model_id, &stats);
            if let Err(error) = model_stats_tx.send_async((model_id.clone(), stats)).await {
                tracing::warn!(model_id, error = %error, "dropping configured fixed input TPS stats");
                return;
            }
        }
    }
    let mut shared_aggregator = SharedStatsAggregator::new(config.clone());
    let (mean_input_tps_tx, mean_input_tps_rx) =
        flume::bounded(config.observation_channel_capacity);
    // This carries thresholded per-model mean updates, not raw request observations. Keep it
    // non-blocking so bounded input backpressure cannot deadlock the collector and aggregator.
    let (mean_input_tps_update_tx, mean_input_tps_update_rx) = flume::unbounded();
    let mean_input_tps_config = MeanInputTpsAggregatorConfig::from(&config);
    let mean_input_tps_task = tokio::spawn(run_mean_input_tps_aggregator(
        mean_input_tps_config,
        mean_input_tps_rx,
        mean_input_tps_update_tx,
    ));
    let http_client = reqwest::Client::new();
    let mut kv_cache_poll = tokio::time::interval(config.kv_cache_poll_interval);
    kv_cache_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut engine_stats_sweep = tokio::time::interval(config.engine_stats_sweep_interval);
    engine_stats_sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut openai_fallback_stats_enabled = config.openai_fallback_stats_enabled;
    let mut stats_aggregator_updated_models = Vec::with_capacity(2);
    let mut stats_aggregator_latest_models = Vec::with_capacity(2);

    'collector: loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break 'collector;
                }
            }
            observation = observation_rx.recv_async() => {
                let Ok(observation) = observation else {
                    break 'collector;
                };
                if openai_fallback_stats_enabled {
                    let mean_input_observation =
                        MeanInputTpsObservation::from_request_observation(&observation);
                    if let Err(error) = mean_input_tps_tx.send_async(mean_input_observation).await {
                        tracing::warn!(error = %error, "stopping stats collector after mean input TPS aggregator closed");
                        break 'collector;
                    }
                    let updated_models = record_observation(
                        &config,
                        &mut per_model,
                        &mut in_flight,
                        &observation,
                    );
                    observe_metrics(&config, &observation, &updated_models);

                    for (model_id, updated_stats) in updated_models {
                        if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                            tracing::warn!(model_id, error = %error, "dropping collected stats");
                            break 'collector;
                        }
                    }
                } else {
                    observe_request_metric(&config, &observation);
                    let updated_model_ids = record_lifecycle_observation(
                        &config,
                        &mut per_model,
                        &mut in_flight,
                        &observation,
                    );
                    let updated_models = shared_snapshots_with_lifecycle_load(
                        &config,
                        &mut per_model,
                        &in_flight,
                        &shared_aggregator,
                        updated_model_ids,
                    );
                    for (model_id, updated_stats) in updated_models {
                        observe_model_metric(&config, &model_id, &updated_stats);
                        if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                            tracing::warn!(model_id, error = %error, "dropping stream-mode request lifecycle stats");
                            break 'collector;
                        }
                    }
                    for update in stream_mode_observation_updates_from_observation(&observation) {
                        let mut updated_models = shared_aggregator.apply_update(update);
                        attach_lifecycle_load(&config, &mut per_model, &in_flight, &mut updated_models);
                        for (model_id, updated_stats) in updated_models {
                            observe_model_metric(&config, &model_id, &updated_stats);
                            if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                                tracing::warn!(model_id, error = %error, "dropping stream-mode request observation stats aggregator update");
                                break 'collector;
                            }
                        }
                    }
                }
                if openai_fallback_stats_enabled {
                    for update in fallback_updates_from_observation(&observation) {
                        let mut updated_models = shared_aggregator.apply_update(update);
                        attach_lifecycle_load(&config, &mut per_model, &in_flight, &mut updated_models);
                        for (model_id, updated_stats) in updated_models {
                            observe_model_metric(&config, &model_id, &updated_stats);
                            if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                                tracing::warn!(model_id, error = %error, "dropping fallback stats aggregator update");
                                break 'collector;
                            }
                        }
                    }
                }
            }
            update = mean_input_tps_update_rx.recv_async() => {
                let Ok(update) = update else {
                    break 'collector;
                };
                if !valid_last_mean_input_tps(update.last_mean_input_tps) {
                    continue;
                }
                let last_mean_input_tps =
                    effective_last_mean_input_tps(&config, update.last_mean_input_tps);
                let model_state = per_model.entry(update.model_id.clone()).or_default();
                model_state.last_mean_input_tps = last_mean_input_tps;
                config
                    .queue_tracker
                    .update_model_throughput(&update.model_id, last_mean_input_tps);
                let updated_stats = snapshot_model_stats(&config, &mut per_model, &in_flight, &update.model_id);
                observe_model_metric(&config, &update.model_id, &updated_stats);
                if let Err(error) = model_stats_tx.send_async((update.model_id.clone(), updated_stats)).await {
                    tracing::warn!(model_id = %update.model_id, error = %error, "dropping collected mean input TPS stats");
                    break 'collector;
                }
            }
            update = async {
                match &stats_update_rx {
                    Some(rx) => rx.recv_async().await.ok(),
                    None => std::future::pending().await,
                }
            } => {
                let Some(update) = update else {
                    stats_update_rx = None;
                    continue;
                };
                if apply_engine_stats_control_update(
                    &config,
                    &mut openai_fallback_stats_enabled,
                    &update,
                ) {
                    continue;
                }
                stats_aggregator_updated_models.clear();
                shared_aggregator.apply_update_into(update, &mut stats_aggregator_updated_models);
                if let Some(rx) = &stats_update_rx {
                    while let Ok(update) = rx.try_recv() {
                        if apply_engine_stats_control_update(
                            &config,
                            &mut openai_fallback_stats_enabled,
                            &update,
                        ) {
                            continue;
                        }
                        shared_aggregator.apply_update_into(
                            update,
                            &mut stats_aggregator_updated_models,
                        );
                    }
                }
                retain_latest_model_updates(
                    &mut stats_aggregator_updated_models,
                    &mut stats_aggregator_latest_models,
                );
                attach_lifecycle_load(
                    &config,
                    &mut per_model,
                    &in_flight,
                    &mut stats_aggregator_updated_models,
                );
                if let Some(metrics) = &config.metrics {
                    metrics.observe_engine_stats_live_requests(
                        "engine_stats_stream",
                        shared_aggregator.live_request_count(),
                    );
                    metrics.observe_engine_stats_model_states(
                        "engine_stats_stream",
                        shared_aggregator.model_state_count(),
                    );
                }
                while let Some((model_id, updated_stats)) = stats_aggregator_updated_models.pop() {
                    observe_model_metric(&config, &model_id, &updated_stats);
                    match model_stats_tx.try_send((model_id, updated_stats)) {
                        Ok(()) => {}
                        Err(flume::TrySendError::Full(update)) => {
                            if let Err(error) = model_stats_tx.send_async(update).await {
                                tracing::warn!(error = %error, "dropping collected engine stats stream stats");
                                break 'collector;
                            }
                        }
                        Err(flume::TrySendError::Disconnected(_)) => {
                            tracing::warn!("dropping collected engine stats stream stats after receiver closed");
                            break 'collector;
                        }
                    }
                }
            }
            _ = engine_stats_sweep.tick() => {
                let mut updated_models = shared_aggregator.sweep_stale(TokioInstant::now());
                attach_lifecycle_load(&config, &mut per_model, &in_flight, &mut updated_models);
                if let Some(metrics) = &config.metrics {
                    metrics.observe_engine_stats_live_requests(
                        "engine_stats_stream",
                        shared_aggregator.live_request_count(),
                    );
                }
                for (model_id, updated_stats) in updated_models {
                    observe_model_metric(&config, &model_id, &updated_stats);
                    if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                        tracing::warn!(model_id, error = %error, "dropping stale engine stats cleanup update");
                        break 'collector;
                    }
                }
            }
            _ = kv_cache_poll.tick(), if config.kv_cache_stats_url.is_some() => {
                let Some(kv_cache) = poll_kv_cache_stats(&config, &http_client).await else {
                    continue;
                };
                if kv_cache.model.is_empty() {
                    tracing::warn!("dropping KV-cache stats without model id");
                    continue;
                }
                if !kv_cache_stats_model_allowed(&config, &kv_cache) {
                    tracing::warn!(
                        model_id = %kv_cache.model,
                        configured_models = ?config.configured_model_ids,
                        "dropping KV-cache stats for unconfigured model"
                    );
                    continue;
                }
                let model_id = kv_cache.model.clone();
                let model_state = per_model.entry(model_id.clone()).or_default();
                model_state.kv_cache = kv_cache;
                model_state.kv_cache_stats_observed = true;
                model_state.stats_observed_at_unix_ms = current_unix_millis();
                let updated_stats =
                    snapshot_model_stats(&config, &mut per_model, &in_flight, &model_id);
                observe_model_metric(&config, &model_id, &updated_stats);
                if let Err(error) = model_stats_tx.send_async((model_id.clone(), updated_stats)).await {
                    tracing::warn!(model_id, error = %error, "dropping collected KV-cache stats");
                    break 'collector;
                }
            }
        }
    }

    mean_input_tps_task.abort();
    let _ = mean_input_tps_task.await;
}

async fn run_mean_input_tps_aggregator(
    config: MeanInputTpsAggregatorConfig,
    observation_rx: flume::Receiver<MeanInputTpsObservation>,
    update_tx: flume::Sender<MeanInputTpsUpdate>,
) {
    let mut tick = tokio::time::interval_at(
        TokioInstant::now() + config.event_config.tick_duration,
        config.event_config.tick_duration,
    );
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_tick = TokioInstant::now();
    let mut aggregator = MeanInputTpsAggregator::new(config);

    loop {
        let updates = tokio::select! {
            observation = observation_rx.recv_async() => {
                let Ok(observation) = observation else {
                    return;
                };
                aggregator.record_observation(observation)
            }
            _ = tick.tick() => {
                let now = TokioInstant::now();
                let elapsed = now
                    .checked_duration_since(last_tick)
                    .expect("tokio time moved backwards while collecting mean input TPS");
                last_tick = now;
                aggregator.tick(elapsed)
            }
        };

        for update in updates {
            if update_tx.send_async(update).await.is_err() {
                return;
            }
        }
    }
}

fn kv_cache_stats_model_allowed(
    config: &StatsCollectorConfig,
    kv_cache: &KvCacheStatsSnapshot,
) -> bool {
    config.configured_model_ids.is_empty()
        || config
            .configured_model_ids
            .iter()
            .any(|model_id| model_id == &kv_cache.model)
}

async fn poll_kv_cache_stats(
    config: &StatsCollectorConfig,
    http_client: &reqwest::Client,
) -> Option<KvCacheStatsSnapshot> {
    let url = config.kv_cache_stats_url.as_ref()?;
    let response = match http_client
        .get(url)
        .timeout(config.kv_cache_request_timeout)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(url, error = %error, "failed to poll KV-cache stats");
            return None;
        }
    };
    if !response.status().is_success() {
        tracing::warn!(url, status = %response.status(), "KV-cache stats endpoint returned non-success status");
        return None;
    }
    match response.json().await {
        Ok(stats) => Some(stats),
        Err(error) => {
            tracing::warn!(url, error = %error, "failed to parse KV-cache stats");
            None
        }
    }
}

fn observe_metrics(
    config: &StatsCollectorConfig,
    observation: &RequestObservation,
    updated_models: &[(String, CurrentModelStats)],
) {
    let Some(metrics) = &config.metrics else {
        return;
    };

    metrics.observe_request_observation(observation);
    observe_model_metrics(config, updated_models);
}

fn observe_request_metric(config: &StatsCollectorConfig, observation: &RequestObservation) {
    let Some(metrics) = &config.metrics else {
        return;
    };

    metrics.observe_request_observation(observation);
}

fn observe_model_metrics(
    config: &StatsCollectorConfig,
    updated_models: &[(String, CurrentModelStats)],
) {
    for (model_id, stats) in updated_models {
        observe_model_metric(config, model_id, stats);
    }
}

fn observe_model_metric(config: &StatsCollectorConfig, model_id: &str, stats: &CurrentModelStats) {
    let Some(metrics) = &config.metrics else {
        return;
    };

    metrics.observe_model_stats(model_id, stats);
}

fn apply_engine_stats_control_update(
    config: &StatsCollectorConfig,
    openai_fallback_stats_enabled: &mut bool,
    update: &StatsAggregatorUpdate,
) -> bool {
    if !matches!(update, StatsAggregatorUpdate::EnableOpenAiFallback) {
        return false;
    }
    if !*openai_fallback_stats_enabled {
        *openai_fallback_stats_enabled = true;
        tracing::warn!("OpenAI fallback stats enabled after engine stats stream was unsupported");
        if let Some(metrics) = &config.metrics {
            metrics.observe_engine_stats_source_transition(
                "engine_stats_stream",
                "openai_fallback",
                "unsupported",
            );
        }
    }
    true
}

fn retain_latest_model_updates(
    updates: &mut Vec<(String, CurrentModelStats)>,
    scratch: &mut Vec<(String, CurrentModelStats)>,
) {
    scratch.clear();
    while let Some(update) = updates.pop() {
        if !scratch.iter().any(|(model_id, _)| model_id == &update.0) {
            scratch.push(update);
        }
    }
    while let Some(update) = scratch.pop() {
        updates.push(update);
    }
}

fn record_in_flight_observation(
    in_flight: &mut HashMap<String, InFlightRequestState>,
    observation: &RequestObservation,
) -> Vec<String> {
    let prior_state = in_flight.remove(&observation.request_id);
    let prior_model_id = prior_state.as_ref().map(|state| state.model_id.clone());

    if !observation.is_terminal() {
        in_flight.insert(
            observation.request_id.clone(),
            InFlightRequestState {
                endpoint: observation.endpoint,
                model_id: observation.model_id.clone(),
                output_tokens: observation.output_tokens,
                time_to_first_output: observation.time_to_first_output,
                time_to_first_token: observation.time_to_first_token,
                total_duration: observation.total_duration,
            },
        );
    }

    let mut changed_models = vec![observation.model_id.clone()];
    if let Some(prior_model_id) = prior_model_id
        && prior_model_id != observation.model_id
    {
        changed_models.push(prior_model_id);
    }
    changed_models.sort();
    changed_models.dedup();
    changed_models
}

fn record_observation(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &mut HashMap<String, InFlightRequestState>,
    observation: &RequestObservation,
) -> Vec<(String, CurrentModelStats)> {
    config.queue_tracker.record_observation(observation);
    let changed_models = record_in_flight_observation(in_flight, observation);

    let model_state = per_model.entry(observation.model_id.clone()).or_default();
    model_state.stats_observed_at_unix_ms = current_unix_millis();
    if observation.has_engine_request_stats {
        model_state.request_metadata_stats_observed = true;
    }
    if observation.output_tokens_from_chunk_usage {
        model_state.chunk_usage_stats_observed = true;
    }
    if observation.state == RequestObservationState::Complete {
        let output_tps = observed_output_tps(config, observation);
        let embedding_item_tps = observed_embeddings_item_tps(config, observation);

        match observation.endpoint {
            RequestObservationEndpoint::ChatCompletions | RequestObservationEndpoint::Responses => {
                if let Some(output_tps) = output_tps {
                    if output_tps > model_state.max_chat_output_tps {
                        model_state.max_chat_output_tps = output_tps;
                    }
                    push_sample(
                        &mut model_state.chat_output_tps_samples,
                        &mut model_state.chat_output_tps_sum,
                        output_tps,
                        config.smoothing_window_size,
                    );
                }
            }
            RequestObservationEndpoint::Embeddings => {
                if let Some(embedding_item_tps) = embedding_item_tps {
                    if embedding_item_tps > model_state.max_embedding_item_tps {
                        model_state.max_embedding_item_tps = embedding_item_tps;
                    }
                    push_sample(
                        &mut model_state.embedding_item_tps_samples,
                        &mut model_state.embedding_item_tps_sum,
                        embedding_item_tps,
                        config.smoothing_window_size,
                    );
                }
            }
        }
    }

    changed_models
        .into_iter()
        .map(|model_id| {
            let stats = snapshot_model_stats(config, per_model, in_flight, &model_id);
            (model_id, stats)
        })
        .collect()
}

fn record_lifecycle_observation(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &mut HashMap<String, InFlightRequestState>,
    observation: &RequestObservation,
) -> Vec<String> {
    config.queue_tracker.record_observation(observation);
    let changed_models = record_in_flight_observation(in_flight, observation);
    let model_state = per_model.entry(observation.model_id.clone()).or_default();
    model_state.stats_observed_at_unix_ms = current_unix_millis();
    changed_models
}

fn shared_snapshots_with_lifecycle_load(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &HashMap<String, InFlightRequestState>,
    shared_aggregator: &SharedStatsAggregator,
    model_ids: Vec<String>,
) -> Vec<(String, CurrentModelStats)> {
    model_ids
        .into_iter()
        .map(|model_id| {
            let mut stats = shared_aggregator.snapshot(&model_id);
            attach_model_lifecycle_load(config, per_model, in_flight, &model_id, &mut stats);
            (model_id, stats)
        })
        .collect()
}

fn attach_lifecycle_load(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &HashMap<String, InFlightRequestState>,
    updates: &mut [(String, CurrentModelStats)],
) {
    for (model_id, stats) in updates {
        attach_model_lifecycle_load(config, per_model, in_flight, model_id, stats);
    }
}

fn attach_model_lifecycle_load(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &HashMap<String, InFlightRequestState>,
    model_id: &str,
    stats: &mut CurrentModelStats,
) {
    let lifecycle_stats = snapshot_model_stats(config, per_model, in_flight, model_id);
    stats.queue_size = lifecycle_stats.queue_size;
    stats.queued_input_size = lifecycle_stats.queued_input_size;
    stats.num_running_queries = lifecycle_stats.num_running_queries;
    stats.total_query_input_size = lifecycle_stats.total_query_input_size;
    stats.input_processing_queries = lifecycle_stats.input_processing_queries;
    stats.output_generation_queries = lifecycle_stats.output_generation_queries;
    stats.queue_time_estimate_ms_by_priority =
        lifecycle_stats.queue_time_estimate_ms_by_priority.clone();
    if has_any_current_model_kv_metrics(&lifecycle_stats) {
        stats.kv_cache_capacity_tokens = lifecycle_stats.kv_cache_capacity_tokens;
        stats.kv_cache_used_tokens = lifecycle_stats.kv_cache_used_tokens;
        stats.kv_cache_free_tokens = lifecycle_stats.kv_cache_free_tokens;
    }
    merge_label_if_observed(
        &mut stats.stats_capabilities,
        &lifecycle_stats.stats_capabilities,
        "machine.kv_cache.http",
    );
    merge_label_if_observed(
        &mut stats.stats_sources,
        &lifecycle_stats.stats_sources,
        "kv_cache_stats",
    );
    if stats.stats_observed_at_unix_ms == 0 {
        stats.stats_observed_at_unix_ms = lifecycle_stats.stats_observed_at_unix_ms;
    }
}

fn has_any_current_model_kv_metrics(stats: &CurrentModelStats) -> bool {
    stats.kv_cache_capacity_tokens > 0
        || stats.kv_cache_used_tokens > 0
        || stats.kv_cache_free_tokens > 0
}

fn merge_label_if_observed(labels: &mut Vec<String>, observed_labels: &[String], label: &str) {
    if observed_labels.iter().any(|observed| observed == label)
        && !labels.iter().any(|existing| existing == label)
    {
        labels.push(label.to_string());
    }
}

fn fallback_updates_from_observation(
    observation: &RequestObservation,
) -> Vec<StatsAggregatorUpdate> {
    let observed_at = TokioInstant::now();
    let tokens_processed = match trusted_input_tokens_processed(observation) {
        0 => None,
        tokens => Some(tokens),
    };
    let tokens_generated = observation
        .output_tokens_explicit
        .then_some(observation.output_tokens);
    if tokens_processed.is_some() || tokens_generated.is_some() {
        return vec![StatsAggregatorUpdate::RequestCounters(
            RequestCounterUpdate::new(
                StatsUpdateSource::OpenAiFallback,
                observation.request_id.clone(),
                observation.model_id.clone(),
                tokens_processed,
                tokens_generated,
                observation.is_terminal(),
                observed_at,
            ),
        )];
    }
    if observation.is_terminal() {
        return vec![StatsAggregatorUpdate::FinalizeRequest(
            FinalizeRequestUpdate::new(
                StatsUpdateSource::OpenAiFallback,
                observation.request_id.clone(),
                observed_at,
            ),
        )];
    }
    Vec::new()
}

fn stream_mode_observation_updates_from_observation(
    observation: &RequestObservation,
) -> Vec<StatsAggregatorUpdate> {
    if observation.endpoint != RequestObservationEndpoint::Embeddings
        || observation.state != RequestObservationState::Complete
    {
        return Vec::new();
    }

    let embedding_duration = observation
        .time_to_response_headers
        .map(|response_headers| observation.total_duration.saturating_sub(response_headers));

    if !observation.embedding_items_observed || embedding_duration.is_none() {
        return Vec::new();
    }

    vec![StatsAggregatorUpdate::RequestObservation(
        RequestObservationStatsUpdate {
            model_id: observation.model_id.clone(),
            input_tokens: None,
            input_duration: None,
            clamp_input_duration_to_floor: false,
            embedding_items: observation
                .embedding_items_observed
                .then_some(observation.embedding_items),
            embedding_duration,
        },
    )]
}

fn snapshot_model_stats(
    config: &StatsCollectorConfig,
    per_model: &mut HashMap<String, ModelMetricsState>,
    in_flight: &HashMap<String, InFlightRequestState>,
    model_id: &str,
) -> CurrentModelStats {
    let queue_snapshot = config.queue_tracker.snapshot_model(model_id);
    let mut active_chat_output_tps_samples = Vec::new();

    for state in in_flight
        .values()
        .filter(|state| state.model_id == model_id)
    {
        if matches!(
            state.endpoint,
            RequestObservationEndpoint::ChatCompletions | RequestObservationEndpoint::Responses
        ) && let Some(output_duration) = output_decode_duration(
            state.total_duration,
            state.time_to_first_output,
            state.time_to_first_token,
            config.duration_floor,
        ) && let Some(output_tps) =
            tps_for_units(state.output_tokens, output_duration, config.duration_floor)
        {
            active_chat_output_tps_samples.push(output_tps);
        }
    }

    let model_state = per_model.entry(model_id.to_string()).or_default();
    let mut stats = model_state.current_stats(ModelStatsSnapshotInputs {
        active_chat_output_tps: average_slice(&active_chat_output_tps_samples),
        queue_size: queue_snapshot.queue_size,
        queued_input_size: queue_snapshot.queued_input_size,
        num_running_queries: queue_snapshot.num_running_queries,
        total_query_input_size: queue_snapshot.total_query_input_size,
        input_processing_queries: queue_snapshot.input_processing_queries,
        output_generation_queries: queue_snapshot.output_generation_queries,
    });
    stats.queue_time_estimate_ms_by_priority = queue_snapshot.queue_time_estimate_ms_by_priority;
    apply_fixed_last_mean_input_tps(config, &mut stats);
    stats
}

fn observed_progress_input_tokens_processed(observation: &RequestObservation) -> u64 {
    if !observation.input_tokens_processed_from_inference_progress {
        return 0;
    }
    let trusted_input_tokens_processed = trusted_input_tokens_processed(observation);
    if trusted_input_tokens_processed > 0 {
        return trusted_input_tokens_processed;
    }
    0
}

fn observed_direct_mean_input_sample(
    observation: &RequestObservation,
) -> Option<MeanInputDirectInputSample> {
    if observation.state == RequestObservationState::Complete {
        let trusted_input_tokens_processed = trusted_input_tokens_processed(observation);
        let (input_tokens, duration) = match observation.endpoint {
            RequestObservationEndpoint::ChatCompletions | RequestObservationEndpoint::Responses => {
                if trusted_input_tokens_processed > 0 {
                    (
                        trusted_input_tokens_processed,
                        observation.time_to_input_tokens_processed?,
                    )
                } else {
                    (observation.input_tokens, observation.time_to_first_output?)
                }
            }
            RequestObservationEndpoint::Embeddings => {
                let duration = observation
                    .time_to_response_headers
                    .unwrap_or(observation.total_duration);
                (observation.input_tokens, duration)
            }
        };
        return Some(MeanInputDirectInputSample {
            input_tokens,
            duration,
            clamp_duration_to_floor: observation.endpoint == RequestObservationEndpoint::Embeddings,
        });
    }
    None
}

fn trusted_input_tokens_processed(observation: &RequestObservation) -> u64 {
    if observation.input_tokens_processed == 0 || observation.input_tokens_total_mismatch {
        return 0;
    }
    let has_matching_total =
        observation.engine_reported_input_tokens_total == Some(observation.input_tokens);
    let progress_without_total = observation.input_tokens_processed_from_inference_progress
        && observation.engine_reported_input_tokens_total.is_none();
    if !has_matching_total && !progress_without_total {
        return 0;
    }

    observation
        .input_tokens_processed
        .min(observation.input_tokens)
}

fn observed_output_tps(
    config: &StatsCollectorConfig,
    observation: &RequestObservation,
) -> Option<f64> {
    if observation.endpoint == RequestObservationEndpoint::Embeddings {
        return None;
    }
    if observation.output_tokens < config.min_output_tokens {
        return None;
    }
    let duration = output_decode_duration(
        observation.total_duration,
        observation.time_to_first_output,
        observation.time_to_first_token,
        config.duration_floor,
    )?;
    tps_for_units(observation.output_tokens, duration, config.duration_floor)
}

fn observed_embeddings_item_tps(
    config: &StatsCollectorConfig,
    observation: &RequestObservation,
) -> Option<f64> {
    if observation.endpoint != RequestObservationEndpoint::Embeddings {
        return None;
    }
    let response_headers_duration = observation.time_to_response_headers?;
    let relay_duration = observation
        .total_duration
        .saturating_sub(response_headers_duration);
    let duration = relay_duration.max(config.duration_floor);
    tps_for_units(observation.embedding_items, duration, config.duration_floor)
}

fn current_unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| u64::try_from(duration.as_millis()).ok())
        .unwrap_or_default()
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn output_decode_duration(
    total_duration: Duration,
    time_to_first_output: Option<Duration>,
    time_to_first_token: Option<Duration>,
    duration_floor: Duration,
) -> Option<Duration> {
    if let Some(time_to_first_token) = time_to_first_token {
        // Observation timestamps can arrive with the same coarse clock tick; never underflow decode time.
        let token_duration = total_duration.saturating_sub(time_to_first_token);
        if token_duration >= duration_floor {
            return Some(token_duration);
        }
    }

    time_to_first_output
        // Observation timestamps can arrive with the same coarse clock tick; never underflow decode time.
        .map(|time_to_first_output| total_duration.saturating_sub(time_to_first_output))
}

fn tps_for_units(units: u64, duration: Duration, duration_floor: Duration) -> Option<f64> {
    if units == 0 || duration < duration_floor {
        return None;
    }
    Some(units as f64 / duration.as_secs_f64())
}

fn valid_last_mean_input_tps(last_mean_input_tps: f64) -> bool {
    last_mean_input_tps > 0.0 && last_mean_input_tps.is_finite()
}

fn fixed_last_mean_input_tps(config: &StatsCollectorConfig) -> Option<f64> {
    config
        .fixed_last_mean_input_tps
        .filter(|value| valid_last_mean_input_tps(*value))
}

fn effective_last_mean_input_tps(config: &StatsCollectorConfig, observed: f64) -> f64 {
    fixed_last_mean_input_tps(config).unwrap_or(observed)
}

fn apply_fixed_last_mean_input_tps(config: &StatsCollectorConfig, stats: &mut CurrentModelStats) {
    if let Some(last_mean_input_tps) = fixed_last_mean_input_tps(config) {
        stats.last_mean_input_tps = last_mean_input_tps;
    }
}

fn push_sample(samples: &mut VecDeque<f64>, sum: &mut f64, sample: f64, window_size: usize) {
    if window_size == 0 {
        return;
    }
    samples.push_back(sample);
    *sum += sample;
    while samples.len() > window_size {
        if let Some(removed) = samples.pop_front() {
            *sum -= removed;
        }
    }
}

fn average_with_sum(samples: &VecDeque<f64>, sum: f64) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        sum / samples.len() as f64
    }
}

fn average_slice(samples: &[f64]) -> f64 {
    if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<f64>() / samples.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use tokio::net::TcpListener;

    fn completed_observation(
        input_tokens: u64,
        output_messages: u64,
        output_tokens: u64,
        time_to_first_output: Duration,
        total_duration: Duration,
    ) -> RequestObservation {
        RequestObservation {
            endpoint: crate::request_observer::RequestObservationEndpoint::ChatCompletions,
            request_id: "req-1".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages,
            output_tokens,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::Complete,
            time_to_response_headers: Some(Duration::from_millis(20)),
            time_to_input_tokens_processed: None,
            time_to_first_output: Some(time_to_first_output),
            time_to_first_token: Some(time_to_first_output),
            total_duration,
        }
    }

    fn completed_embeddings_observation(
        input_tokens: u64,
        embedding_items: u64,
        time_to_response_headers: Duration,
        total_duration: Duration,
    ) -> RequestObservation {
        RequestObservation {
            endpoint: crate::request_observer::RequestObservationEndpoint::Embeddings,
            request_id: "req-embedding".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens,
            embedding_items,
            embedding_items_observed: true,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::Complete,
            time_to_response_headers: Some(time_to_response_headers),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration,
        }
    }

    fn active_chat_observation(
        request_id: &str,
        state: RequestObservationState,
    ) -> RequestObservation {
        let time_to_first_output = (state == RequestObservationState::OutputGeneration)
            .then_some(Duration::from_millis(50));
        RequestObservation {
            endpoint: crate::request_observer::RequestObservationEndpoint::ChatCompletions,
            request_id: request_id.to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 32,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 8,
            input_tokens_processed_from_inference_progress: true,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 1,
            output_tokens: 2,
            output_tokens_explicit: true,
            output_tokens_from_chunk_usage: true,
            has_engine_request_stats: false,
            has_inference_progress_stats: true,
            state,
            time_to_response_headers: Some(Duration::from_millis(10)),
            time_to_input_tokens_processed: Some(Duration::from_millis(25)),
            time_to_first_output,
            time_to_first_token: time_to_first_output,
            total_duration: Duration::from_millis(100),
        }
    }

    fn tick_mean_input_aggregator(
        aggregator: &mut MeanInputTpsAggregator,
    ) -> Vec<MeanInputTpsUpdate> {
        aggregator.tick(EventAggregatorConfig::default().tick_duration)
    }

    async fn receive_mean_input_update(
        update_rx: &flume::Receiver<MeanInputTpsUpdate>,
    ) -> MeanInputTpsUpdate {
        for _ in 0..20 {
            if let Ok(update) = update_rx.try_recv() {
                return update;
            }
            tokio::task::yield_now().await;
        }
        panic!("mean input TPS update was not published");
    }

    fn mean_input_observation(observation: &RequestObservation) -> MeanInputTpsObservation {
        MeanInputTpsObservation::from_request_observation(observation)
    }

    #[test]
    fn latest_model_update_retention_keeps_last_snapshot_per_model() {
        let mut updates = vec![
            (
                "model-a".to_string(),
                CurrentModelStats {
                    output_tps: 1.0,
                    ..Default::default()
                },
            ),
            (
                "model-b".to_string(),
                CurrentModelStats {
                    output_tps: 2.0,
                    ..Default::default()
                },
            ),
            (
                "model-a".to_string(),
                CurrentModelStats {
                    output_tps: 3.0,
                    ..Default::default()
                },
            ),
        ];
        let mut scratch = Vec::new();

        retain_latest_model_updates(&mut updates, &mut scratch);

        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].0, "model-b");
        assert_eq!(updates[0].1.output_tps, 2.0);
        assert_eq!(updates[1].0, "model-a");
        assert_eq!(updates[1].1.output_tps, 3.0);
        assert!(scratch.is_empty());
    }

    fn stream_counter_update(
        request_id: &str,
        tokens_processed: u64,
        tokens_generated: u64,
        finished: bool,
        observed_at: TokioInstant,
    ) -> StatsAggregatorUpdate {
        StatsAggregatorUpdate::RequestCounters(RequestCounterUpdate {
            source: StatsUpdateSource::EngineStatsStream,
            request_id: request_id.to_string(),
            model_id: "model-a".to_string(),
            tokens_processed: Some(tokens_processed),
            tokens_generated: Some(tokens_generated),
            finished,
            observed_at,
        })
    }

    fn stream_counter_partial_update(
        request_id: &str,
        tokens_processed: Option<u64>,
        tokens_generated: Option<u64>,
        finished: bool,
        observed_at: TokioInstant,
    ) -> StatsAggregatorUpdate {
        StatsAggregatorUpdate::RequestCounters(RequestCounterUpdate {
            source: StatsUpdateSource::EngineStatsStream,
            request_id: request_id.to_string(),
            model_id: "model-a".to_string(),
            tokens_processed,
            tokens_generated,
            finished,
            observed_at,
        })
    }

    fn fallback_counter_update(
        request_id: &str,
        tokens_processed: u64,
        tokens_generated: u64,
        finished: bool,
        observed_at: TokioInstant,
    ) -> StatsAggregatorUpdate {
        StatsAggregatorUpdate::RequestCounters(RequestCounterUpdate {
            source: StatsUpdateSource::OpenAiFallback,
            request_id: request_id.to_string(),
            model_id: "model-a".to_string(),
            tokens_processed: Some(tokens_processed),
            tokens_generated: Some(tokens_generated),
            finished,
            observed_at,
        })
    }

    async fn receive_model_stats_with_last_mean_input_tps(
        model_stats_rx: &flume::Receiver<(String, CurrentModelStats)>,
        expected_last_mean_input_tps: f64,
    ) -> CurrentModelStats {
        for _ in 0..50 {
            while let Ok((model_id, stats)) = model_stats_rx.try_recv() {
                if model_id == "model-a"
                    && stats.last_mean_input_tps == expected_last_mean_input_tps
                {
                    return stats;
                }
            }
            tokio::task::yield_now().await;
        }
        panic!("model stats with expected last_mean_input_tps were not published");
    }

    #[test]
    fn stats_stream_cumulative_request_counters_drive_shared_aggregator() {
        let config = StatsCollectorConfig::default();
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-a", 0, 0, false, start));
        let updates = aggregator.apply_update(stream_counter_update(
            "req-a",
            10,
            4,
            false,
            start + Duration::from_millis(100),
        ));
        let stats = updates
            .into_iter()
            .find(|(model_id, _)| model_id == "model-a")
            .expect("model stats should update")
            .1;

        assert_eq!(stats.output_tps, 40.0);
        assert_eq!(stats.max_output_tps, 40.0);
        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);

        for tick in 2..=5 {
            let updates = aggregator.apply_update(stream_counter_update(
                "req-a",
                tick * 10,
                4,
                false,
                start + Duration::from_millis(tick * 100),
            ));
            if tick < 5 {
                continue;
            }
            let stats = updates
                .into_iter()
                .find(|(model_id, _)| model_id == "model-a")
                .expect("fifth input sample should publish sticky mean")
                .1;
            assert_eq!(stats.last_mean_input_tps, 100.0);
        }
    }

    #[test]
    fn fixed_input_tps_is_preserved_across_engine_stats_updates() {
        let config = StatsCollectorConfig {
            fixed_last_mean_input_tps: Some(2_200.0),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        let stats = aggregator
            .apply_update(stream_counter_update("req-a", 0, 0, false, start))
            .pop()
            .expect("first stream update should publish source labels")
            .1;
        assert_eq!(stats.last_mean_input_tps, 2_200.0);

        let mut published = None;
        for tick in 1..=5 {
            published = aggregator
                .apply_update(stream_counter_update(
                    "req-a",
                    tick * 10,
                    0,
                    false,
                    start + Duration::from_millis(tick * 100),
                ))
                .pop()
                .map(|(_, stats)| stats)
                .or(published);
        }
        assert_eq!(
            published
                .expect("sufficient input samples should publish stats")
                .last_mean_input_tps,
            2_200.0
        );
    }

    #[test]
    fn first_engine_stream_counter_without_zero_baseline_contributes_tps() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(100),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        let stats = aggregator
            .apply_update(stream_counter_update(
                "req-first-output",
                0,
                10,
                true,
                start,
            ))
            .pop()
            .expect("first output counter should publish stats")
            .1;
        assert_eq!(stats.output_tps, 100.0);
        assert_eq!(stats.max_output_tps, 100.0);

        let mut latest = None;
        for index in 0..5 {
            latest = aggregator
                .apply_update(stream_counter_update(
                    &format!("req-first-input-{index}"),
                    10,
                    0,
                    true,
                    start + Duration::from_secs(index + 1),
                ))
                .pop()
                .map(|(_, stats)| stats);
        }
        let stats = latest.expect("fifth first input counter should publish mean input stats");
        assert_eq!(stats.last_mean_input_tps, 100.0);
    }

    #[test]
    fn first_post_baseline_engine_stream_delta_under_floor_contributes_tps() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(100),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        let label_stats = aggregator
            .apply_update(stream_counter_update("req-fast", 0, 0, false, start))
            .pop()
            .expect("first engine stream event should publish source labels")
            .1;
        assert_eq!(
            label_stats.stats_sources,
            vec!["engine_stats_stream".to_string()]
        );
        assert_eq!(label_stats.output_tps, 0.0);

        let stats = aggregator
            .apply_update(stream_counter_update(
                "req-fast",
                0,
                10,
                true,
                start + Duration::from_millis(1),
            ))
            .pop()
            .expect("first real counter delta should publish stats");
        assert_eq!(stats.1.output_tps, 100.0);
        assert_eq!(stats.1.max_output_tps, 100.0);
    }

    #[test]
    fn engine_stream_sub_floor_deltas_accumulate_after_fast_first_sample() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(10),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-live", 0, 0, false, start));
        let first_stats = aggregator
            .apply_update(stream_counter_update(
                "req-live",
                0,
                1,
                false,
                start + Duration::from_millis(1),
            ))
            .pop()
            .expect("first fast counter delta should publish with the duration floor")
            .1;
        assert_eq!(first_stats.output_tps, 100.0);
        assert_eq!(first_stats.max_output_tps, 100.0);

        for tick in 2..10 {
            let updates = aggregator.apply_update(stream_counter_update(
                "req-live",
                0,
                tick,
                false,
                start + Duration::from_millis(tick),
            ));
            assert!(
                updates.is_empty(),
                "sub-floor deltas should accumulate without publishing noisy snapshots"
            );
        }

        let stats = aggregator
            .apply_update(stream_counter_update(
                "req-live",
                0,
                11,
                false,
                start + Duration::from_millis(11),
            ))
            .pop()
            .expect("accumulated sub-floor deltas should publish once the sample window is valid")
            .1;
        assert_eq!(stats.max_output_tps, 1_000.0);
        assert_eq!(stats.output_tps, 550.0);
    }

    #[test]
    fn engine_stream_missing_counter_fields_do_not_sample_stale_dimensions() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(10),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_partial_update(
            "req-partial",
            None,
            Some(0),
            false,
            start,
        ));
        let first_stats = aggregator
            .apply_update(stream_counter_partial_update(
                "req-partial",
                None,
                Some(1),
                false,
                start + Duration::from_millis(1),
            ))
            .pop()
            .expect("first output counter should publish with the duration floor")
            .1;
        assert_eq!(first_stats.output_tps, 100.0);

        assert!(
            aggregator
                .apply_update(stream_counter_partial_update(
                    "req-partial",
                    None,
                    Some(2),
                    false,
                    start + Duration::from_millis(2),
                ))
                .is_empty(),
            "second output counter is still below the duration floor"
        );

        let input_only_updates = aggregator.apply_update(stream_counter_partial_update(
            "req-partial",
            Some(1),
            None,
            false,
            start + Duration::from_millis(11),
        ));
        assert!(
            input_only_updates.is_empty(),
            "input-only updates must not publish a stale output TPS sample"
        );
    }

    #[test]
    fn engine_stream_sub_minimum_deltas_accumulate_until_publishable() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(10),
            min_output_tokens: 5,
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-min", 0, 0, false, start));
        for tick in 1..10 {
            let updates = aggregator.apply_update(stream_counter_update(
                "req-min",
                0,
                tick,
                false,
                start + Duration::from_millis(tick),
            ));
            assert!(
                updates.is_empty(),
                "tokens below the minimum or duration floor should remain accumulated"
            );
        }

        let stats = aggregator
            .apply_update(stream_counter_update(
                "req-min",
                0,
                10,
                false,
                start + Duration::from_millis(10),
            ))
            .pop()
            .expect("accumulated tokens should publish after reaching the floor")
            .1;
        assert_eq!(stats.output_tps, 1_000.0);
        assert_eq!(stats.max_output_tps, 1_000.0);
    }

    #[test]
    fn fallback_and_stream_cumulative_counters_share_stats_math() {
        let config = StatsCollectorConfig::default();
        let start = TokioInstant::now();
        let mut stream_aggregator = SharedStatsAggregator::new(config.clone());
        let mut fallback_aggregator = SharedStatsAggregator::new(config);

        for tick in 0..=5 {
            let observed_at = start + Duration::from_millis(tick * 100);
            let tokens_processed = tick * 10;
            let tokens_generated = tick * 2;
            let stream_updates = stream_aggregator.apply_update(stream_counter_update(
                "req-shared",
                tokens_processed,
                tokens_generated,
                tick == 5,
                observed_at,
            ));
            let fallback_updates = fallback_aggregator.apply_update(fallback_counter_update(
                "req-shared",
                tokens_processed,
                tokens_generated,
                tick == 5,
                observed_at,
            ));
            if tick == 0 {
                assert_eq!(stream_updates.len(), 1);
                assert!(fallback_updates.is_empty());
                continue;
            }
            assert_eq!(stream_updates.len(), fallback_updates.len());
            for ((_, stream_stats), (_, fallback_stats)) in
                stream_updates.iter().zip(fallback_updates.iter())
            {
                assert_eq!(
                    stream_stats.last_mean_input_tps,
                    fallback_stats.last_mean_input_tps
                );
                assert_eq!(stream_stats.output_tps, fallback_stats.output_tps);
                assert_eq!(stream_stats.max_output_tps, fallback_stats.max_output_tps);
            }
        }
    }

    #[test]
    fn dirty_fallback_counter_snapshots_preserve_lifecycle_load() {
        let config = StatsCollectorConfig::default();
        let start = TokioInstant::now();
        let mut aggregator = SharedStatsAggregator::new(config.clone());
        let mut per_model: HashMap<String, ModelMetricsState> = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation = active_chat_observation(
            "req-fallback-live-load",
            RequestObservationState::OutputGeneration,
        );

        record_lifecycle_observation(&config, &mut per_model, &mut in_flight, &observation);
        assert!(
            aggregator
                .apply_update(fallback_counter_update(
                    "req-fallback-live-load",
                    0,
                    2,
                    false,
                    start
                ))
                .is_empty(),
            "first fallback counter is a baseline"
        );

        let mut updates = aggregator.apply_update(fallback_counter_update(
            "req-fallback-live-load",
            0,
            4,
            false,
            start + Duration::from_millis(100),
        ));
        attach_lifecycle_load(&config, &mut per_model, &in_flight, &mut updates);
        let stats = updates
            .pop()
            .expect("second fallback counter should publish output TPS")
            .1;

        assert_eq!(stats.output_tps, 20.0);
        assert_eq!(stats.num_running_queries, 1);
        assert_eq!(stats.total_query_input_size, 32);
        assert_eq!(stats.input_processing_queries, 0);
        assert_eq!(stats.output_generation_queries, 1);
    }

    #[test]
    fn engine_stream_snapshots_preserve_local_kv_cache_stats() {
        let config = StatsCollectorConfig::default();
        let start = TokioInstant::now();
        let mut aggregator = SharedStatsAggregator::new(config.clone());
        let mut updates =
            aggregator.apply_update(stream_counter_update("req-stream-kv", 0, 10, true, start));
        let mut per_model: HashMap<String, ModelMetricsState> = HashMap::new();
        let model_state = per_model.entry("model-a".to_string()).or_default();
        model_state.kv_cache = KvCacheStatsSnapshot {
            model: "model-a".to_string(),
            kv_cache_capacity_tokens: 1_000,
            kv_cache_used_tokens: 400,
            kv_cache_free_tokens: 600,
        };
        model_state.kv_cache_stats_observed = true;
        let in_flight = HashMap::new();

        attach_lifecycle_load(&config, &mut per_model, &in_flight, &mut updates);
        let stats = updates
            .pop()
            .expect("stream counter should publish stats with local KV overlay")
            .1;

        assert_eq!(stats.kv_cache_capacity_tokens, 1_000);
        assert_eq!(stats.kv_cache_used_tokens, 400);
        assert_eq!(stats.kv_cache_free_tokens, 600);
        assert_eq!(
            stats.stats_capabilities,
            vec![
                "model.throughput.engine_stream".to_string(),
                "machine.kv_cache.http".to_string(),
            ]
        );
        assert_eq!(
            stats.stats_sources,
            vec![
                "engine_stats_stream".to_string(),
                "kv_cache_stats".to_string(),
            ]
        );
    }

    #[test]
    fn shared_aggregator_keeps_embeddings_observation_with_stream_output_stats() {
        let config = StatsCollectorConfig::default();
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-stream", 0, 0, false, start));
        let stats = aggregator
            .apply_update(stream_counter_update(
                "req-stream",
                0,
                10,
                true,
                start + Duration::from_secs(1),
            ))
            .pop()
            .expect("stream output counters should publish stats")
            .1;
        assert_eq!(stats.output_tps, 10.0);
        assert_eq!(stats.max_output_tps, 10.0);

        let mut latest = None;
        for index in 0..5 {
            let observation = RequestObservation {
                request_id: format!("req-embedding-{index}"),
                ..completed_embeddings_observation(
                    20,
                    2,
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                )
            };
            for update in stream_mode_observation_updates_from_observation(&observation) {
                latest = aggregator
                    .apply_update(update)
                    .pop()
                    .map(|(_, stats)| stats);
            }
        }

        let stats = latest.expect("fifth embeddings observation should publish stats");
        assert_eq!(stats.output_tps, 10.0);
        assert_eq!(stats.max_output_tps, 10.0);
        assert_eq!(stats.last_mean_input_tps, 0.0);
        assert_eq!(stats.embedding_item_tps, 2.0);
        assert_eq!(stats.max_embedding_item_tps, 2.0);
        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);
        assert!(
            !stats
                .stats_capabilities
                .contains(&"request.embeddings_item_throughput".to_string())
        );
    }

    #[test]
    fn stream_mode_embeddings_do_not_double_count_stream_input_tps() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(100),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        let mut latest = None;
        for index in 0..5 {
            latest = aggregator
                .apply_update(stream_counter_update(
                    &format!("req-stream-input-{index}"),
                    10,
                    0,
                    true,
                    start + Duration::from_secs(index + 1),
                ))
                .pop()
                .map(|(_, stats)| stats);
        }
        let stats = latest.expect("stream input counters should publish mean input stats");
        assert_eq!(stats.last_mean_input_tps, 100.0);

        let mut latest = None;
        for index in 0..5 {
            let observation = RequestObservation {
                request_id: format!("req-embedding-{index}"),
                ..completed_embeddings_observation(
                    20,
                    2,
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                )
            };
            for update in stream_mode_observation_updates_from_observation(&observation) {
                latest = aggregator
                    .apply_update(update)
                    .pop()
                    .map(|(_, stats)| stats);
            }
        }

        let stats = latest.expect("embeddings observations should publish item throughput");
        assert_eq!(stats.last_mean_input_tps, 100.0);
        assert_eq!(stats.embedding_item_tps, 2.0);
        assert_eq!(stats.max_embedding_item_tps, 2.0);
    }

    #[tokio::test]
    async fn stats_collector_enables_openai_fallback_only_after_control_update() {
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            openai_fallback_stats_enabled: false,
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        ));

        let fallback_observation = RequestObservation {
            request_id: "req-fallback-disabled".to_string(),
            output_tokens_explicit: true,
            output_tokens_from_chunk_usage: true,
            ..completed_observation(20, 1, 10, Duration::from_secs(1), Duration::from_secs(3))
        };
        observation_tx
            .send_async(fallback_observation)
            .await
            .expect("collector should receive fallback-disabled observation");
        let (_model_id, stats) =
            tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                .await
                .expect("fallback-disabled observation should publish lifecycle-only stats")
                .expect("collector should publish model stats");
        assert_eq!(stats.output_tps, 0.0);
        assert!(!stats.stats_sources.contains(&"chunk_usage".to_string()));

        stats_update_tx
            .send_async(StatsAggregatorUpdate::EnableOpenAiFallback)
            .await
            .expect("collector should receive fallback control update");
        for _ in 0..50 {
            let body = metrics.gather_text().expect("metrics should encode");
            if body.contains(
                r#"pylon_engine_stats_source_transitions_total{from="engine_stats_stream",reason="unsupported",to="openai_fallback"} 1"#,
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
        let body = metrics.gather_text().expect("metrics should encode");
        assert!(
            body.contains(
                r#"pylon_engine_stats_source_transitions_total{from="engine_stats_stream",reason="unsupported",to="openai_fallback"} 1"#
            ),
            "collector should process fallback control update before fallback observations are accepted"
        );
        observation_tx
            .send_async(RequestObservation {
                request_id: "req-fallback-enabled".to_string(),
                output_tokens_explicit: true,
                output_tokens_from_chunk_usage: true,
                ..completed_observation(20, 1, 10, Duration::from_secs(1), Duration::from_secs(3))
            })
            .await
            .expect("collector should receive fallback-enabled observation");

        let (_model_id, stats) =
            tokio::time::timeout(Duration::from_secs(2), model_stats_rx.recv_async())
                .await
                .expect("fallback-enabled observation should publish model stats")
                .expect("collector should publish model stats");
        assert_eq!(stats.output_tps, 5.0);
        assert!(stats.stats_sources.contains(&"chunk_usage".to_string()));

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn stats_collector_keeps_lifecycle_load_when_fallback_stats_disabled() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            openai_fallback_stats_enabled: false,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        ));

        let start = TokioInstant::now();
        stats_update_tx
            .send_async(stream_counter_update(
                "req-prior-stream",
                0,
                0,
                false,
                start,
            ))
            .await
            .expect("collector should receive stream start");
        stats_update_tx
            .send_async(stream_counter_update(
                "req-prior-stream",
                0,
                10,
                true,
                start + Duration::from_secs(1),
            ))
            .await
            .expect("collector should receive stream finish");
        let mut saw_stream_output = false;
        for _ in 0..10 {
            let (_model_id, stats) =
                tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                    .await
                    .expect("stream finish should publish stats")
                    .expect("collector should keep model stats channel open");
            if stats.output_tps == 10.0 {
                saw_stream_output = true;
                break;
            }
        }
        assert!(saw_stream_output);

        observation_tx
            .send_async(active_chat_observation(
                "req-stream-lifecycle",
                RequestObservationState::InputProcessing,
            ))
            .await
            .expect("collector should receive stream-mode lifecycle observation");

        let (model_id, stats) =
            tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                .await
                .expect("stream mode lifecycle observation should publish stats")
                .expect("collector should keep model stats channel open");
        assert_eq!(model_id, "model-a");
        assert_eq!(stats.num_running_queries, 1);
        assert_eq!(stats.queue_size, 1);
        assert_eq!(stats.queued_input_size, 24);
        assert_eq!(stats.total_query_input_size, 32);
        assert_eq!(stats.input_processing_queries, 1);
        assert_eq!(stats.output_generation_queries, 0);
        assert_eq!(stats.output_tps, 10.0);

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn stats_collector_accepts_late_stream_finish_after_terminal_observation() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            openai_fallback_stats_enabled: false,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        ));

        let start = TokioInstant::now();
        stats_update_tx
            .send_async(stream_counter_update("req-stream-race", 0, 0, false, start))
            .await
            .expect("collector should receive stream start");

        let mut terminal_observation =
            completed_observation(32, 1, 10, Duration::from_millis(50), Duration::from_secs(1));
        terminal_observation.request_id = "req-stream-race".to_string();
        observation_tx
            .send_async(terminal_observation)
            .await
            .expect("collector should receive terminal request observation");
        tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
            .await
            .expect("terminal observation should publish lifecycle stats")
            .expect("collector should keep model stats channel open");

        stats_update_tx
            .send_async(stream_counter_update(
                "req-stream-race",
                0,
                10,
                true,
                start + Duration::from_secs(1),
            ))
            .await
            .expect("collector should receive late stream finish");

        let mut saw_final_stream_stats = false;
        for _ in 0..10 {
            let (_model_id, stats) =
                tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                    .await
                    .expect("late stream finish should publish stats")
                    .expect("collector should keep model stats channel open");
            if stats.output_tps == 10.0 && stats.max_output_tps == 10.0 {
                saw_final_stream_stats = true;
                break;
            }
        }
        assert!(saw_final_stream_stats);

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn stats_collector_helper_defaults_stats_stream_to_authoritative() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = start_stats_collector_with_engine_stats(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        );

        let start = TokioInstant::now();
        stats_update_tx
            .send_async(stream_counter_update(
                "req-helper-stream",
                0,
                0,
                false,
                start,
            ))
            .await
            .expect("collector should receive stream start");

        let mut terminal_observation =
            completed_observation(32, 0, 0, Duration::from_millis(50), Duration::from_secs(1));
        terminal_observation.request_id = "req-helper-stream".to_string();
        observation_tx
            .send_async(terminal_observation)
            .await
            .expect("collector should receive terminal request observation");

        stats_update_tx
            .send_async(stream_counter_update(
                "req-helper-stream",
                0,
                10,
                true,
                start + Duration::from_secs(1),
            ))
            .await
            .expect("collector should receive delayed stream finish");

        let mut saw_final_stream_stats = false;
        for _ in 0..10 {
            let (_model_id, stats) =
                tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                    .await
                    .expect("delayed stream finish should publish stats")
                    .expect("collector should keep model stats channel open");
            if stats.output_tps == 10.0 && stats.max_output_tps == 10.0 {
                saw_final_stream_stats = true;
                break;
            }
        }
        assert!(saw_final_stream_stats);

        stop_tx.send(true).expect("collector should receive stop");
        collector.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stats_collector_sweeps_stream_state_after_stats_receiver_closes() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            engine_stats_request_ttl: Duration::from_secs(1),
            engine_stats_model_ttl: Duration::from_secs(60),
            engine_stats_sweep_interval: Duration::from_secs(1),
            openai_fallback_stats_enabled: false,
            ..Default::default()
        };
        let (_observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        ));

        let start = TokioInstant::now();
        stats_update_tx
            .send_async(stream_counter_update(
                "req-stream-stale",
                0,
                0,
                false,
                start,
            ))
            .await
            .expect("collector should receive stream start");
        drop(stats_update_tx);

        let (_model_id, label_stats) = model_stats_rx
            .recv_async()
            .await
            .expect("initial stream label snapshot should publish");
        assert_eq!(
            label_stats.stats_sources,
            vec!["engine_stats_stream".to_string()]
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        let mut stale_snapshot = None;
        for _ in 0..50 {
            if let Ok((model_id, stats)) = model_stats_rx.try_recv()
                && model_id == "model-a"
            {
                stale_snapshot = Some(stats);
                break;
            }
            tokio::task::yield_now().await;
        }
        let stats = stale_snapshot.expect("stale stream request should be swept after close");
        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);
        assert_eq!(stats.num_running_queries, 0);

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn fallback_counter_snapshots_preserve_lifecycle_load() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            None,
            model_stats_tx,
            stop_rx,
        ));

        observation_tx
            .send_async(active_chat_observation(
                "req-fallback-live-load",
                RequestObservationState::OutputGeneration,
            ))
            .await
            .expect("collector should receive fallback observation");

        let mut snapshots = Vec::new();
        let (model_id, stats) =
            tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                .await
                .expect("fallback observation should publish stats")
                .expect("collector should keep model stats channel open");
        assert_eq!(model_id, "model-a");
        snapshots.push(stats);

        for _ in 0..20 {
            while let Ok((model_id, stats)) = model_stats_rx.try_recv() {
                if model_id == "model-a" {
                    snapshots.push(stats);
                }
            }
            tokio::task::yield_now().await;
        }

        for stats in snapshots {
            assert_eq!(stats.num_running_queries, 1);
            assert_eq!(stats.total_query_input_size, 32);
            assert_eq!(stats.input_processing_queries, 0);
            assert_eq!(stats.output_generation_queries, 1);
        }

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn terminal_only_fallback_counter_does_not_clear_observed_output_tps() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            None,
            model_stats_tx,
            stop_rx,
        ));

        observation_tx
            .send_async(RequestObservation {
                request_id: "req-terminal-only-fallback".to_string(),
                output_tokens_explicit: true,
                output_tokens_from_chunk_usage: true,
                ..completed_observation(20, 1, 10, Duration::from_secs(1), Duration::from_secs(3))
            })
            .await
            .expect("collector should receive terminal-only fallback observation");

        let mut output_tps_values = Vec::new();
        let (model_id, stats) =
            tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                .await
                .expect("terminal observation should publish stats")
                .expect("collector should keep model stats channel open");
        assert_eq!(model_id, "model-a");
        output_tps_values.push(stats.output_tps);

        for _ in 0..20 {
            while let Ok((model_id, stats)) = model_stats_rx.try_recv() {
                if model_id == "model-a" {
                    output_tps_values.push(stats.output_tps);
                }
            }
            tokio::task::yield_now().await;
        }

        assert!(!output_tps_values.is_empty());
        assert!(
            output_tps_values
                .iter()
                .all(|output_tps| *output_tps == 5.0),
            "terminal-only fallback stats must not publish a later zero output TPS snapshot: {output_tps_values:?}"
        );

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn stats_collector_keeps_embeddings_observation_when_fallback_stats_disabled() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 32,
            openai_fallback_stats_enabled: false,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (stats_update_tx, stats_update_rx) = stats_aggregator_update_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(32);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            Some(stats_update_rx),
            model_stats_tx,
            stop_rx,
        ));

        let start = TokioInstant::now();
        stats_update_tx
            .send_async(stream_counter_update("req-stream", 0, 0, false, start))
            .await
            .expect("collector should receive stream start");
        stats_update_tx
            .send_async(stream_counter_update(
                "req-stream",
                0,
                10,
                true,
                start + Duration::from_secs(1),
            ))
            .await
            .expect("collector should receive stream finish");

        let mut saw_stream_output = false;
        for _ in 0..10 {
            let (_model_id, stats) =
                tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                    .await
                    .expect("collector should publish stream stats")
                    .expect("collector should keep model stats channel open");
            if stats.output_tps == 10.0 && stats.max_output_tps == 10.0 {
                saw_stream_output = true;
                break;
            }
        }
        assert!(saw_stream_output);

        for index in 0..5 {
            observation_tx
                .send_async(RequestObservation {
                    request_id: format!("req-embedding-{index}"),
                    ..completed_embeddings_observation(
                        20,
                        2,
                        Duration::from_secs(1),
                        Duration::from_secs(2),
                    )
                })
                .await
                .expect("collector should receive embeddings observation");
        }

        let mut latest = None;
        for _ in 0..20 {
            let (_model_id, stats) =
                tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                    .await
                    .expect("collector should publish embeddings stats")
                    .expect("collector should keep model stats channel open");
            if stats.embedding_item_tps > 0.0 {
                latest = Some(stats);
                break;
            }
        }

        let stats = latest.expect("embeddings observations should publish stream-mode stats");
        assert_eq!(stats.output_tps, 10.0);
        assert_eq!(stats.max_output_tps, 10.0);
        assert_eq!(stats.last_mean_input_tps, 0.0);
        assert_eq!(stats.embedding_item_tps, 2.0);
        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[test]
    fn shared_aggregator_ignores_regressions_and_post_finalize_events() {
        let config = StatsCollectorConfig::default();
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-final", 10, 2, false, start));
        aggregator.apply_update(stream_counter_update(
            "req-final",
            20,
            4,
            true,
            start + Duration::from_millis(100),
        ));
        assert_eq!(aggregator.live_request_count(), 0);

        let updates = aggregator.apply_update(stream_counter_update(
            "req-final",
            30,
            8,
            false,
            start + Duration::from_millis(200),
        ));
        assert!(updates.is_empty());

        aggregator.apply_update(stream_counter_update("req-live", 20, 4, false, start));
        let updates = aggregator.apply_update(stream_counter_update(
            "req-live",
            19,
            5,
            false,
            start + Duration::from_millis(100),
        ));
        assert!(updates.is_empty());
    }

    #[test]
    fn fallback_terminal_observation_without_trusted_counters_finalizes_stream_request() {
        let mut observation = completed_observation(
            11,
            12,
            10,
            Duration::from_millis(100),
            Duration::from_millis(1_000),
        );
        observation.request_id = "req-stream-race".to_string();

        let config = StatsCollectorConfig::default();
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();
        aggregator.apply_update(stream_counter_update("req-stream-race", 5, 3, false, start));
        assert_eq!(aggregator.live_request_count(), 1);

        let fallback_updates = fallback_updates_from_observation(&observation);
        assert_eq!(fallback_updates.len(), 1);
        let stats = aggregator
            .apply_update(fallback_updates.into_iter().next().unwrap())
            .pop()
            .expect("terminal request observation should publish the finalized stream snapshot")
            .1;

        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);
        assert_eq!(aggregator.live_request_count(), 0);

        let updates = aggregator.apply_update(stream_counter_update(
            "req-stream-race",
            11,
            10,
            true,
            start + Duration::from_millis(100),
        ));
        assert!(
            updates.is_empty(),
            "post-finalization stream stats must not double-count"
        );
    }

    #[test]
    fn shared_aggregator_sweeps_stale_request_and_model_state() {
        let config = StatsCollectorConfig {
            engine_stats_request_ttl: Duration::from_secs(1),
            engine_stats_model_ttl: Duration::from_secs(1),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        for tick in 0..=5 {
            aggregator.apply_update(stream_counter_update(
                "req-stale",
                tick * 10,
                tick * 2,
                false,
                start + Duration::from_millis(tick * 100),
            ));
        }
        assert_eq!(aggregator.live_request_count(), 1);

        let updates = aggregator.sweep_stale(start + Duration::from_secs(2));
        assert_eq!(aggregator.live_request_count(), 0);
        let stats = updates
            .into_iter()
            .find(|(model_id, _)| model_id == "model-a")
            .expect("stale cleanup should publish a dirty model snapshot")
            .1;

        assert_eq!(stats.last_mean_input_tps, 100.0);
        assert_eq!(stats.output_tps, 0.0);
        assert_eq!(stats.queue_size, 0);
        assert_eq!(stats.queued_input_size, 0);
        assert_eq!(stats.num_running_queries, 0);
        assert_eq!(stats.input_processing_queries, 0);
        assert_eq!(stats.output_generation_queries, 0);
        assert_eq!(stats.stats_sources, vec!["engine_stats_stream".to_string()]);
    }

    #[test]
    fn shared_aggregator_tombstones_stale_request_before_late_finish() {
        let config = StatsCollectorConfig {
            engine_stats_request_ttl: Duration::from_secs(1),
            engine_stats_model_ttl: Duration::from_secs(60),
            ..Default::default()
        };
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();

        aggregator.apply_update(stream_counter_update("req-stale-late", 0, 0, false, start));
        aggregator.apply_update(stream_counter_update(
            "req-stale-late",
            100,
            10,
            false,
            start + Duration::from_millis(100),
        ));
        assert_eq!(aggregator.live_request_count(), 1);

        let stale_updates = aggregator.sweep_stale(start + Duration::from_secs(2));
        assert_eq!(aggregator.live_request_count(), 0);
        assert!(
            stale_updates
                .iter()
                .any(|(model_id, _)| model_id == "model-a"),
            "stale cleanup should publish a dirty model snapshot"
        );

        let late_updates = aggregator.apply_update(stream_counter_update(
            "req-stale-late",
            100,
            20,
            true,
            start + Duration::from_millis(2_100),
        ));
        assert!(
            late_updates.is_empty(),
            "late cumulative finish after stale cleanup must not be replayed from zero"
        );
    }

    #[test]
    fn shared_aggregator_keeps_bounded_request_state_for_many_cumulative_updates() {
        const REQUESTS: usize = 256;
        const EVENTS: usize = 10_000;

        let config = StatsCollectorConfig::default();
        let mut aggregator = SharedStatsAggregator::new(config);
        let start = TokioInstant::now();
        let mut latest = vec![(0u64, 0u64); REQUESTS];

        for index in 0..EVENTS {
            let request_index = index % REQUESTS;
            let step = (index / REQUESTS + 1) as u64;
            let tokens_processed = step * 8;
            let tokens_generated = step;
            latest[request_index] = (tokens_processed, tokens_generated);
            aggregator.apply_update(stream_counter_update(
                &format!("req-{request_index}"),
                tokens_processed,
                tokens_generated,
                false,
                start + Duration::from_millis(index as u64),
            ));
        }

        assert_eq!(aggregator.live_request_count(), REQUESTS);

        for (request_index, (tokens_processed, tokens_generated)) in latest.into_iter().enumerate()
        {
            aggregator.apply_update(stream_counter_update(
                &format!("req-{request_index}"),
                tokens_processed,
                tokens_generated,
                true,
                start + Duration::from_secs(60) + Duration::from_millis(request_index as u64),
            ));
        }

        assert_eq!(aggregator.live_request_count(), 0);
    }

    #[test]
    fn last_mean_input_tps_stays_sticky_without_new_samples() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        per_model
            .entry("model-a".to_string())
            .or_insert_with(ModelMetricsState::default)
            .last_mean_input_tps = 10.0;
        let in_flight = HashMap::new();

        let stats = snapshot_model_stats(&config, &mut per_model, &in_flight, "model-a");
        assert_eq!(stats.last_mean_input_tps, 10.0);

        let stats = snapshot_model_stats(&config, &mut per_model, &in_flight, "model-a");
        assert_eq!(stats.last_mean_input_tps, 10.0);
    }

    #[test]
    fn incremental_token_progress_feeds_tps_distribution_from_rolling_rate() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-rolling-input".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);

        for tick in 1..=5 {
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: tick * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..input_processing.clone()
            });
            let updates = tick_mean_input_aggregator(&mut aggregator);
            if tick < 5 {
                assert!(updates.is_empty());
            } else {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].last_mean_input_tps, 100.0);
            }
        }

        let updates = aggregator.record_request_observation(&RequestObservation {
            state: RequestObservationState::Complete,
            output_messages: 1,
            output_tokens: 8,
            time_to_first_output: Some(Duration::from_millis(500)),
            time_to_first_token: Some(Duration::from_millis(600)),
            total_duration: Duration::from_millis(900),
            ..input_processing
        });
        assert!(updates.is_empty());

        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 100.0);
    }

    #[test]
    fn terminal_input_progress_sample_survives_decode_without_more_input_progress() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-input-progress-long-decode".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 20,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(20),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);
        let mut update = None;
        for tick in 1..=5 {
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: tick * 4,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..input_processing.clone()
            });
            let updates = tick_mean_input_aggregator(&mut aggregator);
            if let Some(last_update) = updates.into_iter().last() {
                update = Some(last_update);
            }
        }

        let complete = RequestObservation {
            state: RequestObservationState::Complete,
            output_messages: 1,
            output_tokens: 32,
            time_to_first_output: Some(Duration::from_millis(500)),
            time_to_first_token: Some(Duration::from_millis(600)),
            total_duration: Duration::from_millis(1_100),
            ..input_processing
        };
        let updates = aggregator.record_request_observation(&complete);

        assert!(updates.is_empty());
        assert_eq!(update.unwrap().last_mean_input_tps, 40.0);
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 40.0);
    }

    #[test]
    fn terminal_input_progress_commits_sparse_progress_sample() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-input-progress-sparse-prefill".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 20,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(20),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);

        for _ in 0..9 {
            assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
        }
        let sparse_progress = RequestObservation {
            input_tokens_processed: 20,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_secs(1)),
            total_duration: Duration::from_secs(1),
            ..input_processing
        };
        aggregator.record_request_observation(&sparse_progress);
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());

        let complete = RequestObservation {
            state: RequestObservationState::Complete,
            output_messages: 1,
            output_tokens: 8,
            time_to_first_output: Some(Duration::from_secs(1)),
            time_to_first_token: Some(Duration::from_millis(1_100)),
            total_duration: Duration::from_secs(2),
            ..sparse_progress
        };
        let updates = aggregator.record_request_observation(&complete);

        assert!(updates.is_empty());
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 1);
        assert_eq!(distribution.mean, 20.0);
    }

    #[test]
    fn decode_progress_first_delta_preserves_accumulated_prefill_elapsed() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-decode-first-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 20,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(20),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);

        for _ in 0..9 {
            assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
        }
        aggregator.record_request_observation(&RequestObservation {
            input_tokens_processed: 20,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            state: RequestObservationState::OutputGeneration,
            time_to_input_tokens_processed: Some(Duration::from_secs(1)),
            time_to_first_output: Some(Duration::from_secs(1)),
            time_to_first_token: Some(Duration::from_millis(1_010)),
            total_duration: Duration::from_secs(1),
            ..input_processing
        });
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());

        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 1);
        assert_eq!(distribution.mean, 20.0);
    }

    #[test]
    fn failed_input_progress_still_feeds_observed_windowed_work() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-failed-input-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);

        let mut update = None;
        for tick in 1..=5 {
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: tick * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..input_processing.clone()
            });
            let updates = tick_mean_input_aggregator(&mut aggregator);
            if let Some(last_update) = updates.into_iter().last() {
                update = Some(last_update);
            }
        }

        let updates = aggregator.record_request_observation(&RequestObservation {
            state: RequestObservationState::Failed,
            upstream_status: Some(500),
            total_duration: Duration::from_millis(700),
            ..input_processing
        });

        assert!(updates.is_empty());
        assert_eq!(update.unwrap().last_mean_input_tps, 100.0);
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 100.0);
    }

    #[test]
    fn concurrent_progress_sums_observed_work_even_when_one_request_fails() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let request_a = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-success".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        let request_b = RequestObservation {
            request_id: "req-failed".to_string(),
            ..request_a.clone()
        };
        aggregator.record_request_observation(&request_a);
        aggregator.record_request_observation(&request_b);
        aggregator.record_request_observation(&RequestObservation {
            input_tokens_processed: 10,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_millis(100)),
            total_duration: Duration::from_millis(100),
            ..request_a.clone()
        });
        aggregator.record_request_observation(&RequestObservation {
            input_tokens_processed: 10,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_millis(100)),
            total_duration: Duration::from_millis(100),
            ..request_b.clone()
        });
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());

        assert!(
            aggregator
                .record_request_observation(&RequestObservation {
                    state: RequestObservationState::Complete,
                    input_tokens_processed: 10,
                    input_tokens_processed_from_inference_progress: true,
                    has_inference_progress_stats: true,
                    time_to_input_tokens_processed: Some(Duration::from_millis(100)),
                    output_messages: 1,
                    output_tokens: 8,
                    time_to_first_output: Some(Duration::from_millis(200)),
                    time_to_first_token: Some(Duration::from_millis(300)),
                    total_duration: Duration::from_millis(400),
                    ..request_a
                })
                .is_empty()
        );
        assert!(
            aggregator
                .record_request_observation(&RequestObservation {
                    state: RequestObservationState::Failed,
                    upstream_status: Some(500),
                    input_tokens_processed: 10,
                    input_tokens_processed_from_inference_progress: true,
                    has_inference_progress_stats: true,
                    time_to_input_tokens_processed: Some(Duration::from_millis(100)),
                    total_duration: Duration::from_millis(400),
                    ..request_b
                })
                .is_empty()
        );

        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 1);
        assert_eq!(distribution.mean, 200.0);
    }

    #[test]
    fn mean_input_tps_aggregator_sums_concurrent_progress_streams() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let request_a = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-a".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        let request_b = RequestObservation {
            request_id: "req-b".to_string(),
            ..request_a.clone()
        };
        aggregator.record_request_observation(&request_a);
        aggregator.record_request_observation(&request_b);

        for tick in 1..=5 {
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: tick * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..request_a.clone()
            });
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: tick * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..request_b.clone()
            });
            let updates = tick_mean_input_aggregator(&mut aggregator);
            if tick < 5 {
                assert!(updates.is_empty());
            } else {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].last_mean_input_tps, 200.0);
            }
        }

        let request_a_complete = RequestObservation {
            state: RequestObservationState::Complete,
            input_tokens_processed: 50,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_millis(500)),
            output_messages: 1,
            output_tokens: 8,
            time_to_first_output: Some(Duration::from_millis(600)),
            time_to_first_token: Some(Duration::from_millis(700)),
            total_duration: Duration::from_millis(900),
            ..request_a
        };
        assert!(
            aggregator
                .record_request_observation(&request_a_complete)
                .is_empty()
        );
        let request_b_complete = RequestObservation {
            request_id: "req-b".to_string(),
            ..request_a_complete
        };
        let updates = aggregator.record_request_observation(&request_b_complete);
        assert!(updates.is_empty());
    }

    #[test]
    fn mean_input_tps_aggregator_counts_zero_ticks_during_sparse_progress() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-sparse-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&input_processing);

        for sample_index in 1..=5 {
            for _ in 0..9 {
                assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
            }
            aggregator.record_request_observation(&RequestObservation {
                input_tokens_processed: sample_index * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_secs(sample_index)),
                total_duration: Duration::from_secs(sample_index),
                ..input_processing.clone()
            });
            let updates = tick_mean_input_aggregator(&mut aggregator);
            if sample_index < 5 {
                assert!(updates.is_empty());
            } else {
                assert_eq!(updates.len(), 1);
                assert_eq!(updates[0].last_mean_input_tps, 10.0);
            }
        }

        let updates = aggregator.record_request_observation(&RequestObservation {
            state: RequestObservationState::Complete,
            input_tokens_processed: 50,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_secs(5)),
            output_messages: 1,
            output_tokens: 8,
            time_to_first_output: Some(Duration::from_millis(5_100)),
            time_to_first_token: Some(Duration::from_millis(5_200)),
            total_duration: Duration::from_secs(6),
            ..input_processing
        });
        assert!(updates.is_empty());
    }

    #[test]
    fn no_progress_request_does_not_poison_next_progress_sample_elapsed_time() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let no_progress = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-no-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&no_progress);
        for _ in 0..10 {
            assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
        }
        assert!(
            aggregator
                .record_request_observation(&RequestObservation {
                    state: RequestObservationState::Failed,
                    upstream_status: Some(500),
                    total_duration: Duration::from_secs(2),
                    ..no_progress
                })
                .is_empty()
        );

        let with_progress = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-with-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        aggregator.record_request_observation(&with_progress);
        aggregator.record_request_observation(&RequestObservation {
            input_tokens_processed: 10,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_millis(100)),
            total_duration: Duration::from_millis(100),
            ..with_progress.clone()
        });
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
        assert!(
            aggregator
                .record_request_observation(&RequestObservation {
                    state: RequestObservationState::Complete,
                    input_tokens_processed: 10,
                    input_tokens_processed_from_inference_progress: true,
                    has_inference_progress_stats: true,
                    time_to_input_tokens_processed: Some(Duration::from_millis(100)),
                    output_messages: 1,
                    output_tokens: 1,
                    time_to_first_output: Some(Duration::from_millis(200)),
                    time_to_first_token: Some(Duration::from_millis(300)),
                    total_duration: Duration::from_millis(400),
                    ..with_progress
                })
                .is_empty()
        );

        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 1);
        assert_eq!(distribution.mean, 100.0);
    }

    #[tokio::test(start_paused = true)]
    async fn mean_input_tps_aggregator_uses_actual_elapsed_time_for_missed_ticks() {
        let config = StatsCollectorConfig::default();
        let (observation_tx, observation_rx) = flume::unbounded();
        let (update_tx, update_rx) = flume::unbounded();
        let task = tokio::spawn(run_mean_input_tps_aggregator(
            MeanInputTpsAggregatorConfig::from(&config),
            observation_rx,
            update_tx,
        ));
        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-delayed-aggregator-ticks".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };

        observation_tx
            .send(mean_input_observation(&input_processing))
            .expect("aggregator should receive input-processing observation");
        for sample_index in 1..=5 {
            let progress = RequestObservation {
                input_tokens_processed: sample_index * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_secs(sample_index)),
                total_duration: Duration::from_secs(sample_index),
                ..input_processing.clone()
            };
            observation_tx
                .send(mean_input_observation(&progress))
                .expect("aggregator should receive progress observation");
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        let update = receive_mean_input_update(&update_rx).await;

        let complete = RequestObservation {
            state: RequestObservationState::Complete,
            input_tokens_processed: 50,
            input_tokens_processed_from_inference_progress: true,
            has_inference_progress_stats: true,
            time_to_input_tokens_processed: Some(Duration::from_secs(5)),
            output_messages: 1,
            output_tokens: 8,
            time_to_first_output: Some(Duration::from_millis(5_100)),
            time_to_first_token: Some(Duration::from_millis(5_200)),
            total_duration: Duration::from_secs(6),
            ..input_processing
        };
        observation_tx
            .send(mean_input_observation(&complete))
            .expect("aggregator should receive terminal observation");

        assert_eq!(update.model_id, "model-a");
        assert_eq!(update.last_mean_input_tps, 10.0);

        task.abort();
        let _ = task.await;
    }

    #[test]
    fn terminal_only_samples_use_request_duration_instead_of_tick_window() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let mut update = None;

        for request_index in 0..5 {
            let updates = aggregator.record_request_observation(&RequestObservation {
                request_id: format!("req-final-only-{request_index}"),
                ..completed_observation(100, 1, 1, Duration::from_secs(2), Duration::from_secs(2))
            });
            if request_index < 4 {
                assert!(updates.is_empty());
            } else {
                update = updates.into_iter().next();
            }
        }
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());

        let update = update.expect("fifth direct sample should publish the sticky mean");
        assert_eq!(update.model_id, "model-a");
        assert_eq!(update.last_mean_input_tps, 50.0);
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 50.0);
    }

    #[test]
    fn terminal_only_samples_do_not_sum_same_tick_request_rates() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let mut update = None;

        for request_index in 0..5 {
            let updates = aggregator.record_request_observation(&RequestObservation {
                request_id: format!("req-final-only-sequential-{request_index}"),
                ..completed_observation(
                    100,
                    1,
                    1,
                    Duration::from_millis(10),
                    Duration::from_millis(10),
                )
            });
            if request_index < 4 {
                assert!(updates.is_empty());
            } else {
                update = updates.into_iter().next();
            }
        }
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());

        let update = update.expect("fifth direct sample should publish the sticky mean");
        assert_eq!(update.last_mean_input_tps, 10_000.0);
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 10_000.0);
    }

    #[test]
    fn completed_request_stats_keep_exact_output_rate_formula() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation =
            completed_observation(120, 6, 30, Duration::from_secs(3), Duration::from_secs(9));

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation)
            .into_iter()
            .find(|(model_id, _)| model_id == "model-a")
            .unwrap()
            .1;

        assert_eq!(stats.last_mean_input_tps, 0.0);
        assert_eq!(stats.output_tps, 5.0);
        assert_eq!(stats.max_output_tps, 5.0);
    }

    #[test]
    fn ignores_observations_below_duration_floor() {
        let config = StatsCollectorConfig {
            duration_floor: Duration::from_millis(50),
            ..Default::default()
        };
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation = completed_observation(
            20,
            4,
            8,
            Duration::from_millis(10),
            Duration::from_millis(20),
        );

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.output_tps, 0.0);
    }

    #[test]
    fn terminal_usage_chunks_use_first_output_for_output_tps() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation = RequestObservation {
            time_to_first_token: Some(Duration::from_millis(5_995)),
            ..completed_observation(20, 4, 8, Duration::from_secs(2), Duration::from_secs(6))
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1.output_tps, 2.0);
        assert_eq!(stats[0].1.max_output_tps, 2.0);
    }

    #[test]
    fn embeddings_stats_update_last_mean_input_tps_without_claiming_output_tps() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation =
            completed_embeddings_observation(20, 4, Duration::from_secs(2), Duration::from_secs(4));

        for _ in 0..4 {
            assert!(
                aggregator
                    .record_request_observation(&observation)
                    .is_empty()
            );
            assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
            let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);
            assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        }
        let updates = aggregator.record_request_observation(&observation);
        assert!(tick_mean_input_aggregator(&mut aggregator).is_empty());
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(updates[0].last_mean_input_tps, 10.0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.output_tps, 0.0);
        assert_eq!(stats[0].1.max_output_tps, 0.0);
        assert_eq!(stats[0].1.embedding_item_tps, 2.0);
        assert_eq!(stats[0].1.max_embedding_item_tps, 2.0);
        assert!(stats[0].1.stats_capabilities.is_empty());
        assert!(stats[0].1.stats_sources.is_empty());

        let live_chat = RequestObservation {
            request_id: "req-live-chat".to_string(),
            state: RequestObservationState::OutputGeneration,
            output_tokens: 20,
            time_to_first_output: Some(Duration::from_secs(1)),
            time_to_first_token: Some(Duration::from_secs(1)),
            total_duration: Duration::from_secs(3),
            ..completed_observation(10, 1, 20, Duration::from_secs(1), Duration::from_secs(3))
        };
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &live_chat);

        assert_eq!(stats[0].1.output_tps, 10.0);
    }

    #[test]
    fn fast_embeddings_input_samples_clamp_to_duration_floor() {
        let config = StatsCollectorConfig::default();
        let mut aggregator =
            MeanInputTpsAggregator::new(MeanInputTpsAggregatorConfig::from(&config));
        let observation = completed_embeddings_observation(
            20,
            4,
            Duration::from_millis(1),
            Duration::from_millis(4),
        );
        let mut update = None;

        for sample_index in 0..5 {
            let updates = aggregator.record_request_observation(&RequestObservation {
                request_id: format!("req-fast-embedding-{sample_index}"),
                ..observation.clone()
            });
            if sample_index < 4 {
                assert!(updates.is_empty());
            } else {
                update = updates.into_iter().next();
            }
        }

        let update = update.expect("fifth embeddings input sample should publish mean input TPS");
        assert_eq!(update.last_mean_input_tps, 2000.0);
        let distribution = &aggregator.per_model["model-a"].distribution;
        assert_eq!(distribution.count, 5);
        assert_eq!(distribution.mean, 2000.0);
    }

    #[test]
    fn embeddings_item_tps_clamps_fast_response_relay_duration() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation = completed_embeddings_observation(
            20,
            2,
            Duration::from_millis(2),
            Duration::from_millis(5),
        );

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1.output_tps, 0.0);
        assert_eq!(stats[0].1.max_output_tps, 0.0);
        assert_eq!(stats[0].1.embedding_item_tps, 200.0);
        assert_eq!(stats[0].1.max_embedding_item_tps, 200.0);
    }

    #[test]
    fn embeddings_stats_do_not_replace_chat_output_tps() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let chat = completed_observation(20, 1, 10, Duration::from_secs(1), Duration::from_secs(3));
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &chat);
        assert_eq!(stats[0].1.output_tps, 5.0);

        let embeddings =
            completed_embeddings_observation(20, 2, Duration::from_secs(1), Duration::from_secs(2));
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &embeddings);

        assert_eq!(stats[0].1.output_tps, 5.0);
        assert_eq!(stats[0].1.max_output_tps, 5.0);
        assert_eq!(stats[0].1.embedding_item_tps, 2.0);
        assert_eq!(stats[0].1.max_embedding_item_tps, 2.0);
        assert!(stats[0].1.stats_capabilities.is_empty());
        assert!(stats[0].1.stats_sources.is_empty());
    }

    #[test]
    fn embeddings_observations_do_not_add_output_throughput_labels() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let chat = completed_observation(20, 1, 10, Duration::from_secs(1), Duration::from_secs(3));
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &chat);
        assert_eq!(stats[0].1.output_tps, 5.0);

        let failed_embeddings = RequestObservation {
            state: RequestObservationState::Failed,
            ..completed_embeddings_observation(
                20,
                2,
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
        };
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &failed_embeddings);

        assert_eq!(
            stats[0].1.output_tps, 5.0,
            "failed embeddings requests must not replace the last completed output sample"
        );
        assert_eq!(stats[0].1.embedding_item_tps, 0.0);
        assert_eq!(stats[0].1.max_embedding_item_tps, 0.0);
        assert!(stats[0].1.stats_capabilities.is_empty());
        assert!(stats[0].1.stats_sources.is_empty());

        let live_embeddings = RequestObservation {
            state: RequestObservationState::UpstreamConnecting,
            total_duration: Duration::ZERO,
            ..completed_embeddings_observation(
                20,
                2,
                Duration::from_secs(1),
                Duration::from_secs(2),
            )
        };
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &live_embeddings);

        assert_eq!(stats[0].1.output_tps, 5.0);
        assert_eq!(stats[0].1.embedding_item_tps, 0.0);
        assert!(stats[0].1.stats_capabilities.is_empty());
        assert!(stats[0].1.stats_sources.is_empty());
    }

    #[test]
    fn ignores_non_complete_observations() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation = RequestObservation {
            state: RequestObservationState::Failed,
            ..completed_observation(20, 4, 8, Duration::from_secs(2), Duration::from_secs(6))
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.output_tps, 0.0);
    }

    #[test]
    fn publishes_live_queue_and_active_stats() {
        let config = StatsCollectorConfig::default();
        config
            .queue_tracker
            .update_model_throughput("model-a", 100.0);
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let queued = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-live".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 2,
            input_tokens: 24,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(5)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(5),
        };
        let queued_stats = record_observation(&config, &mut per_model, &mut in_flight, &queued);
        assert_eq!(queued_stats[0].1.queue_size, 1);
        assert_eq!(queued_stats[0].1.queued_input_size, 24);
        assert_eq!(
            queued_stats[0].1.queue_time_estimate_ms_by_priority,
            Some(HashMap::from([(0, 240)]))
        );
        assert_eq!(queued_stats[0].1.num_running_queries, 1);
        assert_eq!(queued_stats[0].1.total_query_input_size, 24);
        assert_eq!(queued_stats[0].1.input_processing_queries, 1);
        assert_eq!(queued_stats[0].1.output_generation_queries, 0);
        assert_eq!(queued_stats[0].1.last_mean_input_tps, 0.0);

        let generating = RequestObservation {
            output_messages: 2,
            output_tokens: 8,
            state: RequestObservationState::OutputGeneration,
            time_to_first_output: Some(Duration::from_secs(2)),
            time_to_first_token: Some(Duration::from_secs(2)),
            total_duration: Duration::from_secs(3),
            ..queued
        };
        let active_stats = record_observation(&config, &mut per_model, &mut in_flight, &generating);
        assert_eq!(active_stats[0].1.queue_size, 0);
        assert_eq!(active_stats[0].1.queued_input_size, 0);
        assert_eq!(active_stats[0].1.num_running_queries, 1);
        assert_eq!(active_stats[0].1.total_query_input_size, 24);
        assert_eq!(active_stats[0].1.input_processing_queries, 0);
        assert_eq!(active_stats[0].1.output_generation_queries, 1);
        assert_eq!(active_stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(active_stats[0].1.output_tps, 8.0);
    }

    #[test]
    fn live_stats_math_is_exact_for_simultaneous_queued_and_generating_requests() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let queued = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-queued".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 30,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(5)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(5),
        };
        let generating = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-generating".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 20,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 3,
            output_tokens: 6,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::OutputGeneration,
            time_to_response_headers: Some(Duration::from_millis(5)),
            time_to_input_tokens_processed: None,
            time_to_first_output: Some(Duration::from_secs(2)),
            time_to_first_token: Some(Duration::from_secs(2)),
            total_duration: Duration::from_secs(5),
        };

        record_observation(&config, &mut per_model, &mut in_flight, &queued);
        let stats = record_observation(&config, &mut per_model, &mut in_flight, &generating)
            .into_iter()
            .find(|(model_id, _)| model_id == "model-a")
            .unwrap()
            .1;

        assert_eq!(stats.queue_size, 1);
        assert_eq!(stats.queued_input_size, 30);
        assert_eq!(stats.num_running_queries, 2);
        assert_eq!(stats.total_query_input_size, 50);
        assert_eq!(stats.last_mean_input_tps, 0.0);
        assert_eq!(stats.output_tps, 2.0);
    }

    #[test]
    fn live_input_progress_derives_tps_from_processed_tokens() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let observation = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-input-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 40,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(100),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: true,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_secs(2)),
            time_to_input_tokens_processed: Some(Duration::from_secs(2)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_secs(30),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.queued_input_size, 60);
        assert_eq!(
            stats[0].1.stats_capabilities,
            vec!["request.final_headers".to_string()]
        );
        assert_eq!(
            stats[0].1.stats_sources,
            vec!["request_metadata".to_string()]
        );
        assert!(stats[0].1.stats_observed_at_unix_ms > 0);
    }

    #[test]
    fn inference_progress_input_tps_uses_progress_timestamp_not_response_headers() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let observation = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-sse-input-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 40,
            input_tokens_processed_from_inference_progress: true,
            engine_reported_input_tokens_total: Some(100),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: true,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(5)),
            time_to_input_tokens_processed: Some(Duration::from_secs(2)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_secs(30),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.queued_input_size, 60);
        assert!(stats[0].1.stats_sources.is_empty());
    }

    #[test]
    fn inference_progress_input_processed_is_trusted_without_total() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let observation = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-sse-ip-without-it".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 40,
            input_tokens_processed_from_inference_progress: true,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: true,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(5)),
            time_to_input_tokens_processed: Some(Duration::from_secs(2)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_secs(30),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.queued_input_size, 60);
    }

    #[test]
    fn header_input_processed_without_total_is_not_trusted() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let observation = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-header-ip-without-it".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 40,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: None,
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: true,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_secs(2)),
            time_to_input_tokens_processed: Some(Duration::from_secs(2)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_secs(30),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.queued_input_size, 100);
    }

    #[test]
    fn mismatched_input_progress_does_not_feed_routing_stats() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let live = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-mismatch".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 100,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 1_000,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(1_000),
            input_tokens_total_mismatch: true,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: true,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_secs(2)),
            time_to_input_tokens_processed: Some(Duration::from_secs(2)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_secs(2),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &live);
        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
        assert_eq!(stats[0].1.queued_input_size, 100);

        let complete = RequestObservation {
            state: RequestObservationState::Complete,
            output_messages: 1,
            output_tokens: 1,
            time_to_first_output: Some(Duration::from_secs(10)),
            time_to_first_token: Some(Duration::from_secs(10)),
            total_duration: Duration::from_secs(11),
            ..live
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &complete);
        assert_eq!(stats[0].1.last_mean_input_tps, 0.0);
    }

    #[test]
    fn header_output_counters_do_not_claim_chunk_usage_capability() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();

        let observation = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-header-output".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 12,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 12,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(12),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 7,
            output_tokens_explicit: true,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: true,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(100)),
            time_to_input_tokens_processed: Some(Duration::from_millis(100)),
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(500),
        };

        let stats = record_observation(&config, &mut per_model, &mut in_flight, &observation);

        assert_eq!(
            stats[0].1.stats_capabilities,
            vec!["request.final_headers".to_string()]
        );
        assert_eq!(
            stats[0].1.stats_sources,
            vec!["request_metadata".to_string()]
        );
    }

    #[test]
    fn snapshot_includes_polled_kv_cache_stats() {
        let config = StatsCollectorConfig::default();
        let mut per_model = HashMap::<String, ModelMetricsState>::new();
        per_model.entry("model-a".to_string()).or_default().kv_cache = KvCacheStatsSnapshot {
            model: "model-a".to_string(),
            kv_cache_capacity_tokens: 1_000,
            kv_cache_used_tokens: 400,
            kv_cache_free_tokens: 600,
        };

        let stats = snapshot_model_stats(&config, &mut per_model, &HashMap::new(), "model-a");

        assert_eq!(stats.kv_cache_capacity_tokens, 1_000);
        assert_eq!(stats.kv_cache_used_tokens, 400);
        assert_eq!(stats.kv_cache_free_tokens, 600);
    }

    #[tokio::test]
    async fn kv_cache_poll_updates_model_metrics() {
        async fn kv_cache_stats() -> Json<serde_json::Value> {
            Json(serde_json::json!({
                "model": "model-a",
                "kv_cache_capacity_tokens": 1000,
                "kv_cache_used_tokens": 400,
                "kv_cache_free_tokens": 600
            }))
        }

        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener should have address");
        let server = tokio::spawn(async move {
            let app = Router::new().route("/kv-cache", get(kv_cache_stats));
            axum::serve(listener, app)
                .await
                .expect("KV-cache test server should run");
        });

        let config = StatsCollectorConfig {
            kv_cache_stats_url: Some(format!("http://{addr}/kv-cache")),
            kv_cache_poll_interval: Duration::from_millis(10),
            kv_cache_request_timeout: Duration::from_secs(1),
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let (_observation_tx, observation_rx) = request_observation_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(4);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            None,
            model_stats_tx,
            stop_rx,
        ));

        let (model_id, stats) =
            tokio::time::timeout(Duration::from_secs(2), model_stats_rx.recv_async())
                .await
                .expect("KV-cache stats should be published")
                .expect("collector should publish stats");
        assert_eq!(model_id, "model-a");
        assert_eq!(stats.kv_cache_capacity_tokens, 1000);
        assert_eq!(stats.kv_cache_used_tokens, 400);
        assert_eq!(stats.kv_cache_free_tokens, 600);

        let body = metrics.gather_text().expect("metrics should encode");
        assert!(body.contains(r#"pylon_model_kv_cache_capacity_tokens{model="model-a"} 1000"#));
        assert!(body.contains(r#"pylon_model_kv_cache_used_tokens{model="model-a"} 400"#));
        assert!(body.contains(r#"pylon_model_kv_cache_free_tokens{model="model-a"} 600"#));

        stop_tx.send(true).expect("collector should receive stop");
        tokio::time::timeout(Duration::from_secs(2), collector)
            .await
            .expect("collector should stop")
            .expect("collector task should join");
        server.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn stats_collector_publishes_mean_input_tps_from_aggregator_updates() {
        let config = StatsCollectorConfig {
            observation_channel_capacity: 16,
            ..Default::default()
        };
        let (observation_tx, observation_rx) = request_observation_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(16);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            None,
            model_stats_tx,
            stop_rx,
        ));

        let input_processing = RequestObservation {
            endpoint: RequestObservationEndpoint::ChatCompletions,
            request_id: "req-stats-progress".to_string(),
            routing_key: Some("rk-1".to_string()),
            model_id: "model-a".to_string(),
            priority: 0,
            input_tokens: 50,
            embedding_items: 0,
            embedding_items_observed: false,
            input_tokens_processed: 0,
            input_tokens_processed_from_inference_progress: false,
            engine_reported_input_tokens_total: Some(50),
            input_tokens_total_mismatch: false,
            upstream_status: Some(200),
            output_messages: 0,
            output_tokens: 0,
            output_tokens_explicit: false,
            output_tokens_from_chunk_usage: false,
            has_engine_request_stats: false,
            has_inference_progress_stats: false,
            state: RequestObservationState::InputProcessing,
            time_to_response_headers: Some(Duration::from_millis(1)),
            time_to_input_tokens_processed: None,
            time_to_first_output: None,
            time_to_first_token: None,
            total_duration: Duration::from_millis(1),
        };
        observation_tx
            .send_async(input_processing.clone())
            .await
            .expect("collector should receive input-processing observation");
        tokio::task::yield_now().await;

        for tick in 1..=5 {
            let observation = RequestObservation {
                input_tokens_processed: tick * 10,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(tick * 100)),
                total_duration: Duration::from_millis(tick * 100),
                ..input_processing.clone()
            };
            observation_tx
                .send_async(observation)
                .await
                .expect("collector should receive observations");
            tokio::task::yield_now().await;
            tokio::time::advance(EventAggregatorConfig::default().tick_duration).await;
            tokio::task::yield_now().await;
        }
        observation_tx
            .send_async(RequestObservation {
                state: RequestObservationState::Complete,
                input_tokens_processed: 50,
                input_tokens_processed_from_inference_progress: true,
                has_inference_progress_stats: true,
                time_to_input_tokens_processed: Some(Duration::from_millis(500)),
                output_messages: 1,
                output_tokens: 2,
                time_to_first_output: Some(Duration::from_millis(500)),
                time_to_first_token: Some(Duration::from_millis(600)),
                total_duration: Duration::from_secs(1),
                ..input_processing
            })
            .await
            .expect("collector should receive terminal observation");
        tokio::task::yield_now().await;

        let mean_stats = receive_model_stats_with_last_mean_input_tps(&model_stats_rx, 100.0).await;
        assert_eq!(mean_stats.last_mean_input_tps, 100.0);
        let stats = receive_model_stats_with_last_mean_input_tps(&model_stats_rx, 100.0).await;
        assert_eq!(stats.output_tps, 5.0);

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[tokio::test]
    async fn stats_collector_seeds_fixed_input_tps_for_queue_admission() {
        let queue_tracker = QueueAdmissionTracker::default();
        let config = StatsCollectorConfig {
            configured_model_ids: vec!["model-a".to_string()],
            fixed_last_mean_input_tps: Some(2_200.0),
            queue_tracker: queue_tracker.clone(),
            ..Default::default()
        };
        let (_observation_tx, observation_rx) = request_observation_channel(&config);
        let (model_stats_tx, model_stats_rx) = flume::bounded(4);
        let (stop_tx, stop_rx) = watch::channel(false);
        let collector = tokio::spawn(run_stats_collector(
            config,
            observation_rx,
            None,
            model_stats_tx,
            stop_rx,
        ));

        let (model_id, stats) =
            tokio::time::timeout(Duration::from_secs(1), model_stats_rx.recv_async())
                .await
                .expect("fixed TPS stats should be published")
                .expect("collector should stay connected");
        assert_eq!(model_id, "model-a");
        assert_eq!(stats.last_mean_input_tps, 2_200.0);

        let _queued =
            queue_tracker.track_request(&crate::request_observer::RequiredTunnelHeaders {
                request_id: "req-queued".to_string(),
                routing_key: None,
                model_id: "model-a".to_string(),
                priority: 0,
                input_tokens: 32,
                accepted_at: std::time::Instant::now(),
            });
        queue_tracker.record_observation(&active_chat_observation(
            "req-queued",
            RequestObservationState::Queued,
        ));
        assert_eq!(
            queue_tracker
                .snapshot_model("model-a")
                .queue_time_estimate_ms_by_priority,
            Some(HashMap::from([(0, 11)]))
        );

        stop_tx.send(true).expect("collector should receive stop");
        collector.await.expect("collector task should join");
    }

    #[test]
    fn records_metrics_when_configured() {
        let metrics = PylonMetrics::new().expect("metrics should initialize");
        let config = StatsCollectorConfig {
            metrics: Some(metrics.clone()),
            ..Default::default()
        };
        let mut per_model = HashMap::new();
        let mut in_flight = HashMap::new();
        let observation =
            completed_observation(20, 2, 10, Duration::from_secs(2), Duration::from_secs(4));

        for _ in 0..5 {
            let updated_stats =
                record_observation(&config, &mut per_model, &mut in_flight, &observation);
            observe_metrics(&config, &observation, &updated_stats);
        }
        let mut mean_input_stats =
            snapshot_model_stats(&config, &mut per_model, &in_flight, "model-a");
        mean_input_stats.last_mean_input_tps = 10.0;
        observe_model_metric(&config, "model-a", &mean_input_stats);

        let body = metrics.gather_text().expect("metrics should encode");
        assert!(body.contains(
            r#"pylon_requests_total{model="model-a",routing_key="rk-1",status="complete"} 5"#
        ));
        assert!(body.contains(r#"pylon_model_last_mean_input_tps{model="model-a"} 10"#));
        assert!(body.contains(r#"pylon_model_output_tps{model="model-a"} 5"#));
    }

    #[test]
    fn rejects_kv_cache_stats_for_unconfigured_models() {
        let config = StatsCollectorConfig {
            configured_model_ids: vec!["model-a".to_string()],
            ..Default::default()
        };
        let kv_cache = KvCacheStatsSnapshot {
            model: "model-b".to_string(),
            kv_cache_capacity_tokens: 1_000,
            kv_cache_used_tokens: 400,
            kv_cache_free_tokens: 600,
        };

        assert!(!kv_cache_stats_model_allowed(&config, &kv_cache));
    }
}
