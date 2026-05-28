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

mod groq_multiregion;
mod power_of_two;
mod pulsar;
mod random;
mod round_robin;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use scc::HashMap as SccHashMap;
use serde::Deserialize;

use crate::load_balancer_state::{RoutedClusterSnapshot, RoutingTargetKey};
use groq_multiregion::{GroqMultiregionConfig, GroqMultiregionLoadBalancer};
use power_of_two::PowerOfTwoLoadBalancer;
use pulsar::PulsarLoadBalancer;
use random::RandomLoadBalancer;
use round_robin::RoundRobinLoadBalancer;

#[derive(Clone, Debug)]
pub struct LoadBalancerRequest<'a> {
    pub routing_target: &'a RoutingTargetKey,
    pub cache_affinity_key: Option<&'a str>,
    pub input_tokens: Option<u64>,
    pub priority: u32,
    pub received_at: Instant,
    pub request_slo: Option<Duration>,
    pub excluded_cluster_ids: Option<&'a HashSet<String>>,
}

impl LoadBalancerRequest<'_> {
    pub(crate) fn has_excluded_clusters(&self) -> bool {
        self.excluded_cluster_ids
            .is_some_and(|excluded| !excluded.is_empty())
    }

    pub(crate) fn excludes_cluster(&self, cluster_id: &str) -> bool {
        self.excluded_cluster_ids
            .is_some_and(|excluded| excluded.contains(cluster_id))
    }
}

#[derive(Clone, Debug)]
pub struct LoadBalancerChoice {
    pub candidate: RoutedClusterSnapshot,
    pub rank_depth: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoadBalancerCandidateChoice {
    // Hot-path routing returns the slice index so the proxy can borrow the
    // selected snapshot instead of cloning every load-balancer decision.
    pub candidate_index: usize,
    pub rank_depth: usize,
}

impl LoadBalancerCandidateChoice {
    pub(crate) fn with_rank_depth_1(candidate_index: usize) -> Self {
        Self {
            candidate_index,
            rank_depth: 1,
        }
    }

    fn to_owned(self, candidates: &[RoutedClusterSnapshot]) -> LoadBalancerChoice {
        // Keep the old owned API for compatibility; new proxy paths stay on
        // `LoadBalancerCandidateChoice` to avoid this snapshot clone.
        LoadBalancerChoice {
            candidate: candidates[self.candidate_index].clone(),
            rank_depth: self.rank_depth,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum LoadBalancerModelConfig {
    Name(LoadBalancerAlgorithm),
    Detailed(Box<LoadBalancerAlgorithmConfig>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LoadBalancerAlgorithm {
    PowerOfTwo,
    GroqMultiregion,
    RoundRobin,
    Random,
    Pulsar,
}

impl fmt::Display for LoadBalancerAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::PowerOfTwo => "power-of-two",
            Self::GroqMultiregion => "groq-multiregion",
            Self::RoundRobin => "round-robin",
            Self::Random => "random",
            Self::Pulsar => "pulsar",
        };
        write!(f, "{name}")
    }
}

impl FromStr for LoadBalancerAlgorithm {
    type Err = serde::de::value::Error;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::deserialize(serde::de::value::StrDeserializer::<serde::de::value::Error>::new(name))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadBalancerAlgorithmOverride {
    raw: String,
    algorithm: LoadBalancerAlgorithm,
}

impl LoadBalancerAlgorithmOverride {
    pub fn parse(value: &str) -> Result<Self, LoadBalancerRoutingAlgorithmError> {
        value.parse()
    }

    pub fn requested_algorithm(&self) -> &str {
        &self.raw
    }

    pub fn algorithm(&self) -> LoadBalancerAlgorithm {
        self.algorithm
    }
}

impl FromStr for LoadBalancerAlgorithmOverride {
    type Err = LoadBalancerRoutingAlgorithmError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let raw = value.trim();
        if raw.is_empty() {
            return Err(LoadBalancerRoutingAlgorithmError::Unknown {
                raw: raw.to_string(),
            });
        }

        let normalized = raw.to_ascii_lowercase();
        let canonical = normalized.replace('_', "-");
        let algorithm =
            canonical
                .parse()
                .map_err(|_| LoadBalancerRoutingAlgorithmError::Unknown {
                    raw: raw.to_string(),
                })?;

        Ok(Self {
            raw: raw.to_string(),
            algorithm,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadBalancerRoutingAlgorithmError {
    Unknown {
        raw: String,
    },
    Unavailable {
        raw: String,
        algorithm: LoadBalancerAlgorithm,
    },
}

impl LoadBalancerRoutingAlgorithmError {
    pub fn requested_algorithm(&self) -> &str {
        match self {
            Self::Unknown { raw } | Self::Unavailable { raw, .. } => raw,
        }
    }

    pub fn reason(&self) -> &'static str {
        match self {
            Self::Unknown { .. } => "unknown",
            Self::Unavailable { .. } => "unavailable",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBalancerAlgorithmConfig {
    pub algorithm: LoadBalancerAlgorithm,
    #[serde(default)]
    pub seed: Option<String>,
    #[serde(default)]
    pub cache_affinity_virtual_nodes: Option<usize>,
    #[serde(default)]
    pub cache_affinity_backend_selection_count: Option<usize>,
    #[serde(default)]
    pub require_cache_affinity_key: Option<bool>,
    #[serde(default)]
    pub require_input_tokens: Option<bool>,
    #[serde(default)]
    pub require_kv_metrics: Option<bool>,
    #[serde(default)]
    pub queue_slo_ms: Option<u64>,
    #[serde(default)]
    pub max_queue_time_floor_ms: Option<u64>,
    #[serde(default)]
    pub max_queue_time_ceil_ms: Option<u64>,
    #[serde(default)]
    pub max_queue_tokens_factor: Option<f64>,
    #[serde(default)]
    pub hard_token_cap_factor: Option<f64>,
    #[serde(default)]
    pub reentry_hysteresis: Option<f64>,
    pub ttft_bucket_size_ms: Option<u64>,
    #[serde(default)]
    pub next_bucket_unlock_factor: Option<f64>,
    #[serde(default)]
    pub n: Option<usize>,
    #[serde(default)]
    pub max_queued: Option<u64>,
    #[serde(default)]
    pub max_input_work_seconds: Option<f64>,
    #[serde(default)]
    pub ignore_queue_time: Option<bool>,
    #[serde(default)]
    pub ignore_input_processing_time: Option<bool>,
    #[serde(default)]
    pub request_algorithms: HashMap<LoadBalancerAlgorithm, LoadBalancerModelConfig>,
}

impl LoadBalancerAlgorithmConfig {
    pub fn requires_cache_affinity_key(&self) -> bool {
        self.require_cache_affinity_key.unwrap_or(false)
    }

    pub fn requires_input_tokens(&self) -> bool {
        self.require_input_tokens.unwrap_or(false)
    }

    pub fn requires_kv_metrics(&self) -> bool {
        self.require_kv_metrics.unwrap_or(false)
    }
}

impl From<LoadBalancerAlgorithm> for LoadBalancerAlgorithmConfig {
    fn from(algorithm: LoadBalancerAlgorithm) -> Self {
        Self {
            algorithm,
            seed: None,
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: None,
            require_input_tokens: None,
            require_kv_metrics: None,
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        }
    }
}

impl LoadBalancerModelConfig {
    pub fn into_algorithm_config(self) -> LoadBalancerAlgorithmConfig {
        match self {
            Self::Name(algorithm) => LoadBalancerAlgorithmConfig::from(algorithm),
            Self::Detailed(config) => *config,
        }
    }
}

pub trait LoadBalancer: Send + Sync + fmt::Display {
    fn choose_candidate(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
    ) -> Option<LoadBalancerCandidateChoice>;

    fn choose(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
    ) -> Option<LoadBalancerChoice> {
        self.choose_candidate(request, candidates)
            .map(|choice| choice.to_owned(candidates))
    }
}

const HASH_INPUT_STACK_LEN: usize = 256;
pub(super) const MAX_CACHE_AFFINITY_CACHE_KEY_BYTES: usize = 256;

#[inline]
pub(super) fn cache_affinity_key_is_cacheable(cache_affinity_key: &str) -> bool {
    cache_affinity_key.len() <= MAX_CACHE_AFFINITY_CACHE_KEY_BYTES
}

#[inline]
pub(super) fn input_work_units(candidate: &RoutedClusterSnapshot) -> f64 {
    candidate.stats.queued_input_size as f64
}

#[cfg(test)]
fn input_work_seconds(
    candidates: &[RoutedClusterSnapshot],
    request_input_tokens: u64,
    excluded_cluster_ids: Option<&HashSet<String>>,
) -> Option<f64> {
    input_work_seconds_from_candidates(
        candidates.iter().filter(|candidate| {
            !excluded_cluster_ids.is_some_and(|excluded| excluded.contains(&candidate.cluster_id))
        }),
        request_input_tokens,
    )
}

pub(crate) fn input_work_seconds_for_request(
    config: &LoadBalancerAlgorithmConfig,
    request: &LoadBalancerRequest<'_>,
    candidates: &[RoutedClusterSnapshot],
) -> Option<f64> {
    input_work_seconds_from_candidates(
        candidates.iter().filter(|candidate| {
            input_work_admission_includes_candidate(config, request, candidate)
        }),
        request.input_tokens.unwrap_or_default(),
    )
}

fn input_work_admission_includes_candidate(
    config: &LoadBalancerAlgorithmConfig,
    request: &LoadBalancerRequest<'_>,
    candidate: &RoutedClusterSnapshot,
) -> bool {
    match config.algorithm {
        LoadBalancerAlgorithm::Pulsar => {
            pulsar::input_work_admission_candidate(config, request, candidate)
        }
        _ => !request.excludes_cluster(&candidate.cluster_id),
    }
}

fn input_work_seconds_from_candidates<'a>(
    candidates: impl IntoIterator<Item = &'a RoutedClusterSnapshot>,
    request_input_tokens: u64,
) -> Option<f64> {
    let mut work_units = request_input_tokens as f64;
    let mut service_rate = 0.0;
    for candidate in candidates {
        work_units += input_work_units(candidate);
        if candidate.stats.last_mean_input_tps > 0.0
            && candidate.stats.last_mean_input_tps.is_finite()
        {
            service_rate += candidate.stats.last_mean_input_tps;
        }
    }

    (service_rate > 0.0 && service_rate.is_finite())
        .then_some(work_units / service_rate)
        .filter(|seconds| seconds.is_finite())
}

pub(super) struct HashInputBuilder {
    stack: [u8; HASH_INPUT_STACK_LEN],
    len: usize,
    heap: Option<Vec<u8>>,
}

impl HashInputBuilder {
    pub(super) fn new() -> Self {
        Self {
            stack: [0; HASH_INPUT_STACK_LEN],
            len: 0,
            heap: None,
        }
    }

    #[inline]
    pub(super) fn push(&mut self, byte: u8) {
        self.extend_from_slice(&[byte]);
    }

    #[inline]
    pub(super) fn append_tagged_bytes(&mut self, tag: &[u8], value: &[u8]) {
        self.extend_from_slice(tag);
        self.push(0xff);
        self.extend_from_slice(&(value.len() as u64).to_le_bytes());
        self.extend_from_slice(value);
    }

    #[inline]
    pub(super) fn as_slice(&self) -> &[u8] {
        match &self.heap {
            Some(heap) => heap.as_slice(),
            None => &self.stack[..self.len],
        }
    }

    #[inline]
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        if let Some(heap) = &mut self.heap {
            heap.extend_from_slice(bytes);
            return;
        }

        let new_len = self.len + bytes.len();
        if new_len <= self.stack.len() {
            self.stack[self.len..new_len].copy_from_slice(bytes);
            self.len = new_len;
            return;
        }

        let mut heap = Vec::with_capacity(new_len.max(self.stack.len() * 2));
        heap.extend_from_slice(&self.stack[..self.len]);
        heap.extend_from_slice(bytes);
        self.heap = Some(heap);
    }
}

fn default_algorithm() -> LoadBalancerAlgorithm {
    LoadBalancerAlgorithm::PowerOfTwo
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBalancerConfig {
    #[serde(default = "default_algorithm")]
    pub default: LoadBalancerAlgorithm,
    #[serde(default)]
    pub request_algorithms: HashMap<LoadBalancerAlgorithm, LoadBalancerModelConfig>,
    #[serde(default)]
    pub models: HashMap<String, LoadBalancerModelConfig>,
}

impl Default for LoadBalancerConfig {
    fn default() -> Self {
        Self {
            default: default_algorithm(),
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        }
    }
}

pub fn create_load_balancer_with_config(
    config: &LoadBalancerAlgorithmConfig,
) -> anyhow::Result<Arc<dyn LoadBalancer>> {
    match config.algorithm {
        LoadBalancerAlgorithm::PowerOfTwo => Ok(Arc::new(PowerOfTwoLoadBalancer)),
        LoadBalancerAlgorithm::GroqMultiregion => Ok(Arc::new(GroqMultiregionLoadBalancer::new(
            GroqMultiregionConfig::from_algorithm_config(config),
        ))),
        LoadBalancerAlgorithm::RoundRobin => Ok(Arc::new(RoundRobinLoadBalancer::new())),
        LoadBalancerAlgorithm::Random => Ok(Arc::new(RandomLoadBalancer)),
        LoadBalancerAlgorithm::Pulsar => Ok(Arc::new(PulsarLoadBalancer::new(config.clone()))),
    }
}

#[derive(Clone, Debug)]
struct LoadBalancerAlgorithmConfigSet {
    configured: LoadBalancerAlgorithmConfig,
    request_algorithms: HashMap<LoadBalancerAlgorithm, LoadBalancerAlgorithmConfig>,
}

pub struct LoadBalancerRouter {
    default_config: LoadBalancerAlgorithmConfigSet,
    default_per_target: SccHashMap<RoutingTargetKey, Arc<dyn LoadBalancer>>,
    configured_per_target: SccHashMap<RoutingTargetKey, Arc<dyn LoadBalancer>>,
    request_per_target: SccHashMap<LoadBalancerOverrideKey, Arc<dyn LoadBalancer>>,
    per_model_config: HashMap<String, LoadBalancerAlgorithmConfigSet>,
}

#[derive(Clone, Debug)]
pub struct LoadBalancerSelection {
    pub choice: LoadBalancerChoice,
    pub effective_algorithm: LoadBalancerAlgorithm,
    pub requested_algorithm: Option<String>,
}

#[derive(Clone, Debug)]
pub struct LoadBalancerCandidateSelection {
    pub choice: LoadBalancerCandidateChoice,
    pub effective_algorithm: LoadBalancerAlgorithm,
    pub requested_algorithm: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LoadBalancerOverrideKey {
    routing_target: RoutingTargetKey,
    algorithm: LoadBalancerAlgorithm,
}

#[derive(Clone, Debug)]
pub struct LoadBalancerAlgorithmResolution {
    config: LoadBalancerAlgorithmConfig,
    use_request_instances: bool,
    requested_algorithm: Option<String>,
}

impl LoadBalancerAlgorithmResolution {
    pub fn config(&self) -> &LoadBalancerAlgorithmConfig {
        &self.config
    }

    fn effective_algorithm(&self) -> LoadBalancerAlgorithm {
        self.config.algorithm
    }

    fn requested_algorithm(&self) -> Option<String> {
        self.requested_algorithm.clone()
    }
}

impl LoadBalancerRouter {
    pub fn from_config(config: &LoadBalancerConfig) -> anyhow::Result<Self> {
        let default_config = Self::build_algorithm_config_set(
            LoadBalancerAlgorithmConfig::from(config.default),
            &config.request_algorithms,
        )?;
        let mut per_model_config = HashMap::new();
        for (model_id, model_config) in &config.models {
            let mut algorithm_config = model_config.clone().into_algorithm_config();
            let request_algorithms = std::mem::take(&mut algorithm_config.request_algorithms);
            let config_set =
                Self::build_algorithm_config_set(algorithm_config, &request_algorithms)?;
            per_model_config.insert(model_id.clone(), config_set);
        }
        Ok(Self {
            default_config,
            default_per_target: SccHashMap::default(),
            configured_per_target: SccHashMap::default(),
            request_per_target: SccHashMap::default(),
            per_model_config,
        })
    }

    fn build_algorithm_config_set(
        configured: LoadBalancerAlgorithmConfig,
        request_algorithms: &HashMap<LoadBalancerAlgorithm, LoadBalancerModelConfig>,
    ) -> anyhow::Result<LoadBalancerAlgorithmConfigSet> {
        // Validate configured algorithms at startup. Stateful instances are
        // created lazily per routing target to preserve routing-key isolation.
        let _ = create_load_balancer_with_config(&configured)?;
        let request_algorithms = Self::build_request_algorithm_configs(request_algorithms)?;
        Ok(LoadBalancerAlgorithmConfigSet {
            configured,
            request_algorithms,
        })
    }

    fn build_request_algorithm_configs(
        request_algorithms: &HashMap<LoadBalancerAlgorithm, LoadBalancerModelConfig>,
    ) -> anyhow::Result<HashMap<LoadBalancerAlgorithm, LoadBalancerAlgorithmConfig>> {
        let mut configs = HashMap::new();
        for (algorithm, model_config) in request_algorithms {
            let mut algorithm_config = model_config.clone().into_algorithm_config();
            if algorithm_config.algorithm != *algorithm {
                anyhow::bail!(
                    "request_algorithms key {algorithm} does not match configured algorithm {}",
                    algorithm_config.algorithm
                );
            }
            algorithm_config.request_algorithms.clear();
            let _ = create_load_balancer_with_config(&algorithm_config)?;
            configs.insert(*algorithm, algorithm_config);
        }
        Ok(configs)
    }

    fn load_balancer_for_target(
        instances: &SccHashMap<RoutingTargetKey, Arc<dyn LoadBalancer>>,
        target: &RoutingTargetKey,
        config: &LoadBalancerAlgorithmConfig,
    ) -> Arc<dyn LoadBalancer> {
        if let Some(lb) = instances.read_sync(target, |_target, lb| lb.clone()) {
            return lb;
        }

        let lb = create_load_balancer_with_config(config)
            .expect("load balancer config validated during router construction");
        if instances.insert_sync(target.clone(), lb.clone()).is_ok() {
            return lb;
        }

        instances
            .read_sync(target, |_target, lb| lb.clone())
            .expect("per-target load balancer should exist after insert race")
    }

    fn configured_or_default_config_source(
        &self,
        target: &RoutingTargetKey,
    ) -> (
        &SccHashMap<RoutingTargetKey, Arc<dyn LoadBalancer>>,
        &LoadBalancerAlgorithmConfigSet,
    ) {
        let (instances, config_set) =
            if let Some(config_set) = self.per_model_config.get(&target.model_id) {
                (&self.configured_per_target, config_set)
            } else {
                (&self.default_per_target, &self.default_config)
            };
        (instances, config_set)
    }

    fn algorithm_config_set(&self, model_id: &str) -> &LoadBalancerAlgorithmConfigSet {
        self.per_model_config
            .get(model_id)
            .unwrap_or(&self.default_config)
    }

    fn request_algorithm_config_for_override<'a>(
        &'a self,
        config_set: &'a LoadBalancerAlgorithmConfigSet,
        algorithm_override: &LoadBalancerAlgorithmOverride,
    ) -> Result<(&'a LoadBalancerAlgorithmConfig, bool), LoadBalancerRoutingAlgorithmError> {
        let raw = algorithm_override.requested_algorithm();
        let algorithm = algorithm_override.algorithm();

        if config_set.configured.algorithm == algorithm {
            return Ok((&config_set.configured, false));
        }

        if let Some(config) = config_set.request_algorithms.get(&algorithm) {
            return Ok((config, true));
        }

        if !std::ptr::eq(config_set, &self.default_config)
            && let Some(config) = self.default_config.request_algorithms.get(&algorithm)
        {
            return Ok((config, true));
        }

        Err(LoadBalancerRoutingAlgorithmError::Unavailable {
            raw: raw.to_string(),
            algorithm,
        })
    }

    fn request_load_balancer_for_target(
        &self,
        target: &RoutingTargetKey,
        config: &LoadBalancerAlgorithmConfig,
    ) -> Arc<dyn LoadBalancer> {
        let key = LoadBalancerOverrideKey {
            routing_target: target.clone(),
            algorithm: config.algorithm,
        };
        if let Some(lb) = self
            .request_per_target
            .read_sync(&key, |_key, lb| lb.clone())
        {
            return lb;
        }

        let lb = create_load_balancer_with_config(config)
            .expect("request load balancer config validated during router construction");
        if self
            .request_per_target
            .insert_sync(key.clone(), lb.clone())
            .is_ok()
        {
            return lb;
        }

        self.request_per_target
            .read_sync(&key, |_key, lb| lb.clone())
            .expect("request per-target load balancer should exist after insert race")
    }

    fn load_balancer_for_algorithm_resolution(
        &self,
        request: &LoadBalancerRequest<'_>,
        resolution: &LoadBalancerAlgorithmResolution,
    ) -> Arc<dyn LoadBalancer> {
        let (configured_instances, _) =
            self.configured_or_default_config_source(request.routing_target);
        if resolution.use_request_instances {
            self.request_load_balancer_for_target(request.routing_target, resolution.config())
        } else {
            Self::load_balancer_for_target(
                configured_instances,
                request.routing_target,
                resolution.config(),
            )
        }
    }

    pub fn choose(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
    ) -> Option<LoadBalancerChoice> {
        self.choose_candidate(request, candidates)
            .map(|choice| choice.to_owned(candidates))
    }

    pub fn choose_candidate(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
    ) -> Option<LoadBalancerCandidateChoice> {
        if candidates.is_empty() {
            return None;
        }

        // `choose_candidate` is the no-request-override hot path used by the
        // proxy and load-balancer microbenchmarks. Avoid constructing a cloned
        // `LoadBalancerAlgorithmResolution` just to recover the model's
        // configured algorithm immediately afterward.
        let (instances, config_set) =
            self.configured_or_default_config_source(request.routing_target);
        let lb = Self::load_balancer_for_target(
            instances,
            request.routing_target,
            &config_set.configured,
        );
        lb.choose_candidate(request, candidates)
    }

    pub fn choose_with_algorithm_override(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
        algorithm_override: Option<&LoadBalancerAlgorithmOverride>,
    ) -> Result<Option<LoadBalancerSelection>, LoadBalancerRoutingAlgorithmError> {
        let resolution =
            self.resolve_algorithm_override(&request.routing_target.model_id, algorithm_override)?;
        Ok(self.choose_with_algorithm_resolution(request, candidates, &resolution))
    }

    pub fn choose_with_algorithm_resolution(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
        resolution: &LoadBalancerAlgorithmResolution,
    ) -> Option<LoadBalancerSelection> {
        self.choose_candidate_with_algorithm_resolution(request, candidates, resolution)
            .map(|selection| LoadBalancerSelection {
                choice: selection.choice.to_owned(candidates),
                effective_algorithm: selection.effective_algorithm,
                requested_algorithm: selection.requested_algorithm,
            })
    }

    pub fn choose_candidate_with_algorithm_resolution(
        &self,
        request: &LoadBalancerRequest<'_>,
        candidates: &[RoutedClusterSnapshot],
        resolution: &LoadBalancerAlgorithmResolution,
    ) -> Option<LoadBalancerCandidateSelection> {
        if candidates.is_empty() {
            return None;
        }

        let lb = self.load_balancer_for_algorithm_resolution(request, resolution);
        let effective_algorithm = resolution.effective_algorithm();
        let requested_algorithm = resolution.requested_algorithm();

        lb.choose_candidate(request, candidates)
            .map(|choice| LoadBalancerCandidateSelection {
                choice,
                effective_algorithm,
                requested_algorithm,
            })
    }

    pub fn algorithm_name(&self, model_id: &str) -> String {
        self.algorithm_config(model_id).algorithm.to_string()
    }

    pub fn algorithm_config(&self, model_id: &str) -> &LoadBalancerAlgorithmConfig {
        &self.algorithm_config_set(model_id).configured
    }

    pub fn resolve_algorithm_override(
        &self,
        model_id: &str,
        algorithm_override: Option<&LoadBalancerAlgorithmOverride>,
    ) -> Result<LoadBalancerAlgorithmResolution, LoadBalancerRoutingAlgorithmError> {
        let config_set = self.algorithm_config_set(model_id);
        let (config, use_request_instances) = if let Some(algorithm_override) = algorithm_override {
            self.request_algorithm_config_for_override(config_set, algorithm_override)?
        } else {
            (&config_set.configured, false)
        };
        Ok(LoadBalancerAlgorithmResolution {
            config: config.clone(),
            use_request_instances,
            requested_algorithm: algorithm_override
                .map(LoadBalancerAlgorithmOverride::requested_algorithm)
                .map(ToOwned::to_owned),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    use stargate_proto::pb::ModelStats;

    use super::*;
    use crate::load_balancer::groq_multiregion::{
        GroqMultiregionConfig, cache_affinity_candidates, cache_affinity_virtual_node_hash,
    };
    use crate::load_balancer::pulsar::{PulsarLoadBalancer, pulsar_hash64};
    use xxhash_rust::xxh3::xxh3_64;

    fn target_with_model(model_id: &str) -> RoutingTargetKey {
        RoutingTargetKey {
            routing_key: Some("rk-1".to_string()),
            model_id: model_id.to_string(),
        }
    }

    fn target_with_routing_key(routing_key: &str, model_id: &str) -> RoutingTargetKey {
        RoutingTargetKey {
            routing_key: Some(routing_key.to_string()),
            model_id: model_id.to_string(),
        }
    }

    fn target() -> RoutingTargetKey {
        target_with_model("model-a")
    }

    fn request<'a>(
        target: &'a RoutingTargetKey,
        cache_affinity_key: Option<&'a str>,
        input_tokens: Option<u64>,
    ) -> LoadBalancerRequest<'a> {
        request_with_priority(target, cache_affinity_key, input_tokens, 0)
    }

    fn request_with_priority<'a>(
        target: &'a RoutingTargetKey,
        cache_affinity_key: Option<&'a str>,
        input_tokens: Option<u64>,
        priority: u32,
    ) -> LoadBalancerRequest<'a> {
        LoadBalancerRequest {
            routing_target: target,
            cache_affinity_key,
            input_tokens,
            priority,
            received_at: Instant::now(),
            request_slo: None,
            excluded_cluster_ids: None,
        }
    }

    fn multiregion_runtime_config(config: LoadBalancerAlgorithmConfig) -> GroqMultiregionConfig {
        GroqMultiregionConfig::from_algorithm_config(&config)
    }

    fn candidate(id: &str, kv_cache_free_tokens: u64) -> RoutedClusterSnapshot {
        RoutedClusterSnapshot {
            cluster_id: id.to_string(),
            stats: ModelStats {
                output_tps: 0.0,
                last_mean_input_tps: 100.0,
                max_output_tps: 100.0,
                queue_size: 0,
                queued_input_size: 0,
                kv_cache_capacity_tokens: 1024,
                kv_cache_used_tokens: 1024 - kv_cache_free_tokens,
                kv_cache_free_tokens,
                num_running_queries: 0,
                max_engine_concurrency: 0,
                total_query_input_size: 0,
                queue_time_estimate_ms_by_priority: HashMap::new(),
                ..ModelStats::default()
            },
            rtt: Duration::from_millis(5),
            snapshot_updated_at: Instant::now(),
            status: 1,
            active_backend_count: 1,
        }
    }

    fn request_algorithm_map(
        algorithms: &[LoadBalancerAlgorithm],
    ) -> HashMap<LoadBalancerAlgorithm, LoadBalancerModelConfig> {
        algorithms
            .iter()
            .copied()
            .map(|algorithm| (algorithm, LoadBalancerModelConfig::Name(algorithm)))
            .collect()
    }

    fn append_test_tagged_bytes(bytes: &mut Vec<u8>, tag: &[u8], value: &[u8]) {
        bytes.extend_from_slice(tag);
        bytes.push(0xff);
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value);
    }

    #[test]
    fn simple_model_config_parses_to_algorithm_enum() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "groq-multiregion",
                "models": {
                    "model-a": "round-robin"
                }
            }"#,
        )
        .expect("config should parse");

        assert_eq!(config.default, LoadBalancerAlgorithm::GroqMultiregion);
        assert!(matches!(
            config.models.get("model-a"),
            Some(LoadBalancerModelConfig::Name(
                LoadBalancerAlgorithm::RoundRobin
            ))
        ));
    }

    #[test]
    fn detailed_model_config_parses_input_work_admission_limit() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "models": {
                    "model-a": {
                        "algorithm": "power-of-two",
                        "max_input_work_seconds": 2.5
                    }
                }
            }"#,
        )
        .expect("config should parse");

        let detailed = config
            .models
            .get("model-a")
            .cloned()
            .expect("model config should exist")
            .into_algorithm_config();
        assert_eq!(detailed.max_input_work_seconds, Some(2.5));
    }

    #[test]
    fn input_work_seconds_uses_pool_work_and_service_rate() {
        let mut cluster_a = candidate("cluster-a", 1024);
        cluster_a.stats.queued_input_size = 300;
        cluster_a.stats.last_mean_input_tps = 100.0;
        let mut cluster_b = candidate("cluster-b", 1024);
        cluster_b.stats.queued_input_size = 120;
        cluster_b.stats.last_mean_input_tps = 50.0;

        assert_eq!(
            input_work_seconds(&[cluster_a, cluster_b], 30, None),
            Some(3.0)
        );
    }

    #[test]
    fn input_work_seconds_excludes_failed_clusters_and_requires_valid_capacity() {
        let mut excluded = candidate("excluded", 1024);
        excluded.stats.queued_input_size = 300;
        excluded.stats.last_mean_input_tps = 100.0;
        let mut invalid = candidate("invalid", 1024);
        invalid.stats.queued_input_size = 100;
        invalid.stats.last_mean_input_tps = f64::NAN;
        let excluded_ids = HashSet::from(["excluded".to_string()]);

        assert_eq!(
            input_work_seconds(&[excluded, invalid], 30, Some(&excluded_ids)),
            None
        );
    }

    #[test]
    fn input_work_seconds_ignores_decode_only_total_query_input_size() {
        let mut decode_only = candidate("decode-only", 1024);
        decode_only.stats.total_query_input_size = 10_000;
        decode_only.stats.queued_input_size = 0;
        decode_only.stats.last_mean_input_tps = 100.0;

        assert_eq!(input_work_seconds(&[decode_only], 50, None), Some(0.5));
    }

    #[test]
    fn input_work_seconds_for_pulsar_counts_only_request_feasible_capacity() {
        let mut config = LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::Pulsar);
        config.require_kv_metrics = Some(true);
        let target = target();
        let request = request(&target, Some("prefix-a"), Some(100));
        let mut feasible = candidate("feasible", 256);
        feasible.stats.queued_input_size = 50;
        feasible.stats.last_mean_input_tps = 100.0;
        let mut kv_infeasible = candidate("kv-infeasible", 50);
        kv_infeasible.stats.queued_input_size = 900;
        kv_infeasible.stats.last_mean_input_tps = 1000.0;

        assert_eq!(
            input_work_seconds_for_request(&config, &request, &[feasible, kv_infeasible]),
            Some(1.5)
        );
    }

    #[test]
    fn invalid_algorithm_name_fails_during_parse() {
        let err = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "not-a-real-lb"
            }"#,
        )
        .expect_err("config parse should fail for invalid algorithm");

        assert!(
            err.to_string().contains("not-a-real-lb"),
            "unexpected parse error: {err}"
        );
    }

    #[test]
    fn routing_algorithm_override_parses_canonical_algorithm_names_from_algorithm_parser() {
        for algorithm in [
            LoadBalancerAlgorithm::GroqMultiregion,
            LoadBalancerAlgorithm::PowerOfTwo,
            LoadBalancerAlgorithm::Pulsar,
            LoadBalancerAlgorithm::Random,
            LoadBalancerAlgorithm::RoundRobin,
        ] {
            let raw = algorithm.to_string();

            assert_eq!(raw.parse::<LoadBalancerAlgorithm>(), Ok(algorithm));
            assert_eq!(
                LoadBalancerAlgorithmOverride::parse(&raw),
                Ok(LoadBalancerAlgorithmOverride { raw, algorithm })
            );
        }
    }

    #[test]
    fn routing_algorithm_override_parses_header_aliases_for_kebab_case_algorithm_names() {
        for algorithm in [
            LoadBalancerAlgorithm::GroqMultiregion,
            LoadBalancerAlgorithm::PowerOfTwo,
            LoadBalancerAlgorithm::Pulsar,
            LoadBalancerAlgorithm::Random,
            LoadBalancerAlgorithm::RoundRobin,
        ] {
            let raw = algorithm.to_string().replace('-', "_");
            assert_eq!(
                LoadBalancerAlgorithmOverride::parse(&raw),
                Ok(LoadBalancerAlgorithmOverride { raw, algorithm })
            );
        }
    }

    #[test]
    fn routing_algorithm_override_rejects_empty_and_unknown_names() {
        assert_eq!(
            LoadBalancerAlgorithmOverride::parse(""),
            Err(LoadBalancerRoutingAlgorithmError::Unknown { raw: String::new() })
        );
        assert_eq!(
            LoadBalancerAlgorithmOverride::parse("sticky"),
            Err(LoadBalancerRoutingAlgorithmError::Unknown {
                raw: "sticky".to_string()
            })
        );
    }

    #[test]
    fn max_metric_age_config_is_rejected_after_staleness_cleanup() {
        let err = serde_json::from_str::<LoadBalancerAlgorithmConfig>(
            r#"{
                "algorithm": "pulsar",
                "max_metric_age_ms": 10000
            }"#,
        )
        .expect_err("removed metric-age config should fail startup");

        assert!(
            err.to_string().contains("max_metric_age_ms"),
            "unexpected parse error: {err}"
        );
    }

    #[test]
    fn unknown_load_balancer_config_fields_are_rejected() {
        let err = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "legacy_top_level_field": true,
                "models": {
                    "model-a": {
                        "algorithm": "pulsar",
                        "legacy_model_field": 123
                    }
                }
            }"#,
        )
        .expect_err("unknown config fields should fail startup");

        assert!(
            err.to_string().contains("legacy_top_level_field"),
            "unexpected parse error: {err}"
        );
    }

    #[test]
    fn bundled_benchmark_lb_configs_parse() {
        #[derive(serde::Deserialize)]
        struct BenchmarkManifest {
            algorithms: Vec<BenchmarkAlgorithm>,
        }

        #[derive(serde::Deserialize)]
        struct BenchmarkAlgorithm {
            name: String,
            config: serde_json::Value,
        }

        let benches_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../benches");
        let entries = std::fs::read_dir(&benches_dir)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", benches_dir.display()));
        let mut checked = 0usize;

        for entry in entries {
            let entry = entry.expect("failed to read benchmark manifest directory entry");
            let path = entry.path();
            let extension = path.extension().and_then(|extension| extension.to_str());
            if !matches!(extension, Some("yaml" | "yml")) {
                continue;
            }

            let manifest_bytes = std::fs::read(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
            let manifest = serde_yaml::from_slice::<BenchmarkManifest>(&manifest_bytes)
                .unwrap_or_else(|err| panic!("failed to parse {}: {err}", path.display()));

            for algorithm in manifest.algorithms {
                serde_json::from_value::<LoadBalancerConfig>(algorithm.config).unwrap_or_else(
                    |err| {
                        panic!(
                            "{} algorithm {} has invalid load-balancer config: {err}",
                            path.display(),
                            algorithm.name
                        )
                    },
                );
                checked += 1;
            }
        }

        assert!(checked > 0, "expected bundled benchmark LB configs");
    }

    #[test]
    fn detailed_model_config_parses_for_pulsar() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "models": {
                    "model-a": {
                        "algorithm": "pulsar",
                        "seed": "seed-1",
                        "require_cache_affinity_key": true,
                        "require_input_tokens": true,
                        "require_kv_metrics": true
                    }
                }
            }"#,
        )
        .expect("config should parse");

        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let model_config = router.algorithm_config("model-a");
        assert_eq!(model_config.algorithm, LoadBalancerAlgorithm::Pulsar);
        assert_eq!(model_config.seed.as_deref(), Some("seed-1"));
        assert!(model_config.requires_cache_affinity_key());
        assert!(model_config.requires_input_tokens());
        assert!(model_config.requires_kv_metrics());
    }

    #[test]
    fn detailed_model_config_parses_groq_multiregion_cache_affinity() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "models": {
                    "model-a": {
                        "algorithm": "groq-multiregion",
                        "seed": "seed-1",
                        "require_cache_affinity_key": true,
                        "cache_affinity_virtual_nodes": 64,
                        "cache_affinity_backend_selection_count": 2
                    }
                }
            }"#,
        )
        .expect("config should parse");

        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let model_config = router.algorithm_config("model-a");
        assert_eq!(
            model_config.algorithm,
            LoadBalancerAlgorithm::GroqMultiregion
        );
        assert_eq!(model_config.seed.as_deref(), Some("seed-1"));
        assert!(model_config.requires_cache_affinity_key());
        let multiregion_config = GroqMultiregionConfig::from_algorithm_config(model_config);
        assert_eq!(multiregion_config.cache_affinity_virtual_nodes(), 64);
        assert_eq!(
            multiregion_config.cache_affinity_backend_selection_count(),
            Some(2)
        );
    }

    #[test]
    fn request_algorithms_parse_and_override_default_selection() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "request_algorithms": {
                    "round-robin": "round-robin"
                }
            }"#,
        )
        .expect("config should parse");
        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round_robin")
            .expect("routing algorithm override should parse");

        let first = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;
        let second = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;

        assert_eq!(first, "cluster-0");
        assert_eq!(second, "cluster-1");
    }

    #[test]
    fn choose_candidate_returns_slice_index_for_selected_cluster() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "round-robin"
            }"#,
        )
        .expect("config should parse");
        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];

        let first = router
            .choose_candidate(&request, &candidates)
            .expect("candidate should be selected");
        let second = router
            .choose_candidate(&request, &candidates)
            .expect("candidate should be selected");

        assert_eq!(first.candidate_index, 0);
        assert_eq!(first.rank_depth, 1);
        assert_eq!(candidates[first.candidate_index].cluster_id, "cluster-0");
        assert_eq!(second.candidate_index, 1);
        assert_eq!(candidates[second.candidate_index].cluster_id, "cluster-1");
    }

    #[test]
    fn choose_candidate_with_resolution_preserves_algorithm_metadata() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "request_algorithms": {
                    "round-robin": "round-robin"
                }
            }"#,
        )
        .expect("config should parse");
        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round_robin")
            .expect("routing algorithm override should parse");
        let resolution = router
            .resolve_algorithm_override(&target.model_id, Some(&algorithm_override))
            .expect("routing method should be available");

        let selection = router
            .choose_candidate_with_algorithm_resolution(&request, &candidates, &resolution)
            .expect("candidate should be selected");

        assert_eq!(selection.choice.candidate_index, 0);
        assert_eq!(
            selection.effective_algorithm,
            LoadBalancerAlgorithm::RoundRobin
        );
        assert_eq!(
            selection.requested_algorithm.as_deref(),
            Some("round_robin")
        );
    }

    #[test]
    fn model_request_algorithms_override_top_level_request_algorithms() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "request_algorithms": {
                    "round-robin": "round-robin"
                },
                "models": {
                    "model-a": {
                        "algorithm": "power-of-two",
                        "request_algorithms": {
                            "round-robin": {
                                "algorithm": "round-robin",
                                "require_input_tokens": true
                            }
                        }
                    }
                }
            }"#,
        )
        .expect("config should parse");
        let router = LoadBalancerRouter::from_config(&config).expect("router should build");
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round-robin")
            .expect("routing algorithm override should parse");

        let model_config = router
            .resolve_algorithm_override("model-a", Some(&algorithm_override))
            .expect("model routing method should be available");
        let default_config = router
            .resolve_algorithm_override("model-b", Some(&algorithm_override))
            .expect("default routing method should be available");

        assert!(model_config.config().requires_input_tokens());
        assert!(!default_config.config().requires_input_tokens());
    }

    #[test]
    fn request_algorithm_key_must_match_configured_algorithm() {
        let config = serde_json::from_str::<LoadBalancerConfig>(
            r#"{
                "default": "power-of-two",
                "request_algorithms": {
                    "random": "round-robin"
                }
            }"#,
        )
        .expect("config should parse");

        let err = match LoadBalancerRouter::from_config(&config) {
            Ok(_) => panic!("mismatched request algorithm should fail"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("request_algorithms key random"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn groq_multiregion_config_resolves_internal_defaults() {
        let algorithm_config = LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            cache_affinity_virtual_nodes: Some(0),
            cache_affinity_backend_selection_count: Some(0),
            max_queue_time_floor_ms: Some(100),
            max_queue_time_ceil_ms: Some(300),
            n: Some(0),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        };
        let config = GroqMultiregionConfig::from_algorithm_config(&algorithm_config);

        assert_eq!(config.cache_affinity_virtual_nodes(), 1);
        assert_eq!(config.cache_affinity_backend_selection_count(), None);
        assert_eq!(config.ttft_bucket_size(), Duration::from_millis(20));
        assert_eq!(config.next_bucket_unlock_factor(), 0.25);
        assert_eq!(config.sample_count(), 1);
        assert_eq!(config.max_queued(), 0);
        assert!(config.ignore_queue_time());
        assert!(config.ignore_input_processing_time());

        let target = target();
        let request = request(&target, None, Some(0));
        assert_eq!(
            config
                .max_queue_time(&request)
                .expect("floor and ceil should enable queue SLO"),
            Duration::from_millis(300)
        );
    }

    #[test]
    fn router_reports_groq_multiregion_algorithm_name() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: HashMap::new(),
            models: [(
                "model-a".to_string(),
                LoadBalancerModelConfig::Name(LoadBalancerAlgorithm::GroqMultiregion),
            )]
            .into_iter()
            .collect(),
        };

        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        assert_eq!(router.algorithm_name("model-a"), "groq-multiregion");
    }

    #[test]
    fn default_round_robin_uses_independent_sequences_per_model() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let model_a_target = target_with_model("model-a");
        let model_b_target = target_with_model("model-b");
        let model_a_request = request(&model_a_target, None, None);
        let model_b_request = request(&model_b_target, None, None);
        let model_a_candidates = vec![candidate("model-a-0", 1024), candidate("model-a-1", 1024)];
        let model_b_candidates = vec![
            candidate("model-b-0", 1024),
            candidate("model-b-1", 1024),
            candidate("model-b-2", 1024),
        ];
        let mut model_a_selected = Vec::new();
        let mut model_b_selected = Vec::new();

        for _ in 0..3 {
            model_a_selected.push(
                router
                    .choose(&model_a_request, &model_a_candidates)
                    .expect("model-a candidate should be selected")
                    .candidate
                    .cluster_id,
            );
            model_b_selected.push(
                router
                    .choose(&model_b_request, &model_b_candidates)
                    .expect("model-b candidate should be selected")
                    .candidate
                    .cluster_id,
            );
        }

        assert_eq!(
            model_a_selected,
            vec![
                "model-a-0".to_string(),
                "model-a-1".to_string(),
                "model-a-0".to_string()
            ]
        );
        assert_eq!(
            model_b_selected,
            vec![
                "model-b-0".to_string(),
                "model-b-1".to_string(),
                "model-b-2".to_string()
            ]
        );
    }

    #[test]
    fn default_round_robin_uses_independent_sequences_per_routing_target() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let tenant_a_target = target_with_routing_key("tenant-a", "shared-model");
        let tenant_b_target = target_with_routing_key("tenant-b", "shared-model");
        let tenant_a_request = request(&tenant_a_target, None, None);
        let tenant_b_request = request(&tenant_b_target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let mut tenant_a_selected = Vec::new();
        let mut tenant_b_selected = Vec::new();

        for _ in 0..2 {
            tenant_a_selected.push(
                router
                    .choose(&tenant_a_request, &candidates)
                    .expect("tenant-a candidate should be selected")
                    .candidate
                    .cluster_id,
            );
            tenant_b_selected.push(
                router
                    .choose(&tenant_b_request, &candidates)
                    .expect("tenant-b candidate should be selected")
                    .candidate
                    .cluster_id,
            );
        }

        assert_eq!(
            tenant_a_selected,
            vec!["cluster-0".to_string(), "cluster-1".to_string()]
        );
        assert_eq!(
            tenant_b_selected,
            vec!["cluster-0".to_string(), "cluster-1".to_string()]
        );
    }

    #[test]
    fn configured_round_robin_uses_independent_sequences_per_routing_target() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: HashMap::new(),
            models: [(
                "shared-model".to_string(),
                LoadBalancerModelConfig::Name(LoadBalancerAlgorithm::RoundRobin),
            )]
            .into_iter()
            .collect(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let tenant_a_target = target_with_routing_key("tenant-a", "shared-model");
        let tenant_b_target = target_with_routing_key("tenant-b", "shared-model");
        let tenant_a_request = request(&tenant_a_target, None, None);
        let tenant_b_request = request(&tenant_b_target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let mut tenant_a_selected = Vec::new();
        let mut tenant_b_selected = Vec::new();

        for _ in 0..2 {
            tenant_a_selected.push(
                router
                    .choose(&tenant_a_request, &candidates)
                    .expect("tenant-a candidate should be selected")
                    .candidate
                    .cluster_id,
            );
            tenant_b_selected.push(
                router
                    .choose(&tenant_b_request, &candidates)
                    .expect("tenant-b candidate should be selected")
                    .candidate
                    .cluster_id,
            );
        }

        assert_eq!(
            tenant_a_selected,
            vec!["cluster-0".to_string(), "cluster-1".to_string()]
        );
        assert_eq!(
            tenant_b_selected,
            vec!["cluster-0".to_string(), "cluster-1".to_string()]
        );
    }

    #[test]
    fn choose_with_no_candidates_does_not_cache_default_lb_for_target() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("unknown-model");
        let request = request(&target, None, None);
        let empty_candidates: Vec<RoutedClusterSnapshot> = Vec::new();

        assert!(router.choose(&request, &empty_candidates).is_none());
        assert_eq!(router.default_per_target.len(), 0);
    }

    #[test]
    fn request_round_robin_override_uses_stable_per_target_sequence() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: request_algorithm_map(&[LoadBalancerAlgorithm::RoundRobin]),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round-robin")
            .expect("routing algorithm override should parse");
        let mut selected = Vec::new();

        for _ in 0..3 {
            selected.push(
                router
                    .choose_with_algorithm_override(
                        &request,
                        &candidates,
                        Some(&algorithm_override),
                    )
                    .expect("routing method should be available")
                    .expect("candidate should be selected")
                    .choice
                    .candidate
                    .cluster_id,
            );
        }

        assert_eq!(
            selected,
            vec![
                "cluster-0".to_string(),
                "cluster-1".to_string(),
                "cluster-0".to_string()
            ]
        );
        assert_eq!(router.request_per_target.len(), 1);
    }

    #[test]
    fn configured_request_override_caches_per_target_balancer() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: request_algorithm_map(&[LoadBalancerAlgorithm::PowerOfTwo]),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("power-of-two")
            .expect("routing algorithm override should parse");

        let selection = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected");

        assert_eq!(
            selection.effective_algorithm,
            LoadBalancerAlgorithm::PowerOfTwo
        );
        assert_eq!(router.request_per_target.len(), 1);
    }

    #[test]
    fn matching_round_robin_override_reuses_configured_target_sequence() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("model-a");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round-robin")
            .expect("routing algorithm override should parse");

        let without_header = router
            .choose(&request, &candidates)
            .expect("candidate should be selected")
            .candidate
            .cluster_id;
        let with_header = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;
        let without_header_again = router
            .choose(&request, &candidates)
            .expect("candidate should be selected")
            .candidate
            .cluster_id;

        assert_eq!(without_header, "cluster-0");
        assert_eq!(with_header, "cluster-1");
        assert_eq!(without_header_again, "cluster-0");
    }

    #[test]
    fn request_round_robin_override_keeps_routing_targets_isolated() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: request_algorithm_map(&[LoadBalancerAlgorithm::RoundRobin]),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target_a = target_with_routing_key("rk-a", "shared-model");
        let target_b = target_with_routing_key("rk-b", "shared-model");
        let request_a = request(&target_a, None, None);
        let request_b = request(&target_b, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round-robin")
            .expect("routing algorithm override should parse");

        let first_a = router
            .choose_with_algorithm_override(&request_a, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;
        let first_b = router
            .choose_with_algorithm_override(&request_b, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;
        let second_a = router
            .choose_with_algorithm_override(&request_a, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;
        let second_b = router
            .choose_with_algorithm_override(&request_b, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected")
            .choice
            .candidate
            .cluster_id;

        assert_eq!(first_a, "cluster-0");
        assert_eq!(first_b, "cluster-0");
        assert_eq!(second_a, "cluster-1");
        assert_eq!(second_b, "cluster-1");
    }

    #[test]
    fn request_override_beats_configured_model_algorithm() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: request_algorithm_map(&[LoadBalancerAlgorithm::PowerOfTwo]),
            models: [(
                "shared-model".to_string(),
                LoadBalancerModelConfig::Name(LoadBalancerAlgorithm::RoundRobin),
            )]
            .into_iter()
            .collect(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("shared-model");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("power_of_two")
            .expect("routing algorithm override should parse");

        let selection = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect("routing method should be available")
            .expect("candidate should be selected");

        assert_eq!(
            selection.effective_algorithm,
            LoadBalancerAlgorithm::PowerOfTwo
        );
    }

    #[test]
    fn matching_request_override_reuses_configured_algorithm_config() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: HashMap::new(),
            models: [(
                "shared-model".to_string(),
                LoadBalancerModelConfig::Detailed(Box::new(LoadBalancerAlgorithmConfig {
                    algorithm: LoadBalancerAlgorithm::RoundRobin,
                    require_input_tokens: Some(true),
                    ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::RoundRobin)
                })),
            )]
            .into_iter()
            .collect(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("round-robin")
            .expect("routing algorithm override should parse");

        let config = router
            .resolve_algorithm_override("shared-model", Some(&algorithm_override))
            .expect("routing method should be available");

        assert_eq!(config.config().algorithm, LoadBalancerAlgorithm::RoundRobin);
        assert!(config.config().requires_input_tokens());
    }

    #[test]
    fn matching_model_algorithm_beats_top_level_request_config() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: request_algorithm_map(&[LoadBalancerAlgorithm::Pulsar]),
            models: [(
                "shared-model".to_string(),
                LoadBalancerModelConfig::Detailed(Box::new(LoadBalancerAlgorithmConfig {
                    algorithm: LoadBalancerAlgorithm::Pulsar,
                    require_input_tokens: Some(true),
                    ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::Pulsar)
                })),
            )]
            .into_iter()
            .collect(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("pulsar")
            .expect("routing algorithm override should parse");

        let config = router
            .resolve_algorithm_override("shared-model", Some(&algorithm_override))
            .expect("routing algorithm should resolve");

        assert_eq!(config.config().algorithm, LoadBalancerAlgorithm::Pulsar);
        assert!(config.config().requires_input_tokens());
    }

    #[test]
    fn known_unavailable_request_override_returns_error() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::PowerOfTwo,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("shared-model");
        let request = request(&target, None, None);
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];
        let algorithm_override = LoadBalancerAlgorithmOverride::parse("pulsar")
            .expect("routing algorithm override should parse");

        let error = router
            .choose_with_algorithm_override(&request, &candidates, Some(&algorithm_override))
            .expect_err("unconfigured routing method should fail");
        assert_eq!(
            error,
            LoadBalancerRoutingAlgorithmError::Unavailable {
                raw: "pulsar".to_string(),
                algorithm: LoadBalancerAlgorithm::Pulsar,
            }
        );
        assert!(
            router
                .resolve_algorithm_override("shared-model", Some(&algorithm_override))
                .is_err()
        );
    }

    #[test]
    fn unknown_request_override_returns_error() {
        assert_eq!(
            LoadBalancerAlgorithmOverride::parse("sticky")
                .expect_err("unknown routing algorithm should fail"),
            LoadBalancerRoutingAlgorithmError::Unknown {
                raw: "sticky".to_string()
            }
        );
    }

    #[test]
    fn request_excluded_clusters_are_not_selected() {
        let config = LoadBalancerConfig {
            default: LoadBalancerAlgorithm::RoundRobin,
            request_algorithms: HashMap::new(),
            models: HashMap::new(),
        };
        let router = LoadBalancerRouter::from_config(&config).expect("router config should parse");
        let target = target_with_model("model-exclusions");
        let failed_clusters = HashSet::from(["cluster-0".to_string()]);
        let request = LoadBalancerRequest {
            excluded_cluster_ids: Some(&failed_clusters),
            ..request(&target, None, None)
        };
        let candidates = vec![candidate("cluster-0", 1024), candidate("cluster-1", 1024)];

        let chosen = router
            .choose(&request, &candidates)
            .expect("non-excluded candidate should be selected");

        assert_eq!(chosen.candidate.cluster_id, "cluster-1");
    }

    #[test]
    fn groq_multiregion_prefers_lower_estimated_ttft() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(10));
        let mut fast = candidate("fast", 1024);
        fast.rtt = Duration::from_millis(5);
        let mut slow = candidate("slow", 1024);
        slow.rtt = Duration::from_millis(50);

        let chosen = lb
            .choose(&request, &[fast, slow])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "fast");
    }

    #[test]
    fn groq_multiregion_single_excluded_cluster_is_not_selected() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let excluded = HashSet::from(["fast-but-excluded".to_string()]);
        let request = LoadBalancerRequest {
            excluded_cluster_ids: Some(&excluded),
            ..request(&target, None, Some(10))
        };
        let mut fast = candidate("fast-but-excluded", 1024);
        fast.rtt = Duration::from_millis(5);
        let mut slow = candidate("slow-but-eligible", 1024);
        slow.rtt = Duration::from_millis(50);

        let chosen = lb
            .choose(&request, &[fast, slow])
            .expect("eligible candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "slow-but-eligible");
    }

    #[test]
    fn groq_multiregion_cache_affinity_key_selects_stable_primary() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, Some("prefix-a"), Some(1));
        let mut candidates = vec![
            candidate("affinity-a", 1024),
            candidate("affinity-b", 1024),
            candidate("affinity-c", 1024),
        ];
        for candidate in &mut candidates {
            candidate.rtt = Duration::from_millis(5);
        }

        let first = lb
            .choose(&request, &candidates)
            .expect("candidate should be selected")
            .candidate
            .cluster_id;
        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &candidates)
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, first);
        }
    }

    #[test]
    fn groq_multiregion_cache_affinity_retry_skips_excluded_primary() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(32),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let lb = GroqMultiregionLoadBalancer::new(config.clone());
        let target = target();
        let mut excluded_primary = candidate("excluded-primary", 1024);
        excluded_primary.rtt = Duration::from_millis(5);
        let mut affinity_successor = candidate("affinity-successor", 1024);
        affinity_successor.rtt = Duration::from_millis(100);
        let mut global_fast = candidate("global-fast", 1024);
        global_fast.rtt = Duration::from_millis(1);
        let candidates = vec![excluded_primary, affinity_successor, global_fast];
        let excluded = HashSet::from(["excluded-primary".to_string()]);

        for idx in 0..8192 {
            let key = format!("retry-prefix-{idx}");
            let base_request = request(&target, Some(&key), Some(1));
            let selected = cache_affinity_candidates(&config, &base_request, &candidates)
                .expect("cache affinity should select a backend");
            if selected[0].cluster_id != "excluded-primary" {
                continue;
            }
            let retry_request = LoadBalancerRequest {
                excluded_cluster_ids: Some(&excluded),
                ..base_request
            };
            let selected_after_exclusion =
                cache_affinity_candidates(&config, &retry_request, &candidates)
                    .expect("retry should select an affinity successor");
            if selected_after_exclusion[0].cluster_id != "affinity-successor" {
                continue;
            }

            let _ = lb
                .choose(&request(&target, Some(&key), Some(1)), &candidates)
                .expect("initial affinity primary should be selected");
            let chosen = lb
                .choose(&retry_request, &candidates)
                .expect("retry should select a non-excluded candidate");

            assert_eq!(chosen.candidate.cluster_id, "affinity-successor");
            let cached_key_bytes = lb.cached_affinity_key_bytes();
            assert!(cached_key_bytes >= key.len() * 2 + "excluded-primary".len());

            let chosen_again = lb
                .choose(&retry_request, &candidates)
                .expect("cached retry should select a non-excluded candidate");
            assert_eq!(chosen_again.candidate.cluster_id, "affinity-successor");
            assert_eq!(lb.cached_affinity_key_bytes(), cached_key_bytes);
            return;
        }

        panic!("expected to find an affinity key with a distinct excluded primary and successor");
    }

    #[test]
    fn groq_multiregion_cache_affinity_retry_skips_multiple_excluded_primaries() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(32),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let lb = GroqMultiregionLoadBalancer::new(config.clone());
        let target = target();
        let mut excluded_a = candidate("excluded-a", 1024);
        excluded_a.rtt = Duration::from_millis(5);
        let mut excluded_b = candidate("excluded-b", 1024);
        excluded_b.rtt = Duration::from_millis(10);
        let mut affinity_successor = candidate("affinity-successor", 1024);
        affinity_successor.rtt = Duration::from_millis(100);
        let mut global_fast = candidate("global-fast", 1024);
        global_fast.rtt = Duration::from_millis(1);
        let candidates = vec![excluded_a, excluded_b, affinity_successor, global_fast];
        let excluded = HashSet::from(["excluded-a".to_string(), "excluded-b".to_string()]);

        for idx in 0..8192 {
            let key = format!("retry-prefix-{idx}");
            let base_request = request(&target, Some(&key), Some(1));
            let selected = cache_affinity_candidates(&config, &base_request, &candidates)
                .expect("cache affinity should select a backend");
            if selected[0].cluster_id != "excluded-a" {
                continue;
            }
            let retry_request = LoadBalancerRequest {
                excluded_cluster_ids: Some(&excluded),
                ..base_request
            };
            let selected_after_exclusion =
                cache_affinity_candidates(&config, &retry_request, &candidates)
                    .expect("retry should select an affinity successor");
            if selected_after_exclusion[0].cluster_id != "affinity-successor" {
                continue;
            }

            let _ = lb
                .choose(&request(&target, Some(&key), Some(1)), &candidates)
                .expect("initial affinity primary should be selected");
            let chosen = lb
                .choose(&retry_request, &candidates)
                .expect("retry should select a non-excluded candidate");

            assert_eq!(chosen.candidate.cluster_id, "affinity-successor");
            let cached_key_bytes = lb.cached_affinity_key_bytes();
            assert!(cached_key_bytes >= key.len() * 2 + "excluded-a".len() + "excluded-b".len());

            let chosen_again = lb
                .choose(&retry_request, &candidates)
                .expect("cached retry should select a non-excluded candidate");
            assert_eq!(chosen_again.candidate.cluster_id, "affinity-successor");
            assert_eq!(lb.cached_affinity_key_bytes(), cached_key_bytes);
            return;
        }

        panic!(
            "expected to find an affinity key with two excluded primaries and a distinct successor"
        );
    }

    #[test]
    fn groq_multiregion_affinity_cache_invalidates_when_candidates_change() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let lb = GroqMultiregionLoadBalancer::new(config.clone());
        let target = target();
        let first_candidates = vec![
            candidate("old-a", 1024),
            candidate("old-b", 1024),
            candidate("old-c", 1024),
        ];

        for idx in 0..512 {
            let key = format!("prefix-{idx}");
            let request = request(&target, Some(&key), Some(1));
            let selected = cache_affinity_candidates(&config, &request, &first_candidates)
                .expect("cache affinity should select a backend");
            if selected[0].cluster_id == "old-a" {
                continue;
            }

            let _ = lb
                .choose(&request, &first_candidates)
                .expect("initial candidate should be selected");
            let replacement = vec![candidate("replacement", 1024)];
            let chosen = lb
                .choose(&request, &replacement)
                .expect("replacement candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "replacement");
            return;
        }

        panic!("expected to find an affinity key that selects a non-zero candidate index");
    }

    #[test]
    fn groq_multiregion_does_not_cache_oversized_affinity_key() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let lb = GroqMultiregionLoadBalancer::new(config);
        let target = target();
        let oversized_key = "x".repeat(MAX_CACHE_AFFINITY_CACHE_KEY_BYTES + 1);
        let request = request(&target, Some(&oversized_key), Some(1));
        let candidates = vec![
            candidate("large-key-a", 1024),
            candidate("large-key-b", 1024),
        ];

        let choice = lb
            .choose(&request, &candidates)
            .expect("oversized affinity key should still route");

        assert!(choice.candidate.cluster_id.starts_with("large-key-"));
        assert_eq!(lb.cached_affinity_key_bytes(), 0);
    }

    #[test]
    fn groq_multiregion_cache_affinity_hash_keeps_legacy_single_backend_wire_format() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let target = target();
        let request = request(&target, Some("prefix-a"), Some(1));
        let candidate = candidate("inst-a", 1024);

        let hash = cache_affinity_virtual_node_hash(&config, &request, &candidate, 7);

        let mut bytes = Vec::new();
        bytes.push(1);
        append_test_tagged_bytes(&mut bytes, b"seed", b"seed-1");
        append_test_tagged_bytes(&mut bytes, b"routing_key", b"rk-1");
        append_test_tagged_bytes(&mut bytes, b"model_id", b"model-a");
        append_test_tagged_bytes(&mut bytes, b"inference_server_id", b"inst-a");
        append_test_tagged_bytes(&mut bytes, b"virtual_node", &7usize.to_le_bytes());

        assert_eq!(hash, xxh3_64(&bytes));
    }

    #[test]
    fn groq_multiregion_cache_affinity_hash_changes_with_routing_key() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let target_a = target_with_routing_key("tenant-a", "shared-model");
        let target_b = target_with_routing_key("tenant-b", "shared-model");
        let request_a = request(&target_a, Some("same-prefix"), Some(1));
        let request_b = request(&target_b, Some("same-prefix"), Some(1));
        let candidate = candidate("inst-a", 1024);

        let hash_a = cache_affinity_virtual_node_hash(&config, &request_a, &candidate, 7);
        let hash_b = cache_affinity_virtual_node_hash(&config, &request_b, &candidate, 7);

        assert_ne!(
            hash_a, hash_b,
            "routing_key must be part of the affinity hash namespace"
        );
    }

    #[test]
    fn groq_multiregion_cache_affinity_falls_back_when_primary_is_full() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(1),
            cache_affinity_backend_selection_count: Some(1),
            n: Some(3),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, Some("prefix-a"), Some(1));
        let mut candidates = vec![
            candidate("fallback-a", 1024),
            candidate("fallback-b", 1024),
            candidate("fallback-c", 1024),
        ];
        let primary_config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(1),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let primary = cache_affinity_candidates(&primary_config, &request, &candidates)
            .expect("cache affinity should select a primary")[0]
            .cluster_id
            .clone();
        for candidate in &mut candidates {
            if candidate.cluster_id == primary {
                candidate.stats.max_engine_concurrency = 1;
                candidate.stats.num_running_queries = 1;
            }
        }

        let chosen = lb
            .choose(&request, &candidates)
            .expect("fallback candidate should be selected");
        assert_ne!(chosen.candidate.cluster_id, primary);
    }

    #[test]
    fn groq_multiregion_two_affinity_candidates_still_filter_capacity() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(8),
            cache_affinity_backend_selection_count: Some(2),
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let lb = GroqMultiregionLoadBalancer::new(config.clone());
        let target = target();
        let request = request(&target, Some("prefix-a"), Some(1));
        let mut candidates = vec![
            candidate("two-affinity-a", 1024),
            candidate("two-affinity-b", 1024),
            candidate("two-affinity-c", 1024),
        ];
        let selected = cache_affinity_candidates(&config, &request, &candidates)
            .expect("cache affinity should select candidates");
        assert_eq!(selected.len(), 2);
        let full_primary_id = selected[0].cluster_id.clone();
        let available_selected_id = selected[1].cluster_id.clone();
        for candidate in &mut candidates {
            if candidate.cluster_id == full_primary_id {
                candidate.stats.max_engine_concurrency = 1;
                candidate.stats.num_running_queries = 1;
            }
        }

        let chosen = lb
            .choose(&request, &candidates)
            .expect("available affinity candidate should be selected");

        assert_eq!(chosen.candidate.cluster_id, available_selected_id);
    }

    #[test]
    fn groq_multiregion_cache_affinity_keys_distribute_across_backends() {
        let config = multiregion_runtime_config(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(32),
            cache_affinity_backend_selection_count: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        });
        let target = target();
        let candidates = vec![
            candidate("dist-a", 1024),
            candidate("dist-b", 1024),
            candidate("dist-c", 1024),
        ];
        let mut seen = HashSet::new();

        for idx in 0..128 {
            let key = format!("prefix-{idx}");
            let request = request(&target, Some(&key), Some(1));
            let selected = cache_affinity_candidates(&config, &request, &candidates)
                .expect("cache affinity should select a backend");
            seen.insert(selected[0].cluster_id.clone());
            if seen.len() >= 2 {
                break;
            }
        }

        assert!(
            seen.len() >= 2,
            "expected different cache-affinity keys to reach multiple primaries, saw {seen:?}"
        );
    }

    #[test]
    fn groq_multiregion_cache_affinity_is_skipped_without_header() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: Some(1),
            cache_affinity_backend_selection_count: Some(1),
            n: Some(3),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(1));
        let mut affinity_primary = candidate("affinity-primary", 1024);
        affinity_primary.rtt = Duration::from_millis(50);
        let mut fastest = candidate("fastest-without-affinity", 1024);
        fastest.rtt = Duration::from_millis(5);

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[affinity_primary.clone(), fastest.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "fastest-without-affinity");
        }
    }

    #[test]
    fn groq_multiregion_uses_input_tokens_in_ttft_estimate() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(100));
        let mut higher_rtt_higher_cap = candidate("higher-rtt-higher-cap", 1024);
        higher_rtt_higher_cap.rtt = Duration::from_millis(10);
        higher_rtt_higher_cap.stats.last_mean_input_tps = 200.0;
        let mut lower_rtt_lower_cap = candidate("lower-rtt-lower-cap", 1024);
        lower_rtt_lower_cap.rtt = Duration::from_millis(1);
        lower_rtt_lower_cap.stats.last_mean_input_tps = 10.0;

        let chosen = lb
            .choose(&request, &[higher_rtt_higher_cap, lower_rtt_lower_cap])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "higher-rtt-higher-cap");
    }

    #[test]
    fn groq_multiregion_can_ignore_input_processing_time_in_ttft_estimate() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(100));
        let mut lower_rtt_lower_cap = candidate("lower-rtt-lower-cap", 1024);
        lower_rtt_lower_cap.rtt = Duration::from_millis(1);
        lower_rtt_lower_cap.stats.last_mean_input_tps = 10.0;
        let mut higher_rtt_higher_cap = candidate("higher-rtt-higher-cap", 1024);
        higher_rtt_higher_cap.rtt = Duration::from_millis(50);
        higher_rtt_higher_cap.stats.last_mean_input_tps = 200.0;

        let chosen = lb
            .choose(&request, &[lower_rtt_lower_cap, higher_rtt_higher_cap])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "lower-rtt-lower-cap");
    }

    #[test]
    fn groq_multiregion_limits_selection_to_first_ttft_bucket() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(1));
        let mut bucket_a = candidate("bucket-a", 1024);
        bucket_a.rtt = Duration::from_millis(5);
        bucket_a.stats.last_mean_input_tps = 10.0;
        let mut bucket_b = candidate("bucket-b", 1024);
        bucket_b.rtt = Duration::from_millis(20);
        bucket_b.stats.last_mean_input_tps = 10.0;
        let mut second_bucket = candidate("second-bucket", 1024);
        second_bucket.rtt = Duration::from_millis(50);
        second_bucket.stats.last_mean_input_tps = 10.0;

        for _ in 0..32 {
            let chosen = lb
                .choose(
                    &request,
                    &[bucket_a.clone(), bucket_b.clone(), second_bucket.clone()],
                )
                .expect("candidate should be selected");
            assert_ne!(chosen.candidate.cluster_id, "second-bucket");
        }
    }

    #[test]
    fn groq_multiregion_can_ignore_queue_time_in_ttft_estimate() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            ignore_queue_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(0));
        let mut lower_rtt_higher_queue = candidate("lower-rtt-higher-queue", 1024);
        lower_rtt_higher_queue.rtt = Duration::from_millis(5);
        lower_rtt_higher_queue.stats.last_mean_input_tps = 100.0;
        lower_rtt_higher_queue.stats.queued_input_size = 100;
        let mut higher_rtt_lower_queue = candidate("higher-rtt-lower-queue", 1024);
        higher_rtt_lower_queue.rtt = Duration::from_millis(50);
        higher_rtt_lower_queue.stats.last_mean_input_tps = 100.0;

        let chosen = lb
            .choose(&request, &[lower_rtt_higher_queue, higher_rtt_lower_queue])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "lower-rtt-higher-queue");
    }

    #[test]
    fn groq_multiregion_ignore_queue_still_compares_sampled_candidates_by_queue_time() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(512));
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(5);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(5);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[higher_queue.clone(), lower_queue.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_ignore_queue_keeps_later_prefill_buckets_locked() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(512));
        let mut first_bucket = candidate("first-bucket", 1024);
        first_bucket.rtt = Duration::from_millis(5);
        first_bucket.stats.last_mean_input_tps = 10_000.0;
        let mut later_bucket = candidate("later-bucket", 1024);
        later_bucket.rtt = Duration::from_millis(50);
        later_bucket.stats.last_mean_input_tps = 10_000.0;

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[first_bucket.clone(), later_bucket.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "first-bucket");
        }
    }

    #[test]
    fn groq_multiregion_ignore_queue_skips_single_excluded_backend() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let excluded = HashSet::from(["excluded".to_string()]);
        let mut request = request(&target, None, Some(512));
        request.excluded_cluster_ids = Some(&excluded);
        let mut excluded_backend = candidate("excluded", 1024);
        excluded_backend.rtt = Duration::from_millis(5);
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(50);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(50);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        excluded_backend.clone(),
                        higher_queue.clone(),
                        lower_queue.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_ignore_queue_skips_multiple_excluded_backends() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let excluded = HashSet::from(["excluded-a".to_string(), "excluded-b".to_string()]);
        let mut request = request(&target, None, Some(512));
        request.excluded_cluster_ids = Some(&excluded);
        let mut excluded_a = candidate("excluded-a", 1024);
        excluded_a.rtt = Duration::from_millis(5);
        let mut excluded_b = candidate("excluded-b", 1024);
        excluded_b.rtt = Duration::from_millis(5);
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(50);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(50);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        excluded_a.clone(),
                        excluded_b.clone(),
                        higher_queue.clone(),
                        lower_queue.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_deprioritizes_non_finite_ttft_candidates() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(10));
        let finite = candidate("finite", 1024);
        let mut non_finite = candidate("non-finite", 1024);
        non_finite.rtt = Duration::from_millis(1);
        non_finite.stats.last_mean_input_tps = 0.0;
        non_finite.stats.queued_input_size = 5;

        let chosen = lb
            .choose(&request, &[finite, non_finite])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "finite");
    }

    #[test]
    fn groq_multiregion_uses_last_mean_input_tps_for_prefill_estimates() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(100));
        let mut high_capacity = candidate("high-capacity", 1024);
        high_capacity.rtt = Duration::from_millis(10);
        high_capacity.stats.last_mean_input_tps = 200.0;
        let mut low_capacity = candidate("low-capacity", 1024);
        low_capacity.rtt = Duration::from_millis(1);
        low_capacity.stats.last_mean_input_tps = 10.0;

        let chosen = lb
            .choose(&request, &[high_capacity, low_capacity])
            .expect("candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "high-capacity");
    }

    #[test]
    fn groq_multiregion_unlocks_later_ttft_bucket_after_waiting() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(1),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = LoadBalancerRequest {
            routing_target: &target,
            cache_affinity_key: None,
            input_tokens: Some(1),
            priority: 0,
            received_at: Instant::now() - Duration::from_millis(20),
            request_slo: None,
            excluded_cluster_ids: None,
        };
        let mut fast_full = candidate("fast-full", 1024);
        fast_full.rtt = Duration::from_millis(20);
        fast_full.stats.max_engine_concurrency = 1;
        fast_full.stats.num_running_queries = 1;
        let mut slower_available = candidate("slower-available", 1024);
        slower_available.rtt = Duration::from_millis(60);
        slower_available.stats.max_engine_concurrency = 1;

        let chosen = lb
            .choose(&request, &[fast_full, slower_available])
            .expect("later bucket should unlock and provide a candidate");
        assert_eq!(chosen.candidate.cluster_id, "slower-available");
    }

    #[test]
    fn groq_multiregion_filters_full_backends() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig::from(
            LoadBalancerAlgorithm::GroqMultiregion,
        ))
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(1));
        let mut full = candidate("full", 1024);
        full.rtt = Duration::from_millis(5);
        full.stats.max_engine_concurrency = 1;
        full.stats.num_running_queries = 1;
        let mut available = candidate("available", 1024);
        available.rtt = Duration::from_millis(6);
        available.stats.max_engine_concurrency = 1;

        let chosen = lb
            .choose(&request, &[full, available])
            .expect("available candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "available");
    }

    #[test]
    fn groq_multiregion_filters_backends_over_queue_slo() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(5),
            max_queue_time_ceil_ms: Some(5),
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(0));
        let mut over_slo = candidate("over-slo", 1024);
        over_slo.stats.last_mean_input_tps = 100.0;
        over_slo.stats.queued_input_size = 1;
        let mut under_slo = candidate("under-slo", 1024);
        under_slo.stats.last_mean_input_tps = 100.0;
        under_slo.stats.queued_input_size = 0;

        let chosen = lb
            .choose(&request, &[over_slo, under_slo])
            .expect("under-SLO candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "under-slo");
    }

    #[test]
    fn groq_multiregion_filters_queue_slo_before_ttft_bucket_locking() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(5),
            max_queue_time_ceil_ms: Some(5),
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(0));
        let mut over_slo_first_bucket = candidate("over-slo-first-bucket", 1024);
        over_slo_first_bucket.rtt = Duration::from_millis(1);
        over_slo_first_bucket.stats.last_mean_input_tps = 100.0;
        over_slo_first_bucket.stats.queued_input_size = 1;
        let mut under_slo_later_bucket = candidate("under-slo-later-bucket", 1024);
        under_slo_later_bucket.rtt = Duration::from_millis(40);
        under_slo_later_bucket.stats.last_mean_input_tps = 100.0;

        let chosen = lb
            .choose(&request, &[over_slo_first_bucket, under_slo_later_bucket])
            .expect("under-SLO candidate in later TTFT bucket should be selected");
        assert_eq!(chosen.candidate.cluster_id, "under-slo-later-bucket");
    }

    #[test]
    fn groq_multiregion_queue_slo_still_applies_when_queue_time_is_ignored_for_ttft() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            ignore_queue_time: Some(true),
            max_queue_time_floor_ms: Some(5),
            max_queue_time_ceil_ms: Some(5),
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(0));
        let mut over_slo_lower_rtt = candidate("over-slo-lower-rtt", 1024);
        over_slo_lower_rtt.rtt = Duration::from_millis(5);
        over_slo_lower_rtt.stats.last_mean_input_tps = 100.0;
        over_slo_lower_rtt.stats.queued_input_size = 1;
        let mut under_slo_higher_rtt = candidate("under-slo-higher-rtt", 1024);
        under_slo_higher_rtt.rtt = Duration::from_millis(6);
        under_slo_higher_rtt.stats.last_mean_input_tps = 100.0;

        let chosen = lb
            .choose(&request, &[over_slo_lower_rtt, under_slo_higher_rtt])
            .expect("under-SLO candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "under-slo-higher-rtt");
    }

    #[test]
    fn groq_multiregion_returns_none_when_only_candidate_exceeds_queue_slo() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(5),
            max_queue_time_ceil_ms: Some(5),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(0));
        let mut over_slo = candidate("over-slo", 1024);
        over_slo.stats.last_mean_input_tps = 100.0;
        over_slo.stats.queued_input_size = 1;

        let choice = lb.choose(&request, &[over_slo]);
        assert!(choice.is_none());
    }

    #[test]
    fn max_queue_time_interpolates_between_floor_and_ceil() {
        let config = LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(100),
            max_queue_time_ceil_ms: Some(300),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        };
        let target = target();
        let request = LoadBalancerRequest {
            routing_target: &target,
            cache_affinity_key: None,
            input_tokens: Some(0),
            priority: 0,
            received_at: Instant::now() - Duration::from_millis(500),
            request_slo: Some(Duration::from_millis(1000)),
            excluded_cluster_ids: None,
        };

        let config = GroqMultiregionConfig::from_algorithm_config(&config);
        let max_queue_time = config
            .max_queue_time(&request)
            .expect("floor and ceil should enable max queue time");
        assert!(
            (200..=205).contains(&max_queue_time.as_millis()),
            "expected roughly 200ms max queue time, got {max_queue_time:?}"
        );
    }

    #[test]
    fn max_queue_time_uses_ceil_when_request_slo_is_missing() {
        let config = LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(100),
            max_queue_time_ceil_ms: Some(300),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        };
        let target = target();
        let request = request(&target, None, Some(0));

        let config = GroqMultiregionConfig::from_algorithm_config(&config);
        assert_eq!(
            config
                .max_queue_time(&request)
                .expect("floor and ceil should enable max queue time"),
            Duration::from_millis(300)
        );
    }

    #[test]
    fn max_queue_time_uses_ceil_when_request_slo_is_zero() {
        let config = LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            max_queue_time_floor_ms: Some(100),
            max_queue_time_ceil_ms: Some(300),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        };
        let target = target();
        let request = LoadBalancerRequest {
            routing_target: &target,
            cache_affinity_key: None,
            input_tokens: Some(0),
            priority: 0,
            received_at: Instant::now(),
            request_slo: Some(Duration::ZERO),
            excluded_cluster_ids: None,
        };

        let config = GroqMultiregionConfig::from_algorithm_config(&config);
        assert_eq!(
            config
                .max_queue_time(&request)
                .expect("floor and ceil should enable max queue time"),
            Duration::from_millis(300)
        );
    }

    #[test]
    fn legacy_queue_slo_ms_configures_fixed_max_queue_time() {
        let config = LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            queue_slo_ms: Some(75),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        };
        let target = target();
        let request = request(&target, None, Some(0));

        let config = GroqMultiregionConfig::from_algorithm_config(&config);
        assert_eq!(
            config
                .max_queue_time(&request)
                .expect("legacy queue_slo_ms should enable max queue time"),
            Duration::from_millis(75)
        );
    }

    #[test]
    fn groq_multiregion_compares_by_queue_time_within_unlocked_bucket() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(1));
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(5);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(5);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[higher_queue.clone(), lower_queue.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_rtt_only_still_compares_sampled_candidates_by_queue_time() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(512));
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(5);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(5);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[higher_queue.clone(), lower_queue.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_rtt_only_filters_full_backends_before_sampling() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(512));
        let mut full = candidate("full", 1024);
        full.rtt = Duration::from_millis(5);
        full.stats.max_engine_concurrency = 1;
        full.stats.num_running_queries = 1;
        let mut available = candidate("available", 1024);
        available.rtt = Duration::from_millis(5);
        available.stats.max_engine_concurrency = 1;

        let chosen = lb
            .choose(&request, &[full, available])
            .expect("available candidate should be selected");
        assert_eq!(chosen.candidate.cluster_id, "available");
    }

    #[test]
    fn groq_multiregion_rtt_only_keeps_later_rtt_buckets_locked() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request(&target, None, Some(512));
        let mut first_bucket = candidate("first-bucket", 1024);
        first_bucket.rtt = Duration::from_millis(5);
        let mut later_bucket = candidate("later-bucket", 1024);
        later_bucket.rtt = Duration::from_millis(50);

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[first_bucket.clone(), later_bucket.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "first-bucket");
        }
    }

    #[test]
    fn groq_multiregion_rtt_only_skips_single_excluded_backend() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let excluded = HashSet::from(["excluded".to_string()]);
        let mut request = request(&target, None, Some(512));
        request.excluded_cluster_ids = Some(&excluded);
        let mut excluded_backend = candidate("excluded", 1024);
        excluded_backend.rtt = Duration::from_millis(5);
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(50);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(50);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        excluded_backend.clone(),
                        higher_queue.clone(),
                        lower_queue.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_rtt_only_skips_multiple_excluded_backends() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ignore_queue_time: Some(true),
            ignore_input_processing_time: Some(true),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let excluded = HashSet::from(["excluded-a".to_string(), "excluded-b".to_string()]);
        let mut request = request(&target, None, Some(512));
        request.excluded_cluster_ids = Some(&excluded);
        let mut excluded_a = candidate("excluded-a", 1024);
        excluded_a.rtt = Duration::from_millis(5);
        let mut excluded_b = candidate("excluded-b", 1024);
        excluded_b.rtt = Duration::from_millis(5);
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.rtt = Duration::from_millis(50);
        higher_queue.stats.last_mean_input_tps = 100.0;
        higher_queue.stats.queued_input_size = 2;
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.rtt = Duration::from_millis(50);
        lower_queue.stats.last_mean_input_tps = 100.0;
        lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        excluded_a.clone(),
                        excluded_b.clone(),
                        higher_queue.clone(),
                        lower_queue.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_uses_priority_queue_time_estimate() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request_with_priority(&target, None, Some(0), 4);
        let mut aggregate_lower_priority_higher =
            candidate("aggregate-lower-priority-higher", 1024);
        aggregate_lower_priority_higher.stats.last_mean_input_tps = 100.0;
        aggregate_lower_priority_higher.stats.queued_input_size = 0;
        aggregate_lower_priority_higher
            .stats
            .queue_time_estimate_ms_by_priority = HashMap::from([(4, 50)]);
        let mut aggregate_higher_priority_lower =
            candidate("aggregate-higher-priority-lower", 1024);
        aggregate_higher_priority_lower.stats.last_mean_input_tps = 100.0;
        aggregate_higher_priority_lower.stats.queued_input_size = 100;
        aggregate_higher_priority_lower
            .stats
            .queue_time_estimate_ms_by_priority = HashMap::from([(4, 5)]);

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        aggregate_lower_priority_higher.clone(),
                        aggregate_higher_priority_lower.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(
                chosen.candidate.cluster_id,
                "aggregate-higher-priority-lower"
            );
        }
    }

    #[test]
    fn groq_multiregion_clamps_priority_to_max_known_queue_time_priority() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request_with_priority(&target, None, Some(0), 10);
        let mut lower_clamped_queue = candidate("lower-clamped-queue", 1024);
        lower_clamped_queue.stats.queue_time_estimate_ms_by_priority = HashMap::from([(2, 5)]);
        let mut higher_clamped_queue = candidate("higher-clamped-queue", 1024);
        higher_clamped_queue
            .stats
            .queue_time_estimate_ms_by_priority = HashMap::from([(2, 50)]);

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[lower_clamped_queue.clone(), higher_clamped_queue.clone()],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-clamped-queue");
        }
    }

    #[test]
    fn groq_multiregion_uses_next_highest_priority_queue_time_estimate() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request_with_priority(&target, None, Some(0), 3);
        let mut higher_queue = candidate("higher-queue", 1024);
        higher_queue.stats.queue_time_estimate_ms_by_priority = HashMap::from([(2, 50)]);
        let mut lower_queue = candidate("lower-queue", 1024);
        lower_queue.stats.queue_time_estimate_ms_by_priority = HashMap::from([(2, 5)]);

        for _ in 0..16 {
            let chosen = lb
                .choose(&request, &[higher_queue.clone(), lower_queue.clone()])
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "lower-queue");
        }
    }

    #[test]
    fn groq_multiregion_treats_lower_priority_only_queue_as_zero_for_higher_priority_request() {
        let lb = create_load_balancer_with_config(&LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::GroqMultiregion,
            n: Some(2),
            ..LoadBalancerAlgorithmConfig::from(LoadBalancerAlgorithm::GroqMultiregion)
        })
        .expect("factory should accept groq-multiregion");
        let target = target();
        let request = request_with_priority(&target, None, Some(0), 0);
        let mut sparse_lower_priority_only = candidate("sparse-lower-priority-only", 1024);
        sparse_lower_priority_only.stats.last_mean_input_tps = 100.0;
        sparse_lower_priority_only.stats.queued_input_size = 100;
        sparse_lower_priority_only
            .stats
            .queue_time_estimate_ms_by_priority = HashMap::from([(4, 0)]);
        let mut aggregate_lower_queue = candidate("aggregate-lower-queue", 1024);
        aggregate_lower_queue.stats.last_mean_input_tps = 100.0;
        aggregate_lower_queue.stats.queued_input_size = 1;

        for _ in 0..16 {
            let chosen = lb
                .choose(
                    &request,
                    &[
                        sparse_lower_priority_only.clone(),
                        aggregate_lower_queue.clone(),
                    ],
                )
                .expect("candidate should be selected");
            assert_eq!(chosen.candidate.cluster_id, "sparse-lower-priority-only");
        }
    }

    #[test]
    fn pulsar_different_affinity_keys_reach_multiple_backends() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();
        let candidates = vec![
            candidate("inst-a", 1024),
            candidate("inst-b", 1024),
            candidate("inst-c", 1024),
        ];

        let mut seen = std::collections::HashSet::new();
        for idx in 0..128 {
            let key = format!("affinity-{idx}");
            let choice = pulsar
                .choose(&request(&target, Some(&key), Some(128)), &candidates)
                .expect("choice should exist");
            seen.insert(choice.candidate.cluster_id);
            if seen.len() >= 2 {
                break;
            }
        }

        assert!(
            seen.len() >= 2,
            "expected at least two different backends across affinity keys, saw {seen:?}"
        );
    }

    #[test]
    fn pulsar_skips_backend_that_lacks_kv_capacity() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();

        let mut found = false;
        for idx in 0..512 {
            let key = format!("affinity-{idx}");
            let request = request(&target, Some(&key), Some(128));
            let feasible_candidates = vec![candidate("inst-a", 1024), candidate("inst-b", 1024)];
            let baseline = pulsar
                .choose(&request, &feasible_candidates)
                .expect("baseline choice should exist");
            if baseline.candidate.cluster_id != "inst-a" {
                continue;
            }

            let constrained_candidates = vec![candidate("inst-a", 64), candidate("inst-b", 1024)];
            let constrained = pulsar
                .choose(&request, &constrained_candidates)
                .expect("fallback choice should exist");
            assert_eq!(constrained.candidate.cluster_id, "inst-b");
            assert!(constrained.rank_depth > 1);
            found = true;
            break;
        }

        assert!(
            found,
            "expected to find an affinity key that ranks inst-a first"
        );
    }

    #[test]
    fn pulsar_cached_retry_skips_single_excluded_primary() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();
        let candidates = vec![
            candidate("retry-primary", 1024),
            candidate("retry-fallback", 1024),
            candidate("retry-second-fallback", 1024),
        ];
        let base_request = request(&target, Some("retry-prefix"), Some(128));
        let primary = pulsar
            .choose(&base_request, &candidates)
            .expect("initial request should select a primary")
            .candidate
            .cluster_id;
        let excluded = HashSet::from([primary.clone()]);
        let retry_request = LoadBalancerRequest {
            excluded_cluster_ids: Some(&excluded),
            ..base_request
        };

        let retry = pulsar
            .choose(&retry_request, &candidates)
            .expect("retry should select a non-excluded candidate");

        assert_ne!(retry.candidate.cluster_id, primary);
        assert!(retry.rank_depth > 1);
    }

    #[test]
    fn pulsar_returns_none_when_all_candidates_are_excluded() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();
        let candidates = vec![
            candidate("all-excluded-a", 1024),
            candidate("all-excluded-b", 1024),
        ];
        let excluded = candidates
            .iter()
            .map(|candidate| candidate.cluster_id.clone())
            .collect::<HashSet<_>>();
        let request = LoadBalancerRequest {
            excluded_cluster_ids: Some(&excluded),
            ..request(&target, Some("retry-prefix"), Some(128))
        };

        assert!(pulsar.choose(&request, &candidates).is_none());
    }

    #[test]
    fn pulsar_ranking_cache_invalidates_when_capacity_weight_changes() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();

        for idx in 0..1024 {
            let key = format!("affinity-{idx}");
            let request = request(&target, Some(&key), Some(128));
            let mut initial_a = candidate("inst-a", 1024);
            initial_a.stats.last_mean_input_tps = 10_000.0;
            let mut initial_b = candidate("inst-b", 1024);
            initial_b.stats.last_mean_input_tps = 1.0;
            let initial = vec![initial_a, initial_b];

            let mut changed_a = candidate("inst-a", 1024);
            changed_a.stats.last_mean_input_tps = 1.0;
            let mut changed_b = candidate("inst-b", 1024);
            changed_b.stats.last_mean_input_tps = 10_000.0;
            let changed = vec![changed_a, changed_b];

            let first = pulsar
                .choose(&request, &initial)
                .expect("initial choice should exist")
                .candidate
                .cluster_id;
            let second = pulsar
                .choose(&request, &changed)
                .expect("changed choice should exist")
                .candidate
                .cluster_id;
            if first != second {
                assert_eq!(first, "inst-a");
                assert_eq!(second, "inst-b");
                return;
            }
        }

        panic!("expected to find an affinity key whose ranking changes after capacity changes");
    }

    #[test]
    fn pulsar_does_not_cache_oversized_affinity_key() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });
        let target = target();
        let oversized_key = "x".repeat(MAX_CACHE_AFFINITY_CACHE_KEY_BYTES + 1);
        let request = request(&target, Some(&oversized_key), Some(128));
        let candidates = vec![
            candidate("large-key-a", 1024),
            candidate("large-key-b", 1024),
        ];

        let choice = pulsar
            .choose(&request, &candidates)
            .expect("oversized affinity key should still route");

        assert!(choice.candidate.cluster_id.starts_with("large-key-"));
        assert_eq!(pulsar.cached_affinity_key_bytes(), 0);
    }

    #[test]
    fn pulsar_hash_is_pinned_to_a_fixed_algorithm_and_version() {
        let hash = pulsar_hash64(
            Some("seed-1"),
            &Some("rk-1".to_string()),
            "model-a",
            Some("prefix-123"),
            "inst-a",
        );

        assert_eq!(hash, 0x63f6_65cb_6f2e_dbdf);
    }

    #[test]
    fn pulsar_uses_last_mean_input_tps_as_weight() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });

        let mut candidate = candidate("inst-a", 1024);
        candidate.stats.last_mean_input_tps = 123.0;

        assert_eq!(pulsar.weight(&candidate), Some(123.0));
    }

    #[test]
    fn pulsar_excludes_candidate_with_invalid_last_mean_input_tps() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });

        let target = target();
        let mut invalid = candidate("inst-a", 1024);
        invalid.stats.last_mean_input_tps = 0.0;
        let valid = candidate("inst-b", 1024);

        let choice = pulsar
            .choose(
                &request(&target, Some("prefix-1"), Some(128)),
                &[invalid, valid],
            )
            .expect("valid candidate should still be chosen");
        assert_eq!(choice.candidate.cluster_id, "inst-b");
    }

    #[test]
    fn pulsar_returns_none_when_all_candidates_lack_valid_last_mean_input_tps() {
        let pulsar = PulsarLoadBalancer::new(LoadBalancerAlgorithmConfig {
            algorithm: LoadBalancerAlgorithm::Pulsar,
            seed: Some("seed-1".to_string()),
            cache_affinity_virtual_nodes: None,
            cache_affinity_backend_selection_count: None,
            require_cache_affinity_key: Some(true),
            require_input_tokens: Some(true),
            require_kv_metrics: Some(true),
            queue_slo_ms: None,
            max_queue_time_floor_ms: None,
            max_queue_time_ceil_ms: None,
            max_queue_tokens_factor: None,
            hard_token_cap_factor: None,
            reentry_hysteresis: None,
            ttft_bucket_size_ms: None,
            next_bucket_unlock_factor: None,
            n: None,
            max_queued: None,
            max_input_work_seconds: None,
            ignore_queue_time: None,
            ignore_input_processing_time: None,
            request_algorithms: HashMap::new(),
        });

        let target = target();
        let mut invalid_a = candidate("inst-a", 1024);
        invalid_a.stats.last_mean_input_tps = 0.0;
        let mut invalid_b = candidate("inst-b", 1024);
        invalid_b.stats.last_mean_input_tps = f64::NAN;

        let choice = pulsar.choose(
            &request(&target, Some("prefix-1"), Some(128)),
            &[invalid_a, invalid_b],
        );
        assert!(choice.is_none());
    }
}
