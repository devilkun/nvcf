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

use parking_lot::{Mutex, RwLock};
use scc::HashMap as SccHashMap;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tonic::Status;
use tracing::warn;

use crate::metrics::StargateMetrics;
use stargate_proto::pb::{
    CalibrationState, InferenceServerModelRegistration, InferenceServerRegistration,
    InferenceServerStatus, ModelCalibrationDirective, ModelStats,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RoutingTargetKey {
    pub routing_key: Option<String>,
    pub model_id: String,
}

#[derive(Debug)]
pub struct StargateState {
    routing_map: SccHashMap<RoutingTargetKey, Arc<RoutingTargetState>>,
    registered_inference_servers: SccHashMap<String, Arc<RegisteredInferenceServerState>>,
    cluster_calibrations: SccHashMap<ClusterCalibrationKey, Arc<Mutex<ClusterCalibrationState>>>,
    active_models_snapshot: RwLock<Vec<ActiveModelSnapshot>>,
    next_reservation_id: AtomicU64,
    metrics: Option<Arc<StargateMetrics>>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ActiveModelSnapshot {
    routing_key: Option<String>,
    model_id: String,
}

#[derive(Debug, Default)]
struct RoutingTargetState {
    clusters: SccHashMap<String, Arc<RoutedClusterState>>,
}

#[derive(Debug, Default)]
struct RoutedClusterState {
    inference_servers: SccHashMap<String, RoutedInferenceServerSnapshot>,
    round_robin_counter: AtomicUsize,
    // The cluster owns the routable snapshot: backend-scoped load is aggregated
    // across active backends, while cluster-scoped fields are stored here and
    // refreshed from the latest registration update or local reservation logic.
    cluster_snapshot: Mutex<Option<StoredClusterSnapshot>>,
}

#[derive(Debug)]
struct RegisteredInferenceServerState {
    identity: RegistrationIdentity,
    registered_models: SccHashMap<String, ()>,
    last_rtt: Mutex<Option<Duration>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeliveryTarget {
    Local { inference_server_id: String },
}

#[derive(Clone, Debug)]
pub struct RoutedInferenceServerSnapshot {
    pub cluster_id: String,
    pub inference_server_id: String,
    pub inference_server_url: String,
    pub stats: ModelStats,
    pub rtt: Duration,
    pub snapshot_updated_at: Instant,
    pub status: i32,
    pub reverse_tunnel: bool,
    pub delivery_target: DeliveryTarget,
}

#[derive(Clone, Debug)]
pub struct RoutedClusterSnapshot {
    pub cluster_id: String,
    pub stats: ModelStats,
    pub rtt: Duration,
    pub snapshot_updated_at: Instant,
    pub status: i32,
    pub active_backend_count: usize,
}

#[derive(Clone, Debug)]
struct StoredClusterSnapshot {
    snapshot: RoutedClusterSnapshot,
    cluster_stats_source_backend_id: String,
    // Raw cluster-scoped stats from registration heartbeats. Pending local
    // reservations are tracked separately so unrelated backend heartbeats do
    // not wipe optimistic load before the chosen backend reports again.
    cluster_stats_base: ModelStats,
    raw_cluster_updates: HashMap<String, ClusterScopedUpdate>,
    pending_cluster_reservations: BTreeMap<u64, PendingClusterReservation>,
}

#[derive(Clone, Debug)]
struct ClusterScopedUpdate {
    source_backend_id: String,
    stats: ModelStats,
    snapshot_updated_at: Instant,
    status: i32,
}

#[derive(Clone, Debug)]
struct PendingClusterReservation {
    inference_server_id: String,
    input_tokens: u64,
    priority: u32,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RoutingReservation {
    id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClusterCalibrationKey {
    routing_key: Option<String>,
    cluster_id: String,
    model_id: String,
}

#[derive(Clone, Debug, PartialEq)]
enum ClusterCalibrationState {
    Assigned { owner_inference_server_id: String },
    Complete { last_mean_input_tps: f64 },
}

struct RoutedInferenceServerSnapshotInput<'a> {
    cluster_id: &'a str,
    inference_server_id: &'a str,
    inference_server_url: &'a str,
    stats: ModelStats,
    rtt: Duration,
    snapshot_updated_at: Instant,
    status: i32,
    reverse_tunnel: bool,
    delivery_target: DeliveryTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RegistrationIdentity {
    pub(crate) inference_server_id: String,
    pub(crate) cluster_id: String,
    pub(crate) inference_server_url: String,
    pub(crate) routing_key: Option<String>,
    pub(crate) reverse_tunnel: bool,
    pub(crate) coordinated_calibration: bool,
}

pub struct RegisteredReverseTunnel {
    // TODO actually add a validated unique identity per client, when workers are given unique credentials.
    // right now we trust that the worker will send us a unique id.
    // routing key is separate, not tied to worker identity. many workers can share the same routing key.
    pub routing_key: Option<String>,
}

pub(crate) struct RunningRegistration {
    pub(crate) identity: RegistrationIdentity,
    registration_state: Arc<RegisteredInferenceServerState>,
}

impl Default for StargateState {
    fn default() -> Self {
        Self::new()
    }
}

impl StargateState {
    pub fn new() -> Self {
        Self::new_inner(None)
    }

    pub fn new_with_metrics(metrics: Arc<StargateMetrics>) -> Self {
        Self::new_inner(Some(metrics))
    }

    fn new_inner(metrics: Option<Arc<StargateMetrics>>) -> Self {
        Self {
            routing_map: SccHashMap::default(),
            registered_inference_servers: SccHashMap::default(),
            cluster_calibrations: SccHashMap::default(),
            active_models_snapshot: RwLock::new(Vec::new()),
            next_reservation_id: AtomicU64::new(0),
            metrics,
        }
    }

    async fn target_state(&self, target: &RoutingTargetKey) -> Option<Arc<RoutingTargetState>> {
        self.routing_map
            .read_async(target, |_key, state| state.clone())
            .await
    }

    async fn target_state_or_insert(&self, target: &RoutingTargetKey) -> Arc<RoutingTargetState> {
        loop {
            if let Some(existing) = self.target_state(target).await {
                return existing;
            }

            let candidate = Arc::new(RoutingTargetState::default());
            if self
                .routing_map
                .insert_async(target.clone(), candidate.clone())
                .await
                .is_ok()
            {
                return candidate;
            }

            if let Some(existing) = self.target_state(target).await {
                return existing;
            }
        }
    }

    async fn cluster_state(
        target_state: &RoutingTargetState,
        cluster_id: &str,
    ) -> Option<Arc<RoutedClusterState>> {
        target_state
            .clusters
            .read_async(cluster_id, |_key, state| state.clone())
            .await
    }

    async fn cluster_state_or_insert(
        target_state: &RoutingTargetState,
        cluster_id: &str,
    ) -> Arc<RoutedClusterState> {
        loop {
            if let Some(existing) = Self::cluster_state(target_state, cluster_id).await {
                return existing;
            }

            let candidate = Arc::new(RoutedClusterState::default());
            if target_state
                .clusters
                .insert_async(cluster_id.to_string(), candidate.clone())
                .await
                .is_ok()
            {
                return candidate;
            }

            if let Some(existing) = Self::cluster_state(target_state, cluster_id).await {
                return existing;
            }
        }
    }

    async fn registered_inference_server_state(
        &self,
        inference_server_id: &str,
    ) -> Option<Arc<RegisteredInferenceServerState>> {
        self.registered_inference_servers
            .read_async(inference_server_id, |_id, registration| {
                registration.clone()
            })
            .await
    }

    async fn claim_inference_server(
        &self,
        identity: &RegistrationIdentity,
    ) -> Result<Arc<RegisteredInferenceServerState>, Arc<RegisteredInferenceServerState>> {
        let registration = Arc::new(RegisteredInferenceServerState {
            identity: identity.clone(),
            registered_models: SccHashMap::default(),
            last_rtt: Mutex::new(None),
        });
        match self
            .registered_inference_servers
            .insert_async(identity.inference_server_id.clone(), registration.clone())
            .await
        {
            Ok(()) => Ok(registration),
            Err(_) => match self
                .registered_inference_server_state(&identity.inference_server_id)
                .await
            {
                Some(existing) => Err(existing),
                None => Err(registration),
            },
        }
    }

    pub(crate) async fn begin_registration(
        &self,
        identity: &RegistrationIdentity,
    ) -> Result<RunningRegistration, Status> {
        let registration_state =
            self.claim_inference_server(identity)
                .await
                .map_err(|existing| {
                    duplicate_registration_status(&identity.inference_server_id, &existing)
                })?;
        Ok(RunningRegistration {
            identity: identity.clone(),
            registration_state,
        })
    }

    pub(crate) async fn end_registration(&self, inference_server_id: &str) {
        let Some((_id, registration)) = self
            .registered_inference_servers
            .remove_async(inference_server_id)
            .await
        else {
            return;
        };

        let routing_key = registration.identity.routing_key.clone();
        let mut model_ids = HashSet::new();
        let _ = registration
            .registered_models
            .iter_async(|model_id, _registered| {
                model_ids.insert(model_id.clone());
                true
            })
            .await;
        let targets: HashSet<RoutingTargetKey> = model_ids
            .into_iter()
            .map(|model_id| RoutingTargetKey {
                routing_key: routing_key.clone(),
                model_id,
            })
            .collect();
        self.remove_inference_server_targets(
            inference_server_id,
            &registration.identity.cluster_id,
            &targets,
        )
        .await;
        self.release_cluster_calibration(&registration.identity)
            .await;
    }

    async fn upsert_inference_server_target(
        &self,
        target: &RoutingTargetKey,
        snapshot_input: RoutedInferenceServerSnapshotInput<'_>,
    ) {
        let snapshot = RoutedInferenceServerSnapshot {
            cluster_id: snapshot_input.cluster_id.to_string(),
            inference_server_id: snapshot_input.inference_server_id.to_string(),
            inference_server_url: snapshot_input.inference_server_url.to_string(),
            stats: snapshot_input.stats,
            rtt: snapshot_input.rtt,
            snapshot_updated_at: snapshot_input.snapshot_updated_at,
            status: snapshot_input.status,
            reverse_tunnel: snapshot_input.reverse_tunnel,
            delivery_target: snapshot_input.delivery_target,
        };
        let cluster_scoped_update = ClusterScopedUpdate {
            source_backend_id: snapshot.inference_server_id.clone(),
            stats: snapshot.stats.clone(),
            snapshot_updated_at: snapshot.snapshot_updated_at,
            status: snapshot.status,
        };

        let target_state = self.target_state_or_insert(target).await;
        let cluster_state =
            Self::cluster_state_or_insert(&target_state, snapshot_input.cluster_id).await;
        let _ = cluster_state
            .inference_servers
            .upsert_async(snapshot_input.inference_server_id.to_string(), snapshot)
            .await;
        refresh_cluster_snapshot(
            snapshot_input.cluster_id,
            &cluster_state,
            Some(cluster_scoped_update),
        )
        .await;
        self.refresh_active_inference_server_count(target, &target_state)
            .await;
    }

    async fn mark_model_registered(&self, running: &RunningRegistration, model_id: &str) {
        let _ = running
            .registration_state
            .registered_models
            .upsert_async(model_id.to_string(), ())
            .await;
    }

    async fn remove_inference_server_from_target(
        &self,
        inference_server_id: &str,
        cluster_id: &str,
        target: &RoutingTargetKey,
    ) {
        let Some(target_state) = self.target_state(target).await else {
            return;
        };

        let Some(cluster_state) = Self::cluster_state(&target_state, cluster_id).await else {
            return;
        };

        let _ = cluster_state
            .inference_servers
            .remove_async(inference_server_id)
            .await;

        if !cluster_state.inference_servers.is_empty() {
            refresh_cluster_snapshot(cluster_id, &cluster_state, None).await;
        } else {
            *cluster_state.cluster_snapshot.lock() = None;
        }

        if cluster_state.inference_servers.is_empty() {
            let cluster_state_for_remove = cluster_state.clone();
            let _ = target_state
                .clusters
                .remove_if_async(cluster_id, move |current| {
                    Arc::ptr_eq(current, &cluster_state_for_remove)
                        && current.inference_servers.is_empty()
                })
                .await;
        }

        let count = count_target_inference_servers(&target_state).await;
        if count == 0 {
            let target_state_for_remove = target_state.clone();
            let _ = self
                .routing_map
                .remove_if_async(target, move |current| {
                    Arc::ptr_eq(current, &target_state_for_remove) && current.clusters.is_empty()
                })
                .await;
        }

        self.set_active_inference_server_count(target, count);
    }

    async fn remove_inference_server_targets(
        &self,
        inference_server_id: &str,
        cluster_id: &str,
        targets: &HashSet<RoutingTargetKey>,
    ) {
        for target in targets {
            self.remove_inference_server_from_target(inference_server_id, cluster_id, target)
                .await;
        }
    }

    async fn calibration_directive_for_model(
        &self,
        running: &RunningRegistration,
        model_id: &str,
        model: &InferenceServerModelRegistration,
    ) -> Option<ModelCalibrationDirective> {
        if !running.identity.coordinated_calibration {
            return None;
        }

        let key = ClusterCalibrationKey {
            routing_key: running.identity.routing_key.clone(),
            cluster_id: running.identity.cluster_id.clone(),
            model_id: model_id.to_string(),
        };
        let reported_last_mean_input_tps =
            model.stats.as_ref().and_then(reported_last_mean_input_tps);
        let reports_complete = model.status == InferenceServerStatus::Active as i32
            && model.calibration_state == CalibrationState::Complete as i32
            && reported_last_mean_input_tps.is_some();

        loop {
            if let Some(existing) = self
                .cluster_calibrations
                .read_async(&key, |_key, state| state.clone())
                .await
            {
                let mut state = existing.lock();
                return Some(match &mut *state {
                    ClusterCalibrationState::Assigned {
                        owner_inference_server_id,
                    } if owner_inference_server_id == &running.identity.inference_server_id => {
                        if reports_complete {
                            let last_mean_input_tps =
                                reported_last_mean_input_tps.unwrap_or_default();
                            *state = ClusterCalibrationState::Complete {
                                last_mean_input_tps,
                            };
                            calibration_complete_directive(model_id, last_mean_input_tps)
                        } else {
                            calibration_run_directive(model_id)
                        }
                    }
                    ClusterCalibrationState::Assigned { .. } => {
                        calibration_wait_directive(model_id)
                    }
                    ClusterCalibrationState::Complete {
                        last_mean_input_tps,
                    } => calibration_complete_directive(model_id, *last_mean_input_tps),
                });
            }

            let initial_state = if reports_complete {
                ClusterCalibrationState::Complete {
                    last_mean_input_tps: reported_last_mean_input_tps.unwrap_or_default(),
                }
            } else {
                ClusterCalibrationState::Assigned {
                    owner_inference_server_id: running.identity.inference_server_id.clone(),
                }
            };
            let inserted = self
                .cluster_calibrations
                .insert_async(key.clone(), Arc::new(Mutex::new(initial_state)))
                .await
                .is_ok();
            if inserted {
                return Some(if reports_complete {
                    calibration_complete_directive(
                        model_id,
                        reported_last_mean_input_tps.unwrap_or_default(),
                    )
                } else {
                    calibration_run_directive(model_id)
                });
            }
        }
    }

    async fn release_cluster_calibration(&self, identity: &RegistrationIdentity) {
        if !identity.coordinated_calibration {
            return;
        }

        let mut keys_to_check = Vec::new();
        let _ = self
            .cluster_calibrations
            .iter_async(|key, _state| {
                if !same_cluster_calibration_scope(key, identity) {
                    return true;
                }
                keys_to_check.push(key.clone());
                true
            })
            .await;

        for key in keys_to_check {
            let model_still_registered = self
                .cluster_model_has_registered_peer(identity, &key.model_id, None)
                .await;
            let identity = identity.clone();
            let remove_key = key.clone();
            let _ = self
                .cluster_calibrations
                .remove_if_async(&key, move |state| {
                    if !same_cluster_calibration_scope(&remove_key, &identity) {
                        return false;
                    }
                    if !model_still_registered {
                        return true;
                    }
                    matches!(
                        &*state.lock(),
                        ClusterCalibrationState::Assigned {
                            owner_inference_server_id
                        } if owner_inference_server_id == &identity.inference_server_id
                    )
                })
                .await;
        }
    }

    async fn release_removed_model_cluster_calibration(
        &self,
        identity: &RegistrationIdentity,
        removed_models: &HashSet<String>,
    ) {
        if !identity.coordinated_calibration || removed_models.is_empty() {
            return;
        }

        for model_id in removed_models {
            let model_still_registered = self
                .cluster_model_has_registered_peer(
                    identity,
                    model_id,
                    Some(&identity.inference_server_id),
                )
                .await;
            let key = ClusterCalibrationKey {
                routing_key: identity.routing_key.clone(),
                cluster_id: identity.cluster_id.clone(),
                model_id: model_id.clone(),
            };
            let owner_inference_server_id = identity.inference_server_id.clone();
            let _ = self
                .cluster_calibrations
                .remove_if_async(&key, move |state| {
                    if !model_still_registered {
                        return true;
                    }
                    matches!(
                        &*state.lock(),
                        ClusterCalibrationState::Assigned {
                            owner_inference_server_id: owner
                        } if owner == &owner_inference_server_id
                    )
                })
                .await;
        }
    }

    async fn cluster_model_has_registered_peer(
        &self,
        identity: &RegistrationIdentity,
        model_id: &str,
        excluded_inference_server_id: Option<&str>,
    ) -> bool {
        let mut registrations = Vec::new();
        let _ = self
            .registered_inference_servers
            .iter_async(|_id, registration| {
                let excluded = excluded_inference_server_id
                    .is_some_and(|id| id == registration.identity.inference_server_id);
                if !excluded
                    && registration.identity.cluster_id == identity.cluster_id
                    && registration.identity.routing_key == identity.routing_key
                    && registration.identity.coordinated_calibration
                {
                    registrations.push(registration.clone());
                }
                true
            })
            .await;

        for registration in registrations {
            if registration
                .registered_models
                .read_async(model_id, |_model_id, _registered| ())
                .await
                .is_some()
            {
                return true;
            }
        }
        false
    }

    pub(crate) async fn apply_registration_update(
        &self,
        running: &mut RunningRegistration,
        update: &InferenceServerRegistration,
        reverse_connected: bool,
        rtt: Option<Duration>,
    ) -> Vec<ModelCalibrationDirective> {
        let routing_key = &running.registration_state.identity.routing_key;
        let mut calibration_directives = Vec::new();

        *running.registration_state.last_rtt.lock() = rtt;

        let mut registered_models = HashSet::new();
        let _ = running
            .registration_state
            .registered_models
            .iter_async(|model_id, _registered| {
                registered_models.insert(model_id.clone());
                true
            })
            .await;
        let current_models: HashSet<String> = update.models.keys().cloned().collect();
        let removed_models: HashSet<String> = registered_models
            .difference(&current_models)
            .cloned()
            .collect();
        let removed_targets: HashSet<RoutingTargetKey> = removed_models
            .iter()
            .map(|model_id| RoutingTargetKey {
                routing_key: routing_key.clone(),
                model_id: model_id.clone(),
            })
            .collect();
        self.remove_inference_server_targets(
            &running.identity.inference_server_id,
            &running.identity.cluster_id,
            &removed_targets,
        )
        .await;
        self.release_removed_model_cluster_calibration(&running.identity, &removed_models)
            .await;
        for model_id in &removed_models {
            let _ = running
                .registration_state
                .registered_models
                .remove_async(model_id)
                .await;
        }

        for (model_id, model) in &update.models {
            // Identical stats across consecutive updates are expected because
            // heartbeat sends carry full registration snapshots.
            let calibration_directive = self
                .calibration_directive_for_model(running, model_id, model)
                .await;
            if let Some(directive) = calibration_directive.clone() {
                calibration_directives.push(directive);
            }
            let calibration_pending = calibration_directive
                .as_ref()
                .is_some_and(|directive| directive.state != CalibrationState::Complete as i32);
            let target = RoutingTargetKey {
                routing_key: routing_key.clone(),
                model_id: model_id.clone(),
            };
            let stats = model.stats.clone().unwrap_or_default();
            let effective_status =
                if (running.identity.reverse_tunnel && !reverse_connected) || calibration_pending {
                    InferenceServerStatus::Inactive as i32
                } else if model.stats.is_none() {
                    warn!(
                        inference_server_id = %running.identity.inference_server_id,
                        model_id = %model_id,
                        "missing model stats in registration; setting model status to inactive"
                    );
                    InferenceServerStatus::Inactive as i32
                } else {
                    model.status
                };

            self.mark_model_registered(running, model_id).await;

            if effective_status == InferenceServerStatus::Active as i32 {
                let Some(current_rtt) = rtt else {
                    warn!(
                        inference_server_id = %running.identity.inference_server_id,
                        model_id = %model_id,
                        "active model registration missing connection RTT; skipping routing update"
                    );
                    self.remove_inference_server_from_target(
                        &running.identity.inference_server_id,
                        &running.identity.cluster_id,
                        &target,
                    )
                    .await;
                    continue;
                };
                self.upsert_inference_server_target(
                    &target,
                    RoutedInferenceServerSnapshotInput {
                        cluster_id: &running.identity.cluster_id,
                        inference_server_id: &running.identity.inference_server_id,
                        inference_server_url: &running.identity.inference_server_url,
                        stats,
                        rtt: current_rtt,
                        snapshot_updated_at: Instant::now(),
                        status: effective_status,
                        reverse_tunnel: running.identity.reverse_tunnel,
                        delivery_target: DeliveryTarget::Local {
                            inference_server_id: running.identity.inference_server_id.clone(),
                        },
                    },
                )
                .await;
            } else {
                self.remove_inference_server_from_target(
                    &running.identity.inference_server_id,
                    &running.identity.cluster_id,
                    &target,
                )
                .await;
            }
        }

        calibration_directives
    }

    /// Returns all active inference server snapshots for a
    /// `(routing_key, model_id)` pair. The HTTP proxy calls this to get the
    /// candidate set that the load balancer chooses from.
    pub async fn candidates_for_target(
        &self,
        target: &RoutingTargetKey,
    ) -> Vec<RoutedInferenceServerSnapshot> {
        let Some(target_state) = self.target_state(target).await else {
            return Vec::new();
        };

        let mut candidates = Vec::new();
        for (_cluster_id, cluster_state) in collect_target_clusters(&target_state).await {
            let _ = cluster_state
                .inference_servers
                .iter_async(|_inference_server_id, snapshot| {
                    candidates.push(snapshot.clone());
                    true
                })
                .await;
        }
        candidates
    }

    pub async fn cluster_candidates_for_target(
        &self,
        target: &RoutingTargetKey,
    ) -> Vec<RoutedClusterSnapshot> {
        let Some(target_state) = self.target_state(target).await else {
            return Vec::new();
        };

        let mut clusters = Vec::new();
        for (cluster_id, cluster_state) in collect_target_clusters(&target_state).await {
            if let Some(snapshot) = cluster_snapshot_for_target(&cluster_id, &cluster_state).await {
                clusters.push(snapshot);
            }
        }
        clusters
    }

    pub async fn has_registered_model_for_target(&self, target: &RoutingTargetKey) -> bool {
        let mut registrations = Vec::new();
        let _ = self
            .registered_inference_servers
            .iter_async(|_id, registration| {
                if registration.identity.routing_key == target.routing_key {
                    registrations.push(registration.clone());
                }
                true
            })
            .await;

        for registration in registrations {
            if registration
                .registered_models
                .read_async(&target.model_id, |_model_id, _registered| ())
                .await
                .is_some()
            {
                return true;
            }
        }
        false
    }

    pub async fn refresh_active_models_snapshot(&self) {
        let mut targets = Vec::new();
        let _ = self
            .routing_map
            .iter_async(|target, target_state| {
                targets.push((target.clone(), target_state.clone()));
                true
            })
            .await;

        let mut models = BTreeSet::new();
        for (target, target_state) in targets {
            if !target_has_routable_cluster_snapshot(&target_state).await {
                continue;
            }
            models.insert(ActiveModelSnapshot {
                routing_key: target.routing_key,
                model_id: target.model_id,
            });
        }

        *self.active_models_snapshot.write() = models.into_iter().collect();
    }

    pub fn list_active_models(
        &self,
        routing_key: Option<&str>,
        model_ids: &[String],
    ) -> Vec<String> {
        let model_filter = (!model_ids.is_empty()).then(|| {
            model_ids
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        });
        self.active_models_snapshot
            .read()
            .iter()
            .filter(|snapshot| snapshot.routing_key.as_deref() == routing_key)
            .filter(|snapshot| match &model_filter {
                Some(filter) => filter.contains(snapshot.model_id.as_str()),
                None => true,
            })
            .map(|snapshot| snapshot.model_id.clone())
            .collect()
    }

    pub async fn select_backend_for_cluster(
        &self,
        target: &RoutingTargetKey,
        cluster_id: &str,
        failed_backend_ids: &HashSet<String>,
    ) -> Option<RoutedInferenceServerSnapshot> {
        let target_state = self.target_state(target).await?;
        let cluster_state = Self::cluster_state(&target_state, cluster_id).await?;

        let mut candidates = Vec::new();
        let _ = cluster_state
            .inference_servers
            .iter_async(|backend_id, snapshot| {
                if !failed_backend_ids.contains(backend_id) {
                    candidates.push(snapshot.clone());
                }
                true
            })
            .await;

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by(|a, b| a.inference_server_id.cmp(&b.inference_server_id));
        let idx = cluster_state
            .round_robin_counter
            .fetch_add(1, Ordering::Relaxed)
            % candidates.len();
        Some(candidates[idx].clone())
    }

    async fn refresh_active_inference_server_count(
        &self,
        target: &RoutingTargetKey,
        target_state: &RoutingTargetState,
    ) {
        let count = count_target_inference_servers(target_state).await;
        self.set_active_inference_server_count(target, count);
    }

    fn set_active_inference_server_count(&self, target: &RoutingTargetKey, count: usize) {
        if let Some(metrics) = &self.metrics {
            metrics.set_active_inference_servers(
                target.routing_key.as_deref(),
                &target.model_id,
                count,
            );
        }
    }

    pub(crate) async fn reserve_inference_server_for_target(
        &self,
        target: &RoutingTargetKey,
        inference_server_id: &str,
        input_tokens: Option<u64>,
        priority: u32,
    ) -> Option<RoutingReservation> {
        let target_state = self.target_state(target).await?;
        let input_tokens = input_tokens.unwrap_or(0);
        let reservation = RoutingReservation {
            id: self.next_reservation_id.fetch_add(1, Ordering::Relaxed),
        };
        // Reservation counters are optimistic routing stats; saturate at u64::MAX rather than wrap.
        for (_cluster_id, cluster_state) in collect_target_clusters(&target_state).await {
            let backend_exists = cluster_state
                .inference_servers
                .read_async(inference_server_id, |_id, _snapshot| ())
                .await
                .is_some();
            if !backend_exists {
                continue;
            }
            let mut snapshot = cluster_state.cluster_snapshot.lock();
            let stored = snapshot.as_mut()?;
            // Keep optimistic work in routing-owned state: backend snapshots are
            // heartbeat-owned, so rejection can release only its own reservation.
            stored.pending_cluster_reservations.insert(
                reservation.id,
                PendingClusterReservation {
                    inference_server_id: inference_server_id.to_string(),
                    input_tokens,
                    priority,
                },
            );

            stored.snapshot.stats.queue_size = stored.snapshot.stats.queue_size.saturating_add(1);
            stored.snapshot.stats.queued_input_size = stored
                .snapshot
                .stats
                .queued_input_size
                .saturating_add(input_tokens);
            stored.snapshot.stats.num_running_queries =
                stored.snapshot.stats.num_running_queries.saturating_add(1);
            stored.snapshot.stats.total_query_input_size = stored
                .snapshot
                .stats
                .total_query_input_size
                .saturating_add(input_tokens);
            update_reserved_priority_queue_time(&mut stored.snapshot.stats, input_tokens, priority);
            return Some(reservation);
        }
        None
    }

    pub(crate) async fn release_inference_server_reservation_for_target(
        &self,
        target: &RoutingTargetKey,
        reservation: RoutingReservation,
    ) {
        let Some(target_state) = self.target_state(target).await else {
            return;
        };
        for (cluster_id, cluster_state) in collect_target_clusters(&target_state).await {
            let pending = cluster_state
                .cluster_snapshot
                .lock()
                .as_mut()
                .and_then(|stored| stored.pending_cluster_reservations.remove(&reservation.id));
            if pending.is_none() {
                continue;
            }

            refresh_cluster_snapshot(&cluster_id, &cluster_state, None).await;
            return;
        }
    }

    /// Looks up the registration for an inference server that declared
    /// `reverse_tunnel = true` during gRPC registration. Returns `None` if
    /// the server is not registered or was registered without reverse tunnel
    /// mode.
    ///
    /// Called during the QUIC reverse-tunnel handshake to confirm the
    /// connecting server was expected and to retrieve the auth-derived
    /// routing key for comparison against the QUIC handshake's own auth
    /// result.
    pub async fn registered_reverse_tunnel(
        &self,
        inference_server_id: &str,
    ) -> Option<RegisteredReverseTunnel> {
        self.registered_inference_servers
            .read_async(inference_server_id, |_id, registration| {
                if !registration.identity.reverse_tunnel {
                    return None;
                }
                Some(RegisteredReverseTunnel {
                    routing_key: registration.identity.routing_key.clone(),
                })
            })
            .await
            .flatten()
    }
}

async fn count_target_inference_servers(target_state: &RoutingTargetState) -> usize {
    let mut count = 0;
    let _ = target_state
        .clusters
        .iter_async(|_, cluster_state| {
            count += cluster_state.inference_servers.len();
            true
        })
        .await;
    count
}

fn update_reserved_priority_queue_time(stats: &mut ModelStats, input_tokens: u64, priority: u32) {
    if stats.queue_time_estimate_ms_by_priority.is_empty() {
        return;
    }

    let Some(delta_ms) =
        crate::queue_estimate::queue_time_delta_ms(input_tokens, stats.last_mean_input_tps)
    else {
        stats.queue_time_estimate_ms_by_priority.clear();
        return;
    };

    let pre_reservation_estimate_ms =
        crate::queue_estimate::priority_map_estimate_ms_for_priority(stats, priority)
            .unwrap_or_default();
    let estimate = stats
        .queue_time_estimate_ms_by_priority
        .entry(priority)
        .or_insert(pre_reservation_estimate_ms);
    *estimate = estimate.saturating_add(delta_ms);
    for (candidate_priority, estimate_ms) in &mut stats.queue_time_estimate_ms_by_priority {
        if *candidate_priority <= priority {
            continue;
        }
        // Queue-time estimates are advisory routing stats; saturate rather than wrap on extreme input.
        *estimate_ms = estimate_ms.saturating_add(delta_ms);
    }
}

async fn collect_target_clusters(
    target_state: &RoutingTargetState,
) -> Vec<(String, Arc<RoutedClusterState>)> {
    let mut clusters = Vec::new();
    let _ = target_state
        .clusters
        .iter_async(|cluster_id, cluster_state| {
            clusters.push((cluster_id.clone(), cluster_state.clone()));
            true
        })
        .await;
    clusters
}

fn set_backend_scoped_stats(stats: &mut ModelStats, src: &ModelStats) {
    stats.last_mean_input_tps = src.last_mean_input_tps;
    stats.output_tps = src.output_tps;
    stats.queue_size = src.queue_size;
    stats.queued_input_size = src.queued_input_size;
    stats.input_processing_queries = src.input_processing_queries;
    stats.output_generation_queries = src.output_generation_queries;
    stats.stats_observed_at_unix_ms = src.stats_observed_at_unix_ms;
    stats.stats_capabilities = src.stats_capabilities.clone();
    stats.stats_sources = src.stats_sources.clone();
}

fn set_cluster_scoped_stats(stats: &mut ModelStats, src: &ModelStats) {
    stats.max_output_tps = src.max_output_tps;
    stats.kv_cache_capacity_tokens = src.kv_cache_capacity_tokens;
    stats.kv_cache_used_tokens = src.kv_cache_used_tokens;
    stats.kv_cache_free_tokens = src.kv_cache_free_tokens;
    stats.num_running_queries = src.num_running_queries;
    stats.max_engine_concurrency = src.max_engine_concurrency;
    stats.total_query_input_size = src.total_query_input_size;
    stats.queue_time_estimate_ms_by_priority = src.queue_time_estimate_ms_by_priority.clone();
}

fn apply_pending_cluster_reservations(
    stats: &mut ModelStats,
    pending_cluster_reservations: &BTreeMap<u64, PendingClusterReservation>,
) {
    for pending in pending_cluster_reservations.values() {
        // Pending reservations are advisory routing stats; saturate rather than wrap.
        stats.queue_size = stats.queue_size.saturating_add(1);
        stats.queued_input_size = stats.queued_input_size.saturating_add(pending.input_tokens);
        stats.num_running_queries = stats.num_running_queries.saturating_add(1);
        stats.total_query_input_size = stats
            .total_query_input_size
            .saturating_add(pending.input_tokens);
        update_reserved_priority_queue_time(stats, pending.input_tokens, pending.priority);
    }
}

fn valid_last_mean_input_tps(last_mean_input_tps: f64) -> bool {
    last_mean_input_tps > 0.0 && last_mean_input_tps.is_finite()
}

fn reported_last_mean_input_tps(stats: &ModelStats) -> Option<f64> {
    valid_last_mean_input_tps(stats.last_mean_input_tps).then_some(stats.last_mean_input_tps)
}

fn calibration_run_directive(model_id: &str) -> ModelCalibrationDirective {
    ModelCalibrationDirective {
        model_id: model_id.to_string(),
        state: CalibrationState::Run as i32,
        last_mean_input_tps: 0.0,
    }
}

fn calibration_wait_directive(model_id: &str) -> ModelCalibrationDirective {
    ModelCalibrationDirective {
        model_id: model_id.to_string(),
        state: CalibrationState::Waiting as i32,
        last_mean_input_tps: 0.0,
    }
}

fn calibration_complete_directive(
    model_id: &str,
    last_mean_input_tps: f64,
) -> ModelCalibrationDirective {
    ModelCalibrationDirective {
        model_id: model_id.to_string(),
        state: CalibrationState::Complete as i32,
        last_mean_input_tps,
    }
}

fn same_cluster_calibration_scope(
    key: &ClusterCalibrationKey,
    identity: &RegistrationIdentity,
) -> bool {
    key.cluster_id == identity.cluster_id && key.routing_key == identity.routing_key
}

fn build_cluster_snapshot(
    cluster_id: &str,
    stored: &StoredClusterSnapshot,
    backend_stats: &ModelStats,
    rtt: Duration,
    active_backend_count: usize,
) -> RoutedClusterSnapshot {
    let mut snapshot = stored.snapshot.clone();
    snapshot.cluster_id = cluster_id.to_string();
    snapshot.rtt = rtt;
    snapshot.active_backend_count = active_backend_count;
    set_backend_scoped_stats(&mut snapshot.stats, backend_stats);
    set_cluster_scoped_stats(&mut snapshot.stats, &stored.cluster_stats_base);
    apply_pending_cluster_reservations(&mut snapshot.stats, &stored.pending_cluster_reservations);
    snapshot
}

async fn collect_cluster_backend_aggregate(
    cluster_state: &RoutedClusterState,
) -> Option<(ModelStats, Duration, usize)> {
    let mut backend_stats = ModelStats::default();
    let mut active_backend_count = 0usize;
    let mut rtt: Option<Duration> = None;

    let _ = cluster_state
        .inference_servers
        .iter_async(|_backend_id, snapshot| {
            active_backend_count += 1;
            backend_stats.output_tps += snapshot.stats.output_tps;
            if valid_last_mean_input_tps(snapshot.stats.last_mean_input_tps) {
                // `last_mean_input_tps` intentionally has no source provenance; see
                // docs/multi-backend-clusters.md for the accepted convergence limitation.
                backend_stats.last_mean_input_tps += snapshot.stats.last_mean_input_tps;
            }
            backend_stats.queue_size += snapshot.stats.queue_size;
            backend_stats.queued_input_size += snapshot.stats.queued_input_size;
            backend_stats.input_processing_queries += snapshot.stats.input_processing_queries;
            backend_stats.output_generation_queries += snapshot.stats.output_generation_queries;
            backend_stats.stats_observed_at_unix_ms = backend_stats
                .stats_observed_at_unix_ms
                .max(snapshot.stats.stats_observed_at_unix_ms);
            append_unique_strings(
                &mut backend_stats.stats_capabilities,
                &snapshot.stats.stats_capabilities,
            );
            append_unique_strings(
                &mut backend_stats.stats_sources,
                &snapshot.stats.stats_sources,
            );
            rtt = Some(match rtt {
                Some(current) => current.min(snapshot.rtt),
                None => snapshot.rtt,
            });
            true
        })
        .await;

    Some((backend_stats, rtt?, active_backend_count))
}

fn append_unique_strings(target: &mut Vec<String>, values: &[String]) {
    for value in values {
        if !target.iter().any(|existing| existing == value) {
            target.push(value.clone());
        }
    }
}

async fn cluster_snapshot_for_target(
    cluster_id: &str,
    cluster_state: &RoutedClusterState,
) -> Option<RoutedClusterSnapshot> {
    let stored = cluster_state.cluster_snapshot.lock().clone()?;

    let (backend_stats, rtt, active_backend_count) =
        collect_cluster_backend_aggregate(cluster_state).await?;
    Some(build_cluster_snapshot(
        cluster_id,
        &stored,
        &backend_stats,
        rtt,
        active_backend_count,
    ))
}

async fn target_has_routable_cluster_snapshot(target_state: &RoutingTargetState) -> bool {
    for (cluster_id, cluster_state) in collect_target_clusters(target_state).await {
        if cluster_snapshot_for_target(&cluster_id, &cluster_state)
            .await
            .is_some()
        {
            return true;
        }
    }
    false
}

async fn refresh_cluster_snapshot(
    cluster_id: &str,
    cluster_state: &RoutedClusterState,
    cluster_scoped_update: Option<ClusterScopedUpdate>,
) {
    let mut present_backend_ids = HashSet::new();
    let Some((backend_stats, rtt, active_backend_count)) =
        collect_cluster_backend_aggregate(cluster_state).await
    else {
        *cluster_state.cluster_snapshot.lock() = None;
        return;
    };
    let _ = cluster_state
        .inference_servers
        .iter_async(|backend_id, _snapshot| {
            present_backend_ids.insert(backend_id.clone());
            true
        })
        .await;

    let mut stored_opt = cluster_state.cluster_snapshot.lock();
    if stored_opt.is_none() && cluster_scoped_update.is_none() {
        return;
    }

    let next_cluster_scoped = {
        let stored = stored_opt.get_or_insert_with(|| {
            let initial_update = cluster_scoped_update
                .clone()
                .expect("cluster snapshot initialization requires a cluster-scoped update");
            StoredClusterSnapshot {
                snapshot: RoutedClusterSnapshot {
                    cluster_id: cluster_id.to_string(),
                    stats: ModelStats::default(),
                    rtt,
                    snapshot_updated_at: initial_update.snapshot_updated_at,
                    status: initial_update.status,
                    active_backend_count,
                },
                cluster_stats_source_backend_id: initial_update.source_backend_id.clone(),
                cluster_stats_base: initial_update.stats.clone(),
                raw_cluster_updates: HashMap::new(),
                pending_cluster_reservations: BTreeMap::new(),
            }
        });

        if let Some(update) = cluster_scoped_update.clone() {
            stored
                .raw_cluster_updates
                .insert(update.source_backend_id.clone(), update);
        }
        stored
            .raw_cluster_updates
            .retain(|backend_id, _| present_backend_ids.contains(backend_id));
        stored
            .pending_cluster_reservations
            .retain(|_, pending| present_backend_ids.contains(&pending.inference_server_id));

        if let Some(update) = cluster_scoped_update.as_ref() {
            stored
                .pending_cluster_reservations
                .retain(|_, pending| pending.inference_server_id != update.source_backend_id);
        }

        if let Some(update) = cluster_scoped_update {
            Some(update)
        } else if let Some(update) = stored
            .raw_cluster_updates
            .get(&stored.cluster_stats_source_backend_id)
            .cloned()
        {
            Some(update)
        } else {
            stored
                .raw_cluster_updates
                .values()
                .max_by_key(|update| update.snapshot_updated_at)
                .cloned()
        }
    };

    let Some(next_cluster_scoped) = next_cluster_scoped else {
        *stored_opt = None;
        return;
    };

    let stored = stored_opt
        .as_mut()
        .expect("stored cluster snapshot should exist after initialization");

    stored.cluster_stats_source_backend_id = next_cluster_scoped.source_backend_id.clone();
    stored.cluster_stats_base = next_cluster_scoped.stats.clone();
    stored.snapshot.snapshot_updated_at = next_cluster_scoped.snapshot_updated_at;
    stored.snapshot.status = next_cluster_scoped.status;
    stored.snapshot = build_cluster_snapshot(
        cluster_id,
        stored,
        &backend_stats,
        rtt,
        active_backend_count,
    );
}

fn duplicate_registration_status(
    inference_server_id: &str,
    existing: &Arc<RegisteredInferenceServerState>,
) -> Status {
    warn!(
        inference_server_id = %inference_server_id,
        existing_url = %existing.identity.inference_server_url,
        existing_reverse_tunnel = existing.identity.reverse_tunnel,
        "duplicate inference_server_id: another stream already registered this id"
    );
    Status::already_exists(format!(
        "inference_server_id '{}' is already registered",
        inference_server_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use stargate_proto::pb::InferenceServerModelRegistration;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn model_registration(status: i32) -> InferenceServerModelRegistration {
        InferenceServerModelRegistration {
            stats: Some(ModelStats::default()),
            status,
            calibration_state: CalibrationState::Unknown as i32,
        }
    }

    fn model_registration_with_stats(
        status: i32,
        stats: ModelStats,
    ) -> InferenceServerModelRegistration {
        InferenceServerModelRegistration {
            stats: Some(stats),
            status,
            calibration_state: CalibrationState::Unknown as i32,
        }
    }

    async fn running_registration(
        state: &StargateState,
        id: &str,
        url: &str,
        routing_key: Option<&str>,
    ) -> RunningRegistration {
        running_registration_in_cluster(state, id, id, url, routing_key).await
    }

    async fn running_registration_in_cluster(
        state: &StargateState,
        id: &str,
        cluster_id: &str,
        url: &str,
        routing_key: Option<&str>,
    ) -> RunningRegistration {
        let identity = RegistrationIdentity {
            inference_server_id: id.to_string(),
            cluster_id: cluster_id.to_string(),
            inference_server_url: url.to_string(),
            routing_key: routing_key.map(ToOwned::to_owned),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        state.begin_registration(&identity).await.unwrap()
    }

    async fn running_coordinated_registration_in_cluster(
        state: &StargateState,
        id: &str,
        cluster_id: &str,
        url: &str,
        routing_key: Option<&str>,
    ) -> RunningRegistration {
        let identity = RegistrationIdentity {
            inference_server_id: id.to_string(),
            cluster_id: cluster_id.to_string(),
            inference_server_url: url.to_string(),
            routing_key: routing_key.map(ToOwned::to_owned),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        state.begin_registration(&identity).await.unwrap()
    }

    fn make_target(routing_key: Option<&str>, model_id: &str) -> RoutingTargetKey {
        RoutingTargetKey {
            routing_key: routing_key.map(ToOwned::to_owned),
            model_id: model_id.to_string(),
        }
    }

    fn registration_update(
        inference_server_id: &str,
        cluster_id: &str,
        url: &str,
        model_id: &str,
        status: i32,
        stats: ModelStats,
        coordinated_calibration: bool,
    ) -> InferenceServerRegistration {
        InferenceServerRegistration {
            inference_server_id: inference_server_id.to_string(),
            cluster_id: cluster_id.to_string(),
            inference_server_url: url.to_string(),
            models: HashMap::from([(
                model_id.to_string(),
                model_registration_with_stats(status, stats),
            )]),
            reverse_tunnel: false,
            coordinated_calibration,
        }
    }

    fn mark_model_calibration_complete(update: &mut InferenceServerRegistration, model_id: &str) {
        update
            .models
            .get_mut(model_id)
            .expect("model should exist in update")
            .calibration_state = CalibrationState::Complete as i32;
    }

    #[tokio::test]
    async fn apply_registration_update_removes_models_no_longer_advertised() {
        let state = StargateState::default();
        let mut running =
            running_registration(&state, "inst-1", "quic://127.0.0.1:1234", Some("rk-1")).await;
        let initial_update = InferenceServerRegistration {
            inference_server_id: "inst-1".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:1234".to_string(),
            models: HashMap::from([(
                "model-a".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        state
            .apply_registration_update(
                &mut running,
                &initial_update,
                true,
                Some(Duration::from_millis(10)),
            )
            .await;

        let update = InferenceServerRegistration {
            inference_server_id: "inst-1".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:1234".to_string(),
            models: HashMap::from([(
                "model-b".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(10)))
            .await;

        assert!(
            state
                .candidates_for_target(&make_target(Some("rk-1"), "model-a"))
                .await
                .is_empty()
        );
        assert_eq!(
            state
                .candidates_for_target(&make_target(Some("rk-1"), "model-b"))
                .await
                .len(),
            1
        );
        assert!(
            state
                .target_state(&make_target(Some("rk-1"), "model-a"))
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn registered_inactive_model_is_known_without_routable_candidates() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-known-inactive",
            "quic://127.0.0.1:1234",
            Some("rk-known"),
        )
        .await;
        let target = make_target(Some("rk-known"), "model-known");
        let update = registration_update(
            "inst-known-inactive",
            "inst-known-inactive",
            "quic://127.0.0.1:1234",
            "model-known",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            false,
        );

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(10)))
            .await;

        assert!(state.has_registered_model_for_target(&target).await);
        assert!(state.candidates_for_target(&target).await.is_empty());
        assert!(
            !state
                .has_registered_model_for_target(&make_target(Some("wrong-rk"), "model-known"))
                .await
        );

        state.end_registration("inst-known-inactive").await;
        assert!(!state.has_registered_model_for_target(&target).await);
    }

    #[tokio::test]
    async fn active_registration_keeps_connection_rtt_in_snapshot() {
        let state = StargateState::default();
        let mut running =
            running_registration(&state, "inst-rtt", "quic://127.0.0.1:7777", Some("rk-rtt")).await;
        let update = InferenceServerRegistration {
            inference_server_id: "inst-rtt".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:7777".to_string(),
            models: HashMap::from([(
                "model-rtt".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        let expected_rtt = Duration::from_millis(42);
        state
            .apply_registration_update(&mut running, &update, true, Some(expected_rtt))
            .await;

        let candidates = state
            .candidates_for_target(&make_target(Some("rk-rtt"), "model-rtt"))
            .await;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].rtt, expected_rtt);
        assert!(matches!(
            candidates[0].delivery_target,
            DeliveryTarget::Local { .. }
        ));
    }

    #[tokio::test]
    async fn coordinated_calibration_assigns_one_owner_and_gates_siblings_until_complete() {
        let state = StargateState::default();
        let mut running_a = running_coordinated_registration_in_cluster(
            &state,
            "inst-a",
            "cluster-cal",
            "quic://127.0.0.1:1111",
            Some("rk-cal"),
        )
        .await;
        let mut running_b = running_coordinated_registration_in_cluster(
            &state,
            "inst-b",
            "cluster-cal",
            "quic://127.0.0.1:2222",
            Some("rk-cal"),
        )
        .await;
        let target = make_target(Some("rk-cal"), "model-cal");

        let update_a = registration_update(
            "inst-a",
            "cluster-cal",
            "quic://127.0.0.1:1111",
            "model-cal",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].model_id, "model-cal");
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let update_b_active_without_calibration = registration_update(
            "inst-b",
            "cluster-cal",
            "quic://127.0.0.1:2222",
            "model-cal",
            InferenceServerStatus::Active as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b_active_without_calibration,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Waiting as i32);
        assert!(state.candidates_for_target(&target).await.is_empty());

        let mut update_a_complete = registration_update(
            "inst-a",
            "cluster-cal",
            "quic://127.0.0.1:1111",
            "model-cal",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 150.0,
                ..ModelStats::default()
            },
            true,
        );
        mark_model_calibration_complete(&mut update_a_complete, "model-cal");
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a_complete,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);
        assert_eq!(directives[0].last_mean_input_tps, 150.0);

        let update_b_complete = registration_update(
            "inst-b",
            "cluster-cal",
            "quic://127.0.0.1:2222",
            "model-cal",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 120.0,
                ..ModelStats::default()
            },
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b_complete,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);
        assert_eq!(directives[0].last_mean_input_tps, 150.0);

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].active_backend_count, 2);
        assert_eq!(clusters[0].stats.last_mean_input_tps, 270.0);
    }

    #[tokio::test]
    async fn coordinated_calibration_reassigns_when_owner_disconnects_before_completion() {
        let state = StargateState::default();
        let mut running_a = running_coordinated_registration_in_cluster(
            &state,
            "inst-owner",
            "cluster-reassign",
            "quic://127.0.0.1:1111",
            Some("rk-reassign"),
        )
        .await;
        let mut running_b = running_coordinated_registration_in_cluster(
            &state,
            "inst-next",
            "cluster-reassign",
            "quic://127.0.0.1:2222",
            Some("rk-reassign"),
        )
        .await;

        let update_a = registration_update(
            "inst-owner",
            "cluster-reassign",
            "quic://127.0.0.1:1111",
            "model-reassign",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        state.end_registration("inst-owner").await;

        let update_b = registration_update(
            "inst-next",
            "cluster-reassign",
            "quic://127.0.0.1:2222",
            "model-reassign",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Run as i32);
    }

    #[tokio::test]
    async fn coordinated_calibration_accepts_precalibrated_backend_registration() {
        let state = StargateState::default();
        let mut running = running_coordinated_registration_in_cluster(
            &state,
            "inst-precalibrated",
            "cluster-precalibrated",
            "quic://127.0.0.1:1111",
            Some("rk-precalibrated"),
        )
        .await;
        let target = make_target(Some("rk-precalibrated"), "model-precalibrated");

        let mut update = registration_update(
            "inst-precalibrated",
            "cluster-precalibrated",
            "quic://127.0.0.1:1111",
            "model-precalibrated",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 123.0,
                ..ModelStats::default()
            },
            true,
        );
        mark_model_calibration_complete(&mut update, "model-precalibrated");

        let directives = state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;

        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);
        assert_eq!(directives[0].last_mean_input_tps, 123.0);
        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].stats.last_mean_input_tps, 123.0);
        assert_eq!(clusters[0].stats.last_mean_input_tps, 123.0);
    }

    #[tokio::test]
    async fn runtime_last_mean_input_tps_without_complete_state_does_not_complete_calibration() {
        let state = StargateState::default();
        let mut running = running_coordinated_registration_in_cluster(
            &state,
            "inst-runtime-only",
            "cluster-runtime-only",
            "quic://127.0.0.1:1111",
            Some("rk-runtime-only"),
        )
        .await;
        let target = make_target(Some("rk-runtime-only"), "model-runtime-only");

        let update_initial = registration_update(
            "inst-runtime-only",
            "cluster-runtime-only",
            "quic://127.0.0.1:1111",
            "model-runtime-only",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running,
                &update_initial,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let update_runtime_only = registration_update(
            "inst-runtime-only",
            "cluster-runtime-only",
            "quic://127.0.0.1:1111",
            "model-runtime-only",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 999.0,
                ..ModelStats::default()
            },
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running,
                &update_runtime_only,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Run as i32);
        assert_eq!(directives[0].last_mean_input_tps, 0.0);
        assert!(
            state
                .cluster_candidates_for_target(&target)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn coordinated_calibration_reassigns_when_owner_removes_model_before_completion() {
        let state = StargateState::default();
        let mut running_a = running_coordinated_registration_in_cluster(
            &state,
            "inst-owner",
            "cluster-remove-model",
            "quic://127.0.0.1:1111",
            Some("rk-remove-model"),
        )
        .await;
        let mut running_b = running_coordinated_registration_in_cluster(
            &state,
            "inst-next",
            "cluster-remove-model",
            "quic://127.0.0.1:2222",
            Some("rk-remove-model"),
        )
        .await;

        let update_a = registration_update(
            "inst-owner",
            "cluster-remove-model",
            "quic://127.0.0.1:1111",
            "model-remove",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let update_b = registration_update(
            "inst-next",
            "cluster-remove-model",
            "quic://127.0.0.1:2222",
            "model-remove",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Waiting as i32);

        let remove_model_update = InferenceServerRegistration {
            inference_server_id: "inst-owner".to_string(),
            cluster_id: "cluster-remove-model".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::new(),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &remove_model_update,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert!(directives.is_empty());

        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Run as i32);
    }

    #[tokio::test]
    async fn coordinated_calibration_does_not_complete_from_waiting_backend_stats() {
        let state = StargateState::default();
        let mut running_a = running_coordinated_registration_in_cluster(
            &state,
            "inst-owner",
            "cluster-waiting-stats",
            "quic://127.0.0.1:1111",
            Some("rk-waiting-stats"),
        )
        .await;
        let mut running_b = running_coordinated_registration_in_cluster(
            &state,
            "inst-waiting",
            "cluster-waiting-stats",
            "quic://127.0.0.1:2222",
            Some("rk-waiting-stats"),
        )
        .await;

        let update_a = registration_update(
            "inst-owner",
            "cluster-waiting-stats",
            "quic://127.0.0.1:1111",
            "model-waiting-stats",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let update_b_active_with_capacity = registration_update(
            "inst-waiting",
            "cluster-waiting-stats",
            "quic://127.0.0.1:2222",
            "model-waiting-stats",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 999.0,
                ..ModelStats::default()
            },
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b_active_with_capacity,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Waiting as i32);

        let remove_model_update = InferenceServerRegistration {
            inference_server_id: "inst-owner".to_string(),
            cluster_id: "cluster-waiting-stats".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::new(),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        state
            .apply_registration_update(
                &mut running_a,
                &remove_model_update,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b_active_with_capacity,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Run as i32);
        assert_eq!(directives[0].last_mean_input_tps, 0.0);
    }

    #[tokio::test]
    async fn coordinated_calibration_recalibrates_when_completed_model_leaves_cluster() {
        let state = StargateState::default();
        let mut running = running_coordinated_registration_in_cluster(
            &state,
            "inst-owner",
            "cluster-readd-model",
            "quic://127.0.0.1:1111",
            Some("rk-readd-model"),
        )
        .await;

        let update_initial = registration_update(
            "inst-owner",
            "cluster-readd-model",
            "quic://127.0.0.1:1111",
            "model-readd",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running,
                &update_initial,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let mut update_complete = registration_update(
            "inst-owner",
            "cluster-readd-model",
            "quic://127.0.0.1:1111",
            "model-readd",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 321.0,
                ..ModelStats::default()
            },
            true,
        );
        mark_model_calibration_complete(&mut update_complete, "model-readd");
        let directives = state
            .apply_registration_update(
                &mut running,
                &update_complete,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);

        let remove_model_update = InferenceServerRegistration {
            inference_server_id: "inst-owner".to_string(),
            cluster_id: "cluster-readd-model".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::new(),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        let directives = state
            .apply_registration_update(
                &mut running,
                &remove_model_update,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert!(directives.is_empty());

        let directives = state
            .apply_registration_update(
                &mut running,
                &update_initial,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Run as i32);
        assert_eq!(directives[0].last_mean_input_tps, 0.0);
    }

    #[tokio::test]
    async fn coordinated_calibration_keeps_completed_model_while_peer_still_registered() {
        let state = StargateState::default();
        let mut running_a = running_coordinated_registration_in_cluster(
            &state,
            "inst-owner",
            "cluster-keep-model",
            "quic://127.0.0.1:1111",
            Some("rk-keep-model"),
        )
        .await;
        let mut running_b = running_coordinated_registration_in_cluster(
            &state,
            "inst-peer",
            "cluster-keep-model",
            "quic://127.0.0.1:2222",
            Some("rk-keep-model"),
        )
        .await;

        let update_a_initial = registration_update(
            "inst-owner",
            "cluster-keep-model",
            "quic://127.0.0.1:1111",
            "model-keep",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a_initial,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let mut update_a_complete = registration_update(
            "inst-owner",
            "cluster-keep-model",
            "quic://127.0.0.1:1111",
            "model-keep",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 654.0,
                ..ModelStats::default()
            },
            true,
        );
        mark_model_calibration_complete(&mut update_a_complete, "model-keep");
        let directives = state
            .apply_registration_update(
                &mut running_a,
                &update_a_complete,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);

        let update_b = registration_update(
            "inst-peer",
            "cluster-keep-model",
            "quic://127.0.0.1:2222",
            "model-keep",
            InferenceServerStatus::Active as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);

        let remove_model_update = InferenceServerRegistration {
            inference_server_id: "inst-owner".to_string(),
            cluster_id: "cluster-keep-model".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::new(),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        state
            .apply_registration_update(
                &mut running_a,
                &remove_model_update,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let directives = state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives.len(), 1);
        assert_eq!(directives[0].state, CalibrationState::Complete as i32);
        assert_eq!(directives[0].last_mean_input_tps, 654.0);
    }

    #[tokio::test]
    async fn coordinated_calibration_capacity_follows_active_reports_after_model_removed() {
        let state = StargateState::default();
        let mut running_coordinated = running_coordinated_registration_in_cluster(
            &state,
            "inst-coordinated",
            "cluster-clear-capacity",
            "quic://127.0.0.1:1111",
            Some("rk-clear-capacity"),
        )
        .await;
        let mut running_legacy = running_registration_in_cluster(
            &state,
            "inst-legacy",
            "cluster-clear-capacity",
            "quic://127.0.0.1:2222",
            Some("rk-clear-capacity"),
        )
        .await;
        let target = make_target(Some("rk-clear-capacity"), "model-clear-capacity");

        let update_initial = registration_update(
            "inst-coordinated",
            "cluster-clear-capacity",
            "quic://127.0.0.1:1111",
            "model-clear-capacity",
            InferenceServerStatus::Inactive as i32,
            ModelStats::default(),
            true,
        );
        let directives = state
            .apply_registration_update(
                &mut running_coordinated,
                &update_initial,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        assert_eq!(directives[0].state, CalibrationState::Run as i32);

        let mut update_complete = registration_update(
            "inst-coordinated",
            "cluster-clear-capacity",
            "quic://127.0.0.1:1111",
            "model-clear-capacity",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 777.0,
                ..ModelStats::default()
            },
            true,
        );
        mark_model_calibration_complete(&mut update_complete, "model-clear-capacity");
        state
            .apply_registration_update(
                &mut running_coordinated,
                &update_complete,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let update_legacy = registration_update(
            "inst-legacy",
            "cluster-clear-capacity",
            "quic://127.0.0.1:2222",
            "model-clear-capacity",
            InferenceServerStatus::Active as i32,
            ModelStats {
                last_mean_input_tps: 100.0,
                ..ModelStats::default()
            },
            false,
        );
        state
            .apply_registration_update(
                &mut running_legacy,
                &update_legacy,
                true,
                Some(Duration::from_millis(6)),
            )
            .await;

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].stats.last_mean_input_tps, 877.0);

        let remove_model_update = InferenceServerRegistration {
            inference_server_id: "inst-coordinated".to_string(),
            cluster_id: "cluster-clear-capacity".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::new(),
            reverse_tunnel: false,
            coordinated_calibration: true,
        };
        state
            .apply_registration_update(
                &mut running_coordinated,
                &remove_model_update,
                true,
                Some(Duration::from_millis(7)),
            )
            .await;

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].active_backend_count, 1);
        assert_eq!(clusters[0].stats.last_mean_input_tps, 100.0);
    }

    #[tokio::test]
    async fn reservation_updates_local_snapshot_until_next_registration_update() {
        let state = StargateState::default();
        let mut running =
            running_registration(&state, "inst-res", "quic://127.0.0.1:8888", Some("rk-res")).await;
        let target = make_target(Some("rk-res"), "model-res");
        let update = InferenceServerRegistration {
            inference_server_id: "inst-res".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:8888".to_string(),
            models: HashMap::from([(
                "model-res".to_string(),
                InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps: 100.0,
                        max_engine_concurrency: 8,
                        queue_time_estimate_ms_by_priority: HashMap::from([(4, 5)]),
                        ..ModelStats::default()
                    }),
                    status: InferenceServerStatus::Active as i32,
                    calibration_state: CalibrationState::Unknown as i32,
                },
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let _reservation = state
            .reserve_inference_server_for_target(&target, "inst-res", Some(37), 4)
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(candidates[0].stats.num_running_queries, 1);
        assert_eq!(candidates[0].stats.queue_size, 1);
        assert_eq!(candidates[0].stats.total_query_input_size, 37);
        assert_eq!(candidates[0].stats.queued_input_size, 37);
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&4),
            Some(&375)
        );

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(candidates[0].stats.num_running_queries, 0);
        assert_eq!(candidates[0].stats.queue_size, 0);
        assert_eq!(candidates[0].stats.total_query_input_size, 0);
        assert_eq!(candidates[0].stats.queued_input_size, 0);
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&4),
            Some(&5)
        );

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters[0].stats.num_running_queries, 0);
        assert_eq!(clusters[0].stats.queue_size, 0);
        assert_eq!(clusters[0].stats.total_query_input_size, 0);
        assert_eq!(clusters[0].stats.queued_input_size, 0);
        assert_eq!(
            clusters[0].stats.queue_time_estimate_ms_by_priority.get(&4),
            Some(&5)
        );
    }

    #[tokio::test]
    async fn released_reservation_restores_local_snapshot_before_registration_update() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-release",
            "quic://127.0.0.1:8888",
            Some("rk-release"),
        )
        .await;
        let target = make_target(Some("rk-release"), "model-release");
        let update = InferenceServerRegistration {
            inference_server_id: "inst-release".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:8888".to_string(),
            models: HashMap::from([(
                "model-release".to_string(),
                InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps: 100.0,
                        queue_time_estimate_ms_by_priority: HashMap::from([(4, 5)]),
                        ..ModelStats::default()
                    }),
                    status: InferenceServerStatus::Active as i32,
                    calibration_state: CalibrationState::Unknown as i32,
                },
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let reservation = state
            .reserve_inference_server_for_target(&target, "inst-release", Some(37), 4)
            .await
            .expect("active backend should accept reservation");

        state
            .release_inference_server_reservation_for_target(&target, reservation)
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(candidates[0].stats.num_running_queries, 0);
        assert_eq!(candidates[0].stats.total_query_input_size, 0);
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&4),
            Some(&5)
        );

        let consumed_by_heartbeat = state
            .reserve_inference_server_for_target(&target, "inst-release", Some(10), 4)
            .await
            .expect("active backend should accept reservation");
        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let still_pending = state
            .reserve_inference_server_for_target(&target, "inst-release", Some(20), 4)
            .await
            .expect("active backend should accept reservation");

        state
            .release_inference_server_reservation_for_target(&target, consumed_by_heartbeat)
            .await;
        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(candidates[0].stats.num_running_queries, 1);
        assert_eq!(candidates[0].stats.total_query_input_size, 20);
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&4),
            Some(&205)
        );

        state
            .release_inference_server_reservation_for_target(&target, still_pending)
            .await;
        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(candidates[0].stats.num_running_queries, 0);
        assert_eq!(candidates[0].stats.total_query_input_size, 0);
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&4),
            Some(&5)
        );
    }

    #[tokio::test]
    async fn shared_cluster_reservation_updates_cluster_snapshot_even_when_other_backend_is_latest()
    {
        let state = StargateState::default();
        let mut running_a = running_registration_in_cluster(
            &state,
            "inst-a",
            "cluster-reserved",
            "quic://127.0.0.1:1111",
            Some("rk-reserved"),
        )
        .await;
        let mut running_b = running_registration_in_cluster(
            &state,
            "inst-b",
            "cluster-reserved",
            "quic://127.0.0.1:2222",
            Some("rk-reserved"),
        )
        .await;
        let target = make_target(Some("rk-reserved"), "model-reserved");

        let update_a = InferenceServerRegistration {
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-reserved".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::from([(
                "model-reserved".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        output_tps: 0.0,
                        last_mean_input_tps: 100.0,
                        max_output_tps: 50.0,
                        queue_size: 0,
                        queued_input_size: 0,
                        kv_cache_capacity_tokens: 1000,
                        kv_cache_used_tokens: 100,
                        kv_cache_free_tokens: 900,
                        num_running_queries: 3,
                        max_engine_concurrency: 8,
                        total_query_input_size: 30,
                        queue_time_estimate_ms_by_priority: HashMap::from([(4, 10)]),
                        ..ModelStats::default()
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        let update_b = InferenceServerRegistration {
            inference_server_id: "inst-b".to_string(),
            cluster_id: "cluster-reserved".to_string(),
            inference_server_url: "quic://127.0.0.1:2222".to_string(),
            models: HashMap::from([(
                "model-reserved".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        output_tps: 0.0,
                        last_mean_input_tps: 100.0,
                        max_output_tps: 60.0,
                        queue_size: 0,
                        queued_input_size: 0,
                        kv_cache_capacity_tokens: 2000,
                        kv_cache_used_tokens: 500,
                        kv_cache_free_tokens: 1500,
                        num_running_queries: 7,
                        max_engine_concurrency: 9,
                        total_query_input_size: 70,
                        queue_time_estimate_ms_by_priority: HashMap::from([(4, 5)]),
                        ..ModelStats::default()
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let _reservation = state
            .reserve_inference_server_for_target(&target, "inst-a", Some(37), 4)
            .await;

        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        let cluster = &clusters[0];
        assert_eq!(cluster.stats.queue_size, 1);
        assert_eq!(cluster.stats.queued_input_size, 37);
        assert_eq!(cluster.stats.num_running_queries, 8);
        assert_eq!(cluster.stats.total_query_input_size, 107);
        // Reservation delta uses summed backend input capacity:
        // existing 5ms + ceil(37 tokens / 200 input TPS * 1000) = 190ms.
        assert_eq!(
            cluster.stats.queue_time_estimate_ms_by_priority.get(&4),
            Some(&190)
        );
    }

    #[tokio::test]
    async fn reservation_inserts_request_priority_and_preserves_more_urgent_bucket() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-priority-res",
            "quic://127.0.0.1:8888",
            Some("rk-priority-res"),
        )
        .await;
        let target = make_target(Some("rk-priority-res"), "model-priority-res");
        let update = InferenceServerRegistration {
            inference_server_id: "inst-priority-res".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:8888".to_string(),
            models: HashMap::from([(
                "model-priority-res".to_string(),
                InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps: 100.0,
                        queue_time_estimate_ms_by_priority: HashMap::from([(2, 5)]),
                        ..ModelStats::default()
                    }),
                    status: InferenceServerStatus::Active as i32,
                    calibration_state: CalibrationState::Unknown as i32,
                },
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let _reservation = state
            .reserve_inference_server_for_target(&target, "inst-priority-res", Some(10), 3)
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&2),
            Some(&5)
        );
        assert_eq!(
            candidates[0]
                .stats
                .queue_time_estimate_ms_by_priority
                .get(&3),
            Some(&105)
        );
    }

    #[tokio::test]
    async fn reservation_updates_lower_urgency_cumulative_priority_buckets() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-priority-cumulative",
            "quic://127.0.0.1:8888",
            Some("rk-priority-cumulative"),
        )
        .await;
        let target = make_target(Some("rk-priority-cumulative"), "model-priority-cumulative");
        let update = InferenceServerRegistration {
            inference_server_id: "inst-priority-cumulative".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:8888".to_string(),
            models: HashMap::from([(
                "model-priority-cumulative".to_string(),
                InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps: 100.0,
                        queue_time_estimate_ms_by_priority: HashMap::from([(1, 10), (4, 40)]),
                        ..ModelStats::default()
                    }),
                    status: InferenceServerStatus::Active as i32,
                    calibration_state: CalibrationState::Unknown as i32,
                },
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let _reservation = state
            .reserve_inference_server_for_target(&target, "inst-priority-cumulative", Some(10), 2)
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        let priority_estimates = &candidates[0].stats.queue_time_estimate_ms_by_priority;
        assert_eq!(priority_estimates.get(&1), Some(&10));
        assert_eq!(priority_estimates.get(&2), Some(&110));
        assert_eq!(priority_estimates.get(&4), Some(&140));
    }

    #[test]
    fn reservation_updates_existing_request_priority_and_lower_urgency_buckets() {
        let mut stats = ModelStats {
            last_mean_input_tps: 100.0,
            queue_time_estimate_ms_by_priority: HashMap::from([(1, 10), (2, 100), (4, 400)]),
            ..ModelStats::default()
        };

        update_reserved_priority_queue_time(&mut stats, 10, 2);

        let priority_estimates = &stats.queue_time_estimate_ms_by_priority;
        assert_eq!(priority_estimates.get(&1), Some(&10));
        assert_eq!(priority_estimates.get(&2), Some(&200));
        assert_eq!(priority_estimates.get(&4), Some(&500));
    }

    #[test]
    fn reservation_saturates_priority_queue_estimates() {
        let mut stats = ModelStats {
            last_mean_input_tps: 1.0,
            queue_time_estimate_ms_by_priority: HashMap::from([
                (0, u64::MAX - 1),
                (2, u64::MAX - 2),
            ]),
            ..ModelStats::default()
        };

        update_reserved_priority_queue_time(&mut stats, 10, 0);

        let priority_estimates = &stats.queue_time_estimate_ms_by_priority;
        assert_eq!(priority_estimates.get(&0), Some(&u64::MAX));
        assert_eq!(priority_estimates.get(&2), Some(&u64::MAX));
    }

    #[tokio::test]
    async fn reservation_clears_priority_map_when_delta_cannot_be_computed() {
        let mut stats = ModelStats {
            last_mean_input_tps: 0.0,
            queue_time_estimate_ms_by_priority: HashMap::from([(1, 10), (4, 40)]),
            ..ModelStats::default()
        };

        update_reserved_priority_queue_time(&mut stats, 10, 2);

        assert!(stats.queue_time_estimate_ms_by_priority.is_empty());
    }

    #[test]
    fn queue_time_estimate_helper_uses_sparse_priority_and_aggregate_fallback() {
        let priority_stats = ModelStats {
            last_mean_input_tps: 100.0,
            queued_input_size: 25,
            queue_time_estimate_ms_by_priority: HashMap::from([(1, 10), (4, 40)]),
            ..ModelStats::default()
        };
        assert_eq!(
            crate::queue_estimate::queue_time_estimate_ms_for_priority(&priority_stats, 3),
            Some(10)
        );

        let aggregate_stats = ModelStats {
            last_mean_input_tps: 100.0,
            queued_input_size: 25,
            ..ModelStats::default()
        };
        assert_eq!(
            crate::queue_estimate::queue_time_estimate_ms_for_priority(&aggregate_stats, 3),
            Some(250)
        );

        let invalid_capacity_stats = ModelStats {
            last_mean_input_tps: 0.0,
            queued_input_size: 25,
            ..ModelStats::default()
        };
        assert_eq!(
            crate::queue_estimate::queue_time_estimate_ms_for_priority(&invalid_capacity_stats, 3),
            None
        );
    }

    #[test]
    fn queue_time_estimate_helper_treats_lower_priority_only_work_as_known_zero() {
        let stats = ModelStats {
            last_mean_input_tps: 100.0,
            queued_input_size: 25,
            queue_time_estimate_ms_by_priority: HashMap::from([(4, 250)]),
            ..ModelStats::default()
        };

        assert_eq!(
            crate::queue_estimate::queue_time_estimate_ms_for_priority(&stats, 0),
            Some(0)
        );
    }

    #[tokio::test]
    async fn reservation_inserts_high_priority_estimate_when_only_lower_priority_work_exists() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-priority-clear",
            "quic://127.0.0.1:8888",
            Some("rk-priority-clear"),
        )
        .await;
        let target = make_target(Some("rk-priority-clear"), "model-priority-clear");
        let update = InferenceServerRegistration {
            inference_server_id: "inst-priority-clear".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:8888".to_string(),
            models: HashMap::from([(
                "model-priority-clear".to_string(),
                InferenceServerModelRegistration {
                    stats: Some(ModelStats {
                        last_mean_input_tps: 100.0,
                        queue_time_estimate_ms_by_priority: HashMap::from([(4, 5)]),
                        ..ModelStats::default()
                    }),
                    status: InferenceServerStatus::Active as i32,
                    calibration_state: CalibrationState::Unknown as i32,
                },
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(5)))
            .await;
        let _reservation = state
            .reserve_inference_server_for_target(&target, "inst-priority-clear", Some(10), 0)
            .await;

        let candidates = state.cluster_candidates_for_target(&target).await;
        assert_eq!(
            candidates[0].stats.queue_time_estimate_ms_by_priority,
            HashMap::from([(0, 100), (4, 105)])
        );
    }

    #[tokio::test]
    async fn inactive_registration_is_not_routable() {
        let state = StargateState::default();
        let mut running = running_registration(
            &state,
            "inst-inactive",
            "quic://127.0.0.1:9999",
            Some("rk-in"),
        )
        .await;
        let update = InferenceServerRegistration {
            inference_server_id: "inst-inactive".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:9999".to_string(),
            models: HashMap::from([(
                "model-r".to_string(),
                model_registration(InferenceServerStatus::Inactive as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(7)))
            .await;
        assert!(
            state
                .candidates_for_target(&make_target(Some("rk-in"), "model-r"))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn active_models_snapshot_refresh_reports_routable_models() {
        let state = StargateState::default();
        let mut active = running_registration(
            &state,
            "inst-active",
            "quic://127.0.0.1:1111",
            Some("rk-list"),
        )
        .await;
        let mut active_without_rtt = running_registration(
            &state,
            "inst-no-rtt",
            "quic://127.0.0.1:2222",
            Some("rk-list"),
        )
        .await;
        let mut inactive = running_registration(
            &state,
            "inst-inactive-list",
            "quic://127.0.0.1:3333",
            Some("rk-list"),
        )
        .await;

        state
            .apply_registration_update(
                &mut active,
                &InferenceServerRegistration {
                    inference_server_id: "inst-active".to_string(),
                    cluster_id: String::new(),
                    inference_server_url: "quic://127.0.0.1:1111".to_string(),
                    models: HashMap::from([(
                        "model-listed".to_string(),
                        model_registration(InferenceServerStatus::Active as i32),
                    )]),
                    reverse_tunnel: false,
                    coordinated_calibration: false,
                },
                true,
                Some(Duration::from_millis(7)),
            )
            .await;
        state
            .apply_registration_update(
                &mut active_without_rtt,
                &InferenceServerRegistration {
                    inference_server_id: "inst-no-rtt".to_string(),
                    cluster_id: String::new(),
                    inference_server_url: "quic://127.0.0.1:2222".to_string(),
                    models: HashMap::from([(
                        "model-not-ready".to_string(),
                        model_registration(InferenceServerStatus::Active as i32),
                    )]),
                    reverse_tunnel: false,
                    coordinated_calibration: false,
                },
                true,
                None,
            )
            .await;
        state
            .apply_registration_update(
                &mut inactive,
                &InferenceServerRegistration {
                    inference_server_id: "inst-inactive-list".to_string(),
                    cluster_id: String::new(),
                    inference_server_url: "quic://127.0.0.1:3333".to_string(),
                    models: HashMap::from([(
                        "model-inactive".to_string(),
                        model_registration(InferenceServerStatus::Inactive as i32),
                    )]),
                    reverse_tunnel: false,
                    coordinated_calibration: false,
                },
                true,
                Some(Duration::from_millis(7)),
            )
            .await;

        let before_refresh = state.list_active_models(Some("rk-list"), &[]);
        assert!(
            before_refresh.is_empty(),
            "registration updates must not maintain the ListModels snapshot: {before_refresh:?}"
        );

        state.refresh_active_models_snapshot().await;

        let models = state.list_active_models(Some("rk-list"), &[]);
        assert_eq!(models.len(), 1);
        assert_eq!(models[0], "model-listed");

        let filtered = state.list_active_models(Some("rk-list"), &["model-not-ready".to_string()]);
        assert!(filtered.is_empty(), "got: {filtered:?}");
    }

    #[tokio::test]
    async fn active_models_snapshot_uses_routable_cluster_snapshots() {
        let state = StargateState::default();
        let target = make_target(Some("rk-intermediate"), "model-intermediate");
        let target_state = state.target_state_or_insert(&target).await;
        let cluster_state =
            StargateState::cluster_state_or_insert(&target_state, "cluster-intermediate").await;

        let _ = cluster_state
            .inference_servers
            .upsert_async(
                "backend-intermediate".to_string(),
                RoutedInferenceServerSnapshot {
                    cluster_id: "cluster-intermediate".to_string(),
                    inference_server_id: "backend-intermediate".to_string(),
                    inference_server_url: "quic://127.0.0.1:4444".to_string(),
                    stats: ModelStats::default(),
                    rtt: Duration::from_millis(5),
                    snapshot_updated_at: Instant::now(),
                    status: InferenceServerStatus::Active as i32,
                    reverse_tunnel: false,
                    delivery_target: DeliveryTarget::Local {
                        inference_server_id: "backend-intermediate".to_string(),
                    },
                },
            )
            .await;

        assert!(
            state
                .cluster_candidates_for_target(&target)
                .await
                .is_empty(),
            "proxy routing source of truth should not consider this intermediate target routable"
        );

        state.refresh_active_models_snapshot().await;
        let listed = state.list_active_models(Some("rk-intermediate"), &[]);
        assert!(
            listed.is_empty(),
            "ListModels snapshot must not advertise targets without routable cluster snapshots: {listed:?}"
        );
    }

    #[tokio::test]
    async fn active_models_snapshot_filters_by_routing_key() {
        let state = StargateState::default();
        let mut running_a = running_registration(
            &state,
            "inst-list-rk-a",
            "quic://127.0.0.1:1111",
            Some("rk-a"),
        )
        .await;
        let mut running_b = running_registration(
            &state,
            "inst-list-rk-b",
            "quic://127.0.0.1:2222",
            Some("rk-b"),
        )
        .await;

        let update_a = InferenceServerRegistration {
            inference_server_id: "inst-list-rk-a".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::from([(
                "shared-list-model".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        let update_b = InferenceServerRegistration {
            inference_server_id: "inst-list-rk-b".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:2222".to_string(),
            models: HashMap::from([(
                "shared-list-model".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        state.refresh_active_models_snapshot().await;

        let unscoped = state.list_active_models(None, &[]);
        assert!(
            unscoped.is_empty(),
            "unscoped ListModels must not include keyed registrations: {unscoped:?}"
        );

        let models_a = state.list_active_models(Some("rk-a"), &[]);
        assert_eq!(models_a, vec!["shared-list-model"]);

        let models_b = state.list_active_models(Some("rk-b"), &[]);
        assert_eq!(models_b, vec!["shared-list-model"]);

        let wrong_key = state.list_active_models(Some("rk-c"), &[]);
        assert!(
            wrong_key.is_empty(),
            "ListModels must not leak models across routing keys: {wrong_key:?}"
        );

        let filtered = state.list_active_models(Some("rk-a"), &["shared-list-model".to_string()]);
        assert_eq!(filtered, vec!["shared-list-model"]);
    }

    #[tokio::test]
    async fn active_models_snapshot_is_eventually_consistent() {
        let state = StargateState::default();
        let target = make_target(None, "model-list-eventual");

        state
            .upsert_inference_server_target(
                &target,
                RoutedInferenceServerSnapshotInput {
                    cluster_id: "cluster-list-eventual",
                    inference_server_id: "backend-list-eventual",
                    inference_server_url: "quic://127.0.0.1:2222",
                    stats: ModelStats::default(),
                    rtt: Duration::from_millis(5),
                    snapshot_updated_at: Instant::now(),
                    status: InferenceServerStatus::Active as i32,
                    reverse_tunnel: false,
                    delivery_target: DeliveryTarget::Local {
                        inference_server_id: "backend-list-eventual".to_string(),
                    },
                },
            )
            .await;

        assert!(
            state.list_active_models(None, &[]).is_empty(),
            "registration updates should not synchronously refresh the discovery snapshot"
        );

        state.refresh_active_models_snapshot().await;
        let listed = state.list_active_models(None, &[]);
        assert_eq!(listed.len(), 1, "got: {listed:?}");
        assert_eq!(listed[0], "model-list-eventual");

        state
            .remove_inference_server_from_target(
                "backend-list-eventual",
                "cluster-list-eventual",
                &target,
            )
            .await;
        let before_refresh = state.list_active_models(None, &[]);
        assert_eq!(
            before_refresh.len(),
            1,
            "snapshot should remain stale until the next refresh tick: {before_refresh:?}"
        );

        state.refresh_active_models_snapshot().await;
        let after_refresh = state.list_active_models(None, &[]);
        assert!(
            after_refresh.is_empty(),
            "removed model should disappear after snapshot refresh: {after_refresh:?}"
        );
    }

    #[tokio::test]
    async fn different_routing_keys_isolate_candidates() {
        let state = StargateState::default();
        let mut running_a =
            running_registration(&state, "inst-a", "quic://127.0.0.1:1111", Some("rk-a")).await;
        let mut running_b =
            running_registration(&state, "inst-b", "quic://127.0.0.1:2222", Some("rk-b")).await;

        let update_a = InferenceServerRegistration {
            inference_server_id: "inst-a".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        let update_b = InferenceServerRegistration {
            inference_server_id: "inst-b".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:2222".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;
        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let candidates_a = state
            .candidates_for_target(&make_target(Some("rk-a"), "shared-model"))
            .await;
        let candidates_b = state
            .candidates_for_target(&make_target(Some("rk-b"), "shared-model"))
            .await;
        assert_eq!(candidates_a.len(), 1);
        assert_eq!(candidates_b.len(), 1);
        assert_eq!(candidates_a[0].inference_server_id, "inst-a");
        assert_eq!(candidates_b[0].inference_server_id, "inst-b");
    }

    #[tokio::test]
    async fn apply_registration_update_survives_poisoned_last_rtt_lock() {
        let state = StargateState::default();
        let mut running =
            running_registration(&state, "inst-poison", "quic://127.0.0.1:3333", Some("rk-p"))
                .await;
        let update = InferenceServerRegistration {
            inference_server_id: "inst-poison".to_string(),
            cluster_id: String::new(),
            inference_server_url: "quic://127.0.0.1:3333".to_string(),
            models: HashMap::from([(
                "model-p".to_string(),
                model_registration(InferenceServerStatus::Active as i32),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        let poison = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = running.registration_state.last_rtt.lock();
            panic!("poison last_rtt lock");
        }));
        assert!(poison.is_err());

        state
            .apply_registration_update(&mut running, &update, true, Some(Duration::from_millis(9)))
            .await;

        let candidates = state
            .candidates_for_target(&make_target(Some("rk-p"), "model-p"))
            .await;
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].rtt, Duration::from_millis(9));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn target_state_or_insert_does_not_panic_during_concurrent_remove() {
        let state = Arc::new(StargateState::default());
        let target = make_target(Some("rk-race"), "model-race");

        let mut writers = Vec::new();
        for _ in 0..8 {
            let state = Arc::clone(&state);
            let target = target.clone();
            writers.push(tokio::spawn(async move {
                for _ in 0..20_000 {
                    let _ = state.target_state_or_insert(&target).await;
                    tokio::task::yield_now().await;
                }
            }));
        }

        let remover = {
            let state = Arc::clone(&state);
            let target = target.clone();
            tokio::spawn(async move {
                for _ in 0..20_000 {
                    let _ = state.routing_map.remove_async(&target).await;
                    tokio::task::yield_now().await;
                }
            })
        };

        for writer in writers {
            writer.await.unwrap();
        }
        remover.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cluster_state_or_insert_does_not_panic_during_concurrent_remove() {
        let target_state = Arc::new(RoutingTargetState::default());
        let cluster_id = "cluster-race";

        let mut writers = Vec::new();
        for _ in 0..8 {
            let target_state = Arc::clone(&target_state);
            writers.push(tokio::spawn(async move {
                for _ in 0..20_000 {
                    let _ = StargateState::cluster_state_or_insert(&target_state, cluster_id).await;
                    tokio::task::yield_now().await;
                }
            }));
        }

        let remover = {
            let target_state = Arc::clone(&target_state);
            tokio::spawn(async move {
                for _ in 0..20_000 {
                    let _ = target_state.clusters.remove_async(cluster_id).await;
                    tokio::task::yield_now().await;
                }
            })
        };

        for writer in writers {
            writer.await.unwrap();
        }
        remover.await.unwrap();
    }

    #[tokio::test]
    async fn shared_cluster_registration_exposes_one_aggregated_cluster_candidate() {
        let state = StargateState::default();
        let mut running_a = running_registration_in_cluster(
            &state,
            "inst-a",
            "cluster-shared",
            "quic://127.0.0.1:1111",
            Some("rk-a"),
        )
        .await;
        let mut running_b = running_registration_in_cluster(
            &state,
            "inst-b",
            "cluster-shared",
            "quic://127.0.0.1:2222",
            Some("rk-a"),
        )
        .await;

        let update_a = InferenceServerRegistration {
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-shared".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        output_tps: 2.0,
                        last_mean_input_tps: 100.0,
                        max_output_tps: 50.0,
                        queue_size: 1,
                        queued_input_size: 100,
                        input_processing_queries: 1,
                        output_generation_queries: 2,
                        stats_observed_at_unix_ms: 1000,
                        stats_capabilities: vec!["request.final_headers".to_string()],
                        stats_sources: vec!["request_metadata".to_string()],
                        kv_cache_capacity_tokens: 1000,
                        kv_cache_used_tokens: 100,
                        kv_cache_free_tokens: 900,
                        num_running_queries: 11,
                        max_engine_concurrency: 111,
                        total_query_input_size: 1111,
                        queue_time_estimate_ms_by_priority: HashMap::from([(1, 111)]),
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        let update_b = InferenceServerRegistration {
            inference_server_id: "inst-b".to_string(),
            cluster_id: "cluster-shared".to_string(),
            inference_server_url: "quic://127.0.0.1:2222".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        output_tps: 5.0,
                        last_mean_input_tps: 120.0,
                        max_output_tps: 60.0,
                        queue_size: 2,
                        queued_input_size: 200,
                        input_processing_queries: 3,
                        output_generation_queries: 4,
                        stats_observed_at_unix_ms: 2000,
                        stats_capabilities: vec![
                            "request.final_headers".to_string(),
                            "machine.kv_cache.http".to_string(),
                        ],
                        stats_sources: vec![
                            "request_metadata".to_string(),
                            "kv_cache_stats".to_string(),
                        ],
                        kv_cache_capacity_tokens: 2000,
                        kv_cache_used_tokens: 500,
                        kv_cache_free_tokens: 1500,
                        num_running_queries: 7,
                        max_engine_concurrency: 77,
                        total_query_input_size: 777,
                        queue_time_estimate_ms_by_priority: HashMap::from([(1, 222), (2, 333)]),
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(10)),
            )
            .await;
        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        let target = make_target(Some("rk-a"), "shared-model");
        let legacy_backend_candidates = state.candidates_for_target(&target).await;
        assert_eq!(legacy_backend_candidates.len(), 2);

        let clusters = state.cluster_candidates_for_target(&target).await;
        assert_eq!(clusters.len(), 1);
        let cluster = &clusters[0];
        assert_eq!(cluster.cluster_id, "cluster-shared");
        assert_eq!(cluster.active_backend_count, 2);
        assert_eq!(cluster.stats.last_mean_input_tps, 220.0);
        assert_eq!(cluster.stats.output_tps, 7.0);
        assert_eq!(cluster.stats.queue_size, 3);
        assert_eq!(cluster.stats.queued_input_size, 300);
        assert_eq!(cluster.stats.input_processing_queries, 4);
        assert_eq!(cluster.stats.output_generation_queries, 6);
        assert_eq!(cluster.stats.stats_observed_at_unix_ms, 2000);
        assert_eq!(
            cluster.stats.stats_capabilities,
            vec![
                "request.final_headers".to_string(),
                "machine.kv_cache.http".to_string(),
            ]
        );
        assert_eq!(
            cluster.stats.stats_sources,
            vec!["request_metadata".to_string(), "kv_cache_stats".to_string()]
        );
        assert_eq!(cluster.stats.max_output_tps, 60.0);
        assert_eq!(cluster.stats.kv_cache_capacity_tokens, 2000);
        assert_eq!(cluster.stats.kv_cache_used_tokens, 500);
        assert_eq!(cluster.stats.kv_cache_free_tokens, 1500);
        assert_eq!(cluster.stats.num_running_queries, 7);
        assert_eq!(cluster.stats.max_engine_concurrency, 77);
        assert_eq!(cluster.stats.total_query_input_size, 777);
        assert_eq!(
            cluster.stats.queue_time_estimate_ms_by_priority,
            HashMap::from([(1, 222), (2, 333)])
        );
        assert_eq!(cluster.rtt, Duration::from_millis(5));
    }

    #[tokio::test]
    async fn shared_cluster_recomputes_cluster_stats_when_source_backend_is_removed() {
        let state = StargateState::default();
        let mut running_a = running_registration_in_cluster(
            &state,
            "inst-a",
            "cluster-shared",
            "quic://127.0.0.1:1111",
            Some("rk-a"),
        )
        .await;
        let mut running_b = running_registration_in_cluster(
            &state,
            "inst-b",
            "cluster-shared",
            "quic://127.0.0.1:2222",
            Some("rk-a"),
        )
        .await;

        let update_a = InferenceServerRegistration {
            inference_server_id: "inst-a".to_string(),
            cluster_id: "cluster-shared".to_string(),
            inference_server_url: "quic://127.0.0.1:1111".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        last_mean_input_tps: 100.0,
                        max_output_tps: 50.0,
                        kv_cache_capacity_tokens: 1000,
                        kv_cache_used_tokens: 100,
                        kv_cache_free_tokens: 900,
                        num_running_queries: 11,
                        max_engine_concurrency: 111,
                        total_query_input_size: 1111,
                        queue_time_estimate_ms_by_priority: HashMap::from([(1, 111)]),
                        ..ModelStats::default()
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };
        let update_b = InferenceServerRegistration {
            inference_server_id: "inst-b".to_string(),
            cluster_id: "cluster-shared".to_string(),
            inference_server_url: "quic://127.0.0.1:2222".to_string(),
            models: HashMap::from([(
                "shared-model".to_string(),
                model_registration_with_stats(
                    InferenceServerStatus::Active as i32,
                    ModelStats {
                        last_mean_input_tps: 120.0,
                        max_output_tps: 60.0,
                        kv_cache_capacity_tokens: 2000,
                        kv_cache_used_tokens: 500,
                        kv_cache_free_tokens: 1500,
                        num_running_queries: 7,
                        max_engine_concurrency: 77,
                        total_query_input_size: 777,
                        queue_time_estimate_ms_by_priority: HashMap::from([(1, 222), (2, 333)]),
                        ..ModelStats::default()
                    },
                ),
            )]),
            reverse_tunnel: false,
            coordinated_calibration: false,
        };

        state
            .apply_registration_update(
                &mut running_a,
                &update_a,
                true,
                Some(Duration::from_millis(10)),
            )
            .await;
        state
            .apply_registration_update(
                &mut running_b,
                &update_b,
                true,
                Some(Duration::from_millis(5)),
            )
            .await;

        state.end_registration("inst-b").await;

        let clusters = state
            .cluster_candidates_for_target(&make_target(Some("rk-a"), "shared-model"))
            .await;
        assert_eq!(clusters.len(), 1);
        let cluster = &clusters[0];
        assert_eq!(cluster.active_backend_count, 1);
        assert_eq!(cluster.stats.last_mean_input_tps, 100.0);
        assert_eq!(cluster.stats.max_output_tps, 50.0);
        assert_eq!(cluster.stats.kv_cache_capacity_tokens, 1000);
        assert_eq!(cluster.stats.kv_cache_used_tokens, 100);
        assert_eq!(cluster.stats.kv_cache_free_tokens, 900);
        assert_eq!(cluster.stats.num_running_queries, 11);
        assert_eq!(cluster.stats.max_engine_concurrency, 111);
        assert_eq!(cluster.stats.total_query_input_size, 1111);
        assert_eq!(
            cluster.stats.queue_time_estimate_ms_by_priority,
            HashMap::from([(1, 111)])
        );
    }

    #[tokio::test]
    async fn shared_cluster_selects_active_backends_round_robin() {
        let state = StargateState::default();
        let mut running_a = running_registration_in_cluster(
            &state,
            "inst-a",
            "cluster-shared",
            "quic://127.0.0.1:1111",
            Some("rk-a"),
        )
        .await;
        let mut running_b = running_registration_in_cluster(
            &state,
            "inst-b",
            "cluster-shared",
            "quic://127.0.0.1:2222",
            Some("rk-a"),
        )
        .await;
        for (running, inst, url) in [
            (&mut running_a, "inst-a", "quic://127.0.0.1:1111"),
            (&mut running_b, "inst-b", "quic://127.0.0.1:2222"),
        ] {
            let update = InferenceServerRegistration {
                inference_server_id: inst.to_string(),
                cluster_id: "cluster-shared".to_string(),
                inference_server_url: url.to_string(),
                models: HashMap::from([(
                    "shared-model".to_string(),
                    model_registration(InferenceServerStatus::Active as i32),
                )]),
                reverse_tunnel: false,
                coordinated_calibration: false,
            };
            state
                .apply_registration_update(running, &update, true, Some(Duration::from_millis(5)))
                .await;
        }

        let target = make_target(Some("rk-a"), "shared-model");
        let first = state
            .select_backend_for_cluster(&target, "cluster-shared", &HashSet::new())
            .await
            .expect("first backend should be selected");
        let second = state
            .select_backend_for_cluster(&target, "cluster-shared", &HashSet::new())
            .await
            .expect("second backend should be selected");
        let third = state
            .select_backend_for_cluster(&target, "cluster-shared", &HashSet::new())
            .await
            .expect("third backend should be selected");

        assert_eq!(first.inference_server_id, "inst-a");
        assert_eq!(second.inference_server_id, "inst-b");
        assert_eq!(third.inference_server_id, "inst-a");

        let selected = state
            .select_backend_for_cluster(
                &target,
                "cluster-shared",
                &HashSet::from(["inst-a".to_string()]),
            )
            .await
            .expect("remaining backend should be selected");
        assert_eq!(selected.inference_server_id, "inst-b");
    }
}
