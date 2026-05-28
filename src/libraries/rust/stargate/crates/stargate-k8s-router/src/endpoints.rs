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

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice};

pub const ENDPOINT_SLICE_SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodTarget {
    pub pod_name: String,
    pub grpc_addr: String,
    pub quic_addr: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TargetSnapshot {
    pub initialized: bool,
    pub targets: BTreeMap<String, PodTarget>,
    pub ready_targets: Vec<PodTarget>,
}

impl TargetSnapshot {
    pub fn initialized(targets: BTreeMap<String, PodTarget>) -> Self {
        let ready_targets = targets.values().cloned().collect();
        Self {
            initialized: true,
            targets,
            ready_targets,
        }
    }

    pub fn target_for_pod(&self, pod_name: &str) -> Option<PodTarget> {
        self.target_for_pod_ref(pod_name).cloned()
    }

    pub fn target_for_pod_ref(&self, pod_name: &str) -> Option<&PodTarget> {
        self.targets.get(pod_name)
    }

    pub fn first_ready(&self, offset: usize) -> Option<PodTarget> {
        self.first_ready_ref(offset).cloned()
    }

    pub fn first_ready_ref(&self, offset: usize) -> Option<&PodTarget> {
        if self.ready_targets.is_empty() {
            return None;
        }
        let index = offset % self.ready_targets.len();
        self.ready_targets.get(index)
    }

    pub fn ready_count(&self) -> usize {
        self.ready_targets.len()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetBuildConfig {
    pub service_name: String,
    pub grpc_port_name: String,
    pub quic_port_name: String,
}

pub fn snapshot_from_slices<'a>(
    slices: impl IntoIterator<Item = &'a EndpointSlice>,
    config: &TargetBuildConfig,
) -> TargetSnapshot {
    let mut targets = BTreeMap::new();
    for slice in slices {
        if !slice_belongs_to_service(slice, &config.service_name) {
            continue;
        }

        let Some(ports) = slice.ports.as_deref() else {
            continue;
        };
        let Some(grpc_port) = named_port(ports, &config.grpc_port_name, "TCP") else {
            continue;
        };
        let Some(quic_port) = named_port(ports, &config.quic_port_name, "UDP") else {
            continue;
        };

        for endpoint in &slice.endpoints {
            let Some(target) = target_from_endpoint(endpoint, grpc_port, quic_port) else {
                continue;
            };
            targets.insert(target.pod_name.clone(), target);
        }
    }
    TargetSnapshot::initialized(targets)
}

pub fn snapshot_from_slice_store(
    slices: &BTreeMap<String, EndpointSlice>,
    config: &TargetBuildConfig,
) -> TargetSnapshot {
    snapshot_from_slices(slices.values(), config)
}

fn slice_belongs_to_service(slice: &EndpointSlice, service_name: &str) -> bool {
    slice
        .metadata
        .labels
        .as_ref()
        .and_then(|labels| labels.get(ENDPOINT_SLICE_SERVICE_NAME_LABEL))
        .is_some_and(|value| value == service_name)
}

fn named_port(ports: &[EndpointPort], name: &str, protocol: &str) -> Option<u16> {
    ports.iter().find_map(|port| {
        if port.name.as_deref() != Some(name) {
            return None;
        }
        if !port
            .protocol
            .as_deref()
            .unwrap_or("TCP")
            .eq_ignore_ascii_case(protocol)
        {
            return None;
        }
        let value = port.port?;
        u16::try_from(value).ok().filter(|p| *p > 0)
    })
}

fn target_from_endpoint(endpoint: &Endpoint, grpc_port: u16, quic_port: u16) -> Option<PodTarget> {
    if !endpoint_is_ready(endpoint.conditions.as_ref()) {
        return None;
    }
    let pod_name = endpoint_pod_name(endpoint)?;
    let address = endpoint.addresses.first()?;
    Some(PodTarget {
        pod_name,
        grpc_addr: socket_addr(address, grpc_port),
        quic_addr: socket_addr(address, quic_port),
    })
}

fn endpoint_is_ready(conditions: Option<&EndpointConditions>) -> bool {
    let ready = conditions.and_then(|c| c.ready).unwrap_or(true);
    let serving = conditions.and_then(|c| c.serving).unwrap_or(true);
    let terminating = conditions.and_then(|c| c.terminating).unwrap_or(false);
    ready && serving && !terminating
}

fn endpoint_pod_name(endpoint: &Endpoint) -> Option<String> {
    if let Some(name) = endpoint.target_ref.as_ref().and_then(pod_target_name) {
        return Some(name);
    }
    endpoint.hostname.clone()
}

fn pod_target_name(target_ref: &ObjectReference) -> Option<String> {
    if target_ref.kind.as_deref().is_some_and(|kind| kind != "Pod") {
        return None;
    }
    target_ref.name.clone()
}

fn socket_addr(address: &str, port: u16) -> String {
    address
        .parse::<IpAddr>()
        .map(|ip| SocketAddr::new(ip, port).to_string())
        .unwrap_or_else(|_| format!("{address}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::perf_tests::assert_twenty_percent_faster;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::hint::black_box;
    use std::time::Instant;

    fn config() -> TargetBuildConfig {
        TargetBuildConfig {
            service_name: "stargate-headless".to_string(),
            grpc_port_name: "grpc".to_string(),
            quic_port_name: "quic".to_string(),
        }
    }

    fn slice(name: &str, endpoints: Vec<Endpoint>) -> EndpointSlice {
        EndpointSlice {
            address_type: "IPv4".to_string(),
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                labels: Some(BTreeMap::from([(
                    ENDPOINT_SLICE_SERVICE_NAME_LABEL.to_string(),
                    "stargate-headless".to_string(),
                )])),
                ..ObjectMeta::default()
            },
            ports: Some(vec![
                EndpointPort {
                    name: Some("grpc".to_string()),
                    port: Some(50071),
                    protocol: Some("TCP".to_string()),
                    ..EndpointPort::default()
                },
                EndpointPort {
                    name: Some("quic".to_string()),
                    port: Some(50072),
                    protocol: Some("UDP".to_string()),
                    ..EndpointPort::default()
                },
            ]),
            endpoints,
        }
    }

    fn endpoint(name: &str, ip: &str, conditions: Option<EndpointConditions>) -> Endpoint {
        Endpoint {
            addresses: vec![ip.to_string()],
            conditions,
            target_ref: Some(ObjectReference {
                kind: Some("Pod".to_string()),
                name: Some(name.to_string()),
                ..ObjectReference::default()
            }),
            ..Endpoint::default()
        }
    }

    fn many_targets(count: usize) -> TargetSnapshot {
        let targets = (0..count)
            .map(|index| {
                let pod_name = format!("stargate-{index}");
                (
                    pod_name.clone(),
                    PodTarget {
                        pod_name,
                        grpc_addr: format!("10.0.0.{index}:50071"),
                        quic_addr: format!("10.0.0.{index}:50072"),
                    },
                )
            })
            .collect();
        TargetSnapshot::initialized(targets)
    }

    #[test]
    fn snapshot_includes_ready_pod_targets() {
        let slice = slice(
            "slice-a",
            vec![endpoint(
                "stargate-0",
                "10.0.0.10",
                Some(EndpointConditions {
                    ready: Some(true),
                    serving: Some(true),
                    terminating: Some(false),
                }),
            )],
        );

        let snapshot = snapshot_from_slices([&slice], &config());

        assert!(snapshot.initialized);
        assert_eq!(
            snapshot.target_for_pod("stargate-0"),
            Some(PodTarget {
                pod_name: "stargate-0".to_string(),
                grpc_addr: "10.0.0.10:50071".to_string(),
                quic_addr: "10.0.0.10:50072".to_string(),
            })
        );
    }

    #[test]
    fn snapshot_treats_missing_conditions_with_kubernetes_defaults() {
        let slice = slice("slice-a", vec![endpoint("stargate-0", "10.0.0.10", None)]);

        let snapshot = snapshot_from_slices([&slice], &config());

        assert_eq!(snapshot.ready_count(), 1);
    }

    #[test]
    fn snapshot_excludes_unready_serving_false_and_terminating_endpoints() {
        let slice = slice(
            "slice-a",
            vec![
                endpoint(
                    "unready",
                    "10.0.0.10",
                    Some(EndpointConditions {
                        ready: Some(false),
                        ..EndpointConditions::default()
                    }),
                ),
                endpoint(
                    "not-serving",
                    "10.0.0.11",
                    Some(EndpointConditions {
                        serving: Some(false),
                        ..EndpointConditions::default()
                    }),
                ),
                endpoint(
                    "terminating",
                    "10.0.0.12",
                    Some(EndpointConditions {
                        terminating: Some(true),
                        ..EndpointConditions::default()
                    }),
                ),
            ],
        );

        let snapshot = snapshot_from_slices([&slice], &config());

        assert!(snapshot.targets.is_empty());
    }

    #[test]
    fn snapshot_ignores_slices_without_named_tcp_and_udp_ports() {
        let mut slice = slice(
            "slice-a",
            vec![endpoint(
                "stargate-0",
                "10.0.0.10",
                Some(EndpointConditions {
                    ready: Some(true),
                    ..EndpointConditions::default()
                }),
            )],
        );
        slice.ports = Some(vec![EndpointPort {
            name: Some("grpc".to_string()),
            port: Some(50071),
            protocol: Some("TCP".to_string()),
            ..EndpointPort::default()
        }]);

        let snapshot = snapshot_from_slices([&slice], &config());

        assert!(snapshot.targets.is_empty());
    }

    #[test]
    fn snapshot_formats_ipv6_endpoint_addresses() {
        let slice = slice("slice-a", vec![endpoint("stargate-0", "fd00::10", None)]);

        let snapshot = snapshot_from_slices([&slice], &config());

        assert_eq!(
            snapshot
                .target_for_pod("stargate-0")
                .map(|target| target.grpc_addr),
            Some("[fd00::10]:50071".to_string())
        );
    }

    #[test]
    #[ignore = "performance benchmark; run with --ignored --nocapture"]
    fn bench_target_snapshot_ready_lookup() {
        const BASELINE_NS_PER_OP: f64 = 136.95;

        let snapshot = many_targets(128);
        let iterations = 1_000_000usize;
        let started = Instant::now();
        let mut checksum = 0usize;

        for index in 0..iterations {
            let target = black_box(&snapshot)
                .first_ready_ref(black_box(index))
                .expect("snapshot has targets");
            checksum = checksum.wrapping_add(target.grpc_addr.len());
        }

        let elapsed = started.elapsed();
        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        eprintln!(
            "bench_target_snapshot_ready_lookup: iterations={iterations} elapsed={elapsed:?} ns_per_op={ns_per_op:.2} checksum={checksum}"
        );
        assert!(checksum > 0);
        assert_twenty_percent_faster(
            "bench_target_snapshot_ready_lookup",
            BASELINE_NS_PER_OP,
            ns_per_op,
        );
    }
}
