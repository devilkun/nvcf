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

use crate::config::{BenchmarkConfig, PylonQueueAdmissionConfig, ScenarioMetadata};
use crate::manifest::Manifest;
use crate::score::RunSummary;

#[derive(Debug, Clone)]
pub struct ReportContext {
    pub name: String,
    pub metadata: ScenarioMetadata,
    pub model: String,
    pub request_count: usize,
    pub max_concurrency: usize,
    pub stargate_count: usize,
    pub backend_count: usize,
}

impl ReportContext {
    pub fn from_config(config: &BenchmarkConfig) -> Self {
        Self {
            name: config.name.clone(),
            metadata: config.metadata.clone(),
            model: config.model.clone(),
            request_count: config.request_count,
            max_concurrency: config.max_concurrency,
            stargate_count: config.stargates.count,
            backend_count: config.backends.count,
        }
    }

    pub fn from_manifest(manifest: &Manifest) -> Self {
        Self {
            name: manifest.benchmark_name.clone(),
            metadata: manifest.metadata.clone(),
            model: manifest.model.clone(),
            request_count: manifest.request_count,
            max_concurrency: manifest.max_concurrency,
            stargate_count: manifest.stargate_count,
            backend_count: manifest.backend_count,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReportEntry {
    pub algorithm_name: String,
    pub pylon_queue_admission: Option<PylonQueueAdmissionConfig>,
    pub summary: RunSummary,
}

pub fn render_markdown_report(context: &ReportContext, entries: &[ReportEntry]) -> String {
    let mut out = String::new();
    out.push_str(&format!("# Benchmark Report: {}\n\n", context.name));
    if let Some(description) = &context.metadata.description {
        out.push_str(description);
        out.push_str("\n\n");
    }
    out.push_str(&format!("- Model: `{}`\n", context.model));
    out.push_str(&format!("- Requests: `{}`\n", context.request_count));
    out.push_str(&format!(
        "- Max concurrency: `{}`\n",
        context.max_concurrency
    ));
    out.push_str(&format!("- Stargates: `{}`\n", context.stargate_count));
    out.push_str(&format!("- Backends: `{}`\n\n", context.backend_count));
    if !context.metadata.tags.is_empty() {
        out.push_str(&format!(
            "- Tags: `{}`\n",
            context.metadata.tags.join("`, `")
        ));
    }
    if let Some(expected_runtime) = &context.metadata.expected_runtime {
        out.push_str(&format!("- Expected runtime: `{expected_runtime}`\n"));
    }
    if let Some(expected_signal) = &context.metadata.expected_signal {
        out.push_str(&format!("- Expected signal: {expected_signal}\n"));
    }
    if !context.metadata.tags.is_empty()
        || context.metadata.expected_runtime.is_some()
        || context.metadata.expected_signal.is_some()
    {
        out.push('\n');
    }

    let warnings = report_warnings(context, entries);
    if !warnings.is_empty() {
        out.push_str("## Warnings\n\n");
        for warning in warnings {
            out.push_str(&format!("- {warning}\n"));
        }
        out.push('\n');
    }

    out.push_str("| Algorithm | Admission Mode | Success | Avg TTFT | Avg TTLT | P95 TTLT | Max TTLT | Total Length | Equal Balance | Capacity Balance | Cache Hits | Cache Hit Rate | Cache Movement | Cache Evictions | Evicted Tokens | Failure Groups | Pylon Rejected | Pylon Disabled | Queue Mismatch Retries | Retry Exhausted |\n");
    out.push_str("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for entry in entries {
        let summary = &entry.summary;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            entry.algorithm_name,
            admission_mode(entry.pylon_queue_admission.as_ref()),
            percent(summary.success_rate),
            optional_ms(summary.avg_ttft_ms),
            ms_float(summary.avg_ttlt_ms),
            ms(summary.p95_ttlt_ms),
            ms(summary.max_ttlt_ms),
            ms(summary.total_length_ms),
            optional_score(summary.balance_score),
            optional_score(summary.capacity_balance_score),
            cache_hits(
                summary.cache_summary.hit_count,
                summary.cache_summary.miss_count
            ),
            optional_percent(summary.cache_summary.hit_rate),
            optional_percent(summary.stickiness_summary.movement_rate),
            summary.cache_summary.eviction_count,
            summary.cache_summary.evicted_tokens,
            summary.failure_summary.len(),
            counter(summary.queue_admission_summary.pylon_rejected_count),
            counter(summary.queue_admission_summary.pylon_disabled_count),
            counter(
                summary
                    .queue_admission_summary
                    .stargate_queue_mismatch_retry_count
            ),
            retry_exhaustion(&summary.queue_admission_summary),
        ));
    }

    out.push_str("\n## Backend Shares\n\n");
    for entry in entries {
        out.push_str(&format!("### {}\n\n", entry.algorithm_name));
        out.push_str("| Backend | Requests | Success | Request Share | Input Share | Output Share | Capacity Share | Avg TTLT | P95 TTLT | Cache Hit Rate | Evictions |\n");
        out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        let backend_ids = entry
            .summary
            .backend_request_shares
            .keys()
            .chain(entry.summary.backend_capacity_shares.keys())
            .chain(entry.summary.backend_summaries.keys())
            .collect::<std::collections::BTreeSet<_>>();
        for backend_id in backend_ids {
            let request_share = entry
                .summary
                .backend_request_shares
                .get(backend_id)
                .copied();
            let capacity_share = entry
                .summary
                .backend_capacity_shares
                .get(backend_id)
                .copied();
            let input_share = entry
                .summary
                .backend_input_token_shares
                .get(backend_id)
                .copied();
            let output_share = entry
                .summary
                .backend_output_token_shares
                .get(backend_id)
                .copied();
            let backend_summary = entry.summary.backend_summaries.get(backend_id);
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                backend_id,
                backend_summary
                    .map(|summary| summary.request_count.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                backend_summary
                    .map(|summary| summary.success_count.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                optional_percent(request_share),
                optional_percent(input_share),
                optional_percent(output_share),
                optional_percent(capacity_share),
                backend_summary
                    .and_then(|summary| summary.avg_ttlt_ms)
                    .map(ms_float)
                    .unwrap_or_else(|| "-".to_string()),
                backend_summary
                    .and_then(|summary| summary.p95_ttlt_ms)
                    .map(ms)
                    .unwrap_or_else(|| "-".to_string()),
                optional_percent(backend_summary.and_then(|summary| summary.cache_hit_rate)),
                backend_summary
                    .map(|summary| summary.cache_eviction_count.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            ));
        }
        out.push('\n');
    }

    let has_failures = entries
        .iter()
        .any(|entry| !entry.summary.failure_summary.is_empty());
    if has_failures {
        out.push_str("## Failures\n\n");
        out.push_str("| Algorithm | Status | Backend | Count | Error |\n");
        out.push_str("|---|---:|---|---:|---|\n");
        for entry in entries {
            for failure in &entry.summary.failure_summary {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} |\n",
                    entry.algorithm_name,
                    failure.status_code,
                    failure.selected_backend_id.as_deref().unwrap_or("-"),
                    failure.count,
                    failure.error.as_deref().unwrap_or("-"),
                ));
            }
        }
        out.push('\n');
    }

    out
}

fn ms(value: u64) -> String {
    format!("{value} ms")
}

fn ms_float(value: f64) -> String {
    format!("{value:.1} ms")
}

fn optional_ms(value: Option<f64>) -> String {
    value.map(ms_float).unwrap_or_else(|| "-".to_string())
}

fn percent(value: f64) -> String {
    format!("{:.1}%", value * 100.0)
}

fn optional_percent(value: Option<f64>) -> String {
    value.map(percent).unwrap_or_else(|| "-".to_string())
}

fn optional_score(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "-".to_string())
}

fn cache_hits(hit_count: usize, miss_count: usize) -> String {
    if hit_count == 0 && miss_count == 0 {
        "-".to_string()
    } else {
        format!("{hit_count}/{miss_count}")
    }
}

fn admission_mode(config: Option<&PylonQueueAdmissionConfig>) -> String {
    let Some(config) = config else {
        return "runtime default".to_string();
    };
    let mode = if config.enabled {
        "enabled"
    } else {
        "disabled"
    };
    let mut details = Vec::new();
    if let Some(min_delta_ms) = config.min_delta_ms {
        details.push(format!("min={min_delta_ms}ms"));
    }
    if let Some(tolerance_factor) = config.tolerance_factor {
        details.push(format!("factor={}", counter(tolerance_factor)));
    }
    if let Some(retry_after_ms) = config.retry_after_ms {
        details.push(format!("retry-after={retry_after_ms}ms"));
    }
    if details.is_empty() {
        mode.to_string()
    } else {
        format!("{mode} ({})", details.join(", "))
    }
}

fn counter(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.3}")
    }
}

fn retry_exhaustion(summary: &crate::score::QueueAdmissionSummary) -> String {
    let total = counter(summary.stargate_retry_exhausted_count);
    if summary.stargate_retry_exhausted_by_reason.is_empty() {
        return total;
    }
    let reasons = summary
        .stargate_retry_exhausted_by_reason
        .iter()
        .map(|(reason, count)| format!("{reason}={}", counter(*count)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{total} ({reasons})")
}

fn report_warnings(context: &ReportContext, entries: &[ReportEntry]) -> Vec<String> {
    let mut warnings = Vec::new();
    if entries.is_empty() {
        warnings.push("No algorithm summaries were found.".to_string());
        return warnings;
    }
    let cache_focused = context
        .metadata
        .tags
        .iter()
        .any(|tag| matches!(tag.as_str(), "cache" | "pulsar" | "kv-cache"));
    let queue_admission_focused = context
        .metadata
        .tags
        .iter()
        .any(|tag| matches!(tag.as_str(), "queue-admission" | "queue-mismatch"));
    for entry in entries {
        if entry.summary.success_rate < 1.0 {
            warnings.push(format!(
                "{} success rate was {:.1}%.",
                entry.algorithm_name,
                entry.summary.success_rate * 100.0
            ));
        }
        if let Some(score) = entry.summary.capacity_balance_score
            && score < 0.5
        {
            warnings.push(format!(
                "{} capacity balance score was low ({score:.3}).",
                entry.algorithm_name
            ));
        }
        if cache_focused && entry.summary.cache_summary.observed_request_count == 0 {
            warnings.push(format!(
                "{} did not report per-request KV-cache headers.",
                entry.algorithm_name
            ));
        }
        if cache_focused
            && entry.summary.cache_summary.observed_request_count > 0
            && entry.summary.cache_summary.hit_count == 0
        {
            warnings.push(format!(
                "{} reported KV-cache headers but no cache hits.",
                entry.algorithm_name
            ));
        }
    }
    if queue_admission_focused
        && entries.iter().all(|entry| {
            entry.summary.queue_admission_summary.pylon_rejected_count == 0.0
                && entry
                    .summary
                    .queue_admission_summary
                    .stargate_queue_mismatch_retry_count
                    == 0.0
        })
    {
        warnings.push(
            "No pylon queue-mismatch rejections or Stargate queue-mismatch retries were observed."
                .to_string(),
        );
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ArrivalPatternConfig, BackendConfig, BackendProfile, RegistrationConfig, ServiceTimeConfig,
        StargateConfig, TokenDistributionConfig, TrafficPatternConfig, UniformTrafficConfig,
    };
    use crate::score::{CacheSummary, QueueAdmissionSummary, summarize_with_capacity};
    use std::collections::BTreeMap;

    fn config() -> BenchmarkConfig {
        BenchmarkConfig {
            name: "report-test".to_string(),
            metadata: ScenarioMetadata::default(),
            model: "dummy-model".to_string(),
            seed: Some(1),
            request_count: 1,
            max_concurrency: 1,
            tunnel_protocol: crate::config::TunnelProtocol::Custom,
            stargates: StargateConfig { count: 1 },
            backends: BackendConfig {
                count: 1,
                cluster_id_template: None,
                profiles: Vec::new(),
                profile: BackendProfile {
                    name: "default".to_string(),
                    weight: 1.0,
                    max_concurrent_requests: None,
                    kv_cache_capacity_tokens: 0,
                    service_time_ms: ServiceTimeConfig {
                        ttft_mean: 1,
                        ttft_jitter_ms: 0,
                        decode_tokens_per_s: 1,
                        decode_jitter_ms: 0,
                        prefill_tokens_per_s: None,
                    },
                    registration: RegistrationConfig {
                        last_mean_input_tps: 1.0,
                    },
                },
            },
            traffic_pattern: TrafficPatternConfig::Uniform(UniformTrafficConfig {
                routing_keys: 0,
                cache_affinity_keys: 0,
                input_tokens: TokenDistributionConfig::Constant { value: 1 },
                output_tokens: TokenDistributionConfig::Constant { value: 1 },
                arrival: ArrivalPatternConfig::Constant { interval_ms: 1 },
            }),
            degradation: Default::default(),
            algorithms: Vec::new(),
        }
    }

    #[test]
    fn markdown_report_includes_key_columns() {
        let mut request_shares = BTreeMap::new();
        request_shares.insert("backend-0".to_string(), 1.0);
        let summary = RunSummary {
            request_count: 1,
            success_rate: 1.0,
            avg_ttft_ms: Some(10.0),
            p50_ttft_ms: Some(10),
            p95_ttft_ms: Some(10),
            p99_ttft_ms: Some(10),
            avg_ttlt_ms: 20.0,
            p50_ttlt_ms: 20,
            p95_ttlt_ms: 20,
            p99_ttlt_ms: 20,
            max_ttlt_ms: 20,
            total_length_ms: 25,
            balance_score: Some(1.0),
            capacity_balance_score: Some(1.0),
            backend_request_shares: request_shares.clone(),
            backend_capacity_shares: request_shares,
            backend_input_token_shares: BTreeMap::new(),
            backend_output_token_shares: BTreeMap::new(),
            backend_summaries: BTreeMap::new(),
            cache_summary: CacheSummary {
                observed_request_count: 1,
                hit_count: 1,
                miss_count: 0,
                hit_rate: Some(1.0),
                eviction_count: 0,
                evicted_tokens: 0,
            },
            stickiness_summary: Default::default(),
            failure_summary: Vec::new(),
            queue_admission_summary: Default::default(),
        };

        let report = render_markdown_report(
            &ReportContext::from_config(&config()),
            &[ReportEntry {
                algorithm_name: "power-of-two".to_string(),
                pylon_queue_admission: None,
                summary,
            }],
        );

        assert!(report.contains("| Algorithm | Admission Mode | Success |"));
        assert!(report.contains("Capacity Balance"));
        assert!(report.contains("Cache Hit Rate"));
        assert!(report.contains("backend-0"));
    }

    #[test]
    fn markdown_report_warns_for_cache_scenarios_without_cache_headers() {
        let mut config = config();
        config.metadata.tags = vec!["cache".to_string()];
        let summary = RunSummary {
            request_count: 1,
            success_rate: 1.0,
            avg_ttft_ms: Some(10.0),
            p50_ttft_ms: Some(10),
            p95_ttft_ms: Some(10),
            p99_ttft_ms: Some(10),
            avg_ttlt_ms: 20.0,
            p50_ttlt_ms: 20,
            p95_ttlt_ms: 20,
            p99_ttlt_ms: 20,
            max_ttlt_ms: 20,
            total_length_ms: 25,
            balance_score: Some(1.0),
            capacity_balance_score: Some(1.0),
            backend_request_shares: BTreeMap::new(),
            backend_capacity_shares: BTreeMap::new(),
            backend_input_token_shares: BTreeMap::new(),
            backend_output_token_shares: BTreeMap::new(),
            backend_summaries: BTreeMap::new(),
            cache_summary: CacheSummary::default(),
            stickiness_summary: Default::default(),
            failure_summary: Vec::new(),
            queue_admission_summary: Default::default(),
        };

        let report = render_markdown_report(
            &ReportContext::from_config(&config),
            &[ReportEntry {
                algorithm_name: "pulsar".to_string(),
                pylon_queue_admission: None,
                summary,
            }],
        );

        assert!(report.contains("## Warnings"));
        assert!(report.contains("did not report per-request KV-cache headers"));
    }

    #[test]
    fn markdown_report_labels_admission_variant_and_proof_counters() {
        let mut summary = summarize_with_capacity(&[], BTreeMap::new());
        summary.queue_admission_summary = QueueAdmissionSummary {
            pylon_rejected_count: 3.0,
            pylon_disabled_count: 0.0,
            stargate_queue_mismatch_retry_count: 2.0,
            stargate_retry_exhausted_count: 1.0,
            stargate_retry_exhausted_by_reason: BTreeMap::from([(
                "retry_budget_exhausted".to_string(),
                1.0,
            )]),
            ..QueueAdmissionSummary::default()
        };

        let report = render_markdown_report(
            &ReportContext::from_config(&config()),
            &[ReportEntry {
                algorithm_name: "groq-admission-enabled".to_string(),
                pylon_queue_admission: Some(crate::config::PylonQueueAdmissionConfig {
                    enabled: true,
                    min_delta_ms: Some(0),
                    tolerance_factor: Some(1.0),
                    retry_after_ms: Some(5),
                }),
                summary,
            }],
        );

        assert!(report.contains("Admission Mode"));
        assert!(report.contains("enabled (min=0ms, factor=1, retry-after=5ms)"));
        assert!(report.contains("Pylon Rejected"));
        assert!(report.contains("| groq-admission-enabled | enabled"));
        assert!(report.contains("| 3 | 0 | 2 | 1 (retry_budget_exhausted=1) |"));
    }
}
