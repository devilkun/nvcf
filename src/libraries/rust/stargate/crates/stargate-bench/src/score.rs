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

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::config::BackendConfig;
use crate::driver::RequestResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSummary {
    pub request_count: usize,
    pub success_rate: f64,
    pub avg_ttft_ms: Option<f64>,
    pub p50_ttft_ms: Option<u64>,
    pub p95_ttft_ms: Option<u64>,
    pub p99_ttft_ms: Option<u64>,
    pub avg_ttlt_ms: f64,
    pub p50_ttlt_ms: u64,
    pub p95_ttlt_ms: u64,
    pub p99_ttlt_ms: u64,
    pub max_ttlt_ms: u64,
    #[serde(default)]
    pub total_length_ms: u64,
    pub balance_score: Option<f64>,
    #[serde(default)]
    pub capacity_balance_score: Option<f64>,
    pub backend_request_shares: BTreeMap<String, f64>,
    #[serde(default)]
    pub backend_capacity_shares: BTreeMap<String, f64>,
    #[serde(default)]
    pub backend_input_token_shares: BTreeMap<String, f64>,
    #[serde(default)]
    pub backend_output_token_shares: BTreeMap<String, f64>,
    #[serde(default)]
    pub backend_summaries: BTreeMap<String, BackendSummary>,
    #[serde(default)]
    pub cache_summary: CacheSummary,
    #[serde(default)]
    pub stickiness_summary: StickinessSummary,
    #[serde(default)]
    pub failure_summary: Vec<FailureSummary>,
    #[serde(default)]
    pub queue_admission_summary: QueueAdmissionSummary,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CacheSummary {
    pub observed_request_count: usize,
    pub hit_count: usize,
    pub miss_count: usize,
    pub hit_rate: Option<f64>,
    pub eviction_count: u64,
    pub evicted_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct QueueAdmissionSummary {
    pub pylon_accepted_count: f64,
    pub pylon_rejected_count: f64,
    pub pylon_disabled_count: f64,
    pub pylon_missing_estimate_count: f64,
    pub pylon_unknown_local_estimate_count: f64,
    pub stargate_queue_mismatch_retry_count: f64,
    pub stargate_retry_exhausted_count: f64,
    #[serde(default)]
    pub stargate_retry_exhausted_by_reason: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BackendSummary {
    pub request_count: usize,
    pub success_count: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub avg_ttlt_ms: Option<f64>,
    pub p95_ttlt_ms: Option<u64>,
    pub cache_hit_rate: Option<f64>,
    pub cache_eviction_count: u64,
    pub cache_evicted_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StickinessSummary {
    pub observed_cache_key_count: usize,
    pub sticky_cache_key_count: usize,
    pub moved_cache_key_count: usize,
    pub movement_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FailureSummary {
    pub status_code: u16,
    pub selected_backend_id: Option<String>,
    pub error: Option<String>,
    pub count: usize,
}

pub fn summarize_with_capacity(
    results: &[RequestResult],
    backend_capacity_shares: BTreeMap<String, f64>,
) -> RunSummary {
    let request_count = results.len();
    let successes = results.iter().filter(|result| result.ok).count();
    let success_rate = if request_count == 0 {
        0.0
    } else {
        successes as f64 / request_count as f64
    };

    let mut ttft: Vec<u64> = results
        .iter()
        .filter_map(|result| result.first_output_ms)
        .collect();
    let mut ttlt: Vec<u64> = results.iter().map(|result| result.completion_ms).collect();
    ttft.sort_unstable();
    ttlt.sort_unstable();

    let max_ttlt_ms = results
        .iter()
        .map(|result| result.completion_ms)
        .max()
        .unwrap_or(0);
    let total_length_ms = total_length_ms(results);
    let backend_request_shares = backend_request_shares(results);
    let backend_input_token_shares = backend_token_shares(results, TokenKind::Input);
    let backend_output_token_shares = backend_token_shares(results, TokenKind::Output);
    let balance_score = if backend_request_shares.is_empty() {
        None
    } else {
        Some(equal_share_balance_score(&backend_request_shares))
    };
    let capacity_balance_score =
        if backend_request_shares.is_empty() || backend_capacity_shares.is_empty() {
            None
        } else {
            Some(expected_share_balance_score(
                &backend_request_shares,
                &backend_capacity_shares,
            ))
        };
    let cache_summary = cache_summary(results);
    let backend_summaries = backend_summaries(results);
    let stickiness_summary = stickiness_summary(results);
    let failure_summary = failure_summary(results);

    RunSummary {
        request_count,
        success_rate,
        avg_ttft_ms: average(&ttft),
        p50_ttft_ms: percentile(&ttft, 0.50),
        p95_ttft_ms: percentile(&ttft, 0.95),
        p99_ttft_ms: percentile(&ttft, 0.99),
        avg_ttlt_ms: average(&ttlt).unwrap_or(0.0),
        p50_ttlt_ms: percentile(&ttlt, 0.50).unwrap_or(0),
        p95_ttlt_ms: percentile(&ttlt, 0.95).unwrap_or(0),
        p99_ttlt_ms: percentile(&ttlt, 0.99).unwrap_or(0),
        max_ttlt_ms,
        total_length_ms,
        balance_score,
        capacity_balance_score,
        backend_request_shares,
        backend_capacity_shares,
        backend_input_token_shares,
        backend_output_token_shares,
        backend_summaries,
        cache_summary,
        stickiness_summary,
        failure_summary,
        queue_admission_summary: QueueAdmissionSummary::default(),
    }
}

pub fn queue_admission_summary_from_prometheus(metrics: &str) -> QueueAdmissionSummary {
    let mut summary = QueueAdmissionSummary::default();
    for line in metrics.lines() {
        let Some((series, value)) = prometheus_counter_sample(line) else {
            continue;
        };
        let name = series.split_once('{').map_or(series, |(name, _)| name);
        match name {
            "pylon_queue_admission_decisions_total"
            | "pylon_queue_admission_decisions_total_total" => {
                match prometheus_label_value(series, "result") {
                    Some("accepted") => summary.pylon_accepted_count += value,
                    Some("rejected") => summary.pylon_rejected_count += value,
                    Some("disabled") => summary.pylon_disabled_count += value,
                    Some("missing_estimate") => summary.pylon_missing_estimate_count += value,
                    Some("unknown_local_estimate") => {
                        summary.pylon_unknown_local_estimate_count += value;
                    }
                    _ => {}
                }
            }
            "stargate_proxy_retries_total" | "stargate_proxy_retries_total_total"
                if prometheus_label_value(series, "reason") == Some("queue_estimate_mismatch") =>
            {
                summary.stargate_queue_mismatch_retry_count += value;
            }
            "stargate_proxy_retry_exhausted_total"
            | "stargate_proxy_retry_exhausted_total_total" => {
                summary.stargate_retry_exhausted_count += value;
                let reason = prometheus_label_value(series, "reason")
                    .unwrap_or("unlabeled")
                    .to_string();
                *summary
                    .stargate_retry_exhausted_by_reason
                    .entry(reason)
                    .or_default() += value;
            }
            _ => {}
        }
    }
    summary
}

pub fn queue_admission_summary_delta_from_prometheus(
    baseline_metrics: &str,
    post_replay_metrics: &str,
) -> QueueAdmissionSummary {
    let baseline = queue_admission_summary_from_prometheus(baseline_metrics);
    let post_replay = queue_admission_summary_from_prometheus(post_replay_metrics);
    QueueAdmissionSummary {
        pylon_accepted_count: counter_delta(
            post_replay.pylon_accepted_count,
            baseline.pylon_accepted_count,
        ),
        pylon_rejected_count: counter_delta(
            post_replay.pylon_rejected_count,
            baseline.pylon_rejected_count,
        ),
        pylon_disabled_count: counter_delta(
            post_replay.pylon_disabled_count,
            baseline.pylon_disabled_count,
        ),
        pylon_missing_estimate_count: counter_delta(
            post_replay.pylon_missing_estimate_count,
            baseline.pylon_missing_estimate_count,
        ),
        pylon_unknown_local_estimate_count: counter_delta(
            post_replay.pylon_unknown_local_estimate_count,
            baseline.pylon_unknown_local_estimate_count,
        ),
        stargate_queue_mismatch_retry_count: counter_delta(
            post_replay.stargate_queue_mismatch_retry_count,
            baseline.stargate_queue_mismatch_retry_count,
        ),
        stargate_retry_exhausted_count: counter_delta(
            post_replay.stargate_retry_exhausted_count,
            baseline.stargate_retry_exhausted_count,
        ),
        stargate_retry_exhausted_by_reason: post_replay
            .stargate_retry_exhausted_by_reason
            .iter()
            .filter_map(|(reason, post_replay_count)| {
                let count = counter_delta(
                    *post_replay_count,
                    baseline
                        .stargate_retry_exhausted_by_reason
                        .get(reason)
                        .copied()
                        .unwrap_or_default(),
                );
                (count > 0.0).then(|| (reason.clone(), count))
            })
            .collect(),
    }
}

fn counter_delta(post_replay: f64, baseline: f64) -> f64 {
    (post_replay - baseline).max(0.0)
}

fn prometheus_counter_sample(line: &str) -> Option<(&str, f64)> {
    let mut fields = line.split_whitespace();
    let series = fields.next()?;
    if series.starts_with('#') {
        return None;
    }
    let value = fields.next()?.parse::<f64>().ok()?;
    Some((series, value))
}

fn prometheus_label_value<'a>(series: &'a str, label: &str) -> Option<&'a str> {
    let needle = format!(r#"{label}=""#);
    let start = series.find(&needle)? + needle.len();
    let rest = &series[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

pub fn backend_capacity_shares(backends: &BackendConfig) -> BTreeMap<String, f64> {
    let mut capacities = BTreeMap::new();
    let mut total_capacity = 0.0f64;
    for index in 0..backends.count {
        let capacity = backends
            .profile_for_index(index)
            .registration
            .last_mean_input_tps;
        if capacity > 0.0 && capacity.is_finite() {
            capacities.insert(format!("backend-{index}"), capacity);
            total_capacity += capacity;
        }
    }
    if total_capacity <= 0.0 {
        return BTreeMap::new();
    }
    capacities
        .into_iter()
        .map(|(backend_id, capacity)| (backend_id, capacity / total_capacity))
        .collect()
}

fn total_length_ms(results: &[RequestResult]) -> u64 {
    let Some(first_dispatch_ms) = results.iter().map(|result| result.dispatch_offset_ms).min()
    else {
        return 0;
    };
    let last_completion_ms = results
        .iter()
        .map(|result| {
            // Broken or synthetic benchmark inputs should not wrap the report window.
            result
                .dispatch_offset_ms
                .saturating_add(result.completion_ms)
        })
        .max()
        .unwrap_or(first_dispatch_ms);
    // If input rows are out of order or malformed, report a zero-length window instead of wrapping.
    last_completion_ms.saturating_sub(first_dispatch_ms)
}

fn backend_request_shares(results: &[RequestResult]) -> BTreeMap<String, f64> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut total = 0usize;
    for result in results {
        if !result.ok {
            continue;
        }
        if let Some(backend_id) = &result.selected_backend_id {
            *counts.entry(backend_id.clone()).or_default() += 1;
            total += 1;
        }
    }

    if total == 0 {
        return BTreeMap::new();
    }

    counts
        .into_iter()
        .map(|(backend_id, count)| (backend_id, count as f64 / total as f64))
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum TokenKind {
    Input,
    Output,
}

fn backend_token_shares(results: &[RequestResult], token_kind: TokenKind) -> BTreeMap<String, f64> {
    let mut totals: BTreeMap<String, u64> = BTreeMap::new();
    let mut total = 0u64;
    for result in results {
        if !result.ok {
            continue;
        }
        let Some(backend_id) = &result.selected_backend_id else {
            continue;
        };
        let tokens = match token_kind {
            TokenKind::Input => result.input_tokens,
            TokenKind::Output => result.output_tokens,
        };
        *totals.entry(backend_id.clone()).or_default() += tokens;
        // Benchmark token totals are report counters; saturate instead of wrapping on bad input.
        total = total.saturating_add(tokens);
    }

    if total == 0 {
        return BTreeMap::new();
    }

    totals
        .into_iter()
        .map(|(backend_id, tokens)| (backend_id, tokens as f64 / total as f64))
        .collect()
}

fn backend_summaries(results: &[RequestResult]) -> BTreeMap<String, BackendSummary> {
    let mut grouped: BTreeMap<String, Vec<&RequestResult>> = BTreeMap::new();
    for result in results {
        let Some(backend_id) = &result.selected_backend_id else {
            continue;
        };
        grouped.entry(backend_id.clone()).or_default().push(result);
    }

    grouped
        .into_iter()
        .map(|(backend_id, results)| {
            let request_count = results.len();
            let success_count = results.iter().filter(|result| result.ok).count();
            let input_tokens = results
                .iter()
                .map(|result| result.input_tokens)
                .sum::<u64>();
            let output_tokens = results
                .iter()
                .map(|result| result.output_tokens)
                .sum::<u64>();
            let mut ttlt = results
                .iter()
                .map(|result| result.completion_ms)
                .collect::<Vec<_>>();
            ttlt.sort_unstable();
            let observed_cache = results
                .iter()
                .filter(|result| result.kv_cache_hit.is_some())
                .count();
            let cache_hits = results
                .iter()
                .filter(|result| result.kv_cache_hit == Some(true))
                .count();
            let cache_hit_rate =
                (observed_cache > 0).then_some(cache_hits as f64 / observed_cache as f64);
            let cache_eviction_count = results
                .iter()
                .filter_map(|result| result.kv_cache_evicted_entries)
                .sum();
            let cache_evicted_tokens = results
                .iter()
                .filter_map(|result| result.kv_cache_evicted_tokens)
                .sum();
            (
                backend_id,
                BackendSummary {
                    request_count,
                    success_count,
                    input_tokens,
                    output_tokens,
                    avg_ttlt_ms: average(&ttlt),
                    p95_ttlt_ms: percentile(&ttlt, 0.95),
                    cache_hit_rate,
                    cache_eviction_count,
                    cache_evicted_tokens,
                },
            )
        })
        .collect()
}

fn stickiness_summary(results: &[RequestResult]) -> StickinessSummary {
    let mut backends_by_cache_key = BTreeMap::<String, BTreeSet<String>>::new();
    for result in results {
        if !result.ok {
            continue;
        }
        let (Some(cache_affinity_key), Some(backend_id)) =
            (&result.cache_affinity_key, &result.selected_backend_id)
        else {
            continue;
        };
        backends_by_cache_key
            .entry(cache_affinity_key.clone())
            .or_default()
            .insert(backend_id.clone());
    }
    let observed_cache_key_count = backends_by_cache_key.len();
    let moved_cache_key_count = backends_by_cache_key
        .values()
        .filter(|backends| backends.len() > 1)
        .count();
    let sticky_cache_key_count = observed_cache_key_count - moved_cache_key_count;
    let movement_rate = (observed_cache_key_count > 0)
        .then_some(moved_cache_key_count as f64 / observed_cache_key_count as f64);
    StickinessSummary {
        observed_cache_key_count,
        sticky_cache_key_count,
        moved_cache_key_count,
        movement_rate,
    }
}

fn failure_summary(results: &[RequestResult]) -> Vec<FailureSummary> {
    let mut counts = BTreeMap::<(u16, Option<String>, Option<String>), usize>::new();
    for result in results {
        if result.ok {
            continue;
        }
        let key = (
            result.status_code,
            result.selected_backend_id.clone(),
            result.error.clone(),
        );
        *counts.entry(key).or_default() += 1;
    }
    counts
        .into_iter()
        .map(
            |((status_code, selected_backend_id, error), count)| FailureSummary {
                status_code,
                selected_backend_id,
                error,
                count,
            },
        )
        .collect()
}

fn equal_share_balance_score(shares: &BTreeMap<String, f64>) -> f64 {
    let expected = 1.0 / shares.len() as f64;
    let mean_abs_error = shares
        .values()
        .map(|observed| (observed - expected).abs())
        .sum::<f64>()
        / shares.len() as f64;
    (1.0 - mean_abs_error / expected).clamp(0.0, 1.0)
}

fn expected_share_balance_score(
    observed: &BTreeMap<String, f64>,
    expected: &BTreeMap<String, f64>,
) -> f64 {
    let backend_ids = expected
        .keys()
        .chain(observed.keys())
        .collect::<BTreeSet<_>>();
    let total_abs_error = backend_ids
        .into_iter()
        .map(|backend_id| {
            let observed = observed.get(backend_id).copied().unwrap_or(0.0);
            let expected = expected.get(backend_id).copied().unwrap_or(0.0);
            (observed - expected).abs()
        })
        .sum::<f64>();
    (1.0 - total_abs_error / 2.0).clamp(0.0, 1.0)
}

fn cache_summary(results: &[RequestResult]) -> CacheSummary {
    let observed_request_count = results
        .iter()
        .filter(|result| result.kv_cache_hit.is_some())
        .count();
    let hit_count = results
        .iter()
        .filter(|result| result.kv_cache_hit == Some(true))
        .count();
    let miss_count = results
        .iter()
        .filter(|result| result.kv_cache_hit == Some(false))
        .count();
    let hit_rate =
        (observed_request_count > 0).then_some(hit_count as f64 / observed_request_count as f64);
    let eviction_count = results
        .iter()
        .filter_map(|result| result.kv_cache_evicted_entries)
        .sum();
    let evicted_tokens = results
        .iter()
        .filter_map(|result| result.kv_cache_evicted_tokens)
        .sum();
    CacheSummary {
        observed_request_count,
        hit_count,
        miss_count,
        hit_rate,
        eviction_count,
        evicted_tokens,
    }
}

fn average(values: &[u64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64)
    }
}

fn percentile(values: &[u64], q: f64) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) as f64 * q).round() as usize;
    values.get(index).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(id: usize, backend: &str, ttft: u64, ttlt: u64) -> RequestResult {
        RequestResult {
            request_index: id,
            request_id: format!("req-{id}"),
            routing_key: None,
            cache_affinity_key: None,
            input_tokens: 1,
            output_tokens: 1,
            scheduled_offset_ms: 0,
            status_code: 200,
            selected_backend_id: Some(backend.to_string()),
            dispatch_offset_ms: 0,
            response_headers_ms: Some(1),
            first_output_ms: Some(ttft),
            completion_ms: ttlt,
            kv_cache_hit: None,
            kv_cache_evicted_entries: None,
            kv_cache_evicted_tokens: None,
            ok: true,
            error: None,
        }
    }

    #[test]
    fn summary_computes_basic_metrics() {
        let summary = summarize_with_capacity(
            &[
                result(0, "a", 10, 20),
                result(1, "b", 30, 40),
                result(2, "a", 20, 50),
                result(3, "b", 40, 60),
            ],
            BTreeMap::new(),
        );
        assert_eq!(summary.request_count, 4);
        assert_eq!(summary.p50_ttft_ms, Some(30));
        assert_eq!(summary.p95_ttlt_ms, 60);
        assert_eq!(summary.max_ttlt_ms, 60);
        assert_eq!(summary.total_length_ms, 60);
        assert_eq!(summary.balance_score, Some(1.0));
        assert_eq!(summary.capacity_balance_score, None);
    }

    #[test]
    fn total_length_accounts_for_dispatch_offsets() {
        let mut first = result(0, "a", 10, 20);
        first.dispatch_offset_ms = 100;
        let mut second = result(1, "a", 10, 40);
        second.dispatch_offset_ms = 250;

        let summary = summarize_with_capacity(&[first, second], BTreeMap::new());

        assert_eq!(summary.total_length_ms, 190);
        assert_eq!(summary.max_ttlt_ms, 40);
    }

    #[test]
    fn summary_computes_capacity_balance_score() {
        let mut expected = BTreeMap::new();
        expected.insert("a".to_string(), 0.75);
        expected.insert("b".to_string(), 0.25);

        let summary = summarize_with_capacity(
            &[
                result(0, "a", 10, 20),
                result(1, "a", 10, 20),
                result(2, "a", 10, 20),
                result(3, "b", 10, 20),
            ],
            expected,
        );

        assert_eq!(summary.capacity_balance_score, Some(1.0));
    }

    #[test]
    fn summary_computes_cache_metrics() {
        let mut hit = result(0, "a", 10, 20);
        hit.kv_cache_hit = Some(true);
        let mut miss = result(1, "a", 10, 20);
        miss.kv_cache_hit = Some(false);
        miss.kv_cache_evicted_entries = Some(2);
        miss.kv_cache_evicted_tokens = Some(150);

        let summary = summarize_with_capacity(&[hit, miss], BTreeMap::new());

        assert_eq!(
            summary.cache_summary,
            CacheSummary {
                observed_request_count: 2,
                hit_count: 1,
                miss_count: 1,
                hit_rate: Some(0.5),
                eviction_count: 2,
                evicted_tokens: 150,
            }
        );
    }

    #[test]
    fn summary_computes_token_shares_and_backend_summaries() {
        let mut first = result(0, "a", 10, 20);
        first.input_tokens = 100;
        first.output_tokens = 10;
        first.kv_cache_hit = Some(true);
        let mut second = result(1, "b", 10, 40);
        second.input_tokens = 300;
        second.output_tokens = 30;
        second.kv_cache_hit = Some(false);

        let summary = summarize_with_capacity(&[first, second], BTreeMap::new());

        assert_eq!(summary.backend_input_token_shares["a"], 0.25);
        assert_eq!(summary.backend_output_token_shares["b"], 0.75);
        assert_eq!(summary.backend_summaries["a"].request_count, 1);
        assert_eq!(summary.backend_summaries["a"].cache_hit_rate, Some(1.0));
        assert_eq!(summary.backend_summaries["b"].p95_ttlt_ms, Some(40));
    }

    #[test]
    fn summary_computes_stickiness_and_failures() {
        let mut first = result(0, "a", 10, 20);
        first.cache_affinity_key = Some("cak-a".to_string());
        let mut second = result(1, "b", 10, 20);
        second.cache_affinity_key = Some("cak-a".to_string());
        let mut failed = result(2, "b", 10, 20);
        failed.ok = false;
        failed.status_code = 502;
        failed.error = Some("upstream closed".to_string());

        let summary = summarize_with_capacity(&[first, second, failed], BTreeMap::new());

        assert_eq!(summary.stickiness_summary.observed_cache_key_count, 1);
        assert_eq!(summary.stickiness_summary.moved_cache_key_count, 1);
        assert_eq!(summary.stickiness_summary.movement_rate, Some(1.0));
        assert_eq!(summary.failure_summary.len(), 1);
        assert_eq!(summary.failure_summary[0].status_code, 502);
        assert_eq!(summary.failure_summary[0].count, 1);
    }

    #[test]
    fn parses_queue_admission_and_retry_counters_from_native_metrics() {
        let metrics = r#"
pylon_queue_admission_decisions_total{inference_server_id="backend-0",model_id="dummy-model",result="rejected"} 2
pylon_queue_admission_decisions_total{inference_server_id="backend-1",model_id="dummy-model",result="disabled"} 4
stargate_proxy_retries_total{model="dummy-model",reason="queue_estimate_mismatch",routing_key=""} 2
stargate_proxy_retry_exhausted_total{model="dummy-model",reason="retry_budget_exhausted",routing_key=""} 1
"#;

        let summary = queue_admission_summary_from_prometheus(metrics);

        assert_eq!(summary.pylon_rejected_count, 2.0);
        assert_eq!(summary.pylon_disabled_count, 4.0);
        assert_eq!(summary.stargate_queue_mismatch_retry_count, 2.0);
        assert_eq!(summary.stargate_retry_exhausted_count, 1.0);
        assert_eq!(
            summary.stargate_retry_exhausted_by_reason["retry_budget_exhausted"],
            1.0
        );
    }

    #[test]
    fn parses_collector_renamed_counter_metrics() {
        let metrics = r#"
pylon_queue_admission_decisions_total_total{inference_server_id="backend-0",model_id="dummy-model",result="rejected"} 3
pylon_queue_admission_decisions_total_total{inference_server_id="backend-1",model_id="dummy-model",result="disabled"} 7
stargate_proxy_retries_total_total{model="dummy-model",reason="queue_estimate_mismatch",routing_key=""} 3
stargate_proxy_retry_exhausted_total_total{model="dummy-model",reason="queue_estimate_mismatch",routing_key=""} 2
"#;

        let summary = queue_admission_summary_from_prometheus(metrics);

        assert_eq!(summary.pylon_rejected_count, 3.0);
        assert_eq!(summary.pylon_disabled_count, 7.0);
        assert_eq!(summary.stargate_queue_mismatch_retry_count, 3.0);
        assert_eq!(summary.stargate_retry_exhausted_count, 2.0);
        assert_eq!(
            summary.stargate_retry_exhausted_by_reason["queue_estimate_mismatch"],
            2.0
        );
    }

    #[test]
    fn queue_admission_delta_excludes_pre_replay_probe_counters() {
        let baseline = r#"
pylon_queue_admission_decisions_total_total{inference_server_id="backend-0",model_id="dummy-model",result="disabled"} 1
stargate_proxy_retries_total_total{model="dummy-model",reason="queue_estimate_mismatch",routing_key=""} 2
stargate_proxy_retry_exhausted_total_total{model="dummy-model",reason="retry_budget_exhausted",routing_key=""} 1
"#;
        let post_replay = r#"
pylon_queue_admission_decisions_total_total{inference_server_id="backend-0",model_id="dummy-model",result="disabled"} 97
stargate_proxy_retries_total_total{model="dummy-model",reason="queue_estimate_mismatch",routing_key=""} 28
stargate_proxy_retry_exhausted_total_total{model="dummy-model",reason="retry_budget_exhausted",routing_key=""} 4
"#;

        let summary = queue_admission_summary_delta_from_prometheus(baseline, post_replay);

        assert_eq!(summary.pylon_disabled_count, 96.0);
        assert_eq!(summary.stargate_queue_mismatch_retry_count, 26.0);
        assert_eq!(summary.stargate_retry_exhausted_count, 3.0);
        assert_eq!(
            summary.stargate_retry_exhausted_by_reason["retry_budget_exhausted"],
            3.0
        );
    }

    #[test]
    fn legacy_summary_without_queue_admission_metrics_defaults_cleanly() {
        let summary = summarize_with_capacity(&[], BTreeMap::new());
        let mut json = serde_json::to_value(&summary).expect("summary should serialize");
        json.as_object_mut()
            .expect("summary should be an object")
            .remove("queue_admission_summary");

        let parsed: RunSummary =
            serde_json::from_value(json).expect("legacy summary should deserialize");

        assert_eq!(
            parsed.queue_admission_summary,
            QueueAdmissionSummary::default()
        );
    }
}
