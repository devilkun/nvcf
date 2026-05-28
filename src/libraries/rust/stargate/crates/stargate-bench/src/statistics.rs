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

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

const BOOTSTRAP_RESAMPLES: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ConfidenceInterval {
    pub lower: f64,
    pub upper: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DistributionStats {
    pub count: usize,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub median: Option<f64>,
    pub mad: Option<f64>,
    pub coefficient_of_variation: Option<f64>,
    pub p90: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
    pub mean_ci_95: Option<ConfidenceInterval>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum NoiseClassification {
    Reliable,
    Noisy,
    Inconclusive,
}

pub fn summarize_distribution(values: &[f64], bootstrap_seed: u64) -> DistributionStats {
    if values.is_empty() {
        return DistributionStats {
            count: 0,
            min: None,
            max: None,
            mean: None,
            median: None,
            mad: None,
            coefficient_of_variation: None,
            p90: None,
            p95: None,
            p99: None,
            mean_ci_95: None,
        };
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.total_cmp(right));
    let mean = average(&sorted);
    let median = percentile(&sorted, 0.50);
    let mad = median.map(|median| {
        let mut deviations = sorted
            .iter()
            .map(|value| (value - median).abs())
            .collect::<Vec<_>>();
        deviations.sort_by(|left, right| left.total_cmp(right));
        percentile(&deviations, 0.50).unwrap_or(0.0)
    });
    let coefficient_of_variation = if sorted.len() > 1 && mean.abs() > f64::EPSILON {
        Some(stddev(&sorted, mean) / mean.abs())
    } else {
        None
    };

    DistributionStats {
        count: sorted.len(),
        min: sorted.first().copied(),
        max: sorted.last().copied(),
        mean: Some(mean),
        median,
        mad,
        coefficient_of_variation,
        p90: percentile(&sorted, 0.90),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        mean_ci_95: bootstrap_mean_ci(&sorted, bootstrap_seed),
    }
}

pub fn classify_noise(stats: &DistributionStats, noise_threshold_cv: f64) -> NoiseClassification {
    if stats.count < 2 || !noise_threshold_cv.is_finite() || noise_threshold_cv < 0.0 {
        return NoiseClassification::Inconclusive;
    }
    match stats.coefficient_of_variation {
        Some(cv) if cv <= noise_threshold_cv => NoiseClassification::Reliable,
        Some(_) => NoiseClassification::Noisy,
        None => NoiseClassification::Inconclusive,
    }
}

fn bootstrap_mean_ci(values: &[f64], seed: u64) -> Option<ConfidenceInterval> {
    if values.len() < 2 {
        return None;
    }

    let mut rng = StdRng::seed_from_u64(seed);
    let mut means = Vec::with_capacity(BOOTSTRAP_RESAMPLES);
    for _ in 0..BOOTSTRAP_RESAMPLES {
        let mut total = 0.0;
        for _ in 0..values.len() {
            let index = rng.random_range(0..values.len());
            total += values[index];
        }
        means.push(total / values.len() as f64);
    }
    means.sort_by(|left, right| left.total_cmp(right));
    Some(ConfidenceInterval {
        lower: percentile(&means, 0.025).unwrap_or_else(|| means[0]),
        upper: percentile(&means, 0.975).unwrap_or_else(|| means[means.len() - 1]),
    })
}

fn average(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn stddev(values: &[f64], mean: f64) -> f64 {
    let variance = values
        .iter()
        .map(|value| {
            let delta = value - mean;
            delta * delta
        })
        .sum::<f64>()
        / (values.len() - 1) as f64;
    variance.sqrt()
}

fn percentile(sorted_values: &[f64], percentile: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }
    let index = upper_nearest_rank_index(sorted_values.len(), percentile)?;
    sorted_values.get(index).copied()
}

pub(crate) fn upper_nearest_rank_index(len: usize, percentile: f64) -> Option<usize> {
    if len == 0 || !percentile.is_finite() {
        return None;
    }
    // Keep benchmark reports conservative and consistent with the pre-existing transport summary:
    // even-sized samples choose the upper rank, so p50([10, 1000]) reports 1000.
    Some(((len - 1) as f64 * percentile.clamp(0.0, 1.0)).ceil() as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarizes_distribution_with_seeded_confidence_interval() {
        let stats = summarize_distribution(&[10.0, 12.0, 11.0, 13.0], 7);

        assert_eq!(stats.count, 4);
        assert_eq!(stats.min, Some(10.0));
        assert_eq!(stats.max, Some(13.0));
        assert_eq!(stats.median, Some(12.0));
        assert_eq!(stats.p95, Some(13.0));
        let interval = stats.mean_ci_95.expect("confidence interval should exist");
        assert!(interval.lower >= 10.0);
        assert!(interval.upper <= 13.0);
        assert!(interval.lower <= interval.upper);
    }

    #[test]
    fn classifies_single_trial_as_inconclusive() {
        let stats = summarize_distribution(&[100.0], 1);

        assert_eq!(stats.mean_ci_95, None);
        assert_eq!(
            classify_noise(&stats, 0.02),
            NoiseClassification::Inconclusive
        );
    }

    #[test]
    fn classifies_low_and_high_variance_trials() {
        let stable = summarize_distribution(&[100.0, 100.5, 99.8, 100.1], 1);
        let noisy = summarize_distribution(&[50.0, 150.0, 75.0, 200.0], 1);

        assert_eq!(classify_noise(&stable, 0.02), NoiseClassification::Reliable);
        assert_eq!(classify_noise(&noisy, 0.02), NoiseClassification::Noisy);
    }
}
