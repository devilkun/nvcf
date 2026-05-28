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

use super::{
    QualityCheckResult, RequestQualityMonitorConfig, TextQualityMetrics,
    approximate_output_token_count, evaluate_quality,
};
use std::collections::BTreeMap;

#[derive(Debug, Default)]
struct ChoiceQualityObservation {
    output_text: String,
    observed_logprobs: Vec<f32>,
    observed_output_tokens: u64,
}

#[derive(Debug, Default)]
pub struct RequestQualityRecorder {
    choices: BTreeMap<usize, ChoiceQualityObservation>,
    output_tokens: u64,
    observed_chunk: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestOutputTokenProgress {
    Delta(u64),
    Cumulative { tokens: u64, delta: u64 },
}

impl RequestQualityRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    fn observe_output_text(&mut self, choice_index: usize, text: &str) {
        self.choices
            .entry(choice_index)
            .or_default()
            .output_text
            .push_str(text);
    }

    fn observe_logprobs(&mut self, choice_index: usize, logprobs: Vec<f32>) {
        self.choices
            .entry(choice_index)
            .or_default()
            .observed_logprobs
            .extend(logprobs);
    }

    fn observe_output_tokens(&mut self, choice_index: usize, output_tokens: u64) {
        let choice = self.choices.entry(choice_index).or_default();
        // Token observations are telemetry counters; saturate instead of wrapping on malformed streams.
        choice.observed_output_tokens = choice.observed_output_tokens.saturating_add(output_tokens);
    }

    fn observe_cumulative_output_tokens(&mut self, choice_index: usize, output_tokens: u64) {
        let choice = self.choices.entry(choice_index).or_default();
        choice.observed_output_tokens = output_tokens;
    }

    #[cfg(test)]
    pub fn observe_sse_chunk(&mut self, raw_data: &str, output_token_delta: Option<u64>) {
        self.observe_sse_chunk_with_token_progress(
            raw_data,
            output_token_delta.map(RequestOutputTokenProgress::Delta),
        );
    }

    pub(crate) fn observe_sse_chunk_with_token_progress(
        &mut self,
        raw_data: &str,
        output_token_progress: Option<RequestOutputTokenProgress>,
    ) {
        self.observed_chunk = true;
        let parsed: serde_json::Value = serde_json::from_str(raw_data).unwrap_or_default();
        let choice_indices = extract_choice_indices(&parsed);

        for (choice_index, content) in extract_delta_contents(&parsed) {
            self.observe_output_text(choice_index, &content);
        }
        for (choice_index, logprobs) in extract_delta_logprobs(&parsed) {
            self.observe_logprobs(choice_index, logprobs);
        }
        if let Some(progress) = output_token_progress {
            self.observe_output_token_progress_for_choice(
                progress,
                choice_indices
                    .first()
                    .copied()
                    .filter(|_| choice_indices.len() == 1),
            );
        }
    }

    fn observe_output_token_progress_for_choice(
        &mut self,
        progress: RequestOutputTokenProgress,
        single_chunk_choice: Option<usize>,
    ) {
        match progress {
            RequestOutputTokenProgress::Delta(delta) => {
                // Token observations are telemetry counters; saturate instead of wrapping.
                self.output_tokens = self.output_tokens.saturating_add(delta);
                if let Some(choice_index) = single_chunk_choice {
                    self.observe_output_tokens(choice_index, delta);
                } else if self.choices.len() == 1
                    && let Some(choice_index) = self.choices.keys().next().copied()
                {
                    self.observe_output_tokens(choice_index, delta);
                }
            }
            RequestOutputTokenProgress::Cumulative { tokens, delta } => {
                self.output_tokens = tokens;
                if self.choices.len() == 1 {
                    if let Some(choice_index) = self.choices.keys().next().copied() {
                        self.observe_cumulative_output_tokens(choice_index, tokens);
                    }
                } else if let Some(choice_index) = single_chunk_choice {
                    self.observe_output_tokens(choice_index, delta);
                }
            }
        }
    }

    pub fn evaluate(
        &self,
        config: &RequestQualityMonitorConfig,
    ) -> (TextQualityMetrics, QualityCheckResult) {
        let output_tokens = self.total_output_tokens();
        let request_output_tokens_evaluable = config.output_tokens_threshold_min.is_some()
            && request_output_tokens_are_evaluable(self);
        let output_tokens_match_reason = request_output_tokens_evaluable.then_some(()).and(
            config
                .output_tokens_threshold_min
                .filter(|threshold| output_tokens > u64::from(*threshold))
                .map(|_| "output_tokens"),
        );

        // Request-level token thresholds operate on the full request, but the text and
        // logprob heuristics need to score each streamed choice independently.
        let per_choice_config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: None,
            ..config.clone()
        };

        let mut any_choice_evaluated = false;
        let mut representative_metrics = None;
        let mut best_choice_match = None;
        let is_multi_choice_request = self.choices.len() > 1;

        for choice in self.choices.values() {
            let choice_output_tokens =
                choice_output_tokens(choice, is_multi_choice_request, self.output_tokens);
            let (metrics, result) = evaluate_quality(
                &choice.output_text,
                choice_output_tokens,
                median_logprob(&choice.observed_logprobs),
                &per_choice_config,
            );
            if result.evaluated {
                any_choice_evaluated = true;
                representative_metrics.get_or_insert(metrics);
            }
            if let Some(reason) = result.threshold_match_reason {
                let priority = threshold_match_priority(reason);
                match best_choice_match {
                    Some((best_priority, _, _)) if best_priority <= priority => {}
                    _ => best_choice_match = Some((priority, reason, metrics)),
                }
            }
        }

        let threshold_match_reason =
            output_tokens_match_reason.or(best_choice_match.map(|(_, reason, _)| reason));
        let representative_metrics = if output_tokens_match_reason.is_some() {
            representative_metrics.unwrap_or_default()
        } else {
            best_choice_match
                .map(|(_, _, metrics)| metrics)
                .or(representative_metrics)
                .unwrap_or_default()
        };

        (
            representative_metrics,
            QualityCheckResult {
                evaluated: request_output_tokens_evaluable || any_choice_evaluated,
                threshold_match_reason,
            },
        )
    }

    pub fn has_observed_stream_output(&self) -> bool {
        self.observed_chunk
    }
    fn total_output_tokens(&self) -> u64 {
        if self.output_tokens != 0 {
            return self.output_tokens;
        }

        self.choices
            .values()
            .map(|choice| approximate_output_token_count(&choice.output_text))
            .sum()
    }
}

fn extract_choice_indices(value: &serde_json::Value) -> Vec<usize> {
    value
        .get("choices")
        .and_then(|choices| choices.as_array())
        .map(|choices| {
            choices
                .iter()
                .enumerate()
                .map(|(fallback_index, choice)| {
                    choice
                        .get("index")
                        .and_then(|index| index.as_u64())
                        .map(|index| index as usize)
                        .unwrap_or(fallback_index)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_delta_contents(value: &serde_json::Value) -> Vec<(usize, String)> {
    value
        .get("choices")
        .and_then(|choices| choices.as_array())
        .map(|choices| {
            choices
                .iter()
                .enumerate()
                .filter_map(|(fallback_index, choice)| {
                    let choice_index = choice
                        .get("index")
                        .and_then(|index| index.as_u64())
                        .map(|index| index as usize)
                        .unwrap_or(fallback_index);
                    choice
                        .get("delta")
                        .and_then(|delta| delta.get("content"))
                        .and_then(|content| content.as_str())
                        .map(|content| (choice_index, content.to_owned()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn extract_delta_logprobs(value: &serde_json::Value) -> Vec<(usize, Vec<f32>)> {
    let Some(choices) = value.get("choices").and_then(|choices| choices.as_array()) else {
        return Vec::new();
    };

    choices
        .iter()
        .enumerate()
        .map(|(fallback_index, choice)| {
            let choice_index = choice
                .get("index")
                .and_then(|index| index.as_u64())
                .map(|index| index as usize)
                .unwrap_or(fallback_index);
            let logprobs = choice
                .get("logprobs")
                .and_then(|logprobs| logprobs.get("content"))
                .and_then(|content| content.as_array())
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|entry| entry.get("logprob"))
                        .filter_map(|logprob| logprob.as_f64().map(|value| value as f32))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            (choice_index, logprobs)
        })
        .filter(|(_, logprobs)| !logprobs.is_empty())
        .collect()
}

fn median_logprob(values: &[f32]) -> Option<f32> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f32::total_cmp);
    sorted.get(sorted.len() / 2).copied()
}

fn choice_output_tokens(
    choice: &ChoiceQualityObservation,
    is_multi_choice_request: bool,
    request_output_tokens: u64,
) -> u64 {
    if choice.observed_output_tokens != 0 {
        return choice.observed_output_tokens;
    }
    if !choice.observed_logprobs.is_empty() {
        return choice.observed_logprobs.len() as u64;
    }
    if request_output_tokens == 0 {
        return approximate_output_token_count(&choice.output_text);
    }
    if !is_multi_choice_request {
        return request_output_tokens;
    }

    // Request-wide usage cannot be safely split across multiple choices, so ambiguous
    // multi-choice streams should skip text heuristics rather than misclassifying them.
    0
}

fn request_output_tokens_are_evaluable(recorder: &RequestQualityRecorder) -> bool {
    let has_scoreable_choice_signal = recorder.choices.values().any(|choice| {
        approximate_output_token_count(&choice.output_text) > 0
            || !choice.observed_logprobs.is_empty()
    });
    if !has_scoreable_choice_signal {
        return false;
    }
    if recorder.output_tokens == 0 {
        return recorder.total_output_tokens() > 0;
    }
    if recorder.choices.len() == 1 {
        return true;
    }

    // Request-wide usage is only safe for the output-token threshold when every
    // emitted completion token was attributable to concrete scoreable choices.
    let attributable_output_tokens: u64 = recorder
        .choices
        .values()
        .map(|choice| choice.observed_output_tokens)
        .sum();
    attributable_output_tokens == recorder.output_tokens
}

fn threshold_match_priority(reason: &str) -> usize {
    match reason {
        "compression_ratio" => 0,
        "repetition_1gram" => 1,
        "repetition_2gram" => 2,
        "repetition_3gram" => 3,
        "degeneracy_score" => 4,
        "median_logprob" => 5,
        _ => usize::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_passes_median_logprob_to_quality_evaluator() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"delta":{"content":"word"},"logprobs":{"content":[{"token":"word","logprob":-7.5}]}}]}"#,
            Some(1),
        );
        let config = RequestQualityMonitorConfig {
            median_logprob_threshold_max: Some(-7.0),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);
        assert_eq!(result.threshold_match_reason, Some("median_logprob"));
    }

    #[test]
    fn recorder_tracks_whether_stream_output_was_observed() {
        let mut recorder = RequestQualityRecorder::new();
        assert!(!recorder.has_observed_stream_output());

        recorder.observe_sse_chunk(r#"{"choices":[{"delta":{"content":"hello"}}]}"#, Some(1));
        assert!(recorder.has_observed_stream_output());
    }

    #[test]
    fn recorder_accumulates_text_and_token_deltas_across_multiple_chunks() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"delta":{"content":"alpha beta"}}]}"#,
            Some(2),
        );
        recorder.observe_sse_chunk(
            r#"{"choices":[{"delta":{"content":" gamma delta"}}]}"#,
            Some(2),
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 1,
            output_tokens_threshold_min: Some(3),
            ..RequestQualityMonitorConfig::default()
        };

        let (metrics, result) = recorder.evaluate(&config);

        assert_eq!(metrics.compression_ratio, 1.0);
        assert_eq!(result.threshold_match_reason, Some("output_tokens"));
    }

    #[test]
    fn recorder_falls_back_to_whitespace_count_when_usage_deltas_are_absent() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(r#"{"choices":[{"delta":{"content":"alpha beta"}}]}"#, None);
        recorder.observe_sse_chunk(r#"{"choices":[{"delta":{"content":" gamma"}}]}"#, None);
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(2),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert_eq!(result.threshold_match_reason, Some("output_tokens"));
    }

    #[test]
    fn recorder_ignores_malformed_or_irrelevant_chunks_without_matching() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk("not-json", None);
        recorder.observe_sse_chunk(r#"{"object":"other"}"#, None);
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 1,
            output_compression_threshold_max: Some(0.5),
            median_logprob_threshold_max: Some(-1.0),
            ..RequestQualityMonitorConfig::default()
        };

        let (metrics, result) = recorder.evaluate(&config);

        assert_eq!(metrics.compression_ratio, 0.0);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_matches_when_any_choice_is_repetitive() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma delta"}},{"index":1,"delta":{"content":"loop loop loop loop"}}]}"#,
            None,
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 1,
            output_repetition_1gram_threshold_min: Some(0.3),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert_eq!(result.threshold_match_reason, Some("repetition_1gram"));
    }

    #[test]
    fn recorder_does_not_concatenate_choices_for_text_heuristics() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta"}},{"index":1,"delta":{"content":"alpha beta"}}]}"#,
            None,
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 1,
            output_repetition_1gram_threshold_min: Some(0.3),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_uses_observed_output_tokens_to_gate_per_choice_metrics() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"{\"k\":[1,2,3,4]}"}}]}"#,
            Some(8),
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 4,
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(
            result.evaluated,
            "real completion token deltas should open the per-choice metric gate"
        );
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_cumulative_output_tokens_correct_prior_estimate() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma delta epsilon"}}]}"#,
            Some(RequestOutputTokenProgress::Delta(5)),
        );
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"choices":[{"index":0,"delta":{},"usage":{"completion_tokens":3}}]}"#,
            Some(RequestOutputTokenProgress::Cumulative {
                tokens: 3,
                delta: 0,
            }),
        );
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(4),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_cumulative_progress_uses_delta_for_multi_choice_attribution() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma"}}]}"#,
            Some(RequestOutputTokenProgress::Cumulative {
                tokens: 3,
                delta: 3,
            }),
        );
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"choices":[{"index":1,"delta":{"content":"loop loop loop loop"}}]}"#,
            Some(RequestOutputTokenProgress::Cumulative {
                tokens: 4,
                delta: 1,
            }),
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 3,
            output_repetition_1gram_threshold_min: Some(0.3),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_accepts_terminal_usage_chunk_without_choices() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma"}}]}"#,
            Some(RequestOutputTokenProgress::Delta(3)),
        );
        recorder.observe_sse_chunk_with_token_progress(
            r#"{"object":"chat.completion.chunk","choices":[],"usage":{"completion_tokens":3}}"#,
            Some(RequestOutputTokenProgress::Cumulative {
                tokens: 3,
                delta: 0,
            }),
        );
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(2),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, Some("output_tokens"));
    }

    #[test]
    fn recorder_usage_less_no_space_output_still_reaches_quality_thresholds() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"哈哈哈哈哈哈"}}]}"#,
            None,
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 4,
            output_degeneracy_threshold_min: Some(0.8),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, Some("degeneracy_score"));
    }

    #[test]
    fn recorder_usage_less_structured_output_counts_token_like_units() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"{\"k\":[1,2,3,4,5,6]}"}}]}"#,
            None,
        );
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(6),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, Some("output_tokens"));
    }

    #[test]
    fn recorder_does_not_mark_role_only_stream_as_evaluated_for_token_threshold() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"role":"assistant"}}],"usage":{"completion_tokens":3}}"#,
            Some(3),
        );
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(10),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(!result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_marks_below_threshold_text_stream_as_clean_for_token_threshold() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma"}}],"usage":{"completion_tokens":3}}"#,
            Some(3),
        );
        let config = RequestQualityMonitorConfig {
            output_tokens_threshold_min: Some(10),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_skips_ambiguous_multi_choice_usage_for_text_metrics() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha beta gamma delta epsilon zeta"}},{"index":1,"delta":{"content":"loop loop"}}],"usage":{"completion_tokens":8}}"#,
            Some(8),
        );
        let config = RequestQualityMonitorConfig {
            collect_quality_metrics: true,
            collect_quality_metrics_min_tokens: 4,
            output_compression_threshold_max: Some(0.8),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert!(!result.evaluated);
        assert_eq!(result.threshold_match_reason, None);
    }

    #[test]
    fn recorder_aggregates_logprobs_from_all_choices() {
        let mut recorder = RequestQualityRecorder::new();
        recorder.observe_sse_chunk(
            r#"{"choices":[{"index":0,"delta":{"content":"alpha"}},{"index":1,"delta":{"content":"beta"},"logprobs":{"content":[{"token":"beta","logprob":-7.5}]}}]}"#,
            Some(2),
        );
        let config = RequestQualityMonitorConfig {
            median_logprob_threshold_max: Some(-7.0),
            ..RequestQualityMonitorConfig::default()
        };

        let (_metrics, result) = recorder.evaluate(&config);

        assert_eq!(result.threshold_match_reason, Some("median_logprob"));
    }

    #[test]
    fn median_logprob_uses_upper_middle_value_for_even_sample_count() {
        assert_eq!(median_logprob(&[-8.0, -6.0, -7.0, -5.0]), Some(-6.0));
    }
}
