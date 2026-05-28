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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, ensure};
use bytes::{Buf, Bytes};
use futures::future;
use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, StatusCode, Uri};
use quinn::{ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rustls::pki_types::CertificateDer;
use serde::{Deserialize, Serialize};
use stargate_protocol::TunnelTransportProtocol;
use tokio::sync::{Semaphore, oneshot};

use crate::statistics::{
    DistributionStats, NoiseClassification, classify_noise, summarize_distribution,
    upper_nearest_rank_index,
};

const SERVER_NAME: &str = "localhost";
const SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const WEBTRANSPORT_TUNNEL_PATH: &str = "/_stargate/webtransport";
const TRANSPORT_ORDER_SEED: u64 = 0x051A_76A7_E135;

type H3ClientBidiStream = <h3_quinn::OpenStreams as h3::quic::OpenStreams<Bytes>>::BidiStream;
type H3ClientRequestStream = h3::client::RequestStream<H3ClientBidiStream, Bytes>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportBenchConfig {
    pub request_count: usize,
    pub concurrency: usize,
    pub quic_connections: usize,
    pub warmup_requests: usize,
    pub request_body_bytes: usize,
    pub response_body_bytes: usize,
    pub request_chunk_bytes: usize,
    pub response_chunk_bytes: usize,
    pub quic_send_fairness: bool,
    pub http3_send_grease: bool,
    pub trials: usize,
    pub warmup_trials: usize,
    pub cooldown_ms: u64,
    pub randomize_order: bool,
    pub noise_threshold_cv: f64,
    pub min_effect_size_percent: f64,
}

impl TransportBenchConfig {
    fn validate(&self) -> Result<()> {
        ensure!(self.request_count > 0, "requests must be > 0");
        ensure!(self.concurrency > 0, "concurrency must be > 0");
        ensure!(self.quic_connections > 0, "quic-connections must be > 0");
        ensure!(self.trials > 0, "trials must be > 0");
        ensure!(
            self.request_chunk_bytes > 0,
            "request-chunk-bytes must be > 0"
        );
        ensure!(
            self.response_chunk_bytes > 0,
            "response-chunk-bytes must be > 0"
        );
        ensure!(
            self.noise_threshold_cv.is_finite() && self.noise_threshold_cv >= 0.0,
            "noise-threshold-cv must be finite and >= 0"
        );
        ensure!(
            self.min_effect_size_percent.is_finite() && self.min_effect_size_percent >= 0.0,
            "min-effect-size-percent must be finite and >= 0"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportBenchmarkOutcome {
    pub config: TransportBenchConfig,
    pub runs: Vec<TransportRunOutcome>,
    pub warmup_runs: Vec<TransportRunOutcome>,
    pub aggregates: Vec<TransportAggregateSummary>,
    pub comparisons: Vec<TransportComparisonSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportRunOutcome {
    pub transport: TransportKind,
    pub trial_index: usize,
    pub summary: TransportRunSummary,
    pub samples: Vec<RequestSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportAggregateSummary {
    pub transport: TransportKind,
    pub trial_count: usize,
    pub classification: NoiseClassification,
    pub throughput_rps: DistributionStats,
    pub goodput_mib_s: DistributionStats,
    pub latency_p95_us: DistributionStats,
    pub response_headers_p95_us: DistributionStats,
    pub first_body_p95_us: DistributionStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportComparisonSummary {
    pub baseline: TransportKind,
    pub candidate: TransportKind,
    pub throughput_delta_percent: Option<f64>,
    pub min_effect_size_percent: f64,
    pub confidence_intervals_overlap: Option<bool>,
    pub meaningful_difference: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    CustomProtocol,
    Http3H3Quinn,
    WebTransportH3Quinn,
}

impl TransportKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::CustomProtocol => "custom-protocol",
            Self::Http3H3Quinn => "http3-h3-quinn",
            Self::WebTransportH3Quinn => "webtransport-h3-quinn",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TransportRunSummary {
    pub transport: TransportKind,
    pub request_count: usize,
    pub success_count: usize,
    pub failure_count: usize,
    pub measured_duration_ms: u64,
    pub throughput_rps: f64,
    pub goodput_mib_s: f64,
    pub latency_us: LatencySummary,
    pub response_headers_us: LatencySummary,
    pub first_body_us: LatencySummary,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct LatencySummary {
    pub min: Option<u64>,
    pub p50: Option<u64>,
    pub p90: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RequestSample {
    pub request_index: usize,
    pub connection_index: usize,
    pub ok: bool,
    pub response_status: Option<u16>,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub response_headers_us: Option<u64>,
    pub first_body_us: Option<u64>,
    pub completion_us: u64,
    pub error: Option<String>,
}

#[derive(Serialize)]
struct RequestSampleRecord<'a> {
    transport: TransportKind,
    trial_index: usize,
    #[serde(flatten)]
    sample: &'a RequestSample,
}

#[derive(Clone)]
struct PayloadShape {
    request_chunks: Arc<Vec<Bytes>>,
    response_chunks: Arc<Vec<Bytes>>,
    request_bytes: usize,
    response_bytes: usize,
}

struct RunningServer {
    addr: SocketAddr,
    cert_pem: Vec<u8>,
    shutdown_tx: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<()>>,
}

struct H3BenchmarkClient {
    endpoint: Endpoint,
    connection: quinn::Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    driver_task: tokio::task::JoinHandle<Result<()>>,
}

#[derive(Clone)]
struct QuicRequestConnection {
    connection_index: usize,
    connection: quinn::Connection,
}

#[derive(Clone)]
struct WebTransportRequestConnection {
    connection_index: usize,
    connection: quinn::Connection,
    bidi_header: Bytes,
}

struct WebTransportBenchmarkClient {
    endpoint: Endpoint,
    connection: quinn::Connection,
    send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    connect_stream: H3ClientRequestStream,
    bidi_header: Bytes,
    driver_task: tokio::task::JoinHandle<Result<()>>,
}

impl RunningServer {
    async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        let mut task = self.task;
        match tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, &mut task).await {
            Ok(join_result) => join_result.context("transport benchmark server task panicked")?,
            Err(_) => {
                task.abort();
                Err(anyhow!("transport benchmark server did not stop"))
            }
        }
    }
}

pub async fn run_transport_benchmark(
    config: TransportBenchConfig,
) -> Result<TransportBenchmarkOutcome> {
    config.validate()?;
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let shape = PayloadShape {
        request_chunks: Arc::new(chunks(
            config.request_body_bytes,
            config.request_chunk_bytes,
            b'r',
        )),
        response_chunks: Arc::new(chunks(
            config.response_body_bytes,
            config.response_chunk_bytes,
            b's',
        )),
        request_bytes: config.request_body_bytes,
        response_bytes: config.response_body_bytes,
    };

    let warmup_runs = run_transport_trials(&shape, config, true).await?;
    let runs = run_transport_trials(&shape, config, false).await?;
    let aggregates = summarize_aggregates(&runs, config.noise_threshold_cv);
    let comparisons = summarize_comparisons(&aggregates, config.min_effect_size_percent);

    Ok(TransportBenchmarkOutcome {
        config,
        runs,
        warmup_runs,
        aggregates,
        comparisons,
    })
}

pub fn render_transport_benchmark_report(outcome: &TransportBenchmarkOutcome) -> String {
    let mut out = String::new();
    out.push_str("# Transport Benchmark\n\n");
    out.push_str(&format!(
        "- Requests: `{}`\n- Concurrency: `{}`\n- QUIC connections: `{}`\n- Warmup requests: `{}`\n- Trials: `{}`\n- Warmup trials: `{}`\n- Cooldown: `{} ms`\n- Randomized order: `{}`\n- Noise threshold CV: `{:.4}`\n- Min effect size: `{:.2}%`\n- Request bytes: `{}`\n- Response bytes: `{}`\n- QUIC send fairness: `{}`\n- HTTP/3 grease: `{}`\n\n",
        outcome.config.request_count,
        outcome.config.concurrency,
        outcome.config.quic_connections,
        outcome.config.warmup_requests,
        outcome.config.trials,
        outcome.config.warmup_trials,
        outcome.config.cooldown_ms,
        outcome.config.randomize_order,
        outcome.config.noise_threshold_cv,
        outcome.config.min_effect_size_percent,
        outcome.config.request_body_bytes,
        outcome.config.response_body_bytes,
        outcome.config.quic_send_fairness,
        outcome.config.http3_send_grease,
    ));

    if !outcome.aggregates.is_empty() {
        out.push_str("## Aggregate\n\n");
        out.push_str("| Transport | Trials | Classification | Throughput Mean | Throughput 95% CI | Throughput CV | P95 Latency Median | Headers P95 Median | First Body P95 Median |\n");
        out.push_str("|---|---:|---|---:|---:|---:|---:|---:|---:|\n");
        for aggregate in &outcome.aggregates {
            out.push_str(&format!(
                "| {} | {} | {:?} | {} | {} | {} | {} | {} | {} |\n",
                aggregate.transport.label(),
                aggregate.trial_count,
                aggregate.classification,
                optional_float(aggregate.throughput_rps.mean, " req/s"),
                optional_ci(&aggregate.throughput_rps.mean_ci_95, " req/s"),
                optional_cv(aggregate.throughput_rps.coefficient_of_variation),
                optional_float(aggregate.latency_p95_us.median, " us"),
                optional_float(aggregate.response_headers_p95_us.median, " us"),
                optional_float(aggregate.first_body_p95_us.median, " us"),
            ));
        }
        out.push('\n');
    }

    if !outcome.comparisons.is_empty() {
        out.push_str("## Comparisons\n\n");
        out.push_str(
            "| Baseline | Candidate | Throughput Delta | CI Overlap | Meaningful Difference |\n",
        );
        out.push_str("|---|---|---:|---|---|\n");
        for comparison in &outcome.comparisons {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                comparison.baseline.label(),
                comparison.candidate.label(),
                optional_percent(comparison.throughput_delta_percent),
                optional_bool(comparison.confidence_intervals_overlap),
                comparison.meaningful_difference,
            ));
        }
        out.push('\n');
    }

    out.push_str("## Measured Trials\n\n");
    out.push_str("| Trial | Transport | Success | Throughput | Goodput | P50 | P95 | P99 | Max | Headers P50 | Headers P95 | Headers P99 | First Body P50 | First Body P95 | First Body P99 |\n");
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for run in &outcome.runs {
        let summary = &run.summary;
        out.push_str(&format!(
            "| {} | {} | {}/{} | {:.1} req/s | {:.2} MiB/s | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            run.trial_index,
            summary.transport.label(),
            summary.success_count,
            summary.request_count,
            summary.throughput_rps,
            summary.goodput_mib_s,
            optional_us(summary.latency_us.p50),
            optional_us(summary.latency_us.p95),
            optional_us(summary.latency_us.p99),
            optional_us(summary.latency_us.max),
            optional_us(summary.response_headers_us.p50),
            optional_us(summary.response_headers_us.p95),
            optional_us(summary.response_headers_us.p99),
            optional_us(summary.first_body_us.p50),
            optional_us(summary.first_body_us.p95),
            optional_us(summary.first_body_us.p99),
        ));
    }
    out
}

pub fn write_transport_benchmark_artifacts(
    output_dir: &Path,
    outcome: &TransportBenchmarkOutcome,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    let summaries = outcome
        .runs
        .iter()
        .map(|run| run.summary.clone())
        .collect::<Vec<_>>();
    let summary_json = serde_json::json!({
        "config": outcome.config,
        "summaries": summaries,
        "aggregates": outcome.aggregates,
        "comparisons": outcome.comparisons,
        "warmup_run_count": outcome.warmup_runs.len(),
    });
    let summary_path = output_dir.join("transport-summary.json");
    std::fs::write(
        &summary_path,
        serde_json::to_vec_pretty(&summary_json).context("serialize transport summary")?,
    )
    .with_context(|| format!("failed to write {}", summary_path.display()))?;

    let report_path = output_dir.join("transport-report.md");
    std::fs::write(&report_path, render_transport_benchmark_report(outcome))
        .with_context(|| format!("failed to write {}", report_path.display()))?;

    let multiple_trials = [
        TransportKind::CustomProtocol,
        TransportKind::Http3H3Quinn,
        TransportKind::WebTransportH3Quinn,
    ]
    .iter()
    .any(|transport| {
        outcome
            .runs
            .iter()
            .filter(|run| run.transport == *transport)
            .count()
            > 1
    });
    for run in &outcome.runs {
        let samples_path = if multiple_trials {
            output_dir.join(format!(
                "transport-samples-{}-trial-{}.jsonl",
                run.transport.label(),
                run.trial_index
            ))
        } else {
            output_dir.join(format!("transport-samples-{}.jsonl", run.transport.label()))
        };
        let mut out = String::new();
        for sample in &run.samples {
            let record = RequestSampleRecord {
                transport: run.transport,
                trial_index: run.trial_index,
                sample,
            };
            out.push_str(&serde_json::to_string(&record).context("serialize transport sample")?);
            out.push('\n');
        }
        std::fs::write(&samples_path, out)
            .with_context(|| format!("failed to write {}", samples_path.display()))?;
    }

    Ok(())
}

async fn run_transport_trials(
    shape: &PayloadShape,
    config: TransportBenchConfig,
    warmup: bool,
) -> Result<Vec<TransportRunOutcome>> {
    let trial_count = if warmup {
        config.warmup_trials
    } else {
        config.trials
    };
    let run_capacity = trial_count
        .checked_mul(3)
        .context("transport trial count overflows run capacity")?;
    let mut runs = Vec::with_capacity(run_capacity);
    for trial_index in 0..trial_count {
        for transport in transport_order(config, trial_index, warmup) {
            let run = run_transport(transport, trial_index + 1, config, shape.clone()).await?;
            runs.push(run);
            if config.cooldown_ms > 0 {
                tokio::time::sleep(Duration::from_millis(config.cooldown_ms)).await;
            }
        }
    }
    Ok(runs)
}

fn transport_order(
    config: TransportBenchConfig,
    trial_index: usize,
    warmup: bool,
) -> [TransportKind; 3] {
    let mut order = [
        TransportKind::CustomProtocol,
        TransportKind::Http3H3Quinn,
        TransportKind::WebTransportH3Quinn,
    ];
    if config.randomize_order {
        let warmup_salt = if warmup { 0x000A_11CE_u64 } else { 0 };
        let mut rng =
            StdRng::seed_from_u64(TRANSPORT_ORDER_SEED ^ trial_index as u64 ^ warmup_salt);
        let first = rng.random_range(0..order.len());
        order.swap(0, first);
        let second = rng.random_range(1..order.len());
        order.swap(1, second);
    }
    order
}

fn summarize_aggregates(
    runs: &[TransportRunOutcome],
    noise_threshold_cv: f64,
) -> Vec<TransportAggregateSummary> {
    [
        TransportKind::CustomProtocol,
        TransportKind::Http3H3Quinn,
        TransportKind::WebTransportH3Quinn,
    ]
    .into_iter()
    .filter_map(|transport| {
        let transport_runs = runs
            .iter()
            .filter(|run| run.transport == transport)
            .collect::<Vec<_>>();
        if transport_runs.is_empty() {
            return None;
        }
        let seed = match transport {
            TransportKind::CustomProtocol => 11,
            TransportKind::Http3H3Quinn => 17,
            TransportKind::WebTransportH3Quinn => 23,
        };
        let throughput_rps = summarize_distribution(
            &transport_runs
                .iter()
                .map(|run| run.summary.throughput_rps)
                .collect::<Vec<_>>(),
            seed,
        );
        let classification = classify_noise(&throughput_rps, noise_threshold_cv);
        Some(TransportAggregateSummary {
            transport,
            trial_count: transport_runs.len(),
            classification,
            throughput_rps,
            goodput_mib_s: summarize_distribution(
                &transport_runs
                    .iter()
                    .map(|run| run.summary.goodput_mib_s)
                    .collect::<Vec<_>>(),
                seed + 1,
            ),
            latency_p95_us: summarize_distribution(
                &transport_runs
                    .iter()
                    .filter_map(|run| run.summary.latency_us.p95.map(|value| value as f64))
                    .collect::<Vec<_>>(),
                seed + 2,
            ),
            response_headers_p95_us: summarize_distribution(
                &transport_runs
                    .iter()
                    .filter_map(|run| {
                        run.summary
                            .response_headers_us
                            .p95
                            .map(|value| value as f64)
                    })
                    .collect::<Vec<_>>(),
                seed + 3,
            ),
            first_body_p95_us: summarize_distribution(
                &transport_runs
                    .iter()
                    .filter_map(|run| run.summary.first_body_us.p95.map(|value| value as f64))
                    .collect::<Vec<_>>(),
                seed + 4,
            ),
        })
    })
    .collect()
}

fn summarize_comparisons(
    aggregates: &[TransportAggregateSummary],
    min_effect_size_percent: f64,
) -> Vec<TransportComparisonSummary> {
    let baseline = aggregates
        .iter()
        .find(|aggregate| aggregate.transport == TransportKind::CustomProtocol);
    let Some(baseline) = baseline else {
        return Vec::new();
    };

    aggregates
        .iter()
        .filter(|candidate| candidate.transport != TransportKind::CustomProtocol)
        .map(|candidate| {
            let throughput_delta_percent =
                match (baseline.throughput_rps.mean, candidate.throughput_rps.mean) {
                    (Some(baseline), Some(candidate)) if baseline.abs() > f64::EPSILON => {
                        Some((candidate - baseline) / baseline * 100.0)
                    }
                    _ => None,
                };
            let confidence_intervals_overlap = match (
                &baseline.throughput_rps.mean_ci_95,
                &candidate.throughput_rps.mean_ci_95,
            ) {
                (Some(left), Some(right)) => {
                    Some(left.lower <= right.upper && right.lower <= left.upper)
                }
                _ => None,
            };
            let classifications_support_comparison = baseline.classification
                == NoiseClassification::Reliable
                && candidate.classification == NoiseClassification::Reliable;
            let meaningful_difference = throughput_delta_percent.is_some_and(|delta| {
                classifications_support_comparison
                    && baseline.trial_count >= 2
                    && candidate.trial_count >= 2
                    && confidence_intervals_overlap == Some(false)
                    && delta.abs() >= min_effect_size_percent
            });

            TransportComparisonSummary {
                baseline: TransportKind::CustomProtocol,
                candidate: candidate.transport,
                throughput_delta_percent,
                min_effect_size_percent,
                confidence_intervals_overlap,
                meaningful_difference,
            }
        })
        .collect()
}

async fn run_transport(
    transport: TransportKind,
    trial_index: usize,
    config: TransportBenchConfig,
    shape: PayloadShape,
) -> Result<TransportRunOutcome> {
    match transport {
        TransportKind::CustomProtocol => run_custom_protocol(config, shape, trial_index).await,
        TransportKind::Http3H3Quinn => run_http3_h3_quinn(config, shape, trial_index).await,
        TransportKind::WebTransportH3Quinn => {
            run_webtransport_h3_quinn(config, shape, trial_index).await
        }
    }
}

async fn run_custom_protocol(
    config: TransportBenchConfig,
    shape: PayloadShape,
    trial_index: usize,
) -> Result<TransportRunOutcome> {
    let server = start_custom_server(config, shape.response_chunks.clone()).await?;
    let clients = connect_quic_set(
        config,
        server.addr,
        TunnelTransportProtocol::Custom.alpn_protocols(),
        &server.cert_pem,
    )
    .await?;
    let request_connections = Arc::new(
        clients
            .iter()
            .enumerate()
            .map(
                |(connection_index, (_endpoint, connection))| QuicRequestConnection {
                    connection_index,
                    connection: connection.clone(),
                },
            )
            .collect::<Vec<_>>(),
    );

    if config.warmup_requests > 0 {
        let _ = drive_custom_requests(
            request_connections.clone(),
            shape.clone(),
            config.warmup_requests,
            config.concurrency,
        )
        .await?;
    }

    let started_at = Instant::now();
    let samples = drive_custom_requests(
        request_connections,
        shape.clone(),
        config.request_count,
        config.concurrency,
    )
    .await?;
    let measured_duration = started_at.elapsed();

    close_quic_clients(clients).await;
    server.shutdown().await?;

    let summary = summarize_samples(TransportKind::CustomProtocol, &samples, measured_duration);
    Ok(TransportRunOutcome {
        transport: TransportKind::CustomProtocol,
        trial_index,
        summary,
        samples,
    })
}

async fn run_http3_h3_quinn(
    config: TransportBenchConfig,
    shape: PayloadShape,
    trial_index: usize,
) -> Result<TransportRunOutcome> {
    let server = start_h3_server(config, shape.response_chunks.clone()).await?;
    let clients = connect_h3_clients(config, server.addr, &server.cert_pem).await?;
    let send_requests = Arc::new(
        clients
            .iter()
            .enumerate()
            .map(|(connection_index, client)| (connection_index, client.send_request.clone()))
            .collect::<Vec<_>>(),
    );

    if config.warmup_requests > 0 {
        let _ = drive_h3_requests(
            send_requests.clone(),
            server.addr,
            shape.clone(),
            config.warmup_requests,
            config.concurrency,
        )
        .await?;
    }

    let started_at = Instant::now();
    let samples = drive_h3_requests(
        send_requests,
        server.addr,
        shape.clone(),
        config.request_count,
        config.concurrency,
    )
    .await?;
    let measured_duration = started_at.elapsed();

    close_h3_clients(clients).await;
    server.shutdown().await?;

    let summary = summarize_samples(TransportKind::Http3H3Quinn, &samples, measured_duration);
    Ok(TransportRunOutcome {
        transport: TransportKind::Http3H3Quinn,
        trial_index,
        summary,
        samples,
    })
}

async fn run_webtransport_h3_quinn(
    config: TransportBenchConfig,
    shape: PayloadShape,
    trial_index: usize,
) -> Result<TransportRunOutcome> {
    let server = start_webtransport_server(config, shape.response_chunks.clone()).await?;
    let clients = connect_webtransport_clients(config, server.addr, &server.cert_pem).await?;
    let request_connections = Arc::new(
        clients
            .iter()
            .enumerate()
            .map(|(connection_index, client)| WebTransportRequestConnection {
                connection_index,
                connection: client.connection.clone(),
                bidi_header: client.bidi_header.clone(),
            })
            .collect::<Vec<_>>(),
    );

    if config.warmup_requests > 0 {
        let _ = drive_webtransport_requests(
            request_connections.clone(),
            shape.clone(),
            config.warmup_requests,
            config.concurrency,
        )
        .await?;
    }

    let started_at = Instant::now();
    let samples = drive_webtransport_requests(
        request_connections,
        shape.clone(),
        config.request_count,
        config.concurrency,
    )
    .await?;
    let measured_duration = started_at.elapsed();

    close_webtransport_clients(clients).await;
    server.shutdown().await?;

    let summary = summarize_samples(
        TransportKind::WebTransportH3Quinn,
        &samples,
        measured_duration,
    );
    Ok(TransportRunOutcome {
        transport: TransportKind::WebTransportH3Quinn,
        trial_index,
        summary,
        samples,
    })
}

async fn start_custom_server(
    config: TransportBenchConfig,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<RunningServer> {
    let generated = server_config(config, TunnelTransportProtocol::Custom.alpn_protocols())?;
    let endpoint = Endpoint::server(generated.server_config, "127.0.0.1:0".parse()?)
        .context("bind custom protocol QUIC server")?;
    let addr = endpoint.local_addr().context("read custom server addr")?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let response_chunks = response_chunks.clone();
                    tokio::spawn(async move {
                        if let Ok(connection) = incoming.await {
                            let _ = handle_custom_connection(connection, response_chunks).await;
                        }
                    });
                }
            }
        }
        endpoint.close(0_u32.into(), b"benchmark shutdown");
        endpoint.wait_idle().await;
        Ok(())
    });

    Ok(RunningServer {
        addr,
        cert_pem: generated.cert_pem,
        shutdown_tx,
        task,
    })
}

async fn handle_custom_connection(
    connection: quinn::Connection,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    while let Ok((quinn_send, quinn_recv)) = connection.accept_bi().await {
        let response_chunks = response_chunks.clone();
        tokio::spawn(async move {
            let _ = handle_custom_stream(quinn_send, quinn_recv, response_chunks).await;
        });
    }
    Ok(())
}

async fn handle_custom_stream(
    quinn_send: quinn::SendStream,
    quinn_recv: quinn::RecvStream,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let mut recv = stargate_protocol::RecvStream::new(quinn_recv);
    let mut send = stargate_protocol::SendStream::new(quinn_send);
    let _request_headers = recv
        .recv_header()
        .await
        .context("read custom request headers")?;
    while recv.recv_body().await?.is_some() {}

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("x-status"),
        HeaderValue::from_static("200"),
    );
    response_headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    send.send_header(response_headers)
        .await
        .context("send custom response headers")?;
    for chunk in response_chunks.iter() {
        send.send_body(chunk.clone())
            .await
            .context("send custom response body")?;
    }
    send.finish().context("finish custom response")?;
    Ok(())
}

async fn start_h3_server(
    config: TransportBenchConfig,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<RunningServer> {
    let generated = server_config(config, TunnelTransportProtocol::Http3.alpn_protocols())?;
    let endpoint = Endpoint::server(generated.server_config, "127.0.0.1:0".parse()?)
        .context("bind h3 QUIC server")?;
    let addr = endpoint.local_addr().context("read h3 server addr")?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let response_chunks = response_chunks.clone();
                    let config = config;
                    tokio::spawn(async move {
                        if let Ok(connection) = incoming.await {
                            let _ = handle_h3_connection(connection, config, response_chunks).await;
                        }
                    });
                }
            }
        }
        endpoint.close(0_u32.into(), b"benchmark shutdown");
        endpoint.wait_idle().await;
        Ok(())
    });

    Ok(RunningServer {
        addr,
        cert_pem: generated.cert_pem,
        shutdown_tx,
        task,
    })
}

async fn start_webtransport_server(
    config: TransportBenchConfig,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<RunningServer> {
    let generated = server_config(
        config,
        TunnelTransportProtocol::WebTransport.alpn_protocols(),
    )?;
    let endpoint = Endpoint::server(generated.server_config, "127.0.0.1:0".parse()?)
        .context("bind WebTransport QUIC server")?;
    let addr = endpoint
        .local_addr()
        .context("read WebTransport server addr")?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let response_chunks = response_chunks.clone();
                    let config = config;
                    tokio::spawn(async move {
                        if let Ok(connection) = incoming.await {
                            let _ = handle_webtransport_connection(connection, config, response_chunks).await;
                        }
                    });
                }
            }
        }
        endpoint.close(0_u32.into(), b"benchmark shutdown");
        endpoint.wait_idle().await;
        Ok(())
    });

    Ok(RunningServer {
        addr,
        cert_pem: generated.cert_pem,
        shutdown_tx,
        task,
    })
}

async fn handle_webtransport_connection(
    connection: quinn::Connection,
    config: TransportBenchConfig,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let mut builder = h3::server::builder();
    builder
        .send_grease(config.http3_send_grease)
        .enable_webtransport(true)
        .enable_extended_connect(true)
        .enable_datagram(true)
        .max_webtransport_sessions(1);
    let mut h3_connection: h3::server::Connection<h3_quinn::Connection, Bytes> = builder
        .build(h3_quinn::Connection::new(connection.clone()))
        .await
        .map_err(|error| anyhow!("create WebTransport h3 server connection: {error:?}"))?;
    let Some(resolver) = h3_connection
        .accept()
        .await
        .map_err(|error| anyhow!("accept WebTransport CONNECT: {error:?}"))?
    else {
        return Ok(());
    };
    let (request, mut connect_stream) = resolver
        .resolve_request()
        .await
        .map_err(|error| anyhow!("resolve WebTransport CONNECT: {error:?}"))?;
    let is_webtransport = request
        .extensions()
        .get::<h3::ext::Protocol>()
        .is_some_and(|protocol| *protocol == h3::ext::Protocol::WEB_TRANSPORT);
    ensure!(
        request.method() == Method::CONNECT
            && request.uri().path() == WEBTRANSPORT_TUNNEL_PATH
            && is_webtransport,
        "invalid WebTransport CONNECT request"
    );
    let session_id = connect_stream.id().into_inner();
    let response = Response::builder()
        .status(StatusCode::OK)
        .body(())
        .context("build WebTransport CONNECT response")?;
    connect_stream
        .send_response(response)
        .await
        .map_err(|error| anyhow!("send WebTransport CONNECT response: {error:?}"))?;

    while let Ok((quinn_send, quinn_recv)) = connection.accept_bi().await {
        let response_chunks = response_chunks.clone();
        tokio::spawn(async move {
            let _ = handle_webtransport_benchmark_stream(
                quinn_send,
                quinn_recv,
                session_id,
                response_chunks,
            )
            .await;
        });
    }
    Ok(())
}

async fn handle_webtransport_benchmark_stream(
    quinn_send: quinn::SendStream,
    mut quinn_recv: quinn::RecvStream,
    session_id: u64,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let stream_session_id = stargate_protocol::read_webtransport_bidi_header(&mut quinn_recv)
        .await
        .context("read WebTransport stream header")?;
    ensure!(
        stream_session_id == session_id,
        "WebTransport stream session id mismatch: got {stream_session_id}, expected {session_id}"
    );
    handle_webtransport_http_benchmark_stream(quinn_send, quinn_recv, response_chunks).await
}

async fn handle_webtransport_http_benchmark_stream(
    mut quinn_send: quinn::SendStream,
    mut quinn_recv: quinn::RecvStream,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let _request_head = stargate_protocol::read_webtransport_http_request_head(&mut quinn_recv)
        .await
        .context("read WebTransport benchmark request head")?;
    while stargate_protocol::read_webtransport_http_body_chunk(&mut quinn_recv)
        .await
        .context("read WebTransport benchmark request body")?
        .is_some()
    {}

    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let response_head = stargate_protocol::WebTransportHttpResponseHead {
        status: StatusCode::OK,
        headers: response_headers,
    };
    stargate_protocol::write_webtransport_http_response_head(&mut quinn_send, &response_head)
        .await
        .context("send WebTransport benchmark response head")?;
    for chunk in response_chunks.iter() {
        stargate_protocol::write_webtransport_http_body(&mut quinn_send, chunk.clone())
            .await
            .context("send WebTransport benchmark response body")?;
    }
    stargate_protocol::finish_webtransport_http_stream(&mut quinn_send)
        .context("finish WebTransport benchmark response")?;
    Ok(())
}

async fn handle_h3_connection(
    connection: quinn::Connection,
    config: TransportBenchConfig,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let mut h3_connection = h3::server::builder()
        .send_grease(config.http3_send_grease)
        .build(h3_quinn::Connection::new(connection))
        .await
        .map_err(|error| anyhow!("create h3 server connection: {error:?}"))?;
    loop {
        match h3_connection.accept().await {
            Ok(Some(resolver)) => {
                let response_chunks = response_chunks.clone();
                tokio::spawn(async move {
                    let _ = handle_h3_request(resolver, response_chunks).await;
                });
            }
            Ok(None) => break,
            Err(error) if error.is_h3_no_error() => break,
            Err(error) => return Err(anyhow!("h3 accept failed: {error:?}")),
        }
    }
    Ok(())
}

async fn handle_h3_request(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
    response_chunks: Arc<Vec<Bytes>>,
) -> Result<()> {
    let (_request, mut stream) = resolver
        .resolve_request()
        .await
        .map_err(|error| anyhow!("resolve h3 request: {error:?}"))?;
    while let Some(chunk) = stream
        .recv_data()
        .await
        .map_err(|error| anyhow!("read h3 request body: {error:?}"))?
    {
        let _ = chunk.remaining();
    }

    let response = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .body(())
        .context("build h3 response")?;
    stream
        .send_response(response)
        .await
        .map_err(|error| anyhow!("send h3 response headers: {error:?}"))?;
    for chunk in response_chunks.iter() {
        stream
            .send_data(chunk.clone())
            .await
            .map_err(|error| anyhow!("send h3 response body: {error:?}"))?;
    }
    stream
        .finish()
        .await
        .map_err(|error| anyhow!("finish h3 response: {error:?}"))?;
    Ok(())
}

async fn connect_quic(endpoint: &Endpoint, addr: SocketAddr) -> Result<quinn::Connection> {
    endpoint
        .connect(addr, SERVER_NAME)
        .context("start QUIC connection")?
        .await
        .context("complete QUIC connection")
}

fn client_endpoint(
    config: TransportBenchConfig,
    alpn_protocols: Vec<Vec<u8>>,
    server_cert_pem: &[u8],
) -> Result<Endpoint> {
    let mut endpoint =
        Endpoint::client("127.0.0.1:0".parse()?).context("bind QUIC client endpoint")?;
    endpoint.set_default_client_config(client_config(config, alpn_protocols, server_cert_pem)?);
    Ok(endpoint)
}

async fn connect_quic_set(
    config: TransportBenchConfig,
    addr: SocketAddr,
    alpn_protocols: Vec<Vec<u8>>,
    server_cert_pem: &[u8],
) -> Result<Vec<(Endpoint, quinn::Connection)>> {
    let endpoint = client_endpoint(config, alpn_protocols, server_cert_pem)?;
    let mut clients = Vec::with_capacity(config.quic_connections);
    for _ in 0..config.quic_connections {
        clients.push((endpoint.clone(), connect_quic(&endpoint, addr).await?));
    }
    Ok(clients)
}

async fn close_quic_clients(clients: Vec<(Endpoint, quinn::Connection)>) {
    let endpoint = clients.first().map(|(endpoint, _)| endpoint.clone());
    for (_, connection) in clients {
        connection.close(0_u32.into(), b"benchmark complete");
    }
    if let Some(endpoint) = endpoint {
        endpoint.wait_idle().await;
    }
}

async fn connect_h3_clients(
    config: TransportBenchConfig,
    addr: SocketAddr,
    server_cert_pem: &[u8],
) -> Result<Vec<H3BenchmarkClient>> {
    let endpoint = client_endpoint(
        config,
        TunnelTransportProtocol::Http3.alpn_protocols(),
        server_cert_pem,
    )?;
    let mut clients = Vec::with_capacity(config.quic_connections);
    for _ in 0..config.quic_connections {
        let connection = connect_quic(&endpoint, addr).await?;
        let quinn_connection = h3_quinn::Connection::new(connection.clone());
        let (mut driver, send_request) = h3::client::builder()
            .send_grease(config.http3_send_grease)
            .build(quinn_connection)
            .await
            .map_err(|error| anyhow!("create h3 client: {error:?}"))?;
        let driver_task = tokio::spawn(async move {
            let error = future::poll_fn(|cx| driver.poll_close(cx)).await;
            if error.is_h3_no_error() {
                Ok(())
            } else {
                Err(anyhow!("h3 client connection closed: {error:?}"))
            }
        });
        clients.push(H3BenchmarkClient {
            endpoint: endpoint.clone(),
            connection,
            send_request,
            driver_task,
        });
    }
    Ok(clients)
}

async fn close_h3_clients(clients: Vec<H3BenchmarkClient>) {
    let endpoint = clients.first().map(|client| client.endpoint.clone());
    for mut client in clients {
        // Drop the final request sender before closing QUIC so the H3 driver can drain shutdown.
        drop(client.send_request);
        client.connection.close(0_u32.into(), b"benchmark complete");
        if tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, &mut client.driver_task)
            .await
            .is_err()
        {
            client.driver_task.abort();
        }
    }
    if let Some(endpoint) = endpoint {
        endpoint.wait_idle().await;
    }
}

async fn connect_webtransport_clients(
    config: TransportBenchConfig,
    addr: SocketAddr,
    server_cert_pem: &[u8],
) -> Result<Vec<WebTransportBenchmarkClient>> {
    let endpoint = client_endpoint(
        config,
        TunnelTransportProtocol::WebTransport.alpn_protocols(),
        server_cert_pem,
    )?;
    let mut clients = Vec::with_capacity(config.quic_connections);
    for _ in 0..config.quic_connections {
        let connection = connect_quic(&endpoint, addr).await?;
        let mut builder = h3::client::builder();
        builder
            .send_grease(config.http3_send_grease)
            .enable_extended_connect(true)
            .enable_datagram(true);
        let (mut driver, mut send_request): (
            h3::client::Connection<h3_quinn::Connection, Bytes>,
            h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
        ) = builder
            .build(h3_quinn::Connection::new(connection.clone()))
            .await
            .map_err(|error| anyhow!("create WebTransport h3 client: {error:?}"))?;
        let driver_task = tokio::spawn(async move {
            let error = future::poll_fn(|cx| driver.poll_close(cx)).await;
            if error.is_h3_no_error() {
                Ok(())
            } else {
                Err(anyhow!(
                    "WebTransport h3 client connection closed: {error:?}"
                ))
            }
        });

        let mut request = Request::builder()
            .method(Method::CONNECT)
            .uri(format!("https://{SERVER_NAME}{WEBTRANSPORT_TUNNEL_PATH}"))
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
            .map_err(|error| anyhow!("read WebTransport CONNECT response: {error:?}"))?;
        ensure!(
            response.status().is_success(),
            "WebTransport CONNECT rejected with status {}",
            response.status()
        );
        let bidi_header = stargate_protocol::WebTransportBidiHeader::new(session_id)
            .context("precompute WebTransport benchmark stream header")?
            .to_bytes();

        clients.push(WebTransportBenchmarkClient {
            endpoint: endpoint.clone(),
            connection,
            send_request,
            connect_stream,
            bidi_header,
            driver_task,
        });
    }
    Ok(clients)
}

async fn close_webtransport_clients(clients: Vec<WebTransportBenchmarkClient>) {
    let endpoint = clients.first().map(|client| client.endpoint.clone());
    for mut client in clients {
        // Drop the CONNECT stream and final request sender before closing QUIC so the H3 driver can drain shutdown.
        drop(client.connect_stream);
        drop(client.send_request);
        client.connection.close(0_u32.into(), b"benchmark complete");
        if tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, &mut client.driver_task)
            .await
            .is_err()
        {
            client.driver_task.abort();
        }
    }
    if let Some(endpoint) = endpoint {
        endpoint.wait_idle().await;
    }
}

async fn drive_custom_requests(
    connections: Arc<Vec<QuicRequestConnection>>,
    shape: PayloadShape,
    request_count: usize,
    concurrency: usize,
) -> Result<Vec<RequestSample>> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = Vec::with_capacity(request_count);
    for request_index in 0..request_count {
        let request_connection = connections[request_index % connections.len()].clone();
        let shape = shape.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore should remain open");
            execute_custom_request(request_connection, shape, request_index).await
        }));
    }
    collect_samples(tasks).await
}

async fn drive_webtransport_requests(
    connections: Arc<Vec<WebTransportRequestConnection>>,
    shape: PayloadShape,
    request_count: usize,
    concurrency: usize,
) -> Result<Vec<RequestSample>> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = Vec::with_capacity(request_count);
    for request_index in 0..request_count {
        let request_connection = connections[request_index % connections.len()].clone();
        let shape = shape.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore should remain open");
            execute_webtransport_request(request_connection, shape, request_index).await
        }));
    }
    collect_samples(tasks).await
}

async fn execute_webtransport_request(
    request_connection: WebTransportRequestConnection,
    shape: PayloadShape,
    request_index: usize,
) -> RequestSample {
    let started_at = Instant::now();
    let result = async {
        let (quinn_send, quinn_recv) = request_connection
            .connection
            .open_bi()
            .await
            .context("open WebTransport request stream")?;
        let mut quinn_send = quinn_send;
        let mut quinn_recv = quinn_recv;

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&format!("transport-bench-{request_index}"))
                .context("build request id")?,
        );
        headers.insert(
            HeaderName::from_static("x-model"),
            HeaderValue::from_static("transport-bench-model"),
        );
        headers.insert(
            HeaderName::from_static("x-input-tokens"),
            HeaderValue::from_static("1"),
        );
        let request_head = stargate_protocol::WebTransportHttpRequestHead {
            method: Method::POST,
            path_and_query: "/v1/chat/completions".to_string(),
            headers,
        };
        stargate_protocol::write_webtransport_http_request_head_after_prefix(
            &mut quinn_send,
            request_connection.bidi_header.clone(),
            &request_head,
        )
        .await
        .context("send WebTransport request head")?;
        for chunk in shape.request_chunks.iter() {
            stargate_protocol::write_webtransport_http_body(&mut quinn_send, chunk.clone())
                .await
                .context("send WebTransport request body")?;
        }
        stargate_protocol::finish_webtransport_http_stream(&mut quinn_send)
            .context("finish WebTransport request")?;

        let response_head =
            stargate_protocol::read_webtransport_http_response_head(&mut quinn_recv)
                .await
                .context("read WebTransport response head")?;
        let response_headers_us = duration_us(started_at.elapsed());
        let response_status = Some(response_head.status.as_u16());
        let mut first_body_us = None;
        let mut response_bytes = 0usize;
        while let Some(chunk) =
            stargate_protocol::read_webtransport_http_body_chunk(&mut quinn_recv)
                .await
                .context("read WebTransport response body")?
        {
            if first_body_us.is_none() {
                first_body_us = Some(duration_us(started_at.elapsed()));
            }
            response_bytes += chunk.len();
        }
        Ok::<_, anyhow::Error>((
            response_status,
            response_headers_us,
            first_body_us,
            response_bytes,
        ))
    }
    .await;

    match result {
        Ok((response_status, response_headers_us, first_body_us, response_bytes)) => {
            RequestSample {
                request_index,
                connection_index: request_connection.connection_index,
                ok: response_status == Some(200) && response_bytes == shape.response_bytes,
                response_status,
                request_bytes: shape.request_bytes,
                response_bytes,
                response_headers_us: Some(response_headers_us),
                first_body_us,
                completion_us: duration_us(started_at.elapsed()),
                error: None,
            }
        }
        Err(error) => RequestSample {
            request_index,
            connection_index: request_connection.connection_index,
            ok: false,
            response_status: None,
            request_bytes: shape.request_bytes,
            response_bytes: 0,
            response_headers_us: None,
            first_body_us: None,
            completion_us: duration_us(started_at.elapsed()),
            error: Some(error.to_string()),
        },
    }
}

async fn execute_custom_request(
    request_connection: QuicRequestConnection,
    shape: PayloadShape,
    request_index: usize,
) -> RequestSample {
    let started_at = Instant::now();
    let result = async {
        let (quinn_send, quinn_recv) = request_connection
            .connection
            .open_bi()
            .await
            .context("open custom request stream")?;
        let mut send = stargate_protocol::SendStream::new(quinn_send);
        let mut recv = stargate_protocol::RecvStream::new(quinn_recv);

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-method"),
            HeaderValue::from_static("POST"),
        );
        headers.insert(
            HeaderName::from_static("x-path"),
            HeaderValue::from_static("/v1/chat/completions"),
        );
        headers.insert(
            HeaderName::from_static("x-request-id"),
            HeaderValue::from_str(&format!("transport-bench-{request_index}"))
                .context("build request id")?,
        );
        headers.insert(
            HeaderName::from_static("x-model"),
            HeaderValue::from_static("transport-bench-model"),
        );
        headers.insert(
            HeaderName::from_static("x-input-tokens"),
            HeaderValue::from_static("1"),
        );
        send.send_header(headers)
            .await
            .context("send custom request headers")?;
        for chunk in shape.request_chunks.iter() {
            send.send_body(chunk.clone())
                .await
                .context("send custom request body")?;
        }
        send.finish().context("finish custom request")?;

        let response_headers = recv
            .recv_header()
            .await
            .context("read custom response headers")?;
        let response_headers_us = duration_us(started_at.elapsed());
        let response_status = response_headers
            .get("x-status")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u16>().ok());
        let mut first_body_us = None;
        let mut response_bytes = 0usize;
        while let Some(chunk) = recv
            .recv_body()
            .await
            .context("read custom response body")?
        {
            if first_body_us.is_none() {
                first_body_us = Some(duration_us(started_at.elapsed()));
            }
            response_bytes += chunk.len();
        }
        Ok::<_, anyhow::Error>((
            response_status,
            response_headers_us,
            first_body_us,
            response_bytes,
        ))
    }
    .await;

    match result {
        Ok((response_status, response_headers_us, first_body_us, response_bytes)) => {
            RequestSample {
                request_index,
                connection_index: request_connection.connection_index,
                ok: response_status == Some(200) && response_bytes == shape.response_bytes,
                response_status,
                request_bytes: shape.request_bytes,
                response_bytes,
                response_headers_us: Some(response_headers_us),
                first_body_us,
                completion_us: duration_us(started_at.elapsed()),
                error: None,
            }
        }
        Err(error) => RequestSample {
            request_index,
            connection_index: request_connection.connection_index,
            ok: false,
            response_status: None,
            request_bytes: shape.request_bytes,
            response_bytes: 0,
            response_headers_us: None,
            first_body_us: None,
            completion_us: duration_us(started_at.elapsed()),
            error: Some(error.to_string()),
        },
    }
}

async fn drive_h3_requests(
    send_requests: Arc<Vec<(usize, h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>)>>,
    addr: SocketAddr,
    shape: PayloadShape,
    request_count: usize,
    concurrency: usize,
) -> Result<Vec<RequestSample>> {
    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = Vec::with_capacity(request_count);
    for request_index in 0..request_count {
        let (connection_index, send_request) =
            send_requests[request_index % send_requests.len()].clone();
        let shape = shape.clone();
        let semaphore = semaphore.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore should remain open");
            execute_h3_request(send_request, addr, shape, request_index, connection_index).await
        }));
    }
    collect_samples(tasks).await
}

async fn execute_h3_request(
    mut send_request: h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>,
    addr: SocketAddr,
    shape: PayloadShape,
    request_index: usize,
    connection_index: usize,
) -> RequestSample {
    let started_at = Instant::now();
    let result = async {
        let uri: Uri = format!("https://{SERVER_NAME}:{}/v1/chat/completions", addr.port())
            .parse()
            .context("build h3 request URI")?;
        let request = Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header("x-request-id", format!("transport-bench-{request_index}"))
            .header("x-model", "transport-bench-model")
            .header("x-input-tokens", "1")
            .header(http::header::CONTENT_TYPE, "application/octet-stream")
            .body(())
            .context("build h3 request")?;
        let mut stream = send_request
            .send_request(request)
            .await
            .map_err(|error| anyhow!("send h3 request headers: {error:?}"))?;
        for chunk in shape.request_chunks.iter() {
            stream
                .send_data(chunk.clone())
                .await
                .map_err(|error| anyhow!("send h3 request body: {error:?}"))?;
        }
        stream
            .finish()
            .await
            .map_err(|error| anyhow!("finish h3 request: {error:?}"))?;

        let response = stream
            .recv_response()
            .await
            .map_err(|error| anyhow!("read h3 response headers: {error:?}"))?;
        let response_headers_us = duration_us(started_at.elapsed());
        let response_status = Some(response.status().as_u16());
        let mut first_body_us = None;
        let mut response_bytes = 0usize;
        while let Some(chunk) = stream
            .recv_data()
            .await
            .map_err(|error| anyhow!("read h3 response body: {error:?}"))?
        {
            if first_body_us.is_none() {
                first_body_us = Some(duration_us(started_at.elapsed()));
            }
            response_bytes += chunk.remaining();
        }
        Ok::<_, anyhow::Error>((
            response_status,
            response_headers_us,
            first_body_us,
            response_bytes,
        ))
    }
    .await;

    match result {
        Ok((response_status, response_headers_us, first_body_us, response_bytes)) => {
            RequestSample {
                request_index,
                connection_index,
                ok: response_status == Some(200) && response_bytes == shape.response_bytes,
                response_status,
                request_bytes: shape.request_bytes,
                response_bytes,
                response_headers_us: Some(response_headers_us),
                first_body_us,
                completion_us: duration_us(started_at.elapsed()),
                error: None,
            }
        }
        Err(error) => RequestSample {
            request_index,
            connection_index,
            ok: false,
            response_status: None,
            request_bytes: shape.request_bytes,
            response_bytes: 0,
            response_headers_us: None,
            first_body_us: None,
            completion_us: duration_us(started_at.elapsed()),
            error: Some(error.to_string()),
        },
    }
}

async fn collect_samples(
    tasks: Vec<tokio::task::JoinHandle<RequestSample>>,
) -> Result<Vec<RequestSample>> {
    let mut samples = Vec::with_capacity(tasks.len());
    for task in tasks {
        samples.push(task.await.context("transport request task panicked")?);
    }
    samples.sort_by_key(|sample| sample.request_index);
    Ok(samples)
}

fn summarize_samples(
    transport: TransportKind,
    samples: &[RequestSample],
    measured_duration: Duration,
) -> TransportRunSummary {
    let success_count = samples.iter().filter(|sample| sample.ok).count();
    let failure_count = samples.len() - success_count;
    let measured_duration_secs = measured_duration.as_secs_f64();
    let throughput_rps = if measured_duration_secs > 0.0 {
        success_count as f64 / measured_duration_secs
    } else {
        0.0
    };
    let transferred_bytes = samples
        .iter()
        .filter(|sample| sample.ok)
        .map(|sample| sample.request_bytes + sample.response_bytes)
        .sum::<usize>();
    let goodput_mib_s = if measured_duration_secs > 0.0 {
        transferred_bytes as f64 / measured_duration_secs / 1024.0 / 1024.0
    } else {
        0.0
    };

    TransportRunSummary {
        transport,
        request_count: samples.len(),
        success_count,
        failure_count,
        measured_duration_ms: duration_ms(measured_duration),
        throughput_rps,
        goodput_mib_s,
        latency_us: summarize_values(
            samples
                .iter()
                .filter(|sample| sample.ok)
                .map(|sample| sample.completion_us),
        ),
        response_headers_us: summarize_values(
            samples
                .iter()
                .filter(|sample| sample.ok)
                .filter_map(|sample| sample.response_headers_us),
        ),
        first_body_us: summarize_values(
            samples
                .iter()
                .filter(|sample| sample.ok)
                .filter_map(|sample| sample.first_body_us),
        ),
    }
}

fn summarize_values(values: impl Iterator<Item = u64>) -> LatencySummary {
    let mut values = values.collect::<Vec<_>>();
    if values.is_empty() {
        return LatencySummary::default();
    }
    values.sort_unstable();
    LatencySummary {
        min: values.first().copied(),
        p50: percentile(&values, 0.50),
        p90: percentile(&values, 0.90),
        p95: percentile(&values, 0.95),
        p99: percentile(&values, 0.99),
        max: values.last().copied(),
    }
}

fn percentile(sorted_values: &[u64], percentile: f64) -> Option<u64> {
    if sorted_values.is_empty() {
        return None;
    }
    let index = upper_nearest_rank_index(sorted_values.len(), percentile)?;
    sorted_values.get(index).copied()
}

fn chunks(total_bytes: usize, chunk_bytes: usize, byte: u8) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    let mut remaining = total_bytes;
    while remaining > 0 {
        let len = remaining.min(chunk_bytes);
        chunks.push(Bytes::from(vec![byte; len]));
        remaining -= len;
    }
    chunks
}

struct GeneratedServerConfig {
    server_config: ServerConfig,
    cert_pem: Vec<u8>,
}

fn server_config(
    config: TransportBenchConfig,
    alpn_protocols: Vec<Vec<u8>>,
) -> Result<GeneratedServerConfig> {
    let (cert_pem, key_pem) = stargate_tls::generate_self_signed_cert()?;
    let cert_chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &*cert_pem)
        .collect::<std::result::Result<_, _>>()
        .context("parse benchmark server cert")?;
    let key = rustls_pemfile::private_key(&mut &*key_pem)
        .context("parse benchmark server key")?
        .context("missing benchmark server key")?;
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("build benchmark server TLS config")?;
    tls_config.alpn_protocols = alpn_protocols;
    let mut server_config = ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_config)?,
    ));
    server_config.transport_config(tuned_transport_config(config));
    Ok(GeneratedServerConfig {
        server_config,
        cert_pem,
    })
}

fn client_config(
    config: TransportBenchConfig,
    alpn_protocols: Vec<Vec<u8>>,
    server_cert_pem: &[u8],
) -> Result<ClientConfig> {
    let cert_chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut &*server_cert_pem)
        .collect::<std::result::Result<_, _>>()
        .context("parse benchmark client root cert")?;
    let mut roots = rustls::RootCertStore::empty();
    for cert in cert_chain {
        roots.add(cert).context("add benchmark root cert")?;
    }
    let mut tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls_config.alpn_protocols = alpn_protocols;
    let mut client_config = ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)?,
    ));
    client_config.transport_config(tuned_transport_config(config));
    Ok(client_config)
}

fn tuned_transport_config(config: TransportBenchConfig) -> Arc<TransportConfig> {
    let mut transport = TransportConfig::default();
    // The benchmark opens many request streams on one QUIC connection, so the default limit of 100
    // would cap high-concurrency runs before either wire protocol is saturated.
    transport.max_concurrent_bidi_streams(VarInt::from_u32(16_384));
    // Expose Quinn's stream scheduler as a benchmark knob. Its documentation calls out lower
    // fragmentation and overhead for workloads with many small streams when fairness is disabled.
    transport.send_fairness(config.quic_send_fairness);
    // Use larger windows so local flow control is not the first bottleneck for payload-heavy runs.
    transport.stream_receive_window(VarInt::from_u32(16 * 1024 * 1024));
    // Use a larger connection window for aggregate throughput across concurrent request streams.
    transport.receive_window(VarInt::from_u32(64 * 1024 * 1024));
    // Match the receive window so either side can fill the local loopback path during throughput tests.
    transport.send_window(64 * 1024 * 1024);
    Arc::new(transport)
}

fn duration_us(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn optional_us(value: Option<u64>) -> String {
    value
        .map(|value| format!("{value} us"))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_float(value: Option<f64>, unit: &str) -> String {
    value
        .map(|value| format!("{value:.2}{unit}"))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_ci(value: &Option<crate::statistics::ConfidenceInterval>, unit: &str) -> String {
    value
        .as_ref()
        .map(|interval| format!("[{:.2}, {:.2}]{unit}", interval.lower, interval.upper))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_cv(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_percent(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.2}%"))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_bool(value: Option<bool>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn summarizes_successful_samples_only() {
        let samples = vec![
            RequestSample {
                request_index: 0,
                connection_index: 0,
                ok: true,
                response_status: Some(200),
                request_bytes: 10,
                response_bytes: 20,
                response_headers_us: Some(100),
                first_body_us: Some(120),
                completion_us: 200,
                error: None,
            },
            RequestSample {
                request_index: 1,
                connection_index: 0,
                ok: false,
                response_status: Some(500),
                request_bytes: 10,
                response_bytes: 0,
                response_headers_us: Some(80),
                first_body_us: None,
                completion_us: 90,
                error: Some("boom".to_string()),
            },
            RequestSample {
                request_index: 2,
                connection_index: 0,
                ok: true,
                response_status: Some(200),
                request_bytes: 10,
                response_bytes: 20,
                response_headers_us: Some(110),
                first_body_us: Some(130),
                completion_us: 300,
                error: None,
            },
        ];

        let summary = summarize_samples(
            TransportKind::CustomProtocol,
            &samples,
            Duration::from_millis(100),
        );

        assert_eq!(summary.request_count, 3);
        assert_eq!(summary.success_count, 2);
        assert_eq!(summary.failure_count, 1);
        assert_eq!(summary.latency_us.min, Some(200));
        assert_eq!(summary.latency_us.p50, Some(300));
        assert_eq!(summary.response_headers_us.p95, Some(110));
        assert!(summary.throughput_rps > 19.0);
        assert!(summary.goodput_mib_s > 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loopback_benchmark_exercises_all_transports() {
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            run_transport_benchmark(TransportBenchConfig {
                request_count: 4,
                concurrency: 2,
                quic_connections: 2,
                warmup_requests: 1,
                request_body_bytes: 32,
                response_body_bytes: 64,
                request_chunk_bytes: 16,
                response_chunk_bytes: 16,
                quic_send_fairness: true,
                http3_send_grease: true,
                trials: 1,
                warmup_trials: 0,
                cooldown_ms: 0,
                randomize_order: false,
                noise_threshold_cv: 0.02,
                min_effect_size_percent: 1.0,
            }),
        )
        .await
        .expect("benchmark should not hang")
        .expect("benchmark should complete");

        let by_transport = outcome
            .runs
            .iter()
            .map(|run| (run.transport, &run.summary))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(by_transport.len(), 3);
        for transport in [
            TransportKind::CustomProtocol,
            TransportKind::Http3H3Quinn,
            TransportKind::WebTransportH3Quinn,
        ] {
            let summary = by_transport
                .get(&transport)
                .expect("summary should exist for transport");
            assert_eq!(summary.request_count, 4);
            assert_eq!(summary.success_count, 4);
            assert_eq!(summary.failure_count, 0);
            assert!(summary.latency_us.p50.is_some());
            let run = outcome
                .runs
                .iter()
                .find(|run| run.transport == transport)
                .expect("run should exist for transport");
            let used_connections = run
                .samples
                .iter()
                .map(|sample| sample.connection_index)
                .collect::<BTreeSet<_>>();
            assert_eq!(used_connections, BTreeSet::from([0, 1]));
        }
        assert_eq!(outcome.aggregates.len(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_connection_set_uses_one_client_endpoint() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let config = TransportBenchConfig {
            request_count: 2,
            concurrency: 2,
            quic_connections: 2,
            warmup_requests: 0,
            request_body_bytes: 32,
            response_body_bytes: 64,
            request_chunk_bytes: 16,
            response_chunk_bytes: 16,
            quic_send_fairness: true,
            http3_send_grease: true,
            trials: 1,
            warmup_trials: 0,
            cooldown_ms: 0,
            randomize_order: false,
            noise_threshold_cv: 0.02,
            min_effect_size_percent: 1.0,
        };
        let server = start_custom_server(config, Arc::new(chunks(64, 16, b's')))
            .await
            .expect("custom transport benchmark server should start");
        let clients = connect_quic_set(
            config,
            server.addr,
            TunnelTransportProtocol::Custom.alpn_protocols(),
            &server.cert_pem,
        )
        .await
        .expect("client connections should open");

        let endpoint_addrs = clients
            .iter()
            .map(|(endpoint, _)| endpoint.local_addr().expect("endpoint local addr"))
            .collect::<BTreeSet<_>>();
        assert_eq!(endpoint_addrs.len(), 1);

        close_quic_clients(clients).await;
        server.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn loopback_benchmark_exercises_webtransport_with_grease_disabled() {
        let outcome = tokio::time::timeout(
            Duration::from_secs(20),
            run_transport_benchmark(TransportBenchConfig {
                request_count: 2,
                concurrency: 1,
                quic_connections: 1,
                warmup_requests: 0,
                request_body_bytes: 32,
                response_body_bytes: 64,
                request_chunk_bytes: 16,
                response_chunk_bytes: 16,
                quic_send_fairness: true,
                http3_send_grease: false,
                trials: 1,
                warmup_trials: 0,
                cooldown_ms: 0,
                randomize_order: false,
                noise_threshold_cv: 0.02,
                min_effect_size_percent: 1.0,
            }),
        )
        .await
        .expect("benchmark should not hang")
        .expect("benchmark should complete");

        let webtransport = outcome
            .runs
            .iter()
            .find(|run| run.transport == TransportKind::WebTransportH3Quinn)
            .expect("WebTransport run should exist");
        assert_eq!(webtransport.summary.success_count, 2);
        assert_eq!(webtransport.summary.failure_count, 0);
    }

    #[test]
    fn report_includes_transport_knobs_and_ttft_tails() {
        let outcome = TransportBenchmarkOutcome {
            config: TransportBenchConfig {
                request_count: 1,
                concurrency: 1,
                quic_connections: 1,
                warmup_requests: 0,
                request_body_bytes: 16,
                response_body_bytes: 32,
                request_chunk_bytes: 16,
                response_chunk_bytes: 16,
                quic_send_fairness: false,
                http3_send_grease: false,
                trials: 1,
                warmup_trials: 0,
                cooldown_ms: 0,
                randomize_order: false,
                noise_threshold_cv: 0.02,
                min_effect_size_percent: 1.0,
            },
            runs: vec![TransportRunOutcome {
                transport: TransportKind::Http3H3Quinn,
                trial_index: 1,
                samples: Vec::new(),
                summary: TransportRunSummary {
                    transport: TransportKind::Http3H3Quinn,
                    request_count: 1,
                    success_count: 1,
                    failure_count: 0,
                    measured_duration_ms: 1,
                    throughput_rps: 1.0,
                    goodput_mib_s: 1.0,
                    latency_us: LatencySummary {
                        p50: Some(10),
                        p95: Some(20),
                        p99: Some(30),
                        max: Some(40),
                        ..LatencySummary::default()
                    },
                    response_headers_us: LatencySummary {
                        p50: Some(5),
                        p95: Some(6),
                        p99: Some(7),
                        ..LatencySummary::default()
                    },
                    first_body_us: LatencySummary {
                        p50: Some(8),
                        p95: Some(9),
                        p99: Some(10),
                        ..LatencySummary::default()
                    },
                },
            }],
            warmup_runs: Vec::new(),
            aggregates: vec![TransportAggregateSummary {
                transport: TransportKind::Http3H3Quinn,
                trial_count: 1,
                classification: NoiseClassification::Inconclusive,
                throughput_rps: summarize_distribution(&[1.0], 1),
                goodput_mib_s: summarize_distribution(&[1.0], 2),
                latency_p95_us: summarize_distribution(&[20.0], 3),
                response_headers_p95_us: summarize_distribution(&[6.0], 4),
                first_body_p95_us: summarize_distribution(&[9.0], 5),
            }],
            comparisons: Vec::new(),
        };

        let report = render_transport_benchmark_report(&outcome);

        assert!(report.contains("QUIC send fairness: `false`"));
        assert!(report.contains("QUIC connections: `1`"));
        assert!(report.contains("HTTP/3 grease: `false`"));
        assert!(report.contains("Trials: `1`"));
        assert!(report.contains("Min effect size"));
        assert!(report.contains("## Aggregate"));
        assert!(report.contains("Headers P95"));
        assert!(report.contains("First Body P99"));
    }

    #[test]
    fn aggregate_classifies_repeated_transport_trials() {
        let runs = vec![
            TransportRunOutcome {
                transport: TransportKind::CustomProtocol,
                trial_index: 1,
                samples: Vec::new(),
                summary: TransportRunSummary {
                    transport: TransportKind::CustomProtocol,
                    request_count: 1,
                    success_count: 1,
                    failure_count: 0,
                    measured_duration_ms: 1,
                    throughput_rps: 100.0,
                    goodput_mib_s: 1.0,
                    latency_us: LatencySummary {
                        p95: Some(100),
                        ..LatencySummary::default()
                    },
                    response_headers_us: LatencySummary {
                        p95: Some(50),
                        ..LatencySummary::default()
                    },
                    first_body_us: LatencySummary {
                        p95: Some(75),
                        ..LatencySummary::default()
                    },
                },
            },
            TransportRunOutcome {
                transport: TransportKind::CustomProtocol,
                trial_index: 2,
                samples: Vec::new(),
                summary: TransportRunSummary {
                    transport: TransportKind::CustomProtocol,
                    request_count: 1,
                    success_count: 1,
                    failure_count: 0,
                    measured_duration_ms: 1,
                    throughput_rps: 100.5,
                    goodput_mib_s: 1.1,
                    latency_us: LatencySummary {
                        p95: Some(101),
                        ..LatencySummary::default()
                    },
                    response_headers_us: LatencySummary {
                        p95: Some(51),
                        ..LatencySummary::default()
                    },
                    first_body_us: LatencySummary {
                        p95: Some(76),
                        ..LatencySummary::default()
                    },
                },
            },
        ];

        let aggregate = summarize_aggregates(&runs, 0.02)
            .into_iter()
            .next()
            .expect("aggregate should exist");

        assert_eq!(aggregate.transport, TransportKind::CustomProtocol);
        assert_eq!(aggregate.trial_count, 2);
        assert_eq!(aggregate.classification, NoiseClassification::Reliable);
    }

    #[test]
    fn comparison_requires_minimum_effect_and_non_overlapping_intervals() {
        let mut custom = TransportAggregateSummary {
            transport: TransportKind::CustomProtocol,
            trial_count: 3,
            classification: NoiseClassification::Reliable,
            throughput_rps: summarize_distribution(&[100.0, 101.0, 99.0], 1),
            goodput_mib_s: summarize_distribution(&[1.0], 2),
            latency_p95_us: summarize_distribution(&[1.0], 3),
            response_headers_p95_us: summarize_distribution(&[1.0], 4),
            first_body_p95_us: summarize_distribution(&[1.0], 5),
        };
        let mut http3 = custom.clone();
        http3.transport = TransportKind::Http3H3Quinn;
        http3.throughput_rps = summarize_distribution(&[80.0, 81.0, 79.0], 6);

        let comparison = summarize_comparisons(&[custom.clone(), http3], 5.0)
            .into_iter()
            .next()
            .expect("comparison should exist");
        assert!(comparison.meaningful_difference);

        custom.throughput_rps = summarize_distribution(&[100.0, 101.0, 99.0], 7);
        let mut close = custom.clone();
        close.transport = TransportKind::Http3H3Quinn;
        close.throughput_rps = summarize_distribution(&[99.5, 100.5, 100.0], 8);
        let comparison = summarize_comparisons(&[custom, close], 5.0)
            .into_iter()
            .next()
            .expect("comparison should exist");
        assert!(!comparison.meaningful_difference);
    }

    #[test]
    fn comparisons_include_each_non_baseline_transport() {
        let custom = TransportAggregateSummary {
            transport: TransportKind::CustomProtocol,
            trial_count: 3,
            classification: NoiseClassification::Reliable,
            throughput_rps: summarize_distribution(&[100.0, 101.0, 99.0], 1),
            goodput_mib_s: summarize_distribution(&[1.0], 2),
            latency_p95_us: summarize_distribution(&[1.0], 3),
            response_headers_p95_us: summarize_distribution(&[1.0], 4),
            first_body_p95_us: summarize_distribution(&[1.0], 5),
        };
        let mut http3 = custom.clone();
        http3.transport = TransportKind::Http3H3Quinn;
        http3.throughput_rps = summarize_distribution(&[90.0, 91.0, 89.0], 6);
        let mut webtransport = custom.clone();
        webtransport.transport = TransportKind::WebTransportH3Quinn;
        webtransport.throughput_rps = summarize_distribution(&[80.0, 81.0, 79.0], 7);

        let comparisons = summarize_comparisons(&[custom, http3, webtransport], 5.0);

        assert_eq!(comparisons.len(), 2);
        assert_eq!(comparisons[0].baseline, TransportKind::CustomProtocol);
        assert_eq!(comparisons[0].candidate, TransportKind::Http3H3Quinn);
        assert_eq!(comparisons[1].baseline, TransportKind::CustomProtocol);
        assert_eq!(comparisons[1].candidate, TransportKind::WebTransportH3Quinn);
        assert!(comparisons.iter().all(|comparison| {
            comparison.confidence_intervals_overlap == Some(false)
                && comparison.meaningful_difference
        }));
    }

    #[test]
    fn single_trial_comparison_is_not_meaningful() {
        let custom = TransportAggregateSummary {
            transport: TransportKind::CustomProtocol,
            trial_count: 1,
            classification: NoiseClassification::Inconclusive,
            throughput_rps: summarize_distribution(&[100.0], 1),
            goodput_mib_s: summarize_distribution(&[1.0], 2),
            latency_p95_us: summarize_distribution(&[1.0], 3),
            response_headers_p95_us: summarize_distribution(&[1.0], 4),
            first_body_p95_us: summarize_distribution(&[1.0], 5),
        };
        let mut http3 = custom.clone();
        http3.transport = TransportKind::Http3H3Quinn;
        http3.throughput_rps = summarize_distribution(&[80.0], 6);

        let comparison = summarize_comparisons(&[custom, http3], 1.0)
            .into_iter()
            .next()
            .expect("comparison should exist");

        assert_eq!(comparison.confidence_intervals_overlap, None);
        assert!(!comparison.meaningful_difference);
    }

    #[test]
    fn repeated_trial_artifacts_include_trial_numbered_samples() {
        let tempdir = tempfile::tempdir().expect("tempdir should create");
        fn empty_run(transport: TransportKind, trial_index: usize) -> TransportRunOutcome {
            TransportRunOutcome {
                transport,
                trial_index,
                samples: vec![RequestSample {
                    request_index: trial_index,
                    connection_index: 0,
                    ok: true,
                    response_status: Some(200),
                    request_bytes: 1,
                    response_bytes: 1,
                    response_headers_us: Some(1),
                    first_body_us: Some(1),
                    completion_us: 1,
                    error: None,
                }],
                summary: TransportRunSummary {
                    transport,
                    request_count: 0,
                    success_count: 0,
                    failure_count: 0,
                    measured_duration_ms: 0,
                    throughput_rps: 0.0,
                    goodput_mib_s: 0.0,
                    latency_us: LatencySummary::default(),
                    response_headers_us: LatencySummary::default(),
                    first_body_us: LatencySummary::default(),
                },
            }
        }

        let outcome = TransportBenchmarkOutcome {
            config: TransportBenchConfig {
                request_count: 1,
                concurrency: 1,
                quic_connections: 1,
                warmup_requests: 0,
                request_body_bytes: 1,
                response_body_bytes: 1,
                request_chunk_bytes: 1,
                response_chunk_bytes: 1,
                quic_send_fairness: true,
                http3_send_grease: true,
                trials: 2,
                warmup_trials: 0,
                cooldown_ms: 0,
                randomize_order: false,
                noise_threshold_cv: 0.02,
                min_effect_size_percent: 1.0,
            },
            runs: vec![
                empty_run(TransportKind::CustomProtocol, 1),
                empty_run(TransportKind::CustomProtocol, 2),
                empty_run(TransportKind::Http3H3Quinn, 1),
                empty_run(TransportKind::Http3H3Quinn, 2),
                empty_run(TransportKind::WebTransportH3Quinn, 1),
                empty_run(TransportKind::WebTransportH3Quinn, 2),
            ],
            warmup_runs: Vec::new(),
            aggregates: Vec::new(),
            comparisons: Vec::new(),
        };

        write_transport_benchmark_artifacts(tempdir.path(), &outcome)
            .expect("artifacts should write");

        assert!(
            tempdir
                .path()
                .join("transport-samples-custom-protocol-trial-1.jsonl")
                .exists()
        );
        assert!(
            tempdir
                .path()
                .join("transport-samples-custom-protocol-trial-2.jsonl")
                .exists()
        );
        assert!(
            tempdir
                .path()
                .join("transport-samples-http3-h3-quinn-trial-1.jsonl")
                .exists()
        );
        assert!(
            tempdir
                .path()
                .join("transport-samples-http3-h3-quinn-trial-2.jsonl")
                .exists()
        );
        assert!(
            tempdir
                .path()
                .join("transport-samples-webtransport-h3-quinn-trial-1.jsonl")
                .exists()
        );
        assert!(
            tempdir
                .path()
                .join("transport-samples-webtransport-h3-quinn-trial-2.jsonl")
                .exists()
        );
        let sample_json = std::fs::read_to_string(
            tempdir
                .path()
                .join("transport-samples-webtransport-h3-quinn-trial-2.jsonl"),
        )
        .expect("sample file should be readable");
        let sample_record = serde_json::from_str::<serde_json::Value>(
            sample_json
                .lines()
                .next()
                .expect("sample file should include a JSONL row"),
        )
        .expect("sample row should parse as JSON");
        assert_eq!(
            sample_record["transport"],
            serde_json::to_value(TransportKind::WebTransportH3Quinn)
                .expect("transport should serialize")
        );
        assert_eq!(sample_record["trial_index"].as_u64(), Some(2));
        assert_eq!(sample_record["request_index"].as_u64(), Some(2));
        assert_eq!(sample_record["ok"].as_bool(), Some(true));
    }
}
