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

mod body_buffer_microbench;
mod config;
mod driver;
mod header_filter_microbench;
mod k8s;
mod lb_microbench;
mod manifest;
mod metadata;
mod orchestrator;
mod report;
mod score;
mod statistics;
mod transport_bench;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::{Args, Parser, Subcommand};

use crate::body_buffer_microbench::{
    BodyBufferMicrobenchConfig, render_body_buffer_microbench_report, run_body_buffer_microbench,
};
use crate::config::{
    AlgorithmConfig, BenchmarkConfig, DegradationActionConfig, DegradationActionKind,
    PylonQueueAdmissionConfig,
};
use crate::driver::{DriveConfig, drive_manifest, load_manifest};
use crate::header_filter_microbench::{
    HeaderFilterMicrobenchConfig, render_header_filter_microbench_report,
    run_header_filter_microbench,
};
use crate::k8s::{
    apply as apply_k8s, collect_logs, delete as delete_k8s, delete_backend_pod,
    prepare_benchmark_k8s_run, scale_backend, stargate_metrics_endpoints, wait_ready,
};
use crate::lb_microbench::{
    LbMicrobenchConfig, LbMicrobenchScenario, run_lb_microbench, write_lb_microbench_csv,
};
use crate::manifest::{ManifestRequest, generate_manifest, write_manifest_json};
use crate::metadata::{
    BenchmarkTier, DriverMode, ReliabilityMode, collect_run_metadata, write_run_metadata,
};
use crate::orchestrator::prepare_suite;
use crate::report::{ReportContext, ReportEntry, render_markdown_report};
use crate::score::{
    RunSummary, backend_capacity_shares, queue_admission_summary_delta_from_prometheus,
    summarize_with_capacity,
};
use crate::transport_bench::{
    TransportBenchConfig, render_transport_benchmark_report, run_transport_benchmark,
    write_transport_benchmark_artifacts,
};

const BENCHES_DIR: &str = "benches";
const COLLECTOR_SCRAPE_SETTLE_DELAY: Duration = Duration::from_millis(1_100);

#[derive(Parser, Debug)]
#[command(name = "stargate-bench")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// List benchmark scenario aliases from benches/*.yaml
    ListScenarios,
    /// Generate a deterministic benchmark manifest and print it to stdout
    InspectManifest {
        #[command(flatten)]
        source: BenchmarkSourceArgs,
        #[arg(long, value_name = "SEED")]
        seed: Option<u64>,
    },
    /// Materialize a deterministic benchmark manifest and input artifacts
    Materialize {
        #[command(flatten)]
        source: BenchmarkSourceArgs,
        #[arg(long, value_name = "SEED")]
        seed: Option<u64>,
        #[arg(long = "algorithm", value_name = "NAME")]
        algorithms: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: Option<PathBuf>,
    },
    /// Prepare per-algorithm local run directories with docker-compose and LB configs
    PrepareRun {
        #[command(flatten)]
        source: BenchmarkSourceArgs,
        #[arg(long, value_name = "SEED")]
        seed: Option<u64>,
        #[arg(long = "algorithm", value_name = "NAME")]
        algorithms: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: Option<PathBuf>,
    },
    /// Replay a manifest against a running stargate endpoint and record per-request results
    Drive {
        #[arg(long, value_name = "PATH")]
        manifest: PathBuf,
        #[arg(long, value_name = "URL")]
        endpoint: String,
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
        #[arg(long, value_name = "N")]
        concurrency_limit: Option<usize>,
    },
    /// Regenerate report.md for an existing benchmark output directory
    Report {
        #[arg(long, value_name = "PATH")]
        output_dir: PathBuf,
    },
    /// Run benchmark suites against Kubernetes using configured benchmark images
    Run {
        #[command(flatten)]
        source: BenchmarkSourceArgs,
        #[arg(long, value_name = "SEED")]
        seed: Option<u64>,
        #[arg(long = "algorithm", value_name = "NAME")]
        algorithms: Vec<String>,
        #[arg(long, value_name = "PATH")]
        output_dir: Option<PathBuf>,
        #[arg(long)]
        keep_resources_on_failure: bool,
        #[arg(long, value_enum, default_value_t = ReliabilityMode::Smoke, value_name = "MODE")]
        reliability_mode: ReliabilityMode,
    },
    /// Compare custom, HTTP/3, and WebTransport tunnel transports on loopback
    TransportBench {
        #[arg(long, default_value_t = 20_000, value_name = "N")]
        requests: usize,
        #[arg(long, default_value_t = 256, value_name = "N")]
        concurrency: usize,
        #[arg(long, default_value_t = 1, value_name = "N")]
        quic_connections: usize,
        #[arg(long, default_value_t = 1_000, value_name = "N")]
        warmup_requests: usize,
        #[arg(long, default_value_t = 1024, value_name = "BYTES")]
        request_body_bytes: usize,
        #[arg(long, default_value_t = 1024, value_name = "BYTES")]
        response_body_bytes: usize,
        #[arg(long, default_value_t = 16 * 1024, value_name = "BYTES")]
        request_chunk_bytes: usize,
        #[arg(long, default_value_t = 16 * 1024, value_name = "BYTES")]
        response_chunk_bytes: usize,
        #[arg(long)]
        disable_quic_send_fairness: bool,
        #[arg(long)]
        disable_http3_grease: bool,
        #[arg(long, default_value_t = 1, value_name = "N")]
        trials: usize,
        #[arg(long, default_value_t = 0, value_name = "N")]
        warmup_trials: usize,
        #[arg(long, default_value_t = 0, value_name = "MS")]
        cooldown_ms: u64,
        #[arg(long)]
        randomize_order: bool,
        #[arg(long, default_value_t = 0.02, value_name = "CV")]
        noise_threshold_cv: f64,
        #[arg(long, default_value_t = 1.0, value_name = "PERCENT")]
        min_effect_size_percent: f64,
        #[arg(long, value_enum, default_value_t = ReliabilityMode::Smoke, value_name = "MODE")]
        reliability_mode: ReliabilityMode,
        #[arg(long, value_name = "PATH")]
        output_dir: Option<PathBuf>,
    },
    /// Measure in-process groq-multiregion/pulsar load-balancer choose-path overhead
    LbMicrobench {
        #[arg(long, default_value_t = 100_000, value_name = "N")]
        iterations: usize,
        #[arg(long, default_value_t = 10_000, value_name = "N")]
        warmup_iterations: usize,
        #[arg(long, default_value_t = 1, value_name = "N")]
        concurrency: usize,
        #[arg(long, default_value_t = 64, value_name = "N")]
        candidates: usize,
        #[arg(long, default_value_t = 1024, value_name = "N")]
        cache_key_count: usize,
        #[arg(long = "scenario", value_enum, value_name = "NAME")]
        scenarios: Vec<LbMicrobenchScenario>,
    },
    /// Compare lowercasing header filters against allocation-free static matchers
    HeaderFilterMicrobench {
        #[arg(long, default_value_t = 1_000_000, value_name = "N")]
        iterations: usize,
        #[arg(long, default_value_t = 100_000, value_name = "N")]
        warmup_iterations: usize,
        #[arg(long, default_value_t = 128, value_name = "N")]
        header_count: usize,
    },
    /// Compare Pylon-style request body buffering copy strategies
    BodyBufferMicrobench {
        #[arg(long, default_value_t = 20_000, value_name = "N")]
        iterations: usize,
        #[arg(long, default_value_t = 2_000, value_name = "N")]
        warmup_iterations: usize,
        #[arg(long, default_value_t = 65_536, value_name = "BYTES")]
        body_bytes: usize,
        #[arg(long, default_value_t = 1_024, value_name = "BYTES")]
        chunk_bytes: usize,
    },
}

#[derive(Args, Debug, Clone)]
struct BenchmarkSourceArgs {
    /// Path to a benchmark YAML/JSON config
    #[arg(long, conflicts_with = "scenario", value_name = "PATH")]
    config: Option<PathBuf>,
    /// Scenario alias from benches/*.yaml, for example hotset-8-backends
    #[arg(long, conflicts_with = "config", value_name = "NAME")]
    scenario: Option<String>,
}

#[derive(Debug, Clone)]
struct Scenario {
    name: String,
    path: PathBuf,
}

#[derive(serde::Deserialize)]
struct RunInfo {
    algorithm_name: String,
    #[serde(default)]
    pylon_queue_admission: Option<PylonQueueAdmissionConfig>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::ListScenarios => list_scenarios(),
        Command::InspectManifest { source, seed } => inspect_manifest(source, seed),
        Command::Materialize {
            source,
            seed,
            algorithms,
            output_dir,
        } => materialize(source, seed, algorithms, output_dir),
        Command::PrepareRun {
            source,
            seed,
            algorithms,
            output_dir,
        } => prepare_run(source, seed, algorithms, output_dir),
        Command::Drive {
            manifest,
            endpoint,
            output,
            concurrency_limit,
        } => drive(&manifest, &endpoint, &output, concurrency_limit),
        Command::Report { output_dir } => regenerate_report(&output_dir),
        Command::Run {
            source,
            seed,
            algorithms,
            output_dir,
            keep_resources_on_failure,
            reliability_mode,
        } => run(
            source,
            seed,
            algorithms,
            output_dir,
            keep_resources_on_failure,
            reliability_mode,
        ),
        Command::TransportBench {
            requests,
            concurrency,
            quic_connections,
            warmup_requests,
            request_body_bytes,
            response_body_bytes,
            request_chunk_bytes,
            response_chunk_bytes,
            disable_quic_send_fairness,
            disable_http3_grease,
            trials,
            warmup_trials,
            cooldown_ms,
            randomize_order,
            noise_threshold_cv,
            min_effect_size_percent,
            reliability_mode,
            output_dir,
        } => transport_bench(
            TransportBenchConfig {
                request_count: requests,
                concurrency,
                quic_connections,
                warmup_requests,
                request_body_bytes,
                response_body_bytes,
                request_chunk_bytes,
                response_chunk_bytes,
                quic_send_fairness: !disable_quic_send_fairness,
                http3_send_grease: !disable_http3_grease,
                trials,
                warmup_trials,
                cooldown_ms,
                randomize_order,
                noise_threshold_cv,
                min_effect_size_percent,
            },
            reliability_mode,
            output_dir,
        ),
        Command::LbMicrobench {
            iterations,
            warmup_iterations,
            concurrency,
            candidates,
            cache_key_count,
            scenarios,
        } => lb_microbench(LbMicrobenchConfig {
            iterations,
            warmup_iterations,
            concurrency,
            candidates,
            cache_key_count,
            scenarios,
        }),
        Command::HeaderFilterMicrobench {
            iterations,
            warmup_iterations,
            header_count,
        } => header_filter_microbench(HeaderFilterMicrobenchConfig {
            iterations,
            warmup_iterations,
            header_count,
        }),
        Command::BodyBufferMicrobench {
            iterations,
            warmup_iterations,
            body_bytes,
            chunk_bytes,
        } => body_buffer_microbench(BodyBufferMicrobenchConfig {
            iterations,
            warmup_iterations,
            body_bytes,
            chunk_bytes,
        }),
    }
}

fn body_buffer_microbench(config: BodyBufferMicrobenchConfig) -> anyhow::Result<()> {
    let outcome = run_body_buffer_microbench(config)?;
    print!("{}", render_body_buffer_microbench_report(&outcome));
    Ok(())
}

fn header_filter_microbench(config: HeaderFilterMicrobenchConfig) -> anyhow::Result<()> {
    let outcome = run_header_filter_microbench(config)?;
    print!("{}", render_header_filter_microbench_report(&outcome));
    Ok(())
}

fn resolve_config_path(source: BenchmarkSourceArgs) -> anyhow::Result<PathBuf> {
    match (source.config, source.scenario) {
        (Some(config), None) => Ok(config),
        (None, Some(scenario)) => scenario_config_path(&scenario),
        (None, None) => anyhow::bail!("provide either --config <path> or --scenario <name>"),
        (Some(_), Some(_)) => anyhow::bail!("provide only one of --config or --scenario"),
    }
}

fn scenario_config_path(scenario: &str) -> anyhow::Result<PathBuf> {
    let name = scenario
        .strip_suffix(".yaml")
        .or_else(|| scenario.strip_suffix(".yml"))
        .unwrap_or(scenario);
    if name.contains('/') || name.contains('\\') || name.is_empty() {
        anyhow::bail!("scenario names must be file stems from benches/*.yaml");
    }

    let path = benches_dir().join(format!("{name}.yaml"));
    if path.exists() {
        return Ok(path);
    }

    let available = discover_scenarios()?
        .into_iter()
        .map(|scenario| scenario.name)
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::bail!("unknown benchmark scenario '{name}'. Available scenarios: {available}")
}

fn discover_scenarios() -> anyhow::Result<Vec<Scenario>> {
    let mut scenarios = Vec::new();
    let entries = match std::fs::read_dir(benches_dir()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(scenarios),
        Err(error) => return Err(error).context("failed to read benches directory"),
    };
    for entry in entries {
        let entry = entry.context("failed to read benches directory entry")?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("yaml") {
            continue;
        }
        let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        scenarios.push(Scenario {
            name: name.to_string(),
            path,
        });
    }
    scenarios.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(scenarios)
}

fn filter_algorithms(config: &mut BenchmarkConfig, requested: &[String]) -> anyhow::Result<()> {
    if requested.is_empty() {
        return Ok(());
    }

    let requested = requested.iter().cloned().collect::<BTreeSet<_>>();
    let available = config
        .algorithms
        .iter()
        .map(|algorithm| algorithm.name.clone())
        .collect::<BTreeSet<_>>();
    let unknown = requested
        .difference(&available)
        .cloned()
        .collect::<Vec<_>>();
    if !unknown.is_empty() {
        anyhow::bail!(
            "unknown algorithm(s): {}. Available algorithms: {}",
            unknown.join(", "),
            available.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    config
        .algorithms
        .retain(|algorithm| requested.contains(&algorithm.name));
    Ok(())
}

fn default_output_dir(config: &BenchmarkConfig) -> PathBuf {
    Path::new(".bench-out").join(&config.name)
}

fn benches_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap_or_else(|| Path::new("."))
        .join(BENCHES_DIR)
}

fn list_scenarios() -> anyhow::Result<()> {
    let scenarios = discover_scenarios()?;
    if scenarios.is_empty() {
        println!("no benchmark scenarios found under benches/");
        return Ok(());
    }
    println!(
        "{:<28} {:>8} {:>9} {:>8}  {:<24}  Algorithms",
        "Scenario", "Requests", "Backends", "Stargates", "Tags"
    );
    for scenario in scenarios {
        let config = BenchmarkConfig::load(&scenario.path)?;
        let algorithms = config
            .algorithms
            .iter()
            .map(|algorithm| algorithm.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<28} {:>8} {:>9} {:>8}  {:<24}  {}",
            scenario.name,
            config.request_count,
            config.backends.count,
            config.stargates.count,
            config.metadata.tags.join(","),
            algorithms
        );
    }
    Ok(())
}

fn inspect_manifest(source: BenchmarkSourceArgs, seed: Option<u64>) -> anyhow::Result<()> {
    let config_path = resolve_config_path(source)?;
    let config = BenchmarkConfig::load(&config_path)?;
    let manifest = generate_manifest(&config, seed)?;
    let rendered =
        serde_json::to_string_pretty(&manifest).context("failed to render manifest as JSON")?;
    println!("{rendered}");
    Ok(())
}

fn materialize(
    source: BenchmarkSourceArgs,
    seed: Option<u64>,
    algorithms: Vec<String>,
    output_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let config_path = resolve_config_path(source)?;
    let mut config = BenchmarkConfig::load(&config_path)?;
    filter_algorithms(&mut config, &algorithms)?;
    let manifest = generate_manifest(&config, seed)?;
    let output_dir = output_dir.unwrap_or_else(|| default_output_dir(&config));
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;

    let effective_seed = manifest.seed;
    let manifest_path = output_dir.join("manifest.json");
    let config_copy_path = output_dir.join("benchmark-config.json");
    let summary_path = output_dir.join("summary.json");

    write_manifest_json(&manifest_path, &manifest)?;
    let config_bytes =
        serde_json::to_vec_pretty(&config).context("failed to serialize normalized config")?;
    std::fs::write(&config_copy_path, config_bytes)
        .with_context(|| format!("failed to write {}", config_copy_path.display()))?;

    let summary = serde_json::json!({
        "benchmark_name": config.name,
        "metadata": config.metadata.clone(),
        "model": config.model,
        "seed": effective_seed,
        "request_count": manifest.request_count,
        "stargate_count": manifest.stargate_count,
        "backend_count": manifest.backend_count,
        "algorithm_names": config.algorithms.iter().map(|algorithm| algorithm.name.clone()).collect::<Vec<_>>(),
    });
    let summary_bytes =
        serde_json::to_vec_pretty(&summary).context("failed to serialize summary")?;
    std::fs::write(&summary_path, summary_bytes)
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    println!(
        "materialized benchmark input at {}",
        output_dir
            .canonicalize()
            .unwrap_or_else(|_| output_dir.to_path_buf())
            .display()
    );
    Ok(())
}

fn prepare_run(
    source: BenchmarkSourceArgs,
    seed: Option<u64>,
    algorithms: Vec<String>,
    output_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let config_path = resolve_config_path(source)?;
    let mut config = BenchmarkConfig::load(&config_path)?;
    filter_algorithms(&mut config, &algorithms)?;
    let manifest = generate_manifest(&config, seed)?;
    let output_dir = output_dir.unwrap_or_else(|| default_output_dir(&config));
    let prepared = prepare_suite(&config, &manifest, &output_dir)?;
    println!(
        "prepared {} algorithm runs at {}",
        prepared.algorithm_runs.len(),
        output_dir
            .canonicalize()
            .unwrap_or_else(|_| output_dir.to_path_buf())
            .display()
    );
    for run in prepared.algorithm_runs {
        println!(
            "{}: compose={} http={} grpc={}",
            run.algorithm_name,
            run.compose_path.display(),
            run.stargate_http_endpoint,
            run.stargate_grpc_endpoint
        );
    }
    Ok(())
}

fn drive(
    manifest_path: &Path,
    endpoint: &str,
    output: &Path,
    concurrency_limit: Option<usize>,
) -> anyhow::Result<()> {
    let manifest = load_manifest(manifest_path)?;
    let concurrency_limit = concurrency_limit.unwrap_or(manifest.max_concurrency);
    let runtime = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let results = runtime.block_on(drive_manifest(
        DriveConfig {
            endpoint: endpoint.to_string(),
            output_path: output.to_path_buf(),
            concurrency_limit,
        },
        manifest,
    ))?;
    println!(
        "wrote {} request results to {}",
        results.len(),
        output.display()
    );
    Ok(())
}

fn transport_bench(
    config: TransportBenchConfig,
    reliability_mode: ReliabilityMode,
    output_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let metadata = collect_run_metadata(
        BenchmarkTier::TransportLoopback,
        reliability_mode,
        DriverMode::LocalProcess,
    );
    if let Some(output_dir) = &output_dir {
        std::fs::create_dir_all(output_dir)
            .with_context(|| format!("failed to create {}", output_dir.display()))?;
        write_run_metadata(&output_dir.join("run-metadata.json"), &metadata)?;
    }
    if metadata.preflight.should_fail {
        anyhow::bail!(
            "strict reliability preflight failed with {} failure(s); inspect run-metadata.json when --output-dir is set",
            metadata.preflight.failure_count
        );
    }

    let runtime = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let outcome = runtime.block_on(run_transport_benchmark(config))?;
    let report = render_transport_benchmark_report(&outcome);
    println!("{report}");
    if let Some(output_dir) = output_dir {
        write_transport_benchmark_artifacts(&output_dir, &outcome)?;
        println!(
            "wrote transport benchmark artifacts to {}",
            output_dir.display()
        );
    }
    Ok(())
}

fn lb_microbench(config: LbMicrobenchConfig) -> anyhow::Result<()> {
    let rows = run_lb_microbench(&config)?;
    write_lb_microbench_csv(std::io::stdout(), &rows).context("failed to write lb microbench CSV")
}

fn regenerate_report(output_dir: &Path) -> anyhow::Result<()> {
    let manifest = load_manifest(&output_dir.join("manifest.json"))?;
    let context = ReportContext::from_manifest(&manifest);
    let mut entries = Vec::new();
    let dirs = std::fs::read_dir(output_dir)
        .with_context(|| format!("failed to read output dir {}", output_dir.display()))?;
    for entry in dirs {
        let entry = entry.context("failed to read output dir entry")?;
        let path = entry.path();
        if !path.is_dir()
            || !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("run-"))
        {
            continue;
        }
        let summary_path = path.join("summary.json");
        if !summary_path.exists() {
            continue;
        }
        let summary = read_json::<RunSummary>(&summary_path)?;
        let run_info = read_run_info(&path)?;
        entries.push(ReportEntry {
            algorithm_name: run_info.algorithm_name,
            pylon_queue_admission: run_info.pylon_queue_admission,
            summary,
        });
    }
    entries.sort_by(|a, b| a.algorithm_name.cmp(&b.algorithm_name));
    let report = render_markdown_report(&context, &entries);
    let report_path = output_dir.join("report.md");
    std::fs::write(&report_path, report)
        .with_context(|| format!("failed to write {}", report_path.display()))?;
    println!("wrote {}", report_path.display());
    Ok(())
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

fn read_run_info(run_dir: &Path) -> anyhow::Result<RunInfo> {
    let run_info_path = run_dir.join("run-info.json");
    if run_info_path.exists() {
        return read_json::<RunInfo>(&run_info_path);
    }
    let name = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("run-"))
        .context("run directory must be named run-<algorithm>")?;
    Ok(RunInfo {
        algorithm_name: name.to_string(),
        pylon_queue_admission: None,
    })
}

fn run(
    source: BenchmarkSourceArgs,
    seed: Option<u64>,
    algorithms: Vec<String>,
    output_dir: Option<PathBuf>,
    keep_resources_on_failure: bool,
    reliability_mode: ReliabilityMode,
) -> anyhow::Result<()> {
    let config_path = resolve_config_path(source)?;
    let mut config = BenchmarkConfig::load(&config_path)?;
    filter_algorithms(&mut config, &algorithms)?;
    let manifest = generate_manifest(&config, seed)?;
    let output_dir = output_dir.unwrap_or_else(|| default_output_dir(&config));
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output dir {}", output_dir.display()))?;
    let metadata = collect_run_metadata(
        BenchmarkTier::LocalK8sSmoke,
        reliability_mode,
        DriverMode::ExternalNodePort,
    );
    write_run_metadata(&output_dir.join("run-metadata.json"), &metadata)?;
    if metadata.preflight.should_fail {
        anyhow::bail!(
            "strict reliability preflight failed with {} failure(s); inspect {}",
            metadata.preflight.failure_count,
            output_dir.join("run-metadata.json").display()
        );
    }
    ensure_k8s_context()?;
    let manifest_path = output_dir.join("manifest.json");
    write_manifest_json(&manifest_path, &manifest)?;
    println!(
        "running benchmark '{}' with {} request(s), {} backend(s), {} stargate(s)",
        config.name, config.request_count, config.backends.count, config.stargates.count
    );
    println!("output directory: {}", output_dir.display());
    println!(
        "algorithms: {}",
        config
            .algorithms
            .iter()
            .map(|algorithm| algorithm.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut comparison = Vec::with_capacity(config.algorithms.len());
    let mut report_entries = Vec::with_capacity(config.algorithms.len());
    let backend_capacity_shares = backend_capacity_shares(&config.backends);
    for (run_index, algorithm) in config.algorithms.iter().enumerate() {
        println!(
            "starting algorithm {}/{}: {}",
            run_index + 1,
            config.algorithms.len(),
            algorithm.name
        );
        let run =
            prepare_benchmark_k8s_run(&config, algorithm, &manifest_path, &output_dir, run_index)?;
        apply_k8s(&run)?;
        let run_result = run_single_k8s(
            &run,
            manifest.max_concurrency,
            config.backends.count,
            &backend_capacity_shares,
            &config.degradation.actions,
        );
        if (run_result.is_err() || keep_resources_on_failure)
            && let Err(error) = collect_logs(&run)
        {
            eprintln!(
                "warning: failed to collect k8s benchmark logs for {}: {error}",
                run.algorithm_name
            );
        }
        if keep_resources_on_failure && run_result.is_err() {
            eprintln!(
                "keeping k8s benchmark resources for failed run {}",
                run.algorithm_name
            );
        } else {
            let teardown_result = delete_k8s(&run);
            if let Err(error) = teardown_result {
                eprintln!(
                    "warning: failed to delete k8s benchmark resources for {}: {error}",
                    run.algorithm_name,
                );
            }
        }
        let summary = run_result?;
        println!(
            "finished {}: success_rate={:.3}, avg_ttlt_ms={:.1}, run_dir={}",
            run.algorithm_name,
            summary.success_rate,
            summary.avg_ttlt_ms,
            run.run_dir.display()
        );
        let algorithm_name = run.algorithm_name.clone();
        comparison.push(comparison_entry(algorithm, &summary));
        report_entries.push(ReportEntry {
            algorithm_name,
            pylon_queue_admission: algorithm.pylon_queue_admission.clone(),
            summary,
        });
    }

    let comparison_path = output_dir.join("comparison.json");
    let comparison_bytes = serde_json::to_vec_pretty(&comparison)
        .context("failed to serialize benchmark comparison")?;
    std::fs::write(&comparison_path, comparison_bytes)
        .with_context(|| format!("failed to write {}", comparison_path.display()))?;
    let report_path = output_dir.join("report.md");
    let report = render_markdown_report(&ReportContext::from_config(&config), &report_entries);
    std::fs::write(&report_path, report)
        .with_context(|| format!("failed to write {}", report_path.display()))?;
    println!("completed {} algorithm runs", comparison.len());
    Ok(())
}

fn comparison_entry(algorithm: &AlgorithmConfig, summary: &RunSummary) -> serde_json::Value {
    serde_json::json!({
        "algorithm_name": algorithm.name,
        "pylon_queue_admission": algorithm.pylon_queue_admission,
        "success_rate": summary.success_rate,
        "avg_ttft_ms": summary.avg_ttft_ms,
        "avg_ttlt_ms": summary.avg_ttlt_ms,
        "max_ttlt_ms": summary.max_ttlt_ms,
        "total_length_ms": summary.total_length_ms,
        "balance_score": summary.balance_score,
        "capacity_balance_score": summary.capacity_balance_score,
        "cache_observed_request_count": summary.cache_summary.observed_request_count,
        "cache_hit_count": summary.cache_summary.hit_count,
        "cache_miss_count": summary.cache_summary.miss_count,
        "cache_hit_rate": summary.cache_summary.hit_rate,
        "cache_eviction_count": summary.cache_summary.eviction_count,
        "cache_evicted_tokens": summary.cache_summary.evicted_tokens,
        "cache_key_movement_rate": summary.stickiness_summary.movement_rate,
        "moved_cache_key_count": summary.stickiness_summary.moved_cache_key_count,
        "failure_group_count": summary.failure_summary.len(),
        "queue_admission": summary.queue_admission_summary,
    })
}

fn ensure_k8s_context() -> anyhow::Result<()> {
    let output = ProcessCommand::new("kubectl")
        .arg("config")
        .arg("current-context")
        .output()
        .context("failed to query current kubectl context")?;
    if !output.status.success() || String::from_utf8_lossy(&output.stdout).trim().is_empty() {
        anyhow::bail!(
            "no active kubectl context; configure access to a Kubernetes cluster before running Kubernetes benchmarks"
        );
    }
    Ok(())
}

fn run_single_k8s(
    run: &crate::k8s::BenchmarkK8sRun,
    concurrency_limit: usize,
    backend_count: usize,
    backend_capacity_shares: &std::collections::BTreeMap<String, f64>,
    degradation_actions: &[DegradationActionConfig],
) -> anyhow::Result<RunSummary> {
    wait_ready(run, backend_count)?;
    let runtime = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    let manifest = load_manifest(&run.manifest_path)?;
    let routing_probe_request = manifest
        .requests
        .first()
        .ok_or_else(|| anyhow::anyhow!("benchmark manifest must contain at least one request"))?;
    runtime.block_on(wait_for_http_ok(
        &format!("{}/healthz", run.stargate_http_endpoint),
        Duration::from_secs(60),
    ))?;
    let metrics_endpoints = stargate_metrics_endpoints(run)?;
    runtime.block_on(wait_for_active_backend_counts(
        &metrics_endpoints,
        &manifest.model,
        routing_probe_request.routing_key.as_deref(),
        backend_count,
        Duration::from_secs(60),
    ))?;
    runtime.block_on(wait_for_routing(
        &format!("{}/v1/chat/completions", run.stargate_http_endpoint),
        &manifest.model,
        routing_probe_request,
        Duration::from_secs(60),
    ))?;
    let collector_baseline = runtime.block_on(wait_for_scraped_benchmark_metrics(
        &run.collector_metrics_endpoint,
        Duration::from_secs(60),
    ))?;
    let baseline_request_totals = scraped_request_totals(&collector_baseline)
        .context("collector baseline did not expose Stargate and Pylon request counters")?;
    let collector_baseline_path = run.run_dir.join("collector-baseline-metrics.prom");
    std::fs::write(&collector_baseline_path, &collector_baseline)
        .with_context(|| format!("failed to write {}", collector_baseline_path.display()))?;

    let results_path = run.run_dir.join("requests.jsonl");
    let degradation_handles =
        start_degradation_actions(run, &manifest.requests, degradation_actions);
    let results = runtime.block_on(drive_manifest(
        DriveConfig {
            endpoint: format!("{}/v1/chat/completions", run.stargate_http_endpoint),
            output_path: results_path,
            concurrency_limit,
        },
        manifest,
    ));
    join_degradation_actions(degradation_handles);
    let results = results?;
    let successful_request_count = results.iter().filter(|result| result.ok).count();

    let mut summary = summarize_with_capacity(&results, backend_capacity_shares.clone());

    if let Ok(metrics) = runtime.block_on(fetch_text(&run.stargate_metrics_endpoint)) {
        let metrics_path = run.run_dir.join("metrics.prom");
        std::fs::write(&metrics_path, metrics)
            .with_context(|| format!("failed to write {}", metrics_path.display()))?;
    }

    let collector_metrics = runtime.block_on(wait_for_post_replay_scraped_benchmark_metrics(
        &run.collector_metrics_endpoint,
        baseline_request_totals,
        results.len(),
        successful_request_count,
        Duration::from_secs(60),
    ))?;
    let collector_metrics_path = run.run_dir.join("collector-metrics.prom");
    std::fs::write(&collector_metrics_path, &collector_metrics)
        .with_context(|| format!("failed to write {}", collector_metrics_path.display()))?;
    summary.queue_admission_summary =
        queue_admission_summary_delta_from_prometheus(&collector_baseline, &collector_metrics);
    let summary_path = run.run_dir.join("summary.json");
    let summary_bytes =
        serde_json::to_vec_pretty(&summary).context("failed to serialize run summary")?;
    std::fs::write(&summary_path, summary_bytes)
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    Ok(summary)
}

fn start_degradation_actions(
    run: &crate::k8s::BenchmarkK8sRun,
    requests: &[ManifestRequest],
    actions: &[DegradationActionConfig],
) -> Vec<std::thread::JoinHandle<()>> {
    actions
        .iter()
        .map(|action| {
            let action = action.clone();
            let run = run.clone();
            let delay = requests
                .get(action.at_request)
                .map(|request| Duration::from_millis(request.scheduled_offset_ms))
                .unwrap_or_default();
            std::thread::spawn(move || {
                std::thread::sleep(delay);
                let result = match action.action {
                    DegradationActionKind::DeleteBackendPod => {
                        delete_backend_pod(&run, action.backend_index)
                    }
                    DegradationActionKind::ScaleBackend { replicas } => {
                        scale_backend(&run, action.backend_index, replicas)
                    }
                };
                if let Err(error) = result {
                    eprintln!(
                        "warning: degradation action failed for backend-{} in {}: {error}",
                        action.backend_index, run.algorithm_name
                    );
                }
            })
        })
        .collect()
}

fn join_degradation_actions(handles: Vec<std::thread::JoinHandle<()>>) {
    for handle in handles {
        let _ = handle.join();
    }
}

async fn wait_for_http_ok(url: &str, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let client = reqwest::Client::new();
    loop {
        if let Ok(response) = client.get(url).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for {}", url);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn fetch_text(url: &str) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to fetch {}", url))?;
    response
        .text()
        .await
        .with_context(|| format!("failed to read response body from {}", url))
}

async fn wait_for_scraped_benchmark_metrics(
    collector_metrics_endpoint: &str,
    timeout: Duration,
) -> anyhow::Result<String> {
    let deadline = Instant::now() + timeout;
    let mut last_metrics_len = 0usize;
    loop {
        if let Ok(metrics) = fetch_text(collector_metrics_endpoint).await {
            last_metrics_len = metrics.len();
            if has_scraped_benchmark_metrics(&metrics) {
                return Ok(metrics);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for OTel collector to scrape benchmark metrics from {} (last_response_bytes={})",
                collector_metrics_endpoint,
                last_metrics_len
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_post_replay_scraped_benchmark_metrics(
    collector_metrics_endpoint: &str,
    baseline: ScrapedRequestTotals,
    replay_request_count: usize,
    replay_success_count: usize,
    timeout: Duration,
) -> anyhow::Result<String> {
    let started_at = Instant::now();
    let deadline = started_at + timeout;
    let mut last_metrics_len = 0usize;
    loop {
        if let Ok(metrics) = fetch_text(collector_metrics_endpoint).await {
            last_metrics_len = metrics.len();
            if started_at.elapsed() >= COLLECTOR_SCRAPE_SETTLE_DELAY
                && has_post_replay_scraped_benchmark_metrics(
                    &metrics,
                    baseline,
                    replay_request_count,
                    replay_success_count,
                )
            {
                return Ok(metrics);
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for post-replay OTel collector metrics from {} (last_response_bytes={})",
                collector_metrics_endpoint,
                last_metrics_len
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn has_scraped_benchmark_metrics(metrics: &str) -> bool {
    has_any_metric(
        metrics,
        &["stargate_requests_total", "stargate_requests_total_total"],
    ) && has_any_metric(
        metrics,
        &["pylon_requests_total", "pylon_requests_total_total"],
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct ScrapedRequestTotals {
    stargate: f64,
    pylon: f64,
}

fn scraped_request_totals(metrics: &str) -> Option<ScrapedRequestTotals> {
    if !has_scraped_benchmark_metrics(metrics) {
        return None;
    }
    Some(ScrapedRequestTotals {
        stargate: metric_total(
            metrics,
            &["stargate_requests_total", "stargate_requests_total_total"],
        ),
        pylon: metric_total(
            metrics,
            &["pylon_requests_total", "pylon_requests_total_total"],
        ),
    })
}

fn has_post_replay_scraped_benchmark_metrics(
    metrics: &str,
    baseline: ScrapedRequestTotals,
    replay_request_count: usize,
    replay_success_count: usize,
) -> bool {
    let Some(current) = scraped_request_totals(metrics) else {
        return false;
    };
    current.stargate >= baseline.stargate + replay_request_count as f64
        && current.pylon >= baseline.pylon + replay_success_count as f64
}

fn metric_total(metrics: &str, names: &[&str]) -> f64 {
    metrics
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let series = fields.next()?;
            if !names
                .iter()
                .any(|name| series.starts_with(&format!("{name}{{")) || series == *name)
            {
                return None;
            }
            fields.next()?.parse::<f64>().ok()
        })
        .sum()
}

fn has_any_metric(metrics: &str, names: &[&str]) -> bool {
    metrics.lines().any(|line| {
        names.iter().any(|name| {
            line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} "))
        })
    })
}

async fn wait_for_active_backend_counts(
    metrics_endpoints: &[String],
    model: &str,
    routing_key: Option<&str>,
    expected_count: usize,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let mut last_counts = Vec::new();
    loop {
        last_counts.clear();
        for metrics_endpoint in metrics_endpoints {
            let count = match fetch_text(metrics_endpoint).await {
                Ok(metrics) => active_backend_count(&metrics, model, routing_key),
                Err(_) => None,
            };
            last_counts.push(count);
        }
        if active_backend_counts_ready(&last_counts, expected_count) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for {expected_count} active benchmark backends on every stargate metrics endpoint {:?} (model={}, routing_key={:?}, last_counts={:?})",
                metrics_endpoints,
                model,
                routing_key,
                last_counts
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn active_backend_counts_ready(counts: &[Option<usize>], expected_count: usize) -> bool {
    !counts.is_empty()
        && counts
            .iter()
            .all(|count| count.is_some_and(|count| count >= expected_count))
}

fn active_backend_count(metrics: &str, model: &str, routing_key: Option<&str>) -> Option<usize> {
    metrics.lines().find_map(|line| {
        if !line.starts_with("stargate_active_inference_servers{") {
            return None;
        }
        let (metric, value) = line.rsplit_once(' ')?;
        let metric_model = prometheus_label_value(metric, "model")?;
        let metric_routing_key = prometheus_label_value(metric, "routing_key").unwrap_or("");
        if metric_model != model || metric_routing_key != routing_key.unwrap_or("") {
            return None;
        }
        value.parse::<f64>().ok().map(|value| value as usize)
    })
}

fn prometheus_label_value<'a>(metric: &'a str, label: &str) -> Option<&'a str> {
    let needle = format!(r#"{label}=""#);
    let start = metric.find(&needle)? + needle.len();
    let rest = &metric[start..];
    let end = rest.find('"')?;
    Some(&rest[..end])
}

async fn wait_for_routing(
    endpoint: &str,
    model: &str,
    request: &ManifestRequest,
    timeout: Duration,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    let client = reqwest::Client::new();
    let body = serde_json::json!({
        "model": model,
        "messages": [{"role": "user", "content": "benchmark-ready"}],
        "max_tokens": 1,
        "stream": true,
    });
    let mut last_status = None;
    let probe_cache_affinity_key = routing_probe_cache_affinity_key(request);
    loop {
        let mut builder = client
            .post(endpoint)
            .header("content-type", "application/json")
            .header("x-model", model)
            .header(
                "x-request-id",
                format!("benchmark-ready-probe-{}", request.request_index),
            )
            .header("x-input-tokens", "1")
            .header("x-output-tokens", "1");
        if let Some(routing_key) = &request.routing_key {
            builder = builder.header("x-routing-key", routing_key);
        }
        if let Some(cache_affinity_key) = &probe_cache_affinity_key {
            builder = builder.header("x-cache-affinity-key", cache_affinity_key);
        }
        match builder.json(&body).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => {
                last_status = Some(response.status());
            }
            Err(_) => {}
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for routable benchmark traffic on {} (model={}, routing_key={:?}, cache_affinity_key={:?}, last_status={:?})",
                endpoint,
                model,
                request.routing_key,
                probe_cache_affinity_key,
                last_status
            );
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn routing_probe_cache_affinity_key(request: &ManifestRequest) -> Option<String> {
    request.cache_affinity_key.as_ref().map(|_| {
        format!(
            "__stargate_bench_benchmark-ready-probe-{}",
            request.request_index
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_active_backend_count_metric() {
        let metrics = r#"# HELP stargate_active_inference_servers Active inference servers available for a routing target
# TYPE stargate_active_inference_servers gauge
stargate_active_inference_servers{model="dummy-model",routing_key=""} 8
"#;

        assert_eq!(active_backend_count(metrics, "dummy-model", None), Some(8));
    }

    #[test]
    fn recognizes_collector_scraped_benchmark_metrics() {
        let metrics = r#"# HELP stargate_requests_total total
# TYPE stargate_requests_total counter
stargate_requests_total{model="dummy-model",routing_key="",status="ok"} 3
# HELP pylon_requests_total total
# TYPE pylon_requests_total counter
pylon_requests_total{model="dummy-model",routing_key="",status="complete"} 3
"#;

        assert!(has_scraped_benchmark_metrics(metrics));
    }

    #[test]
    fn rejects_collector_metrics_without_pylon_request_metrics() {
        let metrics = r#"# HELP stargate_requests_total total
# TYPE stargate_requests_total counter
stargate_requests_total{model="dummy-model",routing_key="",status="ok"} 3
"#;

        assert!(!has_scraped_benchmark_metrics(metrics));
    }

    #[test]
    fn post_replay_metrics_require_request_counter_progress_beyond_readiness_probe() {
        let baseline_metrics = r#"
stargate_requests_total_total{model="dummy-model",status="ok"} 1
pylon_requests_total_total{model="dummy-model",status="complete"} 1
"#;
        let stale_metrics = baseline_metrics;
        let updated_metrics = r#"
stargate_requests_total_total{model="dummy-model",status="ok"} 4
pylon_requests_total_total{model="dummy-model",status="complete"} 3
"#;
        let baseline = scraped_request_totals(baseline_metrics).expect("baseline should parse");

        assert!(!has_post_replay_scraped_benchmark_metrics(
            stale_metrics,
            baseline,
            3,
            2,
        ));
        assert!(has_post_replay_scraped_benchmark_metrics(
            updated_metrics,
            baseline,
            3,
            2,
        ));
    }

    #[test]
    fn active_backend_readiness_requires_every_metrics_endpoint() {
        assert!(active_backend_counts_ready(&[Some(4), Some(4)], 4));
        assert!(!active_backend_counts_ready(&[Some(4), Some(3)], 4));
        assert!(!active_backend_counts_ready(&[Some(4), None], 4));
    }

    #[test]
    fn routing_probe_uses_synthetic_cache_key() {
        let request = ManifestRequest {
            request_index: 7,
            request_id: "req-7".to_string(),
            scheduled_offset_ms: 0,
            routing_key: None,
            cache_affinity_key: Some("real-cache-key".to_string()),
            input_tokens: 128,
            output_tokens: 16,
            backend_behavior_class: "default".to_string(),
        };

        let probe_key = routing_probe_cache_affinity_key(&request)
            .expect("probe should include a cache key when the benchmark request has one");
        assert_ne!(probe_key, "real-cache-key");
        assert!(probe_key.contains("benchmark-ready-probe"));
    }

    #[test]
    fn run_info_reader_ignores_extra_run_metadata_fields() {
        let tempdir = tempfile::tempdir().expect("tempdir should create");
        let run_dir = tempdir.path().join("run-power-of-two");
        std::fs::create_dir(&run_dir).expect("run dir should create");
        std::fs::write(
            run_dir.join("run-info.json"),
            r#"{
                "algorithm_name": "groq-multiregion",
                "stargate_http_endpoint": "http://127.0.0.1:8000",
                "run_dir": "/tmp/stargate-bench/run-groq-multiregion",
                "backends_namespace": "stargate-bench-backends"
            }"#,
        )
        .expect("run-info should write");

        let algorithm_name = read_run_info(&run_dir)
            .expect("run-info extra fields should be ignored")
            .algorithm_name;

        assert_eq!(algorithm_name, "groq-multiregion");
    }

    #[test]
    fn scenario_name_resolves_to_bench_yaml() {
        assert_eq!(
            scenario_config_path("uniform-4-backends")
                .expect("scenario should resolve")
                .file_name()
                .and_then(|name| name.to_str()),
            Some("uniform-4-backends.yaml")
        );
        assert_eq!(
            scenario_config_path("uniform-4-backends.yaml")
                .expect("scenario with extension should resolve")
                .file_name()
                .and_then(|name| name.to_str()),
            Some("uniform-4-backends.yaml")
        );
    }

    #[test]
    fn queue_mismatch_ab_scenario_changes_only_admission_enabled_behavior() {
        let config =
            BenchmarkConfig::load(&scenario_config_path("queue-mismatch-retry-ab").unwrap())
                .expect("A/B scenario config should load");
        assert!(
            config.request_count >= 2048,
            "queue mismatch A/B evidence needs at least 2048 requests per arm"
        );
        assert_eq!(config.algorithms.len(), 2);
        assert_eq!(config.algorithms[0].config, config.algorithms[1].config);
        let enabled = config.algorithms[0]
            .pylon_queue_admission
            .as_ref()
            .expect("enabled variant should specify admission");
        let disabled = config.algorithms[1]
            .pylon_queue_admission
            .as_ref()
            .expect("disabled variant should specify admission");

        assert!(enabled.enabled);
        assert!(!disabled.enabled);
        assert_eq!(enabled.min_delta_ms, disabled.min_delta_ms);
        assert_eq!(enabled.tolerance_factor, disabled.tolerance_factor);
        assert_eq!(enabled.retry_after_ms, disabled.retry_after_ms);
    }

    #[test]
    fn algorithm_filter_keeps_requested_algorithms_in_config_order() {
        let mut config =
            BenchmarkConfig::load(&scenario_config_path("uniform-4-backends").unwrap())
                .expect("scenario config should load");

        filter_algorithms(
            &mut config,
            &["random".to_string(), "power-of-two".to_string()],
        )
        .expect("algorithm filter should succeed");

        assert_eq!(
            config
                .algorithms
                .iter()
                .map(|algorithm| algorithm.name.as_str())
                .collect::<Vec<_>>(),
            vec!["power-of-two", "random"]
        );
    }

    #[test]
    fn algorithm_filter_rejects_unknown_algorithms() {
        let mut config =
            BenchmarkConfig::load(&scenario_config_path("uniform-4-backends").unwrap())
                .expect("scenario config should load");

        let error = filter_algorithms(&mut config, &["missing".to_string()])
            .expect_err("unknown algorithm should fail");

        assert!(error.to_string().contains("unknown algorithm"));
    }

    #[test]
    fn comparison_entry_contains_admission_configuration_and_proof_counters() {
        let mut config =
            BenchmarkConfig::load(&scenario_config_path("uniform-4-backends").unwrap())
                .expect("scenario config should load");
        let mut algorithm = config.algorithms.remove(0);
        algorithm.pylon_queue_admission = Some(crate::config::PylonQueueAdmissionConfig {
            enabled: true,
            min_delta_ms: Some(0),
            tolerance_factor: Some(1.0),
            retry_after_ms: Some(5),
        });
        let mut summary = summarize_with_capacity(&[], std::collections::BTreeMap::new());
        summary.queue_admission_summary = crate::score::QueueAdmissionSummary {
            pylon_rejected_count: 4.0,
            stargate_queue_mismatch_retry_count: 3.0,
            ..Default::default()
        };

        let entry = comparison_entry(&algorithm, &summary);

        assert_eq!(entry["pylon_queue_admission"]["enabled"], true);
        assert_eq!(entry["queue_admission"]["pylon_rejected_count"], 4.0);
        assert_eq!(
            entry["queue_admission"]["stargate_queue_mismatch_retry_count"],
            3.0
        );
    }
}
