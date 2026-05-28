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

use std::env;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum ReliabilityMode {
    Smoke,
    Controlled,
    Strict,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkTier {
    TransportLoopback,
    LocalK8sSmoke,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum DriverMode {
    LocalProcess,
    ExternalNodePort,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RunMetadata {
    pub schema_version: u32,
    pub benchmark_tier: BenchmarkTier,
    pub reliability_mode: ReliabilityMode,
    pub driver_mode: DriverMode,
    pub command_line: Vec<String>,
    pub started_at_unix_seconds: u64,
    pub current_exe: Option<String>,
    pub working_dir: Option<String>,
    pub git: GitMetadata,
    pub rust: RustMetadata,
    pub host: HostMetadata,
    pub kubernetes: KubernetesMetadata,
    pub preflight: PreflightReport,
    pub local_todos: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct GitMetadata {
    pub sha: Option<String>,
    pub branch: Option<String>,
    pub dirty_tracked_files: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RustMetadata {
    pub rustc_version: Option<String>,
    pub target_profile: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct HostMetadata {
    pub hostname: Option<String>,
    pub uname: Option<String>,
    pub cpu_model: Option<String>,
    pub logical_cpus: Option<usize>,
    pub available_parallelism: Option<usize>,
    pub cpu_governor: Option<String>,
    pub turbo_or_boost_state: Option<String>,
    pub aslr_state: Option<String>,
    pub nmi_watchdog_state: Option<String>,
    pub perf_event_paranoid: Option<String>,
    pub load_average: Option<String>,
    pub cgroup_cpu_quota: Option<CgroupCpuQuota>,
    pub cgroup_cpu_limit_cpus: Option<f64>,
    pub process_affinity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CgroupCpuQuota {
    pub source: String,
    pub value: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct KubernetesMetadata {
    pub current_context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreflightReport {
    pub checks: Vec<PreflightCheck>,
    pub warning_count: usize,
    pub failure_count: usize,
    pub should_fail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PreflightCheck {
    pub name: String,
    pub level: PreflightLevel,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum PreflightLevel {
    Ok,
    Warning,
    Failure,
    Unknown,
}

pub fn collect_run_metadata(
    benchmark_tier: BenchmarkTier,
    reliability_mode: ReliabilityMode,
    driver_mode: DriverMode,
) -> RunMetadata {
    let command_line = env::args().collect::<Vec<_>>();
    let current_exe = env::current_exe()
        .ok()
        .map(|path| path.display().to_string());
    let working_dir = env::current_dir()
        .ok()
        .map(|path| path.display().to_string());
    let rust = RustMetadata {
        rustc_version: command_stdout("rustc", &["--version"]),
        target_profile: current_exe.as_deref().and_then(infer_target_profile),
    };
    let git = collect_git_metadata();
    let host = collect_host_metadata();
    let kubernetes = collect_kubernetes_metadata(benchmark_tier);
    let preflight = classify_preflight(benchmark_tier, reliability_mode, &rust, &host, &kubernetes);

    RunMetadata {
        schema_version: 2,
        benchmark_tier,
        reliability_mode,
        driver_mode,
        command_line,
        started_at_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or_default(),
        current_exe,
        working_dir,
        git,
        rust,
        host,
        kubernetes,
        preflight,
        local_todos: local_todos(),
    }
}

pub fn write_run_metadata(path: &Path, metadata: &RunMetadata) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(metadata).context("failed to serialize run metadata")?;
    std::fs::write(path, bytes)
        .with_context(|| format!("failed to write run metadata {}", path.display()))
}

pub fn classify_preflight(
    benchmark_tier: BenchmarkTier,
    reliability_mode: ReliabilityMode,
    rust: &RustMetadata,
    host: &HostMetadata,
    kubernetes: &KubernetesMetadata,
) -> PreflightReport {
    let mut checks = vec![
        release_binary_check(reliability_mode, rust),
        governor_check(reliability_mode, host),
        aslr_check(reliability_mode, host),
        nmi_watchdog_check(reliability_mode, host),
        load_average_check(reliability_mode, host),
    ];
    if benchmark_tier == BenchmarkTier::LocalK8sSmoke {
        checks.push(kubernetes_context_check(reliability_mode, kubernetes));
    }

    let warning_count = checks
        .iter()
        .filter(|check| check.level == PreflightLevel::Warning)
        .count();
    let failure_count = checks
        .iter()
        .filter(|check| check.level == PreflightLevel::Failure)
        .count();
    PreflightReport {
        checks,
        warning_count,
        failure_count,
        should_fail: reliability_mode == ReliabilityMode::Strict && failure_count > 0,
    }
}

fn release_binary_check(mode: ReliabilityMode, rust: &RustMetadata) -> PreflightCheck {
    match rust.target_profile.as_deref() {
        Some("release") => ok(
            "release_binary",
            "benchmark binary appears to be a release build",
        ),
        Some(profile) => degraded(
            mode,
            "release_binary",
            format!("benchmark binary appears to be a {profile} build"),
        ),
        None => unknown("release_binary", "could not infer benchmark binary profile"),
    }
}

fn governor_check(mode: ReliabilityMode, host: &HostMetadata) -> PreflightCheck {
    match host.cpu_governor.as_deref() {
        Some("performance") => ok("cpu_governor", "CPU governor is performance"),
        Some(governor) => degraded(
            mode,
            "cpu_governor",
            format!("CPU governor is {governor}, not performance"),
        ),
        None => unknown("cpu_governor", "could not read CPU governor"),
    }
}

fn aslr_check(mode: ReliabilityMode, host: &HostMetadata) -> PreflightCheck {
    match host.aslr_state.as_deref() {
        Some("0") => ok("aslr", "ASLR is disabled"),
        Some(value) => degraded(mode, "aslr", format!("ASLR state is {value}, not 0")),
        None => unknown("aslr", "could not read ASLR state"),
    }
}

fn nmi_watchdog_check(mode: ReliabilityMode, host: &HostMetadata) -> PreflightCheck {
    match host.nmi_watchdog_state.as_deref() {
        Some("0") => ok("nmi_watchdog", "NMI watchdog is disabled"),
        Some(value) => degraded(
            mode,
            "nmi_watchdog",
            format!("NMI watchdog state is {value}, not 0"),
        ),
        None => unknown("nmi_watchdog", "could not read NMI watchdog state"),
    }
}

fn load_average_check(mode: ReliabilityMode, host: &HostMetadata) -> PreflightCheck {
    let Some(load_average) = &host.load_average else {
        return unknown("load_average", "could not read load average");
    };
    let one_minute = load_average
        .split_whitespace()
        .next()
        .and_then(|value| value.parse::<f64>().ok());
    let Some(one_minute) = one_minute else {
        return unknown("load_average", "could not parse load average");
    };
    let Some(cpu_count) = load_average_cpu_count(host) else {
        return unknown(
            "load_average",
            "could not compare load average without CPU count",
        );
    };
    let threshold = cpu_count * 0.75;
    if one_minute <= threshold {
        ok(
            "load_average",
            format!("1m load average {one_minute:.2} is below threshold {threshold:.2}"),
        )
    } else {
        degraded(
            mode,
            "load_average",
            format!("1m load average {one_minute:.2} exceeds threshold {threshold:.2}"),
        )
    }
}

fn kubernetes_context_check(
    mode: ReliabilityMode,
    kubernetes: &KubernetesMetadata,
) -> PreflightCheck {
    if let Some(context) = &kubernetes.current_context {
        ok(
            "kubernetes_context",
            format!("kubectl context is {context}"),
        )
    } else {
        degraded(mode, "kubernetes_context", "kubectl context is unavailable")
    }
}

fn load_average_cpu_count(host: &HostMetadata) -> Option<f64> {
    let process_cpus = host.available_parallelism.or(host.logical_cpus);
    match (
        process_cpus.map(|value| value as f64),
        host.cgroup_cpu_limit_cpus,
    ) {
        (Some(process_cpus), Some(cgroup_cpus)) if cgroup_cpus.is_finite() && cgroup_cpus > 0.0 => {
            Some(process_cpus.min(cgroup_cpus))
        }
        (Some(process_cpus), _) => Some(process_cpus),
        (None, Some(cgroup_cpus)) if cgroup_cpus.is_finite() && cgroup_cpus > 0.0 => {
            Some(cgroup_cpus)
        }
        _ => None,
    }
}

fn ok(name: &str, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        name: name.to_string(),
        level: PreflightLevel::Ok,
        message: message.into(),
    }
}

fn degraded(mode: ReliabilityMode, name: &str, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        name: name.to_string(),
        level: if mode == ReliabilityMode::Strict {
            PreflightLevel::Failure
        } else {
            PreflightLevel::Warning
        },
        message: message.into(),
    }
}

fn unknown(name: &str, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        name: name.to_string(),
        level: PreflightLevel::Unknown,
        message: message.into(),
    }
}

fn collect_git_metadata() -> GitMetadata {
    GitMetadata {
        sha: command_stdout("git", &["rev-parse", "HEAD"]),
        branch: command_stdout("git", &["branch", "--show-current"]),
        dirty_tracked_files: command_stdout("git", &["status", "--short", "--untracked-files=no"])
            .is_some_and(|status| !status.trim().is_empty()),
    }
}

fn collect_host_metadata() -> HostMetadata {
    HostMetadata {
        hostname: env::var("HOSTNAME")
            .ok()
            .or_else(|| command_stdout("hostname", &[])),
        uname: command_stdout("uname", &["-a"]),
        cpu_model: cpu_model(),
        logical_cpus: logical_cpus(),
        available_parallelism: available_parallelism(),
        cpu_governor: read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        turbo_or_boost_state: read_trimmed("/sys/devices/system/cpu/cpufreq/boost")
            .or_else(|| read_trimmed("/sys/devices/system/cpu/intel_pstate/no_turbo")),
        aslr_state: read_trimmed("/proc/sys/kernel/randomize_va_space"),
        nmi_watchdog_state: read_trimmed("/proc/sys/kernel/nmi_watchdog"),
        perf_event_paranoid: read_trimmed("/proc/sys/kernel/perf_event_paranoid"),
        load_average: read_trimmed("/proc/loadavg"),
        cgroup_cpu_quota: cgroup_cpu_quota(),
        cgroup_cpu_limit_cpus: cgroup_cpu_limit_cpus(),
        process_affinity: command_stdout("taskset", &["-pc", &std::process::id().to_string()]),
    }
}

fn collect_kubernetes_metadata(benchmark_tier: BenchmarkTier) -> KubernetesMetadata {
    if benchmark_tier == BenchmarkTier::LocalK8sSmoke {
        KubernetesMetadata {
            current_context: command_stdout("kubectl", &["config", "current-context"]),
        }
    } else {
        KubernetesMetadata::default()
    }
}

fn cgroup_cpu_quota() -> Option<CgroupCpuQuota> {
    read_trimmed("/sys/fs/cgroup/cpu.max")
        .map(|value| CgroupCpuQuota {
            source: "cgroup_v2_cpu.max".to_string(),
            value,
        })
        .or_else(|| {
            read_trimmed("/sys/fs/cgroup/cpu/cpu.cfs_quota_us").map(|value| CgroupCpuQuota {
                source: "cgroup_v1_cpu.cfs_quota_us".to_string(),
                value,
            })
        })
}

fn cgroup_cpu_limit_cpus() -> Option<f64> {
    read_trimmed("/sys/fs/cgroup/cpu.max")
        .and_then(|value| parse_cgroup_v2_cpu_limit(&value))
        .or_else(|| {
            let quota = read_trimmed("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")?;
            let period = read_trimmed("/sys/fs/cgroup/cpu/cpu.cfs_period_us")?;
            parse_quota_period_cpu_limit(&quota, &period)
        })
}

fn parse_cgroup_v2_cpu_limit(value: &str) -> Option<f64> {
    let mut parts = value.split_whitespace();
    let quota = parts.next()?;
    let period = parts.next()?;
    parse_quota_period_cpu_limit(quota, period)
}

fn parse_quota_period_cpu_limit(quota: &str, period: &str) -> Option<f64> {
    if quota == "max" {
        return None;
    }
    let quota = quota.parse::<f64>().ok()?;
    let period = period.parse::<f64>().ok()?;
    (quota > 0.0 && period > 0.0).then_some(quota / period)
}

fn cpu_model() -> Option<String> {
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        (name.trim() == "model name").then(|| value.trim().to_string())
    })
}

fn logical_cpus() -> Option<usize> {
    let count = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .map(|cpuinfo| {
            cpuinfo
                .lines()
                .filter(|line| {
                    line.split_once(':')
                        .is_some_and(|(name, _)| name.trim() == "processor")
                })
                .count()
        })
        .unwrap_or_default();
    (count > 0).then_some(count)
}

fn available_parallelism() -> Option<usize> {
    std::thread::available_parallelism()
        .ok()
        .map(|value| value.get())
}

fn infer_target_profile(exe: &str) -> Option<String> {
    if exe.contains("/target/release/") || exe.contains("\\target\\release\\") {
        Some("release".to_string())
    } else if exe.contains("/target/debug/") || exe.contains("\\target\\debug\\") {
        Some("debug".to_string())
    } else {
        None
    }
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn local_todos() -> Vec<String> {
    vec![
        "TODO(LINF-135): implement in-cluster driver mode after it can be run against a local or dedicated Kubernetes benchmark cluster.".to_string(),
        "TODO(LINF-135): implement repeated-trial orchestration for Kubernetes benchmarks after the run lifecycle can be validated end-to-end on a cluster.".to_string(),
        "TODO(LINF-135): implement privileged network shaping and calibration only in an environment where tc/netem setup can be validated end-to-end.".to_string(),
        "TODO(LINF-135): implement host CPU/governor/turbo mutation wrapper after reversible setup and restore can be tested on benchmark hardware.".to_string(),
        "TODO(LINF-135): implement representative multi-node benchmark tier on dedicated benchmark infrastructure, not in local Kind smoke mode.".to_string(),
        "TODO(LINF-135): promote reliable Kubernetes scenario configs after in-cluster drivers and network profiles are implemented.".to_string(),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stable_host() -> HostMetadata {
        HostMetadata {
            logical_cpus: Some(8),
            available_parallelism: Some(8),
            cpu_governor: Some("performance".to_string()),
            aslr_state: Some("0".to_string()),
            nmi_watchdog_state: Some("0".to_string()),
            load_average: Some("1.00 0.50 0.25 1/100 42".to_string()),
            ..HostMetadata::default()
        }
    }

    #[test]
    fn strict_preflight_fails_for_uncontrolled_host() {
        let host = HostMetadata {
            logical_cpus: Some(4),
            cpu_governor: Some("powersave".to_string()),
            aslr_state: Some("2".to_string()),
            nmi_watchdog_state: Some("1".to_string()),
            load_average: Some("10.00 8.00 4.00 1/100 42".to_string()),
            ..HostMetadata::default()
        };
        let report = classify_preflight(
            BenchmarkTier::TransportLoopback,
            ReliabilityMode::Strict,
            &RustMetadata {
                target_profile: Some("debug".to_string()),
                ..RustMetadata::default()
            },
            &host,
            &KubernetesMetadata::default(),
        );

        assert!(report.should_fail);
        assert!(report.failure_count >= 4);
    }

    #[test]
    fn smoke_preflight_warns_without_failing() {
        let host = HostMetadata {
            cpu_governor: Some("powersave".to_string()),
            ..stable_host()
        };
        let report = classify_preflight(
            BenchmarkTier::TransportLoopback,
            ReliabilityMode::Smoke,
            &RustMetadata {
                target_profile: Some("debug".to_string()),
                ..RustMetadata::default()
            },
            &host,
            &KubernetesMetadata::default(),
        );

        assert!(!report.should_fail);
        assert_eq!(report.failure_count, 0);
        assert!(report.warning_count >= 1);
    }

    #[test]
    fn controlled_k8s_preflight_records_missing_context_warning() {
        let report = classify_preflight(
            BenchmarkTier::LocalK8sSmoke,
            ReliabilityMode::Controlled,
            &RustMetadata {
                target_profile: Some("release".to_string()),
                ..RustMetadata::default()
            },
            &stable_host(),
            &KubernetesMetadata::default(),
        );

        assert!(!report.should_fail);
        assert!(report.warning_count >= 1);
        assert!(
            report
                .checks
                .iter()
                .any(|check| check.name == "kubernetes_context")
        );
    }

    #[test]
    fn transport_loopback_preflight_excludes_kubernetes_context() {
        let report = classify_preflight(
            BenchmarkTier::TransportLoopback,
            ReliabilityMode::Strict,
            &RustMetadata {
                target_profile: Some("release".to_string()),
                ..RustMetadata::default()
            },
            &stable_host(),
            &KubernetesMetadata::default(),
        );

        assert!(!report.should_fail);
        assert!(
            report
                .checks
                .iter()
                .all(|check| check.name != "kubernetes_context")
        );
    }

    #[test]
    fn strict_load_preflight_uses_available_parallelism() {
        let host = HostMetadata {
            logical_cpus: Some(64),
            available_parallelism: Some(2),
            load_average: Some("2.00 1.00 0.50 1/100 42".to_string()),
            ..stable_host()
        };
        let report = classify_preflight(
            BenchmarkTier::TransportLoopback,
            ReliabilityMode::Strict,
            &RustMetadata {
                target_profile: Some("release".to_string()),
                ..RustMetadata::default()
            },
            &host,
            &KubernetesMetadata::default(),
        );

        let load_check = report
            .checks
            .iter()
            .find(|check| check.name == "load_average")
            .expect("load_average check should exist");
        assert_eq!(load_check.level, PreflightLevel::Failure);
        assert!(load_check.message.contains("threshold 1.50"));
        assert!(report.should_fail);
    }

    #[test]
    fn strict_load_preflight_uses_cgroup_cpu_limit() {
        let host = HostMetadata {
            logical_cpus: Some(64),
            available_parallelism: Some(32),
            cgroup_cpu_limit_cpus: Some(2.0),
            load_average: Some("2.00 1.00 0.50 1/100 42".to_string()),
            ..stable_host()
        };
        let report = classify_preflight(
            BenchmarkTier::TransportLoopback,
            ReliabilityMode::Strict,
            &RustMetadata {
                target_profile: Some("release".to_string()),
                ..RustMetadata::default()
            },
            &host,
            &KubernetesMetadata::default(),
        );

        let load_check = report
            .checks
            .iter()
            .find(|check| check.name == "load_average")
            .expect("load_average check should exist");
        assert_eq!(load_check.level, PreflightLevel::Failure);
        assert!(load_check.message.contains("threshold 1.50"));
        assert!(report.should_fail);
    }

    #[test]
    fn parses_cgroup_cpu_limits() {
        assert_eq!(parse_cgroup_v2_cpu_limit("200000 100000"), Some(2.0));
        assert_eq!(parse_cgroup_v2_cpu_limit("max 100000"), None);
        assert_eq!(parse_quota_period_cpu_limit("50000", "100000"), Some(0.5));
        assert_eq!(parse_quota_period_cpu_limit("-1", "100000"), None);
    }
}
