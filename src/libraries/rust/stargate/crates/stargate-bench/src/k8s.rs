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

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread::sleep;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};

use crate::config::{AlgorithmConfig, BenchmarkConfig};

#[derive(Clone)]
pub struct BenchmarkK8sRun {
    pub algorithm_name: String,
    pub manifest_path: PathBuf,
    pub run_dir: PathBuf,
    pub stargate_ns: String,
    pub backends_ns: String,
    pub stargate_count: usize,
    pub nodeport_host: String,
    pub stargate_http_endpoint: String,
    pub stargate_metrics_endpoint: String,
    pub collector_metrics_endpoint: String,
}

struct ImageRefs {
    stargate: String,
    mock_dynamo: String,
    pylon: String,
}

pub fn prepare_benchmark_k8s_run(
    config: &BenchmarkConfig,
    algorithm: &AlgorithmConfig,
    manifest_path: &Path,
    output_dir: &Path,
    run_index: usize,
) -> anyhow::Result<BenchmarkK8sRun> {
    let run_slug = slugify(&algorithm.name);
    let run_dir = output_dir.join(format!("run-{run_slug}"));
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create run dir {}", run_dir.display()))?;

    let stargate_ns = format!("sgbench-sg-{run_slug}");
    let backends_ns = format!("sgbench-be-{run_slug}");
    let http_node_port = 30080 + run_index as u16;
    let metrics_node_port = 31080 + run_index as u16;
    let collector_metrics_node_port = 32080 + run_index as u16;
    let service_host = resolve_nodeport_host()?;

    config.validate()?;
    let image_refs = resolve_image_refs()?;
    let lb_config_json = serde_json::to_string_pretty(&algorithm.config)
        .with_context(|| format!("failed to serialize LB config for {}", algorithm.name))?;
    let manifests = render_manifest(RenderManifestConfig {
        config,
        algorithm,
        image_refs: &image_refs,
        stargate_ns: &stargate_ns,
        backends_ns: &backends_ns,
        lb_config_json: &lb_config_json,
        http_node_port,
        metrics_node_port,
        collector_metrics_node_port,
    });
    let manifest_out = run_dir.join("k8s-manifest.yaml");
    let stargate_manifest_out = run_dir.join("k8s-stargate-manifest.yaml");
    let backends_manifest_out = run_dir.join("k8s-backends-manifest.yaml");
    fs::write(
        &manifest_out,
        format!("{}{}", manifests.stargate, manifests.backends),
    )
    .with_context(|| format!("failed to write {}", manifest_out.display()))?;
    fs::write(&stargate_manifest_out, manifests.stargate)
        .with_context(|| format!("failed to write {}", stargate_manifest_out.display()))?;
    fs::write(&backends_manifest_out, manifests.backends)
        .with_context(|| format!("failed to write {}", backends_manifest_out.display()))?;

    let run_info = serde_json::json!({
        "algorithm_name": algorithm.name,
        "stargate_namespace": stargate_ns,
        "backends_namespace": backends_ns,
        "k8s_manifest_path": manifest_out,
        "stargate_k8s_manifest_path": stargate_manifest_out,
        "backends_k8s_manifest_path": backends_manifest_out,
        "http_node_port": http_node_port,
        "metrics_node_port": metrics_node_port,
        "collector_metrics_node_port": collector_metrics_node_port,
        "stargate_http_endpoint": format!("http://{service_host}:{http_node_port}"),
        "stargate_metrics_endpoint": format!("http://{service_host}:{metrics_node_port}/metrics"),
        "collector_metrics_endpoint": format!("http://{service_host}:{collector_metrics_node_port}/metrics"),
        "pylon_queue_admission": algorithm.pylon_queue_admission,
        "manifest_path": manifest_path,
    });
    fs::write(
        run_dir.join("run-info.json"),
        serde_json::to_vec_pretty(&run_info).context("failed to serialize run info")?,
    )
    .with_context(|| {
        format!(
            "failed to write {}",
            run_dir.join("run-info.json").display()
        )
    })?;

    Ok(BenchmarkK8sRun {
        algorithm_name: algorithm.name.clone(),
        manifest_path: manifest_path.to_path_buf(),
        run_dir,
        stargate_ns,
        backends_ns,
        stargate_count: config.stargates.count,
        nodeport_host: service_host.clone(),
        stargate_http_endpoint: format!("http://{service_host}:{http_node_port}"),
        stargate_metrics_endpoint: format!("http://{service_host}:{metrics_node_port}/metrics"),
        collector_metrics_endpoint: format!(
            "http://{service_host}:{collector_metrics_node_port}/metrics"
        ),
    })
}

pub fn apply(run: &BenchmarkK8sRun) -> anyhow::Result<()> {
    wait_for_namespace_reuse(&run.stargate_ns, Duration::from_secs(60))?;
    wait_for_namespace_reuse(&run.backends_ns, Duration::from_secs(60))?;
    kubectl_apply(&run.run_dir.join("k8s-stargate-manifest.yaml"), || {
        format!("stargate resources for {}", run.algorithm_name)
    })
}

pub fn delete(run: &BenchmarkK8sRun) -> anyhow::Result<()> {
    for path in [
        run.run_dir.join("k8s-backends-manifest.yaml"),
        run.run_dir.join("stargate-external-services.yaml"),
        run.run_dir.join("k8s-stargate-manifest.yaml"),
    ] {
        kubectl_delete(&path, || {
            format!("k8s benchmark resources for {}", run.algorithm_name)
        })?;
    }
    Ok(())
}

pub fn collect_logs(run: &BenchmarkK8sRun) -> anyhow::Result<()> {
    let logs_dir = run.run_dir.join("logs");
    fs::create_dir_all(&logs_dir)
        .with_context(|| format!("failed to create logs dir {}", logs_dir.display()))?;

    collect_namespace_snapshot(&logs_dir, "stargate", &run.stargate_ns)?;
    collect_namespace_snapshot(&logs_dir, "backends", &run.backends_ns)?;
    collect_labeled_logs(&logs_dir, "stargate", &run.stargate_ns, "app=stargate")?;
    for backend_index in 0.. {
        let inference_selector = format!("app=backend-{backend_index}-inference-server");
        let client_selector = format!("app=backend-{backend_index}-pylon");
        let inference = collect_labeled_logs(
            &logs_dir,
            &format!("backend-{backend_index}-inference-server"),
            &run.backends_ns,
            &inference_selector,
        )?;
        let client = collect_labeled_logs(
            &logs_dir,
            &format!("backend-{backend_index}-pylon"),
            &run.backends_ns,
            &client_selector,
        )?;
        if !inference && !client {
            break;
        }
    }
    Ok(())
}

pub fn delete_backend_pod(run: &BenchmarkK8sRun, backend_index: usize) -> anyhow::Result<()> {
    let selector = format!("app=backend-{backend_index}-inference-server");
    let status = Command::new("kubectl")
        .arg("-n")
        .arg(&run.backends_ns)
        .arg("delete")
        .arg("pod")
        .arg("-l")
        .arg(&selector)
        .status()
        .with_context(|| format!("failed to delete backend pod for backend-{backend_index}"))?;
    if !status.success() {
        bail!("kubectl delete pod failed for selector {selector}");
    }
    Ok(())
}

pub fn scale_backend(
    run: &BenchmarkK8sRun,
    backend_index: usize,
    replicas: u32,
) -> anyhow::Result<()> {
    let deployment = format!("deployment/backend-{backend_index}-inference-server");
    let status = Command::new("kubectl")
        .arg("-n")
        .arg(&run.backends_ns)
        .arg("scale")
        .arg(&deployment)
        .arg(format!("--replicas={replicas}"))
        .status()
        .with_context(|| format!("failed to scale {deployment}"))?;
    if !status.success() {
        bail!("kubectl scale failed for {deployment}");
    }
    Ok(())
}

fn kubectl_apply(path: &Path, description: impl FnOnce() -> String) -> anyhow::Result<()> {
    let status = Command::new("kubectl")
        .arg("apply")
        .arg("-f")
        .arg(path)
        .status()
        .context("failed to run kubectl apply")?;
    if !status.success() {
        bail!("kubectl apply failed for {}", description());
    }
    Ok(())
}

fn kubectl_delete(path: &Path, description: impl FnOnce() -> String) -> anyhow::Result<()> {
    if !path.exists() {
        return Ok(());
    }

    let status = Command::new("kubectl")
        .arg("delete")
        .arg("-f")
        .arg(path)
        .arg("--ignore-not-found=true")
        .status()
        .context("failed to run kubectl delete")?;
    if !status.success() {
        bail!("kubectl delete failed for {}", description());
    }
    Ok(())
}

fn collect_namespace_snapshot(logs_dir: &Path, name: &str, namespace: &str) -> anyhow::Result<()> {
    write_kubectl_output(
        logs_dir.join(format!("{name}-pods.txt")),
        Command::new("kubectl")
            .arg("-n")
            .arg(namespace)
            .arg("get")
            .arg("pods")
            .arg("-o")
            .arg("wide")
            .output(),
    )?;
    write_kubectl_output(
        logs_dir.join(format!("{name}-describe-pods.txt")),
        Command::new("kubectl")
            .arg("-n")
            .arg(namespace)
            .arg("describe")
            .arg("pods")
            .output(),
    )?;
    write_kubectl_output(
        logs_dir.join(format!("{name}-events.txt")),
        Command::new("kubectl")
            .arg("-n")
            .arg(namespace)
            .arg("get")
            .arg("events")
            .arg("--sort-by=.lastTimestamp")
            .output(),
    )?;
    Ok(())
}

fn collect_labeled_logs(
    logs_dir: &Path,
    name: &str,
    namespace: &str,
    selector: &str,
) -> anyhow::Result<bool> {
    let pods = pods_for_selector(namespace, selector)?;
    if pods.is_empty() {
        return Ok(false);
    }
    write_kubectl_output(
        logs_dir.join(format!("{name}.log")),
        Command::new("kubectl")
            .arg("-n")
            .arg(namespace)
            .arg("logs")
            .arg("-l")
            .arg(selector)
            .arg("--all-containers=true")
            .arg("--prefix=true")
            .arg("--tail=-1")
            .output(),
    )?;
    Ok(true)
}

fn pods_for_selector(namespace: &str, selector: &str) -> anyhow::Result<Vec<String>> {
    let output = Command::new("kubectl")
        .arg("-n")
        .arg(namespace)
        .arg("get")
        .arg("pods")
        .arg("-l")
        .arg(selector)
        .arg("-o")
        .arg("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
        .output()
        .with_context(|| format!("failed to query pods for selector {selector}"))?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect())
}

fn write_kubectl_output(
    path: impl AsRef<Path>,
    output: std::io::Result<Output>,
) -> anyhow::Result<()> {
    let path = path.as_ref();
    let output = output.with_context(|| format!("failed to run kubectl for {}", path.display()))?;
    let mut text = String::new();
    text.push_str(&format!("status: {}\n\n", output.status));
    if !output.stdout.is_empty() {
        text.push_str("stdout:\n");
        text.push_str(&String::from_utf8_lossy(&output.stdout));
        text.push('\n');
    }
    if !output.stderr.is_empty() {
        text.push_str("stderr:\n");
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        text.push('\n');
    }
    fs::write(path, text).with_context(|| format!("failed to write {}", path.display()))
}

pub fn wait_ready(run: &BenchmarkK8sRun, backend_count: usize) -> anyhow::Result<()> {
    rollout("statefulset", "stargate", &run.stargate_ns)?;
    apply_stargate_external_services(run)?;
    kubectl_apply(&run.run_dir.join("k8s-backends-manifest.yaml"), || {
        format!("backend resources for {}", run.algorithm_name)
    })?;
    for backend_index in 0..backend_count {
        rollout(
            "deployment",
            &format!("backend-{backend_index}-inference-server"),
            &run.backends_ns,
        )?;
        rollout(
            "deployment",
            &format!("backend-{backend_index}-pylon"),
            &run.backends_ns,
        )?;
    }
    Ok(())
}

pub fn stargate_metrics_endpoints(run: &BenchmarkK8sRun) -> anyhow::Result<Vec<String>> {
    let output = Command::new("kubectl")
        .arg("-n")
        .arg(&run.stargate_ns)
        .arg("get")
        .arg("services")
        .arg("-l")
        .arg("benchmark.stargate/role=pod-metrics")
        .arg("-o")
        .arg("jsonpath={range .items[*]}{.metadata.name}{\" \"}{.spec.ports[0].nodePort}{\"\\n\"}{end}")
        .output()
        .context("failed to query per-stargate metrics services")?;
    if !output.status.success() {
        bail!("kubectl get services failed while querying per-stargate metrics endpoints");
    }

    let mut endpoints = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let mut fields = line.split_whitespace();
            let name = fields
                .next()
                .context("missing per-stargate metrics service name")?;
            let node_port = fields
                .next()
                .context("missing per-stargate metrics NodePort")?;
            if fields.next().is_some() {
                bail!("unexpected per-stargate metrics service record: {line}");
            }
            Ok((
                name.to_string(),
                format!("http://{}:{node_port}/metrics", run.nodeport_host),
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    endpoints.sort_by(|left, right| left.0.cmp(&right.0));

    if endpoints.len() != run.stargate_count {
        bail!(
            "expected {} per-stargate metrics endpoints but found {}",
            run.stargate_count,
            endpoints.len()
        );
    }

    Ok(endpoints
        .into_iter()
        .map(|(_, endpoint)| endpoint)
        .collect())
}

fn apply_stargate_external_services(run: &BenchmarkK8sRun) -> anyhow::Result<()> {
    let pods = list_stargate_pods(run)?;
    if pods.len() != run.stargate_count {
        bail!(
            "expected {} stargate pods but found {}",
            run.stargate_count,
            pods.len()
        );
    }

    let manifest = render_stargate_external_services(&run.stargate_ns, &pods);

    let manifest_path = run.run_dir.join("stargate-external-services.yaml");
    fs::write(&manifest_path, manifest)
        .with_context(|| format!("failed to write {}", manifest_path.display()))?;

    let status = Command::new("kubectl")
        .arg("apply")
        .arg("-f")
        .arg(&manifest_path)
        .status()
        .context("failed to apply stargate external services")?;
    if !status.success() {
        bail!("kubectl apply failed for stargate external services");
    }
    Ok(())
}

fn render_stargate_external_services(namespace: &str, pods: &[StargatePod]) -> String {
    let mut manifest = String::new();
    for pod in pods {
        let service_name = format!("{}-external", pod.name);
        manifest.push_str(&format!(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: {service_name}\n  namespace: {namespace}\nspec:\n  selector:\n    statefulset.kubernetes.io/pod-name: {pod_name}\n  ports:\n    - name: grpc\n      port: 50071\n      targetPort: grpc\n    - name: http\n      port: 8000\n      targetPort: http\n    - name: reverse\n      port: 50072\n      targetPort: reverse\n      protocol: UDP\n---\n",
            pod_name = pod.name,
        ));
        let metrics_service_name = format!("{}-metrics", pod.name);
        manifest.push_str(&format!(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: {metrics_service_name}\n  namespace: {namespace}\n  labels:\n    benchmark.stargate/role: pod-metrics\nspec:\n  type: NodePort\n  selector:\n    statefulset.kubernetes.io/pod-name: {pod_name}\n  ports:\n    - name: metrics\n      port: 9090\n      targetPort: metrics\n---\n",
            pod_name = pod.name,
        ));
    }
    manifest
}

struct StargatePod {
    name: String,
}

fn list_stargate_pods(run: &BenchmarkK8sRun) -> anyhow::Result<Vec<StargatePod>> {
    let output = Command::new("kubectl")
        .arg("-n")
        .arg(&run.stargate_ns)
        .arg("get")
        .arg("pods")
        .arg("-l")
        .arg("app=stargate")
        .arg("-o")
        .arg("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}")
        .output()
        .context("failed to query stargate pods")?;
    if !output.status.success() {
        bail!("kubectl get pods failed while querying stargate pods");
    }

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            Ok(StargatePod {
                name: line.to_string(),
            })
        })
        .collect()
}

fn rollout(kind: &str, name: &str, namespace: &str) -> anyhow::Result<()> {
    let status = Command::new("kubectl")
        .arg("-n")
        .arg(namespace)
        .arg("rollout")
        .arg("status")
        .arg(format!("{kind}/{name}"))
        .arg("--timeout=180s")
        .status()
        .with_context(|| format!("failed waiting for rollout of {kind}/{name}"))?;
    if !status.success() {
        bail!("rollout failed for {kind}/{name} in namespace {namespace}");
    }
    Ok(())
}

fn wait_for_namespace_reuse(namespace: &str, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let output = Command::new("kubectl")
            .arg("get")
            .arg("namespace")
            .arg(namespace)
            .arg("-o")
            .arg("jsonpath={.status.phase}")
            .output()
            .with_context(|| format!("failed to query namespace {namespace}"))?;
        if !output.status.success() {
            return Ok(());
        }

        let phase = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if phase != "Terminating" {
            return Ok(());
        }

        if Instant::now() >= deadline {
            bail!("timed out waiting for namespace {namespace} to finish terminating");
        }
        sleep(Duration::from_millis(500));
    }
}

fn resolve_nodeport_host() -> anyhow::Result<String> {
    if let Ok(host) = std::env::var("STARGATE_BENCH_NODE_HOST") {
        let host = host.trim();
        if !host.is_empty() {
            return Ok(host.to_string());
        }
    }

    let context = Command::new("kubectl")
        .arg("config")
        .arg("current-context")
        .output()
        .context("failed to query current kubectl context")?;
    if context.status.success()
        && String::from_utf8_lossy(&context.stdout).trim() == "docker-desktop"
    {
        return Ok("127.0.0.1".to_string());
    }

    let external = query_first_node_address("ExternalIP")?;
    if let Some(address) = external {
        return Ok(address);
    }

    let internal = query_first_node_address("InternalIP")?;
    internal.ok_or_else(|| anyhow::anyhow!("failed to resolve Kubernetes node address for NodePort access; set STARGATE_BENCH_NODE_HOST"))
}

fn query_first_node_address(address_type: &str) -> anyhow::Result<Option<String>> {
    let output = Command::new("kubectl")
        .arg("get")
        .arg("nodes")
        .arg("-o")
        .arg(format!(
            "jsonpath={{.items[0].status.addresses[?(@.type==\"{address_type}\")].address}}"
        ))
        .output()
        .with_context(|| format!("failed to query Kubernetes node {address_type} address"))?;
    if !output.status.success() {
        bail!("kubectl get nodes failed while resolving NodePort host");
    }
    let address = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok((!address.is_empty()).then_some(address))
}

struct RenderedManifests {
    stargate: String,
    backends: String,
}

struct RenderManifestConfig<'a> {
    config: &'a BenchmarkConfig,
    algorithm: &'a AlgorithmConfig,
    image_refs: &'a ImageRefs,
    stargate_ns: &'a str,
    backends_ns: &'a str,
    lb_config_json: &'a str,
    http_node_port: u16,
    metrics_node_port: u16,
    collector_metrics_node_port: u16,
}

fn render_manifest(render: RenderManifestConfig<'_>) -> RenderedManifests {
    let config = render.config;
    let image_refs = render.image_refs;
    let stargate_ns = render.stargate_ns;
    let backends_ns = render.backends_ns;
    let mut stargate = String::new();
    stargate.push_str(&format!(
        "apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {stargate_ns}\n---\napiVersion: v1\nkind: Namespace\nmetadata:\n  name: {backends_ns}\n---\n"
    ));
    stargate.push_str(&format!(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: stargate-lb-config\n  namespace: {stargate_ns}\ndata:\n  lb-config.json: |\n"
    ));
    for line in render.lb_config_json.lines() {
        stargate.push_str("    ");
        stargate.push_str(line);
        stargate.push('\n');
    }

    stargate.push_str(&format!(
        "---\napiVersion: v1\nkind: Service\nmetadata:\n  name: stargate\n  namespace: {stargate_ns}\nspec:\n  selector:\n    app: stargate\n  ports:\n    - name: grpc\n      port: 50071\n      targetPort: grpc\n    - name: reverse\n      port: 50072\n      targetPort: reverse\n      protocol: UDP\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: stargate-http\n  namespace: {stargate_ns}\nspec:\n  type: NodePort\n  selector:\n    app: stargate\n  ports:\n    - name: http\n      port: 8000\n      targetPort: http\n      nodePort: {http_node_port}\n    - name: metrics\n      port: 9090\n      targetPort: metrics\n      nodePort: {metrics_node_port}\n---\napiVersion: v1\nkind: Service\nmetadata:\n  name: stargate-headless\n  namespace: {stargate_ns}\nspec:\n  clusterIP: None\n  selector:\n    app: stargate\n  ports:\n    - name: http\n      port: 8000\n      targetPort: http\n    - name: metrics\n      port: 9090\n      targetPort: metrics\n    - name: grpc\n      port: 50071\n      targetPort: grpc\n    - name: reverse\n      port: 50072\n      targetPort: reverse\n      protocol: UDP\n---\napiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: stargate\n  namespace: {stargate_ns}\nspec:\n  serviceName: stargate-headless\n  replicas: {stargate_count}\n  selector:\n    matchLabels:\n      app: stargate\n  template:\n    metadata:\n      labels:\n        app: stargate\n    spec:\n      containers:\n        - name: stargate\n          image: {stargate_image}\n          imagePullPolicy: IfNotPresent\n          args:\n            - --stargate-id=$(POD_NAME)\n            - --listen-addr=0.0.0.0:50071\n            - --model-discovery-listen-addr=0.0.0.0:50073\n            - --http-listen-addr=0.0.0.0:8000\n            - --advertise-addr=$(POD_IP):50071\n            - --stargate-discovery-dns-name=stargate-headless.{stargate_ns}.svc.cluster.local\n            - --advertised-hostname-template={{pod_name}}-external.{stargate_ns}.svc.cluster.local\n            - --pod-name=$(POD_NAME)\n            - --pod-namespace=$(POD_NAMESPACE)\n            - --metrics-port=9090\n            - --lb-config-path=/config/lb-config.json\n            - --reverse-tunnel-listen-addr=0.0.0.0:50072\n            - --quic-insecure\n            - --tunnel-protocol={tunnel_protocol}\n          env:\n            - name: POD_NAME\n              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.name\n            - name: POD_NAMESPACE\n              valueFrom:\n                fieldRef:\n                  fieldPath: metadata.namespace\n            - name: POD_IP\n              valueFrom:\n                fieldRef:\n                  fieldPath: status.podIP\n          ports:\n            - name: grpc\n              containerPort: 50071\n            - name: model-discovery\n              containerPort: 50073\n            - name: reverse\n              containerPort: 50072\n              protocol: UDP\n            - name: http\n              containerPort: 8000\n            - name: metrics\n              containerPort: 9090\n          readinessProbe:\n            httpGet:\n              path: /readyz\n              port: http\n            initialDelaySeconds: 2\n            periodSeconds: 2\n          livenessProbe:\n            httpGet:\n              path: /healthz\n              port: http\n            initialDelaySeconds: 5\n            periodSeconds: 5\n          volumeMounts:\n            - name: lb-config\n              mountPath: /config\n      volumes:\n        - name: lb-config\n          configMap:\n            name: stargate-lb-config\n---\n",
        http_node_port = render.http_node_port,
        metrics_node_port = render.metrics_node_port,
        stargate_count = config.stargates.count,
        stargate_image = image_refs.stargate,
        tunnel_protocol = config.tunnel_protocol.as_arg(),
    ));
    stargate.push_str(&render_otel_collector(
        stargate_ns,
        backends_ns,
        render.collector_metrics_node_port,
    ));

    let pylon_queue_admission_args = render
        .algorithm
        .pylon_queue_admission
        .as_ref()
        .map(|admission| {
            admission
                .pylon_args()
                .into_iter()
                .map(|arg| format!("            - {arg}\n"))
                .collect::<String>()
        })
        .unwrap_or_default();
    let mut backends = String::new();
    for backend_index in 0..config.backends.count {
        let profile = config.backends.profile_for_index(backend_index);
        let ttft = profile.service_time_ms.ttft_mean;
        let ttft_jitter = profile.service_time_ms.ttft_jitter_ms;
        let decode_jitter = profile.service_time_ms.decode_jitter_ms;
        let prefill_tps = profile.service_time_ms.prefill_tokens_per_s.unwrap_or(0.0);
        let max_concurrent_requests = profile.max_concurrent_requests.unwrap_or(0);
        let kv_cache_capacity_tokens = profile.kv_cache_capacity_tokens;
        let last_mean_input_tps = profile.registration.last_mean_input_tps;
        let decode_tps = profile.service_time_ms.decode_tokens_per_s;
        // The mock backend delay is millisecond-granular, so rates above 1000 TPS floor at 1 ms.
        let per_token_delay_ms = (1000 / decode_tps).max(1);
        let cluster_id_arg = config
            .backends
            .cluster_id_for_index(backend_index)
            .map(|cluster_id| format!("            - --cluster-id={cluster_id}\n"))
            .unwrap_or_default();
        backends.push_str(&format!(
            "apiVersion: v1\nkind: Service\nmetadata:\n  name: backend-{backend_index}-http\n  namespace: {backends_ns}\nspec:\n  selector:\n    app: backend-{backend_index}-inference-server\n  ports:\n    - port: 8090\n      targetPort: http\n      name: http\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: backend-{backend_index}-inference-server\n  namespace: {backends_ns}\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: backend-{backend_index}-inference-server\n  template:\n    metadata:\n      labels:\n        app: backend-{backend_index}-inference-server\n        benchmark.stargate/profile: {profile_name}\n    spec:\n      containers:\n        - name: inference-server\n          image: {mock_dynamo_image}\n          imagePullPolicy: IfNotPresent\n          args:\n            - --http-listen-addr=0.0.0.0:8090\n            - --model-name={model}\n            - --num-tokens=32\n            - --token-delay-ms={per_token_delay_ms}\n            - --decode-jitter-ms={decode_jitter}\n            - --ttft-ms={ttft}\n            - --ttft-jitter-ms={ttft_jitter}\n            - --prefill-tokens-per-s={prefill_tps}\n            - --max-concurrent-requests={max_concurrent_requests}\n            - --kv-cache-capacity-tokens={kv_cache_capacity_tokens}\n          ports:\n            - containerPort: 8090\n              name: http\n          readinessProbe:\n            httpGet:\n              path: /health\n              port: http\n            initialDelaySeconds: 2\n            periodSeconds: 2\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: backend-{backend_index}-pylon\n  namespace: {backends_ns}\nspec:\n  replicas: 1\n  selector:\n    matchLabels:\n      app: backend-{backend_index}-pylon\n  template:\n    metadata:\n      labels:\n        app: backend-{backend_index}-pylon\n        benchmark.stargate/profile: {profile_name}\n    spec:\n      containers:\n        - name: pylon\n          image: {pylon_image}\n          imagePullPolicy: IfNotPresent\n          args:\n            - --upstream-http-base-url=http://backend-{backend_index}-http.{backends_ns}.svc.cluster.local:8090\n            - --model-name={model}\n            - --stargate-address=stargate.{stargate_ns}.svc.cluster.local:50071\n            - --inference-server-id=backend-{backend_index}\n{cluster_id_arg}            - --reverse-tunnel\n            - --quic-insecure\n            - --tunnel-protocol={tunnel_protocol}\n            - --kv-cache-stats-path=/kv-cache/stats\n            - --min-update-interval-ms=100\n            - --disable-bringup\n            - --active-canary-interval-ms=0\n            - --benchmark-fixed-last-mean-input-tps={last_mean_input_tps}\n",
            mock_dynamo_image = image_refs.mock_dynamo,
            pylon_image = image_refs.pylon,
            model = config.model,
            profile_name = slugify(&profile.name),
            tunnel_protocol = config.tunnel_protocol.as_arg(),
        ));
        backends.push_str(&pylon_queue_admission_args);
        backends.push_str("---\n");
    }

    RenderedManifests { stargate, backends }
}

fn render_otel_collector(
    stargate_ns: &str,
    backends_ns: &str,
    collector_metrics_node_port: u16,
) -> String {
    r#"apiVersion: v1
kind: ServiceAccount
metadata:
  name: otel-collector
  namespace: __STARGATE_NS__
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: otel-collector-__STARGATE_NS__
rules:
  - apiGroups: [""]
    resources: ["pods", "namespaces", "endpoints", "services"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: otel-collector-__STARGATE_NS__
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: otel-collector-__STARGATE_NS__
subjects:
  - kind: ServiceAccount
    name: otel-collector
    namespace: __STARGATE_NS__
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-config
  namespace: __STARGATE_NS__
data:
  otel-collector.yaml: |
    receivers:
      prometheus:
        config:
          scrape_configs:
            - job_name: stargate
              scrape_interval: 1s
              kubernetes_sd_configs:
                - role: pod
                  namespaces:
                    names: ["__STARGATE_NS__"]
              relabel_configs:
                - action: keep
                  source_labels: [__meta_kubernetes_pod_label_app]
                  regex: stargate
                - source_labels: [__meta_kubernetes_pod_ip]
                  target_label: __address__
                  replacement: $1:9090
              metric_relabel_configs:
                - action: keep
                  source_labels: [__name__]
                  regex: stargate_requests_total|stargate_proxy_retries_total|stargate_proxy_retry_exhausted_total|stargate_proxy_duration_seconds_.+|stargate_routing_duration_seconds_.+|stargate_active_inference_servers
            - job_name: pylon
              scrape_interval: 1s
              kubernetes_sd_configs:
                - role: pod
                  namespaces:
                    names: ["__BACKENDS_NS__"]
              relabel_configs:
                - action: keep
                  source_labels: [__meta_kubernetes_pod_label_app]
                  regex: backend-.+-pylon
                - source_labels: [__meta_kubernetes_pod_ip]
                  target_label: __address__
                  replacement: $1:9089
              metric_relabel_configs:
                - action: keep
                  source_labels: [__name__]
                  regex: target_info|pylon_requests_total|pylon_.+
    exporters:
      prometheus:
        endpoint: 0.0.0.0:9464
    service:
      pipelines:
        metrics:
          receivers: [prometheus]
          exporters: [prometheus]
---
apiVersion: v1
kind: Service
metadata:
  name: otel-collector
  namespace: __STARGATE_NS__
spec:
  type: NodePort
  selector:
    app: otel-collector
  ports:
    - name: prometheus
      port: 9464
      targetPort: prometheus
      nodePort: __COLLECTOR_METRICS_NODE_PORT__
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
  namespace: __STARGATE_NS__
spec:
  replicas: 1
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      serviceAccountName: otel-collector
      containers:
        - name: otel-collector
          image: otel/opentelemetry-collector-contrib:0.111.0
          imagePullPolicy: IfNotPresent
          args:
            - --config=/conf/otel-collector.yaml
          ports:
            - name: prometheus
              containerPort: 9464
          volumeMounts:
            - name: config
              mountPath: /conf
      volumes:
        - name: config
          configMap:
            name: otel-collector-config
---
"#
    .replace("__STARGATE_NS__", stargate_ns)
    .replace("__BACKENDS_NS__", backends_ns)
    .replace(
        "__COLLECTOR_METRICS_NODE_PORT__",
        &collector_metrics_node_port.to_string(),
    )
}

fn resolve_image_refs() -> anyhow::Result<ImageRefs> {
    Ok(ImageRefs {
        stargate: resolve_image_with_override("STARGATE_BENCH_STARGATE_IMAGE", "stargate-dev")?,
        mock_dynamo: resolve_image_with_override(
            "STARGATE_BENCH_MOCK_DYNAMO_IMAGE",
            "mock-dynamo-dev",
        )?,
        pylon: resolve_image_with_override("STARGATE_BENCH_PYLON_IMAGE", "pylon-dev")?,
    })
}

fn resolve_image_with_override(env_var: &str, name: &str) -> anyhow::Result<String> {
    if let Ok(image) = std::env::var(env_var) {
        let image = image.trim();
        if !image.is_empty() {
            return Ok(image.to_string());
        }
    }

    resolve_image(env_var, name)
}

fn resolve_image(env_var: &str, name: &str) -> anyhow::Result<String> {
    let output = Command::new("docker")
        .arg("images")
        .arg("--format")
        .arg("{{.Repository}}:{{.Tag}}")
        .output()
        .with_context(|| format!("failed to query docker images for {name}"))?;
    if !output.status.success() {
        bail!("docker images failed while resolving {name}");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .find(|line| tilt_image_matches(line, name))
        .map(str::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve benchmark image for {name}; set {env_var} to a cluster-visible image reference before running Kubernetes benchmarks"
            )
        })
}

fn tilt_image_matches(image: &str, name: &str) -> bool {
    let Some((repository, tag)) = image.rsplit_once(':') else {
        return false;
    };
    if repository != name && !repository.ends_with(&format!("/{name}")) {
        return false;
    }
    tag.starts_with("tilt-")
}

fn slugify(value: &str) -> String {
    let slug: String = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    slug.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        AlgorithmConfig, ArrivalPatternConfig, BackendConfig, BackendProfile, DegradationConfig,
        RegistrationConfig, ScenarioMetadata, ServiceTimeConfig, StargateConfig,
        TokenDistributionConfig, TrafficPatternConfig, UniformTrafficConfig,
    };
    use serde::Deserialize;
    use serde_yaml::Value;

    fn config() -> BenchmarkConfig {
        BenchmarkConfig {
            name: "collector".to_string(),
            metadata: ScenarioMetadata::default(),
            model: "dummy-model".to_string(),
            seed: Some(42),
            request_count: 5,
            max_concurrency: 2,
            tunnel_protocol: crate::config::TunnelProtocol::Custom,
            stargates: StargateConfig { count: 1 },
            backends: BackendConfig {
                count: 2,
                cluster_id_template: None,
                profiles: Vec::new(),
                profile: BackendProfile {
                    name: "balanced".to_string(),
                    weight: 1.0,
                    max_concurrent_requests: None,
                    kv_cache_capacity_tokens: 0,
                    service_time_ms: ServiceTimeConfig {
                        ttft_mean: 150,
                        ttft_jitter_ms: 10,
                        decode_tokens_per_s: 50,
                        decode_jitter_ms: 0,
                        prefill_tokens_per_s: None,
                    },
                    registration: RegistrationConfig {
                        last_mean_input_tps: 100.0,
                    },
                },
            },
            traffic_pattern: TrafficPatternConfig::Uniform(UniformTrafficConfig {
                routing_keys: 2,
                cache_affinity_keys: 2,
                input_tokens: TokenDistributionConfig::Constant { value: 100 },
                output_tokens: TokenDistributionConfig::Constant { value: 20 },
                arrival: ArrivalPatternConfig::Constant { interval_ms: 10 },
            }),
            degradation: DegradationConfig::default(),
            algorithms: vec![AlgorithmConfig {
                name: "power-of-two".to_string(),
                config: serde_json::json!({"default": "power-of-two"}),
                pylon_queue_admission: None,
            }],
        }
    }

    fn parse_yaml_documents(manifest: &str) -> Vec<Value> {
        serde_yaml::Deserializer::from_str(manifest)
            .map(|doc| Value::deserialize(doc).expect("manifest document should deserialize"))
            .filter(|doc| !doc.is_null())
            .collect()
    }

    fn yaml_str_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
        let mut current = value;
        for segment in path {
            current = current.get(*segment)?;
        }
        current.as_str()
    }

    fn find_doc_by_kind_and_name<'a>(docs: &'a [Value], kind: &str, name: &str) -> &'a Value {
        docs.iter()
            .find(|doc| {
                yaml_str_at_path(doc, &["kind"]) == Some(kind)
                    && yaml_str_at_path(doc, &["metadata", "name"]) == Some(name)
            })
            .unwrap_or_else(|| panic!("expected {kind}/{name} in manifest"))
    }

    fn first_metric_relabel_regex(job: &Value) -> Option<&str> {
        job.get("metric_relabel_configs")
            .and_then(Value::as_sequence)
            .and_then(|configs| configs.first())
            .and_then(|config| config.get("regex"))
            .and_then(Value::as_str)
    }

    fn service_port_names(service: &Value) -> Vec<&str> {
        service
            .get("spec")
            .and_then(|spec| spec.get("ports"))
            .and_then(Value::as_sequence)
            .expect("service should contain ports")
            .iter()
            .map(|port| yaml_str_at_path(port, &["name"]).expect("service port should have a name"))
            .collect()
    }

    #[test]
    fn rendered_benchmark_manifest_includes_otel_prometheus_scraper() {
        let images = ImageRefs {
            stargate: "stargate-dev:tilt-test".to_string(),
            mock_dynamo: "mock-dynamo-dev:tilt-test".to_string(),
            pylon: "pylon-dev:tilt-test".to_string(),
        };

        let config = config();
        let rendered = render_manifest(RenderManifestConfig {
            config: &config,
            algorithm: &config.algorithms[0],
            image_refs: &images,
            stargate_ns: "sgbench-sg-power",
            backends_ns: "sgbench-be-power",
            lb_config_json: r#"{"default":"power-of-two"}"#,
            http_node_port: 30080,
            metrics_node_port: 31080,
            collector_metrics_node_port: 32080,
        });

        let docs = parse_yaml_documents(&rendered.stargate);
        let collector_sa = find_doc_by_kind_and_name(&docs, "ServiceAccount", "otel-collector");
        assert_eq!(
            yaml_str_at_path(collector_sa, &["metadata", "namespace"]),
            Some("sgbench-sg-power")
        );

        let collector_service = find_doc_by_kind_and_name(&docs, "Service", "otel-collector");
        assert_eq!(
            yaml_str_at_path(collector_service, &["spec", "type"]),
            Some("NodePort")
        );

        let collector_config =
            find_doc_by_kind_and_name(&docs, "ConfigMap", "otel-collector-config");
        let collector_yaml = yaml_str_at_path(collector_config, &["data", "otel-collector.yaml"])
            .expect("collector config should include otel-collector.yaml");
        let collector_cfg: Value =
            serde_yaml::from_str(collector_yaml).expect("collector config yaml should parse");
        let scrape_configs = collector_cfg
            .get("receivers")
            .and_then(|receivers| receivers.get("prometheus"))
            .and_then(|prometheus| prometheus.get("config"))
            .and_then(|config| config.get("scrape_configs"))
            .and_then(Value::as_sequence)
            .expect("collector config should contain scrape_configs");
        let stargate_job = scrape_configs
            .iter()
            .find(|job| yaml_str_at_path(job, &["job_name"]) == Some("stargate"))
            .expect("collector config should include stargate scrape job");
        assert_eq!(
            first_metric_relabel_regex(stargate_job),
            Some(
                "stargate_requests_total|stargate_proxy_retries_total|stargate_proxy_retry_exhausted_total|stargate_proxy_duration_seconds_.+|stargate_routing_duration_seconds_.+|stargate_active_inference_servers"
            )
        );
        let client_job = scrape_configs
            .iter()
            .find(|job| yaml_str_at_path(job, &["job_name"]) == Some("pylon"))
            .expect("collector config should include pylon scrape job");
        assert_eq!(
            first_metric_relabel_regex(client_job),
            Some("target_info|pylon_requests_total|pylon_.+")
        );
    }

    #[test]
    fn rendered_benchmark_manifest_does_not_expose_list_models_via_headless_service() {
        let images = ImageRefs {
            stargate: "stargate-dev:tilt-test".to_string(),
            mock_dynamo: "mock-dynamo-dev:tilt-test".to_string(),
            pylon: "pylon-dev:tilt-test".to_string(),
        };

        let config = config();
        let rendered = render_manifest(RenderManifestConfig {
            config: &config,
            algorithm: &config.algorithms[0],
            image_refs: &images,
            stargate_ns: "sgbench-sg-power",
            backends_ns: "sgbench-be-power",
            lb_config_json: r#"{"default":"power-of-two"}"#,
            http_node_port: 30080,
            metrics_node_port: 31080,
            collector_metrics_node_port: 32080,
        });

        let docs = parse_yaml_documents(&rendered.stargate);
        let headless = find_doc_by_kind_and_name(&docs, "Service", "stargate-headless");
        let ports = service_port_names(headless);
        assert!(
            !ports.contains(&"model-discovery"),
            "benchmark headless service must not expose local-only ListModels"
        );
    }

    #[test]
    fn tilt_image_match_accepts_unprefixed_and_prefixed_repositories() {
        assert!(tilt_image_matches("stargate-dev:tilt-123", "stargate-dev"));
        assert!(tilt_image_matches(
            "stargate-dev:tilt-bench-abc123-20260422000000",
            "stargate-dev"
        ));
        assert!(tilt_image_matches(
            "localhost:5001/stargate-dev:tilt-123",
            "stargate-dev"
        ));
        assert!(tilt_image_matches(
            "gcr.io/project/stargate-dev:tilt-bench-123",
            "stargate-dev"
        ));
        assert!(!tilt_image_matches(
            "localhost:5001/not-stargate-dev:tilt-123",
            "stargate-dev"
        ));
        assert!(!tilt_image_matches("stargate-dev:latest", "stargate-dev"));
    }

    #[test]
    fn external_services_include_per_pod_metrics_nodeport() {
        let manifest = render_stargate_external_services(
            "sgbench-sg-test",
            &[StargatePod {
                name: "stargate-0".to_string(),
            }],
        );

        let docs = parse_yaml_documents(&manifest);
        assert_eq!(
            docs.len(),
            2,
            "expected one external and one metrics service"
        );
        let external = find_doc_by_kind_and_name(&docs, "Service", "stargate-0-external");
        let metrics = find_doc_by_kind_and_name(&docs, "Service", "stargate-0-metrics");
        assert_eq!(
            yaml_str_at_path(
                external,
                &["spec", "selector", "statefulset.kubernetes.io/pod-name"]
            ),
            Some("stargate-0")
        );
        assert_eq!(
            yaml_str_at_path(metrics, &["metadata", "labels", "benchmark.stargate/role"]),
            Some("pod-metrics")
        );
        assert_eq!(
            yaml_str_at_path(metrics, &["spec", "type"]),
            Some("NodePort")
        );
        let external_ports = service_port_names(external);
        assert!(
            !external_ports.contains(&"model-discovery"),
            "per-pod external services must not expose local-only ListModels"
        );

        let has_metrics_port = metrics
            .get("spec")
            .and_then(|spec| spec.get("ports"))
            .and_then(Value::as_sequence)
            .map(|ports| {
                ports.iter().any(|port| {
                    yaml_str_at_path(port, &["name"]) == Some("metrics")
                        && yaml_str_at_path(port, &["targetPort"]) == Some("metrics")
                })
            })
            .unwrap_or(false);
        assert!(
            has_metrics_port,
            "metrics service should expose targetPort=metrics"
        );
    }

    #[test]
    fn rendered_backends_include_optional_cluster_id() {
        let images = ImageRefs {
            stargate: "stargate-dev:tilt-test".to_string(),
            mock_dynamo: "mock-dynamo-dev:tilt-test".to_string(),
            pylon: "pylon-dev:tilt-test".to_string(),
        };

        let mut config = config();
        config.backends.cluster_id_template = Some("cluster-{backend_index}".to_string());
        let rendered = render_manifest(RenderManifestConfig {
            config: &config,
            algorithm: &config.algorithms[0],
            image_refs: &images,
            stargate_ns: "sgbench-sg-power",
            backends_ns: "sgbench-be-power",
            lb_config_json: r#"{"default":"power-of-two"}"#,
            http_node_port: 30080,
            metrics_node_port: 31080,
            collector_metrics_node_port: 32080,
        });

        assert!(rendered.backends.contains("- --cluster-id=cluster-0"));
        assert!(rendered.backends.contains("- --cluster-id=cluster-1"));
    }

    #[test]
    fn rendered_manifests_include_tunnel_protocol() {
        let images = ImageRefs {
            stargate: "stargate-dev:tilt-test".to_string(),
            mock_dynamo: "mock-dynamo-dev:tilt-test".to_string(),
            pylon: "pylon-dev:tilt-test".to_string(),
        };

        let mut config = config();
        config.tunnel_protocol = crate::config::TunnelProtocol::WebTransport;
        let rendered = render_manifest(RenderManifestConfig {
            config: &config,
            algorithm: &config.algorithms[0],
            image_refs: &images,
            stargate_ns: "sgbench-sg-power",
            backends_ns: "sgbench-be-power",
            lb_config_json: r#"{"default":"power-of-two"}"#,
            http_node_port: 30080,
            metrics_node_port: 31080,
            collector_metrics_node_port: 32080,
        });

        assert!(
            rendered
                .stargate
                .contains("- --tunnel-protocol=webtransport")
        );
        assert!(
            rendered
                .backends
                .contains("- --tunnel-protocol=webtransport")
        );
    }

    #[test]
    fn rendered_pylons_include_per_algorithm_queue_admission_args() {
        let images = ImageRefs {
            stargate: "stargate-dev:tilt-test".to_string(),
            mock_dynamo: "mock-dynamo-dev:tilt-test".to_string(),
            pylon: "pylon-dev:tilt-test".to_string(),
        };
        let config = config();
        let algorithm = AlgorithmConfig {
            name: "queue-admission-enabled".to_string(),
            config: serde_json::json!({"default": "groq-multiregion"}),
            pylon_queue_admission: Some(crate::config::PylonQueueAdmissionConfig {
                enabled: true,
                min_delta_ms: Some(0),
                tolerance_factor: Some(1.0),
                retry_after_ms: Some(5),
            }),
        };
        let rendered = render_manifest(RenderManifestConfig {
            config: &config,
            algorithm: &algorithm,
            image_refs: &images,
            stargate_ns: "sgbench-sg-queue",
            backends_ns: "sgbench-be-queue",
            lb_config_json: r#"{"default":"groq-multiregion"}"#,
            http_node_port: 30080,
            metrics_node_port: 31080,
            collector_metrics_node_port: 32080,
        });

        assert!(
            rendered
                .backends
                .contains("- --pylon-queue-mismatch-retry-enabled=true")
        );
        assert!(
            rendered
                .backends
                .contains("- --pylon-queue-mismatch-min-delta-ms=0")
        );
        assert!(rendered.backends.contains("- --disable-bringup"));
        assert!(
            rendered
                .backends
                .contains("- --active-canary-interval-ms=0")
        );
        assert!(
            rendered
                .backends
                .contains("- --benchmark-fixed-last-mean-input-tps=100")
        );
        assert!(
            rendered
                .backends
                .contains("- --pylon-queue-mismatch-tolerance-factor=1")
        );
        assert!(
            rendered
                .backends
                .contains("- --pylon-queue-mismatch-retry-after-ms=5")
        );
    }
}
