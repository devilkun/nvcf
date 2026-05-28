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

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use anyhow::Result;
use axum::Json;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast};

use std::sync::Arc;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(clap::Parser, Debug)]
#[command(name = "mock-dynamo")]
struct Args {
    /// HTTP listen address for the mock inference server
    #[arg(long, default_value = "127.0.0.1:8090", value_name = "ADDR")]
    http_listen_addr: String,

    /// Model name served by this server
    #[arg(long, default_value = "dummy-model", value_name = "MODEL")]
    model_name: String,

    /// Number of dummy tokens to generate
    #[arg(long, default_value_t = 10, value_name = "N")]
    num_tokens: usize,

    /// Delay between tokens in milliseconds
    #[arg(long, default_value_t = 100, value_name = "MS")]
    token_delay_ms: u64,

    /// Deterministic bounded jitter added to each decode token delay based on request id
    #[arg(long, default_value_t = 0, value_name = "MS")]
    decode_jitter_ms: u64,

    /// Delay before the first output token in milliseconds
    #[arg(long, default_value_t = 0, value_name = "MS")]
    ttft_ms: u64,

    /// Deterministic bounded jitter added to TTFT based on request id
    #[arg(long, default_value_t = 0, value_name = "MS")]
    ttft_jitter_ms: u64,

    /// Approximate prefill throughput. When set, TTFT scales with input token count
    #[arg(long, default_value_t = 0.0, value_name = "TPS")]
    prefill_tokens_per_s: f64,

    /// Maximum concurrent requests the mock backend processes. 0 means unlimited
    #[arg(long, default_value_t = 0, value_name = "N")]
    max_concurrent_requests: usize,

    /// Delay /health responses to create deterministic RTT differences in tests
    #[arg(long, default_value_t = 0, value_name = "MS")]
    health_delay_ms: u64,

    /// Total mock KV-cache capacity in tokens. 0 disables cache tracking
    #[arg(long, default_value_t = 0, value_name = "TOKENS")]
    kv_cache_capacity_tokens: u64,

    /// Emit the pylon engine stats stream contract by default.
    /// Set to off for fallback/no-stats tests.
    #[arg(long, value_enum, default_value_t = EngineStatsContract::On)]
    engine_stats_contract: EngineStatsContract,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum EngineStatsContract {
    On,
    Off,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
enum StatsStreamEvent {
    #[serde(rename = "stats")]
    Stats {
        v: u8,
        request_id: String,
        model: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        tokens_processed: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tokens_generated: Option<u64>,
        #[serde(skip_serializing_if = "is_false")]
        finished: bool,
    },
    #[serde(rename = "ping")]
    Ping { v: u8 },
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone)]
struct AppState {
    model_name: String,
    num_tokens: usize,
    token_delay: Duration,
    decode_jitter_ms: u64,
    ttft: Duration,
    ttft_jitter_ms: u64,
    prefill_tokens_per_s: f64,
    request_slots: Option<Arc<Semaphore>>,
    health_delay: Duration,
    kv_cache: Arc<Mutex<KvCacheState>>,
    engine_stats_contract: EngineStatsContract,
    stats_events: broadcast::Sender<StatsStreamEvent>,
}

const DUMMY_TOKENS: &[&str] = &[
    "Hello",
    ",",
    " how",
    " can",
    " I",
    " help",
    " you",
    " today",
    "?",
    " I",
    " am",
    " a",
    " helpful",
    " AI",
    " assistant",
    ".",
    " Let",
    " me",
    " know",
    " what",
    " you",
    " need",
    ".",
    " I",
    "'m",
    " here",
    " to",
    " assist",
    " you",
    "!",
];

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let args = <Args as clap::Parser>::parse();
    let http_addr: std::net::SocketAddr = args.http_listen_addr.parse()?;

    let (stats_events, _) = broadcast::channel(1024);
    let state = AppState {
        model_name: args.model_name.clone(),
        num_tokens: args.num_tokens,
        token_delay: Duration::from_millis(args.token_delay_ms),
        decode_jitter_ms: args.decode_jitter_ms,
        ttft: Duration::from_millis(args.ttft_ms),
        ttft_jitter_ms: args.ttft_jitter_ms,
        prefill_tokens_per_s: args.prefill_tokens_per_s,
        request_slots: (args.max_concurrent_requests > 0)
            .then(|| Arc::new(Semaphore::new(args.max_concurrent_requests))),
        health_delay: Duration::from_millis(args.health_delay_ms),
        kv_cache: Arc::new(Mutex::new(KvCacheState::new(args.kv_cache_capacity_tokens))),
        engine_stats_contract: args.engine_stats_contract,
        stats_events,
    };

    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/responses", post(responses))
        .route("/v1/embeddings", post(embeddings))
        .route("/pylon/v1/stats/stream", get(stats_stream))
        .route("/kv-cache/stats", get(kv_cache_stats))
        .route("/health", get(health))
        .with_state(state);

    let listener = TcpListener::bind(http_addr).await?;
    let actual_http_addr = listener.local_addr()?;
    info!(addr = %actual_http_addr, "mock-dynamo HTTP listening");
    info!("send POST to http://{actual_http_addr}/v1/chat/completions");
    info!("send POST to http://{actual_http_addr}/v1/responses");
    info!("send POST to http://{actual_http_addr}/v1/embeddings");

    axum::serve(listener, app).await?;

    Ok(())
}

#[derive(Deserialize)]
struct ChatRequest {
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    max_tokens: Option<usize>,
    #[serde(default)]
    messages: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct ResponsesRequest {
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    max_output_tokens: Option<usize>,
    #[serde(default)]
    input: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct EmbeddingsRequest {
    input: serde_json::Value,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    encoding_format: Option<EmbeddingEncodingFormat>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum EmbeddingEncodingFormat {
    Float,
    Base64,
}

#[derive(Serialize)]
struct ChatCompletionChunk {
    id: String,
    object: &'static str,
    model: String,
    choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<Usage>,
}

#[derive(Serialize)]
struct ChunkChoice {
    index: u32,
    delta: Delta,
    finish_reason: Option<&'static str>,
}

#[derive(Serialize)]
struct Delta {
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
}

#[derive(Serialize)]
struct ChatCompletion {
    id: String,
    object: &'static str,
    model: String,
    choices: Vec<NonStreamChoice>,
    usage: Usage,
}

#[derive(Serialize)]
struct NonStreamChoice {
    index: u32,
    message: Message,
    finish_reason: &'static str,
}

#[derive(Serialize)]
struct Message {
    role: &'static str,
    content: String,
}

#[derive(Serialize)]
struct ResponsesApiResponse {
    id: String,
    object: &'static str,
    created_at: u64,
    status: &'static str,
    model: String,
    output: Vec<ResponseOutputMessage>,
    usage: ResponsesUsage,
}

#[derive(Serialize)]
struct ResponseOutputMessage {
    id: String,
    r#type: &'static str,
    status: &'static str,
    role: &'static str,
    content: Vec<ResponseOutputContent>,
}

#[derive(Serialize)]
struct ResponseOutputContent {
    r#type: &'static str,
    text: String,
    annotations: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct ResponsesUsage {
    input_tokens: usize,
    output_tokens: usize,
    total_tokens: usize,
}

#[derive(Serialize)]
struct Usage {
    prompt_tokens: usize,
    completion_tokens: usize,
    total_tokens: usize,
}

#[derive(Serialize)]
struct EmbeddingsResponse {
    object: &'static str,
    data: Vec<EmbeddingItem>,
    model: String,
    usage: EmbeddingsUsage,
}

#[derive(Serialize)]
struct EmbeddingItem {
    object: &'static str,
    embedding: EmbeddingValue,
    index: usize,
}

#[derive(Serialize)]
#[serde(untagged)]
enum EmbeddingValue {
    Float(Vec<f32>),
    Base64(&'static str),
}

#[derive(Serialize)]
struct EmbeddingsUsage {
    prompt_tokens: usize,
    total_tokens: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct KvCacheStats {
    model: String,
    kv_cache_capacity_tokens: u64,
    kv_cache_used_tokens: u64,
    kv_cache_free_tokens: u64,
    kv_cache_entries: usize,
    kv_cache_hit_count: u64,
    kv_cache_miss_count: u64,
    kv_cache_eviction_count: u64,
    kv_cache_evicted_tokens: u64,
}

#[derive(Debug, Clone)]
struct KvCacheEntry {
    tokens: u64,
}

#[derive(Debug, Default)]
struct KvCacheState {
    capacity_tokens: u64,
    used_tokens: u64,
    hit_count: u64,
    miss_count: u64,
    eviction_count: u64,
    evicted_tokens: u64,
    entries: HashMap<String, KvCacheEntry>,
    lru: VecDeque<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KvCacheAccess {
    hit: bool,
    evicted_entries: u64,
    evicted_tokens: u64,
}

struct StreamResponseConfig {
    state: AppState,
    model: String,
    id: String,
    request_id: String,
    input_tokens: usize,
    first_token_delay: Duration,
    output_tokens: usize,
    kv_cache_access: KvCacheAccess,
    request_slot: Option<OwnedSemaphorePermit>,
}

struct ResponsesStreamConfig {
    state: AppState,
    model: String,
    id: String,
    request_id: String,
    input_tokens: usize,
    first_token_delay: Duration,
    output_tokens: usize,
    kv_cache_access: KvCacheAccess,
    request_slot: Option<OwnedSemaphorePermit>,
}

impl KvCacheState {
    fn new(capacity_tokens: u64) -> Self {
        Self {
            capacity_tokens,
            used_tokens: 0,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
            evicted_tokens: 0,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn access(&mut self, cache_affinity_key: Option<&str>, input_tokens: usize) -> KvCacheAccess {
        // Mock cache counters saturate like telemetry so pathological tests cannot wrap them.
        let Some(cache_affinity_key) = cache_affinity_key else {
            self.miss_count = self.miss_count.saturating_add(1);
            return KvCacheAccess {
                hit: false,
                evicted_entries: 0,
                evicted_tokens: 0,
            };
        };
        if self.capacity_tokens == 0 {
            self.miss_count = self.miss_count.saturating_add(1);
            return KvCacheAccess {
                hit: false,
                evicted_entries: 0,
                evicted_tokens: 0,
            };
        }

        if self.entries.contains_key(cache_affinity_key) {
            self.touch(cache_affinity_key);
            self.hit_count = self.hit_count.saturating_add(1);
            return KvCacheAccess {
                hit: true,
                evicted_entries: 0,
                evicted_tokens: 0,
            };
        }

        self.miss_count = self.miss_count.saturating_add(1);
        let tokens = input_tokens as u64;
        if tokens > self.capacity_tokens {
            return KvCacheAccess {
                hit: false,
                evicted_entries: 0,
                evicted_tokens: 0,
            };
        }

        let mut evicted_entries = 0u64;
        let mut evicted_tokens = 0u64;
        // used_tokens is maintained as <= capacity, but saturating arithmetic avoids wrapping on bad input.
        while self.used_tokens.saturating_add(tokens) > self.capacity_tokens {
            let Some(evicted_key) = self.lru.pop_front() else {
                break;
            };
            if let Some(evicted) = self.entries.remove(&evicted_key) {
                self.used_tokens = self.used_tokens.saturating_sub(evicted.tokens);
                evicted_entries = evicted_entries.saturating_add(1);
                evicted_tokens = evicted_tokens.saturating_add(evicted.tokens);
            }
        }
        self.eviction_count = self.eviction_count.saturating_add(evicted_entries);
        self.evicted_tokens = self.evicted_tokens.saturating_add(evicted_tokens);

        self.entries
            .insert(cache_affinity_key.to_string(), KvCacheEntry { tokens });
        self.lru.push_back(cache_affinity_key.to_string());
        self.used_tokens = self.used_tokens.saturating_add(tokens);
        KvCacheAccess {
            hit: false,
            evicted_entries,
            evicted_tokens,
        }
    }

    fn touch(&mut self, cache_affinity_key: &str) {
        self.lru.retain(|key| key != cache_affinity_key);
        self.lru.push_back(cache_affinity_key.to_string());
    }

    fn stats(&self, model: &str) -> KvCacheStats {
        KvCacheStats {
            model: model.to_string(),
            kv_cache_capacity_tokens: self.capacity_tokens,
            kv_cache_used_tokens: self.used_tokens,
            // Keep exported mock stats nonnegative even if a test mutates internal counters.
            kv_cache_free_tokens: self.capacity_tokens.saturating_sub(self.used_tokens),
            kv_cache_entries: self.entries.len(),
            kv_cache_hit_count: self.hit_count,
            kv_cache_miss_count: self.miss_count,
            kv_cache_eviction_count: self.eviction_count,
            kv_cache_evicted_tokens: self.evicted_tokens,
        }
    }
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ChatRequest>,
) -> Response {
    let request_slot = acquire_request_slot(&state).await;
    let input_tokens = request_input_tokens(&headers, &req);
    let output_tokens = request_output_tokens(&headers, &req, state.num_tokens);
    let model = req.model.clone().unwrap_or(state.model_name.clone());
    let stream = req.stream == Some(true);
    let id = format!("chatcmpl-mock-{}", rand_id());
    info!(id = %id, model = %model, stream = stream, "received chat/completions request");
    let request_id = optional_header(&headers, "x-request-id").unwrap_or_else(|| id.clone());
    let cache_affinity_key = optional_header(&headers, "x-cache-affinity-key");
    let kv_cache_access = state
        .kv_cache
        .lock()
        .await
        .access(cache_affinity_key.as_deref(), input_tokens);
    let prefill = if kv_cache_access.hit {
        Duration::ZERO
    } else {
        prefill_delay(input_tokens, state.prefill_tokens_per_s)
    };
    let first_token_delay = state.ttft
        + prefill
        + Duration::from_millis(jitter_ms(&request_id, "ttft", state.ttft_jitter_ms));
    info!(
        id = %id,
        cache_affinity_key = ?cache_affinity_key,
        kv_cache_hit = kv_cache_access.hit,
        kv_cache_evicted_entries = kv_cache_access.evicted_entries,
        kv_cache_evicted_tokens = kv_cache_access.evicted_tokens,
        input_tokens = input_tokens,
        "computed mock request timing"
    );

    if stream {
        info!(id = %id, status = 200, "responding with SSE stream");
        return stream_response(StreamResponseConfig {
            state,
            model,
            id,
            request_id,
            input_tokens,
            first_token_delay,
            output_tokens,
            kv_cache_access,
            request_slot,
        })
        .await;
    }

    tokio::time::sleep(non_streaming_delay(
        &state,
        &request_id,
        first_token_delay,
        output_tokens,
    ))
    .await;

    let content: String = DUMMY_TOKENS
        .iter()
        .cycle()
        .take(output_tokens)
        .copied()
        .collect();

    info!(id = %id, status = 200, "responding with JSON");
    emit_request_stats_event(
        &state,
        &request_id,
        &model,
        Some(input_tokens),
        Some(output_tokens),
        true,
    );
    let mut response = Json(ChatCompletion {
        id,
        object: "chat.completion",
        model,
        choices: vec![NonStreamChoice {
            index: 0,
            message: Message {
                role: "assistant",
                content,
            },
            finish_reason: "stop",
        }],
        usage: Usage {
            prompt_tokens: input_tokens,
            completion_tokens: output_tokens,
            total_tokens: input_tokens + output_tokens,
        },
    })
    .into_response();
    insert_kv_cache_headers(response.headers_mut(), kv_cache_access);
    response
}

async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ResponsesRequest>,
) -> Response {
    if req.stream != Some(true) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "mock-dynamo /v1/responses requires stream=true",
            })),
        )
            .into_response();
    }

    let request_slot = acquire_request_slot(&state).await;
    let input_tokens = response_input_tokens(&headers, &req);
    let output_tokens = response_output_tokens(&headers, &req, state.num_tokens);
    let model = req.model.clone().unwrap_or(state.model_name.clone());
    let id = format!("resp-mock-{}", rand_id());
    info!(id = %id, model = %model, "received responses request");
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let cache_affinity_key = optional_header(&headers, "x-cache-affinity-key");
    let kv_cache_access = state
        .kv_cache
        .lock()
        .await
        .access(cache_affinity_key.as_deref(), input_tokens);
    let prefill = if kv_cache_access.hit {
        Duration::ZERO
    } else {
        prefill_delay(input_tokens, state.prefill_tokens_per_s)
    };
    let first_token_delay = state.ttft
        + prefill
        + Duration::from_millis(jitter_ms(request_id, "ttft", state.ttft_jitter_ms));

    stream_responses_response(ResponsesStreamConfig {
        state,
        model,
        id,
        request_id: request_id.to_string(),
        input_tokens,
        first_token_delay,
        output_tokens,
        kv_cache_access,
        request_slot,
    })
    .await
}

async fn stream_responses_response(config: ResponsesStreamConfig) -> Response {
    let ResponsesStreamConfig {
        state,
        model,
        id,
        request_id,
        input_tokens,
        first_token_delay,
        output_tokens,
        kv_cache_access,
        request_slot,
    } = config;
    let created_at = current_unix_timestamp();
    let stream = async_stream::stream! {
        let _request_slot = request_slot;
        let mut output_text = String::new();
        emit_request_stats_event(&state, &request_id, &model, Some(0), Some(0), false);

        yield Ok::<_, std::convert::Infallible>(responses_sse_event(
            "response.created",
            &serde_json::json!({
                "type": "response.created",
                "response": {
                    "id": id.clone(),
                    "object": "response",
                    "created_at": created_at,
                    "status": "in_progress",
                    "model": model.clone(),
                    "output": [],
                    "usage": null,
                },
            }),
        ));

        tokio::time::sleep(first_token_delay).await;
        emit_request_stats_event(&state, &request_id, &model, Some(input_tokens), Some(0), false);

        for i in 0..output_tokens {
            if i > 0 {
                tokio::time::sleep(token_delay(&state, &request_id, i)).await;
            }
            let token = DUMMY_TOKENS[i % DUMMY_TOKENS.len()];
            output_text.push_str(token);
            yield Ok(responses_sse_event(
                "response.output_text.delta",
                &serde_json::json!({
                    "type": "response.output_text.delta",
                    "response_id": id.clone(),
                    "output_index": 0,
                    "content_index": 0,
                    "delta": token,
                }),
            ));
            emit_request_stats_event(
                &state,
                &request_id,
                &model,
                Some(input_tokens),
                Some(i + 1),
                false,
            );
        }

        yield Ok(responses_sse_event(
            "response.completed",
            &serde_json::json!({
                "type": "response.completed",
                "response": ResponsesApiResponse {
                    id: id.clone(),
                    object: "response",
                    created_at,
                    status: "completed",
                    model: model.clone(),
                    output: vec![ResponseOutputMessage {
                        id: format!("msg-{id}"),
                        r#type: "message",
                        status: "completed",
                        role: "assistant",
                        content: vec![ResponseOutputContent {
                            r#type: "output_text",
                            text: output_text,
                            annotations: Vec::new(),
                        }],
                    }],
                    usage: ResponsesUsage {
                        input_tokens,
                        output_tokens,
                        total_tokens: input_tokens + output_tokens,
                    },
                },
            }),
        ));
        emit_request_stats_event(
            &state,
            &request_id,
            &model,
            Some(input_tokens),
            Some(output_tokens),
            true,
        );
    };

    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    insert_kv_cache_headers(response.headers_mut(), kv_cache_access);
    response
}

fn responses_sse_event<T: Serialize>(event_name: &'static str, value: &T) -> Event {
    Event::default()
        .event(event_name)
        .data(serde_json::to_string(value).expect("response stream event should serialize"))
}

async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<EmbeddingsRequest>,
) -> Response {
    let _request_slot = acquire_request_slot(&state).await;
    let item_count = embedding_item_count(&req.input);
    let prompt_tokens = request_embedding_tokens(&headers, &req.input);
    let model = req.model.clone().unwrap_or(state.model_name.clone());
    let encoding_format = req
        .encoding_format
        .unwrap_or(EmbeddingEncodingFormat::Float);
    let id = format!("embd-mock-{}", rand_id());
    let request_id = optional_header(&headers, "x-request-id").unwrap_or_else(|| id.clone());
    info!(
        id = %id,
        model = %model,
        item_count = item_count,
        prompt_tokens = prompt_tokens,
        encoding_format = ?encoding_format,
        "received embeddings request"
    );

    let data = (0..item_count)
        .map(|index| EmbeddingItem {
            object: "embedding",
            embedding: deterministic_embedding_value(index, encoding_format),
            index,
        })
        .collect();

    emit_request_stats_event(&state, &request_id, &model, Some(prompt_tokens), None, true);

    Json(EmbeddingsResponse {
        object: "list",
        data,
        model,
        usage: EmbeddingsUsage {
            prompt_tokens,
            total_tokens: prompt_tokens,
        },
    })
    .into_response()
}

async fn health(State(state): State<AppState>) -> &'static str {
    if !state.health_delay.is_zero() {
        tokio::time::sleep(state.health_delay).await;
    }
    "ok"
}

async fn kv_cache_stats(State(state): State<AppState>) -> Json<KvCacheStats> {
    Json(state.kv_cache.lock().await.stats(&state.model_name))
}

async fn stats_stream(State(state): State<AppState>) -> Response {
    if state.engine_stats_contract == EngineStatsContract::Off {
        return StatusCode::NOT_FOUND.into_response();
    }

    let mut events = state.stats_events.subscribe();
    let stream = async_stream::stream! {
        let mut ping = tokio::time::interval(Duration::from_secs(1));
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Ok(event) => yield Ok::<Bytes, std::convert::Infallible>(ndjson_event(&event)),
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
                _ = ping.tick() => {
                    yield Ok::<Bytes, std::convert::Infallible>(ndjson_event(&StatsStreamEvent::Ping { v: 1 }));
                }
            }
        }
    };
    Response::builder()
        .header("content-type", "application/x-ndjson")
        .body(Body::from_stream(stream))
        .expect("stats stream response should build")
}

fn ndjson_event(event: &StatsStreamEvent) -> Bytes {
    let mut line = serde_json::to_vec(event).expect("stats stream event should serialize");
    line.push(b'\n');
    Bytes::from(line)
}

fn emit_stats_event(state: &AppState, event: StatsStreamEvent) {
    if state.engine_stats_contract == EngineStatsContract::On {
        let _ = state.stats_events.send(event);
    }
}

fn emit_request_stats_event(
    state: &AppState,
    request_id: &str,
    model: &str,
    tokens_processed: Option<usize>,
    tokens_generated: Option<usize>,
    finished: bool,
) {
    emit_stats_event(
        state,
        StatsStreamEvent::Stats {
            v: 1,
            request_id: request_id.to_string(),
            model: model.to_string(),
            tokens_processed: tokens_processed.map(|tokens| tokens as u64),
            tokens_generated: tokens_generated.map(|tokens| tokens as u64),
            finished,
        },
    );
}

async fn stream_response(config: StreamResponseConfig) -> Response {
    let StreamResponseConfig {
        state,
        model,
        id,
        request_id,
        input_tokens,
        first_token_delay,
        output_tokens,
        kv_cache_access,
        request_slot,
    } = config;
    let stream = async_stream::stream! {
        let _request_slot = request_slot;
        emit_request_stats_event(&state, &request_id, &model, Some(0), Some(0), false);

        tokio::time::sleep(first_token_delay).await;

        emit_request_stats_event(&state, &request_id, &model, Some(input_tokens), Some(0), false);

        yield Ok::<_, std::convert::Infallible>(Event::default().data(
            serde_json::to_string(&ChatCompletionChunk {
                id: id.clone(),
                object: "chat.completion.chunk",
                model: model.clone(),
                choices: vec![ChunkChoice {
                    index: 0,
                    delta: Delta {
                        role: Some("assistant"),
                        content: None,
                    },
                    finish_reason: None,
                }],
                usage: None,
            })
            .unwrap(),
        ));

        for i in 0..output_tokens {
            if i > 0 {
                tokio::time::sleep(token_delay(&state, &request_id, i)).await;
            }
            let token = DUMMY_TOKENS[i % DUMMY_TOKENS.len()];
            yield Ok(Event::default().data(
                serde_json::to_string(&ChatCompletionChunk {
                    id: id.clone(),
                    object: "chat.completion.chunk",
                    model: model.clone(),
                    choices: vec![ChunkChoice {
                        index: 0,
                        delta: Delta {
                            role: None,
                            content: Some(token.to_string()),
                        },
                        finish_reason: None,
                    }],
                    usage: None,
                })
                .unwrap(),
            ));
            emit_request_stats_event(
                &state,
                &request_id,
                &model,
                Some(input_tokens),
                Some(i + 1),
                false,
            );
        }

        yield Ok(Event::default().data(
            serde_json::to_string(&ChatCompletionChunk {
                id: id.clone(),
                object: "chat.completion.chunk",
                model: model.clone(),
                choices: vec![ChunkChoice {
                    index: 0,
                    delta: Delta {
                        role: None,
                        content: None,
                    },
                    finish_reason: Some("stop"),
                }],
                usage: None,
            })
            .unwrap(),
        ));

        emit_request_stats_event(
            &state,
            &request_id,
            &model,
            Some(input_tokens),
            Some(output_tokens),
            true,
        );

        yield Ok(Event::default().data("[DONE]"));
    };

    let mut response = Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response();
    insert_kv_cache_headers(response.headers_mut(), kv_cache_access);
    response
}

fn insert_kv_cache_headers(headers: &mut HeaderMap, access: KvCacheAccess) {
    headers.insert(
        HeaderName::from_static("x-kv-cache-hit"),
        HeaderValue::from_static(if access.hit { "true" } else { "false" }),
    );
    headers.insert(
        HeaderName::from_static("x-kv-cache-evicted-entries"),
        HeaderValue::from_str(&access.evicted_entries.to_string())
            .expect("evicted entry count should be a valid header value"),
    );
    headers.insert(
        HeaderName::from_static("x-kv-cache-evicted-tokens"),
        HeaderValue::from_str(&access.evicted_tokens.to_string())
            .expect("evicted token count should be a valid header value"),
    );
}

fn rand_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{t:x}")
}

fn current_unix_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn acquire_request_slot(state: &AppState) -> Option<OwnedSemaphorePermit> {
    match &state.request_slots {
        Some(slots) => slots.clone().acquire_owned().await.ok(),
        None => None,
    }
}

fn request_input_tokens(headers: &HeaderMap, request: &ChatRequest) -> usize {
    at_least_one_token(
        header_usize(headers, "x-input-tokens")
            .unwrap_or_else(|| estimate_prompt_tokens(&request.messages)),
    )
}

fn request_output_tokens(
    headers: &HeaderMap,
    request: &ChatRequest,
    default_tokens: usize,
) -> usize {
    if let Some(tokens) = header_usize(headers, "x-output-tokens") {
        return at_least_one_token(tokens);
    }
    let default_tokens = at_least_one_token(default_tokens);
    request
        .max_tokens
        .map(|tokens| at_least_one_token(tokens.min(default_tokens)))
        .unwrap_or(default_tokens)
}

fn response_input_tokens(headers: &HeaderMap, request: &ResponsesRequest) -> usize {
    at_least_one_token(
        header_usize(headers, "x-input-tokens")
            .unwrap_or_else(|| estimate_response_input_tokens(request.input.as_ref())),
    )
}

fn response_output_tokens(
    headers: &HeaderMap,
    request: &ResponsesRequest,
    default_tokens: usize,
) -> usize {
    if let Some(tokens) = header_usize(headers, "x-output-tokens") {
        return at_least_one_token(tokens);
    }
    let default_tokens = at_least_one_token(default_tokens);
    request
        .max_output_tokens
        .map(|tokens| at_least_one_token(tokens.min(default_tokens)))
        .unwrap_or(default_tokens)
}

fn request_embedding_tokens(headers: &HeaderMap, input: &serde_json::Value) -> usize {
    at_least_one_token(
        header_usize(headers, "x-input-tokens").unwrap_or_else(|| estimate_embedding_tokens(input)),
    )
}

fn embedding_item_count(input: &serde_json::Value) -> usize {
    match input {
        serde_json::Value::String(_) => 1,
        serde_json::Value::Array(items) if items.is_empty() => 0,
        serde_json::Value::Array(items) if items.iter().all(serde_json::Value::is_number) => 1,
        serde_json::Value::Array(items) => items.len(),
        _ => 1,
    }
}

fn deterministic_embedding_value(index: usize, format: EmbeddingEncodingFormat) -> EmbeddingValue {
    match format {
        EmbeddingEncodingFormat::Float => EmbeddingValue::Float(vec![
            index as f32,
            index as f32 + 0.125,
            -(index as f32) - 0.25,
        ]),
        EmbeddingEncodingFormat::Base64 => EmbeddingValue::Base64("AAAAAAAAAAA="),
    }
}

fn header_usize(headers: &HeaderMap, name: &str) -> Option<usize> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn optional_header(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn estimate_prompt_tokens(messages: &[serde_json::Value]) -> usize {
    let chars: usize = messages
        .iter()
        .map(|message| {
            message
                .get("content")
                .and_then(|content| content.as_str())
                .map(str::len)
                .unwrap_or_default()
        })
        .sum();
    at_least_one_token(chars.max(messages.len()))
}

fn estimate_response_input_tokens(input: Option<&serde_json::Value>) -> usize {
    let Some(input) = input else {
        return 1;
    };
    match input {
        serde_json::Value::String(text) => text.len(),
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| match item {
                serde_json::Value::String(text) => text.len(),
                value => value.to_string().len(),
            })
            .sum(),
        value => value.to_string().len(),
    }
}

fn estimate_embedding_tokens(input: &serde_json::Value) -> usize {
    match input {
        serde_json::Value::String(value) => value.len(),
        serde_json::Value::Array(items) if items.iter().all(serde_json::Value::is_number) => {
            items.len()
        }
        serde_json::Value::Array(items) => items
            .iter()
            .map(|item| match item {
                serde_json::Value::String(value) => value.len(),
                serde_json::Value::Array(values) => values.len(),
                _ => 1,
            })
            .sum(),
        _ => 1,
    }
}

fn at_least_one_token(tokens: usize) -> usize {
    // The mock backend's timing and cache math require nonzero token work.
    tokens.max(1)
}

fn prefill_delay(input_tokens: usize, tokens_per_s: f64) -> Duration {
    if tokens_per_s > 0.0 && tokens_per_s.is_finite() {
        Duration::from_secs_f64(input_tokens as f64 / tokens_per_s)
    } else {
        Duration::ZERO
    }
}

fn non_streaming_delay(
    state: &AppState,
    request_id: &str,
    first_token_delay: Duration,
    output_tokens: usize,
) -> Duration {
    let decode_delay = (1..output_tokens)
        .map(|token_index| token_delay(state, request_id, token_index))
        .fold(Duration::ZERO, |total, delay| total + delay);
    first_token_delay + decode_delay
}

fn token_delay(state: &AppState, request_id: &str, token_index: usize) -> Duration {
    state.token_delay
        + Duration::from_millis(jitter_ms(
            request_id,
            &format!("decode-{token_index}"),
            state.decode_jitter_ms,
        ))
}

fn jitter_ms(request_id: &str, salt: &str, max_jitter_ms: u64) -> u64 {
    if max_jitter_ms == 0 {
        return 0;
    }

    let mut hash: u64 = 1469598103934665603;
    for byte in request_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1099511628211);
    }
    for byte in salt.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(1099511628211);
    }
    hash % (max_jitter_ms + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use axum::routing::post;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn request(max_tokens: Option<usize>) -> ChatRequest {
        ChatRequest {
            stream: Some(true),
            model: Some("dummy-model".to_string()),
            max_tokens,
            messages: Vec::new(),
        }
    }

    fn test_stats_events() -> broadcast::Sender<StatsStreamEvent> {
        let (tx, _) = broadcast::channel(1024);
        tx
    }

    #[test]
    fn counts_openai_embedding_input_items() {
        assert_eq!(embedding_item_count(&serde_json::json!("single input")), 1);
        assert_eq!(embedding_item_count(&serde_json::json!(["a", "b"])), 2);
        assert_eq!(embedding_item_count(&serde_json::json!([1, 2, 3])), 1);
        assert_eq!(
            embedding_item_count(&serde_json::json!([[1, 2], [3, 4], [5]])),
            3
        );
        assert_eq!(embedding_item_count(&serde_json::json!([])), 0);
    }

    #[test]
    fn embedding_format_controls_mock_embedding_value_shape() {
        assert!(matches!(
            deterministic_embedding_value(0, EmbeddingEncodingFormat::Float),
            EmbeddingValue::Float(_)
        ));
        assert!(matches!(
            deterministic_embedding_value(0, EmbeddingEncodingFormat::Base64),
            EmbeddingValue::Base64(_)
        ));
    }

    #[tokio::test]
    async fn embeddings_endpoint_returns_json_without_stream() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 1,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::ZERO,
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(0))),
            engine_stats_contract: EngineStatsContract::On,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/embeddings", post(embeddings))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let request_body = serde_json::json!({
            "model": "request-model",
            "input": ["alpha", "beta"],
            "encoding_format": "float",
        })
        .to_string();
        stream
            .write_all(
                format!(
                    "POST /v1/embeddings HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\nx-input-tokens: 11\r\n\r\n{request_body}",
                    request_body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        let response = read_to_end(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains(r#""object":"list""#));
        assert!(response.contains(r#""model":"request-model""#));
        assert!(response.contains(r#""index":1"#));
        assert!(response.contains(r#""prompt_tokens":11"#));
        assert!(response.contains(r#""total_tokens":11"#));

        server.abort();
    }

    #[test]
    fn output_tokens_prefer_benchmark_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-output-tokens", HeaderValue::from_static("64"));

        assert_eq!(request_output_tokens(&headers, &request(Some(16)), 8), 64);
    }

    #[test]
    fn max_tokens_is_capped_by_default_when_header_absent() {
        assert_eq!(
            request_output_tokens(&HeaderMap::new(), &request(Some(128)), 8),
            8
        );
    }

    #[test]
    fn prefill_delay_scales_with_input_tokens() {
        assert_eq!(prefill_delay(4_000, 2_000.0), Duration::from_secs(2));
    }

    #[test]
    fn kv_cache_hit_skips_subsequent_prefill() {
        let mut cache = KvCacheState::new(1_000);

        assert!(!cache.access(Some("cak-a"), 100).hit);
        assert!(cache.access(Some("cak-a"), 100).hit);
        assert_eq!(cache.stats("dummy-model").kv_cache_used_tokens, 100);
        assert_eq!(cache.stats("dummy-model").kv_cache_free_tokens, 900);
        assert_eq!(cache.stats("dummy-model").kv_cache_hit_count, 1);
        assert_eq!(cache.stats("dummy-model").kv_cache_miss_count, 1);
    }

    #[test]
    fn kv_cache_evicts_least_recently_used_entry() {
        let mut cache = KvCacheState::new(300);

        assert!(!cache.access(Some("cak-a"), 100).hit);
        assert!(!cache.access(Some("cak-b"), 100).hit);
        assert!(cache.access(Some("cak-a"), 100).hit);
        let access = cache.access(Some("cak-c"), 150);
        assert!(!access.hit);
        assert_eq!(access.evicted_entries, 1);
        assert_eq!(access.evicted_tokens, 100);

        assert!(cache.access(Some("cak-a"), 100).hit);
        assert!(!cache.access(Some("cak-b"), 100).hit);
        assert_eq!(cache.stats("dummy-model").kv_cache_eviction_count, 2);
        assert_eq!(cache.stats("dummy-model").kv_cache_evicted_tokens, 250);
    }

    #[tokio::test]
    async fn streaming_response_delays_first_data_frame_until_ttft() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 1,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::from_millis(120),
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(0))),
            engine_stats_contract: EngineStatsContract::On,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .route("/pylon/v1/stats/stream", get(stats_stream))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let body = r#"{"model":"dummy-model","messages":[],"max_tokens":1,"stream":true}"#;
        stream
            .write_all(
                format!(
                    "POST /v1/chat/completions HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-request-id: req-ttft\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        assert!(
            tokio::time::timeout(Duration::from_millis(50), read_until_sse_data(&mut stream))
                .await
                .is_err(),
            "mock emitted an SSE data frame before configured TTFT elapsed"
        );
        tokio::time::timeout(Duration::from_secs(1), read_until_sse_data(&mut stream))
            .await
            .expect("SSE data should arrive after TTFT")
            .expect("SSE data read should succeed");
        server.abort();
    }

    #[tokio::test]
    async fn streaming_response_does_not_emit_legacy_progress_contract() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 2,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::ZERO,
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(0))),
            engine_stats_contract: EngineStatsContract::On,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let body = r#"{"model":"dummy-model","messages":[],"max_tokens":2,"stream":true}"#;
        stream
            .write_all(
                format!(
                    "POST /v1/chat/completions HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-request-id: req-contract\r\nx-input-tokens: 11\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        let response = read_until_done(&mut stream).await;
        assert!(!response.contains("x-pylon-engine-stat-"));
        assert!(!response.contains("inference-progress.v1"));
        assert!(!response.contains(r#""usage":"#));

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        stream
            .write_all(
                format!(
                    "GET /pylon/v1/stats/stream HTTP/1.1\r\nhost: {addr}\r\nconnection: close\r\n\r\n",
                )
                .as_bytes(),
            )
            .await
            .expect("stats stream request should write");

        let response = read_to_end(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
        server.abort();
    }

    #[tokio::test]
    async fn engine_stats_stream_contract_can_be_disabled() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 1,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::ZERO,
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(0))),
            engine_stats_contract: EngineStatsContract::Off,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let body = r#"{"model":"dummy-model","messages":[],"max_tokens":1,"stream":true}"#;
        stream
            .write_all(
                format!(
                    "POST /v1/chat/completions HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nx-request-id: req-no-contract\r\nx-input-tokens: 11\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        let response = read_until_done(&mut stream).await;
        assert!(!response.contains("x-pylon-engine-stat-"));
        assert!(!response.contains("inference-progress.v1"));
        assert!(!response.contains(r#""usage":"#));
        server.abort();
    }

    #[test]
    fn stats_stream_events_are_ndjson() {
        let event = StatsStreamEvent::Stats {
            v: 1,
            request_id: "req-1".to_string(),
            model: "dummy-model".to_string(),
            tokens_processed: Some(11),
            tokens_generated: Some(2),
            finished: true,
        };

        let line = String::from_utf8(ndjson_event(&event).to_vec()).unwrap();

        assert_eq!(
            line,
            r#"{"type":"stats","v":1,"request_id":"req-1","model":"dummy-model","tokens_processed":11,"tokens_generated":2,"finished":true}"#
                .to_string()
                + "\n"
        );
    }

    #[tokio::test]
    async fn responses_endpoint_streams_response_events_without_legacy_stats_headers() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 2,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::ZERO,
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(10_000))),
            engine_stats_contract: EngineStatsContract::On,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/responses", post(responses))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let request_body = serde_json::json!({
            "model": "request-model",
            "input": "hello",
            "max_output_tokens": 2,
            "stream": true,
        })
        .to_string();
        stream
            .write_all(
                format!(
                    "POST /v1/responses HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\nx-request-id: req-responses-contract\r\nx-input-tokens: 7\r\nx-cache-affinity-key: cache-a\r\n\r\n{request_body}",
                    request_body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        let response = read_to_end(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(!response.contains("x-pylon-engine-stat-"));
        assert!(response.contains("x-kv-cache-hit: false"));
        assert!(response.contains("content-type: text/event-stream"));

        let (_, body_text) = response
            .split_once("\r\n\r\n")
            .expect("response should contain a body");
        assert!(body_text.contains("event: response.created"));
        assert!(body_text.contains("event: response.output_text.delta"));
        assert!(body_text.contains("event: response.completed"));
        assert!(body_text.contains(r#""type":"response.completed""#));
        assert!(body_text.contains(r#""object":"response""#));
        assert!(body_text.contains(r#""status":"completed""#));
        assert!(body_text.contains(r#""model":"request-model""#));
        assert!(body_text.contains(r#""input_tokens":7"#));
        assert!(body_text.contains(r#""output_tokens":2"#));
        assert!(body_text.contains(r#""total_tokens":9"#));

        server.abort();
    }

    #[tokio::test]
    async fn responses_endpoint_rejects_non_streaming_requests() {
        let state = AppState {
            model_name: "dummy-model".to_string(),
            num_tokens: 2,
            token_delay: Duration::ZERO,
            decode_jitter_ms: 0,
            ttft: Duration::ZERO,
            ttft_jitter_ms: 0,
            prefill_tokens_per_s: 0.0,
            request_slots: None,
            health_delay: Duration::ZERO,
            kv_cache: Arc::new(Mutex::new(KvCacheState::new(10_000))),
            engine_stats_contract: EngineStatsContract::On,
            stats_events: test_stats_events(),
        };
        let app = Router::new()
            .route("/v1/responses", post(responses))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test server should bind");
        let addr = listener.local_addr().expect("local address should exist");
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("test server should serve");
        });

        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("test client should connect");
        let request_body = serde_json::json!({
            "model": "request-model",
            "input": "hello",
            "stream": false,
        })
        .to_string();
        stream
            .write_all(
                format!(
                    "POST /v1/responses HTTP/1.1\r\nhost: {addr}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\nx-request-id: req-responses-nonstream\r\nx-input-tokens: 7\r\n\r\n{request_body}",
                    request_body.len()
                )
                .as_bytes(),
            )
            .await
            .expect("request should write");

        let response = read_to_end(&mut stream).await;
        assert!(response.starts_with("HTTP/1.1 400 Bad Request"));
        assert!(response.contains("stream=true"));

        server.abort();
    }

    async fn read_until_sse_data(stream: &mut tokio::net::TcpStream) -> std::io::Result<()> {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = stream.read(&mut buffer).await?;
            if read == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&buffer[..read]);
            if String::from_utf8_lossy(&bytes).contains("\ndata:") {
                return Ok(());
            }
        }
    }

    async fn read_until_done(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buffer))
                .await
                .expect("response should continue")
                .expect("response should read");
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buffer[..read]);
            if String::from_utf8_lossy(&bytes).contains("data: [DONE]") {
                break;
            }
        }
        String::from_utf8_lossy(&bytes).to_string()
    }

    async fn read_to_end(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .await
            .expect("response should read to end");
        String::from_utf8_lossy(&bytes).to_string()
    }
}
