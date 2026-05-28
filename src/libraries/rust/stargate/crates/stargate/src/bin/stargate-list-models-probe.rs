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

use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use stargate_proto::pb::ListModelsRequest;
use stargate_proto::pb::stargate_model_discovery_client::StargateModelDiscoveryClient;

#[derive(Parser, Debug)]
#[command(name = "stargate-list-models-probe")]
struct Args {
    #[arg(long, value_name = "ADDR")]
    addr: String,

    #[arg(long, value_name = "KEY")]
    routing_key: Option<String>,

    #[arg(long = "model-id", value_name = "MODEL")]
    model_ids: Vec<String>,

    #[arg(long = "expect", value_name = "MODEL")]
    expected_model_ids: Vec<String>,

    #[arg(long, default_value_t = 30, value_name = "N")]
    attempts: u32,

    #[arg(long, default_value_t = 1000, value_name = "MS")]
    interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let endpoint = if args.addr.starts_with("http://") || args.addr.starts_with("https://") {
        args.addr.clone()
    } else {
        format!("http://{}", args.addr)
    };

    let mut expected = args.expected_model_ids.clone();
    expected.sort();

    let mut last_error = None;
    for attempt in 1..=args.attempts {
        match list_models(&endpoint, &args).await {
            Ok(mut actual) => {
                actual.sort();
                if actual == expected {
                    println!("ListModels returned expected models: {actual:?}");
                    return Ok(());
                }
                last_error = Some(format!(
                    "attempt {attempt}/{} returned {actual:?}; expected {expected:?}",
                    args.attempts
                ));
            }
            Err(error) => {
                last_error = Some(format!(
                    "attempt {attempt}/{} failed: {error:#}",
                    args.attempts
                ));
            }
        }

        if attempt < args.attempts {
            tokio::time::sleep(Duration::from_millis(args.interval_ms)).await;
        }
    }

    bail!(
        "ListModels did not return expected models from {endpoint}: {}",
        last_error.unwrap_or_else(|| "no attempts ran".to_string())
    )
}

async fn list_models(endpoint: &str, args: &Args) -> Result<Vec<String>> {
    let mut client = StargateModelDiscoveryClient::connect(endpoint.to_string())
        .await
        .with_context(|| format!("connect to {endpoint}"))?;
    let response = client
        .list_models(ListModelsRequest {
            routing_key: args.routing_key.clone(),
            model_ids: args.model_ids.clone(),
        })
        .await
        .context("call ListModels")?
        .into_inner();

    Ok(response.model_ids)
}
