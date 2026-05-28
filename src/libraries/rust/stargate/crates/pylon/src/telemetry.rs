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

const TRACED_ROOT_SPAN: &str = "pylon_upstream_http_request";
pub const DEFAULT_SERVICE_NAME: &str = "pylon";

pub use stargate_telemetry::TelemetryGuard;

pub fn init_telemetry(
    otel_endpoint: Option<&str>,
    service_name: &str,
) -> anyhow::Result<TelemetryGuard> {
    stargate_telemetry::init_telemetry(otel_endpoint, service_name, TRACED_ROOT_SPAN)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::Key;

    #[test]
    fn telemetry_resource_uses_configured_service_name() {
        let resource = stargate_telemetry::telemetry_resource("pylon");

        assert_eq!(
            resource
                .get(&Key::new("service.name"))
                .map(|value| value.to_string()),
            Some("pylon".to_string())
        );
    }

    #[test]
    fn traced_root_span_is_pylon_upstream_request() {
        assert_eq!(TRACED_ROOT_SPAN, "pylon_upstream_http_request");
    }
}
