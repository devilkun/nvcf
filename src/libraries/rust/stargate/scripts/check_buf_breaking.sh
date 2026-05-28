#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

against="${1:-.git#branch=main}"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

if ! command -v buf >/dev/null 2>&1; then
  echo "buf is required on PATH" >&2
  exit 127
fi

if buf breaking --against "$against" --error-format=json >"$tmp"; then
  exit 0
fi

python3 - "$tmp" <<'PY'
import collections
import json
import sys

# This migration intentionally makes ModelStats a clean backward-incompatible
# proto surface: the old input_tps/max_input_tps/calibrated_max_input_tps fields
# are gone, the remaining ModelStats fields are compactly renumbered from 1,
# and the calibration directive payload is renamed to last_mean_input_tps. Keep
# the allowance exact so future protobuf breaks still fail after this PR merges.
PROTO_PATH = "crates/proto/proto/stargate.proto"
expected_messages_by_rule = {
    "FIELD_NO_DELETE": [
        'Previously present field "18" with name "stats_capabilities" on message "ModelStats" was deleted.',
        'Previously present field "19" with name "stats_sources" on message "ModelStats" was deleted.',
    ],
    "FIELD_SAME_CARDINALITY": [
        'Field "12" with name "queue_time_estimate_ms_by_priority" on message "ModelStats" changed cardinality from "optional with implicit presence" to "map".',
        'Field "13" with name "input_processing_queries" on message "ModelStats" changed cardinality from "map" to "optional with implicit presence".',
        'Field "16" with name "stats_capabilities" on message "ModelStats" changed cardinality from "optional with implicit presence" to "repeated".',
        'Field "17" with name "stats_sources" on message "ModelStats" changed cardinality from "optional with implicit presence" to "repeated".',
    ],
    "FIELD_SAME_JSON_NAME": [
        'Field "1" with name "last_mean_input_tps" on message "ModelStats" changed option "json_name" from "inputTps" to "lastMeanInputTps".',
        'Field "3" with name "queue_size" on message "ModelStats" changed option "json_name" from "maxInputTps" to "queueSize".',
        'Field "4" with name "queued_input_size" on message "ModelStats" changed option "json_name" from "maxOutputTps" to "queuedInputSize".',
        'Field "5" with name "max_output_tps" on message "ModelStats" changed option "json_name" from "queueSize" to "maxOutputTps".',
        'Field "6" with name "kv_cache_capacity_tokens" on message "ModelStats" changed option "json_name" from "queuedInputSize" to "kvCacheCapacityTokens".',
        'Field "7" with name "kv_cache_used_tokens" on message "ModelStats" changed option "json_name" from "kvCacheCapacityTokens" to "kvCacheUsedTokens".',
        'Field "8" with name "kv_cache_free_tokens" on message "ModelStats" changed option "json_name" from "kvCacheUsedTokens" to "kvCacheFreeTokens".',
        'Field "9" with name "num_running_queries" on message "ModelStats" changed option "json_name" from "kvCacheFreeTokens" to "numRunningQueries".',
        'Field "10" with name "max_engine_concurrency" on message "ModelStats" changed option "json_name" from "numRunningQueries" to "maxEngineConcurrency".',
        'Field "11" with name "total_query_input_size" on message "ModelStats" changed option "json_name" from "maxEngineConcurrency" to "totalQueryInputSize".',
        'Field "12" with name "queue_time_estimate_ms_by_priority" on message "ModelStats" changed option "json_name" from "totalQueryInputSize" to "queueTimeEstimateMsByPriority".',
        'Field "13" with name "input_processing_queries" on message "ModelStats" changed option "json_name" from "queueTimeEstimateMsByPriority" to "inputProcessingQueries".',
        'Field "14" with name "output_generation_queries" on message "ModelStats" changed option "json_name" from "calibratedMaxInputTps" to "outputGenerationQueries".',
        'Field "15" with name "stats_observed_at_unix_ms" on message "ModelStats" changed option "json_name" from "inputProcessingQueries" to "statsObservedAtUnixMs".',
        'Field "16" with name "stats_capabilities" on message "ModelStats" changed option "json_name" from "outputGenerationQueries" to "statsCapabilities".',
        'Field "17" with name "stats_sources" on message "ModelStats" changed option "json_name" from "statsObservedAtUnixMs" to "statsSources".',
        'Field "3" with name "last_mean_input_tps" on message "ModelCalibrationDirective" changed option "json_name" from "maxInputTps" to "lastMeanInputTps".',
    ],
    "FIELD_SAME_NAME": [
        'Field "1" on message "ModelStats" changed name from "input_tps" to "last_mean_input_tps".',
        'Field "3" on message "ModelStats" changed name from "max_input_tps" to "queue_size".',
        'Field "4" on message "ModelStats" changed name from "max_output_tps" to "queued_input_size".',
        'Field "5" on message "ModelStats" changed name from "queue_size" to "max_output_tps".',
        'Field "6" on message "ModelStats" changed name from "queued_input_size" to "kv_cache_capacity_tokens".',
        'Field "7" on message "ModelStats" changed name from "kv_cache_capacity_tokens" to "kv_cache_used_tokens".',
        'Field "8" on message "ModelStats" changed name from "kv_cache_used_tokens" to "kv_cache_free_tokens".',
        'Field "9" on message "ModelStats" changed name from "kv_cache_free_tokens" to "num_running_queries".',
        'Field "10" on message "ModelStats" changed name from "num_running_queries" to "max_engine_concurrency".',
        'Field "11" on message "ModelStats" changed name from "max_engine_concurrency" to "total_query_input_size".',
        'Field "12" on message "ModelStats" changed name from "total_query_input_size" to "queue_time_estimate_ms_by_priority".',
        'Field "13" on message "ModelStats" changed name from "queue_time_estimate_ms_by_priority" to "input_processing_queries".',
        'Field "14" on message "ModelStats" changed name from "calibrated_max_input_tps" to "output_generation_queries".',
        'Field "15" on message "ModelStats" changed name from "input_processing_queries" to "stats_observed_at_unix_ms".',
        'Field "16" on message "ModelStats" changed name from "output_generation_queries" to "stats_capabilities".',
        'Field "17" on message "ModelStats" changed name from "stats_observed_at_unix_ms" to "stats_sources".',
        'Field "3" on message "ModelCalibrationDirective" changed name from "max_input_tps" to "last_mean_input_tps".',
    ],
    "FIELD_SAME_TYPE": [
        'Field "3" with name "queue_size" on message "ModelStats" changed type from "double" to "uint64".',
        'Field "4" with name "queued_input_size" on message "ModelStats" changed type from "double" to "uint64".',
        'Field "5" with name "max_output_tps" on message "ModelStats" changed type from "uint64" to "double".',
        'Field "12" with name "queue_time_estimate_ms_by_priority" on message "ModelStats" changed type from "uint64" to "message".',
        'Field "13" with name "input_processing_queries" on message "ModelStats" changed type from "message" to "uint64".',
        'Field "14" with name "output_generation_queries" on message "ModelStats" changed type from "double" to "uint64".',
        'Field "16" with name "stats_capabilities" on message "ModelStats" changed type from "uint64" to "string".',
        'Field "17" with name "stats_sources" on message "ModelStats" changed type from "uint64" to "string".',
    ],
}
allowed = collections.Counter(
    (PROTO_PATH, rule, message)
    for rule, messages in expected_messages_by_rule.items()
    for message in messages
)

actual = collections.Counter()
path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    for line_number, line in enumerate(handle, start=1):
        line = line.strip()
        if not line:
            continue
        try:
            violation = json.loads(line)
        except json.JSONDecodeError as error:
            print(
                f"buf breaking returned non-JSON output on line {line_number}: {error}",
                file=sys.stderr,
            )
            sys.exit(1)
        actual[
            (
                violation.get("path"),
                violation.get("type"),
                violation.get("message"),
            )
        ] += 1

extra = actual - allowed
missing = allowed - actual
if extra or missing:
    if extra:
        print("unexpected protobuf breaking changes:", file=sys.stderr)
        for (path, rule, message), count in sorted(extra.items()):
            print(f"- {path} {rule} x{count}: {message}", file=sys.stderr)
    if missing:
        print("expected clean last_mean_input_tps migration changes were not observed:", file=sys.stderr)
        for (path, rule, message), count in sorted(missing.items()):
            print(f"- {path} {rule} x{count}: {message}", file=sys.stderr)
    sys.exit(1)

print("buf breaking: only the expected clean last_mean_input_tps migration changes were detected")
PY
