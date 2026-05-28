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

use std::path::PathBuf;
use std::str;

use sonic_rs::JsonValueTrait;

/// Provides an authentication token from a configured source.
///
/// Tokens are treated as opaque strings. This provider does not parse,
/// validate, or track token expiry. When backed by a file, the file is
/// re-read on every call so that an external process (e.g. a vault-agent
/// sidecar) can rotate the token transparently.
#[derive(Clone, Debug)]
pub enum AuthTokenProvider {
    /// A fixed token value supplied at startup.
    Static(String),
    /// A plain-text file whose trimmed contents are the token.
    File(PathBuf),
    /// A JSON file where the token is extracted by key path.
    ///
    /// `key` is a list of path segments navigated with `sonic_rs::get`.
    /// For example, `vec!["nvcfApiToken"]` reads a top-level key, while
    /// `vec!["auth", "token"]` reads a nested value.
    JsonFile { path: PathBuf, key: Vec<String> },
}

impl AuthTokenProvider {
    /// Returns the current token value.
    ///
    /// For file-backed variants the file is read on every call. Tokens are
    /// opaque -- expiry management is the responsibility of whatever process
    /// writes the backing file (typically a vault-agent sidecar).
    pub async fn get_token(&self) -> anyhow::Result<String> {
        match self {
            Self::Static(token) => Ok(token.clone()),
            Self::File(path) => {
                let bytes = tokio::fs::read(path).await.map_err(|source| {
                    anyhow::anyhow!("failed to read {}: {source}", path.display())
                })?;
                let contents = str::from_utf8(&bytes).map_err(|source| {
                    anyhow::anyhow!("token file {} is not UTF-8: {source}", path.display())
                })?;
                Ok(contents.trim().to_owned())
            }
            Self::JsonFile { path, key } => {
                let bytes = tokio::fs::read(path).await.map_err(|source| {
                    anyhow::anyhow!("failed to read {}: {source}", path.display())
                })?;
                let value = sonic_rs::get(&*bytes, key.iter().map(String::as_str))
                    .map_err(|e| anyhow::anyhow!("failed to extract key from secrets file: {e}"))?;
                let s: &str = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("token value at key path is not a string"))?;
                Ok(s.to_owned())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[tokio::test]
    async fn static_returns_literal() {
        let provider = AuthTokenProvider::Static("my-token".to_string());
        assert_eq!(provider.get_token().await.unwrap(), "my-token");
    }

    #[tokio::test]
    async fn file_reads_and_trims() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "  file-token  ").unwrap();
        let provider = AuthTokenProvider::File(tmp.path().to_path_buf());
        assert_eq!(provider.get_token().await.unwrap(), "file-token");
    }

    #[tokio::test]
    async fn json_file_top_level_key() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, r#"{{"authToken": "abc123"}}"#).unwrap();
        let provider = AuthTokenProvider::JsonFile {
            path: tmp.path().to_path_buf(),
            key: vec!["authToken".to_string()],
        };
        assert_eq!(provider.get_token().await.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn json_file_nested_key() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, r#"{{"auth": {{"token": "nested_val"}}}}"#).unwrap();
        let provider = AuthTokenProvider::JsonFile {
            path: tmp.path().to_path_buf(),
            key: vec!["auth".to_string(), "token".to_string()],
        };
        assert_eq!(provider.get_token().await.unwrap(), "nested_val");
    }

    #[tokio::test]
    async fn json_file_missing_key_errors() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, r#"{{"other": "val"}}"#).unwrap();
        let provider = AuthTokenProvider::JsonFile {
            path: tmp.path().to_path_buf(),
            key: vec!["authToken".to_string()],
        };
        assert!(provider.get_token().await.is_err());
    }

    #[tokio::test]
    async fn json_file_invalid_json_errors() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        write!(tmp, "not-json").unwrap();
        let provider = AuthTokenProvider::JsonFile {
            path: tmp.path().to_path_buf(),
            key: vec!["authToken".to_string()],
        };
        assert!(provider.get_token().await.is_err());
    }
}
