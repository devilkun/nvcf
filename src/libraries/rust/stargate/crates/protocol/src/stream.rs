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

use crate::ProtocolError;
use crate::protocol::{
    QuicBodyOrTrailer, QuicMessage, read_body_or_trailer_from_stream, read_from_stream,
    read_header_map_from_stream, write_body_to_stream, write_header_map_to_stream,
    write_trailer_map_to_stream,
};
use quinn::{StoppedError, VarInt};
use tracing::warn;

pub struct SendStream {
    sent_headers: bool,
    send: quinn::SendStream,
    eos: bool,
}

impl SendStream {
    pub fn new(send: quinn::SendStream) -> Self {
        Self {
            sent_headers: false,
            send,
            eos: false,
        }
    }

    pub async fn send_header(&mut self, header: http::HeaderMap) -> Result<(), ProtocolError> {
        if self.sent_headers {
            return Err(ProtocolError::ProtocolViolation(
                "headers already sent".to_string(),
            ));
        }
        write_header_map_to_stream(&mut self.send, &header).await?;
        self.sent_headers = true;
        Ok(())
    }

    pub async fn send_body(&mut self, body: bytes::Bytes) -> Result<(), ProtocolError> {
        if !self.sent_headers {
            return Err(ProtocolError::ProtocolViolation(
                "must send headers before sending body".to_string(),
            ));
        }
        write_body_to_stream(&mut self.send, body).await
    }

    pub async fn send_trailer(&mut self, trailer: http::HeaderMap) -> Result<(), ProtocolError> {
        if !self.sent_headers {
            return Err(ProtocolError::ProtocolViolation(
                "must send headers before sending trailer".to_string(),
            ));
        }
        if self.eos {
            return Err(ProtocolError::ProtocolViolation(
                "stream already finished".to_string(),
            ));
        }
        write_trailer_map_to_stream(&mut self.send, &trailer).await
    }

    pub fn finish(&mut self) -> Result<(), ProtocolError> {
        if self.eos {
            return Ok(());
        }
        self.send
            .finish()
            .map_err(|e| ProtocolError::Io(std::io::Error::other(e)))?;
        self.eos = true;
        Ok(())
    }

    pub async fn stopped(&mut self) -> Result<Option<VarInt>, StoppedError> {
        self.send.stopped().await
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        if !self.eos {
            warn!("SendStream: dropped before finishing");
            let _ = self.send.reset(0_u8.into());
        }
    }
}

pub struct RecvStream {
    recv: Option<quinn::RecvStream>,
    received_header: bool,
    received_trailer: Option<http::HeaderMap>,
    eos: bool,
}

impl RecvStream {
    pub fn new(recv: quinn::RecvStream) -> Self {
        Self {
            recv: Some(recv),
            received_header: false,
            received_trailer: None,
            eos: false,
        }
    }

    pub async fn recv_header(&mut self) -> Result<http::HeaderMap, ProtocolError> {
        if self.received_header {
            return Err(ProtocolError::ProtocolViolation(
                "recv_header called more than once".to_string(),
            ));
        }
        let header_map = match read_header_map_from_stream(self.recv.as_mut().ok_or_else(|| {
            ProtocolError::ProtocolViolation("stream already stopped or dropped".to_string())
        })?)
        .await?
        {
            Some(header_map) => header_map,
            None => {
                return Err(ProtocolError::ProtocolViolation(
                    "expected header message, got none".to_string(),
                ));
            }
        };
        self.received_header = true;
        Ok(header_map)
    }

    pub async fn recv_body(&mut self) -> Result<Option<bytes::Bytes>, ProtocolError> {
        if !self.received_header {
            return Err(ProtocolError::ProtocolViolation(
                "must call recv_header once before recv_body".to_string(),
            ));
        }
        match read_body_or_trailer_from_stream(self.recv.as_mut().ok_or_else(|| {
            ProtocolError::ProtocolViolation("stream already stopped or dropped".to_string())
        })?)
        .await?
        {
            Some(QuicBodyOrTrailer::Body(body)) => Ok(Some(body)),
            Some(QuicBodyOrTrailer::Trailer(trailer)) => {
                self.received_trailer = Some(trailer);
                Ok(None)
            }
            None => {
                self.eos = true;
                Ok(None)
            }
        }
    }

    pub async fn recv_trailer(&mut self) -> Result<Option<http::HeaderMap>, ProtocolError> {
        if !self.received_header {
            return Err(ProtocolError::ProtocolViolation(
                "must call recv_header once before recv_trailer".to_string(),
            ));
        }
        let trailer = match (self.received_trailer.take(), self.eos) {
            (Some(t), _) => Some(t),
            (None, true) => None,
            (None, false) => {
                match read_body_or_trailer_from_stream(self.recv.as_mut().ok_or_else(|| {
                    ProtocolError::ProtocolViolation(
                        "stream already stopped or dropped".to_string(),
                    )
                })?)
                .await?
                {
                    Some(QuicBodyOrTrailer::Trailer(trailer)) => Some(trailer),
                    None => {
                        self.eos = true;
                        None
                    }
                    Some(QuicBodyOrTrailer::Body(_)) => {
                        return Err(ProtocolError::ProtocolViolation(
                            "expected trailer message, got body".to_string(),
                        ));
                    }
                }
            }
        };

        if !self.eos {
            match self.recv_any().await? {
                None => (),
                Some(m) => {
                    return Err(ProtocolError::ProtocolViolation(format!(
                        "expected none after trailer, got {m}"
                    )));
                }
            }
        }

        Ok(trailer)
    }

    pub async fn recv_any(&mut self) -> Result<Option<QuicMessage>, ProtocolError> {
        let recv = self.recv.as_mut().ok_or_else(|| {
            ProtocolError::ProtocolViolation("stream already stopped or dropped".to_string())
        })?;
        let reader = match read_from_stream(recv).await? {
            Some(reader) => reader,
            None => {
                self.eos = true;
                return Ok(None);
            }
        };
        let message = QuicMessage::from_reader(reader)?;
        Ok(Some(message))
    }

    pub async fn stop(mut self, code: u32) -> Option<VarInt> {
        let mut recv = self.recv.take()?;
        if recv.stop(code.into()).is_err() {
            return None;
        }
        recv.received_reset().await.unwrap_or(None)
    }
}

impl Drop for RecvStream {
    fn drop(&mut self) {
        if !self.eos {
            warn!("RecvStream: dropped before eos");
        }
        if let Some(mut recv) = self.recv.take() {
            let _ = recv.stop(0_u8.into());
        }
    }
}
