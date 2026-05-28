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
use crate::common::{append_header_entry, header_value_to_str};
use crate::quic_capnp;
use bytes::Bytes;
use capnp::message::{TypedBuilder, TypedReader};
use capnp::traits::Owned;
use http::HeaderMap;
use quinn::VarInt;

pub const ALPN_PROTOCOL: &str = "stargate-quic/1";
const MAX_QUIC_HEADER_COUNT: usize = 4096;
const CAPNP_BYTES_PER_WORD: usize = 8;
const CAPNP_BODY_MESSAGE_FIXED_WORDS: usize = 4;
// Cap'n Proto stores list lengths in a 29-bit pointer field; Text/Data are byte lists.
const CAPNP_LIST_ELEMENT_COUNT_MAX: usize = (1usize << 29) - 1;
// Intra-segment pointer offsets are signed 30-bit word counts.
const CAPNP_POINTER_OFFSET_WORDS_MAX: usize = (1usize << 29) - 1;
static CAPNP_ZERO_PADDING: [u8; CAPNP_BYTES_PER_WORD - 1] = [0; CAPNP_BYTES_PER_WORD - 1];

const READER_OPTIONS: capnp::message::ReaderOptions = capnp::message::ReaderOptions {
    traversal_limit_in_words: Some(1024 * 1024 * 128), // 1GB
    nesting_limit: 64,
};

#[derive(Debug, Clone)]
pub enum QuicMessage {
    Header(QuicHeader),
    Body(QuicBody),
    Trailer(QuicTrailer),
}

impl std::fmt::Display for QuicMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuicMessage::Header(_) => write!(f, "Header"),
            QuicMessage::Body(_) => write!(f, "Body"),
            QuicMessage::Trailer(_) => write!(f, "Trailer"),
        }
    }
}

impl QuicMessage {
    pub fn to_builder(&self) -> Result<TypedBuilder<quic_capnp::message::Owned>, ProtocolError> {
        match self {
            QuicMessage::Header(header) => {
                let mut builder: TypedBuilder<quic_capnp::message::Owned> =
                    TypedBuilder::new_default();
                builder.init_root();
                let mut header_builder = builder.get_root()?.init_headers();
                header_builder
                    .reborrow()
                    .init_entries(header.entries.len() as u32);
                let mut entries_builder = header_builder.get_entries()?;
                for (i, (key, value)) in header.entries.iter().enumerate() {
                    let mut entry = entries_builder.reborrow().get(i as u32);
                    entry.set_key(key)?;
                    entry.set_value(value)?;
                }
                Ok(builder)
            }
            QuicMessage::Body(body) => {
                let mut builder: TypedBuilder<quic_capnp::message::Owned> =
                    TypedBuilder::new_default();
                builder.init_root();
                let mut body_builder = builder.get_root()?.init_body();
                body_builder.reborrow().set_content(&body.content);
                Ok(builder)
            }
            QuicMessage::Trailer(trailer) => {
                let mut builder: TypedBuilder<quic_capnp::message::Owned> =
                    TypedBuilder::new_default();
                builder.init_root();
                let mut header_builder = builder.get_root()?.init_trailers();
                header_builder
                    .reborrow()
                    .init_entries(trailer.entries.len() as u32);
                let mut entries_builder = header_builder.get_entries()?;
                for (i, (key, value)) in trailer.entries.iter().enumerate() {
                    let mut entry = entries_builder.reborrow().get(i as u32);
                    entry.set_key(key)?;
                    entry.set_value(value)?;
                }
                Ok(builder)
            }
        }
    }

    pub fn from_reader(
        reader: TypedReader<capnp::serialize::OwnedSegments, quic_capnp::message::Owned>,
    ) -> Result<Self, ProtocolError> {
        let root = reader.get()?;
        let which = root.which().map_err(|e| {
            ProtocolError::ProtocolViolation(format!("unknown QuicMessage discriminant: {e}"))
        })?;
        match which {
            quic_capnp::message::Which::Headers(Ok(headers)) => {
                let entries = headers.get_entries()?;
                let mut vec = Vec::with_capacity(entries.len() as usize);
                for entry in entries {
                    let k = entry.get_key()?.to_str().map_err(|e| {
                        ProtocolError::InvalidHeader(format!("non-UTF8 header key: {e}"))
                    })?;
                    let v = entry.get_value()?.to_str().map_err(|e| {
                        ProtocolError::InvalidHeader(format!("non-UTF8 header value: {e}"))
                    })?;
                    vec.push((k.to_owned(), v.to_owned()));
                }
                Ok(QuicMessage::Header(QuicHeader { entries: vec }))
            }
            quic_capnp::message::Which::Body(Ok(body)) => {
                let content = body.get_content()?.to_vec().into();
                Ok(QuicMessage::Body(QuicBody { content }))
            }
            quic_capnp::message::Which::Trailers(Ok(trailers)) => {
                let entries = trailers.get_entries()?;
                let mut vec = Vec::with_capacity(entries.len() as usize);
                for entry in entries {
                    let k = entry.get_key()?.to_str().map_err(|e| {
                        ProtocolError::InvalidHeader(format!("non-UTF8 trailer key: {e}"))
                    })?;
                    let v = entry.get_value()?.to_str().map_err(|e| {
                        ProtocolError::InvalidHeader(format!("non-UTF8 trailer value: {e}"))
                    })?;
                    vec.push((k.to_owned(), v.to_owned()));
                }
                Ok(QuicMessage::Trailer(QuicTrailer { entries: vec }))
            }
            _ => Err(ProtocolError::ProtocolViolation(
                "unknown QuicMessage type".to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuicHeader {
    pub entries: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct QuicBody {
    pub content: Bytes,
}

#[derive(Debug, Clone)]
pub struct QuicTrailer {
    pub entries: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum QuicBodyOrTrailer {
    Body(Bytes),
    Trailer(HeaderMap),
}

pub async fn read_from_stream<T: Owned>(
    rx: &mut quinn::RecvStream,
) -> Result<Option<TypedReader<capnp::serialize::OwnedSegments, T>>, ProtocolError> {
    let reader = capnp_futures::serialize::try_read_message(rx, READER_OPTIONS).await?;
    Ok(reader.map(|reader| TypedReader::new(reader)))
}

pub async fn write_to_stream(
    tx: &mut quinn::SendStream,
    builder: TypedBuilder<quic_capnp::message::Owned>,
) -> Result<(), ProtocolError> {
    tx.write_chunk(serialize_message(builder))
        .await
        .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
}

pub async fn write_header_map_to_stream(
    tx: &mut quinn::SendStream,
    headers: &HeaderMap,
) -> Result<(), ProtocolError> {
    tx.write_chunk(serialize_header_map_message(
        headers,
        HeaderMapMessageKind::Header,
    )?)
    .await
    .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
}

pub async fn write_trailer_map_to_stream(
    tx: &mut quinn::SendStream,
    trailers: &HeaderMap,
) -> Result<(), ProtocolError> {
    tx.write_chunk(serialize_header_map_message(
        trailers,
        HeaderMapMessageKind::Trailer,
    )?)
    .await
    .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
}

pub async fn write_body_to_stream(
    tx: &mut quinn::SendStream,
    body: Bytes,
) -> Result<(), ProtocolError> {
    let (prefix, body, padding) = body_message_chunks(body)?;
    if body.is_empty() && padding.is_empty() {
        tx.write_chunk(prefix)
            .await
            .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
    } else if padding.is_empty() {
        let mut chunks = [prefix, body];
        tx.write_all_chunks(&mut chunks)
            .await
            .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
    } else {
        let mut chunks = [prefix, body, padding];
        tx.write_all_chunks(&mut chunks)
            .await
            .map_err(|error| ProtocolError::Io(std::io::Error::other(error)))
    }
}

pub async fn read_header_map_from_stream(
    rx: &mut quinn::RecvStream,
) -> Result<Option<HeaderMap>, ProtocolError> {
    let Some(reader) = read_from_stream::<quic_capnp::message::Owned>(rx).await? else {
        return Ok(None);
    };
    let root = reader.get()?;
    let which = root.which().map_err(|e| {
        ProtocolError::ProtocolViolation(format!("unknown QuicMessage discriminant: {e}"))
    })?;
    match which {
        quic_capnp::message::Which::Headers(headers) => {
            header_map_from_capnp_map(headers?).map(Some)
        }
        quic_capnp::message::Which::Body(_) => Err(ProtocolError::ProtocolViolation(
            "expected header message, got body".to_string(),
        )),
        quic_capnp::message::Which::Trailers(_) => Err(ProtocolError::ProtocolViolation(
            "expected header message, got trailer".to_string(),
        )),
    }
}

pub async fn read_body_or_trailer_from_stream(
    rx: &mut quinn::RecvStream,
) -> Result<Option<QuicBodyOrTrailer>, ProtocolError> {
    let Some(reader) = read_from_stream::<quic_capnp::message::Owned>(rx).await? else {
        return Ok(None);
    };
    let root = reader.get()?;
    let which = root.which().map_err(|e| {
        ProtocolError::ProtocolViolation(format!("unknown QuicMessage discriminant: {e}"))
    })?;
    match which {
        quic_capnp::message::Which::Body(body) => {
            let content = body?.get_content()?.to_vec().into();
            Ok(Some(QuicBodyOrTrailer::Body(content)))
        }
        quic_capnp::message::Which::Trailers(trailers) => Ok(Some(QuicBodyOrTrailer::Trailer(
            header_map_from_capnp_map(trailers?)?,
        ))),
        quic_capnp::message::Which::Headers(_) => Err(ProtocolError::ProtocolViolation(
            "expected body message, got header".to_string(),
        )),
    }
}

fn serialize_message(builder: TypedBuilder<quic_capnp::message::Owned>) -> Bytes {
    Bytes::from(capnp::serialize::write_message_to_words(
        builder.borrow_inner(),
    ))
}

#[cfg(test)]
fn serialize_body_message(body: Bytes) -> Result<Bytes, ProtocolError> {
    let (prefix, body, padding) = body_message_chunks(body)?;
    let mut encoded = Vec::with_capacity(prefix.len() + body.len() + padding.len());
    encoded.extend_from_slice(&prefix);
    encoded.extend_from_slice(&body);
    encoded.extend_from_slice(&padding);
    Ok(Bytes::from(encoded))
}

fn serialize_header_map_message(
    headers: &HeaderMap,
    kind: HeaderMapMessageKind,
) -> Result<Bytes, ProtocolError> {
    let entries = header_map_text_entries(headers)?;
    let entry_count = entries.len();
    let entry_words = checked_header_map_words(entry_count, 2)?;
    let mut text_cursor_words = 5usize
        .checked_add(entry_words)
        .ok_or_else(|| custom_header_too_large(entry_count))?;
    let mut text_layout = Vec::with_capacity(entry_count);
    for (index, (key, value)) in entries.iter().enumerate() {
        let key_word = text_cursor_words;
        text_cursor_words = text_cursor_words
            .checked_add(capnp_text_word_len(key.len())?)
            .ok_or_else(|| custom_header_too_large(entry_count))?;
        let value_word = text_cursor_words;
        text_cursor_words = text_cursor_words
            .checked_add(capnp_text_word_len(value.len())?)
            .ok_or_else(|| custom_header_too_large(entry_count))?;
        let key_pointer_word = 5 + index * 2;
        let value_pointer_word = key_pointer_word + 1;
        checked_capnp_pointer_offset_words(key_pointer_word, key_word)?;
        checked_capnp_pointer_offset_words(value_pointer_word, value_word)?;
        text_layout.push((key_word, value_word));
    }

    let segment_words_u32 =
        u32::try_from(text_cursor_words).map_err(|_| custom_header_too_large(entry_count))?;
    let entry_count_u32 =
        u32::try_from(entry_count).map_err(|_| custom_header_too_large(entry_count))?;
    let entry_words = checked_capnp_list_element_count(entry_words, || {
        format!("custom tunnel header list too large: {entry_count} entries")
    })?;
    let total_len = CAPNP_BYTES_PER_WORD
        .checked_add(
            text_cursor_words
                .checked_mul(CAPNP_BYTES_PER_WORD)
                .ok_or_else(|| custom_header_too_large(entry_count))?,
        )
        .ok_or_else(|| custom_header_too_large(entry_count))?;
    let mut encoded = vec![0_u8; total_len];

    put_u32_le(&mut encoded, 0, 0);
    put_u32_le(&mut encoded, 4, segment_words_u32);
    put_capnp_word(&mut encoded, 0, capnp_struct_pointer_word(0, 1, 1));
    put_capnp_word(&mut encoded, 1, kind.discriminant_word());
    put_capnp_word(&mut encoded, 2, capnp_struct_pointer_word(0, 0, 1));
    put_capnp_word(
        &mut encoded,
        3,
        capnp_composite_list_pointer_word(0, entry_words),
    );
    put_capnp_word(
        &mut encoded,
        4,
        capnp_struct_list_tag_word(entry_count_u32, 0, 2),
    );

    for (index, ((key, value), (key_word, value_word))) in
        entries.iter().zip(text_layout.iter()).enumerate()
    {
        let key_pointer_word = 5 + index * 2;
        let value_pointer_word = key_pointer_word + 1;
        put_capnp_word(
            &mut encoded,
            key_pointer_word,
            capnp_text_pointer_word(key_pointer_word, *key_word, key.len())?,
        );
        put_capnp_word(
            &mut encoded,
            value_pointer_word,
            capnp_text_pointer_word(value_pointer_word, *value_word, value.len())?,
        );
        put_capnp_text(&mut encoded, *key_word, key.as_bytes());
        put_capnp_text(&mut encoded, *value_word, value.as_bytes());
    }

    Ok(Bytes::from(encoded))
}

fn body_message_chunks(body: Bytes) -> Result<(Bytes, Bytes, Bytes), ProtocolError> {
    let data_words = body.len().div_ceil(CAPNP_BYTES_PER_WORD);
    let segment_words = CAPNP_BODY_MESSAGE_FIXED_WORDS
        .checked_add(data_words)
        .ok_or_else(|| {
            ProtocolError::ProtocolViolation(format!(
                "custom tunnel body too large: {} bytes",
                body.len()
            ))
        })?;
    let segment_words_u32 = u32::try_from(segment_words).map_err(|_| {
        ProtocolError::ProtocolViolation(format!(
            "custom tunnel body too large: {} bytes",
            body.len()
        ))
    })?;
    let body_len = checked_body_list_element_count(body.len())?;

    let mut prefix = Vec::with_capacity(
        CAPNP_BYTES_PER_WORD + CAPNP_BODY_MESSAGE_FIXED_WORDS * CAPNP_BYTES_PER_WORD,
    );
    prefix.extend_from_slice(&0_u32.to_le_bytes());
    prefix.extend_from_slice(&segment_words_u32.to_le_bytes());
    push_capnp_word(&mut prefix, capnp_struct_pointer_word(0, 1, 1));
    push_capnp_word(&mut prefix, 1);
    push_capnp_word(&mut prefix, capnp_struct_pointer_word(0, 0, 1));
    push_capnp_word(&mut prefix, capnp_byte_list_pointer_word(0, body_len));
    let padding_len = data_words * CAPNP_BYTES_PER_WORD - body.len();
    let padding = Bytes::from_static(&CAPNP_ZERO_PADDING[..padding_len]);
    Ok((Bytes::from(prefix), body, padding))
}

fn push_capnp_word(out: &mut Vec<u8>, word: u64) {
    out.extend_from_slice(&word.to_le_bytes());
}

fn put_u32_le(out: &mut [u8], offset: usize, value: u32) {
    out[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_capnp_word(out: &mut [u8], segment_word_index: usize, word: u64) {
    let offset = CAPNP_BYTES_PER_WORD + segment_word_index * CAPNP_BYTES_PER_WORD;
    out[offset..offset + CAPNP_BYTES_PER_WORD].copy_from_slice(&word.to_le_bytes());
}

fn capnp_struct_pointer_word(offset_words: i32, data_words: u16, pointer_words: u16) -> u64 {
    let offset = (offset_words as u32) & 0x3fff_ffff;
    u64::from(offset << 2) | (u64::from(data_words) << 32) | (u64::from(pointer_words) << 48)
}

fn capnp_composite_list_pointer_word(offset_words: i32, word_count: u32) -> u64 {
    const CAPNP_POINTER_KIND_LIST: u64 = 1;
    const CAPNP_LIST_ELEMENT_SIZE_INLINE_COMPOSITE: u64 = 7;
    let offset = (offset_words as u32) & 0x3fff_ffff;
    CAPNP_POINTER_KIND_LIST
        | u64::from(offset << 2)
        | (CAPNP_LIST_ELEMENT_SIZE_INLINE_COMPOSITE << 32)
        | (u64::from(word_count) << 35)
}

fn capnp_struct_list_tag_word(element_count: u32, data_words: u16, pointer_words: u16) -> u64 {
    u64::from(element_count << 2) | (u64::from(data_words) << 32) | (u64::from(pointer_words) << 48)
}

fn capnp_byte_list_pointer_word(offset_words: i32, len: u32) -> u64 {
    const CAPNP_POINTER_KIND_LIST: u64 = 1;
    const CAPNP_LIST_ELEMENT_SIZE_BYTE: u64 = 2;
    let offset = (offset_words as u32) & 0x3fff_ffff;
    CAPNP_POINTER_KIND_LIST
        | u64::from(offset << 2)
        | (CAPNP_LIST_ELEMENT_SIZE_BYTE << 32)
        | (u64::from(len) << 35)
}

fn capnp_text_pointer_word(
    pointer_word_index: usize,
    text_word_index: usize,
    byte_len: usize,
) -> Result<u64, ProtocolError> {
    let offset_words = checked_capnp_pointer_offset_words(pointer_word_index, text_word_index)?;
    let len_with_nul = byte_len
        .checked_add(1)
        .ok_or_else(|| ProtocolError::ProtocolViolation("custom tunnel header too large".into()))?;
    let len_with_nul = checked_capnp_list_element_count(len_with_nul, || {
        format!("custom tunnel header field too large: {byte_len} bytes")
    })?;
    Ok(capnp_byte_list_pointer_word(offset_words, len_with_nul))
}

fn checked_capnp_pointer_offset_words(
    pointer_word_index: usize,
    target_word_index: usize,
) -> Result<i32, ProtocolError> {
    let offset_words = target_word_index
        .checked_sub(pointer_word_index + 1)
        .ok_or_else(|| {
            ProtocolError::ProtocolViolation("invalid Cap'n Proto pointer layout".into())
        })?;
    if offset_words > CAPNP_POINTER_OFFSET_WORDS_MAX {
        return Err(ProtocolError::ProtocolViolation(
            "custom tunnel header text offset too large".into(),
        ));
    }
    Ok(offset_words as i32)
}

fn put_capnp_text(out: &mut [u8], text_word_index: usize, text: &[u8]) {
    let offset = CAPNP_BYTES_PER_WORD + text_word_index * CAPNP_BYTES_PER_WORD;
    out[offset..offset + text.len()].copy_from_slice(text);
}

fn capnp_text_word_len(byte_len: usize) -> Result<usize, ProtocolError> {
    byte_len
        .checked_add(1)
        .ok_or_else(|| ProtocolError::ProtocolViolation("custom tunnel header too large".into()))
        .and_then(|len_with_nul| {
            checked_capnp_list_element_count(len_with_nul, || {
                format!("custom tunnel header field too large: {byte_len} bytes")
            })?;
            Ok(len_with_nul.div_ceil(CAPNP_BYTES_PER_WORD))
        })
}

fn checked_header_map_words(
    entry_count: usize,
    words_per_entry: usize,
) -> Result<usize, ProtocolError> {
    entry_count
        .checked_mul(words_per_entry)
        .ok_or_else(|| custom_header_too_large(entry_count))
}

fn checked_capnp_list_element_count<F>(count: usize, message: F) -> Result<u32, ProtocolError>
where
    F: FnOnce() -> String,
{
    if count > CAPNP_LIST_ELEMENT_COUNT_MAX {
        return Err(ProtocolError::ProtocolViolation(message()));
    }
    Ok(count as u32)
}

fn checked_body_list_element_count(body_len: usize) -> Result<u32, ProtocolError> {
    checked_capnp_list_element_count(body_len, || {
        format!("custom tunnel body too large: {body_len} bytes")
    })
}

fn header_map_text_entries(headers: &HeaderMap) -> Result<Vec<(&str, &str)>, ProtocolError> {
    if headers.len() > MAX_QUIC_HEADER_COUNT {
        return Err(custom_header_too_large(headers.len()));
    }
    let mut entries = Vec::with_capacity(headers.len());
    for (key, value) in headers {
        entries.push((key.as_str(), header_value_to_str(key, value)?));
    }
    Ok(entries)
}

fn custom_header_too_large(entry_count: usize) -> ProtocolError {
    ProtocolError::ProtocolViolation(format!("too many custom tunnel headers: {entry_count}"))
}

#[derive(Debug, Clone, Copy)]
enum HeaderMapMessageKind {
    Header,
    Trailer,
}

impl HeaderMapMessageKind {
    fn discriminant_word(self) -> u64 {
        match self {
            Self::Header => 0,
            Self::Trailer => 2,
        }
    }
}

#[cfg(test)]
fn header_map_message_builder(
    headers: &HeaderMap,
    kind: HeaderMapMessageKind,
) -> Result<TypedBuilder<quic_capnp::message::Owned>, ProtocolError> {
    if headers.len() > MAX_QUIC_HEADER_COUNT {
        return Err(ProtocolError::ProtocolViolation(format!(
            "too many custom tunnel headers: {}",
            headers.len()
        )));
    }
    let mut builder: TypedBuilder<quic_capnp::message::Owned> = TypedBuilder::new_default();
    builder.init_root();
    let mut root = builder.get_root()?;
    match kind {
        HeaderMapMessageKind::Header => {
            let mut header_builder = root.reborrow().init_headers();
            write_header_map_entries(header_builder.reborrow(), headers)?;
        }
        HeaderMapMessageKind::Trailer => {
            let mut trailer_builder = root.reborrow().init_trailers();
            write_header_map_entries(trailer_builder.reborrow(), headers)?;
        }
    }
    Ok(builder)
}

#[cfg(test)]
fn write_header_map_entries(
    mut header_builder: quic_capnp::map::Builder<'_, capnp::text::Owned, capnp::text::Owned>,
    headers: &HeaderMap,
) -> Result<(), ProtocolError> {
    let mut entries_builder = header_builder
        .reborrow()
        .init_entries(headers.len().try_into().expect("header count checked"));
    for (index, (key, value)) in headers.iter().enumerate() {
        let value = header_value_to_str(key, value)?;
        let mut entry = entries_builder.reborrow().get(index as u32);
        entry.set_key(key.as_str())?;
        entry.set_value(value)?;
    }
    Ok(())
}

fn header_map_from_capnp_map(
    headers: quic_capnp::map::Reader<'_, capnp::text::Owned, capnp::text::Owned>,
) -> Result<HeaderMap, ProtocolError> {
    let entries = headers.get_entries()?;
    let entry_count = entries.len() as usize;
    if entry_count > MAX_QUIC_HEADER_COUNT {
        return Err(ProtocolError::ProtocolViolation(format!(
            "too many custom tunnel headers: {entry_count}"
        )));
    }
    let mut header_map = HeaderMap::with_capacity(entry_count);
    for entry in entries {
        let key = entry
            .get_key()?
            .to_str()
            .map_err(|e| ProtocolError::InvalidHeader(format!("non-UTF8 header key: {e}")))?;
        let value = entry
            .get_value()?
            .to_str()
            .map_err(|e| ProtocolError::InvalidHeader(format!("non-UTF8 header value: {e}")))?;
        append_header_entry(&mut header_map, key, value)?;
    }
    Ok(header_map)
}

#[derive(Debug, Clone)]
pub struct HandshakeRequest {
    pub inference_server_id: String,
    pub auth_token: Option<String>,
}

impl HandshakeRequest {
    pub fn to_builder(&self) -> Result<TypedBuilder<quic_capnp::handshake::Owned>, ProtocolError> {
        let mut builder: TypedBuilder<quic_capnp::handshake::Owned> = TypedBuilder::new_default();
        builder.init_root();
        let mut root = builder.get_root()?;
        root.reborrow()
            .set_inference_server_id(&self.inference_server_id);
        if let Some(token) = &self.auth_token {
            root.reborrow().set_auth_token(token);
        }
        Ok(builder)
    }

    pub fn from_reader(
        reader: TypedReader<capnp::serialize::OwnedSegments, quic_capnp::handshake::Owned>,
    ) -> Result<Self, ProtocolError> {
        let root = reader.get()?;
        let inference_server_id = root
            .get_inference_server_id()?
            .to_str()
            .map_err(|e| {
                ProtocolError::ProtocolViolation(format!(
                    "non-UTF8 inference_server_id in handshake: {e}"
                ))
            })?
            .to_owned();
        let auth_token = if root.has_auth_token() {
            Some(
                root.get_auth_token()?
                    .to_str()
                    .map_err(|e| {
                        ProtocolError::ProtocolViolation(format!(
                            "non-UTF8 auth_token in handshake: {e}"
                        ))
                    })?
                    .to_owned(),
            )
        } else {
            None
        };
        Ok(Self {
            inference_server_id,
            auth_token,
        })
    }
}

#[derive(Debug, Clone)]
pub struct HandshakeAck {
    pub accepted: bool,
    pub reason: String,
}

impl HandshakeAck {
    pub fn to_builder(
        &self,
    ) -> Result<TypedBuilder<quic_capnp::handshake_ack::Owned>, ProtocolError> {
        let mut builder: TypedBuilder<quic_capnp::handshake_ack::Owned> =
            TypedBuilder::new_default();
        builder.init_root();
        let mut root = builder.get_root()?;
        root.reborrow().set_accepted(self.accepted);
        root.reborrow().set_reason(&self.reason);
        Ok(builder)
    }

    pub fn from_reader(
        reader: TypedReader<capnp::serialize::OwnedSegments, quic_capnp::handshake_ack::Owned>,
    ) -> Result<Self, ProtocolError> {
        let root = reader.get()?;
        let accepted = root.get_accepted();
        let reason = root
            .get_reason()?
            .to_str()
            .map_err(|e| {
                ProtocolError::ProtocolViolation(format!("non-UTF8 reason in handshake ack: {e}"))
            })?
            .to_owned();
        Ok(Self { accepted, reason })
    }
}

pub async fn write_handshake(
    tx: &mut quinn::SendStream,
    request: &HandshakeRequest,
) -> Result<(), ProtocolError> {
    let builder = request.to_builder()?;
    capnp_futures::serialize::write_message(tx, builder.borrow_inner())
        .await
        .map_err(|e| ProtocolError::Io(std::io::Error::other(e)))
}

pub async fn read_handshake(rx: &mut quinn::RecvStream) -> Result<HandshakeRequest, ProtocolError> {
    let reader: TypedReader<capnp::serialize::OwnedSegments, quic_capnp::handshake::Owned> =
        read_from_stream(rx).await?.ok_or_else(|| {
            ProtocolError::ProtocolViolation("expected handshake message, got none".to_string())
        })?;
    HandshakeRequest::from_reader(reader)
}

pub async fn write_handshake_ack(
    tx: &mut quinn::SendStream,
    ack: &HandshakeAck,
) -> Result<(), ProtocolError> {
    let builder = ack.to_builder()?;
    capnp_futures::serialize::write_message(tx, builder.borrow_inner())
        .await
        .map_err(|e| ProtocolError::Io(std::io::Error::other(e)))
}

pub async fn read_handshake_ack(rx: &mut quinn::RecvStream) -> Result<HandshakeAck, ProtocolError> {
    let reader: TypedReader<capnp::serialize::OwnedSegments, quic_capnp::handshake_ack::Owned> =
        read_from_stream(rx).await?.ok_or_else(|| {
            ProtocolError::ProtocolViolation("expected handshake ack message, got none".to_string())
        })?;
    HandshakeAck::from_reader(reader)
}

pub enum StreamStopCode {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    GoAway = 3,
}

impl From<StreamStopCode> for VarInt {
    fn from(val: StreamStopCode) -> Self {
        VarInt::from_u32(val as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &QuicMessage) -> QuicMessage {
        let builder = msg.to_builder().unwrap();
        let mut buf = Vec::new();
        capnp::serialize::write_message(&mut buf, builder.borrow_inner()).unwrap();
        let reader =
            capnp::serialize::read_message(&mut &buf[..], capnp::message::ReaderOptions::default())
                .unwrap();
        let typed_reader =
            TypedReader::<capnp::serialize::OwnedSegments, quic_capnp::message::Owned>::new(reader);
        QuicMessage::from_reader(typed_reader).unwrap()
    }

    #[test]
    fn multi_value_header_roundtrip() {
        let msg = QuicMessage::Header(QuicHeader {
            entries: vec![
                ("set-cookie".to_string(), "a=1".to_string()),
                ("set-cookie".to_string(), "b=2".to_string()),
                ("content-type".to_string(), "text/plain".to_string()),
            ],
        });
        let decoded = roundtrip(&msg);
        match decoded {
            QuicMessage::Header(h) => {
                assert_eq!(h.entries.len(), 3);
                assert_eq!(h.entries[0], ("set-cookie".to_string(), "a=1".to_string()));
                assert_eq!(h.entries[1], ("set-cookie".to_string(), "b=2".to_string()));
            }
            other => panic!("expected Header, got {other}"),
        }
    }

    #[test]
    fn multi_value_trailer_roundtrip() {
        let msg = QuicMessage::Trailer(QuicTrailer {
            entries: vec![
                ("grpc-status".to_string(), "0".to_string()),
                ("x-multi".to_string(), "first".to_string()),
                ("x-multi".to_string(), "second".to_string()),
            ],
        });
        let decoded = roundtrip(&msg);
        match decoded {
            QuicMessage::Trailer(t) => {
                assert_eq!(t.entries.len(), 3);
                assert_eq!(t.entries[1], ("x-multi".to_string(), "first".to_string()));
                assert_eq!(t.entries[2], ("x-multi".to_string(), "second".to_string()));
            }
            other => panic!("expected Trailer, got {other}"),
        }
    }

    #[test]
    fn body_roundtrip() {
        let msg = QuicMessage::Body(QuicBody {
            content: Bytes::from("hello world"),
        });
        let decoded = roundtrip(&msg);
        match decoded {
            QuicMessage::Body(b) => assert_eq!(b.content, Bytes::from("hello world")),
            other => panic!("expected Body, got {other}"),
        }
    }

    #[test]
    fn optimized_body_serialization_decodes_as_capnp_body() {
        for content in [Bytes::new(), Bytes::from_static(b"hello world")] {
            let encoded = serialize_body_message(content.clone()).unwrap();
            let reader = capnp::serialize::read_message(
                &mut &encoded[..],
                capnp::message::ReaderOptions::default(),
            )
            .unwrap();
            let typed_reader = TypedReader::<
                capnp::serialize::OwnedSegments,
                quic_capnp::message::Owned,
            >::new(reader);

            match QuicMessage::from_reader(typed_reader).unwrap() {
                QuicMessage::Body(body) => assert_eq!(body.content, content),
                other => panic!("expected Body, got {other}"),
            }
        }
    }

    #[test]
    fn optimized_body_serialization_matches_capnp_builder_wire_format() {
        for len in [0_usize, 1, 7, 8, 9, 31, 32, 33, 1024] {
            let content: Bytes = (0..len).map(|index| index as u8).collect::<Vec<_>>().into();
            let optimized = serialize_body_message(content.clone()).unwrap();
            let reference = serialize_message(
                QuicMessage::Body(QuicBody { content })
                    .to_builder()
                    .unwrap(),
            );

            assert_eq!(optimized, reference, "body length {len}");
        }
    }

    #[test]
    fn optimized_body_serialization_rejects_unrepresentable_capnp_data_length() {
        assert_eq!(
            checked_body_list_element_count(CAPNP_LIST_ELEMENT_COUNT_MAX).unwrap(),
            CAPNP_LIST_ELEMENT_COUNT_MAX as u32
        );

        let error = checked_body_list_element_count(CAPNP_LIST_ELEMENT_COUNT_MAX + 1)
            .expect_err("body length above Cap'n Proto list count must fail");
        assert!(matches!(error, ProtocolError::ProtocolViolation(message)
            if message.contains("custom tunnel body too large")));
    }

    #[test]
    fn optimized_header_serialization_matches_capnp_builder_wire_format() {
        let mut populated = HeaderMap::new();
        populated.append("x-method", "POST".parse().unwrap());
        populated.append("x-path", "/v1/chat/completions".parse().unwrap());
        populated.append("set-cookie", "a=1".parse().unwrap());
        populated.append("set-cookie", "b=2".parse().unwrap());

        for headers in [HeaderMap::new(), populated] {
            for kind in [HeaderMapMessageKind::Header, HeaderMapMessageKind::Trailer] {
                let optimized = serialize_header_map_message(&headers, kind).unwrap();
                let reference =
                    serialize_message(header_map_message_builder(&headers, kind).unwrap());

                assert_eq!(
                    optimized,
                    reference,
                    "{kind:?} with {} headers",
                    headers.len()
                );
            }
        }
    }

    #[test]
    fn optimized_header_serialization_rejects_unrepresentable_capnp_text_length() {
        let max_text_without_nul = CAPNP_LIST_ELEMENT_COUNT_MAX - 1;
        assert_eq!(
            capnp_text_word_len(max_text_without_nul).unwrap(),
            CAPNP_LIST_ELEMENT_COUNT_MAX.div_ceil(CAPNP_BYTES_PER_WORD)
        );

        let word = capnp_text_pointer_word(0, 1, max_text_without_nul).unwrap();
        assert_eq!(word >> 35, CAPNP_LIST_ELEMENT_COUNT_MAX as u64);

        let error = capnp_text_word_len(CAPNP_LIST_ELEMENT_COUNT_MAX)
            .expect_err("text length plus NUL above Cap'n Proto list count must fail");
        assert!(matches!(error, ProtocolError::ProtocolViolation(message)
            if message.contains("custom tunnel header field too large")));

        let error = capnp_text_pointer_word(0, 1, CAPNP_LIST_ELEMENT_COUNT_MAX)
            .expect_err("text pointer length plus NUL above Cap'n Proto list count must fail");
        assert!(matches!(error, ProtocolError::ProtocolViolation(message)
            if message.contains("custom tunnel header field too large")));
    }

    #[test]
    fn optimized_header_serialization_rejects_unrepresentable_capnp_text_offset() {
        let word = capnp_text_pointer_word(0, CAPNP_POINTER_OFFSET_WORDS_MAX + 1, 0).unwrap();
        assert_eq!(
            (word >> 2) & 0x3fff_ffff,
            CAPNP_POINTER_OFFSET_WORDS_MAX as u64
        );

        let error = capnp_text_pointer_word(0, CAPNP_POINTER_OFFSET_WORDS_MAX + 2, 0)
            .expect_err("text offset above Cap'n Proto signed pointer field must fail");
        assert!(matches!(error, ProtocolError::ProtocolViolation(message)
            if message.contains("custom tunnel header text offset too large")));
    }

    #[test]
    fn to_builder_returns_result() {
        let msg = QuicMessage::Body(QuicBody {
            content: Bytes::from("test"),
        });
        assert!(msg.to_builder().is_ok());
    }
}
