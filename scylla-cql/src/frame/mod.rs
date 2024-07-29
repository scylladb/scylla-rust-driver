pub mod frame_errors;
pub mod protocol_features;
pub mod request;
pub mod response;
pub mod server_event_type;
pub mod types;
pub mod value;

#[cfg(test)]
mod value_tests;

use crate::frame::frame_errors::FrameError;
use bytes::{Buf, BufMut, Bytes};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use std::fmt::Display;
use std::{collections::HashMap, convert::TryFrom};

use request::SerializableRequest;
use response::ResponseOpcode;

const HEADER_SIZE: usize = 9;

// Frame flags
const FLAG_COMPRESSION: u8 = 0x01;
const FLAG_TRACING: u8 = 0x02;
const FLAG_CUSTOM_PAYLOAD: u8 = 0x04;
const FLAG_WARNING: u8 = 0x08;

// All of the Authenticators supported by Scylla
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Authenticator {
    AllowAllAuthenticator,
    PasswordAuthenticator,
    CassandraPasswordAuthenticator,
    CassandraAllowAllAuthenticator,
    ScyllaTransitionalAuthenticator,
}

/// The wire protocol compression algorithm.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Compression {
    /// LZ4 compression algorithm.
    Lz4,
    /// Snappy compression algorithm.
    Snappy,
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::Lz4 => f.write_str("lz4"),
            Compression::Snappy => f.write_str("snappy"),
        }
    }
}

pub struct SerializedRequest {
    data: Vec<u8>,
}

impl SerializedRequest {
    pub fn make<R: SerializableRequest>(
        req: &R,
        compression: Option<Compression>,
        tracing: bool,
    ) -> Result<SerializedRequest, FrameError> {
        let mut flags = 0;
        let mut data = vec![0; HEADER_SIZE];

        if let Some(compression) = compression {
            flags |= FLAG_COMPRESSION;
            let body = req.to_bytes()?;
            compress_append(&body, compression, &mut data)?;
        } else {
            req.serialize(&mut data)?;
        }

        if tracing {
            flags |= FLAG_TRACING;
        }

        data[0] = 4; // We only support version 4 for now
        data[1] = flags;
        // Leave space for the stream number
        data[4] = R::OPCODE as u8;

        let req_size = (data.len() - HEADER_SIZE) as u32;
        data[5..9].copy_from_slice(&req_size.to_be_bytes());

        Ok(Self { data })
    }

    pub fn set_stream(&mut self, stream: i16) {
        self.data[2..4].copy_from_slice(&stream.to_be_bytes());
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data[..]
    }
}

// Parts of the frame header which are not determined by the request/response type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameParams {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
}

impl Default for FrameParams {
    fn default() -> Self {
        Self {
            version: 0x04,
            flags: 0x00,
            stream: 0,
        }
    }
}

pub async fn read_response_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameParams, ResponseOpcode, Bytes), FrameError> {
    let mut raw_header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut raw_header[..]).await?;

    let mut buf = &raw_header[..];

    // TODO: Validate version
    let version = buf.get_u8();
    if version & 0x80 != 0x80 {
        return Err(FrameError::FrameFromClient);
    }
    if version & 0x7F != 0x04 {
        return Err(FrameError::VersionNotSupported(version & 0x7f));
    }

    let flags = buf.get_u8();
    let stream = buf.get_i16();

    let frame_params = FrameParams {
        version,
        flags,
        stream,
    };

    let opcode = ResponseOpcode::try_from(buf.get_u8())?;

    // TODO: Guard from frames that are too large
    let length = buf.get_u32() as usize;

    let mut raw_body = Vec::with_capacity(length).limit(length);
    while raw_body.has_remaining_mut() {
        let n = reader.read_buf(&mut raw_body).await?;
        if n == 0 {
            // EOF, too early
            return Err(FrameError::ConnectionClosed(
                raw_body.remaining_mut(),
                length,
            ));
        }
    }

    Ok((frame_params, opcode, raw_body.into_inner().into()))
}

pub struct ResponseBodyWithExtensions {
    pub trace_id: Option<Uuid>,
    pub warnings: Vec<String>,
    pub body: Bytes,
    pub custom_payload: Option<HashMap<String, Vec<u8>>>,
}

pub fn parse_response_body_extensions(
    flags: u8,
    compression: Option<Compression>,
    mut body: Bytes,
) -> Result<ResponseBodyWithExtensions, FrameError> {
    if flags & FLAG_COMPRESSION != 0 {
        if let Some(compression) = compression {
            body = decompress(&body, compression)?.into();
        } else {
            return Err(FrameError::NoCompressionNegotiated);
        }
    }

    let trace_id = if flags & FLAG_TRACING != 0 {
        let buf = &mut &*body;
        let trace_id = types::read_uuid(buf).map_err(frame_errors::ParseError::from)?;
        body.advance(16);
        Some(trace_id)
    } else {
        None
    };

    let warnings = if flags & FLAG_WARNING != 0 {
        let body_len = body.len();
        let buf = &mut &*body;
        let warnings = types::read_string_list(buf).map_err(frame_errors::ParseError::from)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
        warnings
    } else {
        Vec::new()
    };

    let custom_payload = if flags & FLAG_CUSTOM_PAYLOAD != 0 {
        let body_len = body.len();
        let buf = &mut &*body;
        let payload_map = types::read_bytes_map(buf).map_err(frame_errors::ParseError::from)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
        Some(payload_map)
    } else {
        None
    };

    Ok(ResponseBodyWithExtensions {
        trace_id,
        warnings,
        body,
        custom_payload,
    })
}

fn compress_append(
    uncomp_body: &[u8],
    compression: Compression,
    out: &mut Vec<u8>,
) -> Result<(), FrameError> {
    match compression {
        Compression::Lz4 => {
            let uncomp_len = uncomp_body.len() as u32;
            let tmp = lz4_flex::compress(uncomp_body);
            out.reserve_exact(std::mem::size_of::<u32>() + tmp.len());
            out.put_u32(uncomp_len);
            out.extend_from_slice(&tmp[..]);
            Ok(())
        }
        Compression::Snappy => {
            let old_size = out.len();
            out.resize(old_size + snap::raw::max_compress_len(uncomp_body.len()), 0);
            let compressed_size = snap::raw::Encoder::new()
                .compress(uncomp_body, &mut out[old_size..])
                .map_err(|_| FrameError::FrameCompression)?;
            out.truncate(old_size + compressed_size);
            Ok(())
        }
    }
}

fn decompress(mut comp_body: &[u8], compression: Compression) -> Result<Vec<u8>, FrameError> {
    match compression {
        Compression::Lz4 => {
            let uncomp_len = comp_body.get_u32() as usize;
            let uncomp_body = lz4_flex::decompress(comp_body, uncomp_len)?;
            Ok(uncomp_body)
        }
        Compression::Snappy => snap::raw::Decoder::new()
            .decompress_vec(comp_body)
            .map_err(|_| FrameError::FrameDecompression),
    }
}

/// An error type for parsing an enum value from a primitive.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("No discrimant in enum `{enum_name}` matches the value `{primitive:?}`")]
pub struct TryFromPrimitiveError<T: Copy + std::fmt::Debug> {
    enum_name: &'static str,
    primitive: T,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_lz4_compress() {
        let mut out = Vec::from(&b"Hello"[..]);
        let uncomp_body = b", World!";
        let compression = Compression::Lz4;
        let expect = vec![
            72, 101, 108, 108, 111, 0, 0, 0, 8, 128, 44, 32, 87, 111, 114, 108, 100, 33,
        ];

        compress_append(uncomp_body, compression, &mut out).unwrap();
        assert_eq!(expect, out);
    }

    #[test]
    fn test_lz4_decompress() {
        let mut comp_body = Vec::new();
        let uncomp_body = "Hello, World!".repeat(100);
        let compression = Compression::Lz4;
        compress_append(uncomp_body.as_bytes(), compression, &mut comp_body).unwrap();
        let result = decompress(&comp_body[..], compression).unwrap();
        assert_eq!(32, comp_body.len());
        assert_eq!(uncomp_body.as_bytes(), result);
    }
}
