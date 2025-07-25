//! Abstractions of the CQL wire protocol:
//! - request and response frames' representation and ser/de;
//! - frame header and body;
//! - serialization and deserialization of low-level CQL protocol types;
//! - protocol features negotiation;
//! - compression, tracing, custom payload support;
//! - consistency levels;
//! - errors that can occur during the above operations.
//!

pub mod frame_errors;
pub mod protocol_features;
pub mod request;
pub mod response;
pub mod server_event_type;
pub mod types;

use bytes::{Buf, BufMut, Bytes};
use frame_errors::{
    CqlRequestSerializationError, FrameBodyExtensionsParseError, FrameHeaderParseError,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::{collections::HashMap, convert::TryFrom};

use request::SerializableRequest;
use response::ResponseOpcode;

const HEADER_SIZE: usize = 9;

pub mod flag {
    //! Frame flags

    /// The frame contains a compressed body.
    pub const COMPRESSION: u8 = 0x01;

    /// The frame contains tracing ID.
    pub const TRACING: u8 = 0x02;

    /// The frame contains a custom payload.
    pub const CUSTOM_PAYLOAD: u8 = 0x04;

    /// The frame contains warnings.
    pub const WARNING: u8 = 0x08;
}

/// All of the Authenticators supported by ScyllaDB
#[derive(Debug, PartialEq, Eq, Clone)]
// Check triggers because all variants end with "Authenticator".
// TODO(2.0): Remove the "Authenticator" postfix from variants.
#[expect(clippy::enum_variant_names)]
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

impl Compression {
    /// Returns the string representation of the compression algorithm.
    pub fn as_str(&self) -> &'static str {
        match self {
            Compression::Lz4 => "lz4",
            Compression::Snappy => "snappy",
        }
    }
}

/// Unknown compression.
#[derive(Error, Debug, Clone)]
#[error("Unknown compression: {name}")]
pub struct CompressionFromStrError {
    name: String,
}

impl FromStr for Compression {
    type Err = CompressionFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lz4" => Ok(Self::Lz4),
            "snappy" => Ok(Self::Snappy),
            other => Err(Self::Err {
                name: other.to_owned(),
            }),
        }
    }
}

impl Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A serialized CQL request frame, nearly ready to be sent over the wire.
///
/// The only difference from a real frame is that it does not contain the stream number yet.
/// The stream number is set by the `set_stream` method before sending.
pub struct SerializedRequest {
    data: Vec<u8>,
}

impl SerializedRequest {
    /// Creates a new serialized request frame from a request object.
    ///
    /// # Parameters
    /// - `req`: The request object to serialize. Must implement `SerializableRequest`.
    /// - `compression`: An optional compression algorithm to use for the request body.
    /// - `tracing`: A boolean indicating whether to request tracing information in the response.
    pub fn make<R: SerializableRequest>(
        req: &R,
        compression: Option<Compression>,
        tracing: bool,
    ) -> Result<SerializedRequest, CqlRequestSerializationError> {
        let mut flags = 0;
        let mut data = vec![0; HEADER_SIZE];

        if let Some(compression) = compression {
            flags |= flag::COMPRESSION;
            let body = req.to_bytes()?;
            compress_append(&body, compression, &mut data)?;
        } else {
            req.serialize(&mut data)?;
        }

        if tracing {
            flags |= flag::TRACING;
        }

        data[0] = 4; // We only support version 4 for now
        data[1] = flags;
        // Leave space for the stream number
        data[4] = R::OPCODE as u8;

        let req_size = (data.len() - HEADER_SIZE) as u32;
        data[5..9].copy_from_slice(&req_size.to_be_bytes());

        Ok(Self { data })
    }

    /// Sets the stream number for this request frame.
    /// Intended to be called before sending the request,
    /// once a stream ID has been assigned.
    pub fn set_stream(&mut self, stream: i16) {
        self.data[2..4].copy_from_slice(&stream.to_be_bytes());
    }

    /// Returns the serialized frame data, including the header and body.
    pub fn get_data(&self) -> &[u8] {
        &self.data[..]
    }
}

/// Parts of the frame header which are not determined by the request/response type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameParams {
    /// The version of the frame protocol. Currently, only version 4 is supported.
    /// The most significant bit (0x80) is treated specially:
    /// it indicates whether the frame is from the client or server.
    pub version: u8,

    /// Flags for the frame, indicating features like compression, tracing, etc.
    pub flags: u8,

    /// The stream ID for this frame, which allows matching requests and responses
    /// in a multiplexed connection.
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

/// Reads a response frame from the provided reader (usually, a socket).
/// Then parses and validates the frame header and extracts the body.
pub async fn read_response_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameParams, ResponseOpcode, Bytes), FrameHeaderParseError> {
    let mut raw_header = [0u8; HEADER_SIZE];
    reader
        .read_exact(&mut raw_header[..])
        .await
        .map_err(FrameHeaderParseError::HeaderIoError)?;

    let mut buf = &raw_header[..];

    // TODO: Validate version
    let version = buf.get_u8();
    if version & 0x80 != 0x80 {
        return Err(FrameHeaderParseError::FrameFromClient);
    }
    if version & 0x7F != 0x04 {
        return Err(FrameHeaderParseError::VersionNotSupported(version & 0x7f));
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
        let n = reader.read_buf(&mut raw_body).await.map_err(|err| {
            FrameHeaderParseError::BodyChunkIoError(raw_body.remaining_mut(), err)
        })?;
        if n == 0 {
            // EOF, too early
            return Err(FrameHeaderParseError::ConnectionClosed(
                raw_body.remaining_mut(),
                length,
            ));
        }
    }

    Ok((frame_params, opcode, raw_body.into_inner().into()))
}

/// Represents the already parsed response body extensions,
/// including trace ID, warnings, and custom payload,
/// and the remaining body raw data.
pub struct ResponseBodyWithExtensions {
    /// The trace ID if tracing was requested in the request.
    ///
    /// This can be used to issue a follow-up request to the server
    /// to get detailed tracing information about the request.
    pub trace_id: Option<Uuid>,

    /// Warnings returned by the server, if any.
    pub warnings: Vec<String>,

    /// Custom payload (see [the CQL protocol description of the feature](https://github.com/apache/cassandra/blob/a39f3b066f010d465a1be1038d5e06f1e31b0391/doc/native_protocol_v4.spec#L276))
    /// returned by the server, if any.
    pub custom_payload: Option<HashMap<String, Bytes>>,

    /// The remaining body data after parsing the extensions.
    pub body: Bytes,
}

/// Decompresses the response body if compression is enabled,
/// and parses any extensions like trace ID, warnings, and custom payload.
pub fn parse_response_body_extensions(
    flags: u8,
    compression: Option<Compression>,
    mut body: Bytes,
) -> Result<ResponseBodyWithExtensions, FrameBodyExtensionsParseError> {
    if flags & flag::COMPRESSION != 0 {
        if let Some(compression) = compression {
            body = decompress(&body, compression)?.into();
        } else {
            return Err(FrameBodyExtensionsParseError::NoCompressionNegotiated);
        }
    }

    let trace_id = if flags & flag::TRACING != 0 {
        let buf = &mut &*body;
        let trace_id =
            types::read_uuid(buf).map_err(FrameBodyExtensionsParseError::TraceIdParse)?;
        body.advance(16);
        Some(trace_id)
    } else {
        None
    };

    let warnings = if flags & flag::WARNING != 0 {
        let body_len = body.len();
        let buf = &mut &*body;
        let warnings = types::read_string_list(buf)
            .map_err(FrameBodyExtensionsParseError::WarningsListParse)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
        warnings
    } else {
        Vec::new()
    };

    let custom_payload = if flags & flag::CUSTOM_PAYLOAD != 0 {
        let body_len = body.len();
        let buf = &mut &*body;
        let payload_map = types::read_bytes_map(buf)
            .map_err(FrameBodyExtensionsParseError::CustomPayloadMapParse)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
        Some(payload_map)
    } else {
        None
    };

    Ok(ResponseBodyWithExtensions {
        trace_id,
        warnings,
        custom_payload,
        body,
    })
}

/// Compresses the request body using the specified compression algorithm,
/// appending the compressed data to the provided output buffer.
pub fn compress_append(
    uncomp_body: &[u8],
    compression: Compression,
    out: &mut Vec<u8>,
) -> Result<(), CqlRequestSerializationError> {
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
                .map_err(|err| CqlRequestSerializationError::SnapCompressError(Arc::new(err)))?;
            out.truncate(old_size + compressed_size);
            Ok(())
        }
    }
}

/// Deompresses the response body using the specified compression algorithm
/// and returns the decompressed data as an owned buffer.
pub fn decompress(
    mut comp_body: &[u8],
    compression: Compression,
) -> Result<Vec<u8>, FrameBodyExtensionsParseError> {
    match compression {
        Compression::Lz4 => {
            let uncomp_len = comp_body.get_u32() as usize;
            let uncomp_body = lz4_flex::decompress(comp_body, uncomp_len)
                .map_err(|err| FrameBodyExtensionsParseError::Lz4DecompressError(Arc::new(err)))?;
            Ok(uncomp_body)
        }
        Compression::Snappy => snap::raw::Decoder::new()
            .decompress_vec(comp_body)
            .map_err(|err| FrameBodyExtensionsParseError::SnapDecompressError(Arc::new(err))),
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
