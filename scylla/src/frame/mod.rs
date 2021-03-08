pub mod frame_errors;
pub mod request;
pub mod response;
pub mod types;
pub mod value;

#[cfg(test)]
mod value_tests;

#[cfg(test)]
mod cql_collections_test;

#[cfg(test)]
mod cql_types_test;

use crate::frame::frame_errors::FrameError;
use crate::transport::Compression;
use bytes::{Buf, BufMut, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use std::convert::TryFrom;

use compress::lz4;
use request::RequestOpcode;
use response::ResponseOpcode;

// Frame flags
pub const FLAG_COMPRESSION: u8 = 0x01;
pub const FLAG_TRACING: u8 = 0x02;
pub const FLAG_CUSTOM_PAYLOAD: u8 = 0x04;
pub const FLAG_WARNING: u8 = 0x08;

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

pub async fn write_request_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    params: FrameParams,
    opcode: RequestOpcode,
    body: Bytes,
) -> Result<(), std::io::Error> {
    let mut header = [0u8; 9];
    let mut v = &mut header[..];
    v.put_u8(params.version);
    v.put_u8(params.flags);
    v.put_i16(params.stream);
    v.put_u8(opcode as u8);

    // TODO: Return an error if the frame is too big?
    v.put_u32(body.len() as u32);

    writer.write_all(&header).await?;
    writer.write_all(&body).await?;

    Ok(())
}

pub async fn read_response_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameParams, ResponseOpcode, Bytes), FrameError> {
    let mut raw_header = [0u8; 9];
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

pub struct RequestBodyWithExtensions {
    pub body: Bytes,
}

pub fn prepare_request_body_with_extensions(
    body_with_ext: RequestBodyWithExtensions,
    compression: Option<Compression>,
) -> Result<(u8, Bytes), FrameError> {
    let mut flags = 0;

    let mut body = body_with_ext.body;
    if let Some(compression) = compression {
        flags |= FLAG_COMPRESSION;
        body = compress(&body, compression)?.into();
    }

    Ok((flags, body))
}

pub struct ResponseBodyWithExtensions {
    pub trace_id: Option<Uuid>,
    pub warnings: Vec<String>,
    pub body: Bytes,
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
        let trace_id = types::read_uuid(buf)?;
        body.advance(16);
        Some(trace_id)
    } else {
        None
    };

    let warnings = if flags & FLAG_WARNING != 0 {
        let body_len = body.len();
        let buf = &mut &*body;
        let warnings = types::read_string_list(buf)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
        warnings
    } else {
        Vec::new()
    };

    if flags & FLAG_CUSTOM_PAYLOAD != 0 {
        // TODO: Do something useful with the custom payload map
        // For now, just skip it
        let body_len = body.len();
        let buf = &mut &*body;
        types::read_bytes_map(buf)?;
        let buf_len = buf.len();
        body.advance(body_len - buf_len);
    }

    Ok(ResponseBodyWithExtensions {
        trace_id,
        warnings,
        body,
    })
}

pub fn compress(uncomp_body: &[u8], compression: Compression) -> Result<Vec<u8>, FrameError> {
    match compression {
        Compression::LZ4 => {
            let uncomp_len = uncomp_body.len() as u32;
            let mut tmp =
                Vec::with_capacity(lz4::compression_bound(uncomp_len).unwrap_or(0) as usize);
            lz4::encode_block(&uncomp_body[..], &mut tmp);

            let mut comp_body = Vec::with_capacity(std::mem::size_of::<u32>() + tmp.len());
            comp_body.put_u32(uncomp_len);
            comp_body.extend_from_slice(&tmp[..]);
            Ok(comp_body)
        }
        Compression::Snappy => snap::raw::Encoder::new()
            .compress_vec(uncomp_body)
            .map_err(|_| FrameError::FrameCompression),
    }
}

pub fn decompress(mut comp_body: &[u8], compression: Compression) -> Result<Vec<u8>, FrameError> {
    match compression {
        Compression::LZ4 => {
            let uncomp_len = comp_body.get_u32() as usize;
            let mut uncomp_body = Vec::with_capacity(uncomp_len);
            if uncomp_len == 0 {
                return Ok(uncomp_body);
            }
            if lz4::decode_block(&comp_body[..], &mut uncomp_body) > 0 {
                Ok(uncomp_body)
            } else {
                Err(FrameError::LZ4BodyDecompression)
            }
        }
        Compression::Snappy => snap::raw::Decoder::new()
            .decompress_vec(comp_body)
            .map_err(|_| FrameError::FrameDecompression),
    }
}
