pub mod request;
pub mod response;
pub mod types;
pub mod value;

use crate::transport::Compression;
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use std::convert::TryFrom;

use lz4_compression::{compress, decompress};
use request::RequestOpcode;
use response::ResponseOpcode;

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

pub async fn write_request(
    writer: &mut (impl AsyncWrite + Unpin),
    params: FrameParams,
    opcode: RequestOpcode,
    body: Bytes,
    compression: Option<Compression>,
) -> Result<()> {
    let mut v = Vec::new();
    v.put_u8(params.version);
    let compression_flag = if compression.is_some() { 0x01 } else { 0x00 };
    v.put_u8(params.flags | compression_flag);
    v.put_i16(params.stream);
    v.put_u8(opcode as u8);

    // TODO: Return an error if the frame is too big?
    v.put_u32(body.len() as u32);

    writer.write_all(&v).await?;

    if let Some(compression) = compression {
        // Compress body
        let comp_body = compress(&body, compression);
        writer.write_all(&comp_body).await?;
    } else {
        writer.write_all(&body).await?;
    }
    Ok(())
}

pub async fn read_response(
    reader: &mut (impl AsyncRead + Unpin),
    compression: Option<Compression>,
) -> Result<(FrameParams, ResponseOpcode, Bytes)> {
    let mut raw_header = [0u8; 9];
    reader.read_exact(&mut raw_header[..]).await?;

    let mut buf = &raw_header[..];

    // TODO: Validate version
    let version = buf.get_u8();
    if version & 0x80 != 0x80 {
        return Err(anyhow!("Received frame marked as coming from a client"));
    }
    if version & 0x7F != 0x04 {
        return Err(anyhow!(
            "Received a frame from version {}, but only 4 is supported",
            version & 0x7f
        ));
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
    let length = buf.get_u32();

    let body_compressed = flags & 0x01 == 0x01;

    let raw_body = read_body(reader, length as usize, compression, body_compressed).await?;

    Ok((frame_params, opcode, raw_body.into()))
}

async fn read_body(
    reader: &mut (impl AsyncRead + Unpin),
    length: usize,
    compression: Option<Compression>,
    body_compressed: bool,
) -> Result<Vec<u8>> {
    // TODO: Figure out how to skip zeroing out the buffer
    let mut raw_body = vec![0u8; length];
    reader.read_exact(&mut raw_body[..]).await?;
    if body_compressed {
        if let Some(compression) = compression {
            decompress(&raw_body, compression)
        } else {
            Err(anyhow!(
                "Frame is compressed, but no compression negotiated for connection."
            ))
        }
    } else {
        Ok(raw_body)
    }
}

fn compress(uncomp_body: &[u8], compression: Compression) -> Vec<u8> {
    match compression {
        Compression::LZ4 => {
            let mut comp_body = Vec::new();
            comp_body.put_u32((uncomp_body.len() as u32).to_be());
            compress::compress_into(uncomp_body, &mut comp_body);
            comp_body
        }
    }
}

fn decompress(comp_body: &[u8], compression: Compression) -> Result<Vec<u8>> {
    match compression {
        Compression::LZ4 => match decompress::decompress(&comp_body[4..]) {
            Ok(uncomp_body) => Ok(uncomp_body),
            Err(e) => Err(anyhow!("Frame decompression failed: {:?}", e)),
        },
    }
}
