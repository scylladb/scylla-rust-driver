pub mod request;
pub mod response;
pub mod types;

use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt};

use std::convert::TryFrom;

use request::Request;
use response::{Response, ResponseOpcode};

// Parts of the frame header which are not determined by the request/response type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameParams {
    pub version: u8,
    pub flags: u8,
    pub stream: u16,
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

pub fn serialize_request<R: Request>(params: FrameParams, request: &R) -> Result<Bytes> {
    let mut v = Vec::new();
    v.put_u8(params.version);
    v.put_u8(params.flags);
    v.put_u16(params.stream);
    v.put_u8(R::OPCODE as u8);

    // Leave some place for the frame size
    let frame_len_pos = v.len();
    v.put_u32(0);

    request.serialize(&mut v)?;

    // Write the request length
    let frame_len_size = std::mem::size_of::<u32>();
    let body_size = v.len() - frame_len_pos - frame_len_size;
    let mut frame_len_place = &mut v[frame_len_pos..frame_len_pos + frame_len_size];

    // TODO: Return an error if the frame is too big?
    frame_len_place.put_u32(body_size as u32);

    Ok(v.into())
}

pub async fn read_response(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<(FrameParams, Response)> {
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
    assert_eq!(flags, 0);

    let stream = buf.get_u16();

    let frame_params = FrameParams {
        version,
        flags,
        stream,
    };

    let opcode = ResponseOpcode::try_from(buf.get_u8())?;

    // TODO: Guard from frames that are too large
    let length = buf.get_u32();

    // TODO: Figure out how to skip zeroing out the buffer
    let mut raw_body = vec![0u8; length as usize];
    reader.read_exact(&mut raw_body[..]).await?;

    let response = Response::deserialize(opcode, &mut &raw_body[..])?;

    Ok((frame_params, response))
}
