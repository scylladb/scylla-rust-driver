pub mod request;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use request::Request;

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
