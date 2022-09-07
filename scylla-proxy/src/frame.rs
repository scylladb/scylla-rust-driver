use bytes::{Buf, BufMut, Bytes};
use scylla_cql::frame::frame_errors::FrameError;
pub use scylla_cql::frame::request::RequestOpcode;
pub use scylla_cql::frame::response::ResponseOpcode;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use tracing::warn;

const HEADER_SIZE: usize = 9;

// Parts of the frame header which are not determined by the request/response type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameParams {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
}

impl FrameParams {
    pub fn for_request(&self) -> FrameParams {
        Self {
            version: self.version & 0x7F,
            ..*self
        }
    }
    pub fn for_response(&self) -> FrameParams {
        Self {
            version: 0x80 | (self.version & 0x7F),
            ..*self
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum FrameType {
    Request,
    Response,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FrameOpcode {
    Request(RequestOpcode),
    Response(ResponseOpcode),
}

#[derive(Clone, Debug)]
pub struct RequestFrame {
    pub params: FrameParams,
    pub opcode: RequestOpcode,
    pub body: Bytes,
}

impl RequestFrame {
    pub async fn write(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), tokio::io::Error> {
        write_frame(
            self.params,
            FrameOpcode::Request(self.opcode),
            &self.body,
            writer,
        )
        .await
    }
}
#[derive(Clone, Debug)]
pub struct ResponseFrame {
    pub params: FrameParams,
    pub opcode: ResponseOpcode,
    pub body: Bytes,
}

impl ResponseFrame {
    pub(crate) async fn write(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
    ) -> Result<(), tokio::io::Error> {
        write_frame(
            self.params,
            FrameOpcode::Response(self.opcode),
            &self.body,
            writer,
        )
        .await
    }
}

pub(crate) async fn write_frame(
    params: FrameParams,
    opcode: FrameOpcode,
    body: &Bytes,
    writer: &mut (impl AsyncWrite + Unpin),
) -> Result<(), tokio::io::Error> {
    let mut header = [0; HEADER_SIZE];

    header[0] = params.version;
    header[1] = params.flags;
    header[2..=3].copy_from_slice(&params.stream.to_be_bytes());
    header[4] = match opcode {
        FrameOpcode::Request(op) => op as u8,
        FrameOpcode::Response(op) => op as u8,
    };
    header[5..9].copy_from_slice(&(body.len() as u32).to_be_bytes());

    writer.write_all(&header).await?;
    writer.write_all(body).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn read_frame(
    reader: &mut (impl AsyncRead + Unpin),
    frame_type: FrameType,
) -> Result<(FrameParams, FrameOpcode, Bytes), FrameError> {
    let mut raw_header = [0u8; HEADER_SIZE];
    reader.read_exact(&mut raw_header[..]).await?;

    let mut buf = &raw_header[..];

    let version = buf.get_u8();
    {
        let (err, valid_direction, direction_str) = match frame_type {
            FrameType::Request => (FrameError::FrameFromServer, 0x00, "request"),
            FrameType::Response => (FrameError::FrameFromClient, 0x80, "response"),
        };
        if version & 0x80 != valid_direction {
            return Err(err);
        }
        let protocol_version = version & 0x7F;
        if protocol_version != 0x04 {
            warn!(
                "Received {} with protocol version {}.",
                direction_str, protocol_version
            );
        }
    }

    let flags = buf.get_u8();
    let stream = buf.get_i16();

    let frame_params = FrameParams {
        version,
        flags,
        stream,
    };

    let opcode = match frame_type {
        FrameType::Request => FrameOpcode::Request(
            RequestOpcode::try_from(buf.get_u8()).map_err(|_| FrameError::FrameFromServer)?,
        ),
        FrameType::Response => FrameOpcode::Response(
            ResponseOpcode::try_from(buf.get_u8()).map_err(|_| FrameError::FrameFromClient)?,
        ),
    };

    let length = buf.get_u32() as usize;

    let mut body = Vec::with_capacity(length).limit(length);

    while body.has_remaining_mut() {
        let n = reader.read_buf(&mut body).await?;
        if n == 0 {
            // EOF, too early
            return Err(FrameError::ConnectionClosed(body.remaining_mut(), length));
        }
    }

    Ok((frame_params, opcode, body.into_inner().into()))
}

pub(crate) async fn read_request_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<RequestFrame, FrameError> {
    read_frame(reader, FrameType::Request)
        .await
        .map(|(params, opcode, body)| RequestFrame {
            params,
            opcode: match opcode {
                FrameOpcode::Request(op) => op,
                FrameOpcode::Response(_) => unreachable!(),
            },
            body,
        })
}

pub(crate) async fn read_response_frame(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<ResponseFrame, FrameError> {
    read_frame(reader, FrameType::Response)
        .await
        .map(|(params, opcode, body)| ResponseFrame {
            params,
            opcode: match opcode {
                FrameOpcode::Request(_) => unreachable!(),
                FrameOpcode::Response(op) => op,
            },
            body,
        })
}
