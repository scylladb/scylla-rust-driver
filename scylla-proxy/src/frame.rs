use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use scylla_cql::frame::frame_errors::FrameHeaderParseError;
use scylla_cql::frame::protocol_features::ProtocolFeatures;
pub use scylla_cql::frame::request::RequestOpcode;
use scylla_cql::frame::request::{Request, RequestDeserializationError};
pub use scylla_cql::frame::response::ResponseOpcode;
use scylla_cql::frame::{response::error::DbError, types};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use tracing::warn;

use crate::errors::ReadFrameError;
use crate::proxy::CompressionReader;

const HEADER_SIZE: usize = 9;

// Parts of the frame header which are not determined by the request/response type.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FrameParams {
    pub version: u8,
    pub flags: u8,
    pub stream: i16,
}

impl FrameParams {
    pub const fn for_request(&self) -> FrameParams {
        Self {
            version: self.version & 0x7F,
            ..*self
        }
    }
    pub const fn for_response(&self) -> FrameParams {
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
pub(crate) enum FrameOpcode {
    Request(RequestOpcode),
    Response(ResponseOpcode),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestFrame {
    pub params: FrameParams,
    pub opcode: RequestOpcode,
    pub body: Bytes,
}

impl RequestFrame {
    pub(crate) async fn write(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        compression: &CompressionReader,
    ) -> Result<(), tokio::io::Error> {
        write_frame(
            self.params,
            FrameOpcode::Request(self.opcode),
            &self.body,
            writer,
            compression,
        )
        .await
    }

    pub fn deserialize(&self) -> Result<Request<'_>, RequestDeserializationError> {
        Request::deserialize(&mut &self.body[..], self.opcode)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponseFrame {
    pub params: FrameParams,
    pub opcode: ResponseOpcode,
    pub body: Bytes,
}

impl ResponseFrame {
    /// Creates a response frame that signifies the given DbError type.
    /// Useful for testing server-side error handling in drivers.
    pub fn forged_error(
        request_params: FrameParams,
        error: DbError,
        msg: Option<&str>,
    ) -> Result<Self, std::num::TryFromIntError> {
        let msg = msg.unwrap_or("Proxy-triggered error.");
        let len_bytes = (msg.len() as u16).to_be_bytes(); // string len is a short in CQL protocol
        let code_bytes = error.code(&ProtocolFeatures::default()).to_be_bytes(); // TODO: configurable features
        let body_len = msg.len() + code_bytes.len() + len_bytes.len();
        let mut buf = BytesMut::with_capacity(body_len);

        buf.extend_from_slice(&code_bytes);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(msg.as_bytes());

        serialize_error_specific_fields(&mut buf, error)?;

        Ok(ResponseFrame {
            params: request_params.for_response(),
            opcode: ResponseOpcode::Error,
            body: buf.freeze(),
        })
    }

    /// Creates a Supported response frame with given supported options.
    pub fn forged_supported(
        request_params: FrameParams,
        options: &HashMap<String, Vec<String>>,
    ) -> Result<Self, std::num::TryFromIntError> {
        let mut buf = BytesMut::new();
        types::write_string_multimap(options, &mut buf)?;

        Ok(ResponseFrame {
            params: request_params.for_response(),
            opcode: ResponseOpcode::Supported,
            body: buf.freeze(),
        })
    }

    pub fn forged_ready(request_params: FrameParams) -> Self {
        ResponseFrame {
            params: request_params.for_response(),
            opcode: ResponseOpcode::Ready,
            body: Bytes::new(),
        }
    }

    pub(crate) async fn write(
        &self,
        writer: &mut (impl AsyncWrite + Unpin),
        compression: &CompressionReader,
    ) -> Result<(), tokio::io::Error> {
        write_frame(
            self.params,
            FrameOpcode::Response(self.opcode),
            &self.body,
            writer,
            compression,
        )
        .await
    }
}

fn serialize_error_specific_fields(
    buf: &mut BytesMut,
    error: DbError,
) -> Result<(), std::num::TryFromIntError> {
    match error {
        DbError::Unavailable {
            consistency,
            required,
            alive,
        } => {
            types::write_consistency(consistency, buf);
            types::write_int(required, buf);
            types::write_int(alive, buf);
        }
        DbError::WriteTimeout {
            consistency,
            received,
            required,
            write_type,
        } => {
            types::write_consistency(consistency, buf);
            types::write_int(received, buf);
            types::write_int(required, buf);
            types::write_string(write_type.as_str(), buf)?;
        }
        DbError::ReadTimeout {
            consistency,
            received,
            required,
            data_present,
        } => {
            types::write_consistency(consistency, buf);
            types::write_int(received, buf);
            types::write_int(required, buf);
            buf.put_u8(u8::from(data_present));
        }
        DbError::ReadFailure {
            consistency,
            received,
            required,
            numfailures,
            data_present,
        } => {
            types::write_consistency(consistency, buf);
            types::write_int(received, buf);
            types::write_int(required, buf);
            types::write_int(numfailures, buf);
            buf.put_u8(u8::from(data_present));
        }
        DbError::WriteFailure {
            consistency,
            received,
            required,
            numfailures,
            write_type,
        } => {
            types::write_consistency(consistency, buf);
            types::write_int(received, buf);
            types::write_int(required, buf);
            types::write_int(numfailures, buf);
            types::write_string(write_type.as_str(), buf)?;
        }
        DbError::FunctionFailure {
            keyspace,
            function,
            arg_types,
        } => {
            types::write_string(keyspace.as_str(), buf)?;
            types::write_string(function.as_str(), buf)?;
            types::write_string_list(&arg_types, buf)?;
        }
        DbError::AlreadyExists { keyspace, table } => {
            types::write_string(keyspace.as_str(), buf)?;
            types::write_string(table.as_str(), buf)?;
        }
        DbError::Unprepared { statement_id } => {
            types::write_short_bytes(statement_id.as_ref(), buf)?;
        }
        _ => (),
    }
    Ok(())
}

pub(crate) async fn write_frame(
    params: FrameParams,
    opcode: FrameOpcode,
    body: &[u8],
    writer: &mut (impl AsyncWrite + Unpin),
    compression: &CompressionReader,
) -> Result<(), tokio::io::Error> {
    let compressed_body = compression
        .maybe_compress_body(params.flags, body)
        .map_err(tokio::io::Error::other)?;

    let body = compressed_body.as_deref().unwrap_or(body);

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
    compression: &CompressionReader,
) -> Result<(FrameParams, FrameOpcode, Bytes), ReadFrameError> {
    let mut raw_header = [0u8; HEADER_SIZE];
    reader
        .read_exact(&mut raw_header[..])
        .await
        .map_err(FrameHeaderParseError::HeaderIoError)?;

    let mut buf = &raw_header[..];

    let version = buf.get_u8();
    {
        let (err, valid_direction, direction_str) = match frame_type {
            FrameType::Request => (FrameHeaderParseError::FrameFromServer, 0x00, "request"),
            FrameType::Response => (FrameHeaderParseError::FrameFromClient, 0x80, "response"),
        };
        if version & 0x80 != valid_direction {
            return Err(err.into());
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
            RequestOpcode::try_from(buf.get_u8())
                .map_err(|_| FrameHeaderParseError::FrameFromServer)?,
        ),
        FrameType::Response => FrameOpcode::Response(
            ResponseOpcode::try_from(buf.get_u8())
                .map_err(|_| FrameHeaderParseError::FrameFromClient)?,
        ),
    };

    let length = buf.get_u32() as usize;

    let mut body = Vec::with_capacity(length).limit(length);

    while body.has_remaining_mut() {
        let n = reader
            .read_buf(&mut body)
            .await
            .map_err(|err| FrameHeaderParseError::BodyChunkIoError(body.remaining_mut(), err))?;
        if n == 0 {
            // EOF, too early
            return Err(
                FrameHeaderParseError::ConnectionClosed(body.remaining_mut(), length).into(),
            );
        }
    }

    let body = compression.maybe_decompress_body(flags, body.into_inner().into())?;

    Ok((frame_params, opcode, body))
}

pub(crate) async fn read_request_frame(
    reader: &mut (impl AsyncRead + Unpin),
    compression: &CompressionReader,
) -> Result<RequestFrame, ReadFrameError> {
    read_frame(reader, FrameType::Request, compression)
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
    compression: &CompressionReader,
) -> Result<ResponseFrame, ReadFrameError> {
    read_frame(reader, FrameType::Response, compression)
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
