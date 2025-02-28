use std::{borrow::Cow, num::TryFromIntError, ops::ControlFlow, sync::Arc};

use crate::frame::{frame_errors::CqlRequestSerializationError, types::SerialConsistency};
use crate::serialize::row::SerializedValues;
use bytes::{Buf, BufMut};
use thiserror::Error;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest},
    frame::types,
};

use super::{DeserializableRequest, RequestDeserializationError};

// Query flags
const FLAG_VALUES: u8 = 0x01;
const FLAG_SKIP_METADATA: u8 = 0x02;
const FLAG_PAGE_SIZE: u8 = 0x04;
const FLAG_WITH_PAGING_STATE: u8 = 0x08;
const FLAG_WITH_SERIAL_CONSISTENCY: u8 = 0x10;
const FLAG_WITH_DEFAULT_TIMESTAMP: u8 = 0x20;
const FLAG_WITH_NAMES_FOR_VALUES: u8 = 0x40;
const ALL_FLAGS: u8 = FLAG_VALUES
    | FLAG_SKIP_METADATA
    | FLAG_PAGE_SIZE
    | FLAG_WITH_PAGING_STATE
    | FLAG_WITH_SERIAL_CONSISTENCY
    | FLAG_WITH_DEFAULT_TIMESTAMP
    | FLAG_WITH_NAMES_FOR_VALUES;

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Query<'q> {
    pub contents: Cow<'q, str>,
    pub parameters: QueryParameters<'q>,
}

impl SerializableRequest for Query<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Query;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        types::write_long_string(&self.contents, buf)
            .map_err(QuerySerializationError::StatementStringSerialization)?;
        self.parameters
            .serialize(buf)
            .map_err(QuerySerializationError::QueryParametersSerialization)?;
        Ok(())
    }
}

impl DeserializableRequest for Query<'_> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let contents = Cow::Owned(types::read_long_string(buf)?.to_owned());
        let parameters = QueryParameters::deserialize(buf)?;

        Ok(Self {
            contents,
            parameters,
        })
    }
}

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct QueryParameters<'a> {
    pub consistency: types::Consistency,
    pub serial_consistency: Option<types::SerialConsistency>,
    pub timestamp: Option<i64>,
    pub page_size: Option<i32>,
    pub paging_state: PagingState,
    pub skip_metadata: bool,
    pub values: Cow<'a, SerializedValues>,
}

impl Default for QueryParameters<'_> {
    fn default() -> Self {
        Self {
            consistency: Default::default(),
            serial_consistency: None,
            timestamp: None,
            page_size: None,
            paging_state: PagingState::start(),
            skip_metadata: false,
            values: Cow::Borrowed(SerializedValues::EMPTY),
        }
    }
}

impl QueryParameters<'_> {
    pub fn serialize(
        &self,
        buf: &mut impl BufMut,
    ) -> Result<(), QueryParametersSerializationError> {
        types::write_consistency(self.consistency, buf);

        let paging_state_bytes = self.paging_state.as_bytes_slice();

        let mut flags = 0;
        if !self.values.is_empty() {
            flags |= FLAG_VALUES;
        }

        if self.skip_metadata {
            flags |= FLAG_SKIP_METADATA;
        }

        if self.page_size.is_some() {
            flags |= FLAG_PAGE_SIZE;
        }

        if paging_state_bytes.is_some() {
            flags |= FLAG_WITH_PAGING_STATE;
        }

        if self.serial_consistency.is_some() {
            flags |= FLAG_WITH_SERIAL_CONSISTENCY;
        }

        if self.timestamp.is_some() {
            flags |= FLAG_WITH_DEFAULT_TIMESTAMP;
        }

        buf.put_u8(flags);

        if !self.values.is_empty() {
            self.values.write_to_request(buf);
        }

        if let Some(page_size) = self.page_size {
            types::write_int(page_size, buf);
        }

        if let Some(paging_state_bytes) = paging_state_bytes {
            types::write_bytes(paging_state_bytes, buf)?;
        }

        if let Some(serial_consistency) = self.serial_consistency {
            types::write_serial_consistency(serial_consistency, buf);
        }

        if let Some(timestamp) = self.timestamp {
            types::write_long(timestamp, buf);
        }

        Ok(())
    }
}

impl QueryParameters<'_> {
    pub fn deserialize(buf: &mut &[u8]) -> Result<Self, RequestDeserializationError> {
        let consistency = types::read_consistency(buf)?;

        let flags = buf.get_u8();
        let unknown_flags = flags & (!ALL_FLAGS);
        if unknown_flags != 0 {
            return Err(RequestDeserializationError::UnknownFlags {
                flags: unknown_flags,
            });
        }
        let values_flag = (flags & FLAG_VALUES) != 0;
        let skip_metadata = (flags & FLAG_SKIP_METADATA) != 0;
        let page_size_flag = (flags & FLAG_PAGE_SIZE) != 0;
        let paging_state_flag = (flags & FLAG_WITH_PAGING_STATE) != 0;
        let serial_consistency_flag = (flags & FLAG_WITH_SERIAL_CONSISTENCY) != 0;
        let default_timestamp_flag = (flags & FLAG_WITH_DEFAULT_TIMESTAMP) != 0;
        let values_have_names_flag = (flags & FLAG_WITH_NAMES_FOR_VALUES) != 0;

        if values_have_names_flag {
            return Err(RequestDeserializationError::NamedValuesUnsupported);
        }

        let values = Cow::Owned(if values_flag {
            SerializedValues::new_from_frame(buf)?
        } else {
            SerializedValues::new()
        });

        let page_size = page_size_flag.then(|| types::read_int(buf)).transpose()?;
        let paging_state = if paging_state_flag {
            PagingState::new_from_raw_bytes(types::read_bytes(buf)?)
        } else {
            PagingState::start()
        };
        let serial_consistency = serial_consistency_flag
            .then(|| types::read_consistency(buf))
            .transpose()?
            .map(
                |consistency| match SerialConsistency::try_from(consistency) {
                    Ok(serial_consistency) => Ok(serial_consistency),
                    Err(_) => Err(RequestDeserializationError::ExpectedSerialConsistency(
                        consistency,
                    )),
                },
            )
            .transpose()?;
        let timestamp = if default_timestamp_flag {
            Some(types::read_long(buf)?)
        } else {
            None
        };

        Ok(Self {
            consistency,
            serial_consistency,
            timestamp,
            page_size,
            paging_state,
            skip_metadata,
            values,
        })
    }
}

#[derive(Debug, Clone)]
pub enum PagingStateResponse {
    HasMorePages { state: PagingState },
    NoMorePages,
}

impl PagingStateResponse {
    /// Determines if the query has finished or it should be resumed with given
    /// [PagingState] in order to fetch next pages.
    #[inline]
    pub fn finished(&self) -> bool {
        matches!(*self, Self::NoMorePages)
    }

    pub(crate) fn new_from_raw_bytes(raw_paging_state: Option<&[u8]>) -> Self {
        match raw_paging_state {
            Some(raw_bytes) => Self::HasMorePages {
                state: PagingState::new_from_raw_bytes(raw_bytes),
            },
            None => Self::NoMorePages,
        }
    }

    /// Converts the response into [ControlFlow], signalling whether the query has finished
    /// or it should be resumed with given [PagingState] in order to fetch next pages.
    #[inline]
    pub fn into_paging_control_flow(self) -> ControlFlow<(), PagingState> {
        match self {
            Self::HasMorePages {
                state: next_page_handle,
            } => ControlFlow::Continue(next_page_handle),
            Self::NoMorePages => ControlFlow::Break(()),
        }
    }
}

/// The state of a paged query, i.e. where to resume fetching result rows
/// upon next request.
///
/// Cheaply clonable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagingState(Option<Arc<[u8]>>);

impl PagingState {
    /// A start state - the state of a not-yet-started paged query.
    #[inline]
    pub fn start() -> Self {
        Self(None)
    }

    /// Returns the inner representation of [PagingState].
    /// One can use this to store paging state for a longer time,
    /// and later restore it using [Self::new_from_raw_bytes].
    /// In case None is returned, this signifies
    /// [PagingState::start()] being underneath.
    #[inline]
    pub fn as_bytes_slice(&self) -> Option<&Arc<[u8]>> {
        self.0.as_ref()
    }

    /// Creates PagingState from its inner representation.
    /// One can use this to restore paging state after longer time,
    /// having previously stored it using [Self::as_bytes_slice].
    #[inline]
    pub fn new_from_raw_bytes(raw_bytes: impl Into<Arc<[u8]>>) -> Self {
        Self(Some(raw_bytes.into()))
    }
}

impl Default for PagingState {
    fn default() -> Self {
        Self::start()
    }
}

/// An error type returned when serialization of QUERY request fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum QuerySerializationError {
    /// Failed to serialize query parameters.
    #[error("Invalid query parameters: {0}")]
    QueryParametersSerialization(QueryParametersSerializationError),

    /// Failed to serialize the CQL statement string.
    #[error("Failed to serialize a statement content: {0}")]
    StatementStringSerialization(TryFromIntError),
}

/// An error type returned when serialization of query parameters fails.
#[non_exhaustive]
#[derive(Error, Debug, Clone)]
pub enum QueryParametersSerializationError {
    /// Failed to serialize paging state.
    #[error("Malformed paging state: {0}")]
    BadPagingState(#[from] TryFromIntError),
}
