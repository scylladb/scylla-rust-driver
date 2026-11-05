//! CQL protocol-level representation of a `EXECUTE` request.

use crate::frame::frame_errors::CqlRequestSerializationError;
use crate::frame::protocol_features::ProtocolFeatures;
use crate::frame::response::result::cow_bytes::CowBytes;

use crate::{
    frame::request::{RequestOpcode, SerializableRequest, query},
    frame::types,
};

use super::{DeserializableRequest, RequestDeserializationError, query::QueryParameters};

use crate::frame::frame_errors::ExecuteSerializationError;

/// CQL protocol-level representation of an `EXECUTE` request,
/// used to execute a single prepared statement.
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Execute<'a> {
    /// ID of the prepared statement to execute.
    pub id: CowBytes<'a>,

    /// ID of the result metadata stored locally.
    pub result_metadata_id: Option<CowBytes<'a>>,

    /// Various parameters controlling the execution of the statement.
    pub parameters: query::QueryParameters<'a>,

    /// Trailing "tablet-version block" byte defined by the `TABLETS_ROUTING_V2` protocol
    /// extension. When `Some`, exactly one byte is appended after the query parameters.
    /// The connection layer sets this to `Some(_)` on every `EXECUTE` sent over a
    /// connection that negotiated V2 (see `Connection::execute_raw_with_consistency`), and
    /// to `None` otherwise.
    pub tablet_version_block: Option<u8>,
}

impl SerializableRequest for Execute<'_> {
    const OPCODE: RequestOpcode = RequestOpcode::Execute;

    fn serialize(&self, buf: &mut Vec<u8>) -> Result<(), CqlRequestSerializationError> {
        // Serializing statement id
        types::write_short_bytes(self.id.as_ref(), buf)
            .map_err(ExecuteSerializationError::StatementIdSerialization)?;

        // Serializing result metadata id
        if let Some(id) = self.result_metadata_id.as_ref() {
            types::write_short_bytes(id.as_ref(), buf)
                .map_err(ExecuteSerializationError::ResultMetadataIdSerialization)?;
        }

        // Serializing params
        self.parameters
            .serialize(buf)
            .map_err(ExecuteSerializationError::QueryParametersSerialization)?;

        // Serializing the trailing TABLETS_ROUTING_V2 tablet-version block. On a V2
        // connection the server reads exactly one such byte after the query parameters for
        // every EXECUTE, so the connection layer always sets this to `Some(_)` there.
        if let Some(block) = self.tablet_version_block {
            buf.push(block);
        }
        Ok(())
    }

    fn serialized_size_hint(&self) -> (usize, Option<usize>) {
        // Both ids are [short bytes].
        let id_size = size_of::<u16>() + self.id.as_ref().len();
        let result_metadata_id_size = self
            .result_metadata_id
            .as_ref()
            .map_or(0, |id| size_of::<u16>() + id.as_ref().len());
        let tablet_version_block_size = self.tablet_version_block.map_or(0, |_| size_of::<u8>());
        let size = id_size
            + result_metadata_id_size
            + self.parameters.serialized_size()
            + tablet_version_block_size;
        (size, Some(size))
    }
}

impl DeserializableRequest for Execute<'static> {
    fn deserialize(
        buf: &mut &[u8],
        features: &ProtocolFeatures,
    ) -> Result<Self, RequestDeserializationError> {
        let id = CowBytes::from(types::read_short_bytes(buf)?).into_owned();
        let result_metadata_id = if features.scylla_metadata_id_supported {
            Some(CowBytes::from(types::read_short_bytes(buf)?).into_owned())
        } else {
            None
        };
        let parameters = QueryParameters::deserialize(buf)?;

        // On a V2 connection every EXECUTE carries exactly one trailing tablet-version
        // block byte after the query parameters.
        let tablet_version_block = features
            .tablets_v2_supported
            .then(|| types::read_byte(buf))
            .transpose()?;

        Ok(Self {
            id,
            result_metadata_id,
            parameters,
            tablet_version_block,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::frame::protocol_features::ProtocolFeatures;
    use crate::frame::request::{DeserializableRequest, SerializableRequest};
    use crate::frame::response::result::cow_bytes::CowBytes;
    use crate::frame::types::Consistency;
    use crate::serialize::row::SerializedValues;

    use super::Execute;
    use super::query::{PagingState, QueryParameters};

    fn sample_parameters() -> QueryParameters<'static> {
        QueryParameters {
            consistency: Consistency::One,
            serial_consistency: None,
            timestamp: None,
            page_size: None,
            paging_state: PagingState::start(),
            skip_metadata: false,
            values: Cow::Owned(SerializedValues::new()),
        }
    }

    fn v2_features() -> ProtocolFeatures {
        let mut features = ProtocolFeatures::default();
        features.tablets_v2_supported = true;
        features
    }

    fn non_v2_features() -> ProtocolFeatures {
        let mut features = ProtocolFeatures::default();
        features.tablets_v2_supported = false;
        features
    }

    /// On a TABLETS_ROUTING_V2 connection the frame is exactly the base EXECUTE frame plus
    /// one trailing tablet-version block byte.
    #[test]
    fn execute_v2_appends_exactly_one_tablet_block_byte() {
        let with_block = Execute {
            id: CowBytes::from([1u8, 2, 3].as_slice()),
            result_metadata_id: None,
            parameters: sample_parameters(),
            tablet_version_block: Some(0xAB),
        };
        let without_block = Execute {
            id: CowBytes::from([1u8, 2, 3].as_slice()),
            result_metadata_id: None,
            parameters: sample_parameters(),
            tablet_version_block: None,
        };

        let mut buf_with = Vec::new();
        with_block.serialize(&mut buf_with).unwrap();
        let mut buf_without = Vec::new();
        without_block.serialize(&mut buf_without).unwrap();

        assert_eq!(buf_with.len(), buf_without.len() + 1);
        assert_eq!(&buf_with[..buf_without.len()], buf_without.as_slice());
        assert_eq!(*buf_with.last().unwrap(), 0xAB);
    }

    /// Serializing with a block and deserializing with the V2 feature negotiated must
    /// recover the exact same request, including the tablet block.
    #[test]
    fn execute_v2_tablet_block_roundtrip() {
        let execute = Execute {
            id: CowBytes::from([9u8, 8, 7].as_slice()),
            result_metadata_id: None,
            parameters: sample_parameters(),
            tablet_version_block: Some(0x5C),
        };

        let mut buf = Vec::new();
        execute.serialize(&mut buf).unwrap();

        let deserialized = Execute::deserialize(&mut &buf[..], &v2_features()).unwrap();
        assert_eq!(deserialized, execute);
    }

    /// Without the V2 feature negotiated, no trailing byte is expected or consumed.
    #[test]
    fn execute_v2_no_tablet_block_without_feature() {
        let execute = Execute {
            id: CowBytes::from([9u8, 8, 7].as_slice()),
            result_metadata_id: None,
            parameters: sample_parameters(),
            tablet_version_block: None,
        };

        let mut buf = Vec::new();
        execute.serialize(&mut buf).unwrap();

        let deserialized = Execute::deserialize(&mut &buf[..], &non_v2_features()).unwrap();
        assert_eq!(deserialized, execute);
        assert!(deserialized.tablet_version_block.is_none());
    }
}
