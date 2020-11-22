use crate::frame::response::result::PreparedMetadata;
use crate::frame::value::SerializedValues;
use bytes::{BufMut, Bytes, BytesMut};
use std::convert::TryInto;
use thiserror::Error;

/// Represents a statement prepared on the server.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    id: Bytes,
    metadata: PreparedMetadata,
    statement: String,
    page_size: Option<i32>,
}

impl PreparedStatement {
    pub fn new(id: Bytes, metadata: PreparedMetadata, statement: String) -> Self {
        Self {
            id,
            metadata,
            statement,
            page_size: None,
        }
    }

    pub fn get_id(&self) -> &Bytes {
        &self.id
    }

    pub fn get_statement(&self) -> &str {
        &self.statement
    }

    /// Sets the page size for this CQL query.
    pub fn set_page_size(&mut self, page_size: i32) {
        assert!(page_size > 0, "page size must be larger than 0");
        self.page_size = Some(page_size);
    }

    /// Disables paging for this CQL query.
    pub fn disable_paging(&mut self) {
        self.page_size = None;
    }

    /// Returns the page size for this CQL query.
    pub fn get_page_size(&self) -> Option<i32> {
        self.page_size
    }

    /// Computes the partition key of the target table from given values
    /// Partition keys have a specific serialization rules.
    /// Ref: https://github.com/scylladb/scylla/blob/40adf38915b6d8f5314c621a94d694d172360833/compound_compat.hh#L33-L47
    pub fn compute_partition_key(
        &self,
        bound_values: &SerializedValues,
    ) -> Result<Bytes, PartitionKeyError> {
        let mut buf = BytesMut::new();

        if self.metadata.pk_indexes.len() == 1 {
            if let Some(v) = bound_values
                .iter()
                .nth(self.metadata.pk_indexes[0] as usize)
                .ok_or_else(|| {
                    PartitionKeyError::NoPkIndexValue(
                        self.metadata.pk_indexes[0],
                        bound_values.len(),
                    )
                })?
            {
                buf.extend_from_slice(v);
            }
            return Ok(buf.into());
        }
        // TODO: consider what happens if a prepared statement is of type (?, something, ?),
        // where all three parameters form a partition key. The middle one is not available
        // in bound values.

        // TODO: Optimize - maybe we could check if pk_indexes are sorted and do an allocation-free algorithm then?
        // We can't just sort them because the hash will break:
        // https://github.com/apache/cassandra/blob/caeecf6456b87886a79f47a2954788e6c856697c/doc/native_protocol_v4.spec#L673

        let values: Vec<Option<&[u8]>> = bound_values.iter().collect();
        for pk_index in &self.metadata.pk_indexes {
            // Find value matching current pk_index
            let next_val: &Option<&[u8]> = values
                .get(*pk_index as usize)
                .ok_or_else(|| PartitionKeyError::NoPkIndexValue(*pk_index, bound_values.len()))?;

            // Add value's bytes
            if let Some(v) = next_val {
                let v_len_u16: u16 = v
                    .len()
                    .try_into()
                    .map_err(|_| PartitionKeyError::ValueTooLong(v.len()))?;

                buf.put_u16(v_len_u16);
                buf.extend_from_slice(v);
                buf.put_u8(0);
            }
        }
        Ok(buf.into())
    }
}

#[derive(Debug, Error, PartialEq, Eq, PartialOrd, Ord)]
pub enum PartitionKeyError {
    #[error("No value with given pk_index! pk_index: {0}, values.len(): {1}")]
    NoPkIndexValue(u16, i16),
    #[error("Value bytes too long to create partition key, max 65 535 allowed! value.len(): {0}")]
    ValueTooLong(usize),
}
