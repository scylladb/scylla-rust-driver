use crate::frame::response::result::PreparedMetadata;
use crate::frame::value::Value;
use bytes::{BufMut, Bytes, BytesMut};

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
    pub fn compute_partition_key(&self, bound_values: &[Value]) -> Bytes {
        let mut buf = BytesMut::new();
        if self.metadata.pk_indexes.len() == 1 {
            if let Value::Val(v) = &bound_values[self.metadata.pk_indexes[0] as usize] {
                buf.extend_from_slice(&v[..]);
            }
            return buf.into();
        }
        // TODO: consider what happens if a prepared statement is of type (?, something, ?),
        // where all three parameters form a partition key. The middle one is not available
        // in bound values.
        for &pk_index in &self.metadata.pk_indexes {
            if let Value::Val(v) = &bound_values[pk_index as usize] {
                buf.put_u16(v.len() as u16);
                buf.extend_from_slice(&v[..]);
                buf.put_u8(0);
            }
        }
        buf.into()
    }
}
