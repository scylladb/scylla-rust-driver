//! CQL protocol-level error types.

/// Type of the operation rejected by rate limiting
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationType {
    Read,
    Write,
    Other(u8),
}

impl From<u8> for OperationType {
    fn from(operation_type: u8) -> OperationType {
        match operation_type {
            0 => OperationType::Read,
            1 => OperationType::Write,
            other => OperationType::Other(other),
        }
    }
}
