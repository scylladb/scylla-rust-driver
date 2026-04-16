use thiserror::Error;

/// The type of a batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchType {
    /// By default, all operations in the batch are performed as logged, to ensure all mutations
    /// eventually complete (or none will). See the notes on [UNLOGGED](BatchType::Unlogged) batches for more details.
    /// A `LOGGED` batch to a single partition will be converted to an `UNLOGGED` batch as an optimization.
    Logged = 0,

    /// By default, ScyllaDB uses a batch log to ensure all operations in a batch eventually complete or none will
    /// (note, however, that operations are only isolated within a single partition).
    /// There is a performance penalty for batch atomicity when a batch spans multiple partitions. If you do not want
    /// to incur this penalty, you can tell Scylla to skip the batchlog with the `UNLOGGED` option. If the `UNLOGGED`
    /// option is used, a failed batch might leave the batch only partly applied.
    Unlogged = 1,

    /// Use the `COUNTER` option for batched counter updates. Unlike other updates in ScyllaDB, counter updates
    /// are not idempotent.
    Counter = 2,
}

/// Encountered a malformed batch type.
#[derive(Debug, Error)]
#[error("Malformed batch type: {value}")]
pub struct BatchTypeParseError {
    value: u8,
}

impl TryFrom<u8> for BatchType {
    type Error = BatchTypeParseError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Logged),
            1 => Ok(Self::Unlogged),
            2 => Ok(Self::Counter),
            _ => Err(BatchTypeParseError { value }),
        }
    }
}
