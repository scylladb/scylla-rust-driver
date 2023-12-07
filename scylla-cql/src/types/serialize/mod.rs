use std::{error::Error, fmt::Display, sync::Arc};

use thiserror::Error;

pub mod row;
pub mod value;
pub mod writers;

pub use writers::{
    BufBackedCellValueBuilder, BufBackedCellWriter, BufBackedRowWriter, CellValueBuilder,
    CellWriter, RowWriter,
};
#[derive(Debug, Clone, Error)]
pub struct SerializationError(Arc<dyn Error + Send + Sync>);

impl SerializationError {
    /// Constructs a new `SerializationError`.
    #[inline]
    pub fn new(err: impl Error + Send + Sync + 'static) -> SerializationError {
        SerializationError(Arc::new(err))
    }
}

impl Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerializationError: {}", self.0)
    }
}
