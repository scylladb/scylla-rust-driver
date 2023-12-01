use std::{error::Error, fmt::Display, sync::Arc};

use thiserror::Error;

pub mod row;
pub mod value;
pub mod writers;

pub use writers::{
    BufBackedCellValueBuilder, BufBackedCellWriter, BufBackedRowWriter, CellValueBuilder,
    CellWriter, CountingWriter, RowWriter,
};
#[derive(Debug, Clone, Error)]
pub struct SerializationError(Arc<dyn Error + Send + Sync>);

impl Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerializationError: {}", self.0)
    }
}
