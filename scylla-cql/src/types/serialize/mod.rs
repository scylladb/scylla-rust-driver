use std::{error::Error, sync::Arc};

pub mod row;
pub mod value;
pub mod writers;

pub use writers::{
    BufBackedCellValueBuilder, BufBackedCellWriter, BufBackedRowWriter, CellValueBuilder,
    CellWriter, CountingWriter, RowWriter,
};

type SerializationError = Arc<dyn Error + Send + Sync>;
