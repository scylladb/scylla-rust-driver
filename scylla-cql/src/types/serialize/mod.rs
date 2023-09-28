use std::{any::Any, sync::Arc};

pub mod row;
pub mod value;

type SerializationError = Arc<dyn Any + Send + Sync>;
