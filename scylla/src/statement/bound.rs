//! Defines the [`BoundStatement`] type, which represents a prepared statement
//! that has already been bound with values to be executed with.

use std::fmt::Debug;

use crate::routing::Token;
use crate::serialize::SerializationError;
use crate::serialize::row::{SerializeRow, SerializedValues};

use super::prepared::{PartitionKey, PartitionKeyError, PreparedStatement};

/// Represents a prepared statement together with its values already bound.
#[derive(Debug, Clone)]
pub struct BoundStatement {
    pub(crate) prepared: PreparedStatement,
    pub(crate) values: SerializedValues,
}

impl BoundStatement {
    pub(crate) fn new(
        prepared: PreparedStatement,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        Ok(Self { prepared, values })
    }

    pub(crate) fn extract_partition_key_and_calculate_token<'ps>(
        &'ps self,
    ) -> Result<Option<(PartitionKey<'ps>, Token)>, PartitionKeyError> {
        self.prepared
            .extract_partition_key_and_calculate_token(&self.values)
    }

    /// Calculates the token for the bound statement.
    ///
    /// Returns the token that would be computed for executing the provided bound statement.
    pub fn calculate_token(&self) -> Result<Option<Token>, PartitionKeyError> {
        self.extract_partition_key_and_calculate_token()
            .map(|p| p.map(|(_, t)| t))
    }

    /// Returns the prepared statement behind the `BoundStatement`.
    pub fn prepared(&self) -> &PreparedStatement {
        &self.prepared
    }
}
