//! Defines the [`BoundStatement`] type, which represents a prepared statement whose values have
//! already been bound (serialized).

use std::borrow::Cow;

use scylla_cql::serialize::{
    SerializationError,
    row::{SerializeRow, SerializedValues},
};

use crate::routing::Token;

use super::prepared::{
    PartitionKey, PartitionKeyError, PartitionKeyExtractionError, PreparedStatement,
};

/// Represents a statement that already had all its values bound
#[derive(Debug, Clone)]
pub struct BoundStatement<'p> {
    pub(crate) prepared: Cow<'p, PreparedStatement>,
    pub(crate) values: SerializedValues,
}

impl<'p> BoundStatement<'p> {
    pub(crate) fn new(
        prepared: Cow<'p, PreparedStatement>,
        values: &impl SerializeRow,
    ) -> Result<Self, SerializationError> {
        let values = prepared.serialize_values(values)?;
        Ok(Self { prepared, values })
    }

    #[cfg(test)]
    /// Create a new bound statement with no values to serialize
    pub fn empty(prepared: Cow<'p, PreparedStatement>) -> Self {
        Self {
            prepared,
            values: SerializedValues::new(),
        }
    }

    /// Determines which values constitute the partition key and puts them in order.
    ///
    /// This is a preparation step necessary for calculating token based on a prepared statement.
    pub(crate) fn pk(&self) -> Result<PartitionKey<'_>, PartitionKeyExtractionError> {
        PartitionKey::new(self.prepared.get_prepared_metadata(), &self.values)
    }

    pub(crate) fn pk_and_token(
        &self,
    ) -> Result<Option<(PartitionKey<'_>, Token)>, PartitionKeyError> {
        if !self.prepared.is_token_aware() {
            return Ok(None);
        }

        let partition_key = self.pk()?;
        let token = partition_key.calculate_token(self.prepared.get_partitioner_name())?;
        Ok(Some((partition_key, token)))
    }

    /// Calculates the token for the prepared statement and its bound values
    ///
    /// Returns the token that would be computed for executing the provided prepared statement with
    /// the provided values.
    pub fn token(&self) -> Result<Option<Token>, PartitionKeyError> {
        self.pk_and_token().map(|p| p.map(|(_, t)| t))
    }

    /// Returns the prepared statement behind the `BoundStatement`
    pub fn prepared(&self) -> &PreparedStatement {
        &self.prepared
    }
}
