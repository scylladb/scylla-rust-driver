//! PrivateLink configuration for Scylla Cloud.
//!
//! Enables connecting to Scylla Cloud clusters using PrivateLink.

use thiserror::Error;

/// Represents a single PrivateLink endpoint, identified by a connection ID.
///
/// The endpoint is used to connect to Scylla Cloud clusters using PrivateLink.
/// Each endpoint corresponds to a specific connection ID, and allows connecting
/// to some subset of the cluster's nodes, as determined by the configured
/// architecture of the PrivateLink setup in Scylla Cloud and by the contents of the
/// client_routes table in the system keyspace of the cluster.
///
/// The hostname for the endpoint will be obtained from client_routes table,
/// using the connection ID for filtering.
/// Optionally, the hostname can be overridden with a custom one, which can
/// be useful for testing and for some cloud architectures.
#[derive(Debug, Clone)]
pub struct PrivateLinkEndpoint {
    connection_id: String,
    overriden_hostname: Option<String>,
}

impl PrivateLinkEndpoint {
    /// Creates a new PrivateLink endpoint with the given connection ID.
    /// The hostname will be obtained from client_routes table, using the connection ID
    /// for filtering.
    pub fn new_with_connection_id(connection_id: String) -> Self {
        Self {
            connection_id,
            overriden_hostname: None,
        }
    }

    /// Overrides the hostname obtained from client_routes table with the provided one.
    ///
    /// Useful for testing and for some cloud architectures.
    pub fn with_overriden_hostname(mut self, hostname: String) -> Self {
        self.overriden_hostname = Some(hostname);
        self
    }

    pub(crate) fn connection_id(&self) -> &str {
        &self.connection_id
    }

    #[expect(unused)] // temporarily, removed in further commit
    pub(crate) fn overriden_hostname(&self) -> Option<&str> {
        self.overriden_hostname.as_deref()
    }
}

// For convenience, allow creating PrivateLinkEndpoint directly from a connection ID string.
// Overriding the hostname is rare enough that it doesn't warrant a separate From implementation,
// and can be done with the builder-like API.
impl From<String> for PrivateLinkEndpoint {
    fn from(connection_id: String) -> Self {
        Self::new_with_connection_id(connection_id)
    }
}

/// PrivateLink configuration for Scylla Cloud.
#[derive(Debug, Clone)]
pub struct PrivateLinkConfig {
    #[expect(unused)] // temporarily, removed in further commit
    endpoints: Vec<PrivateLinkEndpoint>,
    /// Comma-separated list of connection IDs, for filtering client_routes table's contents.
    connection_ids: String,
    contact_points: Vec<String>,
}

impl PrivateLinkConfig {
    /// Creates a new PrivateLink configuration for Scylla Cloud.
    pub fn new(
        endpoints: impl IntoIterator<Item = impl Into<PrivateLinkEndpoint>>,
        contact_points: Vec<String>,
    ) -> Result<Self, PrivateLinkConfigError> {
        let endpoints: Vec<PrivateLinkEndpoint> = endpoints.into_iter().map(Into::into).collect();
        let connection_ids = itertools::join(
            endpoints
                .iter()
                .map(PrivateLinkEndpoint::connection_id)
                .map(|id| format!("'{}'", id)),
            ", ",
        );
        if connection_ids.is_empty() {
            return Err(PrivateLinkConfigError::EmptyConnectionIds);
        }
        Ok(Self {
            endpoints,
            connection_ids,
            contact_points,
        })
    }

    #[expect(unused)] // temporarily, removed in further commit
    pub(crate) fn connection_ids(&self) -> &str {
        &self.connection_ids
    }

    pub(crate) fn contact_points(&self) -> &[String] {
        &self.contact_points
    }
}

// TODO: consider introducing PrivateLinkConfigBuilder for more future-proof design.

/// Error that occurred when creating [PrivateLinkConfig].
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PrivateLinkConfigError {
    /// Passed no connection ids.
    #[error("Passed no connection ids")]
    EmptyConnectionIds,
}
