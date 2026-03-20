//! Routing configuration, enabling routing using `system.client_routes`.
//!
//! Enables connecting to Scylla Cloud clusters using AWS PrivateLink or GCP Private Service Connect, among others.

use scylla_cql::frame::response::event::ClientRoutesChangeEvent;

use crate::cluster::metadata::ClientRoutes;
use thiserror::Error;

/// Represents a single ClientRoutes proxy, identified by a connection ID.
///
/// The proxy is used to connect to Scylla Cloud clusters using custom routing.
/// Each proxy corresponds to a specific connection ID, and allows connecting
/// to some subset of the cluster's nodes, as determined by the configured
/// architecture setup in Scylla Cloud and by the contents of the
/// `system.client_routes` table in the system keyspace of the cluster.
///
/// The hostname for the proxy will be obtained from `system.client_routes` table,
/// using the connection ID for filtering.
/// Optionally, the hostname can be overridden with a custom one, which can
/// be useful for testing and for some cloud architectures.
#[derive(Debug, Clone)]
pub struct ClientRoutesProxy {
    #[expect(unused)] // temporarily, removed in further commit
    connection_id: String,
    overridden_hostname: Option<String>,
}

impl ClientRoutesProxy {
    /// Creates a new ClientRoutes proxy configuration with the given connection ID.
    /// The hostname will be obtained from client_routes table, using the connection ID
    /// for filtering.
    pub fn new_with_connection_id(connection_id: String) -> Self {
        Self {
            connection_id,
            overridden_hostname: None,
        }
    }

    /// Overrides the hostname obtained from client_routes table with the provided one.
    ///
    /// Useful for testing and for some cloud architectures.
    pub fn with_overridden_hostname(mut self, hostname: String) -> Self {
        self.overridden_hostname = Some(hostname);
        self
    }
}

// For convenience, allow creating ClientRoutesProxy directly from a connection ID string.
// Overriding the hostname is rare enough that it doesn't warrant a separate From implementation,
// and can be done with the builder-like API.
impl From<String> for ClientRoutesProxy {
    fn from(connection_id: String) -> Self {
        Self::new_with_connection_id(connection_id)
    }
}

/// Routing configuration for Scylla Cloud.
#[derive(Debug, Clone)]
pub struct ClientRoutesConfig {
    #[expect(unused)] // temporarily, removed in further commit
    proxies: Vec<ClientRoutesProxy>,
}

impl ClientRoutesConfig {
    /// Creates a new routing configuration for Scylla Cloud.
    pub fn new(proxies: Vec<ClientRoutesProxy>) -> Result<Self, ClientRoutesConfigError> {
        if proxies.is_empty() {
            return Err(ClientRoutesConfigError::EmptyConnectionIds);
        }
        Ok(Self { proxies })
    }
}

// TODO: consider introducing ClientRoutesConfigBuilder for more future-proof design.

/// Error that occurred when creating [ClientRoutesConfig].
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ClientRoutesConfigError {
    /// Passed no connection ids.
    #[error("Passed no connection ids")]
    EmptyConnectionIds,
}

pub(crate) trait ClientRoutesSubscriber: Send + Sync {
    /// Specifies connection IDs that are to be monitored, i.e., whose entries
    /// shall be fetched from `system.client_routes`.
    fn get_connection_ids(&self) -> &[String];

    /// Replaces the old knowledge about client routes with a new full snapshot.
    /// The snapshot contains all entries that match any of connection ids yielded by
    /// [Self::get_connection_ids]. In particular, no filtering by host ids is done.
    fn replace_client_routes(&self, client_routes: ClientRoutes);

    /// Merges existing knowledge about client routes with a partial snapshot,
    /// fetched in response to a CLIENT_ROUTES_CHANGE:UPDATE_NODES event.
    /// The snapshot contains all entries that match connection ids and host ids
    /// present in the event.
    #[expect(dead_code)] // TODO: remove once event handling is added in a further commit.
    fn merge_client_routes_update(
        &self,
        event: &ClientRoutesChangeEvent,
        client_routes: ClientRoutes,
    );
}
