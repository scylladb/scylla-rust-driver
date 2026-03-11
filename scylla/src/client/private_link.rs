//! PrivateLink configuration for Scylla Cloud.
//!
//! Enables connecting to Scylla Cloud clusters using PrivateLink.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use thiserror::Error;
use tracing::debug;

use crate::{
    cluster::{metadata::ClientRoutes, node::resolve_hostname},
    errors::TranslationError,
    policies::{
        address_translator::{AddressTranslator, UntranslatedPeer},
        client_routes_subscriber::ClientRoutesSubscriber,
    },
};

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

/// Translator for client routes: uses content of system.client_routes
/// to translate addresses of nodes in the cluster for the driver.
/// Used in PrivateLink, among others.
pub(crate) struct ClientRoutesAddressTranslator {
    config: PrivateLinkConfig,
    client_routes: ArcSwapOption<ClientRoutes>,
    hostname_resolution_timeout: Option<Duration>,
    use_tls: bool,
}

impl ClientRoutesAddressTranslator {
    pub(crate) fn new(
        config: PrivateLinkConfig,
        hostname_resolution_timeout: Option<Duration>,
        use_tls: bool,
    ) -> Self {
        Self {
            config,
            client_routes: ArcSwapOption::empty(),
            hostname_resolution_timeout,
            use_tls,
        }
    }
}

#[async_trait]
impl ClientRoutesSubscriber for ClientRoutesAddressTranslator {
    async fn update_client_routes(&self, client_routes: &Arc<ClientRoutes>) {
        self.client_routes.store(Some(Arc::clone(client_routes)));
    }
}

#[async_trait]
impl AddressTranslator for ClientRoutesAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        let client_routes_guard = self.client_routes.load();
        let Some(client_routes) = client_routes_guard.as_deref() else {
            // If we don't have client_routes data yet, we can't do any translation, so we return the original address.
            // This is OK, because there anyway can be nodes that are not covered by client_routes table,
            // and for those we want to use the original address. This is for example when some nodes belong
            // to a PrivateLink setup and some don't.
            return Ok(untranslated_peer.untranslated_address);
        };

        self.translate_with_client_routes(client_routes, untranslated_peer)
            .await
    }
}

impl ClientRoutesAddressTranslator {
    pub(crate) async fn translate_with_client_routes(
        &self,
        client_routes: &ClientRoutes,
        untranslated_peer: &UntranslatedPeer<'_>,
    ) -> Result<SocketAddr, TranslationError> {
        let host_id = untranslated_peer.host_id;

        let Some(client_route) = client_routes.get(host_id) else {
            // If the node is not present in client_routes, we return the original address.
            // This is OK, because there can be nodes that are not covered by client_routes table,
            // and for those we want to use the original address. This is for example when some nodes belong
            // to a PrivateLink setup and some don't.
            return Ok(untranslated_peer.untranslated_address);
        };

        // Check for overriden hostname first, as it takes precedence over the one from client_routes table.
        let hostname = self
            .config
            .endpoints
            .iter()
            .find_map(|endpoint| {
                if endpoint.connection_id() == client_route.connection_id {
                    endpoint.overriden_hostname()
                } else {
                    None
                }
            })
            // If no override found for this connection ID, use the hostname from client_routes table.
            // This is typical case.
            .unwrap_or(&client_route.hostname);

        let port = if self.use_tls {
            client_route
                .tls_port
                .ok_or_else(|| TranslationError::MissingPortForHost(host_id))?
        } else {
            client_route
                .port
                .ok_or_else(|| TranslationError::MissingPortForHost(host_id))?
        };
        let hostport = format!("{}:{}", hostname, port);
        let addr = resolve_hostname(&hostport, self.hostname_resolution_timeout)
            .await
            .map_err(TranslationError::DnsLookupFailed)?;

        debug!(
            "ClientRoutesAddressTranslator: translated host_id {} to address {}",
            host_id, addr
        );

        Ok(addr)
    }
}
