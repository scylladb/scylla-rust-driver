//! Routing configuration, enabling routing using `system.client_routes`.
//!
//! Enables connecting to Scylla Cloud clusters using AWS PrivateLink or GCP Private Service Connect, among others.

use async_trait::async_trait;
use scylla_cql::frame::response::event::ClientRoutesChangeEvent;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;
use thiserror::Error;
use tracing::debug;
use uuid::Uuid;

use crate::cluster::metadata::{ClientRoute, ClientRoutes};
use crate::cluster::node::resolve_hostname;
use crate::errors::TranslationError;
use crate::policies::address_translator::{AddressTranslator, UntranslatedPeer};

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
    fn merge_client_routes_update(
        &self,
        event: &ClientRoutesChangeEvent,
        client_routes: ClientRoutes,
    );
}

/// Translator for client routes: uses content of system.client_routes
/// to translate addresses of nodes in the cluster for the driver.
/// Used in PrivateLink, among others.
pub(crate) struct ClientRoutesAddressTranslator {
    connection_ids: Vec<String>,
    hostname_overrides: HashMap<String, String>,
    client_routes: RwLock<HashMap<Uuid, KnownHostRoutes>>,
    hostname_resolution_timeout: Option<Duration>,
    use_tls: bool,
}

/// Known routes for a host, with a distinguished preferred ("sticky" route).
#[derive(Debug)]
struct KnownHostRoutes {
    sticky_route: ClientRoute,
    other_routes: HashSet<RouteCmpByConnId>,
}

/// Wrapper over ClientRoute, used to compare by connection id only.
#[derive(Debug)]
struct RouteCmpByConnId(ClientRoute);
impl PartialEq for RouteCmpByConnId {
    fn eq(&self, other: &Self) -> bool {
        self.0.connection_id == other.0.connection_id
    }
}
impl Eq for RouteCmpByConnId {}
impl std::hash::Hash for RouteCmpByConnId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.connection_id.hash(state);
    }
}
impl std::borrow::Borrow<str> for RouteCmpByConnId {
    fn borrow(&self) -> &str {
        &self.0.connection_id
    }
}

impl From<RouteCmpByConnId> for ClientRoute {
    fn from(RouteCmpByConnId(route): RouteCmpByConnId) -> Self {
        route
    }
}

impl From<ClientRoute> for RouteCmpByConnId {
    fn from(route: ClientRoute) -> Self {
        Self(route)
    }
}

impl ClientRoutesAddressTranslator {
    pub(crate) fn new(
        config: ClientRoutesConfig,
        hostname_resolution_timeout: Option<Duration>,
        use_tls: bool,
    ) -> Self {
        let connection_ids = config
            .proxies
            .iter()
            .map(|proxy| proxy.connection_id.clone())
            .collect();
        let hostname_overrides = config
            .proxies
            .into_iter()
            .filter_map(|proxy| {
                proxy
                    .overridden_hostname
                    .map(|hostname_override| (proxy.connection_id, hostname_override))
            })
            .collect();

        Self {
            connection_ids,
            hostname_overrides,
            client_routes: RwLock::new(HashMap::new()),
            hostname_resolution_timeout,
            use_tls,
        }
    }
}

// We want connection ids to be "sticky": once a connection is successfully established to a node
// using some connection id, we want the driver to continue using that connection id as a safety measure.
// See https://github.com/scylladb/scylla-rust-driver/pull/1582#discussion_r2965490779 for discussion.
impl ClientRoutesSubscriber for ClientRoutesAddressTranslator {
    fn get_connection_ids(&self) -> &[String] {
        &self.connection_ids
    }

    fn replace_client_routes(&self, mut new_routes: ClientRoutes) {
        let mut guard = self.client_routes.write().unwrap();

        // Handle existing entries, i.e. ones for hosts that we already had routes to:
        // - remove dangling routes;
        // - update routes in a sticky way, if possible;
        // - replace a route to given host with a route having different connection id, otherwise.
        guard.retain(
            |host_id, known_host_routes| match new_routes.routes.remove(host_id) {
                Some(mut new_routes_for_host) => {
                    // We have some route for that host. Let's first try to preserve connection id stickiness.

                    let (new_sticky_route, other_routes) = match new_routes_for_host
                        .remove(&known_host_routes.sticky_route.connection_id)
                    {
                        Some(new_sticky_route) => {
                            // We have the sticky route available. Let's update it.
                            (new_sticky_route, new_routes_for_host.into_values())
                        }
                        None => {
                            // Can't preserve stickiness - entry with previously used connection id is absent.
                            // Let's use any route for that host as the sticky route.
                            let mut new_routes_for_host = new_routes_for_host.into_values();
                            let Some(new_sticky_route) = new_routes_for_host.next() else {
                                // Should never happen: `new_routes_for_host` hashmap is always created as nonempty.
                                // As a defensive measure, don't panic though. It is logically correct to assume no
                                // routes for this host.
                                return false;
                            };
                            (new_sticky_route, new_routes_for_host)
                        }
                    };

                    // We always replace the sticky route because it could get updated
                    // (wrt hostname or ports, for example).
                    known_host_routes.sticky_route = new_sticky_route;

                    // Let's update non-sticky routes.
                    known_host_routes.other_routes.clear();
                    known_host_routes
                        .other_routes
                        .extend(other_routes.map(RouteCmpByConnId));

                    true
                }

                // No entry for that host in the new client routes. Let's remove the dangling entry.
                None => false,
            },
        );

        // Add entries for hosts that weren't present in the map before.
        // Entries for previously present hosts have already been removed from the new entries map.
        guard.extend(
            new_routes
                .routes
                .into_iter()
                .flat_map(|(host_id, routes_for_host)| {
                    let mut values = routes_for_host.into_values();
                    // TODO: consider if we should pick randomly here, because perhaps users expect uniform distribution
                    // over the routes to given host.
                    let Some(sticky_route) = values.next() else {
                        // Should never happen: `routes_for_host` hashmap is always created as nonempty.
                        // As a defensive measure, don't panic though. It is logically correct to assume no
                        // routes for this host.
                        return None;
                    };
                    let other_routes = values.map(RouteCmpByConnId).collect();
                    let host_routes = KnownHostRoutes {
                        sticky_route,
                        other_routes,
                    };
                    Some((host_id, host_routes))
                }),
        );
    }

    fn merge_client_routes_update(
        &self,
        event: &ClientRoutesChangeEvent,
        new_routes: ClientRoutes,
    ) {
        #[deny(clippy::wildcard_enum_match_arm)]
        let (connection_ids, host_ids) = match event {
            ClientRoutesChangeEvent::UpdateNodes {
                connection_ids,

                host_ids,
            } => (connection_ids, host_ids),
            _ => unreachable!("clippy testifies the match is exhaustive"),
        };

        let mut guard = self.client_routes.write().unwrap();

        // Iterate over all entries listed in the event.
        // Filter only those with connection ids that the driver cares about.
        // For each, determine the type of the entry change: deletion, creation or update.
        for (connection_id, &host_id) in connection_ids
            .iter()
            .zip(host_ids)
            .filter(|(connection_id, _host_id)| self.get_connection_ids().contains(connection_id))
        {
            let new_routes_for_host = new_routes.routes.get(&host_id);
            let maybe_route = new_routes_for_host
                .as_ref()
                .and_then(|host_routes| host_routes.get(connection_id).cloned());
            match maybe_route {
                None => {
                    // Route specified in the event is absent, which means that the event reported route deletion.
                    let Some(known_host_routes) = guard.get_mut(&host_id) else {
                        // We didn't have any routes for that host, so nothing to be deleted.
                        continue;
                    };

                    // If the route was in other_routes, we remove it here. Else, this is no-op.
                    known_host_routes
                        .other_routes
                        .remove(connection_id.as_str());

                    // If the route was the sticky route, we need to replace it with any from other_routes, if present.
                    // Otherwise, we remove the whole entry for that host.
                    if &known_host_routes.sticky_route.connection_id == connection_id {
                        if let Some(RouteCmpByConnId(replacement_route)) =
                            known_host_routes.other_routes.extract_if(|_| true).next()
                        {
                            known_host_routes.sticky_route = replacement_route;
                        } else {
                            guard.remove(&host_id);
                        }
                    }
                }

                Some(new_route) => {
                    // Route specified in the event is present, which means that the event reported route creation or update.
                    // In both cases, we just insert it. Let's just handle the sticky route update separately.
                    match guard.get_mut(&host_id) {
                        // There have already been routes for that host.
                        Some(host_routes) => {
                            // Let's see if the new route is the sticky one.
                            if host_routes.sticky_route.connection_id == new_route.connection_id {
                                // We update the sticky route.
                                host_routes.sticky_route = new_route;
                            } else {
                                // We add a new route or update an existing one.
                                host_routes.other_routes.insert(RouteCmpByConnId(new_route));
                            }
                        }
                        // There was no route for that host yet.
                        None => {
                            guard.insert(
                                host_id,
                                KnownHostRoutes {
                                    sticky_route: new_route,
                                    other_routes: HashSet::new(),
                                },
                            );
                        }
                    }
                }
            }
        }
    }
}

#[async_trait]
impl AddressTranslator for ClientRoutesAddressTranslator {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        let host_id = untranslated_peer.host_id;

        let hostport = {
            let client_routes_guard = self.client_routes.read().unwrap();

            let Some(host_routes) = client_routes_guard.get(&host_id) else {
                // If the node is not present in client_routes, we return an error.
                // This is crucial, because if we returned the original address, driver would attempt to connect
                // directly, which could happen to succeed and silently make driver connected not through the
                // intended proxy infrastructure (e.g., PL/PSC).
                // Note that there can be scenarios where there are nodes that are intentionally not covered
                // by client_routes table, meaning that connecting to them directly is expected.
                // This is for example when some nodes belong to a PrivateLink setup and some don't.
                // **We don't support such settings yet.**
                return Err(TranslationError::NoRuleForHost(host_id));
            };
            let client_route = &host_routes.sticky_route;

            // Check for overridden hostname first, as it takes precedence over the one from client_routes table.
            let hostname = self
                .hostname_overrides
                .get(&client_route.connection_id)
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

            format!("{}:{}", hostname, port)
        };

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

// Tests by Claude Opus 4.6, reviewed and improved by @wprzytula.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::metadata::{ClientRoute, ClientRoutes};
    use assert_matches::assert_matches;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use uuid::Uuid;

    use scylla_cql::frame::response::event::ClientRoutesChangeEvent;

    fn make_peer(host_id: Uuid, addr: SocketAddr) -> UntranslatedPeer<'static> {
        UntranslatedPeer {
            host_id,
            untranslated_address: addr,
            datacenter: None,
            rack: None,
        }
    }

    fn make_client_routes(routes: Vec<ClientRoute>) -> ClientRoutes {
        let mut cr = ClientRoutes::default();
        cr.extend(routes);
        cr
    }

    fn make_config(proxies: Vec<ClientRoutesProxy>) -> ClientRoutesConfig {
        ClientRoutesConfig::new(proxies).unwrap()
    }

    fn localhost_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    // When client_routes haven't been fetched yet (the translator was just created and
    // update_client_routes was never called), translate_address cannot perform any
    // translation. It returns an error, because the driver must not connect directly
    // to nodes that are supposed to be reached through the proxy infrastructure.
    // Contact points are not affected — they use `NodeAddr::Untranslatable`, which
    // bypasses the translator entirely.
    #[tokio::test]
    async fn translate_address_errors_when_no_client_routes() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let original_addr = localhost_addr(9042);
        let peer = make_peer(host_id, original_addr);

        let result = translator.translate_address(&peer).await;
        assert_matches!(result, Err(TranslationError::NoRuleForHost(id)) if id == host_id);
    }

    // After routes have been loaded, nodes whose host_id is not present in
    // client_routes get an error — the driver must not connect to them directly.
    #[tokio::test]
    async fn translate_address_errors_when_host_id_not_in_routes() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let route_host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id: route_host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: Some(9142),
        }]);
        translator.replace_client_routes(routes);

        // The peer has a different host_id that is not present in client_routes.
        let different_host_id = Uuid::new_v4();
        let original_addr = localhost_addr(19042);
        let peer = make_peer(different_host_id, original_addr);

        let result = translator.translate_address(&peer).await;
        assert_matches!(result, Err(TranslationError::NoRuleForHost(id)) if id == different_host_id);
    }

    // When a matching route exists and TLS is not enabled, the translator should
    // resolve the route's hostname combined with its plaintext `port` field.
    // This is the standard client routes translation path: the client_routes table
    // tells the driver which hostname and port to use for each node.
    #[tokio::test]
    async fn translate_with_client_routes_resolves_hostname_and_port() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: Some(9142),
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await.unwrap();

        assert_eq!(result, localhost_addr(9042));
    }

    // A typical custom routing setup has multiple nodes, each with its own entry in
    // the client_routes table, potentially served by different connection_ids
    // (i.e. different client routes proxies). This test verifies that when
    // multiple nodes are present in client_routes, each peer is independently
    // translated to the correct address based on its own host_id and matching
    // route. This is the standard multi-node happy path.
    #[tokio::test]
    async fn translate_address_multiple_nodes_each_resolved_independently() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-2".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id_a = Uuid::new_v4();
        let host_id_b = Uuid::new_v4();

        let routes = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_id_a,
                hostname: "127.0.0.1".to_string(),
                port: Some(9042),
                tls_port: Some(9142),
            },
            ClientRoute {
                connection_id: "conn-2".to_owned(),
                host_id: host_id_b,
                hostname: "127.0.0.2".to_string(),
                port: Some(9043),
                tls_port: Some(9143),
            },
        ]);
        translator.replace_client_routes(routes);

        // Each peer should be translated to the address from its own route,
        // regardless of the other route's contents.
        let peer_a = make_peer(host_id_a, localhost_addr(19999));
        let result_a = translator.translate_address(&peer_a).await.unwrap();
        assert_eq!(result_a, localhost_addr(9042));

        let peer_b = make_peer(host_id_b, localhost_addr(19999));
        let result_b = translator.translate_address(&peer_b).await.unwrap();
        let expected_b = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9043);
        assert_eq!(result_b, expected_b);
    }

    // When TLS is enabled (`use_tls=true`), the translator must use the route's
    // `tls_port` instead of the plaintext `port`, because the TLS proxy
    // typically listens on a different port.
    #[tokio::test]
    async fn translate_with_client_routes_uses_tls_port_when_use_tls() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, true);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: Some(9142),
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await.unwrap();

        assert_eq!(result, localhost_addr(9142));
    }

    // The client_routes table allows `port` to be null (only `tls_port` may be
    // set). If TLS is not enabled but the route has no plaintext port, the
    // translator cannot construct a valid address and must report the error
    // via MissingPortForHost so the driver can surface a clear diagnostic.
    #[tokio::test]
    async fn translate_with_client_routes_errors_when_port_missing() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: None,
            tls_port: Some(9142),
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await;

        assert_matches!(result, Err(TranslationError::MissingPortForHost(id)) if id == host_id);
    }

    // Symmetric to the test above: when TLS is enabled but the route has no
    // `tls_port`, the translator cannot construct a valid TLS address and must
    // return MissingPortForHost.
    #[tokio::test]
    async fn translate_with_client_routes_errors_when_tls_port_missing() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, true);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await;

        assert_matches!(result, Err(TranslationError::MissingPortForHost(id)) if id == host_id);
    }

    // A ClientRoutesProxy can override the hostname for a given connection_id.
    // This is useful for testing and for cloud architectures where the hostname
    // advertised in client_routes is not directly reachable from the client,
    // but an alternative hostname (e.g. a local DNS alias) is.
    // When the proxy's connection_id matches the route's, the overridden
    // hostname should take precedence over the one from client_routes.
    #[tokio::test]
    async fn translate_with_client_routes_uses_overridden_hostname() {
        let proxy = ClientRoutesProxy::new_with_connection_id("conn-1".to_string())
            .with_overridden_hostname("127.0.0.2".to_string());
        let config = make_config(vec![proxy]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await.unwrap();

        // The overridden hostname 127.0.0.2 should be used, not the route's 127.0.0.1.
        let expected = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9042);
        assert_eq!(result, expected);
    }

    // When a hostname override exists on a proxy but its connection_id does
    // not match the route's connection_id, the override must not apply. The
    // translator should fall back to the hostname from the client_routes table.
    // This verifies that overrides are scoped to matching connection_ids only.
    #[tokio::test]
    async fn translate_with_client_routes_falls_back_to_route_hostname_when_no_override() {
        // "conn-other" has an override but won't match the route's "conn-1".
        let proxy = ClientRoutesProxy::new_with_connection_id("conn-other".to_string())
            .with_overridden_hostname("127.0.0.2".to_string());
        let config = make_config(vec![
            proxy,
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes);

        let peer = make_peer(host_id, localhost_addr(19999));
        let result = translator.translate_address(&peer).await.unwrap();

        // No override matched "conn-1", so the route's hostname (127.0.0.1) is used.
        assert_eq!(result, localhost_addr(9042));
    }

    // The ClientRoutesSubscriber trait allows the cluster worker to push updated
    // client_routes to the translator after each metadata refresh. This test
    // verifies the full lifecycle: before any routes are stored, the translator
    // returns an error (no translation possible); after replace_client_routes
    // is called, the translator uses the stored routes for subsequent
    // translate_address calls.
    #[tokio::test]
    async fn update_client_routes_makes_subsequent_translations_use_routes() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let original_addr = localhost_addr(19999);
        let peer = make_peer(host_id, original_addr);

        // Before updating: no routes stored, so translation fails.
        let result = translator.translate_address(&peer).await;
        assert_matches!(result, Err(TranslationError::NoRuleForHost(id)) if id == host_id);

        // Push client routes, simulating what the cluster worker does after
        // fetching the system.client_routes table.
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes);

        // After updating: the route is found and the translated address is returned.
        let result = translator.translate_address(&peer).await.unwrap();
        assert_eq!(result, localhost_addr(9042));

        // Push a second update with a different port, simulating a subsequent
        // metadata refresh that returned changed routing information.
        let updated_routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9043),
            tls_port: None,
        }]);
        translator.replace_client_routes(updated_routes);

        // The translator should reflect the second update, not remain stuck
        // on the first one.
        let result = translator.translate_address(&peer).await.unwrap();
        assert_eq!(result, localhost_addr(9043));
    }

    // When host_id and connection_id stay the same but non-key properties
    // (hostname, port) change, the translator's map must reflect those
    // updates. Verified via both full replacement and merge event.
    #[tokio::test]
    async fn non_key_property_updates_are_reflected() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let peer = make_peer(host_id, localhost_addr(19999));

        // Initial state: hostname 127.0.0.1, port 9042.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042),
        );

        // --- Full replacement path ---

        // Port changes.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9043),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9043),
        );

        // Hostname changes (127.0.0.1 →  127.0.0.2).
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.2".to_string(),
            port: Some(9043),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 9043),
        );

        // Both hostname and port change simultaneously.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.3".to_string(),
            port: Some(9044),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 9044),
        );

        // --- Merge event path ---

        let make_event = || ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned()],
            host_ids: vec![host_id],
        };

        // Port changes via merge.
        translator.merge_client_routes_update(
            &make_event(),
            make_client_routes(vec![ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id,
                hostname: "127.0.0.3".to_string(),
                port: Some(9050),
                tls_port: None,
            }]),
        );
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 9050),
        );

        // Hostname changes via merge.
        translator.merge_client_routes_update(
            &make_event(),
            make_client_routes(vec![ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id,
                hostname: "127.0.0.4".to_string(),
                port: Some(9050),
                tls_port: None,
            }]),
        );
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 4)), 9050),
        );

        // Both hostname and port change via merge.
        translator.merge_client_routes_update(
            &make_event(),
            make_client_routes(vec![ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id,
                hostname: "127.0.0.5".to_string(),
                port: Some(9060),
                tls_port: None,
            }]),
        );
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 5)), 9060),
        );
    }

    // `replace_client_routes` receives a full snapshot of the client_routes
    // table (filtered by connection ids, of course).  Hosts that were previously
    // known but are absent from the new snapshot must be removed, so the translator
    // falls back to the original (untranslated) address for them.
    #[tokio::test]
    async fn replace_client_routes_removes_hosts_absent_from_snapshot() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();

        // Initial snapshot: both hosts present.
        let routes = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_a,
                hostname: "127.0.0.1".to_string(),
                port: Some(9042),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_b,
                hostname: "127.0.0.1".to_string(),
                port: Some(9043),
                tls_port: None,
            },
        ]);
        translator.replace_client_routes(routes);

        // Both should translate.
        let peer_a = make_peer(host_a, localhost_addr(19999));
        let peer_b = make_peer(host_b, localhost_addr(19998));
        assert_eq!(
            translator.translate_address(&peer_a).await.unwrap(),
            localhost_addr(9042)
        );
        assert_eq!(
            translator.translate_address(&peer_b).await.unwrap(),
            localhost_addr(9043)
        );

        // Second snapshot: only host_a remains.
        let routes2 = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id: host_a,
            hostname: "127.0.0.1".to_string(),
            port: Some(9044),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes2);

        // host_a got the updated port.
        assert_eq!(
            translator.translate_address(&peer_a).await.unwrap(),
            localhost_addr(9044)
        );
        // host_b was removed →  translation fails.
        assert_matches!(
            translator.translate_address(&peer_b).await,
            Err(TranslationError::NoRuleForHost(id)) if id == host_b
        );
    }

    // `merge_client_routes_update` receives a partial update (triggered by a
    // CLIENT_ROUTES_CHANGED event) that covers only the affected hosts.
    // Hosts absent from the partial update must be preserved — they are not
    // being removed from the cluster, they simply weren't part of this event.
    #[tokio::test]
    async fn merge_client_routes_update_preserves_hosts_absent_from_update() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();

        // Initial full snapshot: both hosts present.
        let routes = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_a,
                hostname: "127.0.0.1".to_string(),
                port: Some(9042),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_b,
                hostname: "127.0.0.1".to_string(),
                port: Some(9043),
                tls_port: None,
            },
        ]);
        translator.replace_client_routes(routes);

        // Partial update: only host_a changed its port.
        // The event mentions only host_a; host_b is unaffected.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned()],
            host_ids: vec![host_a],
        };
        let partial = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id: host_a,
            hostname: "127.0.0.1".to_string(),
            port: Some(9044),
            tls_port: None,
        }]);
        translator.merge_client_routes_update(&event, partial);

        // host_a got the updated port.
        let peer_a = make_peer(host_a, localhost_addr(19999));
        assert_eq!(
            translator.translate_address(&peer_a).await.unwrap(),
            localhost_addr(9044)
        );
        // host_b still translates with the original port (not removed).
        let peer_b = make_peer(host_b, localhost_addr(19998));
        assert_eq!(
            translator.translate_address(&peer_b).await.unwrap(),
            localhost_addr(9043)
        );
    }

    // When the event mentions a (connection_id, host_id) pair but the
    // re-fetched ClientRoutes no longer contains a matching entry, the
    // translator removes that host from its map.  This happens when a node
    // is decommissioned: the event says "this route changed", but the
    // follow-up query returns nothing for that host.
    //
    // Three sub-cases are checked:
    // - host_a: event says (conn-1, host_a), routes still have (conn-1,
    //   host_a) →  resilient, NOT removed, updated.
    // - host_b: event says (conn-1, host_b), routes have NO entry for
    //   host_b at all →  dangling, removed.
    // - host_c: event says (conn-1, host_c), routes have host_c but only
    //   under conn-2 (not conn-1) →  old entry is dangling and deleted,
    //   but merge_update re-adds host_c with the conn-2 route.
    #[tokio::test]
    async fn merge_client_routes_update_removes_dangling_entries() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-2".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();
        let host_c = Uuid::new_v4();

        // Initial full snapshot: all three hosts present under conn-1.
        let routes = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_a,
                hostname: "127.0.0.1".to_string(),
                port: Some(9042),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_b,
                hostname: "127.0.0.1".to_string(),
                port: Some(9043),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_c,
                hostname: "127.0.0.1".to_string(),
                port: Some(9044),
                tls_port: None,
            },
        ]);
        translator.replace_client_routes(routes);

        let peer_a = make_peer(host_a, localhost_addr(19999));
        let peer_b = make_peer(host_b, localhost_addr(19998));
        let peer_c = make_peer(host_c, localhost_addr(19997));

        // All three should translate.
        assert_eq!(
            translator.translate_address(&peer_a).await.unwrap(),
            localhost_addr(9042)
        );
        assert_eq!(
            translator.translate_address(&peer_b).await.unwrap(),
            localhost_addr(9043)
        );
        assert_eq!(
            translator.translate_address(&peer_c).await.unwrap(),
            localhost_addr(9044)
        );

        // Event mentions all three hosts under conn-1.
        // Re-fetched routes:
        //   host_a: still has conn-1 (updated port) →  resilient
        //   host_b: completely absent →  dangling, removed
        //   host_c: present but only under conn-2 (conn-1 gone) →  old
        //           entry dangling, deleted; re-added via merge_update
        //           with conn-2's route
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec![
                "conn-1".to_owned(),
                "conn-1".to_owned(),
                "conn-1".to_owned(),
                "conn-2".to_owned(),
            ],
            host_ids: vec![host_a, host_b, host_c, host_c],
        };
        let partial = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_a,
                hostname: "127.0.0.1".to_string(),
                port: Some(9050),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-2".to_owned(),
                host_id: host_c,
                hostname: "127.0.0.1".to_string(),
                port: Some(9060),
                tls_port: None,
            },
        ]);
        translator.merge_client_routes_update(&event, partial);

        // Resilient: host_a updated to new port, not removed.
        assert_eq!(
            translator.translate_address(&peer_a).await.unwrap(),
            localhost_addr(9050)
        );
        // Dangling (absent): host_b removed →  translation fails.
        assert_matches!(
            translator.translate_address(&peer_b).await,
            Err(TranslationError::NoRuleForHost(id)) if id == host_b
        );
        // Dangling (different connection_id): old conn-1 entry deleted,
        // re-added via merge_update with conn-2's route.
        assert_eq!(
            translator.translate_address(&peer_c).await.unwrap(),
            localhost_addr(9060)
        );
    }

    // Connection IDs are "sticky": once a host is associated with a given
    // connection ID, the translator prefers to keep using that same connection
    // ID on subsequent updates (as long as a route with that ID is still
    // available for the host). This avoids unnecessary NLB switching and prevents
    // breakdown in case of misconfiguration when updating client_routes table.
    //
    // When the sticky connection ID disappears, the translator must switch to
    // the remaining one. Tested via both full replacement and merge event.
    #[tokio::test]
    async fn merge_update_prefers_sticky_connection_id() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-0".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let peer = make_peer(host_id, localhost_addr(19999));

        // Initial: host_id gets conn-1 with port 9042.
        let routes = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]);
        translator.replace_client_routes(routes);
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9042)
        );

        // Update: both conn-1 (port 9043) and conn-0 (port 9044) available.
        // The translator should stick to conn-1 because that's the one
        // previously established.
        let routes2 = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9043),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-0".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9044),
                tls_port: None,
            },
        ]);
        translator.replace_client_routes(routes2);

        // Sticky: conn-1's port (9043) should be used, not conn-0's (9044).
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9043)
        );

        // Case 1 via full replacement: conn-1 disappears, only conn-0 remains.
        // BEFORE: {(conn-1, host_id), (conn-0, host_id)}, AFTER: {(conn-0, host_id)}.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-0".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9050),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9050)
        );

        // Re-establish conn-1 stickiness for the merge variant.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9060),
            tls_port: None,
        }]));
        translator.replace_client_routes(make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9061),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-0".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9062),
                tls_port: None,
            },
        ]));
        // Verify conn-1 is sticky again.
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9061)
        );

        // Case 1 via merge event: conn-1 disappears, only conn-0 remains and is updated.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned(), "conn-0".to_owned()],
            host_ids: vec![host_id, host_id],
        };
        translator.merge_client_routes_update(
            &event,
            make_client_routes(vec![ClientRoute {
                connection_id: "conn-0".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9070),
                tls_port: None,
            }]),
        );
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9070)
        );
    }

    // When the previously-used connection ID is no longer available for a
    // host in the update AND no other connection ID was previously known,
    // the translator falls back to whichever connection ID is available.
    // Tested via both full replacement and merge event.
    #[tokio::test]
    async fn merge_update_falls_back_when_sticky_connection_id_absent() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-0".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let peer = make_peer(host_id, localhost_addr(19999));

        // Initial: host_id gets conn-1 with port 9042.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9042)
        );

        // Case 2 via full replacement: conn-1 gone, conn-0 is brand new.
        // BEFORE: {(conn-1, host_id)}, AFTER: {(conn-0, host_id)}.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-0".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9044),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9044)
        );

        // Re-establish conn-1 for the merge variant.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9050),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9050)
        );

        // Case 2 via merge event: conn-1 gone, conn-0 is brand new.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned(), "conn-0".to_owned()],
            host_ids: vec![host_id, host_id],
        };
        translator.merge_client_routes_update(
            &event,
            make_client_routes(vec![ClientRoute {
                connection_id: "conn-0".to_owned(),
                host_id,
                hostname: "127.0.0.1".to_string(),
                port: Some(9060),
                tls_port: None,
            }]),
        );
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9060)
        );
    }

    // Regression test for the bug that deletion events cleared unrelated hosts'
    // routes.
    //
    // Before the fix, `merge_client_routes_update` delegated to a shared
    // `merge_update` helper that iterated over *all* hosts present in the
    // re-fetched `new_routes` — including hosts that appeared in the re-fetch
    // as cross-product artifacts of the `WHERE connection_id IN ? AND host_id
    // IN ?` query, but were NOT listed in the event itself.
    //
    // Scenario: host_x has a sticky route under conn-1; the event only
    // mentions host_y.  The re-fetch query (`WHERE connection_id IN [conn-2]
    // AND host_id IN [host_y]`) should not return host_x, but if the caller
    // passes extra data for host_x (simulating a cross-product artifact),
    // the old `merge_update` would process host_x, fail the sticky check
    // (conn-1 not in {conn-2}), and clobber the existing route with conn-2.
    //
    // The fix scopes merging to only the (connection_id, host_id) pairs
    // listed in the event, so unrelated hosts are left untouched.
    #[tokio::test]
    async fn merge_event_cross_product_does_not_clobber_unrelated_host() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-2".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_x = Uuid::new_v4();
        let host_y = Uuid::new_v4();
        let peer_x = make_peer(host_x, localhost_addr(19999));
        let peer_y = make_peer(host_y, localhost_addr(19998));

        // Initial state: host_x reachable via conn-1 (port 9042),
        //                host_y has no route yet.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id: host_x,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer_x).await.unwrap(),
            localhost_addr(9042),
        );

        // Event mentions only host_y under conn-2.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-2".to_owned()],
            host_ids: vec![host_y],
        };
        // The re-fetch contains host_y's route (expected) AND host_x under
        // conn-2 (cross-product artifact — the event never mentioned host_x).
        let refetched = make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-2".to_owned(),
                host_id: host_y,
                hostname: "127.0.0.1".to_string(),
                port: Some(9099),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-2".to_owned(),
                host_id: host_x,
                hostname: "127.0.0.1".to_string(),
                port: Some(9088),
                tls_port: None,
            },
        ]);
        translator.merge_client_routes_update(&event, refetched);

        // host_y should now translate via conn-2.
        assert_eq!(
            translator.translate_address(&peer_y).await.unwrap(),
            localhost_addr(9099),
        );
        // host_x's conn-1 route must be preserved — the event never
        // mentioned host_x, so the cross-product artifact must be ignored.
        assert_eq!(
            translator.translate_address(&peer_x).await.unwrap(),
            localhost_addr(9042),
        );
    }

    // Regression test for the bug described I once introduced and then fixed.
    //
    // When one connection ID for a host is deleted and the re-fetch (scoped to
    // only that connection ID) returns nothing, the code would remove the host
    // entirely.  But the host may still be reachable via another connection ID
    // whose route was previously held. The driver should not lose the host.
    #[tokio::test]
    async fn merge_event_deletion_should_not_lose_host_when_other_conn_ids_not_refetched() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-2".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_x = Uuid::new_v4();
        let peer = make_peer(host_x, localhost_addr(19999));

        // Initial state: host_x has routes under both conn-1 and conn-2.
        // Stickiness picks conn-1 (inserted first).
        translator.replace_client_routes(make_client_routes(vec![
            ClientRoute {
                connection_id: "conn-1".to_owned(),
                host_id: host_x,
                hostname: "127.0.0.1".to_string(),
                port: Some(9042),
                tls_port: None,
            },
            ClientRoute {
                connection_id: "conn-2".to_owned(),
                host_id: host_x,
                hostname: "127.0.0.1".to_string(),
                port: Some(9043),
                tls_port: None,
            },
        ]));

        // Event: conn-1 for host_x changed (it was deleted in the DB).
        // Re-fetch scoped to conn-1 + host_x returns nothing.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned()],
            host_ids: vec![host_x],
        };
        translator.merge_client_routes_update(&event, ClientRoutes::default());

        // Correct behaviour: host_x should still be translatable.
        // The driver previously knew about conn-2's route for this host, so it
        // should fall back to that rather than losing the host entirely.
        let result = translator.translate_address(&peer).await.unwrap();
        assert_ne!(
            result,
            localhost_addr(19999),
            "host_x must not fall back to the untranslated address; \
             the previously-held conn-2 route should still be usable",
        );
    }

    // Regression test for the double-iteration consumption bug in `merge_client_routes_update`,
    // which I introduced at some point of development and then fixed.
    //
    // When the event lists the same host_id under two connection IDs, the
    // first iteration would call `new_routes.routes.remove(host_id)`, consuming
    // the entire routes-for-host map.  The second iteration would get `None` from
    // `remove` and incorrectly enter the deletion branch, removing the host
    // that was just (re-)inserted by the first iteration.
    #[tokio::test]
    async fn merge_event_same_host_two_conn_ids_second_iteration_should_not_delete() {
        let config = make_config(vec![
            ClientRoutesProxy::new_with_connection_id("conn-1".to_string()),
            ClientRoutesProxy::new_with_connection_id("conn-2".to_string()),
        ]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_x = Uuid::new_v4();
        let peer = make_peer(host_x, localhost_addr(19999));

        // Initial state: host_x reachable via conn-1, port 9042.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id: host_x,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9042),
        );

        // Event: both conn-1 and conn-2 changed for host_x.
        // Re-fetch: conn-1 was deleted, conn-2 was created with port 9099.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned(), "conn-2".to_owned()],
            host_ids: vec![host_x, host_x],
        };
        let refetched = make_client_routes(vec![ClientRoute {
            connection_id: "conn-2".to_owned(),
            host_id: host_x,
            hostname: "127.0.0.1".to_string(),
            port: Some(9099),
            tls_port: None,
        }]);
        translator.merge_client_routes_update(&event, refetched);

        // Correct behaviour: host_x should be reachable via conn-2 at port 9099.
        // The first iteration handles (conn-1, host_x) — deletion + re-insert
        // with conn-2.  The second iteration handles (conn-2, host_x) — should
        // recognise that the route is present and keep it, not delete it.
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9099),
        );
    }

    // Duplicate entries in a merge event (same connection_id and host_id
    // appearing multiple times) are handled correctly: the route is applied
    // once and subsequent duplicate occurrences are no-ops, not
    // misinterpreted as deletions.
    //
    // Without deduplication, `merge_client_routes_update` consumed the route
    // from `new_routes` via `remove()` on the first occurrence.  The second
    // occurrence found `None` and entered the deletion branch, incorrectly
    // removing the route that was just applied.
    #[tokio::test]
    async fn merge_event_duplicate_entries_are_idempotent() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_id = Uuid::new_v4();
        let peer = make_peer(host_id, localhost_addr(19999));

        // Initial state: host has a route via conn-1 at port 9042.
        translator.replace_client_routes(make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9042),
            tls_port: None,
        }]));
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9042),
        );

        // Event with duplicate: (conn-1, host_id) appears twice.
        // Re-fetch contains the updated route (port 9099).
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned(), "conn-1".to_owned()],
            host_ids: vec![host_id, host_id],
        };
        let refetched = make_client_routes(vec![ClientRoute {
            connection_id: "conn-1".to_owned(),
            host_id,
            hostname: "127.0.0.1".to_string(),
            port: Some(9099),
            tls_port: None,
        }]);
        translator.merge_client_routes_update(&event, refetched);

        // The route should be updated to port 9099, not deleted.
        assert_eq!(
            translator.translate_address(&peer).await.unwrap(),
            localhost_addr(9099),
        );
    }
}
