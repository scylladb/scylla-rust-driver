use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

use crate::cluster::metadata::{
    ClientRoute, ClientRoutes, Keyspace, Metadata, Peer, SingleKeyspaceMetadataError,
};
use crate::errors::MetadataError;

/// An explicit request to refresh cluster metadata, sent by
/// [`Cluster::refresh_metadata`](crate::cluster::worker::Cluster::refresh_metadata).
#[derive(Debug)]
pub(in super::super) struct RefreshRequest {
    pub(in super::super) response_chan: oneshot::Sender<Result<(), MetadataError>>,
}

#[derive(PartialEq, Eq)]
pub(in super::super) enum StatusHint {
    Up,
    Down,
}

/// Everything the [`MetadataWorker`] learned about the cluster and did not yet
/// hand over to the cluster worker.
///
/// Because the channel carrying it has capacity one, several consecutive
/// discoveries may end up merged into a single value - see the `merge_*`
/// constructors below, which are the only way to fill it in.
#[derive(Default)]
pub(in super::super) struct MetadataUpdate {
    pub(in super::super) metadata_changes: Option<MetadataChanges>,
    /// Nodes for which we received status hints, and the type of hints.
    /// Latest hint wins during merge.
    pub(in super::super) status_hints: HashMap<SocketAddr, StatusHint>,
}

pub(in super::super) enum MetadataChanges {
    Full {
        /// Full fetch of metadata. Later partial fetch is allowed to modify this.
        metadata: Metadata,
        /// Response channels of explicit refresh requests, to be answered once the
        /// state resulting from `metadata` has been published.
        refresh_responses: Vec<tokio::sync::oneshot::Sender<Result<(), MetadataError>>>,
    },
    /// Partial updates: everything learned since the last full fetch, one field
    /// per partial fetch type.
    Partial(PartialMetadataChanges),
}

/// The partial counterpart of a full metadata fetch: independently fetched
/// updates of individual metadata aspects, one field per partial fetch type.
///
/// Each field merges on its own, without looking at the others. This makes
/// merging commutative across types, which keeps this struct correct even when
/// partial fetches of different types run concurrently and complete in an
/// order different from the one they were started in.
#[derive(Default)]
pub(in super::super) struct PartialMetadataChanges {
    /// Partial client-routes snapshots fetched in response to
    /// CLIENT_ROUTES_CHANGE events.
    pub(in super::super) client_routes_updates: Option<ClientRoutesUpdate>,
    /// The peer list fetched in response to TOPOLOGY_CHANGE / STATUS_CHANGE events.
    ///
    /// Not partial within its own aspect: a topology fetch always reads the
    /// whole peer list, so this replaces the topology of the state it is
    /// applied to.
    pub(in super::super) peers: Option<Vec<Peer>>,
    /// The schema of the keyspaces that SCHEMA_CHANGE events named, fetched in
    /// response to them.
    pub(in super::super) schema: Option<SchemaUpdate>,
}

impl PartialMetadataChanges {
    /// Records a partial client-routes snapshot, merging it into the one
    /// already pending, if any.
    fn merge_client_routes_update(&mut self, new_client_routes: ClientRoutesUpdate) {
        match &mut self.client_routes_updates {
            None => self.client_routes_updates = Some(new_client_routes),
            Some(existing) => existing.merge(new_client_routes),
        }
    }

    /// Records a freshly fetched peer list, dropping the one pending so far:
    /// each fetch reads the whole list, so the newest one subsumes the rest.
    fn merge_peers(&mut self, peers: Vec<Peer>) {
        self.peers = Some(peers);
    }

    /// Records a partial schema snapshot, merging it into the one already
    /// pending, if any.
    fn merge_schema_update(&mut self, new_schema: SchemaUpdate) {
        match &mut self.schema {
            None => self.schema = Some(new_schema),
            Some(existing) => existing.merge(new_schema),
        }
    }
}

impl MetadataUpdate {
    fn slot_mut(slot: &mut Option<Self>) -> &mut Self {
        slot.get_or_insert_with(Self::default)
    }

    /// Records freshly fetched metadata, together with the response channel of
    /// the explicit refresh request that triggered the fetch, if any.
    ///
    /// Any partial updates pending so far are dropped: they were
    /// fetched before this metadata that subsumes them.
    pub(crate) fn merge_metadata(
        slot: &mut Option<Self>,
        mut metadata: Metadata,
        refresh_response: Option<tokio::sync::oneshot::Sender<Result<(), MetadataError>>>,
    ) {
        let update = Self::slot_mut(slot);
        // Newest wins: an older, not-yet-applied fetch is worthless now.
        // Caveat: We need to NOT drop the old refresh channel.
        match &mut update.metadata_changes {
            None => {
                update.metadata_changes = Some(MetadataChanges::Full {
                    metadata,
                    refresh_responses: refresh_response.into_iter().collect(),
                });
            }
            Some(MetadataChanges::Partial(partial_changes)) => {
                // Merging per-keyspace results: if newer fetch has a per-keyspace error,
                // but older has a working ks metadata, we need to reuse the old one.
                if let Some(old_schema_update) = &partial_changes.schema {
                    for (name, ks) in metadata.keyspaces.iter_mut() {
                        if let Err(e) = ks
                            && let Some(FetchedKeyspace::Present(Ok(old_ks))) =
                                old_schema_update.keyspaces.get(name)
                        {
                            warn!(
                                "Encountered an error while processing\
                                metadata of keyspace \"{name}\": {e}.\
                                Re-using older version of this keyspace metadata"
                            );
                            *ks = Ok(old_ks.clone())
                        }
                    }
                }

                update.metadata_changes = Some(MetadataChanges::Full {
                    metadata,
                    refresh_responses: refresh_response.into_iter().collect(),
                });
            }
            Some(MetadataChanges::Full {
                metadata: slot_metadata,
                refresh_responses,
            }) => {
                // Merging per-keyspace results: if newer fetch has a per-keyspace error,
                // but older has a working ks metadata, we need to reuse the old one.
                for (name, ks) in metadata.keyspaces.iter_mut() {
                    if let Err(e) = ks
                        && let Some(Ok(old_ks)) = slot_metadata.keyspaces.get(name)
                    {
                        warn!(
                            "Encountered an error while processing\
                            metadata of keyspace \"{name}\": {e}.\
                            Re-using older version of this keyspace metadata"
                        );
                        *ks = Ok(old_ks.clone())
                    }
                }
                *slot_metadata = metadata;
                if let Some(response_channel) = refresh_response {
                    refresh_responses.push(response_channel);
                }
            }
        }
    }

    /// Records a partial client-routes snapshot fetched in response to `event`.
    pub(crate) fn merge_client_routes_update(
        slot: &mut Option<Self>,
        new_client_routes: ClientRoutesUpdate,
    ) {
        let update = Self::slot_mut(slot);
        match &mut update.metadata_changes {
            None => {
                let mut partial = PartialMetadataChanges::default();
                partial.merge_client_routes_update(new_client_routes);
                update.metadata_changes = Some(MetadataChanges::Partial(partial));
            }
            Some(MetadataChanges::Partial(partial)) => {
                partial.merge_client_routes_update(new_client_routes);
            }
            Some(MetadataChanges::Full {
                metadata,
                refresh_responses: _,
            }) => {
                let Some(metadata_routes) = metadata.client_routes.as_mut() else {
                    warn!(
                        "Received update for client routes despite client routes not being configured"
                    );
                    return;
                };
                metadata_routes.merge(new_client_routes);
            }
        }
    }

    /// Records a freshly fetched peer list, obtained by a partial topology
    /// fetch performed in response to TOPOLOGY_CHANGE events.
    pub(crate) fn merge_topology_update(slot: &mut Option<Self>, peers: Vec<Peer>) {
        let update = Self::slot_mut(slot);
        match &mut update.metadata_changes {
            None => {
                let mut partial = PartialMetadataChanges::default();
                partial.merge_peers(peers);
                update.metadata_changes = Some(MetadataChanges::Partial(partial));
            }
            Some(MetadataChanges::Partial(partial)) => partial.merge_peers(peers),
            Some(MetadataChanges::Full {
                metadata,
                refresh_responses: _,
            }) => {
                // The partial fetch is necessarily newer: a full fetch preempts
                // the partial ones in flight, so a partial fetch can only
                // complete after a pending full fetch if it was started after
                // that fetch completed.
                metadata.peers = peers;
            }
        }
    }

    /// Records the per-keyspace schema obtained by a partial schema fetch
    /// performed in response to SCHEMA_CHANGE events.
    pub(crate) fn merge_schema_update(slot: &mut Option<Self>, schema: SchemaUpdate) {
        let update = Self::slot_mut(slot);
        match &mut update.metadata_changes {
            None => {
                let mut partial = PartialMetadataChanges::default();
                partial.merge_schema_update(schema);
                update.metadata_changes = Some(MetadataChanges::Partial(partial));
            }
            Some(MetadataChanges::Partial(partial)) => partial.merge_schema_update(schema),
            Some(MetadataChanges::Full {
                metadata,
                refresh_responses: _,
            }) => {
                // Older full fetch was not consumed yet. We need to update it
                // with the newly fetched keyspaces.
                for (name, keyspace) in schema.keyspaces {
                    match keyspace {
                        // If new metadata has Err for some keyspace, and the old one has Ok, we should
                        // not overwrite.
                        FetchedKeyspace::Present(Err(_))
                            if matches!(metadata.keyspaces.get(&name), Some(Ok(_))) => {}
                        FetchedKeyspace::Present(keyspace) => {
                            metadata.keyspaces.insert(name, keyspace);
                        }
                        FetchedKeyspace::Absent => {
                            metadata.keyspaces.remove(&name);
                        }
                    }
                }
            }
        }
    }

    /// Records that `addr` was hinted to be UP.
    pub(crate) fn merge_up_hint(slot: &mut Option<Self>, addr: SocketAddr) {
        Self::slot_mut(slot)
            .status_hints
            .insert(addr, StatusHint::Up);
    }

    /// Records that `addr` was hinted to be DOWN.
    pub(crate) fn merge_down_hint(slot: &mut Option<Self>, addr: SocketAddr) {
        Self::slot_mut(slot)
            .status_hints
            .insert(addr, StatusHint::Down);
    }
}

/// A partial, mergeable update of client routes, derived from the
/// (connection id, host id) pairs listed by CLIENT_ROUTES_CHANGE:UPDATE_NODES
/// events and from the partial snapshot of `system.client_routes` fetched in
/// response to them.
///
/// Each entry corresponds to a (host id, connection id) pair listed by an
/// event and relevant to the driver (i.e. with a connection id the driver
/// monitors):
/// - `Some(route)` - the route was created or updated;
/// - `None` - the route was removed.
#[derive(Debug, Default)]
pub(crate) struct ClientRoutesUpdate {
    /// Grouped by host id first, then by connection id - same as `ClientRoutes`.
    pub(crate) updates: HashMap<Uuid, HashMap<String, Option<ClientRoute>>>,
}

impl ClientRoutesUpdate {
    /// Builds an update from the (connection id, host id) pairs listed by
    /// CLIENT_ROUTES_CHANGE:UPDATE_NODES events and the partial snapshot
    /// fetched in response to them.
    ///
    /// Only the pairs with a connection id monitored by the driver (i.e.
    /// present in `relevant_connection_ids`) are taken into account. Routes
    /// present in `fetched` that do not correspond to any such pair are
    /// ignored: the fetch query is `WHERE connection_id IN ? AND host_id IN ?`,
    /// so it returns the full cross product of the ids, while the events'
    /// semantics is the pairs.
    pub(crate) fn from_pairs(
        pairs: &HashSet<(String, Uuid)>,
        relevant_connection_ids: &[String],
        fetched: &ClientRoutes,
    ) -> Self {
        let mut updates: HashMap<Uuid, HashMap<String, Option<ClientRoute>>> = HashMap::new();

        for (connection_id, host_id) in pairs
            .iter()
            .filter(|(conn_id, _host_id)| relevant_connection_ids.contains(conn_id))
        {
            let route = fetched
                .routes
                .get(host_id)
                .and_then(|routes_for_host| routes_for_host.get(connection_id).cloned());
            updates
                .entry(*host_id)
                .or_default()
                .insert(connection_id.clone(), route);
        }

        Self { updates }
    }

    /// Flattens the update into (host id, connection id, maybe route) triples.
    pub(crate) fn into_entries(self) -> impl Iterator<Item = (Uuid, String, Option<ClientRoute>)> {
        self.updates
            .into_iter()
            .flat_map(|(host_id, routes_for_host)| {
                routes_for_host
                    .into_iter()
                    .map(move |(connection_id, route)| (host_id, connection_id, route))
            })
    }

    /// Merges a newer update into this one. Entries of `newer` override entries of `self`
    /// for the same (host id, connection id) pair; all other entries are kept.
    pub(crate) fn merge(&mut self, newer: ClientRoutesUpdate) {
        for (host_id, connection_id, route) in newer.into_entries() {
            self.updates
                .entry(host_id)
                .or_default()
                .insert(connection_id, route);
        }
    }
}

/// A partial, mergeable update of the schema metadata, derived from the
/// keyspaces named by SCHEMA_CHANGE events and from the per-keyspace schema
/// fetched in response to them.
///
/// Only the named keyspaces are described; every other keyspace of the state
/// this is applied to is left untouched.
#[derive(Default)]
pub(crate) struct SchemaUpdate {
    /// One entry per named keyspace. A map keyed by keyspace, so that a
    /// keyspace cannot be said to be both present and absent, and so that
    /// merging is nothing but "the newer statement wins" - see
    /// [`merge`](Self::merge).
    pub(crate) keyspaces: HashMap<String, FetchedKeyspace>,
}

/// What a partial schema fetch established about one keyspace.
// Storing `Keyspace` object directly is something we do normally,
// for example in `Metadata` / `ClusterState`. Here clippy complains
// only because there is a second, smaller variant. If we boxed the
// keyspace here, we would then have to unbox it to get it into
// `ClusterState`. This will change if / when we decide to put keyspaces
// in `ClusterState` inside `Arc`. Until then, let's just ignore clippy.
#[expect(clippy::large_enum_variant)]
pub(crate) enum FetchedKeyspace {
    /// The keyspace exists; this is its freshly read metadata.
    ///
    /// The `Result` layer is the one of [`Metadata::keyspaces`]: a keyspace
    /// whose fetched metadata turned out inconsistent is reported as an error
    /// rather than silently replaced.
    Present(Result<Keyspace, SingleKeyspaceMetadataError>),
    /// The keyspace does not exist: its last event dropped it, or the fetch
    /// found no row for it.
    Absent,
}

impl SchemaUpdate {
    /// Merges a newer update into this one: the newer statement about a
    /// keyspace overrides the older one. We need to handle per-keyspace
    /// errors: if newer keyspace has an error, but older one doesn't, then
    /// we need to drop the newer one. This is the same logic we have in
    /// ClusterState construction.
    pub(crate) fn merge(&mut self, newer: SchemaUpdate) {
        for (name, newer_keyspace) in newer.keyspaces {
            match newer_keyspace {
                // If newer keyspace is an error, and we already have the non-Err keyspace
                // with this name, we would only lose information by updating.
                FetchedKeyspace::Present(Err(_))
                    if matches!(
                        self.keyspaces.get(&name),
                        Some(FetchedKeyspace::Present(Ok(_)))
                    ) => {}
                newer_keyspace => {
                    self.keyspaces.insert(name, newer_keyspace);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    // ---------------------------------------------------------------
    // Tests for ClientRoutesUpdate::merge and ClientRoutes::merge
    // ---------------------------------------------------------------

    use std::collections::HashMap;
    use uuid::Uuid;

    use crate::client::client_routes::{
        ClientRoutesAddressTranslator, ClientRoutesConfig, ClientRoutesProxy,
        ClientRoutesSubscriber,
    };
    use crate::cluster::metadata::update::ClientRoutesUpdate;
    use crate::cluster::metadata::{ClientRoute, ClientRoutes};
    use crate::frame::response::event::ClientRoutesChangeEvent;

    fn route(host: Uuid, conn: &str, port: u16) -> ClientRoute {
        ClientRoute {
            connection_id: conn.to_owned(),
            host_id: host,
            hostname: "127.0.0.1".to_string(),
            port: Some(port),
            tls_port: None,
        }
    }

    // Builds a ClientRoutesUpdate directly from (host, connection id, entry) triples,
    // bypassing `from_pairs` — the merge logic is independent of how the update
    // was derived.
    fn update_from_entries(entries: Vec<(Uuid, &str, Option<ClientRoute>)>) -> ClientRoutesUpdate {
        let mut updates: HashMap<Uuid, HashMap<String, Option<ClientRoute>>> = HashMap::new();
        for (host, conn, entry) in entries {
            updates
                .entry(host)
                .or_default()
                .insert(conn.to_owned(), entry);
        }
        ClientRoutesUpdate { updates }
    }

    // Flattens a ClientRoutes into a deterministically ordered list, so that two
    // snapshots can be compared for equality (ClientRoutes itself is not PartialEq).
    fn sorted_entries(routes: &ClientRoutes) -> Vec<(Uuid, String, ClientRoute)> {
        let mut entries: Vec<(Uuid, String, ClientRoute)> = routes
            .routes
            .iter()
            .flat_map(|(host, per_conn)| {
                per_conn
                    .iter()
                    .map(move |(conn, route)| (*host, conn.clone(), route.clone()))
            })
            .collect();
        entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        entries
    }

    // Flattens a ClientRoutesUpdate the same way, for assertions on merge results.
    fn sorted_update_entries(
        update: &ClientRoutesUpdate,
    ) -> Vec<(Uuid, String, Option<ClientRoute>)> {
        let mut entries: Vec<(Uuid, String, Option<ClientRoute>)> = update
            .updates
            .iter()
            .flat_map(|(host, per_conn)| {
                per_conn
                    .iter()
                    .map(move |(conn, entry)| (*host, conn.clone(), entry.clone()))
            })
            .collect();
        entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        entries
    }

    fn make_client_routes(routes: Vec<ClientRoute>) -> ClientRoutes {
        let mut cr = ClientRoutes::default();
        cr.extend(routes);
        cr
    }

    fn make_config(proxies: Vec<ClientRoutesProxy>) -> ClientRoutesConfig {
        ClientRoutesConfig::new(proxies).unwrap()
    }

    fn make_update(
        translator: &ClientRoutesAddressTranslator,
        event: &ClientRoutesChangeEvent,
        fetched: ClientRoutes,
    ) -> ClientRoutesUpdate {
        let ClientRoutesChangeEvent::UpdateNodes {
            connection_ids,
            host_ids,
        } = event
        else {
            unreachable!("tests only construct UpdateNodes events")
        };
        ClientRoutesUpdate::from_pairs(
            &connection_ids
                .iter()
                .cloned()
                .zip(host_ids.iter().copied())
                .collect(),
            translator.get_connection_ids(),
            &fetched,
        )
    }

    // `ClientRoutesUpdate::merge`: entries of the newer update override entries of
    // the older one for the same (host id, connection id) pair; entries only present
    // in one of the updates are retained as-is. This covers all four combinations
    // of Some/None overriding Some/None.
    #[test]
    fn update_merge_newer_overrides_older_and_keeps_disjoint() {
        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();
        let host_c = Uuid::new_v4();

        let mut older = update_from_entries(vec![
            // Overridden by a newer Some.
            (host_a, "conn-1", Some(route(host_a, "conn-1", 9042))),
            // Overridden by a newer None (deletion wins).
            (host_a, "conn-2", Some(route(host_a, "conn-2", 9043))),
            // Overridden by a newer Some (resurrection wins).
            (host_b, "conn-1", None),
            // Disjoint: only in the older update.
            (host_c, "conn-1", Some(route(host_c, "conn-1", 9044))),
        ]);

        let newer = update_from_entries(vec![
            (host_a, "conn-1", Some(route(host_a, "conn-1", 9099))),
            (host_a, "conn-2", None),
            (host_b, "conn-1", Some(route(host_b, "conn-1", 9050))),
            // Disjoint: only in the newer update.
            (host_c, "conn-2", None),
        ]);

        older.merge(newer);

        let expected = sorted_update_entries(&update_from_entries(vec![
            (host_a, "conn-1", Some(route(host_a, "conn-1", 9099))),
            (host_a, "conn-2", None),
            (host_b, "conn-1", Some(route(host_b, "conn-1", 9050))),
            (host_c, "conn-1", Some(route(host_c, "conn-1", 9044))),
            (host_c, "conn-2", None),
        ]));
        assert_eq!(sorted_update_entries(&older), expected);
    }

    // `ClientRoutes::merge`: a `Some` entry for a host absent from the snapshot
    // inserts a brand-new host entry.
    #[test]
    fn client_routes_merge_some_inserts_new_host() {
        let host_existing = Uuid::new_v4();
        let host_new = Uuid::new_v4();

        let mut routes = make_client_routes(vec![route(host_existing, "conn-1", 9042)]);
        routes.merge(update_from_entries(vec![(
            host_new,
            "conn-1",
            Some(route(host_new, "conn-1", 9099)),
        )]));

        assert_eq!(
            routes.routes.get(&host_new).unwrap().get("conn-1"),
            Some(&route(host_new, "conn-1", 9099))
        );
        // The pre-existing host is untouched.
        assert_eq!(
            routes.routes.get(&host_existing).unwrap().get("conn-1"),
            Some(&route(host_existing, "conn-1", 9042))
        );
    }

    // `ClientRoutes::merge`: a `Some` entry for an already-known
    // (host id, connection id) overwrites the stored route.
    #[test]
    fn client_routes_merge_some_overwrites_existing_route() {
        let host = Uuid::new_v4();

        let mut routes = make_client_routes(vec![
            route(host, "conn-1", 9042),
            route(host, "conn-2", 9043),
        ]);
        routes.merge(update_from_entries(vec![(
            host,
            "conn-1",
            Some(route(host, "conn-1", 9099)),
        )]));

        let per_conn = routes.routes.get(&host).unwrap();
        assert_eq!(per_conn.get("conn-1"), Some(&route(host, "conn-1", 9099)));
        // The other connection id of the same host is unaffected.
        assert_eq!(per_conn.get("conn-2"), Some(&route(host, "conn-2", 9043)));
    }

    // `ClientRoutes::merge`: a `None` entry removes only that connection id's route,
    // leaving the host's other routes in place.
    #[test]
    fn client_routes_merge_none_removes_single_route() {
        let host = Uuid::new_v4();

        let mut routes = make_client_routes(vec![
            route(host, "conn-1", 9042),
            route(host, "conn-2", 9043),
        ]);
        routes.merge(update_from_entries(vec![(host, "conn-1", None)]));

        let per_conn = routes.routes.get(&host).unwrap();
        assert_eq!(per_conn.get("conn-1"), None);
        assert_eq!(per_conn.get("conn-2"), Some(&route(host, "conn-2", 9043)));
    }

    // `ClientRoutes::merge`: removing a host's last route drops the host entry
    // entirely. ClientRoutes maintains the invariant that inner maps are never
    // empty, so an empty inner map must not be left behind.
    #[test]
    fn client_routes_merge_none_removing_last_route_drops_host() {
        let host = Uuid::new_v4();
        let other_host = Uuid::new_v4();

        let mut routes = make_client_routes(vec![
            route(host, "conn-1", 9042),
            route(other_host, "conn-1", 9043),
        ]);
        routes.merge(update_from_entries(vec![(host, "conn-1", None)]));

        assert!(
            !routes.routes.contains_key(&host),
            "host with no remaining routes must be dropped, got: {:?}",
            routes.routes.get(&host)
        );
        // Unrelated hosts survive.
        assert_eq!(
            routes.routes.get(&other_host).unwrap().get("conn-1"),
            Some(&route(other_host, "conn-1", 9043))
        );
    }

    // `ClientRoutes::merge`: a `None` for a host that is not known at all is a no-op.
    #[test]
    fn client_routes_merge_none_for_unknown_host_is_noop() {
        let host = Uuid::new_v4();
        let unknown = Uuid::new_v4();

        let mut routes = make_client_routes(vec![route(host, "conn-1", 9042)]);
        let before = sorted_entries(&routes);
        routes.merge(update_from_entries(vec![
            (unknown, "conn-1", None),
            // Also: unknown connection id of a known host.
            (host, "conn-9", None),
        ]));

        assert_eq!(sorted_entries(&routes), before);
    }

    // The point of making updates mergeable: applying update A and then update B to
    // a snapshot must yield exactly the same snapshot as applying the single merged
    // update `A.merge(B)`. This is what allows the driver to coalesce pending
    // partial updates before applying them.
    #[test]
    fn client_routes_merge_is_equivalent_to_sequential_application() {
        let host_a = Uuid::new_v4();
        let host_b = Uuid::new_v4();
        let host_c = Uuid::new_v4();

        let initial = || {
            make_client_routes(vec![
                route(host_a, "conn-1", 9042),
                route(host_a, "conn-2", 9043),
                route(host_b, "conn-1", 9044),
            ])
        };

        // A: updates host_a/conn-1, deletes host_a/conn-2, adds host_c/conn-1.
        let update_a = || {
            update_from_entries(vec![
                (host_a, "conn-1", Some(route(host_a, "conn-1", 9050))),
                (host_a, "conn-2", None),
                (host_c, "conn-1", Some(route(host_c, "conn-1", 9051))),
            ])
        };
        // B: overrides host_a/conn-1 again, resurrects host_a/conn-2,
        //    deletes host_b's only route, adds host_c/conn-2.
        let update_b = || {
            update_from_entries(vec![
                (host_a, "conn-1", Some(route(host_a, "conn-1", 9060))),
                (host_a, "conn-2", Some(route(host_a, "conn-2", 9061))),
                (host_b, "conn-1", None),
                (host_c, "conn-2", Some(route(host_c, "conn-2", 9062))),
            ])
        };

        // Sequential application.
        let mut sequential = initial();
        sequential.merge(update_a());
        sequential.merge(update_b());

        // Coalesced application.
        let mut coalesced_update = update_a();
        coalesced_update.merge(update_b());
        let mut coalesced = initial();
        coalesced.merge(coalesced_update);

        assert_eq!(sorted_entries(&sequential), sorted_entries(&coalesced));
        // Sanity: the merged result is what we expect, not two equal empty maps.
        assert_eq!(
            sorted_entries(&sequential),
            sorted_entries(&make_client_routes(vec![
                route(host_a, "conn-1", 9060),
                route(host_a, "conn-2", 9061),
                route(host_c, "conn-1", 9051),
                route(host_c, "conn-2", 9062),
            ]))
        );
        assert!(!sequential.routes.contains_key(&host_b));
    }

    // `ClientRoutesUpdate::from_pairs` must only pick up (host id, connection id)
    // pairs listed by the event and relevant to the driver; routes returned by the
    // re-fetch that do not correspond to an event pair are cross-product artifacts
    // of the `WHERE connection_id IN ? AND host_id IN ?` query and must be ignored.
    #[test]
    fn update_from_pairs_ignores_cross_product_artifacts_and_irrelevant_conn_ids() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host_x = Uuid::new_v4();
        let host_y = Uuid::new_v4();

        // The event lists (conn-1, host_x) — relevant — and (conn-other, host_y),
        // whose connection id the driver does not monitor.
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned(), "conn-other".to_owned()],
            host_ids: vec![host_x, host_y],
        };
        // The re-fetch additionally contains (conn-1, host_y), a cross-product artifact.
        let fetched = make_client_routes(vec![
            route(host_x, "conn-1", 9042),
            route(host_y, "conn-1", 9043),
        ]);

        let update = make_update(&translator, &event, fetched);

        assert_eq!(
            sorted_update_entries(&update),
            vec![(
                host_x,
                "conn-1".to_owned(),
                Some(route(host_x, "conn-1", 9042))
            )]
        );
    }

    // `ClientRoutesUpdate::from_pairs`: an event pair with no matching route in
    // the re-fetch yields an explicit `None` entry, i.e. a deletion.
    #[test]
    fn update_from_pairs_yields_none_for_missing_route() {
        let config = make_config(vec![ClientRoutesProxy::new_with_connection_id(
            "conn-1".to_string(),
        )]);
        let translator = ClientRoutesAddressTranslator::new(config, None, false);

        let host = Uuid::new_v4();
        let event = ClientRoutesChangeEvent::UpdateNodes {
            connection_ids: vec!["conn-1".to_owned()],
            host_ids: vec![host],
        };

        let update = make_update(&translator, &event, ClientRoutes::default());

        assert_eq!(
            sorted_update_entries(&update),
            vec![(host, "conn-1".to_owned(), None)]
        );
    }
}
