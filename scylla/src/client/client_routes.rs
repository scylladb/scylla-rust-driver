//! Routing configuration, enabling routing using `system.client_routes`.
//!
//! Enables connecting to Scylla Cloud clusters using AWS PrivateLink or GCP Private Service Connect, among others.

use scylla_cql::frame::response::event::ClientRoutesChangeEvent;

use crate::cluster::metadata::ClientRoutes;

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
