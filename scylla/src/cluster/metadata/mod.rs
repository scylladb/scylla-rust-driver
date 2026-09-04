//! This module holds entities that represent the cluster metadata,
//! which includes:
//! - topology metadata:
//!   - [Peer],
//! - schema metadata:
//!   - [Keyspace],
//!   - [Strategy] - replication strategy employed by a keyspace,
//!   - [Table],
//!   - [Column],
//!   - [ColumnKind],
//!   - [MaterializedView],
//!   - CQL types (re-exported from scylla-cql):
//!     - [ColumnType],
//!     - [NativeType],
//!     - [UserDefinedType],
//!     - [CollectionType],
//  - client routes:
//    - [ClientRoute]

pub(super) mod cc_establisher;
mod fetching;
pub(crate) mod merge_channel;
pub(crate) mod update;
pub(super) mod worker;

use crate::cluster::metadata::update::ClientRoutesUpdate;
use crate::cluster::node::{NodeAddr, ResolvedContactPoint};
use crate::routing::Token;

use crate::frame::response::result::ColumnSpec;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

// Re-export of CQL types.
pub use crate::frame::response::result::{CollectionType, ColumnType, NativeType, UserDefinedType};

#[derive(Clone, Copy, Debug)]
pub(crate) enum SchemaMetadataFetchMode {
    Disabled,
    Enabled(SchemaMetadataFetchLevel),
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum SchemaMetadataFetchLevel {
    Minimal,
    Full,
}

/// What the periodic cluster metadata refresh re-reads.
///
/// Set with
/// [`SessionBuilder::periodic_metadata_fetch_mode`](crate::client::session_builder::SessionBuilder::periodic_metadata_fetch_mode);
/// how often the refresh happens is a separate setting,
/// [`SessionBuilder::cluster_metadata_refresh_interval`](crate::client::session_builder::SessionBuilder::cluster_metadata_refresh_interval).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PeriodicFetchMode {
    /// The schema of the keyspaces that `SCHEMA_CHANGE` events named since the
    /// previous refresh, and nothing else. The default.
    ///
    /// Every other aspect of the metadata is re-read in reaction to the server
    /// event announcing its change, so the refresh only has to cover the
    /// schema, and only the part of it that changed.
    AffectedKeyspaces,
    /// All metadata selected by the session's schema-fetch configuration,
    /// ignoring which keyspaces the `SCHEMA_CHANGE` events named.
    ///
    /// This is the behaviour of driver versions that did not handle
    /// `SCHEMA_CHANGE` events, kept as an escape hatch for clusters whose
    /// events cannot be relied upon. It costs a query per schema table per
    /// refresh interval, hence it is not the default.
    FullMetadata,
}

/// Indicates that reading metadata failed, but in a way
/// that we can handle, by throwing out data for a keyspace.
/// It is possible that some of the errors could be handled in even
/// more granular way (e.g. throwing out a single table), but keyspace
/// granularity seems like a good choice given how independent keyspaces
/// are from each other.
#[derive(Clone, Debug, Error)]
pub(crate) enum SingleKeyspaceMetadataError {
    #[error(transparent)]
    MissingUDT(MissingUserDefinedType),
    #[error("Partition key column with position {0} is missing from metadata")]
    IncompletePartitionKey(i32),
    #[error("Clustering key column with position {0} is missing from metadata")]
    IncompleteClusteringKey(i32),
}

/// Describes all metadata retrieved from the cluster
pub(crate) struct Metadata {
    pub(crate) peers: Vec<Peer>,
    pub(crate) keyspaces: HashMap<String, Result<Keyspace, SingleKeyspaceMetadataError>>,
    pub(crate) cluster_name: Option<String>,

    /// The raw snapshot of client routes, as fetched from `system.client_routes`.
    /// `None` if client routes are not configured for this session - in that case
    /// the table is not queried at all and nothing should be applied.
    pub(crate) client_routes: Option<ClientRoutes>,
}

/// Represents a node in the cluster, as fetched from the `system.{peers,local}` tables.
#[cfg_attr(all(scylla_unstable, feature = "unstable-python-rs"), derive(Clone))] // <- for python-rs HostFilter wrapper to store Peer and reconstruct it from &Peer in accept()
#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct Peer {
    /// Unique identifier of the node.
    pub host_id: Uuid,
    /// Address of the node, which may be translatable by the driver or not,
    /// depending on whether the node is a contact point or a peer.
    pub address: NodeAddr,
    /// Tokens owned by this node.
    pub tokens: Vec<Token>,
    /// Datacenter this node is in, if known.
    pub datacenter: Option<String>,
    /// Rack this node is in, if known.
    pub rack: Option<String>,
}

/// An endpoint for a node that the driver is to issue connections to,
/// possibly after prior address translation.
#[derive(Clone, Debug)]
pub(crate) enum UntranslatedEndpoint {
    /// Provided by user in SessionConfig (initial contact points).
    ContactPoint(ResolvedContactPoint),
    /// Fetched in Metadata with `query_peers()`
    Peer(PeerEndpoint),
}

impl UntranslatedEndpoint {
    pub(crate) fn address(&self) -> NodeAddr {
        match *self {
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint { address, .. }) => {
                NodeAddr::Untranslatable(address)
            }
            UntranslatedEndpoint::Peer(PeerEndpoint { address, .. }) => address,
        }
    }
    pub(crate) fn set_port(&mut self, port: u16) {
        let inner_addr = match self {
            UntranslatedEndpoint::ContactPoint(ResolvedContactPoint { address, .. }) => address,
            UntranslatedEndpoint::Peer(PeerEndpoint { address, .. }) => address.inner_mut(),
        };
        inner_addr.set_port(port);
    }
}

/// Data used to issue connections to a node.
///
/// Fetched from the cluster in Metadata.
#[derive(Clone, Debug)]
pub(crate) struct PeerEndpoint {
    pub(crate) host_id: Uuid,
    pub(crate) address: NodeAddr,
    pub(crate) datacenter: Option<String>,
    pub(crate) rack: Option<String>,
}

impl Peer {
    pub(crate) fn to_peer_endpoint(&self) -> PeerEndpoint {
        PeerEndpoint {
            host_id: self.host_id,
            address: self.address,
            datacenter: self.datacenter.clone(),
            rack: self.rack.clone(),
        }
    }

    pub(crate) fn into_peer_endpoint_and_tokens(self) -> (PeerEndpoint, Vec<Token>) {
        (
            PeerEndpoint {
                host_id: self.host_id,
                address: self.address,
                datacenter: self.datacenter,
                rack: self.rack,
            },
            self.tokens,
        )
    }
}

/// The consistency mode of a keyspace, as reported by the `consistency` column of
/// `system_schema.scylla_keyspaces`.
///
/// Strong consistency is an experimental feature, and so this type is still unstable.
/// That's why it's gated behind an unstable feature.
//
// Note: Local strong consistency isn't implemented yet.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[cfg(all(scylla_unstable, feature = "unstable-strong-consistency"))]
pub enum ConsistencyMode {
    /// Eventual consistency. Covers every non-tablet keyspace and every keyspace
    /// on a server that does not report a consistency mode.
    Eventual,
    /// Global strong consistency (`consistency = 'global'`): the keyspace uses
    /// strongly-consistent (Raft-based) tablets.
    Global,
}

/// The consistency mode of a keyspace, as reported by the `consistency` column of
/// `system_schema.scylla_keyspaces`.
///
/// Strong consistency is an experimental feature, and so this type is still unstable.
/// Hence crate-private.
//
// Note: Local strong consistency isn't implemented yet.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg(not(all(scylla_unstable, feature = "unstable-strong-consistency")))]
pub(crate) enum ConsistencyMode {
    /// Eventual consistency. Covers every non-tablet keyspace and every keyspace
    /// on a server that does not report a consistency mode.
    Eventual,
    /// Global strong consistency (`consistency = 'global'`): the keyspace uses
    /// strongly-consistent (Raft-based) tablets.
    Global,
}

/// Describes a keyspace in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Keyspace {
    /// Replication strategy used by the keyspace.
    pub strategy: Strategy,
    /// Whether the keyspace has durable writes enabled.
    pub durable_writes: bool,
    /// Whether the keyspace is tablet-based.
    ///
    /// This is determined based on whether `initial_tablets` is set in
    /// `system_schema.scylla_keyspaces`. On Cassandra and old ScyllaDB versions,
    /// where this table or column is not present, this is always `false`.
    pub tablet_based: bool,
    /// The consistency mode of the keyspace.
    ///
    /// Public only behind the `unstable-strong-consistency` feature because
    /// strong consistency is still experimental.
    #[cfg(all(scylla_unstable, feature = "unstable-strong-consistency"))]
    pub consistency_mode: ConsistencyMode,
    /// The consistency mode of the keyspace.
    ///
    /// Strong consistency is still experimental; hence crate-private.
    #[cfg(not(all(scylla_unstable, feature = "unstable-strong-consistency")))]
    pub(crate) consistency_mode: ConsistencyMode,
    /// Tables in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub tables: HashMap<String, Table>,
    /// Materialized views in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub views: HashMap<String, MaterializedView>,
    /// User defined types in the keyspace.
    ///
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig.
    pub user_defined_types: HashMap<String, Arc<UserDefinedType<'static>>>,
}

/// Describes a table in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Table {
    /// Columns that constitute the table.
    pub columns: HashMap<String, Column>,
    /// Names of the columns that constitute the partition key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub partition_key: Vec<String>,
    /// Names of the columns that constitute the clustering key.
    /// All of the names are guaranteed to be present in `columns` field.
    pub clustering_key: Vec<String>,
    /// Name of the partitioner used by the table.
    pub partitioner: Option<String>,
    /// Column specs for the partition key columns.
    pub(crate) pk_column_specs: Vec<ColumnSpec<'static>>,
}

/// Describes a materialized view in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct MaterializedView {
    /// As materialized views are a special kind of table,
    /// they have the same metadata as a table.
    pub view_metadata: Table,
    /// The name of a table that the materialized view is an index of.
    pub base_table_name: String,
}

/// Describes a column of the table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Column {
    /// CQL type that the value stored in this column has.
    pub typ: ColumnType<'static>,
    /// Describes role of the column in the table.
    pub kind: ColumnKind,
}

/// Represents a user defined type whose definition is missing from the metadata.
#[derive(Clone, Debug, Error)]
#[error("Missing UDT: {keyspace}, {name}")]
pub(crate) struct MissingUserDefinedType {
    pub(crate) name: String,
    pub(crate) keyspace: String,
}

/// Some columns have a specific meaning in the context of a table,
/// and this meaning is represented by the [ColumnKind] enum.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnKind {
    /// Just a regular column.
    Regular,
    /// Column that has the same value for all rows in a partition.
    Static,
    /// Column that is part of the clustering key.
    Clustering,
    /// Column that is part of the partition key.
    PartitionKey,
}

/// [ColumnKind] parse error
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnKindFromStrError;

impl std::str::FromStr for ColumnKind {
    type Err = ColumnKindFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "regular" => Ok(Self::Regular),
            "static" => Ok(Self::Static),
            "clustering" => Ok(Self::Clustering),
            "partition_key" => Ok(Self::PartitionKey),
            _ => Err(ColumnKindFromStrError),
        }
    }
}

/// Replication strategy used by a keyspace.
///
/// This specifies how data is replicated across the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
// Check triggers because all variants end with "Strategy".
// TODO(2.0): Remove the "Strategy" postfix from variants.
#[expect(clippy::enum_variant_names)]
pub enum Strategy {
    /// _Deprecated in ScyllaDB._
    /// **Use only for a single datacenter and one rack.**
    /// Places the first replica on a node determined by the partitioner.
    /// Additional replicas are placed on the next nodes clockwise in the ring
    /// without considering topology (rack or datacenter location).
    SimpleStrategy {
        /// Replication factor, i.e. how many replicas of each piece of data there are.
        replication_factor: usize,
    },
    /// Use this strategy when you have (or plan to have) your cluster deployed across
    /// multiple datacenters. This strategy specifies how many replicas you want in each
    /// datacenter.
    ///
    /// `NetworkTopologyStrategy` places replicas in the same datacenter by walking the ring
    /// clockwise until reaching the first node in another rack. It attempts to place replicas
    /// on distinct racks because nodes in the same rack (or similar physical grouping) often
    /// fail at the same time due to power, cooling, or network issues.
    NetworkTopologyStrategy {
        /// Replication factors of datacenters with given names, i.e. how many replicas of each piece
        /// of data there are in each datacenter.
        datacenter_repfactors: HashMap<String, usize>,
    },
    /// Used for internal purposes, e.g. for system tables.
    LocalStrategy, // replication_factor == 1
    /// Unknown other strategy, which is not supported by the driver.
    Other {
        /// Name of the strategy.
        name: String,
        /// Additional parameters of the strategy, which the driver does not understand.
        data: HashMap<String, String>,
    },
}

/// Represents an entry of `system.client_routes` table, in a more refined form (port as u16).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ClientRoute {
    pub(crate) connection_id: String,
    pub(crate) host_id: Uuid,
    pub(crate) hostname: String,
    // At least one of `port` and `tls_port` must be non-null, as per the REST API constraints.
    // This is not validated by the driver, as it anyway requires specific one to be non-null
    // based on the `use_tls` setting, so the non-nullability of _any_ of them is not helpful
    // for the driver.
    pub(crate) port: Option<u16>,
    pub(crate) tls_port: Option<u16>,
}

/// A subset of client routes present in the `system.client_routes` table.
/// This is always filtered by specified connection ids, and may be filtered by
/// host ids, too.
#[derive(Debug, Default)] // Default is needed for `try_collect()`.
pub(crate) struct ClientRoutes {
    // Routes are grouped by host id first, because this is how AddressTranslator
    // looks them up. Then, routes for given host id are grouped by connection id,
    // because it's the AddressTranslator's responsibility to choose the proper connection id.
    pub(crate) routes: HashMap<Uuid, HashMap<String, ClientRoute>>,
}

// Needed for `Stream::try_collect()` to work.
impl Extend<ClientRoute> for ClientRoutes {
    fn extend<T: IntoIterator<Item = ClientRoute>>(&mut self, into_iter: T) {
        for route in into_iter {
            self.routes
                .entry(route.host_id)
                .or_default() // Insert empty HashMap.
                .insert(route.connection_id.clone(), route);
        }
    }
}

impl ClientRoutes {
    /// Applies a partial update to this full snapshot: `Some(route)` inserts or overwrites
    /// the route, `None` removes it. A host entry whose inner map becomes empty is removed
    /// entirely, to uphold the invariant that inner maps are never empty.
    pub(crate) fn merge(&mut self, update: ClientRoutesUpdate) {
        for (host_id, connection_id, route) in update.into_entries() {
            match route {
                Some(route) => {
                    self.routes
                        .entry(host_id)
                        .or_default()
                        .insert(connection_id, route);
                }
                None => {
                    if let std::collections::hash_map::Entry::Occupied(mut entry) =
                        self.routes.entry(host_id)
                    {
                        entry.get_mut().remove(&connection_id);
                        if entry.get().is_empty() {
                            entry.remove();
                        }
                    }
                }
            }
        }
    }
}

impl Metadata {
    /// Creates new, dummy metadata from a given list of peers.
    ///
    /// It can be used as a replacement for real metadata when initial
    /// metadata read fails.
    pub(crate) fn new_dummy(initial_peers: &[UntranslatedEndpoint]) -> Self {
        let peers = initial_peers
            .iter()
            .enumerate()
            .map(|(id, endpoint)| {
                // Given N nodes, divide the ring into N roughly equal parts
                // and assign them to each node.
                let token = ((id as u128) << 64) / initial_peers.len() as u128;

                Peer {
                    address: endpoint.address(),
                    tokens: vec![Token::new(token as i64)],
                    datacenter: None,
                    rack: None,
                    host_id: Uuid::new_v4(),
                }
            })
            .collect();

        Metadata {
            peers,
            keyspaces: HashMap::new(),
            cluster_name: None,
            client_routes: None,
        }
    }
}
