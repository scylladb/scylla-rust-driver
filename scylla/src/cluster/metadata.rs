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
//!

use crate::routing::Token;

use scylla_cql::frame::response::result::ColumnSpec;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use crate::cluster::node::{NodeAddr, ResolvedContactPoint};

// Re-export of CQL types.
pub use scylla_cql::frame::response::result::{
    CollectionType, ColumnType, NativeType, UserDefinedType,
};

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
}

/// Represents a node in the cluster, as fetched from the `system.{peers,local}` tables.
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

/// Describes a keyspace in the cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Keyspace {
    /// Replication strategy used by the keyspace.
    pub strategy: Strategy,
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
        }
    }
}
