use crate::frame::response::event::Event;
use crate::routing::Token;
use crate::statement::query::Query;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig, PoolSize};
use crate::transport::errors::QueryError;
use crate::transport::session::IntoTypedRows;

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use strum_macros::EnumString;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

/// Allows to read current metadata from the cluster
pub struct MetadataReader {
    connection_config: ConnectionConfig,
    control_connection_address: SocketAddr,
    control_connection: NodeConnectionPool,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<SocketAddr>,
}

/// Describes all metadata retrieved from the cluster
pub struct Metadata {
    pub peers: Vec<Peer>,
    pub keyspaces: HashMap<String, Keyspace>,
}

pub struct Peer {
    pub address: SocketAddr,
    pub tokens: Vec<Token>,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    pub strategy: Strategy,
    pub tables: HashMap<String, Table>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub columns: HashMap<String, Column>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub type_: CqlType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CqlType {
    Native(NativeType),
    Collection { frozen: bool, type_: CollectionType },
    Tuple(Vec<CqlType>),
    UserDefinedType { frozen: bool, name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum NativeType {
    Ascii,
    Boolean,
    Blob,
    Counter,
    Date,
    Decimal,
    Double,
    Duration,
    Float,
    Int,
    BigInt,
    Text,
    Timestamp,
    Inet,
    SmallInt,
    TinyInt,
    Time,
    Timeuuid,
    Uuid,
    Varint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CollectionType {
    List(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Set(Box<CqlType>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
pub enum Strategy {
    SimpleStrategy {
        replication_factor: usize,
    },
    NetworkTopologyStrategy {
        // Replication factors of datacenters with given names
        datacenter_repfactors: HashMap<String, usize>,
    },
    LocalStrategy, // replication_factor == 1
    Other {
        name: String,
        data: HashMap<String, String>,
    },
}

#[derive(Clone, Debug)]
struct InvalidCqlType(String);

impl fmt::Display for InvalidCqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<InvalidCqlType> for QueryError {
    fn from(e: InvalidCqlType) -> Self {
        // FIXME: The correct error type is QueryError:ProtocolError but at the moment it accepts only &'static str
        QueryError::InvalidMessage(format!("type {} is not implemented", e.0))
    }
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to known_peers in the background
    pub fn new(
        known_peers: &[SocketAddr],
        mut connection_config: ConnectionConfig,
        server_event_sender: mpsc::Sender<Event>,
    ) -> Self {
        let control_connection_address = *known_peers
            .choose(&mut thread_rng())
            .expect("Tried to initialize MetadataReader with empty known_peers list!");

        // setting event_sender field in connection config will cause control connection to
        // - send REGISTER message to receive server events
        // - send received events via server_event_sender
        connection_config.event_sender = Some(server_event_sender);

        let control_connection = Self::make_control_connection_pool(
            control_connection_address,
            connection_config.clone(),
        );

        MetadataReader {
            control_connection_address,
            control_connection,
            connection_config,
            known_peers: known_peers.into(),
        }
    }

    /// Fetches current metadata from the cluster
    pub async fn read_metadata(&mut self) -> Result<Metadata, QueryError> {
        let mut result = self.fetch_metadata().await;
        if let Ok(metadata) = result {
            self.update_known_peers(&metadata);
            return Ok(metadata);
        }

        // shuffle known_peers to iterate through them in random order later
        self.known_peers.shuffle(&mut thread_rng());

        let address_of_failed_control_connection = self.control_connection_address;
        let filtered_known_peers = self
            .known_peers
            .iter()
            .filter(|&peer| peer != &address_of_failed_control_connection);

        // if fetching metadata on current control connection failed,
        // try to fetch metadata from other known peer
        for peer in filtered_known_peers {
            let err = match result {
                Ok(_) => break,
                Err(err) => err,
            };

            warn!(
                control_connection_address = self.control_connection_address.to_string().as_str(),
                error = err.to_string().as_str(),
                "Failed to fetch metadata using current control connection"
            );

            self.control_connection_address = *peer;
            self.control_connection = Self::make_control_connection_pool(
                self.control_connection_address,
                self.connection_config.clone(),
            );

            result = self.fetch_metadata().await;
        }

        match &result {
            Ok(metadata) => {
                self.update_known_peers(metadata);
                debug!("Fetched new metadata");
            }
            Err(error) => error!(
                error = error.to_string().as_str(),
                "Could not fetch metadata"
            ),
        }

        result
    }

    async fn fetch_metadata(&self) -> Result<Metadata, QueryError> {
        // TODO: Timeouts?

        query_metadata(
            &self.control_connection,
            self.control_connection_address.port(),
        )
        .await
    }

    fn update_known_peers(&mut self, metadata: &Metadata) {
        self.known_peers = metadata.peers.iter().map(|peer| peer.address).collect();
    }

    fn make_control_connection_pool(
        addr: SocketAddr,
        connection_config: ConnectionConfig,
    ) -> NodeConnectionPool {
        let pool_config = PoolConfig {
            connection_config,

            // We want to have only one connection to receive events from
            pool_size: PoolSize::PerHost(NonZeroUsize::new(1).unwrap()),

            // The shard-aware port won't be used with PerHost pool size anyway,
            // so explicitly disable it here
            can_use_shard_aware_port: false,
        };

        NodeConnectionPool::new(addr.ip(), addr.port(), pool_config, None)
    }
}

async fn query_metadata(
    pool: &NodeConnectionPool,
    connect_port: u16,
) -> Result<Metadata, QueryError> {
    pool.wait_until_initialized().await;
    let conn: &Connection = &*pool.random_connection()?;

    let peers_query = query_peers(conn, connect_port);
    let keyspaces_query = query_keyspaces(conn);

    let (peers, keyspaces) = tokio::try_join!(peers_query, keyspaces_query)?;

    // There must be at least one peer
    if peers.is_empty() {
        return Err(QueryError::ProtocolError(
            "Bad Metadata: peers list is empty",
        ));
    }

    // At least one peer has to have some tokens
    if peers.iter().all(|peer| peer.tokens.is_empty()) {
        return Err(QueryError::ProtocolError(
            "Bad Metadata: All peers have empty token list",
        ));
    }

    Ok(Metadata { peers, keyspaces })
}

async fn query_peers(conn: &Connection, connect_port: u16) -> Result<Vec<Peer>, QueryError> {
    let mut peers_query = Query::new("select peer, data_center, rack, tokens from system.peers");
    peers_query.set_page_size(1024);
    let peers_query_future = conn.query_all(&peers_query, &[]);

    let mut local_query =
        Query::new("select rpc_address, data_center, rack, tokens from system.local");
    local_query.set_page_size(1024);
    let local_query_future = conn.query_all(&local_query, &[]);

    let (peers_res, local_res) = tokio::try_join!(peers_query_future, local_query_future)?;

    let peers_rows = peers_res.rows.ok_or(QueryError::ProtocolError(
        "system.peers query response was not Rows",
    ))?;

    let local_rows = local_res.rows.ok_or(QueryError::ProtocolError(
        "system.local query response was not Rows",
    ))?;

    let mut result: Vec<Peer> = Vec::with_capacity(peers_rows.len() + 1);

    let typed_peers_rows =
        peers_rows.into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>();

    // For the local node we should use connection's address instead of rpc_address unless SNI is enabled (TODO)
    // Replace address in local_rows with connection's address
    let local_address: IpAddr = conn.get_connect_address().ip();
    let typed_local_rows = local_rows
        .into_typed::<(IpAddr, Option<String>, Option<String>, Option<Vec<String>>)>()
        .map(|res| res.map(|(_addr, dc, rack, tokens)| (local_address, dc, rack, tokens)));

    for row in typed_peers_rows.chain(typed_local_rows) {
        let (ip_address, datacenter, rack, tokens) = row.map_err(|_| {
            QueryError::ProtocolError("system.peers or system.local has invalid column type")
        })?;

        let tokens_str: Vec<String> = tokens.unwrap_or_default();

        let address = SocketAddr::new(ip_address, connect_port);

        // Parse string representation of tokens as integer values
        let tokens: Vec<Token> = match tokens_str
            .iter()
            .map(|s| Token::from_str(s))
            .collect::<Result<Vec<Token>, _>>()
        {
            Ok(parsed) => parsed,
            Err(e) => {
                // FIXME: we could allow the users to provide custom partitioning information
                // in order for it to work with non-standard token sizes.
                // Also, we could implement support for Cassandra's other standard partitioners
                // like RandomPartitioner or ByteOrderedPartitioner.
                trace!("Couldn't parse tokens as 64-bit integers: {}, proceeding with a dummy token. If you're using a partitioner with different token size, consider migrating to murmur3", e);
                vec![Token {
                    value: rand::thread_rng().gen::<i64>(),
                }]
            }
        };
        result.push(Peer {
            address,
            tokens,
            datacenter,
            rack,
        });
    }

    Ok(result)
}

async fn query_keyspaces(conn: &Connection) -> Result<HashMap<String, Keyspace>, QueryError> {
    let mut keyspaces_query =
        Query::new("select keyspace_name, replication from system_schema.keyspaces");
    keyspaces_query.set_page_size(1024);

    let rows =
        conn.query_all(&keyspaces_query, &[])
            .await?
            .rows
            .ok_or(QueryError::ProtocolError(
                "system_schema.keyspaces query response was not Rows",
            ))?;

    let mut result = HashMap::with_capacity(rows.len());
    let mut all_tables = query_tables(conn).await?;

    for row in rows.into_typed::<(String, HashMap<String, String>)>() {
        let (keyspace_name, strategy_map) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.keyspaces has invalid column type")
        })?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;
        let tables = all_tables.remove(&keyspace_name).unwrap_or_default();

        result.insert(keyspace_name, Keyspace { strategy, tables });
    }

    Ok(result)
}

async fn query_tables(
    conn: &Connection,
) -> Result<HashMap<String, HashMap<String, Table>>, QueryError> {
    let mut tables_query = Query::new("select keyspace_name, table_name from system_schema.tables");
    tables_query.set_page_size(1024);

    let rows = conn
        .query_all(&tables_query, &[])
        .await?
        .rows
        .ok_or(QueryError::ProtocolError(
            "system_schema.tables query response was not Rows",
        ))?;

    let mut result = HashMap::with_capacity(rows.len());
    let mut columns = query_columns(conn).await?;

    for row in rows.into_typed::<(String, String)>() {
        let (keyspace_name, table_name) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.tables has invalid column type")
        })?;

        let keyspace_and_table_name = (keyspace_name, table_name);

        let columns = columns.remove(&keyspace_and_table_name).unwrap_or_default();

        result
            .entry(keyspace_and_table_name.0)
            .or_insert_with(HashMap::new)
            .insert(keyspace_and_table_name.1, Table { columns });
    }

    Ok(result)
}

async fn query_columns(
    conn: &Connection,
) -> Result<HashMap<(String, String), HashMap<String, Column>>, QueryError> {
    // Upon migration from thrift to CQL, Cassandra internally creates a surrogate column "value" of
    // type EmptyType for dense tables. This resolves into this CQL type name.
    // This column shouldn't be exposed to the user but is currently exposed in system tables.
    const THRIFT_EMPTY_TYPE: &str = "empty";

    let mut columns_query = Query::new(
        "select keyspace_name, table_name, column_name, type from system_schema.columns",
    );
    columns_query.set_page_size(1024);

    let rows = conn
        .query_all(&columns_query, &[])
        .await?
        .rows
        .ok_or(QueryError::ProtocolError(
            "system_schema.columns query response was not Rows",
        ))?;

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String, String, String)>() {
        let (keyspace_name, table_name, column_name, type_) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.columns has invalid column type")
        })?;

        if type_ == THRIFT_EMPTY_TYPE {
            continue;
        }

        let cql_type = map_string_to_cql_type(&type_)?;
        result
            .entry((keyspace_name, table_name))
            .or_insert_with(HashMap::new)
            .insert(column_name, Column { type_: cql_type });
    }

    Ok(result)
}

fn map_string_to_cql_type(type_: &str) -> Result<CqlType, InvalidCqlType> {
    use CollectionType::*;

    let frozen = type_.starts_with("frozen<");

    let type_ = if frozen {
        let langle_position = type_.find('<').unwrap();
        &type_[langle_position + 1..type_.len() - 1]
    } else {
        type_
    };

    if type_.ends_with('>') && type_.contains('<') {
        let langle_position = type_.find('<').unwrap();
        let inner_type = &type_[langle_position + 1..type_.len() - 1];
        let outer_type = &type_[..langle_position];
        let type_ = match outer_type {
            "map" | "tuple" => {
                let types = inner_type.split(", ").collect::<Vec<_>>();
                if outer_type == "map" {
                    if types.len() != 2 {
                        return Err(InvalidCqlType(type_.to_string()));
                    }
                    Map(
                        Box::new(map_string_to_cql_type(types[0])?),
                        Box::new(map_string_to_cql_type(types[1])?),
                    )
                } else {
                    return Ok(CqlType::Tuple(
                        types
                            .iter()
                            .map(|type_| map_string_to_cql_type(type_))
                            .collect::<Result<Vec<_>, _>>()?,
                    ));
                }
            }
            "set" => Set(Box::new(map_string_to_cql_type(inner_type)?)),
            "list" => List(Box::new(map_string_to_cql_type(inner_type)?)),
            _ => {
                return Err(InvalidCqlType(type_.to_string()));
            }
        };
        return Ok(CqlType::Collection { frozen, type_ });
    }

    Ok(match NativeType::from_str(type_) {
        Ok(type_) => CqlType::Native(type_),
        _ => CqlType::UserDefinedType {
            frozen,
            name: type_.to_string(),
        },
    })
}

fn strategy_from_string_map(
    mut strategy_map: HashMap<String, String>,
) -> Result<Strategy, QueryError> {
    let strategy_name: String = strategy_map
        .remove("class")
        .ok_or(QueryError::ProtocolError(
            "strategy map should have a 'class' field",
        ))?;

    let strategy: Strategy = match strategy_name.as_str() {
        "org.apache.cassandra.locator.SimpleStrategy" | "SimpleStrategy" => {
            let rep_factor_str: String =
                strategy_map
                    .remove("replication_factor")
                    .ok_or(QueryError::ProtocolError(
                        "SimpleStrategy in strategy map does not have a replication factor",
                    ))?;

            let replication_factor: usize = usize::from_str(&rep_factor_str).map_err(|_| {
                QueryError::ProtocolError("Could not parse replication factor as an integer")
            })?;

            Strategy::SimpleStrategy { replication_factor }
        }
        "org.apache.cassandra.locator.NetworkTopologyStrategy" | "NetworkTopologyStrategy" => {
            let mut datacenter_repfactors: HashMap<String, usize> =
                HashMap::with_capacity(strategy_map.len());

            for (datacenter, rep_factor_str) in strategy_map.drain() {
                let rep_factor: usize = match usize::from_str(&rep_factor_str) {
                    Ok(number) => number,
                    Err(_) => continue, // There might be other things in the map, we care only about rep_factors
                };

                datacenter_repfactors.insert(datacenter, rep_factor);
            }

            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            }
        }
        "org.apache.cassandra.locator.LocalStrategy" | "LocalStrategy" => Strategy::LocalStrategy,
        _ => Strategy::Other {
            name: strategy_name,
            data: strategy_map,
        },
    };

    Ok(strategy)
}
