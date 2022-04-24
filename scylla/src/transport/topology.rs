use crate::frame::response::event::Event;
use crate::routing::Token;
use crate::statement::query::Query;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig, PoolSize};
use crate::transport::errors::{DbError, QueryError};
use crate::transport::session::IntoTypedRows;
use crate::utils::parse::{ParseErrorCause, ParseResult, ParserState};

use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::time::Duration;
use strum_macros::EnumString;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    connection_config: ConnectionConfig,
    keepalive_interval: Option<Duration>,

    control_connection_address: SocketAddr,
    control_connection: NodeConnectionPool,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<SocketAddr>,
    fetch_schema: bool,
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
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub tables: HashMap<String, Table>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub user_defined_types: HashMap<String, Vec<(String, CqlType)>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub columns: HashMap<String, Column>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub partitioner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Column {
    pub type_: CqlType,
    pub kind: ColumnKind,
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

#[derive(Clone, Debug, PartialEq, Eq, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum ColumnKind {
    Regular,
    Static,
    Clustering,
    PartitionKey,
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
struct InvalidCqlType {
    type_: String,
    position: usize,
    reason: String,
}

impl fmt::Display for InvalidCqlType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<InvalidCqlType> for QueryError {
    fn from(e: InvalidCqlType) -> Self {
        // FIXME: The correct error type is QueryError:ProtocolError but at the moment it accepts only &'static str
        QueryError::InvalidMessage(format!(
            "error parsing type \"{:?}\" at position {}: {}",
            e.type_, e.position, e.reason
        ))
    }
}

impl MetadataReader {
    /// Creates new MetadataReader, which connects to known_peers in the background
    pub fn new(
        known_peers: &[SocketAddr],
        mut connection_config: ConnectionConfig,
        keepalive_interval: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
        fetch_schema: bool,
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
            keepalive_interval,
        );

        MetadataReader {
            control_connection_address,
            control_connection,
            keepalive_interval,
            connection_config,
            known_peers: known_peers.into(),
            fetch_schema,
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
        debug!(
            "Known peers: {}",
            self.known_peers
                .iter()
                .map(std::net::SocketAddr::to_string)
                .collect::<Vec<String>>()
                .join(", ")
        );

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
                self.keepalive_interval,
            );

            debug!(
                "Retrying to establish the control connection on {}",
                self.control_connection_address
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
            self.fetch_schema,
        )
        .await
    }

    fn update_known_peers(&mut self, metadata: &Metadata) {
        self.known_peers = metadata.peers.iter().map(|peer| peer.address).collect();
    }

    fn make_control_connection_pool(
        addr: SocketAddr,
        connection_config: ConnectionConfig,
        keepalive_interval: Option<Duration>,
    ) -> NodeConnectionPool {
        let pool_config = PoolConfig {
            connection_config,
            keepalive_interval,

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
    fetch_schema: bool,
) -> Result<Metadata, QueryError> {
    pool.wait_until_initialized().await;
    let conn: &Connection = &*pool.random_connection()?;

    let peers_query = query_peers(conn, connect_port);
    let keyspaces_query = query_keyspaces(conn, fetch_schema);

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

async fn query_keyspaces(
    conn: &Connection,
    fetch_schema: bool,
) -> Result<HashMap<String, Keyspace>, QueryError> {
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
    let (mut all_tables, mut all_user_defined_types) = if fetch_schema {
        (
            query_tables(conn).await?,
            query_user_defined_types(conn).await?,
        )
    } else {
        (HashMap::new(), HashMap::new())
    };

    for row in rows.into_typed::<(String, HashMap<String, String>)>() {
        let (keyspace_name, strategy_map) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.keyspaces has invalid column type")
        })?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;
        let tables = all_tables.remove(&keyspace_name).unwrap_or_default();
        let user_defined_types = all_user_defined_types
            .remove(&keyspace_name)
            .unwrap_or_default();

        result.insert(
            keyspace_name,
            Keyspace {
                strategy,
                tables,
                user_defined_types,
            },
        );
    }

    Ok(result)
}

async fn query_user_defined_types(
    conn: &Connection,
) -> Result<HashMap<String, HashMap<String, Vec<(String, CqlType)>>>, QueryError> {
    let mut user_defined_types_query = Query::new(
        "select keyspace_name, type_name, field_names, field_types from system_schema.types",
    );
    user_defined_types_query.set_page_size(1024);

    let rows = conn
        .query_all(&user_defined_types_query, &[])
        .await?
        .rows
        .ok_or(QueryError::ProtocolError(
            "system_schema.types query response was not Rows",
        ))?;

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String, Vec<String>, Vec<String>)>() {
        let (keyspace_name, type_name, field_names, field_types) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.types has invalid column type")
        })?;

        let mut fields = Vec::with_capacity(field_names.len());

        for (field_name, field_type) in field_names.into_iter().zip(field_types.iter()) {
            fields.push((field_name, map_string_to_cql_type(field_type)?));
        }

        result
            .entry(keyspace_name)
            .or_insert_with(HashMap::new)
            .insert(type_name, fields);
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
    let mut tables = query_tables_schema(conn).await?;

    for row in rows.into_typed::<(String, String)>() {
        let (keyspace_name, table_name) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.tables has invalid column type")
        })?;

        let keyspace_and_table_name = (keyspace_name, table_name);

        let table = tables.remove(&keyspace_and_table_name).unwrap_or(Table {
            columns: HashMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            partitioner: None,
        });

        result
            .entry(keyspace_and_table_name.0)
            .or_insert_with(HashMap::new)
            .insert(keyspace_and_table_name.1, table);
    }

    Ok(result)
}

async fn query_tables_schema(
    conn: &Connection,
) -> Result<HashMap<(String, String), Table>, QueryError> {
    // Upon migration from thrift to CQL, Cassandra internally creates a surrogate column "value" of
    // type EmptyType for dense tables. This resolves into this CQL type name.
    // This column shouldn't be exposed to the user but is currently exposed in system tables.
    const THRIFT_EMPTY_TYPE: &str = "empty";

    let mut columns_query = Query::new(
        "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns",
    );
    columns_query.set_page_size(1024);

    let rows = conn
        .query_all(&columns_query, &[])
        .await?
        .rows
        .ok_or(QueryError::ProtocolError(
            "system_schema.columns query response was not Rows",
        ))?;

    let mut tables_schema = HashMap::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String, String, String, i32, String)>() {
        let (keyspace_name, table_name, column_name, kind, position, type_) =
            row.map_err(|_| {
                QueryError::ProtocolError("system_schema.columns has invalid column type")
            })?;

        if type_ == THRIFT_EMPTY_TYPE {
            continue;
        }

        let entry = tables_schema.entry((keyspace_name, table_name)).or_insert((
            HashMap::new(), // columns
            HashMap::new(), // partition key
            HashMap::new(), // clustering key
        ));

        let cql_type = map_string_to_cql_type(&type_)?;

        let kind = ColumnKind::from_str(&kind)
            // FIXME: The correct error type is QueryError:ProtocolError but at the moment it accepts only &'static str
            .map_err(|_| QueryError::InvalidMessage(format!("invalid column kind {}", kind)))?;

        if kind == ColumnKind::PartitionKey || kind == ColumnKind::Clustering {
            let key_map = if kind == ColumnKind::PartitionKey {
                entry.1.borrow_mut()
            } else {
                entry.2.borrow_mut()
            };
            key_map.insert(position, column_name.clone());
        }

        entry.0.insert(
            column_name,
            Column {
                type_: cql_type,
                kind,
            },
        );
    }

    let mut all_partitioners = query_table_partitioners(conn).await?;
    let mut result = HashMap::new();

    for ((keyspace_name, table_name), (columns, partition_key_columns, clustering_key_columns)) in
        tables_schema
    {
        let mut partition_key = vec!["".to_string(); partition_key_columns.len()];
        for (position, column_name) in partition_key_columns {
            partition_key[position as usize] = column_name;
        }

        let mut clustering_key = vec!["".to_string(); clustering_key_columns.len()];
        for (position, column_name) in clustering_key_columns {
            clustering_key[position as usize] = column_name;
        }

        let keyspace_and_table_name = (keyspace_name, table_name);

        let partitioner = all_partitioners
            .remove(&keyspace_and_table_name)
            .unwrap_or_default();

        result.insert(
            keyspace_and_table_name,
            Table {
                columns,
                partition_key,
                clustering_key,
                partitioner,
            },
        );
    }

    Ok(result)
}

fn map_string_to_cql_type(type_: &str) -> Result<CqlType, InvalidCqlType> {
    match parse_cql_type(ParserState::new(type_)) {
        Err(err) => Err(InvalidCqlType {
            type_: type_.to_string(),
            position: err.calculate_position(type_).unwrap_or(0),
            reason: err.get_cause().to_string(),
        }),
        Ok((_, p)) if !p.is_at_eof() => Err(InvalidCqlType {
            type_: type_.to_string(),
            position: p.calculate_position(type_).unwrap_or(0),
            reason: "leftover characters".to_string(),
        }),
        Ok((typ, _)) => Ok(typ),
    }
}

fn parse_cql_type(p: ParserState) -> ParseResult<(CqlType, ParserState)> {
    if let Ok(p) = p.accept("frozen<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let frozen_type = freeze_type(inner_type);

        Ok((frozen_type, p))
    } else if let Ok(p) = p.accept("map<") {
        let (key, p) = parse_cql_type(p)?;
        let p = p.accept(",")?.skip_white();
        let (value, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = CqlType::Collection {
            frozen: false,
            type_: CollectionType::Map(Box::new(key), Box::new(value)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("list<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = CqlType::Collection {
            frozen: false,
            type_: CollectionType::List(Box::new(inner_type)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("set<") {
        let (inner_type, p) = parse_cql_type(p)?;
        let p = p.accept(">")?;

        let typ = CqlType::Collection {
            frozen: false,
            type_: CollectionType::Set(Box::new(inner_type)),
        };

        Ok((typ, p))
    } else if let Ok(p) = p.accept("tuple<") {
        let mut types = Vec::new();
        let p = p.parse_while(|p| {
            let (inner_type, p) = parse_cql_type(p)?;
            types.push(inner_type);

            if let Ok(p) = p.accept(",") {
                let p = p.skip_white();
                Ok((true, p))
            } else if let Ok(p) = p.accept(">") {
                Ok((false, p))
            } else {
                Err(p.error(ParseErrorCause::Other("expected \",\" or \">\"")))
            }
        })?;

        Ok((CqlType::Tuple(types), p))
    } else if let Ok((typ, p)) = parse_native_type(p) {
        Ok((CqlType::Native(typ), p))
    } else if let Ok((name, p)) = parse_user_defined_type(p) {
        let typ = CqlType::UserDefinedType {
            frozen: false,
            name: name.to_string(),
        };
        Ok((typ, p))
    } else {
        Err(p.error(ParseErrorCause::Other("invalid cql type")))
    }
}

fn parse_native_type(p: ParserState) -> ParseResult<(NativeType, ParserState)> {
    let (tok, p) = p.take_while(|c| c.is_alphanumeric() || c == '_');
    let typ = NativeType::from_str(tok)
        .map_err(|_| p.error(ParseErrorCause::Other("invalid native type")))?;
    Ok((typ, p))
}

fn parse_user_defined_type(p: ParserState) -> ParseResult<(&str, ParserState)> {
    // Java identifiers allow letters, underscores and dollar signs at any position
    // and digits in non-first position. Dots are accepted here because the names
    // are usually fully qualified.
    let (tok, p) = p.take_while(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '$');
    if tok.is_empty() {
        return Err(p.error(ParseErrorCause::Other("invalid user defined type")));
    }
    Ok((tok, p))
}

fn freeze_type(type_: CqlType) -> CqlType {
    match type_ {
        CqlType::Collection { type_, .. } => CqlType::Collection {
            frozen: true,
            type_,
        },
        CqlType::UserDefinedType { name, .. } => CqlType::UserDefinedType { frozen: true, name },
        other => other,
    }
}

async fn query_table_partitioners(
    conn: &Connection,
) -> Result<HashMap<(String, String), Option<String>>, QueryError> {
    let mut partitioner_query = Query::new(
        "select keyspace_name, table_name, partitioner from system_schema.scylla_tables",
    );
    partitioner_query.set_page_size(1024);

    let rows = match conn.query_all(&partitioner_query, &[]).await {
        // FIXME: This match catches all database errors with this error code despite the fact
        // that we are only interested in the ones resulting from non-existent table
        // system_schema.scylla_tables.
        // For more information please refer to https://github.com/scylladb/scylla-rust-driver/pull/349#discussion_r762050262
        Err(QueryError::DbError(DbError::Invalid, _)) => return Ok(HashMap::new()),
        query_result => query_result?.rows.ok_or(QueryError::ProtocolError(
            "system_schema.scylla_tables query response was not Rows",
        ))?,
    };

    let mut result = HashMap::with_capacity(rows.len());

    for row in rows.into_typed::<(String, String, Option<String>)>() {
        let (keyspace_name, table_name, partitioner) = row.map_err(|_| {
            QueryError::ProtocolError("system_schema.tables has invalid column type")
        })?;
        result.insert((keyspace_name, table_name), partitioner);
    }
    Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cql_type_parsing() {
        let test_cases = [
            ("bigint", CqlType::Native(NativeType::BigInt)),
            (
                "list<int>",
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int))),
                },
            ),
            (
                "set<ascii>",
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Set(Box::new(CqlType::Native(NativeType::Ascii))),
                },
            ),
            (
                "map<blob, boolean>",
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Native(NativeType::Blob)),
                        Box::new(CqlType::Native(NativeType::Boolean)),
                    ),
                },
            ),
            (
                "frozen<map<text, text>>",
                CqlType::Collection {
                    frozen: true,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Native(NativeType::Text)),
                        Box::new(CqlType::Native(NativeType::Text)),
                    ),
                },
            ),
            (
                "tuple<tinyint, smallint, int, bigint, varint>",
                CqlType::Tuple(vec![
                    CqlType::Native(NativeType::TinyInt),
                    CqlType::Native(NativeType::SmallInt),
                    CqlType::Native(NativeType::Int),
                    CqlType::Native(NativeType::BigInt),
                    CqlType::Native(NativeType::Varint),
                ]),
            ),
            (
                "com.scylladb.types.AwesomeType",
                CqlType::UserDefinedType {
                    frozen: false,
                    name: "com.scylladb.types.AwesomeType".to_string(),
                },
            ),
            (
                "frozen<ks.my_udt>",
                CqlType::UserDefinedType {
                    frozen: true,
                    name: "ks.my_udt".to_string(),
                },
            ),
            (
                "map<text, frozen<map<text, text>>>",
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Native(NativeType::Text)),
                        Box::new(CqlType::Collection {
                            frozen: true,
                            type_: CollectionType::Map(
                                Box::new(CqlType::Native(NativeType::Text)),
                                Box::new(CqlType::Native(NativeType::Text)),
                            ),
                        }),
                    ),
                },
            ),
            (
                "map<\
                    frozen<list<int>>, \
                    set<\
                        list<\
                            tuple<\
                                list<list<text>>, \
                                map<text, map<ks.my_type, blob>>, \
                                frozen<set<set<int>>>\
                            >\
                        >\
                    >\
                >",
                // map<...>
                CqlType::Collection {
                    frozen: false,
                    type_: CollectionType::Map(
                        Box::new(CqlType::Collection {
                            // frozen<list<int>>
                            frozen: true,
                            type_: CollectionType::List(Box::new(CqlType::Native(NativeType::Int))),
                        }),
                        Box::new(CqlType::Collection {
                            // set<...>
                            frozen: false,
                            type_: CollectionType::Set(Box::new(CqlType::Collection {
                                // list<tuple<...>>
                                frozen: false,
                                type_: CollectionType::List(Box::new(CqlType::Tuple(vec![
                                    CqlType::Collection {
                                        // list<list<text>>
                                        frozen: false,
                                        type_: CollectionType::List(Box::new(
                                            CqlType::Collection {
                                                frozen: false,
                                                type_: CollectionType::List(Box::new(
                                                    CqlType::Native(NativeType::Text),
                                                )),
                                            },
                                        )),
                                    },
                                    CqlType::Collection {
                                        // map<text, map<ks.my_type, blob>>
                                        frozen: false,
                                        type_: CollectionType::Map(
                                            Box::new(CqlType::Native(NativeType::Text)),
                                            Box::new(CqlType::Collection {
                                                frozen: false,
                                                type_: CollectionType::Map(
                                                    Box::new(CqlType::UserDefinedType {
                                                        frozen: false,
                                                        name: "ks.my_type".to_string(),
                                                    }),
                                                    Box::new(CqlType::Native(NativeType::Blob)),
                                                ),
                                            }),
                                        ),
                                    },
                                    CqlType::Collection {
                                        // frozen<set<set<int>>>
                                        frozen: true,
                                        type_: CollectionType::Set(Box::new(CqlType::Collection {
                                            frozen: false,
                                            type_: CollectionType::Set(Box::new(CqlType::Native(
                                                NativeType::Int,
                                            ))),
                                        })),
                                    },
                                ]))),
                            })),
                        }),
                    ),
                },
            ),
        ];

        for (s, expected) in test_cases {
            let parsed = map_string_to_cql_type(s).unwrap();
            assert_eq!(parsed, expected);
        }
    }
}
