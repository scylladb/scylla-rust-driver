use crate::frame::response::event::Event;
use crate::routing::Token;
use crate::statement::query::Query;
use crate::transport::connection::{Connection, ConnectionConfig};
use crate::transport::connection_pool::{NodeConnectionPool, PoolConfig, PoolSize};
use crate::transport::errors::{DbError, QueryError};
use crate::transport::host_filter::HostFilter;
use crate::transport::session::AddressTranslator;
use crate::utils::parse::{ParseErrorCause, ParseResult, ParserState};

use futures::future::{self, FutureExt};
use futures::stream::{self, StreamExt, TryStreamExt};
use futures::Stream;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use scylla_cql::frame::response::result::Row;
use scylla_cql::frame::value::ValueList;
use scylla_macros::FromRow;
use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use strum_macros::EnumString;
use tokio::sync::mpsc;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

/// Allows to read current metadata from the cluster
pub(crate) struct MetadataReader {
    connection_config: ConnectionConfig,
    keepalive_interval: Option<Duration>,

    control_connection_address: SocketAddr,
    control_connection: NodeConnectionPool,

    // when control connection fails, MetadataReader tries to connect to one of known_peers
    known_peers: Vec<SocketAddr>,
    keyspaces_to_fetch: Vec<String>,
    fetch_schema: bool,

    address_translator: Option<Arc<dyn AddressTranslator>>,
    host_filter: Option<Arc<dyn HostFilter>>,
}

/// Describes all metadata retrieved from the cluster
pub struct Metadata {
    pub peers: Vec<Peer>,
    pub keyspaces: HashMap<String, Keyspace>,
}

#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct Peer {
    pub host_id: Uuid,
    pub address: SocketAddr,
    pub untranslated_address: Option<SocketAddr>,
    pub tokens: Vec<Token>,
    pub datacenter: Option<String>,
    pub rack: Option<String>,
}

#[non_exhaustive] // <- so that we can add more fields in a backwards-compatible way
pub struct UntranslatedPeer {
    pub host_id: Uuid,
    pub untranslated_address: SocketAddr,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Keyspace {
    pub strategy: Strategy,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub tables: HashMap<String, Table>,
    /// Empty HashMap may as well mean that the client disabled schema fetching in SessionConfig
    pub views: HashMap<String, MaterializedView>,
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
pub struct MaterializedView {
    pub view_metadata: Table,
    pub base_table_name: String,
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

impl Metadata {
    /// Creates new, dummy metadata from a given list of peers.
    ///
    /// It can be used as a replacement for real metadata when initial
    /// metadata read fails.
    pub fn new_dummy(initial_peers: &[SocketAddr]) -> Self {
        let peers = initial_peers
            .iter()
            .enumerate()
            .map(|(id, addr)| {
                // Given N nodes, divide the ring into N roughly equal parts
                // and assign them to each node.
                let token = ((id as u128) << 64) / initial_peers.len() as u128;

                Peer {
                    address: *addr,
                    tokens: vec![Token {
                        value: token as i64,
                    }],
                    datacenter: None,
                    rack: None,
                    untranslated_address: None,
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

impl MetadataReader {
    /// Creates new MetadataReader, which connects to known_peers in the background
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        known_peers: &[SocketAddr],
        mut connection_config: ConnectionConfig,
        keepalive_interval: Option<Duration>,
        server_event_sender: mpsc::Sender<Event>,
        keyspaces_to_fetch: Vec<String>,
        fetch_schema: bool,
        address_translator: &Option<Arc<dyn AddressTranslator>>,
        host_filter: &Option<Arc<dyn HostFilter>>,
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
            keyspaces_to_fetch,
            fetch_schema,
            address_translator: address_translator.clone(),
            host_filter: host_filter.clone(),
        }
    }

    /// Fetches current metadata from the cluster
    pub async fn read_metadata(&mut self, initial: bool) -> Result<Metadata, QueryError> {
        let mut result = self.fetch_metadata(initial).await;
        if let Ok(metadata) = result {
            self.update_known_peers(&metadata);
            if initial {
                self.handle_unaccepted_host_in_control_connection(&metadata);
            }
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
            result = self.fetch_metadata(initial).await;
        }

        match &result {
            Ok(metadata) => {
                self.update_known_peers(metadata);
                self.handle_unaccepted_host_in_control_connection(metadata);
                debug!("Fetched new metadata");
            }
            Err(error) => error!(
                error = error.to_string().as_str(),
                "Could not fetch metadata"
            ),
        }

        result
    }

    async fn fetch_metadata(&self, initial: bool) -> Result<Metadata, QueryError> {
        // TODO: Timeouts?
        self.control_connection.wait_until_initialized().await;
        let conn = &self.control_connection.random_connection()?;

        let res = query_metadata(
            conn,
            self.control_connection_address.port(),
            self.address_translator.as_deref(),
            &self.keyspaces_to_fetch,
            self.fetch_schema,
        )
        .await;

        if initial {
            if let Err(err) = res {
                warn!(
                    error = ?err,
                    "Initial metadata read failed, proceeding with metadata \
                    consisting only of the initial peer list and dummy tokens. \
                    This might result in suboptimal performance and schema \
                    information not being available."
                );
                return Ok(Metadata::new_dummy(&self.known_peers));
            }
        }

        res
    }

    fn update_known_peers(&mut self, metadata: &Metadata) {
        let host_filter = self.host_filter.as_ref();
        self.known_peers = metadata
            .peers
            .iter()
            .filter(|peer| host_filter.map_or(true, |f| f.accept(peer)))
            .map(|peer| peer.address)
            .collect();

        // Check if the host filter isn't accidentally too restrictive,
        // and print an error message about this fact
        if !metadata.peers.is_empty() && self.known_peers.is_empty() {
            error!(
                node_ips = ?metadata
                    .peers
                    .iter()
                    .map(|peer| peer.address)
                    .collect::<Vec<_>>(),
                "The host filter rejected all nodes in the cluster, \
                no connections that can serve user queries have been \
                established. The session cannot serve any queries!"
            )
        }
    }

    fn handle_unaccepted_host_in_control_connection(&mut self, metadata: &Metadata) {
        let control_connection_peer = metadata
            .peers
            .iter()
            .find(|peer| peer.address == self.control_connection_address);
        if let Some(peer) = control_connection_peer {
            if !self.host_filter.as_ref().map_or(true, |f| f.accept(peer)) {
                warn!(
                    filtered_node_ips = ?metadata
                        .peers
                        .iter()
                        .filter(|peer| self.host_filter.as_ref().map_or(true, |p| p.accept(peer)))
                        .map(|peer| peer.address)
                        .collect::<Vec<_>>(),
                    control_connection_address = ?self.control_connection_address,
                    "The node that the control connection is established to \
                    is not accepted by the host filter. Please verify that \
                    the nodes in your initial peers list are accepted by the \
                    host filter. The driver will try to re-establish the \
                    control connection to a different node."
                );

                // Assuming here that known_peers are up-to-date
                if !self.known_peers.is_empty() {
                    self.control_connection_address = *self
                        .known_peers
                        .choose(&mut thread_rng())
                        .expect("known_peers is empty - should be impossible");

                    self.control_connection = Self::make_control_connection_pool(
                        self.control_connection_address,
                        self.connection_config.clone(),
                        self.keepalive_interval,
                    );
                }
            }
        }
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
    conn: &Arc<Connection>,
    connect_port: u16,
    address_translator: Option<&dyn AddressTranslator>,
    keyspace_to_fetch: &[String],
    fetch_schema: bool,
) -> Result<Metadata, QueryError> {
    let peers_query = query_peers(conn, connect_port, address_translator);
    let keyspaces_query = query_keyspaces(conn, keyspace_to_fetch, fetch_schema);

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

#[derive(FromRow)]
#[scylla_crate = "scylla_cql"]
struct NodeInfoRow {
    host_id: Option<Uuid>,
    untranslated_ip_addr: IpAddr,
    datacenter: Option<String>,
    rack: Option<String>,
    tokens: Option<Vec<String>>,
}

#[derive(Clone, Copy)]
enum NodeInfoSource {
    Local,
    Peer,
}

impl NodeInfoSource {
    fn describe(&self) -> &'static str {
        match self {
            Self::Local => "local node",
            Self::Peer => "peer",
        }
    }
}

async fn query_peers(
    conn: &Arc<Connection>,
    connect_port: u16,
    address_translator: Option<&dyn AddressTranslator>,
) -> Result<Vec<Peer>, QueryError> {
    let mut peers_query =
        Query::new("select host_id, rpc_address, data_center, rack, tokens from system.peers");
    peers_query.set_page_size(1024);
    let peers_query_stream = conn
        .clone()
        .query_iter(peers_query, &[])
        .into_stream()
        .try_flatten()
        .and_then(|row_result| future::ok((NodeInfoSource::Peer, row_result)));

    let mut local_query =
        Query::new("select host_id, rpc_address, data_center, rack, tokens from system.local");
    local_query.set_page_size(1024);
    let local_query_stream = conn
        .clone()
        .query_iter(local_query, &[])
        .into_stream()
        .try_flatten()
        .and_then(|row_result| future::ok((NodeInfoSource::Local, row_result)));

    let untranslated_rows = stream::select(peers_query_stream, local_query_stream);

    let local_ip: IpAddr = conn.get_connect_address().ip();
    let local_address = SocketAddr::new(local_ip, connect_port);

    let translated_peers_futures = untranslated_rows.map(|row_result| async {
        let (source, raw_row) = row_result?;
        let row = raw_row.into_typed().map_err(|_| {
            QueryError::ProtocolError("system.peers or system.local has invalid column type")
        })?;
        create_peer_from_row(source, row, local_address, address_translator).await
    });

    let peers = translated_peers_futures
        .buffer_unordered(256)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(peers.into_iter().flatten().collect())
}

async fn create_peer_from_row(
    source: NodeInfoSource,
    row: NodeInfoRow,
    local_address: SocketAddr,
    address_translator: Option<&dyn AddressTranslator>,
) -> Result<Option<Peer>, QueryError> {
    let NodeInfoRow {
        host_id,
        untranslated_ip_addr,
        datacenter,
        rack,
        tokens,
    } = row;

    let host_id = match host_id {
        Some(host_id) => host_id,
        None => {
            warn!("{} (untranslated ip: {}, dc: {:?}, rack: {:?}) has Host ID set to null; skipping node.", source.describe(), untranslated_ip_addr, datacenter, rack);
            return Ok(None);
        }
    };

    let connect_port = local_address.port();
    let untranslated_address = SocketAddr::new(untranslated_ip_addr, connect_port);

    let (untranslated_address, address) = match (source, address_translator) {
        (NodeInfoSource::Local, None) => {
            // We need to replace rpc_address with control connection address.
            (Some(untranslated_address), local_address)
        }
        (NodeInfoSource::Local, Some(_)) => {
            // The address we used to connect is most likely different and we just don't know.
            (None, local_address)
        }
        (NodeInfoSource::Peer, None) => {
            // The usual case - no translation.
            (Some(untranslated_address), untranslated_address)
        }
        (NodeInfoSource::Peer, Some(translator)) => {
            // We use the provided translator and skip the peer if there is no rule for translating it.
            (
                Some(untranslated_address),
                match translator
                    .translate_address(&UntranslatedPeer {
                        host_id,
                        untranslated_address,
                    })
                    .await
                {
                    Ok(address) => address,
                    Err(err) => {
                        warn!("Could not translate address {}; TranslationError: {:?}; node therefore skipped.",
                                untranslated_address, err);
                        return Ok::<Option<Peer>, QueryError>(None);
                    }
                },
            )
        }
    };

    let tokens_str: Vec<String> = tokens.unwrap_or_default();

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

    Ok(Some(Peer {
        host_id,
        untranslated_address,
        address,
        tokens,
        datacenter,
        rack,
    }))
}

fn query_filter_keyspace_name(
    conn: &Arc<Connection>,
    query_str: &str,
    keyspaces_to_fetch: &[String],
) -> impl Stream<Item = Result<Row, QueryError>> {
    let keyspaces = &[keyspaces_to_fetch] as &[&[String]];
    let (query_str, query_values) = if !keyspaces_to_fetch.is_empty() {
        (format!("{query_str} where keyspace_name in ?"), keyspaces)
    } else {
        (query_str.into(), &[] as &[&[String]])
    };
    let query_values = query_values.serialized().map(|sv| sv.into_owned());
    let mut query = Query::new(query_str);
    let conn = conn.clone();
    query.set_page_size(1024);
    let fut = async move {
        let query_values = query_values?;
        conn.query_iter(query, query_values).await
    };
    fut.into_stream().try_flatten()
}

async fn query_keyspaces(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
    fetch_schema: bool,
) -> Result<HashMap<String, Keyspace>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "select keyspace_name, replication from system_schema.keyspaces",
        keyspaces_to_fetch,
    );

    let (mut all_tables, mut all_views, mut all_user_defined_types) = if fetch_schema {
        let udts = query_user_defined_types(conn, keyspaces_to_fetch).await?;
        (
            query_tables(conn, keyspaces_to_fetch).await?,
            query_views(conn, keyspaces_to_fetch).await?,
            udts,
        )
    } else {
        (HashMap::new(), HashMap::new(), HashMap::new())
    };

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, strategy_map) = row.into_typed().map_err(|_| {
            QueryError::ProtocolError("system_schema.keyspaces has invalid column type")
        })?;

        let strategy: Strategy = strategy_from_string_map(strategy_map)?;
        let tables = all_tables.remove(&keyspace_name).unwrap_or_default();
        let views = all_views.remove(&keyspace_name).unwrap_or_default();
        let user_defined_types = all_user_defined_types
            .remove(&keyspace_name)
            .unwrap_or_default();

        let keyspace = Keyspace {
            strategy,
            tables,
            views,
            user_defined_types,
        };

        Ok((keyspace_name, keyspace))
    })
    .try_collect()
    .await
}

#[derive(FromRow, Debug)]
#[scylla_crate = "crate"]
struct UdtRow {
    keyspace_name: String,
    type_name: String,
    field_names: Vec<String>,
    field_types: Vec<String>,
}

async fn query_user_defined_types(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
) -> Result<HashMap<String, HashMap<String, Vec<(String, CqlType)>>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "select keyspace_name, type_name, field_names, field_types from system_schema.types",
        keyspaces_to_fetch,
    );

    let mut result = HashMap::new();

    rows.map(|row_result| {
        let row = row_result?;
        let UdtRow {
            keyspace_name,
            type_name,
            field_names,
            field_types,
        } = row.into_typed().map_err(|_| {
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

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    Ok(result)
}

async fn query_tables(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
) -> Result<HashMap<String, HashMap<String, Table>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "SELECT keyspace_name, table_name FROM system_schema.tables",
        keyspaces_to_fetch,
    );
    let mut result = HashMap::new();
    let mut tables = query_tables_schema(conn, keyspaces_to_fetch).await?;

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, table_name) = row.into_typed().map_err(|_| {
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

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    Ok(result)
}

async fn query_views(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
) -> Result<HashMap<String, HashMap<String, MaterializedView>>, QueryError> {
    let rows = query_filter_keyspace_name(
        conn,
        "SELECT keyspace_name, view_name, base_table_name FROM system_schema.views",
        keyspaces_to_fetch,
    );

    let mut result = HashMap::new();
    let mut tables = query_tables_schema(conn, keyspaces_to_fetch).await?;

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, view_name, base_table_name) = row.into_typed().map_err(|_| {
            QueryError::ProtocolError("system_schema.views has invalid column type")
        })?;

        let keyspace_and_view_name = (keyspace_name, view_name);

        let table = tables.remove(&keyspace_and_view_name).unwrap_or(Table {
            columns: HashMap::new(),
            partition_key: vec![],
            clustering_key: vec![],
            partitioner: None,
        });
        let materialized_view = MaterializedView {
            view_metadata: table,
            base_table_name,
        };

        result
            .entry(keyspace_and_view_name.0)
            .or_insert_with(HashMap::new)
            .insert(keyspace_and_view_name.1, materialized_view);

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

    Ok(result)
}

async fn query_tables_schema(
    conn: &Arc<Connection>,
    keyspaces_to_fetch: &[String],
) -> Result<HashMap<(String, String), Table>, QueryError> {
    // Upon migration from thrift to CQL, Cassandra internally creates a surrogate column "value" of
    // type EmptyType for dense tables. This resolves into this CQL type name.
    // This column shouldn't be exposed to the user but is currently exposed in system tables.
    const THRIFT_EMPTY_TYPE: &str = "empty";

    let rows = query_filter_keyspace_name(conn,
        "select keyspace_name, table_name, column_name, kind, position, type from system_schema.columns", keyspaces_to_fetch
    );

    let mut tables_schema = HashMap::new();

    rows.map(|row_result| {
        let row = row_result?;
        let (keyspace_name, table_name, column_name, kind, position, type_): (
            String,
            String,
            String,
            String,
            i32,
            String,
        ) = row.into_typed().map_err(|_| {
            QueryError::ProtocolError("system_schema.columns has invalid column type")
        })?;

        if type_ == THRIFT_EMPTY_TYPE {
            return Ok::<_, QueryError>(());
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

        Ok::<_, QueryError>(())
    })
    .try_for_each(|_| future::ok(()))
    .await?;

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
    conn: &Arc<Connection>,
) -> Result<HashMap<(String, String), Option<String>>, QueryError> {
    let mut partitioner_query = Query::new(
        "select keyspace_name, table_name, partitioner from system_schema.scylla_tables",
    );
    partitioner_query.set_page_size(1024);

    let rows = conn
        .clone()
        .query_iter(partitioner_query, &[])
        .into_stream()
        .try_flatten();

    let result = rows
        .map(|row_result| {
            let (keyspace_name, table_name, partitioner) =
                row_result?.into_typed().map_err(|_| {
                    QueryError::ProtocolError("system_schema.tables has invalid column type")
                })?;
            Ok::<_, QueryError>(((keyspace_name, table_name), partitioner))
        })
        .try_collect::<HashMap<_, _>>()
        .await;

    match result {
        // FIXME: This match catches all database errors with this error code despite the fact
        // that we are only interested in the ones resulting from non-existent table
        // system_schema.scylla_tables.
        // For more information please refer to https://github.com/scylladb/scylla-rust-driver/pull/349#discussion_r762050262
        Err(QueryError::DbError(DbError::Invalid, _)) => Ok(HashMap::new()),
        result => result,
    }
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
