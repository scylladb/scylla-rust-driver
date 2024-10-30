//! `Session` is the main object used in the driver.\
//! It manages all connections to the cluster and allows to perform queries.

use crate::batch::batch_values;
#[cfg(feature = "cloud")]
use crate::cloud::CloudConfig;
use crate::LegacyQueryResult;

use crate::history;
use crate::history::HistoryListener;
pub use crate::transport::errors::TranslationError;
use crate::transport::errors::{
    BadQuery, NewSessionError, ProtocolError, QueryError, UserRequestError,
};
use crate::utils::pretty::{CommaSeparatedDisplayer, CqlValueDisplayer};
use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use futures::future::join_all;
use futures::future::try_join_all;
use itertools::{Either, Itertools};
use scylla_cql::frame::response::result::{deser_cql_value, ColumnSpec};
use scylla_cql::frame::response::NonErrorResponse;
use scylla_cql::types::serialize::batch::BatchValues;
use scylla_cql::types::serialize::row::{SerializeRow, SerializedValues};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, error, trace, trace_span, Instrument};
use uuid::Uuid;

use super::connection::NonErrorQueryResponse;
use super::connection::QueryResponse;
#[cfg(feature = "ssl")]
use super::connection::SslConfig;
use super::errors::TracingProtocolError;
use super::execution_profile::{ExecutionProfile, ExecutionProfileHandle, ExecutionProfileInner};
use super::iterator::RawIterator;
use super::legacy_query_result::MaybeFirstRowTypedError;
#[cfg(feature = "cloud")]
use super::node::CloudEndpoint;
use super::node::{InternalKnownNode, KnownNode};
use super::partitioner::PartitionerName;
use super::topology::UntranslatedPeer;
use super::{NodeRef, SelfIdentity};
use crate::cql_to_rust::FromRow;
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::prepared_statement::PreparedStatement;
use crate::query::Query;
use crate::routing::{Shard, Token};
use crate::statement::{Consistency, PageSize, PagingState, PagingStateResponse};
use crate::tracing::{TracingEvent, TracingInfo};
use crate::transport::cluster::{Cluster, ClusterData, ClusterNeatDebug};
use crate::transport::connection::{Connection, ConnectionConfig, VerifiedKeyspaceName};
use crate::transport::connection_pool::PoolConfig;
use crate::transport::host_filter::HostFilter;
use crate::transport::iterator::{LegacyRowIterator, PreparedIteratorConfig};
use crate::transport::load_balancing::{self, RoutingInfo};
use crate::transport::metrics::Metrics;
use crate::transport::node::Node;
use crate::transport::query_result::QueryResult;
use crate::transport::retry_policy::{QueryInfo, RetryDecision, RetrySession};
use crate::transport::speculative_execution;
use crate::transport::Compression;
use crate::{
    batch::{Batch, BatchStatement},
    statement::StatementConfig,
};

pub use crate::transport::connection_pool::PoolSize;

use crate::authentication::AuthenticatorProvider;
#[cfg(feature = "ssl")]
use openssl::ssl::SslContext;

pub(crate) const TABLET_CHANNEL_SIZE: usize = 8192;

const TRACING_QUERY_PAGE_SIZE: i32 = 1024;

/// Translates IP addresses received from ScyllaDB nodes into locally reachable addresses.
///
/// The driver auto-detects new ScyllaDB nodes added to the cluster through server side pushed
/// notifications and through checking the system tables. For each node, the address the driver
/// receives corresponds to the address set as `rpc_address` in the node yaml file. In most
/// cases, this is the correct address to use by the driver and that is what is used by default.
/// However, sometimes the addresses received through this mechanism will either not be reachable
/// directly by the driver or should not be the preferred address to use to reach the node (for
/// instance, the `rpc_address` set on ScyllaDB nodes might be a private IP, but some clients
/// may have to use a public IP, or pass by a router, e.g. through NAT, to reach that node).
/// This interface allows to deal with such cases, by allowing to translate an address as sent
/// by a ScyllaDB node to another address to be used by the driver for connection.
///
/// Please note that the "known nodes" addresses provided while creating the [`Session`]
/// instance are not translated, only IP address retrieved from or sent by Cassandra nodes
/// to the driver are.
#[async_trait]
pub trait AddressTranslator: Send + Sync {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError>;
}

#[async_trait]
impl AddressTranslator for HashMap<SocketAddr, SocketAddr> {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        match self.get(&untranslated_peer.untranslated_address) {
            Some(&translated_addr) => Ok(translated_addr),
            None => Err(TranslationError::NoRuleForAddress(
                untranslated_peer.untranslated_address,
            )),
        }
    }
}

#[async_trait]
// Notice: this is inefficient, but what else can we do with such poor representation as str?
// After all, the cluster size is small enough to make this irrelevant.
impl AddressTranslator for HashMap<&'static str, &'static str> {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        for (&rule_addr_str, &translated_addr_str) in self.iter() {
            if let Ok(rule_addr) = SocketAddr::from_str(rule_addr_str) {
                if rule_addr == untranslated_peer.untranslated_address {
                    return SocketAddr::from_str(translated_addr_str).map_err(|reason| {
                        TranslationError::InvalidAddressInRule {
                            translated_addr_str,
                            reason,
                        }
                    });
                }
            }
        }
        Err(TranslationError::NoRuleForAddress(
            untranslated_peer.untranslated_address,
        ))
    }
}

/// `Session` manages connections to the cluster and allows to perform queries
pub struct Session {
    cluster: Cluster,
    default_execution_profile_handle: ExecutionProfileHandle,
    schema_agreement_interval: Duration,
    metrics: Arc<Metrics>,
    schema_agreement_timeout: Duration,
    schema_agreement_automatic_waiting: bool,
    refresh_metadata_on_auto_schema_agreement: bool,
    keyspace_name: ArcSwapOption<String>,
    tracing_info_fetch_attempts: NonZeroU32,
    tracing_info_fetch_interval: Duration,
    tracing_info_fetch_consistency: Consistency,
}

/// This implementation deliberately omits some details from Cluster in order
/// to avoid cluttering the print with much information of little usability.
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("cluster", &ClusterNeatDebug(&self.cluster))
            .field(
                "default_execution_profile_handle",
                &self.default_execution_profile_handle,
            )
            .field("schema_agreement_interval", &self.schema_agreement_interval)
            .field("metrics", &self.metrics)
            .field(
                "auto_await_schema_agreement_timeout",
                &self.schema_agreement_timeout,
            )
            .finish()
    }
}

/// Configuration options for [`Session`].
/// Can be created manually, but usually it's easier to use
/// [SessionBuilder](super::session_builder::SessionBuilder)
#[derive(Clone)]
#[non_exhaustive]
pub struct SessionConfig {
    /// List of database servers known on Session startup.
    /// Session will connect to these nodes to retrieve information about other nodes in the cluster.
    /// Each node can be represented as a hostname or an IP address.
    pub known_nodes: Vec<KnownNode>,

    /// Preferred compression algorithm to use on connections.
    /// If it's not supported by database server Session will fall back to no compression.
    pub compression: Option<Compression>,
    pub tcp_nodelay: bool,
    pub tcp_keepalive_interval: Option<Duration>,

    pub default_execution_profile_handle: ExecutionProfileHandle,

    pub used_keyspace: Option<String>,
    pub keyspace_case_sensitive: bool,

    /// Provide our Session with TLS
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,

    pub authenticator: Option<Arc<dyn AuthenticatorProvider>>,

    pub connect_timeout: Duration,

    /// Size of the per-node connection pool, i.e. how many connections the driver should keep to each node.
    /// The default is `PerShard(1)`, which is the recommended setting for Scylla clusters.
    pub connection_pool_size: PoolSize,

    /// If true, prevents the driver from connecting to the shard-aware port, even if the node supports it.
    /// Generally, this options is best left as default (false).
    pub disallow_shard_aware_port: bool,

    /// If empty, fetch all keyspaces
    pub keyspaces_to_fetch: Vec<String>,

    /// If true, full schema is fetched with every metadata refresh.
    pub fetch_schema_metadata: bool,

    /// Interval of sending keepalive requests.
    /// If `None`, keepalives are never sent, so `Self::keepalive_timeout` has no effect.
    pub keepalive_interval: Option<Duration>,

    /// Controls after what time of not receiving response to keepalives a connection is closed.
    /// If `None`, connections are never closed due to lack of response to a keepalive message.
    pub keepalive_timeout: Option<Duration>,

    /// How often the driver should ask if schema is in agreement.
    pub schema_agreement_interval: Duration,

    /// Controls the timeout for waiting for schema agreement.
    /// This works both for manual awaiting schema agreement and for
    /// automatic waiting after a schema-altering statement is sent.
    pub schema_agreement_timeout: Duration,

    /// Controls whether schema agreement is automatically awaited
    /// after sending a schema-altering statement.
    pub schema_agreement_automatic_waiting: bool,

    /// If true, full schema metadata is fetched after successfully reaching a schema agreement.
    /// It is true by default but can be disabled if successive schema-altering statements should be performed.
    pub refresh_metadata_on_auto_schema_agreement: bool,

    /// The address translator is used to translate addresses received from ScyllaDB nodes
    /// (either with cluster metadata or with an event) to addresses that can be used to
    /// actually connect to those nodes. This may be needed e.g. when there is NAT
    /// between the nodes and the driver.
    pub address_translator: Option<Arc<dyn AddressTranslator>>,

    /// The host filter decides whether any connections should be opened
    /// to the node or not. The driver will also avoid filtered out nodes when
    /// re-establishing the control connection.
    pub host_filter: Option<Arc<dyn HostFilter>>,

    /// If the driver is to connect to ScyllaCloud, there is a config for it.
    #[cfg(feature = "cloud")]
    pub cloud_config: Option<Arc<CloudConfig>>,

    /// If true, the driver will inject a small delay before flushing data
    /// to the socket - by rescheduling the task that writes data to the socket.
    /// This gives the task an opportunity to collect more write requests
    /// and write them in a single syscall, increasing the efficiency.
    ///
    /// However, this optimization may worsen latency if the rate of requests
    /// issued by the application is low, but otherwise the application is
    /// heavily loaded with other tasks on the same tokio executor.
    /// Please do performance measurements before committing to disabling
    /// this option.
    pub enable_write_coalescing: bool,

    /// Number of attempts to fetch [`TracingInfo`]
    /// in [`Session::get_tracing_info`]. Tracing info
    /// might not be available immediately on queried node - that's why
    /// the driver performs a few attempts with sleeps in between.
    pub tracing_info_fetch_attempts: NonZeroU32,

    /// Delay between attempts to fetch [`TracingInfo`]
    /// in [`Session::get_tracing_info`]. Tracing info
    /// might not be available immediately on queried node - that's why
    /// the driver performs a few attempts with sleeps in between.
    pub tracing_info_fetch_interval: Duration,

    /// Consistency level of fetching [`TracingInfo`]
    /// in [`Session::get_tracing_info`].
    pub tracing_info_fetch_consistency: Consistency,

    /// Interval between refreshing cluster metadata. This
    /// can be configured according to the traffic pattern
    /// for e.g: if they do not want unexpected traffic
    /// or they expect the topology to change frequently.
    pub cluster_metadata_refresh_interval: Duration,

    /// Driver and application self-identifying information,
    /// to be sent to server in STARTUP message.
    pub identity: SelfIdentity<'static>,
}

impl SessionConfig {
    /// Creates a [`SessionConfig`] with default configuration
    /// # Default configuration
    /// * Compression: None
    /// * Load balancing policy: Token-aware Round-robin
    ///
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// let config = SessionConfig::new();
    /// ```
    pub fn new() -> Self {
        SessionConfig {
            known_nodes: Vec::new(),
            compression: None,
            tcp_nodelay: true,
            tcp_keepalive_interval: None,
            schema_agreement_interval: Duration::from_millis(200),
            default_execution_profile_handle: ExecutionProfile::new_from_inner(Default::default())
                .into_handle(),
            used_keyspace: None,
            keyspace_case_sensitive: false,
            #[cfg(feature = "ssl")]
            ssl_context: None,
            authenticator: None,
            connect_timeout: Duration::from_secs(5),
            connection_pool_size: Default::default(),
            disallow_shard_aware_port: false,
            keyspaces_to_fetch: Vec::new(),
            fetch_schema_metadata: true,
            keepalive_interval: Some(Duration::from_secs(30)),
            keepalive_timeout: Some(Duration::from_secs(30)),
            schema_agreement_timeout: Duration::from_secs(60),
            schema_agreement_automatic_waiting: true,
            address_translator: None,
            host_filter: None,
            refresh_metadata_on_auto_schema_agreement: true,
            #[cfg(feature = "cloud")]
            cloud_config: None,
            enable_write_coalescing: true,
            tracing_info_fetch_attempts: NonZeroU32::new(10).unwrap(),
            tracing_info_fetch_interval: Duration::from_millis(3),
            tracing_info_fetch_consistency: Consistency::One,
            cluster_metadata_refresh_interval: Duration::from_secs(60),
            identity: SelfIdentity::default(),
        }
    }

    /// Adds a known database server with a hostname.
    /// If the port is not explicitly specified, 9042 is used as default
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// let mut config = SessionConfig::new();
    /// config.add_known_node("127.0.0.1");
    /// config.add_known_node("db1.example.com:9042");
    /// ```
    pub fn add_known_node(&mut self, hostname: impl AsRef<str>) {
        self.known_nodes
            .push(KnownNode::Hostname(hostname.as_ref().to_string()));
    }

    /// Adds a known database server with an IP address
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let mut config = SessionConfig::new();
    /// config.add_known_node_addr(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9042));
    /// ```
    pub fn add_known_node_addr(&mut self, node_addr: SocketAddr) {
        self.known_nodes.push(KnownNode::Address(node_addr));
    }

    /// Adds a list of known database server with hostnames.
    /// If the port is not explicitly specified, 9042 is used as default
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let mut config = SessionConfig::new();
    /// config.add_known_nodes(&["127.0.0.1:9042", "db1.example.com"]);
    /// ```
    pub fn add_known_nodes(&mut self, hostnames: impl IntoIterator<Item = impl AsRef<str>>) {
        for hostname in hostnames {
            self.add_known_node(hostname);
        }
    }

    /// Adds a list of known database servers with IP addresses
    /// # Example
    /// ```
    /// # use scylla::SessionConfig;
    /// # use std::net::{SocketAddr, IpAddr, Ipv4Addr};
    /// let addr1 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 3)), 9042);
    /// let addr2 = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(172, 17, 0, 4)), 9042);
    ///
    /// let mut config = SessionConfig::new();
    /// config.add_known_nodes_addr(&[addr1, addr2]);
    /// ```
    pub fn add_known_nodes_addr(
        &mut self,
        node_addrs: impl IntoIterator<Item = impl Borrow<SocketAddr>>,
    ) {
        for address in node_addrs {
            self.add_known_node_addr(*address.borrow());
        }
    }
}

/// Creates default [`SessionConfig`], same as [`SessionConfig::new`]
impl Default for SessionConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait used to implement `Vec<result::Row>::into_typed<RowT>`
// This is the only way to add custom method to Vec
pub trait IntoTypedRows {
    fn into_typed<RowT: FromRow>(self) -> TypedRowIter<RowT>;
}

// Adds method Vec<result::Row>::into_typed<RowT>(self)
// It transforms the Vec into iterator mapping to custom row type
impl IntoTypedRows for Vec<result::Row> {
    fn into_typed<RowT: FromRow>(self) -> TypedRowIter<RowT> {
        TypedRowIter {
            row_iter: self.into_iter(),
            phantom_data: Default::default(),
        }
    }
}

/// Iterator over rows parsed as the given type\
/// Returned by `rows.into_typed::<(...)>()`
pub struct TypedRowIter<RowT: FromRow> {
    row_iter: std::vec::IntoIter<result::Row>,
    phantom_data: std::marker::PhantomData<RowT>,
}

impl<RowT: FromRow> Iterator for TypedRowIter<RowT> {
    type Item = Result<RowT, FromRowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.row_iter.next().map(RowT::from_row)
    }
}

pub(crate) enum RunQueryResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    /// Establishes a CQL session with the database
    ///
    /// Usually it's easier to use [SessionBuilder](crate::transport::session_builder::SessionBuilder)
    /// instead of calling `Session::connect` directly, because it's more convenient.
    /// # Arguments
    /// * `config` - Connection configuration - known nodes, Compression, etc.
    ///   Must contain at least one known node.
    ///
    /// # Example
    /// ```rust
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use scylla::{Session, SessionConfig};
    /// use scylla::transport::KnownNode;
    ///
    /// let mut config = SessionConfig::new();
    /// config.known_nodes.push(KnownNode::Hostname("127.0.0.1:9042".to_string()));
    ///
    /// let session: Session = Session::connect(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(config: SessionConfig) -> Result<Session, NewSessionError> {
        let known_nodes = config.known_nodes;

        #[cfg(feature = "cloud")]
        let cloud_known_nodes: Option<Vec<InternalKnownNode>> =
            if let Some(ref cloud_config) = config.cloud_config {
                let cloud_servers = cloud_config
                    .get_datacenters()
                    .iter()
                    .map(|(dc_name, dc_data)| {
                        InternalKnownNode::CloudEndpoint(CloudEndpoint {
                            hostname: dc_data.get_server().to_owned(),
                            datacenter: dc_name.clone(),
                        })
                    })
                    .collect();
                Some(cloud_servers)
            } else {
                None
            };

        #[cfg(not(feature = "cloud"))]
        let cloud_known_nodes: Option<Vec<InternalKnownNode>> = None;

        let known_nodes = cloud_known_nodes
            .unwrap_or_else(|| known_nodes.into_iter().map(|node| node.into()).collect());

        // Ensure there is at least one known node
        if known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        let (tablet_sender, tablet_receiver) = tokio::sync::mpsc::channel(TABLET_CHANNEL_SIZE);

        let connection_config = ConnectionConfig {
            compression: config.compression,
            tcp_nodelay: config.tcp_nodelay,
            tcp_keepalive_interval: config.tcp_keepalive_interval,
            #[cfg(feature = "ssl")]
            ssl_config: config.ssl_context.map(SslConfig::new_with_global_context),
            authenticator: config.authenticator.clone(),
            connect_timeout: config.connect_timeout,
            event_sender: None,
            default_consistency: Default::default(),
            address_translator: config.address_translator,
            #[cfg(feature = "cloud")]
            cloud_config: config.cloud_config,
            enable_write_coalescing: config.enable_write_coalescing,
            keepalive_interval: config.keepalive_interval,
            keepalive_timeout: config.keepalive_timeout,
            tablet_sender: Some(tablet_sender),
            identity: config.identity,
        };

        let pool_config = PoolConfig {
            connection_config,
            pool_size: config.connection_pool_size,
            can_use_shard_aware_port: !config.disallow_shard_aware_port,
            keepalive_interval: config.keepalive_interval,
        };

        let cluster = Cluster::new(
            known_nodes,
            pool_config,
            config.keyspaces_to_fetch,
            config.fetch_schema_metadata,
            config.host_filter,
            config.cluster_metadata_refresh_interval,
            tablet_receiver,
        )
        .await?;

        let default_execution_profile_handle = config.default_execution_profile_handle;

        let session = Session {
            cluster,
            default_execution_profile_handle,
            schema_agreement_interval: config.schema_agreement_interval,
            metrics: Arc::new(Metrics::new()),
            schema_agreement_timeout: config.schema_agreement_timeout,
            schema_agreement_automatic_waiting: config.schema_agreement_automatic_waiting,
            refresh_metadata_on_auto_schema_agreement: config
                .refresh_metadata_on_auto_schema_agreement,
            keyspace_name: ArcSwapOption::default(), // will be set by use_keyspace
            tracing_info_fetch_attempts: config.tracing_info_fetch_attempts,
            tracing_info_fetch_interval: config.tracing_info_fetch_interval,
            tracing_info_fetch_consistency: config.tracing_info_fetch_consistency,
        };

        if let Some(keyspace_name) = config.used_keyspace {
            session
                .use_keyspace(keyspace_name, config.keyspace_case_sensitive)
                .await?;
        }

        Ok(session)
    }

    /// Sends a request to the database and receives a response.\
    /// Performs an unpaged query, i.e. all results are received in a single response.
    ///
    /// This is the easiest way to make a query, but performance is worse than that of prepared queries.
    ///
    /// It is discouraged to use this method with non-empty values argument (`is_empty()` method from `SerializeRow`
    /// trait returns false). In such case, query first needs to be prepared (on a single connection), so
    /// driver will perform 2 round trips instead of 1. Please use [`Session::execute_unpaged()`] instead.
    ///
    /// As all results come in one response (no paging is done!), the memory footprint and latency may be huge
    /// for statements returning rows (i.e. SELECTs)! Prefer this method for non-SELECTs, and for SELECTs
    /// it is best to use paged queries:
    /// - to receive multiple pages and transparently iterate through them, use [query_iter](Session::query_iter).
    /// - to manually receive multiple pages and iterate through them, use [query_single_page](Session::query_single_page).
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/simple.html) for more information
    /// # Arguments
    /// * `query` - statement to be executed, can be just a `&str` or the [Query] struct.
    /// * `values` - values bound to the query, the easiest way is to use a tuple of bound values.
    ///
    /// # Examples
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// // Insert an int and text into a table.
    /// session
    ///     .query_unpaged(
    ///         "INSERT INTO ks.tab (a, b) VALUES(?, ?)",
    ///         (2_i32, "some text")
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::IntoTypedRows;
    ///
    /// // Read rows containing an int and text.
    /// // Keep in mind that all results come in one response (no paging is done!),
    /// // so the memory footprint and latency may be huge!
    /// // To prevent that, use `Session::query_iter` or `Session::query_single_page`.
    /// let rows_opt = session
    /// .query_unpaged("SELECT a, b FROM ks.tab", &[])
    ///     .await?
    ///     .rows;
    ///
    /// if let Some(rows) = rows_opt {
    ///     for row in rows.into_typed::<(i32, String)>() {
    ///         // Parse row as int and text \
    ///         let (int_val, text_val): (i32, String) = row?;
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_unpaged(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<LegacyQueryResult, QueryError> {
        let query = query.into();
        let (result, paging_state_response) = self
            .query(&query, values, None, PagingState::start())
            .await?;
        if !paging_state_response.finished() {
            error!("Unpaged unprepared query returned a non-empty paging state! This is a driver-side or server-side bug.");
            return Err(ProtocolError::NonfinishedPagingState.into());
        }
        Ok(result)
    }

    /// Queries a single page from the database, optionally continuing from a saved point.
    ///
    /// It is discouraged to use this method with non-empty values argument (`is_empty()` method from `SerializeRow`
    /// trait returns false). In such case, query first needs to be prepared (on a single connection), so
    /// driver will perform 2 round trips instead of 1. Please use [`Session::execute_single_page()`] instead.
    ///
    /// # Arguments
    ///
    /// * `query` - statement to be executed
    /// * `values` - values bound to the query
    /// * `paging_state` - previously received paging state or [PagingState::start()]
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use std::ops::ControlFlow;
    /// use scylla::statement::PagingState;
    ///
    /// // Manual paging in a loop, unprepared statement.
    /// let mut paging_state = PagingState::start();
    /// loop {
    ///    let (res, paging_state_response) = session
    ///        .query_single_page("SELECT a, b, c FROM ks.tbl", &[], paging_state)
    ///        .await?;
    ///
    ///    // Do something with a single page of results.
    ///    for row in res.rows_typed::<(i32, String)>()? {
    ///        let (a, b) = row?;
    ///    }
    ///
    ///    match paging_state_response.into_paging_control_flow() {
    ///        ControlFlow::Break(()) => {
    ///            // No more pages to be fetched.
    ///            break;
    ///        }
    ///        ControlFlow::Continue(new_paging_state) => {
    ///            // Update paging state from the response, so that query
    ///            // will be resumed from where it ended the last time.
    ///            paging_state = new_paging_state;
    ///        }
    ///    }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_single_page(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(LegacyQueryResult, PagingStateResponse), QueryError> {
        let query = query.into();
        self.query(
            &query,
            values,
            Some(query.get_validated_page_size()),
            paging_state,
        )
        .await
    }

    /// Sends a request to the database.
    /// Optionally continues fetching results from a saved point.
    ///
    /// This is now an internal method only.
    ///
    /// Tl;dr: use [Session::query_unpaged], [Session::query_single_page] or [Session::query_iter] instead.
    ///
    /// The rationale is that we believe that paging is so important concept (and it has shown to be error-prone as well)
    /// that we need to require users to make a conscious decision to use paging or not. For that, we expose
    /// the aforementioned 3 methods clearly differing in naming and API, so that no unconscious choices about paging
    /// should be made.
    async fn query(
        &self,
        query: &Query,
        values: impl SerializeRow,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<(LegacyQueryResult, PagingStateResponse), QueryError> {
        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let statement_info = RoutingInfo {
            consistency: query
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: query
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            ..Default::default()
        };

        let span = RequestSpan::new_query(&query.contents);
        let span_ref = &span;
        let run_query_result = self
            .run_query(
                statement_info,
                &query.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = query
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    // Needed to avoid moving query and values into async move block
                    let query_ref = &query;
                    let values_ref = &values;
                    let paging_state_ref = &paging_state;
                    async move {
                        if values_ref.is_empty() {
                            span_ref.record_request_size(0);
                            connection
                                .query_raw_with_consistency(
                                    query_ref,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                                .map_err(Into::into)
                        } else {
                            let prepared = connection.prepare(query_ref).await?;
                            let serialized = prepared.serialize_values(values_ref)?;
                            span_ref.record_request_size(serialized.buffer_size());
                            connection
                                .execute_raw_with_consistency(
                                    &prepared,
                                    &serialized,
                                    consistency,
                                    serial_consistency,
                                    page_size,
                                    paging_state_ref.clone(),
                                )
                                .await
                                .and_then(QueryResponse::into_non_error_query_response)
                                .map_err(Into::into)
                        }
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&response).await?;

        let (result, paging_state) = response.into_query_result_and_paging_state()?;
        let result = result.into_legacy_result()?;
        Ok((result, paging_state))
    }

    async fn handle_set_keyspace_response(
        &self,
        response: &NonErrorQueryResponse,
    ) -> Result<(), QueryError> {
        if let Some(set_keyspace) = response.as_set_keyspace() {
            debug!(
                "Detected USE KEYSPACE query, setting session's keyspace to {}",
                set_keyspace.keyspace_name
            );
            self.use_keyspace(set_keyspace.keyspace_name.clone(), true)
                .await?;
        }

        Ok(())
    }

    async fn handle_auto_await_schema_agreement(
        &self,
        response: &NonErrorQueryResponse,
    ) -> Result<(), QueryError> {
        if self.schema_agreement_automatic_waiting {
            if response.as_schema_change().is_some() {
                self.await_schema_agreement().await?;
            }

            if self.refresh_metadata_on_auto_schema_agreement
                && response.as_schema_change().is_some()
            {
                self.refresh_metadata().await?;
            }
        }

        Ok(())
    }

    /// Run an unprepared query with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [Query] passed to the function
    ///
    /// It is discouraged to use this method with non-empty values argument (`is_empty()` method from `SerializeRow`
    /// trait returns false). In such case, query first needs to be prepared (on a single connection), so
    /// driver will initially perform 2 round trips instead of 1. Please use [`Session::execute_iter()`] instead.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information.
    ///
    /// # Arguments
    /// * `query` - statement to be executed, can be just a `&str` or the [Query] struct.
    /// * `values` - values bound to the query, the easiest way is to use a tuple of bound values.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::IntoTypedRows;
    /// use futures::stream::StreamExt;
    ///
    /// let mut rows_stream = session
    ///    .query_iter("SELECT a, b FROM ks.t", &[])
    ///    .await?
    ///    .into_typed::<(i32, i32)>();
    ///
    /// while let Some(next_row_res) = rows_stream.next().await {
    ///     let (a, b): (i32, i32) = next_row_res?;
    ///     println!("a, b: {}, {}", a, b);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_iter(
        &self,
        query: impl Into<Query>,
        values: impl SerializeRow,
    ) -> Result<LegacyRowIterator, QueryError> {
        let query: Query = query.into();

        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        if values.is_empty() {
            RawIterator::new_for_query(
                query,
                execution_profile,
                self.cluster.get_data(),
                self.metrics.clone(),
            )
            .await
            .map(RawIterator::into_legacy)
        } else {
            // Making RawIterator::new_for_query work with values is too hard (if even possible)
            // so instead of sending one prepare to a specific connection on each iterator query,
            // we fully prepare a statement beforehand.
            let prepared = self.prepare(query).await?;
            let values = prepared.serialize_values(&values)?;
            RawIterator::new_for_prepared_statement(PreparedIteratorConfig {
                prepared,
                values,
                execution_profile,
                cluster_data: self.cluster.get_data(),
                metrics: self.metrics.clone(),
            })
            .await
            .map(RawIterator::into_legacy)
        }
    }

    /// Prepares a statement on the server side and returns a prepared statement,
    /// which can later be used to perform more efficient queries
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html#performance))
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information.
    /// See the documentation of [`PreparedStatement`].
    ///
    /// # Arguments
    /// * `query` - query to prepare, can be just a `&str` or the [Query] struct.
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Run the prepared query with some values, just like a simple query
    /// let to_insert: i32 = 12345;
    /// session.execute_unpaged(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare(&self, query: impl Into<Query>) -> Result<PreparedStatement, QueryError> {
        let query = query.into();
        let query_ref = &query;

        let cluster_data = self.get_cluster_data();
        let connections_iter = cluster_data.iter_working_connections()?;

        // Prepare statements on all connections concurrently
        let handles = connections_iter.map(|c| async move { c.prepare(query_ref).await });
        let mut results = join_all(handles).await.into_iter();

        // If at least one prepare was successful, `prepare()` returns Ok.
        // Find the first result that is Ok, or Err if all failed.

        // Safety: there is at least one node in the cluster, and `Cluster::iter_working_connections()`
        // returns either an error or an iterator with at least one connection, so there will be at least one result.
        let first_ok: Result<PreparedStatement, UserRequestError> =
            results.by_ref().find_or_first(Result::is_ok).unwrap();
        let mut prepared: PreparedStatement = first_ok?;

        // Validate prepared ids equality
        for statement in results.flatten() {
            if prepared.get_id() != statement.get_id() {
                return Err(ProtocolError::PreparedStatementIdsMismatch.into());
            }

            // Collect all tracing ids from prepare() queries in the final result
            prepared
                .prepare_tracing_ids
                .extend(statement.prepare_tracing_ids);
        }

        prepared.set_partitioner_name(
            self.extract_partitioner_name(&prepared, &self.cluster.get_data())
                .and_then(PartitionerName::from_str)
                .unwrap_or_default(),
        );

        Ok(prepared)
    }

    fn extract_partitioner_name<'a>(
        &self,
        prepared: &PreparedStatement,
        cluster_data: &'a ClusterData,
    ) -> Option<&'a str> {
        let table_spec = prepared.get_table_spec()?;
        cluster_data
            .keyspaces
            .get(table_spec.ks_name())?
            .tables
            .get(table_spec.table_name())?
            .partitioner
            .as_deref()
    }

    /// Execute a prepared statement. Requires a [PreparedStatement]
    /// generated using [`Session::prepare`](Session::prepare).\
    /// Performs an unpaged query, i.e. all results are received in a single response.
    ///
    /// As all results come in one response (no paging is done!), the memory footprint and latency may be huge
    /// for statements returning rows (i.e. SELECTs)! Prefer this method for non-SELECTs, and for SELECTs
    /// it is best to use paged queries:
    /// - to receive multiple pages and transparently iterate through them, use [execute_iter](Session::execute_iter).
    /// - to manually receive multiple pages and iterate through them, use [execute_single_page](Session::execute_single_page).
    ///
    /// Prepared queries are much faster than simple queries:
    /// * Database doesn't need to parse the query
    /// * They are properly load balanced using token aware routing
    ///
    /// > ***Warning***\
    /// > For token/shard aware load balancing to work properly, all partition key values
    /// > must be sent as bound values
    /// > (see [performance section](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html#performance)).
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information.
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, the easiest way is to use a tuple of bound values
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    ///     .await?;
    ///
    /// // Run the prepared query with some values, just like a simple query.
    /// let to_insert: i32 = 12345;
    /// session.execute_unpaged(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_unpaged(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
    ) -> Result<LegacyQueryResult, QueryError> {
        let serialized_values = prepared.serialize_values(&values)?;
        let (result, paging_state) = self
            .execute(prepared, &serialized_values, None, PagingState::start())
            .await?;
        if !paging_state.finished() {
            error!("Unpaged prepared query returned a non-empty paging state! This is a driver-side or server-side bug.");
            return Err(ProtocolError::NonfinishedPagingState.into());
        }
        Ok(result)
    }

    /// Executes a prepared statement, restricting results to single page.
    /// Optionally continues fetching results from a saved point.
    ///
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::Session::prepare)
    /// * `values` - values bound to the query
    /// * `paging_state` - continuation based on a paging state received from a previous paged query or None
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use std::ops::ControlFlow;
    /// use scylla::query::Query;
    /// use scylla::statement::{PagingState, PagingStateResponse};
    ///
    /// let paged_prepared = session
    ///     .prepare(
    ///         Query::new("SELECT a, b FROM ks.tbl")
    ///             .with_page_size(100.try_into().unwrap()),
    ///     )
    ///     .await?;
    ///
    /// // Manual paging in a loop, prepared statement.
    /// let mut paging_state = PagingState::start();
    /// loop {
    ///     let (res, paging_state_response) = session
    ///         .execute_single_page(&paged_prepared, &[], paging_state)
    ///         .await?;
    ///
    ///    // Do something with a single page of results.
    ///    for row in res.rows_typed::<(i32, String)>()? {
    ///        let (a, b) = row?;
    ///    }
    ///
    ///     match paging_state_response.into_paging_control_flow() {
    ///         ControlFlow::Break(()) => {
    ///             // No more pages to be fetched.
    ///             break;
    ///         }
    ///         ControlFlow::Continue(new_paging_state) => {
    ///             // Update paging continuation from the paging state, so that query
    ///             // will be resumed from where it ended the last time.
    ///             paging_state = new_paging_state;
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_single_page(
        &self,
        prepared: &PreparedStatement,
        values: impl SerializeRow,
        paging_state: PagingState,
    ) -> Result<(LegacyQueryResult, PagingStateResponse), QueryError> {
        let serialized_values = prepared.serialize_values(&values)?;
        let page_size = prepared.get_validated_page_size();
        self.execute(prepared, &serialized_values, Some(page_size), paging_state)
            .await
    }

    /// Sends a prepared request to the database, optionally continuing from a saved point.
    ///
    /// This is now an internal method only.
    ///
    /// Tl;dr: use [Session::execute_unpaged], [Session::execute_single_page] or [Session::execute_iter] instead.
    ///
    /// The rationale is that we believe that paging is so important concept (and it has shown to be error-prone as well)
    /// that we need to require users to make a conscious decision to use paging or not. For that, we expose
    /// the aforementioned 3 methods clearly differing in naming and API, so that no unconscious choices about paging
    /// should be made.
    async fn execute(
        &self,
        prepared: &PreparedStatement,
        serialized_values: &SerializedValues,
        page_size: Option<PageSize>,
        paging_state: PagingState,
    ) -> Result<(LegacyQueryResult, PagingStateResponse), QueryError> {
        let values_ref = &serialized_values;
        let paging_state_ref = &paging_state;

        let (partition_key, token) = prepared
            .extract_partition_key_and_calculate_token(prepared.get_partitioner_name(), values_ref)?
            .unzip();

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let table_spec = prepared.get_table_spec();

        let statement_info = RoutingInfo {
            consistency: prepared
                .config
                .consistency
                .unwrap_or(execution_profile.consistency),
            serial_consistency: prepared
                .config
                .serial_consistency
                .unwrap_or(execution_profile.serial_consistency),
            token,
            table: table_spec,
            is_confirmed_lwt: prepared.is_confirmed_lwt(),
        };

        let span = RequestSpan::new_prepared(
            partition_key.as_ref().map(|pk| pk.iter()),
            token,
            serialized_values.buffer_size(),
        );

        if !span.span().is_disabled() {
            if let (Some(table_spec), Some(token)) = (statement_info.table, token) {
                let cluster_data = self.get_cluster_data();
                let replicas: smallvec::SmallVec<[_; 8]> = cluster_data
                    .get_token_endpoints_iter(table_spec, token)
                    .collect();
                span.record_replicas(&replicas)
            }
        }

        let run_query_result: RunQueryResult<NonErrorQueryResponse> = self
            .run_query(
                statement_info,
                &prepared.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = prepared
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .execute_raw_with_consistency(
                                prepared,
                                values_ref,
                                consistency,
                                serial_consistency,
                                page_size,
                                paging_state_ref.clone(),
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                            .map_err(Into::into)
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&response).await?;

        let (result, paging_state) = response.into_query_result_and_paging_state()?;
        let result = result.into_legacy_result()?;
        Ok((result, paging_state))
    }

    /// Run a prepared query with paging.\
    /// This method will query all pages of the result.\
    ///
    /// Returns an async iterator (stream) over all received rows.\
    /// Page size can be specified in the [PreparedStatement] passed to the function.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information.
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, the easiest way is to use a tuple of bound values
    ///
    /// # Example
    ///
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::prepared_statement::PreparedStatement;
    /// use scylla::IntoTypedRows;
    /// use futures::stream::StreamExt;
    ///
    /// // Prepare the query for later execution
    /// let prepared: PreparedStatement = session
    ///     .prepare("SELECT a, b FROM ks.t")
    ///     .await?;
    ///
    /// // Execute the query and receive all pages
    /// let mut rows_stream = session
    ///    .execute_iter(prepared, &[])
    ///    .await?
    ///    .into_typed::<(i32, i32)>();
    ///
    /// while let Some(next_row_res) = rows_stream.next().await {
    ///     let (a, b): (i32, i32) = next_row_res?;
    ///     println!("a, b: {}, {}", a, b);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_iter(
        &self,
        prepared: impl Into<PreparedStatement>,
        values: impl SerializeRow,
    ) -> Result<LegacyRowIterator, QueryError> {
        let prepared = prepared.into();
        let serialized_values = prepared.serialize_values(&values)?;

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        RawIterator::new_for_prepared_statement(PreparedIteratorConfig {
            prepared,
            values: serialized_values,
            execution_profile,
            cluster_data: self.cluster.get_data(),
            metrics: self.metrics.clone(),
        })
        .await
        .map(RawIterator::into_legacy)
    }

    /// Perform a batch request.\
    /// Batch contains many `simple` or `prepared` queries which are executed at once.\
    /// Batch doesn't return any rows.
    ///
    /// Batch values must contain values for each of the queries.
    ///
    /// Avoid using non-empty values (`SerializeRow::is_empty()` return false) for unprepared statements
    /// inside the batch. Such statements will first need to be prepared, so the driver will need to
    /// send (numer_of_unprepared_statements_with_values + 1) requests instead of 1 request, severly
    /// affecting performance.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/batch.html) for more information.
    ///
    /// # Arguments
    /// * `batch` - [Batch] to be performed
    /// * `values` - List of values for each query, it's the easiest to use a tuple of tuples
    ///
    /// # Example
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::batch::Batch;
    ///
    /// let mut batch: Batch = Default::default();
    ///
    /// // A query with two bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(?, ?)");
    ///
    /// // A query with one bound value
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(3, ?)");
    ///
    /// // A query with no bound values
    /// batch.append_statement("INSERT INTO ks.tab(a, b) VALUES(5, 6)");
    ///
    /// // Batch values is a tuple of 3 tuples containing values for each query
    /// let batch_values = ((1_i32, 2_i32), // Tuple with two values for the first query
    ///                     (4_i32,),       // Tuple with one value for the second query
    ///                     ());            // Empty tuple/unit for the third query
    ///
    /// // Run the batch
    /// session.batch(&batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch(
        &self,
        batch: &Batch,
        values: impl BatchValues,
    ) -> Result<LegacyQueryResult, QueryError> {
        // Shard-awareness behavior for batch will be to pick shard based on first batch statement's shard
        // If users batch statements by shard, they will be rewarded with full shard awareness

        // check to ensure that we don't send a batch statement with more than u16::MAX queries
        let batch_statements_length = batch.statements.len();
        if batch_statements_length > u16::MAX as usize {
            return Err(QueryError::BadQuery(
                BadQuery::TooManyQueriesInBatchStatement(batch_statements_length),
            ));
        }

        let execution_profile = batch
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let consistency = batch
            .config
            .consistency
            .unwrap_or(execution_profile.consistency);

        let serial_consistency = batch
            .config
            .serial_consistency
            .unwrap_or(execution_profile.serial_consistency);

        let (first_value_token, values) =
            batch_values::peek_first_token(values, batch.statements.first())?;
        let values_ref = &values;

        let table_spec =
            if let Some(BatchStatement::PreparedStatement(ps)) = batch.statements.first() {
                ps.get_table_spec()
            } else {
                None
            };

        let statement_info = RoutingInfo {
            consistency,
            serial_consistency,
            token: first_value_token,
            table: table_spec,
            is_confirmed_lwt: false,
        };

        let span = RequestSpan::new_batch();

        let run_query_result = self
            .run_query(
                statement_info,
                &batch.config,
                execution_profile,
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = batch
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .batch_with_consistency(
                                batch,
                                values_ref,
                                consistency,
                                serial_consistency,
                            )
                            .await
                    }
                },
                &span,
            )
            .instrument(span.span().clone())
            .await?;

        let result = match run_query_result {
            RunQueryResult::IgnoredWriteError => LegacyQueryResult::mock_empty(),
            RunQueryResult::Completed(response) => response.into_legacy_result()?,
        };
        Ok(result)
    }

    /// Prepares all statements within the batch and returns a new batch where every
    /// statement is prepared.
    /// /// # Example
    /// ```rust
    /// # extern crate scylla;
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// use scylla::batch::Batch;
    ///
    /// // Create a batch statement with unprepared statements
    /// let mut batch: Batch = Default::default();
    /// batch.append_statement("INSERT INTO ks.simple_unprepared1 VALUES(?, ?)");
    /// batch.append_statement("INSERT INTO ks.simple_unprepared2 VALUES(?, ?)");
    ///
    /// // Prepare all statements in the batch at once
    /// let prepared_batch: Batch = session.prepare_batch(&batch).await?;
    ///
    /// // Specify bound values to use with each query
    /// let batch_values = ((1_i32, 2_i32),
    ///                     (3_i32, 4_i32));
    ///
    /// // Run the prepared batch
    /// session.batch(&prepared_batch, batch_values).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare_batch(&self, batch: &Batch) -> Result<Batch, QueryError> {
        let mut prepared_batch = batch.clone();

        try_join_all(
            prepared_batch
                .statements
                .iter_mut()
                .map(|statement| async move {
                    if let BatchStatement::Query(query) = statement {
                        let prepared = self.prepare(query.clone()).await?;
                        *statement = BatchStatement::PreparedStatement(prepared);
                    }
                    Ok::<(), QueryError>(())
                }),
        )
        .await?;

        Ok(prepared_batch)
    }

    /// Sends `USE <keyspace_name>` request on all connections\
    /// This allows to write `SELECT * FROM table` instead of `SELECT * FROM keyspace.table`\
    ///
    /// Note that even failed `use_keyspace` can change currently used keyspace - the request is sent on all connections and
    /// can overwrite previously used keyspace.
    ///
    /// Call only one `use_keyspace` at a time.\
    /// Trying to do two `use_keyspace` requests simultaneously with different names
    /// can end with some connections using one keyspace and the rest using the other.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/usekeyspace.html) for more information
    ///
    /// # Arguments
    ///
    /// * `keyspace_name` - keyspace name to use,
    ///   keyspace names can have up to 48 alphanumeric characters and contain underscores
    /// * `case_sensitive` - if set to true the generated query will put keyspace name in quotes
    /// # Example
    /// ```rust
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;
    /// session
    ///     .query_unpaged("INSERT INTO my_keyspace.tab (a) VALUES ('test1')", &[])
    ///     .await?;
    ///
    /// session.use_keyspace("my_keyspace", false).await?;
    ///
    /// // Now we can omit keyspace name in the query
    /// session
    ///     .query_unpaged("INSERT INTO tab (a) VALUES ('test2')", &[])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn use_keyspace(
        &self,
        keyspace_name: impl Into<String>,
        case_sensitive: bool,
    ) -> Result<(), QueryError> {
        let keyspace_name = keyspace_name.into();
        self.keyspace_name
            .store(Some(Arc::new(keyspace_name.clone())));

        // Trying to pass keyspace as bound value in "USE ?" doesn't work
        // So we have to create a string for query: "USE " + new_keyspace
        // To avoid any possible CQL injections it's good to verify that the name is valid
        let verified_ks_name = VerifiedKeyspaceName::new(keyspace_name, case_sensitive)?;

        self.cluster.use_keyspace(verified_ks_name).await?;

        Ok(())
    }

    /// Manually trigger a metadata refresh\
    /// The driver will fetch current nodes in the cluster and update its metadata
    ///
    /// Normally this is not needed,
    /// the driver should automatically detect all metadata changes in the cluster
    pub async fn refresh_metadata(&self) -> Result<(), QueryError> {
        self.cluster.refresh_metadata().await
    }

    /// Access metrics collected by the driver\
    /// Driver collects various metrics like number of queries or query latencies.
    /// They can be read using this method
    pub fn get_metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Access cluster data collected by the driver\
    /// Driver collects various information about network topology or schema.
    /// They can be read using this method
    pub fn get_cluster_data(&self) -> Arc<ClusterData> {
        self.cluster.get_data()
    }

    /// Get [`TracingInfo`] of a traced query performed earlier
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/tracing/tracing.html)
    /// for more information about query tracing
    pub async fn get_tracing_info(&self, tracing_id: &Uuid) -> Result<TracingInfo, QueryError> {
        // tracing_info_fetch_attempts is NonZeroU32 so at least one attempt will be made
        for _ in 0..self.tracing_info_fetch_attempts.get() {
            let current_try: Option<TracingInfo> = self
                .try_getting_tracing_info(tracing_id, Some(self.tracing_info_fetch_consistency))
                .await?;

            match current_try {
                Some(tracing_info) => return Ok(tracing_info),
                None => tokio::time::sleep(self.tracing_info_fetch_interval).await,
            };
        }

        Err(ProtocolError::Tracing(TracingProtocolError::EmptyResults).into())
    }

    /// Gets the name of the keyspace that is currently set, or `None` if no
    /// keyspace was set.
    ///
    /// It will initially return the name of the keyspace that was set
    /// in the session configuration, but calling `use_keyspace` will update
    /// it.
    ///
    /// Note: the return value might be wrong if `use_keyspace` was called
    /// concurrently or it previously failed. It is also unspecified
    /// if `get_keyspace` is called concurrently with `use_keyspace`.
    #[inline]
    pub fn get_keyspace(&self) -> Option<Arc<String>> {
        self.keyspace_name.load_full()
    }

    // Tries getting the tracing info
    // If the queries return 0 rows then returns None - the information didn't reach this node yet
    // If there is some other error returns this error
    async fn try_getting_tracing_info(
        &self,
        tracing_id: &Uuid,
        consistency: Option<Consistency>,
    ) -> Result<Option<TracingInfo>, QueryError> {
        // Query system_traces.sessions for TracingInfo
        let mut traces_session_query = Query::new(crate::tracing::TRACES_SESSION_QUERY_STR);
        traces_session_query.config.consistency = consistency;
        traces_session_query.set_page_size(TRACING_QUERY_PAGE_SIZE);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query = Query::new(crate::tracing::TRACES_EVENTS_QUERY_STR);
        traces_events_query.config.consistency = consistency;
        traces_events_query.set_page_size(TRACING_QUERY_PAGE_SIZE);

        let (traces_session_res, traces_events_res) = tokio::try_join!(
            self.query_unpaged(traces_session_query, (tracing_id,)),
            self.query_unpaged(traces_events_query, (tracing_id,))
        )?;

        // Get tracing info
        let maybe_tracing_info: Option<TracingInfo> = traces_session_res
            .maybe_first_row_typed()
            .map_err(|err| match err {
                MaybeFirstRowTypedError::RowsExpected(e) => {
                    ProtocolError::Tracing(TracingProtocolError::TracesSessionNotRows(e))
                }
                MaybeFirstRowTypedError::FromRowError(e) => {
                    ProtocolError::Tracing(TracingProtocolError::TracesSessionInvalidColumnType(e))
                }
            })?;

        let mut tracing_info = match maybe_tracing_info {
            None => return Ok(None),
            Some(tracing_info) => tracing_info,
        };

        // Get tracing events
        let tracing_event_rows = traces_events_res.rows_typed().map_err(|err| {
            ProtocolError::Tracing(TracingProtocolError::TracesEventsNotRows(err))
        })?;

        for event in tracing_event_rows {
            let tracing_event: TracingEvent = event.map_err(|err| {
                ProtocolError::Tracing(TracingProtocolError::TracesEventsInvalidColumnType(err))
            })?;

            tracing_info.events.push(tracing_event);
        }

        if tracing_info.events.is_empty() {
            return Ok(None);
        }

        Ok(Some(tracing_info))
    }

    // This method allows to easily run a query using load balancing, retry policy etc.
    // Requires some information about the query and a closure.
    // The closure is used to do the query itself on a connection.
    // - query will use connection.query()
    // - execute will use connection.execute()
    // If this query closure fails with some errors retry policy is used to perform retries
    // On success this query's result is returned
    // I tried to make this closures take a reference instead of an Arc but failed
    // maybe once async closures get stabilized this can be fixed
    async fn run_query<'a, QueryFut, ResT>(
        &'a self,
        statement_info: RoutingInfo<'a>,
        statement_config: &'a StatementConfig,
        execution_profile: Arc<ExecutionProfileInner>,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        request_span: &'a RequestSpan,
    ) -> Result<RunQueryResult<ResT>, QueryError>
    where
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let history_listener_and_id: Option<(&'a dyn HistoryListener, history::QueryId)> =
            statement_config
                .history_listener
                .as_ref()
                .map(|hl| (&**hl, hl.log_query_start()));

        let load_balancer = &execution_profile.load_balancing_policy;

        let runner = async {
            let cluster_data = self.cluster.get_data();
            let query_plan =
                load_balancing::Plan::new(load_balancer.as_ref(), &statement_info, &cluster_data);

            // If a speculative execution policy is used to run query, query_plan has to be shared
            // between different async functions. This struct helps to wrap query_plan in mutex so it
            // can be shared safely.
            struct SharedPlan<'a, I>
            where
                I: Iterator<Item = (NodeRef<'a>, Shard)>,
            {
                iter: std::sync::Mutex<I>,
            }

            impl<'a, I> Iterator for &SharedPlan<'a, I>
            where
                I: Iterator<Item = (NodeRef<'a>, Shard)>,
            {
                type Item = (NodeRef<'a>, Shard);

                fn next(&mut self) -> Option<Self::Item> {
                    self.iter.lock().unwrap().next()
                }
            }

            let retry_policy = statement_config
                .retry_policy
                .as_deref()
                .unwrap_or(&*execution_profile.retry_policy);

            let speculative_policy = execution_profile.speculative_execution_policy.as_ref();

            match speculative_policy {
                Some(speculative) if statement_config.is_idempotent => {
                    let shared_query_plan = SharedPlan {
                        iter: std::sync::Mutex::new(query_plan),
                    };

                    let execute_query_generator = |is_speculative: bool| {
                        let history_data: Option<HistoryData> = history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, query_id)| {
                                let speculative_id: Option<history::SpeculativeId> =
                                    if is_speculative {
                                        Some(history_listener.log_new_speculative_fiber(*query_id))
                                    } else {
                                        None
                                    };
                                HistoryData {
                                    listener: *history_listener,
                                    query_id: *query_id,
                                    speculative_id,
                                }
                            });

                        if is_speculative {
                            request_span.inc_speculative_executions();
                        }

                        self.execute_query(
                            &shared_query_plan,
                            &do_query,
                            &execution_profile,
                            ExecuteQueryContext {
                                is_idempotent: statement_config.is_idempotent,
                                consistency_set_on_statement: statement_config.consistency,
                                retry_session: retry_policy.new_session(),
                                history_data,
                                query_info: &statement_info,
                                request_span,
                            },
                        )
                    };

                    let context = speculative_execution::Context {
                        metrics: self.metrics.clone(),
                    };

                    speculative_execution::execute(
                        speculative.as_ref(),
                        &context,
                        execute_query_generator,
                    )
                    .await
                }
                _ => {
                    let history_data: Option<HistoryData> =
                        history_listener_and_id
                            .as_ref()
                            .map(|(history_listener, query_id)| HistoryData {
                                listener: *history_listener,
                                query_id: *query_id,
                                speculative_id: None,
                            });
                    self.execute_query(
                        query_plan,
                        &do_query,
                        &execution_profile,
                        ExecuteQueryContext {
                            is_idempotent: statement_config.is_idempotent,
                            consistency_set_on_statement: statement_config.consistency,
                            retry_session: retry_policy.new_session(),
                            history_data,
                            query_info: &statement_info,
                            request_span,
                        },
                    )
                    .await
                    .unwrap_or(Err(QueryError::EmptyPlan))
                }
            }
        };

        let effective_timeout = statement_config
            .request_timeout
            .or(execution_profile.request_timeout);
        let result = match effective_timeout {
            Some(timeout) => tokio::time::timeout(timeout, runner)
                .await
                .unwrap_or_else(|e| {
                    Err(QueryError::RequestTimeout(format!(
                        "Request took longer than {}ms: {}",
                        timeout.as_millis(),
                        e
                    )))
                }),
            None => runner.await,
        };

        if let Some((history_listener, query_id)) = history_listener_and_id {
            match &result {
                Ok(_) => history_listener.log_query_success(query_id),
                Err(e) => history_listener.log_query_error(query_id, e),
            }
        }

        result
    }

    async fn execute_query<'a, QueryFut, ResT>(
        &'a self,
        query_plan: impl Iterator<Item = (NodeRef<'a>, Shard)>,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        execution_profile: &ExecutionProfileInner,
        mut context: ExecuteQueryContext<'a>,
    ) -> Option<Result<RunQueryResult<ResT>, QueryError>>
    where
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let mut last_error: Option<QueryError> = None;
        let mut current_consistency: Consistency = context
            .consistency_set_on_statement
            .unwrap_or(execution_profile.consistency);

        'nodes_in_plan: for (node, shard) in query_plan {
            let span = trace_span!("Executing query", node = %node.address);
            'same_node_retries: loop {
                trace!(parent: &span, "Execution started");
                let connection = match node.connection_for_shard(shard).await {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: &span,
                            error = %e,
                            "Choosing connection failed"
                        );
                        last_error = Some(e.into());
                        // Broken connection doesn't count as a failed query, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };
                context.request_span.record_shard_id(&connection);

                self.metrics.inc_total_nonpaged_queries();
                let query_start = std::time::Instant::now();

                trace!(
                    parent: &span,
                    connection = %connection.get_connect_address(),
                    "Sending"
                );
                let attempt_id: Option<history::AttemptId> =
                    context.log_attempt_start(connection.get_connect_address());
                let query_result: Result<ResT, QueryError> =
                    do_query(connection, current_consistency, execution_profile)
                        .instrument(span.clone())
                        .await;

                let elapsed = query_start.elapsed();
                last_error = match query_result {
                    Ok(response) => {
                        trace!(parent: &span, "Query succeeded");
                        let _ = self.metrics.log_query_latency(elapsed.as_millis() as u64);
                        context.log_attempt_success(&attempt_id);
                        execution_profile.load_balancing_policy.on_query_success(
                            context.query_info,
                            elapsed,
                            node,
                        );
                        return Some(Ok(RunQueryResult::Completed(response)));
                    }
                    Err(e) => {
                        trace!(
                            parent: &span,
                            last_error = %e,
                            "Query failed"
                        );
                        self.metrics.inc_failed_nonpaged_queries();
                        execution_profile.load_balancing_policy.on_query_failure(
                            context.query_info,
                            elapsed,
                            node,
                            &e,
                        );
                        Some(e)
                    }
                };

                let the_error: &QueryError = last_error.as_ref().unwrap();
                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: the_error,
                    is_idempotent: context.is_idempotent,
                    consistency: context
                        .consistency_set_on_statement
                        .unwrap_or(execution_profile.consistency),
                };

                let retry_decision = context.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: &span,
                    retry_decision = format!("{:?}", retry_decision).as_str()
                );
                context.log_attempt_error(&attempt_id, the_error, &retry_decision);
                match retry_decision {
                    RetryDecision::RetrySameNode(new_cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'same_node_retries;
                    }
                    RetryDecision::RetryNextNode(new_cl) => {
                        self.metrics.inc_retries_num();
                        current_consistency = new_cl.unwrap_or(current_consistency);
                        continue 'nodes_in_plan;
                    }
                    RetryDecision::DontRetry => break 'nodes_in_plan,

                    RetryDecision::IgnoreWriteError => {
                        return Some(Ok(RunQueryResult::IgnoredWriteError))
                    }
                };
            }
        }

        last_error.map(Result::Err)
    }

    async fn await_schema_agreement_indefinitely(&self) -> Result<Uuid, QueryError> {
        loop {
            tokio::time::sleep(self.schema_agreement_interval).await;
            if let Some(agreed_version) = self.check_schema_agreement().await? {
                return Ok(agreed_version);
            }
        }
    }

    pub async fn await_schema_agreement(&self) -> Result<Uuid, QueryError> {
        timeout(
            self.schema_agreement_timeout,
            self.await_schema_agreement_indefinitely(),
        )
        .await
        .unwrap_or(Err(QueryError::RequestTimeout(
            "schema agreement not reached in time".to_owned(),
        )))
    }

    pub async fn check_schema_agreement(&self) -> Result<Option<Uuid>, QueryError> {
        let cluster_data = self.get_cluster_data();
        let connections_iter = cluster_data.iter_working_connections()?;

        let handles = connections_iter.map(|c| async move { c.fetch_schema_version().await });
        let versions = try_join_all(handles).await?;

        let local_version: Uuid = versions[0];
        let in_agreement = versions.into_iter().all(|v| v == local_version);
        Ok(in_agreement.then_some(local_version))
    }

    /// Retrieves the handle to execution profile that is used by this session
    /// by default, i.e. when an executed statement does not define its own handle.
    pub fn get_default_execution_profile_handle(&self) -> &ExecutionProfileHandle {
        &self.default_execution_profile_handle
    }
}

// run_query, execute_query, etc have a template type called ResT.
// There was a bug where ResT was set to QueryResponse, which could
// be an error response. This was not caught by retry policy which
// assumed all errors would come from analyzing Result<ResT, QueryError>.
// This trait is a guard to make sure that this mistake doesn't
// happen again.
// When using run_query make sure that the ResT type is NOT able
// to contain any errors.
// See https://github.com/scylladb/scylla-rust-driver/issues/501
pub(crate) trait AllowedRunQueryResTType {}

impl AllowedRunQueryResTType for Uuid {}
impl AllowedRunQueryResTType for QueryResult {}
impl AllowedRunQueryResTType for NonErrorQueryResponse {}

struct ExecuteQueryContext<'a> {
    is_idempotent: bool,
    consistency_set_on_statement: Option<Consistency>,
    retry_session: Box<dyn RetrySession>,
    history_data: Option<HistoryData<'a>>,
    query_info: &'a load_balancing::RoutingInfo<'a>,
    request_span: &'a RequestSpan,
}

struct HistoryData<'a> {
    listener: &'a dyn HistoryListener,
    query_id: history::QueryId,
    speculative_id: Option<history::SpeculativeId>,
}

impl<'a> ExecuteQueryContext<'a> {
    fn log_attempt_start(&self, node_addr: SocketAddr) -> Option<history::AttemptId> {
        self.history_data.as_ref().map(|hd| {
            hd.listener
                .log_attempt_start(hd.query_id, hd.speculative_id, node_addr)
        })
    }

    fn log_attempt_success(&self, attempt_id_opt: &Option<history::AttemptId>) {
        let attempt_id: &history::AttemptId = match attempt_id_opt {
            Some(id) => id,
            None => return,
        };

        let history_data: &HistoryData = match &self.history_data {
            Some(data) => data,
            None => return,
        };

        history_data.listener.log_attempt_success(*attempt_id);
    }

    fn log_attempt_error(
        &self,
        attempt_id_opt: &Option<history::AttemptId>,
        error: &QueryError,
        retry_decision: &RetryDecision,
    ) {
        let attempt_id: &history::AttemptId = match attempt_id_opt {
            Some(id) => id,
            None => return,
        };

        let history_data: &HistoryData = match &self.history_data {
            Some(data) => data,
            None => return,
        };

        history_data
            .listener
            .log_attempt_error(*attempt_id, error, retry_decision);
    }
}

pub(crate) struct RequestSpan {
    span: tracing::Span,
    speculative_executions: AtomicUsize,
}

impl RequestSpan {
    pub(crate) fn new_query(contents: &str) -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "unprepared",
            contents = contents,
            //
            request_size = Empty,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn new_prepared<'ps, 'spec: 'ps>(
        partition_key: Option<impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec<'spec>)> + Clone>,
        token: Option<Token>,
        request_size: usize,
    ) -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "prepared",
            partition_key = Empty,
            token = Empty,
            //
            request_size = request_size,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        if let Some(partition_key) = partition_key {
            span.record(
                "partition_key",
                tracing::field::display(
                    format_args!("{}", partition_key_displayer(partition_key),),
                ),
            );
        }
        if let Some(token) = token {
            span.record("token", token.value());
        }

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn new_batch() -> Self {
        use tracing::field::Empty;

        let span = trace_span!(
            "Request",
            kind = "batch",
            //
            request_size = Empty,
            result_size = Empty,
            result_rows = Empty,
            replicas = Empty,
            shard = Empty,
            speculative_executions = Empty,
        );

        Self {
            span,
            speculative_executions: 0.into(),
        }
    }

    pub(crate) fn record_shard_id(&self, conn: &Connection) {
        if let Some(info) = conn.get_shard_info() {
            self.span.record("shard", info.shard);
        }
    }

    pub(crate) fn record_replicas<'a>(&'a self, replicas: &'a [(impl Borrow<Arc<Node>>, Shard)]) {
        struct ReplicaIps<'a, N>(&'a [(N, Shard)]);
        impl<'a, N> Display for ReplicaIps<'a, N>
        where
            N: Borrow<Arc<Node>>,
        {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut nodes_with_shards = self.0.iter();
                if let Some((node, shard)) = nodes_with_shards.next() {
                    write!(f, "{}-shard{}", node.borrow().address.ip(), shard)?;

                    for (node, shard) in nodes_with_shards {
                        write!(f, ",{}-shard{}", node.borrow().address.ip(), shard)?;
                    }
                }
                Ok(())
            }
        }
        self.span
            .record("replicas", tracing::field::display(&ReplicaIps(replicas)));
    }

    pub(crate) fn record_request_size(&self, size: usize) {
        self.span.record("request_size", size);
    }

    pub(crate) fn inc_speculative_executions(&self) {
        self.speculative_executions.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn span(&self) -> &tracing::Span {
        &self.span
    }
}

impl Drop for RequestSpan {
    fn drop(&mut self) {
        self.span.record(
            "speculative_executions",
            self.speculative_executions.load(Ordering::Relaxed),
        );
    }
}

fn partition_key_displayer<'ps, 'res, 'spec: 'ps>(
    mut pk_values_iter: impl Iterator<Item = (&'ps [u8], &'ps ColumnSpec<'spec>)> + 'res + Clone,
) -> impl Display + 'res {
    CommaSeparatedDisplayer(
        std::iter::from_fn(move || {
            pk_values_iter
                .next()
                .map(|(mut cell, spec)| deser_cql_value(spec.typ(), &mut cell))
        })
        .map(|c| match c {
            Ok(c) => Either::Left(CqlValueDisplayer(c)),
            Err(_) => Either::Right("<decoding error>"),
        }),
    )
}
