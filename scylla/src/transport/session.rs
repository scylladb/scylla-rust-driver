//! `Session` is the main object used in the driver.\
//! It manages all connections to the cluster and allows to perform queries.

#[cfg(feature = "cloud")]
use crate::cloud::CloudConfig;

use crate::frame::types::LegacyConsistency;
use crate::history;
use crate::history::HistoryListener;
use crate::scylladb_tag;
use crate::transport::session::spans::PendingAttemptSpan;
use crate::utils::RecordError;
use crate::utils::SpanExt;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::join_all;
use futures::future::try_join_all;
pub use scylla_cql::errors::TranslationError;
use scylla_cql::frame::response::NonErrorResponse;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::lookup_host;
use tokio::time::timeout;
use tracing::{debug, error, trace, Instrument};
use uuid::Uuid;

use self::spans::PreparationSpan;
use self::spans::{BatchSpan, PreparedSpan, QuerySpan};

use super::cluster::ContactPoint;
#[cfg(feature = "ssl")]
use super::connection::SslConfig;
use super::connection::{NonErrorQueryResponse, QueryResponse};
use super::errors::{BadQuery, NewSessionError, QueryError};
use super::execution_profile::{ExecutionProfile, ExecutionProfileHandle, ExecutionProfileInner};
use super::partitioner::PartitionerName;
use super::topology::UntranslatedPeer;
use super::NodeRef;
use crate::cql_to_rust::FromRow;
use crate::frame::response::cql_to_rust::FromRowError;
use crate::frame::response::result;
use crate::frame::value::{
    BatchValues, BatchValuesFirstSerialized, BatchValuesIterator, SerializedValues, ValueList,
};
use crate::prepared_statement::{PartitionKeyError, PreparedStatement};
use crate::query::Query;
use crate::routing::Token;
use crate::statement::{Consistency, SerialConsistency};
use crate::tracing::{GetTracingConfig, TracingEvent, TracingInfo};
use crate::transport::cluster::{Cluster, ClusterData, ClusterNeatDebug};
use crate::transport::connection::{Connection, ConnectionConfig, VerifiedKeyspaceName};
use crate::transport::connection_pool::PoolConfig;
use crate::transport::host_filter::HostFilter;
use crate::transport::iterator::{PreparedIteratorConfig, RowIterator};
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
            None => Err(TranslationError::NoRuleForAddress),
        }
    }
}

#[async_trait]
// Notice: this is unefficient, but what else can we do with such poor representation as str?
// After all, the cluster size is small enough to make this irrelevant.
impl AddressTranslator for HashMap<&'static str, &'static str> {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        for (&rule_addr_str, &translated_addr_str) in self.iter() {
            if let Ok(rule_addr) = SocketAddr::from_str(rule_addr_str) {
                if rule_addr == untranslated_peer.untranslated_address {
                    return SocketAddr::from_str(translated_addr_str)
                        .map_err(|_| TranslationError::InvalidAddressInRule);
                }
            }
        }
        Err(TranslationError::NoRuleForAddress)
    }
}

/// `Session` manages connections to the cluster and allows to perform queries
pub struct Session {
    cluster: Cluster,
    default_execution_profile_handle: ExecutionProfileHandle,
    schema_agreement_interval: Duration,
    metrics: Arc<Metrics>,
    auto_await_schema_agreement_timeout: Option<Duration>,
    refresh_metadata_on_auto_schema_agreement: bool,
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
                &self.auto_await_schema_agreement_timeout,
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

    pub default_execution_profile_handle: ExecutionProfileHandle,

    pub used_keyspace: Option<String>,
    pub keyspace_case_sensitive: bool,

    /// Provide our Session with TLS
    #[cfg(feature = "ssl")]
    pub ssl_context: Option<SslContext>,

    pub authenticator: Option<Arc<dyn AuthenticatorProvider>>,

    pub schema_agreement_interval: Duration,
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

    /// Interval of sending keepalive requests
    pub keepalive_interval: Option<Duration>,

    /// Controls the timeout for the automatic wait for schema agreement after sending a schema-altering statement.
    /// If `None`, the automatic schema agreement is disabled.
    pub auto_await_schema_agreement_timeout: Option<Duration>,

    pub address_translator: Option<Arc<dyn AddressTranslator>>,

    /// The host filter decides whether any connections should be opened
    /// to the node or not. The driver will also avoid filtered out nodes when
    /// re-establishing the control connection.
    pub host_filter: Option<Arc<dyn HostFilter>>,

    /// If true, full schema metadata is fetched after successfully reaching a schema agreement.
    /// It is true by default but can be disabled if successive schema-altering statements should be performed.
    pub refresh_metadata_on_auto_schema_agreement: bool,

    // If the driver is to connect to ScyllaCloud, there is a config for it.
    #[cfg(feature = "cloud")]
    pub(crate) cloud_config: Option<Arc<CloudConfig>>,
}

/// Describes database server known on Session startup.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[non_exhaustive]
pub enum KnownNode {
    Hostname(String),
    Address(SocketAddr),
    #[cfg(feature = "cloud")]
    CloudEndpoint(CloudEndpoint),
}

#[cfg(feature = "cloud")]
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct CloudEndpoint {
    pub hostname: String,
    pub datacenter: String,
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
            auto_await_schema_agreement_timeout: Some(std::time::Duration::from_secs(60)),
            address_translator: None,
            host_filter: None,
            refresh_metadata_on_auto_schema_agreement: true,
            #[cfg(feature = "cloud")]
            cloud_config: None,
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
    pub fn add_known_nodes(&mut self, hostnames: &[impl AsRef<str>]) {
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
    pub fn add_known_nodes_addr(&mut self, node_addrs: &[SocketAddr]) {
        for address in node_addrs {
            self.add_known_node_addr(*address);
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

pub enum RunQueryResult<ResT> {
    IgnoredWriteError,
    Completed(ResT),
}

mod spans {
    use scylla_cql::frame::value::SerializedValues;
    use std::{borrow::Cow, ops::Deref};

    use crate::{
        batch::Batch,
        otel_span,
        prepared_statement::PreparedStatement,
        query::Query,
        scylladb_tag,
        statement::StatementConfig,
        transport::{execution_profile::ExecutionProfileInner, Node},
        utils::SpanExt,
    };

    fn make_request_span(
        execution_profile: &ExecutionProfileInner,
        config: &StatementConfig,
    ) -> tracing::Span {
        use tracing::field::Empty;

        let span = otel_span!(
            "Request",
            db.scylladb.load_balancing_policy = Empty,
            db.scylladb.speculative_execution_policy = Empty,
            db.scylladb.consistency_level = Empty,
            db.scylladb.retry_policy = Empty,
            db.scylladb.statement_type = Empty,
            db.scylladb.statement = Empty,
            db.scylladb.page_size = Empty,
            db.scylladb.prepared_id = Empty,
            db.scylladb.keyspace = Empty,
            db.scylladb.bound_values = Empty,
            db.scylladb.partition_key = Empty,
            db.scylladb.table = Empty,
            db.scylladb.batch_size = Empty,
            db.scylladb.shard_id = Empty,
            db.scylladb.attempt_count = Empty,
            db.scylladb.rows_count = Empty,
        );

        span.record(
            scylladb_tag!("retry_policy"),
            execution_profile.retry_policy.name(),
        );

        span.record(
            scylladb_tag!("consistency_level"),
            config
                .determine_consistency(execution_profile.consistency)
                .name(),
        );

        span.record(
            scylladb_tag!("load_balancing_policy"),
            execution_profile.load_balancing_policy.name(),
        );

        span.record_none_explicitly(
            scylladb_tag!("speculative_execution_policy"),
            execution_profile
                .speculative_execution_policy
                .as_ref()
                .map(|policy| policy.name()),
        );

        span
    }

    pub(super) struct QuerySpan;
    impl QuerySpan {
        #[allow(clippy::new_ret_no_self)]
        pub(super) fn new(
            execution_profile: &ExecutionProfileInner,
            query: &Query,
            bound_values: &Cow<SerializedValues>,
        ) -> tracing::Span {
            let span = make_request_span(execution_profile, &query.config);
            span.record(scylladb_tag!("statement_type"), "regular");
            span.record(scylladb_tag!("statement"), query.contents.as_str());
            span.record_none_explicitly(scylladb_tag!("page_size"), query.get_page_size());
            span.record(scylladb_tag!("bound_values"), &bound_values.for_debug());
            span
        }
    }

    pub(super) struct PreparedSpan;
    impl PreparedSpan {
        #[allow(clippy::new_ret_no_self)]
        pub(super) fn new(
            execution_profile: &ExecutionProfileInner,
            prepared: &PreparedStatement,
            bound_values: &Cow<SerializedValues>,
        ) -> tracing::Span {
            let span = make_request_span(execution_profile, &prepared.config);
            span.record(scylladb_tag!("statement_type"), "prepared");
            span.record(scylladb_tag!("statement"), prepared.get_statement());
            span.record_none_explicitly(scylladb_tag!("page_size"), prepared.get_page_size());
            span.record(
                scylladb_tag!("prepared_id"),
                format_args!("{:X}", prepared.get_id()),
            );
            span.record_none_explicitly(scylladb_tag!("table"), prepared.get_table_name());
            span.record_none_explicitly(scylladb_tag!("keyspace"), prepared.get_keyspace_name());
            span.record(scylladb_tag!("bound_values"), &bound_values.for_debug());

            let metadata = prepared.get_prepared_metadata();

            let gen_bound_values_repr = |columns: &mut dyn Iterator<Item = usize>| {
                let mut s = String::new();
                for i in columns {
                    s.push_str(&metadata.col_specs[i].name);
                    s.push('=');
                    bound_values.for_debug_append_nth(&mut s, i).ok()?;
                    s.push_str(", ");
                }
                if metadata.col_count > 0 {
                    s.pop();
                    s.pop();
                }
                Some(s)
            };

            let bound_values_repr = gen_bound_values_repr(&mut (0..metadata.col_count))
                .unwrap_or_else(|| bound_values.for_debug());
            span.record(scylladb_tag!("bound_values"), &bound_values_repr);
            let partition_key_repr =
                gen_bound_values_repr(&mut metadata.pk_indexes.iter().map(|pk| pk.index as usize));
            span.record(scylladb_tag!("partition_key"), &partition_key_repr);
            span.record_none_explicitly(scylladb_tag!("page_size"), prepared_stmt.get_page_size());

            span
        }
    }

    pub(super) struct BatchSpan;
    impl BatchSpan {
        #[allow(clippy::new_ret_no_self)]
        pub(super) fn new(
            execution_profile: &ExecutionProfileInner,
            batch: &Batch,
        ) -> tracing::Span {
            let span = make_request_span(execution_profile, &batch.config);
            span.record(scylladb_tag!("statement_type"), "batch");
            span.record(scylladb_tag!("batch_size"), batch.statements.len());
            span
        }
    }

    pub(super) struct PendingAttemptSpan(tracing::Span);

    impl PendingAttemptSpan {
        pub(super) fn new(node: &Node, attempt_count: usize) -> Self {
            use tracing::field::Empty;

            Self(otel_span!(
                "Query attempt",
                net.peer.name = format_args!(
                    "datacenter: {}, rack: {}",
                    node.datacenter.as_deref().unwrap_or("<None>"),
                    node.rack.as_deref().unwrap_or("<None>")
                ),
                net.peer.port = node.address.port(),
                net.peer.address = display(node.address.ip()),
                db.scylladb.attempt_count = attempt_count,
                db.scylladb.shard_id = Empty,
                // this is the default, changes to `completed` when the query completes
                db.scylladb.completion_status = "interrupted",
            ))
        }

        pub(super) fn record_shard_id(&self, shard_id: Option<u16>) {
            self.0
                .record_none_explicitly(scylladb_tag!("shard_id"), shard_id);
        }

        pub(super) fn complete(&self) {
            self.0.record("completion_status", "completed");
        }
    }

    impl Deref for PendingAttemptSpan {
        type Target = tracing::Span;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl AsRef<tracing::Span> for PendingAttemptSpan {
        fn as_ref(&self) -> &tracing::Span {
            &self.0
        }
    }

    pub(super) struct PreparationSpan(tracing::Span);

    impl PreparationSpan {
        pub(super) fn new(query: &Query) -> Self {
            Self(otel_span!(
                "Preparation",
                db.scylladb.statement_type = "prepare_request",
                db.scylladb.statement = query.contents.as_str(),
                db.scylladb.prepared_id = tracing::field::Empty,
            ))
        }

        pub(super) fn record_prepared_id(&self, prepared: &PreparedStatement) {
            self.record(
                scylladb_tag!("prepared_id"),
                format_args!("{:X}", prepared.get_id()),
            );
        }
    }

    impl Deref for PreparationSpan {
        type Target = tracing::Span;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
}

/// Represents a CQL session, which can be used to communicate
/// with the database
impl Session {
    /// Estabilishes a CQL session with the database
    ///
    /// Usually it's easier to use [SessionBuilder](crate::transport::session_builder::SessionBuilder)
    /// instead of calling `Session::connect` directly, because it's more convenient.
    /// # Arguments
    /// * `config` - Connection configuration - known nodes, Compression, etc.
    /// Must contain at least one known node.
    ///
    /// # Example
    /// ```rust
    /// # use std::error::Error;
    /// # async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
    /// use scylla::{Session, SessionConfig};
    /// use scylla::transport::session::KnownNode;
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
        let known_nodes = if let Some(cloud_servers) =
            config.cloud_config.as_ref().map(|cloud_config| {
                cloud_config
                    .get_datacenters()
                    .iter()
                    .map(|(dc_name, dc_data)| {
                        KnownNode::CloudEndpoint(CloudEndpoint {
                            hostname: dc_data.get_server().to_owned(),
                            datacenter: dc_name.clone(),
                        })
                    })
                    .collect()
            }) {
            cloud_servers
        } else {
            known_nodes
        };

        // Ensure there is at least one known node
        if known_nodes.is_empty() {
            return Err(NewSessionError::EmptyKnownNodesList);
        }

        // Find IP addresses of all known nodes passed in the config
        let mut initial_peers: Vec<ContactPoint> = Vec::with_capacity(known_nodes.len());

        let mut to_resolve: Vec<(String, Option<String>)> = Vec::new();

        for node in known_nodes {
            match node {
                KnownNode::Hostname(hostname) => to_resolve.push((hostname, None)),
                KnownNode::Address(address) => initial_peers.push(ContactPoint {
                    address,
                    datacenter: None,
                }),
                #[cfg(feature = "cloud")]
                KnownNode::CloudEndpoint(CloudEndpoint {
                    hostname,
                    datacenter,
                }) => to_resolve.push((hostname, Some(datacenter))),
            };
        }
        let resolve_futures = to_resolve
            .into_iter()
            .map(|(hostname, datacenter)| async move {
                Ok::<_, NewSessionError>(ContactPoint {
                    address: resolve_hostname(&hostname).await?,
                    datacenter,
                })
            });
        let resolved: Vec<ContactPoint> = futures::future::try_join_all(resolve_futures).await?;
        initial_peers.extend(resolved);

        let connection_config = ConnectionConfig {
            compression: config.compression,
            tcp_nodelay: config.tcp_nodelay,
            #[cfg(feature = "ssl")]
            ssl_config: config.ssl_context.map(SslConfig::new_with_global_context),
            authenticator: config.authenticator.clone(),
            connect_timeout: config.connect_timeout,
            event_sender: None,
            default_consistency: Default::default(),
            address_translator: config.address_translator,
            #[cfg(feature = "cloud")]
            cloud_config: config.cloud_config,
        };

        let pool_config = PoolConfig {
            connection_config,
            pool_size: config.connection_pool_size,
            can_use_shard_aware_port: !config.disallow_shard_aware_port,
            keepalive_interval: config.keepalive_interval,
        };

        let cluster = Cluster::new(
            initial_peers,
            pool_config,
            config.keyspaces_to_fetch,
            config.fetch_schema_metadata,
            config.host_filter,
        )
        .await?;

        let default_execution_profile_handle = config.default_execution_profile_handle;

        let session = Session {
            cluster,
            default_execution_profile_handle,
            schema_agreement_interval: config.schema_agreement_interval,
            metrics: Arc::new(Metrics::new()),
            auto_await_schema_agreement_timeout: config.auto_await_schema_agreement_timeout,
            refresh_metadata_on_auto_schema_agreement: config
                .refresh_metadata_on_auto_schema_agreement,
        };

        if let Some(keyspace_name) = config.used_keyspace {
            session
                .use_keyspace(keyspace_name, config.keyspace_case_sensitive)
                .await?;
        }

        Ok(session)
    }

    /// Sends a query to the database and receives a response.\
    /// Returns only a single page of results, to receive multiple pages use [query_iter](Session::query_iter)
    ///
    /// This is the easiest way to make a query, but performance is worse than that of prepared queries.
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/simple.html) for more information
    /// # Arguments
    /// * `query` - query to perform, can be just a `&str` or the [Query](crate::query::Query) struct.
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
    ///
    /// # Examples
    /// ```rust
    /// # use scylla::Session;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
    /// // Insert an int and text into a table
    /// session
    ///     .query(
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
    /// // Read rows containing an int and text
    /// let rows_opt = session
    /// .query("SELECT a, b FROM ks.tab", &[])
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
    pub async fn query(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        self.query_paged(query, values, None).await
    }

    /// Queries the database with a custom paging state.
    /// # Arguments
    ///
    /// * `query` - query to be performed
    /// * `values` - values bound to the query
    /// * `paging_state` - previously received paging state or None
    pub async fn query_paged(
        &self,
        query: impl Into<Query>,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let query: Query = query.into();
        let serialized_values = values.serialized()?;

        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();
        let span = QuerySpan::new(&execution_profile, &query);

        let run_query_result = self
            .run_query(
                RoutingInfo::default(),
                &query.config,
                |node: Arc<Node>| async move { node.random_connection().await },
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = query
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    // Needed to avoid moving query and values into async move block
                    let query_ref = &query;
                    let values_ref = &serialized_values;
                    let paging_state_ref = &paging_state;
                    async move {
                        connection
                            .query_with_consistency(
                                query_ref,
                                values_ref,
                                consistency,
                                serial_consistency,
                                paging_state_ref.clone(),
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
            )
            .instrument(span.clone())
            .await
            .record_error(&span)?;
        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(&query.contents, &response)
            .await?;

        let query_result = response.into_query_result().record_error(&span);
        if let Ok(ref res) = query_result {
            span.record_none_explicitly(scylladb_tag!("rows_count"), res.rows_num().ok());
        }
        query_result
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
        contents: &str,
        response: &NonErrorQueryResponse,
    ) -> Result<(), QueryError> {
        if let Some(timeout) = self.auto_await_schema_agreement_timeout {
            if response.as_schema_change().is_some()
                && !self.await_timed_schema_agreement(timeout).await?
            {
                // TODO: The TimeoutError should allow to provide more context.
                // For now, print an error to the logs
                error!(
                    "Failed to reach schema agreement after a schema-altering statement: {}",
                    contents,
                );
                return Err(QueryError::TimeoutError);
            }

            if self.refresh_metadata_on_auto_schema_agreement
                && response.as_schema_change().is_some()
            {
                self.refresh_metadata().await?;
            }
        }

        Ok(())
    }

    /// Run a simple query with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [Query](crate::query::Query) passed to the function
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information
    ///
    /// # Arguments
    /// * `query` - query to perform, can be just a `&str` or the [Query](crate::query::Query) struct.
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
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
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let query: Query = query.into();
        let serialized_values = values.serialized()?;

        let execution_profile = query
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let span = QuerySpan::new(&execution_profile, &query);

        RowIterator::new_for_query(
            query,
            serialized_values.into_owned(),
            execution_profile,
            self.cluster.get_data(),
            self.metrics.clone(),
        )
        .instrument(span)
        .await
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
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information
    ///
    /// # Arguments
    /// * `query` - query to prepare, can be just a `&str` or the [Query](crate::query::Query) struct.
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
    /// session.execute(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prepare(&self, query: impl Into<Query>) -> Result<PreparedStatement, QueryError> {
        let query = query.into();

        let span = PreparationSpan::new(&query);

        let connections = self.cluster.get_working_connections().await?;

        // Prepare statements on all connections concurrently
        let handles = connections.iter().map(|c| c.prepare(&query));
        let mut results = join_all(handles).instrument(span.clone()).await;

        // If at least one prepare was successful prepare returns Ok

        // Find first result that is Ok, or Err if all failed
        let mut first_ok: Option<Result<PreparedStatement, QueryError>> = None;

        while let Some(res) = results.pop() {
            let is_ok: bool = res.is_ok();

            first_ok = Some(res);

            if is_ok {
                break;
            }
        }

        let mut prepared: PreparedStatement = first_ok.unwrap().map_err(|err| {
            span.record_error(&err);
            err
        })?;

        // Validate prepared ids equality
        for statement in results.into_iter().flatten() {
            if prepared.get_id() != statement.get_id() {
                return Err(QueryError::ProtocolError(
                    "Prepared statement Ids differ, all should be equal",
                ));
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

        span.record_prepared_id(&prepared);

        Ok(prepared)
    }

    fn extract_partitioner_name<'a>(
        &self,
        prepared: &PreparedStatement,
        cluster_data: &'a ClusterData,
    ) -> Option<&'a str> {
        cluster_data
            .keyspaces
            .get(prepared.get_keyspace_name()?)?
            .tables
            .get(prepared.get_table_name()?)?
            .partitioner
            .as_deref()
    }

    /// Execute a prepared query. Requires a [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// generated using [`Session::prepare`](Session::prepare)\
    /// Returns only a single page of results, to receive multiple pages use [execute_iter](Session::execute_iter)
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
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/prepared.html) for more information
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
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
    /// session.execute(&prepared, (to_insert,)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
    ) -> Result<QueryResult, QueryError> {
        self.execute_paged(prepared, values, None).await
    }

    /// Executes a previously prepared statement with previously received paging state
    /// # Arguments
    ///
    /// * `prepared` - a statement prepared with [prepare](crate::transport::session::Session::prepare)
    /// * `values` - values bound to the query
    /// * `paging_state` - paging state from the previous query or None
    pub async fn execute_paged(
        &self,
        prepared: &PreparedStatement,
        values: impl ValueList,
        paging_state: Option<Bytes>,
    ) -> Result<QueryResult, QueryError> {
        let serialized_values = values.serialized()?;
        let values_ref = &serialized_values;
        let paging_state_ref = &paging_state;

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();
        let span = PreparedSpan::new(&execution_profile, prepared, values_ref);

        let token = self.calculate_token(prepared, &serialized_values)?;

        let statement_info = RoutingInfo {
            consistency: prepared
                .get_consistency()
                .unwrap_or(self.default_execution_profile_handle.access().consistency),
            serial_consistency: prepared.get_serial_consistency(),
            token,
            keyspace: prepared.get_keyspace_name(),
            is_confirmed_lwt: prepared.is_confirmed_lwt(),
        };

        let run_query_result: RunQueryResult<NonErrorQueryResponse> = self
            .run_query(
                statement_info,
                &prepared.config,
                |node: Arc<Node>| async move {
                    match token {
                        Some(token) => node.connection_for_token(token).await,
                        None => node.random_connection().await,
                    }
                },
                |connection: Arc<Connection>,
                 consistency: Consistency,
                 execution_profile: &ExecutionProfileInner| {
                    let serial_consistency = prepared
                        .config
                        .serial_consistency
                        .unwrap_or(execution_profile.serial_consistency);
                    async move {
                        connection
                            .execute_with_consistency(
                                prepared,
                                values_ref,
                                consistency,
                                serial_consistency,
                                paging_state_ref.clone(),
                            )
                            .await
                            .and_then(QueryResponse::into_non_error_query_response)
                    }
                },
            )
            .instrument(span.clone())
            .await
            .record_error(&span)?;

        let response = match run_query_result {
            RunQueryResult::IgnoredWriteError => NonErrorQueryResponse {
                response: NonErrorResponse::Result(result::Result::Void),
                tracing_id: None,
                warnings: Vec::new(),
            },
            RunQueryResult::Completed(response) => response,
        };

        self.handle_set_keyspace_response(&response).await?;
        self.handle_auto_await_schema_agreement(prepared.get_statement(), &response)
            .await?;

        let query_result = response.into_query_result().record_error(&span);
        if let Ok(ref res) = query_result {
            span.record_none_explicitly(scylladb_tag!("rows_count"), res.rows_num().ok());
        }
        query_result
    }

    /// Run a prepared query with paging\
    /// This method will query all pages of the result\
    ///
    /// Returns an async iterator (stream) over all received rows\
    /// Page size can be specified in the [PreparedStatement](crate::prepared_statement::PreparedStatement)
    /// passed to the function
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/paged.html) for more information
    ///
    /// # Arguments
    /// * `prepared` - the prepared statement to execute, generated using [`Session::prepare`](Session::prepare)
    /// * `values` - values bound to the query, easiest way is to use a tuple of bound values
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
        values: impl ValueList,
    ) -> Result<RowIterator, QueryError> {
        let prepared = prepared.into();
        let serialized_values = values.serialized()?;

        let token = self.calculate_token(&prepared, &serialized_values)?;

        let execution_profile = prepared
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

        let span = PreparedSpan::new(&execution_profile, &prepared, &serialized_values);

        RowIterator::new_for_prepared_statement(PreparedIteratorConfig {
            prepared,
            values: serialized_values.into_owned(),
            token,
            execution_profile,
            cluster_data: self.cluster.get_data(),
            metrics: self.metrics.clone(),
        })
        .instrument(span)
        .await
    }

    /// Perform a batch query\
    /// Batch contains many `simple` or `prepared` queries which are executed at once\
    /// Batch doesn't return any rows
    ///
    /// Batch values must contain values for each of the queries
    ///
    /// See [the book](https://rust-driver.docs.scylladb.com/stable/queries/batch.html) for more information
    ///
    /// # Arguments
    /// * `batch` - [Batch](crate::batch::Batch) to be performed
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
    ) -> Result<QueryResult, QueryError> {
        // Shard-awareness behavior for batch will be to pick shard based on first batch statement's shard
        // If users batch statements by shard, they will be rewarded with full shard awareness

        // Extract first serialized_value
        let first_serialized_value = values.batch_values_iter().next_serialized().transpose()?;
        let first_serialized_value = first_serialized_value.as_deref();
        let statement_info = match (first_serialized_value, batch.statements.first()) {
            (Some(first_serialized_value), Some(BatchStatement::PreparedStatement(ps))) => {
                RoutingInfo {
                    consistency: batch
                        .get_consistency()
                        .unwrap_or(self.default_execution_profile_handle.access().consistency),
                    serial_consistency: batch.get_serial_consistency(),
                    token: self.calculate_token(ps, first_serialized_value)?,
                    keyspace: ps.get_keyspace_name(),
                    is_confirmed_lwt: false,
                }
            }
            _ => RoutingInfo::default(),
        };
        let first_value_token = statement_info.token;

        // Reuse first serialized value when serializing query, and delegate to `BatchValues::write_next_to_request`
        // directly for others (if they weren't already serialized, possibly don't even allocate the `SerializedValues`)
        let values = BatchValuesFirstSerialized::new(&values, first_serialized_value);
        let values_ref = &values;

        let execution_profile = batch
            .get_execution_profile_handle()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();
        let span = BatchSpan::new(&execution_profile, batch);

        let run_query_result = self
            .run_query(
                statement_info,
                &batch.config,
                |node: Arc<Node>| async move {
                    match first_value_token {
                        Some(first_value_token) => {
                            node.connection_for_token(first_value_token).await
                        }
                        None => node.random_connection().await,
                    }
                },
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
            )
            .instrument(span.clone())
            .await
            .record_error(&span)?;

        Ok(match run_query_result {
            RunQueryResult::IgnoredWriteError => QueryResult::default(),
            RunQueryResult::Completed(response) => response,
        })
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
    /// keyspace names can have up to 48 alphanumeric characters and contain underscores
    /// * `case_sensitive` - if set to true the generated query will put keyspace name in quotes
    /// # Example
    /// ```rust
    /// # use scylla::{Session, SessionBuilder};
    /// # use scylla::transport::Compression;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let session = SessionBuilder::new().known_node("127.0.0.1:9042").build().await?;
    /// session
    ///     .query("INSERT INTO my_keyspace.tab (a) VALUES ('test1')", &[])
    ///     .await?;
    ///
    /// session.use_keyspace("my_keyspace", false).await?;
    ///
    /// // Now we can omit keyspace name in the query
    /// session
    ///     .query("INSERT INTO tab (a) VALUES ('test2')", &[])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn use_keyspace(
        &self,
        keyspace_name: impl Into<String>,
        case_sensitive: bool,
    ) -> Result<(), QueryError> {
        // Trying to pass keyspace as bound value in "USE ?" doesn't work
        // So we have to create a string for query: "USE " + new_keyspace
        // To avoid any possible CQL injections it's good to verify that the name is valid
        let verified_ks_name = VerifiedKeyspaceName::new(keyspace_name.into(), case_sensitive)?;

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
        self.get_tracing_info_custom(tracing_id, &GetTracingConfig::default())
            .await
    }

    /// Queries tracing info with custom retry settings.\
    /// Tracing info might not be available immediately on queried node -
    /// that's why the driver performs a few attempts with sleeps in between.
    /// [`GetTracingConfig`] allows to specify a custom querying strategy.
    pub async fn get_tracing_info_custom(
        &self,
        tracing_id: &Uuid,
        config: &GetTracingConfig,
    ) -> Result<TracingInfo, QueryError> {
        // config.attempts is NonZeroU32 so at least one attempt will be made
        for _ in 0..config.attempts.get() {
            let current_try: Option<TracingInfo> = self
                .try_getting_tracing_info(tracing_id, Some(config.consistency))
                .await?;

            match current_try {
                Some(tracing_info) => return Ok(tracing_info),
                None => tokio::time::sleep(config.interval).await,
            };
        }

        Err(QueryError::ProtocolError(
            "All tracing queries returned an empty result, \
            maybe information didn't reach this node yet. \
            Consider using get_tracing_info_custom with \
            bigger interval in GetTracingConfig",
        ))
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
        traces_session_query.set_page_size(1024);

        // Query system_traces.events for TracingEvents
        let mut traces_events_query = Query::new(crate::tracing::TRACES_EVENTS_QUERY_STR);
        traces_events_query.config.consistency = consistency;
        traces_events_query.set_page_size(1024);

        let (traces_session_res, traces_events_res) = tokio::try_join!(
            self.query(traces_session_query, (tracing_id,)),
            self.query(traces_events_query, (tracing_id,))
        )?;

        // Get tracing info
        let tracing_info_row_res: Option<Result<TracingInfo, _>> = traces_session_res
            .rows
            .ok_or(QueryError::ProtocolError(
                "Response to system_traces.sessions query was not Rows",
            ))?
            .into_typed::<TracingInfo>()
            .next();

        let mut tracing_info: TracingInfo = match tracing_info_row_res {
            Some(tracing_info_row_res) => tracing_info_row_res.map_err(|_| {
                QueryError::ProtocolError(
                    "Columns from system_traces.session have an unexpected type",
                )
            })?,
            None => return Ok(None),
        };

        // Get tracing events
        let tracing_event_rows = traces_events_res
            .rows
            .ok_or(QueryError::ProtocolError(
                "Response to system_traces.events query was not Rows",
            ))?
            .into_typed::<TracingEvent>();

        for event in tracing_event_rows {
            let tracing_event: TracingEvent = event.map_err(|_| {
                QueryError::ProtocolError(
                    "Columns from system_traces.events have an unexpected type",
                )
            })?;

            tracing_info.events.push(tracing_event);
        }

        if tracing_info.events.is_empty() {
            return Ok(None);
        }

        Ok(Some(tracing_info))
    }

    // Returns which replicas are likely to take part in handling the query.
    // If a list of replicas cannot be easily narrowed down, all available replicas
    // will be returned.
    pub fn estimate_replicas_for_query(&self, statement: &RoutingInfo) -> Vec<Arc<Node>> {
        let cluster_data = self.cluster.get_data();
        match statement.token {
            Some(token) => {
                let cluster_data = self.cluster.get_data();
                cluster_data.get_token_endpoints(statement.keyspace.unwrap_or(""), token)
            }
            None => cluster_data.get_nodes_info().to_owned(),
        }
    }

    // This method allows to easily run a query using load balancing, retry policy etc.
    // Requires some information about the query and two closures
    // First closure is used to choose a connection
    // - query will use node.random_connection()
    // - execute will use node.connection_for_token()
    // The second closure is used to do the query itself on a connection
    // - query will use connection.query()
    // - execute will use connection.execute()
    // If this query closure fails with some errors retry policy is used to perform retries
    // On success this query's result is returned
    // I tried to make this closures take a reference instead of an Arc but failed
    // maybe once async closures get stabilized this can be fixed
    async fn run_query<'a, ConnFut, QueryFut, ResT>(
        &'a self,
        statement_info: RoutingInfo<'a>,
        statement_config: &'a StatementConfig,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
    ) -> Result<RunQueryResult<ResT>, QueryError>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let history_listener_and_id: Option<(&'a dyn HistoryListener, history::QueryId)> =
            statement_config
                .history_listener
                .as_ref()
                .map(|hl| (&**hl, hl.log_query_start()));

        let execution_profile = statement_config
            .execution_profile_handle
            .as_ref()
            .unwrap_or_else(|| self.get_default_execution_profile_handle())
            .access();

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
                I: Iterator<Item = NodeRef<'a>>,
            {
                iter: std::sync::Mutex<I>,
            }

            impl<'a, I> Iterator for &SharedPlan<'a, I>
            where
                I: Iterator<Item = NodeRef<'a>>,
            {
                type Item = NodeRef<'a>;

                fn next(&mut self) -> Option<Self::Item> {
                    self.iter.lock().unwrap().next()
                }
            }

            let retry_policy = &execution_profile.retry_policy;

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

                        self.execute_query(
                            &shared_query_plan,
                            &choose_connection,
                            &do_query,
                            &execution_profile,
                            ExecuteQueryContext {
                                is_idempotent: statement_config.is_idempotent,
                                consistency: statement_config.consistency,
                                retry_session: retry_policy.new_session(),
                                history_data,
                                query_info: &statement_info,
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
                        &choose_connection,
                        &do_query,
                        &execution_profile,
                        ExecuteQueryContext {
                            is_idempotent: statement_config.is_idempotent,
                            consistency: statement_config.consistency,
                            retry_session: retry_policy.new_session(),
                            history_data,
                            query_info: &statement_info,
                        },
                    )
                    .await
                    .unwrap_or(Err(QueryError::ProtocolError(
                        "Empty query plan - driver bug!",
                    )))
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

    async fn execute_query<'a, ConnFut, QueryFut, ResT>(
        &'a self,
        query_plan: impl Iterator<Item = NodeRef<'a>>,
        choose_connection: impl Fn(Arc<Node>) -> ConnFut,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
        execution_profile: &ExecutionProfileInner,
        mut context: ExecuteQueryContext<'a>,
    ) -> Option<Result<RunQueryResult<ResT>, QueryError>>
    where
        ConnFut: Future<Output = Result<Arc<Connection>, QueryError>>,
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let mut last_error: Option<QueryError> = None;
        let mut current_consistency: Consistency =
            context.consistency.unwrap_or(execution_profile.consistency);
        let mut attempt_count = 0_usize;

        'nodes_in_plan: for node in query_plan {
            'same_node_retries: loop {
                attempt_count += 1;
                let span = PendingAttemptSpan::new(node, attempt_count);
                trace!(parent: span.as_ref(), "Execution started");

                let connection: Arc<Connection> = match choose_connection(node.clone())
                    .instrument(span.clone())
                    .await
                {
                    Ok(connection) => connection,
                    Err(e) => {
                        trace!(
                            parent: span.as_ref(),
                            error = %e,
                            "Choosing connection failed"
                        );
                        span.complete();
                        span.record_error(&e);
                        last_error = Some(e);
                        // Broken connection doesn't count as a failed query, don't log in metrics
                        continue 'nodes_in_plan;
                    }
                };

                span.record_shard_id(connection.get_shard_info().as_ref().map(|info| info.shard));
                self.metrics.inc_total_nonpaged_queries();
                let query_start = std::time::Instant::now();

                trace!(
                    parent: span.as_ref(),
                    connection = %connection.get_connect_address(),
                    "Sending"
                );
                let attempt_id: Option<history::AttemptId> =
                    context.log_attempt_start(connection.get_connect_address());
                let query_result: Result<ResT, QueryError> =
                    do_query(connection, current_consistency, execution_profile)
                        .instrument(span.clone())
                        .await;

                span.complete();

                let elapsed = query_start.elapsed();
                last_error = match query_result {
                    Ok(response) => {
                        trace!(parent: span.as_ref(), "Query succeeded");
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
                            parent: span.as_ref(),
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

                // Safety: last_error must have been set, because else we would have returned.
                let the_error: &QueryError = last_error.as_ref().unwrap();
                span.record_error(the_error);

                // Use retry policy to decide what to do next
                let query_info = QueryInfo {
                    error: the_error,
                    is_idempotent: context.is_idempotent,
                    consistency: LegacyConsistency::Regular(
                        context.consistency.unwrap_or(execution_profile.consistency),
                    ),
                };

                let retry_decision = context.retry_session.decide_should_retry(query_info);
                trace!(
                    parent: span.as_ref(),
                    retry_decision = format_args!("{:?}", retry_decision)
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

    pub async fn await_schema_agreement(&self) -> Result<(), QueryError> {
        while !self.check_schema_agreement().await? {
            tokio::time::sleep(self.schema_agreement_interval).await
        }
        Ok(())
    }

    pub async fn await_timed_schema_agreement(
        &self,
        timeout_duration: Duration,
    ) -> Result<bool, QueryError> {
        timeout(timeout_duration, self.await_schema_agreement())
            .await
            .map_or(Ok(false), |res| res.and(Ok(true)))
    }

    async fn schema_agreement_auxiliary<ResT, QueryFut>(
        &self,
        do_query: impl Fn(Arc<Connection>, Consistency, &ExecutionProfileInner) -> QueryFut,
    ) -> Result<ResT, QueryError>
    where
        QueryFut: Future<Output = Result<ResT, QueryError>>,
        ResT: AllowedRunQueryResTType,
    {
        let info = RoutingInfo::default();
        let config = StatementConfig {
            is_idempotent: true,
            serial_consistency: Some(Some(SerialConsistency::LocalSerial)),
            ..Default::default()
        };

        match self
            .run_query(
                info,
                &config,
                |node: Arc<Node>| async move { node.random_connection().await },
                do_query,
            )
            .await?
        {
            RunQueryResult::IgnoredWriteError => Err(QueryError::ProtocolError(
                "Retry policy has made the driver ignore schema's agreement query.",
            )),
            RunQueryResult::Completed(result) => Ok(result),
        }
    }

    pub async fn check_schema_agreement(&self) -> Result<bool, QueryError> {
        let connections = self.cluster.get_working_connections().await?;

        let handles = connections.iter().map(|c| c.fetch_schema_version());
        let versions = try_join_all(handles).await?;

        let local_version: Uuid = versions[0];
        let in_agreement = versions.into_iter().all(|v| v == local_version);
        Ok(in_agreement)
    }

    pub async fn fetch_schema_version(&self) -> Result<Uuid, QueryError> {
        // We ignore custom Consistency that a retry policy could decide to put here, using the default instead.
        self.schema_agreement_auxiliary(
            |connection: Arc<Connection>, _: Consistency, _: &ExecutionProfileInner| async move {
                connection.fetch_schema_version().await
            },
        )
        .await
    }

    fn calculate_token(
        &self,
        prepared: &PreparedStatement,
        serialized_values: &SerializedValues,
    ) -> Result<Option<Token>, QueryError> {
        if !prepared.is_token_aware() {
            return Ok(None);
        }

        let partitioner_name = prepared.get_partitioner_name();

        let partition_key = calculate_partition_key(prepared, serialized_values)?;

        Ok(Some(partitioner_name.hash(partition_key)))
    }

    fn get_default_execution_profile_handle(&self) -> &ExecutionProfileHandle {
        &self.default_execution_profile_handle
    }
}

fn calculate_partition_key(
    stmt: &PreparedStatement,
    values: &SerializedValues,
) -> Result<Bytes, QueryError> {
    match stmt.compute_partition_key(values) {
        Ok(key) => Ok(key),
        Err(PartitionKeyError::NoPkIndexValue(_, _)) => Err(QueryError::ProtocolError(
            "No pk indexes - can't calculate token",
        )),
        Err(PartitionKeyError::ValueTooLong(values_len)) => Err(QueryError::BadQuery(
            BadQuery::ValuesTooLongForKey(values_len, u16::MAX.into()),
        )),
    }
}

// Resolve the given hostname using a DNS lookup if necessary.
// The resolution may return multiple IPs and the function returns one of them.
// It prefers to return IPv4s first, and only if there are none, IPv6s.
pub(crate) async fn resolve_hostname(hostname: &str) -> Result<SocketAddr, NewSessionError> {
    let mut ret = None;
    let addrs: Vec<SocketAddr> = match lookup_host(hostname).await {
        Ok(addrs) => addrs.collect(),
        // Use a default port in case of error, but propagate the original error on failure
        Err(e) => lookup_host((hostname, 9042)).await.or(Err(e))?.collect(),
    };
    for a in addrs {
        match a {
            SocketAddr::V4(_) => return Ok(a),
            _ => {
                ret = Some(a);
            }
        }
    }

    ret.ok_or_else(|| NewSessionError::FailedToResolveAddress(hostname.to_string()))
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
pub trait AllowedRunQueryResTType {}

impl AllowedRunQueryResTType for Uuid {}
impl AllowedRunQueryResTType for QueryResult {}
impl AllowedRunQueryResTType for NonErrorQueryResponse {}

struct ExecuteQueryContext<'a> {
    is_idempotent: bool,
    consistency: Option<Consistency>,
    retry_session: Box<dyn RetrySession>,
    history_data: Option<HistoryData<'a>>,
    query_info: &'a load_balancing::RoutingInfo<'a>,
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
