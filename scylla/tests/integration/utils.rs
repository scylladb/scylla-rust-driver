use futures::future::try_join_all;
use futures::Future;
use itertools::Either;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::{GenericSessionBuilder, SessionBuilderKind};
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::deserialize::DeserializeValue;
use scylla::errors::ExecutionError;
use scylla::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use scylla::prepared_statement::PreparedStatement;
use scylla::query::Query;
use scylla::response::query_result::QueryResult;
use scylla::routing::Shard;
use scylla::serialize::row::SerializeRow;
use std::collections::HashMap;
use std::env;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use scylla_proxy::{Node, Proxy, ProxyError, RunningProxy, ShardAwareness};

pub(crate) fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .try_init();
}

static UNIQUE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub(crate) fn unique_keyspace_name() -> String {
    let cnt = UNIQUE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let name = format!(
        "test_rust_{}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        cnt
    );
    println!("Unique name: {}", name);
    name
}

pub(crate) async fn test_with_3_node_cluster<F, Fut>(
    shard_awareness: ShardAwareness,
    test: F,
) -> Result<(), ProxyError>
where
    F: FnOnce([String; 3], HashMap<SocketAddr, SocketAddr>, RunningProxy) -> Fut,
    Fut: Future<Output = RunningProxy>,
{
    let real1_uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    let proxy1_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let real2_uri = env::var("SCYLLA_URI2").unwrap_or_else(|_| "127.0.0.2:9042".to_string());
    let proxy2_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());
    let real3_uri = env::var("SCYLLA_URI3").unwrap_or_else(|_| "127.0.0.3:9042".to_string());
    let proxy3_uri = format!("{}:9042", scylla_proxy::get_exclusive_local_address());

    let real1_addr = SocketAddr::from_str(real1_uri.as_str()).unwrap();
    let proxy1_addr = SocketAddr::from_str(proxy1_uri.as_str()).unwrap();
    let real2_addr = SocketAddr::from_str(real2_uri.as_str()).unwrap();
    let proxy2_addr = SocketAddr::from_str(proxy2_uri.as_str()).unwrap();
    let real3_addr = SocketAddr::from_str(real3_uri.as_str()).unwrap();
    let proxy3_addr = SocketAddr::from_str(proxy3_uri.as_str()).unwrap();

    let proxy = Proxy::new(
        [
            (proxy1_addr, real1_addr),
            (proxy2_addr, real2_addr),
            (proxy3_addr, real3_addr),
        ]
        .map(|(proxy_addr, real_addr)| {
            Node::builder()
                .real_address(real_addr)
                .proxy_address(proxy_addr)
                .shard_awareness(shard_awareness)
                .build()
        }),
    );

    let translation_map = proxy.translation_map();
    let running_proxy = proxy.run().await.unwrap();

    let running_proxy = test(
        [proxy1_uri, proxy2_uri, proxy3_uri],
        translation_map,
        running_proxy,
    )
    .await;

    running_proxy.finish().await
}

pub(crate) async fn supports_feature(session: &Session, feature: &str) -> bool {
    // Cassandra doesn't have a concept of features, so first detect
    // if there is the `supported_features` column in system.local

    let meta = session.get_cluster_state();
    let system_local = meta
        .get_keyspace_info()
        .get("system")
        .unwrap()
        .tables
        .get("local")
        .unwrap();

    if !system_local.columns.contains_key("supported_features") {
        return false;
    }

    let result = session
        .query_unpaged("SELECT supported_features FROM system.local", ())
        .await
        .unwrap()
        .into_rows_result()
        .unwrap();

    let (features,): (Option<&str>,) = result.single_row().unwrap();

    features
        .unwrap_or_default()
        .split(',')
        .any(|f| f == feature)
}

pub(crate) async fn scylla_supports_tablets(session: &Session) -> bool {
    supports_feature(session, "TABLETS").await
}

// Creates a generic session builder based on conditional compilation configuration
// For SessionBuilder of DefaultMode type, adds localhost to known hosts, as all of the tests
// connect to localhost.
pub(crate) fn create_new_session_builder() -> GenericSessionBuilder<impl SessionBuilderKind> {
    let session_builder = {
        #[cfg(not(scylla_cloud_tests))]
        {
            use scylla::client::session_builder::SessionBuilder;

            let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

            SessionBuilder::new().known_node(uri)
        }

        #[cfg(scylla_cloud_tests)]
        {
            use scylla::client::session_builder::{CloudMode, CloudSessionBuilder};
            use std::path::Path;

            std::env::var("CLOUD_CONFIG_PATH")
                .map(|config_path| CloudSessionBuilder::new(Path::new(&config_path)))
                .expect("Failed to initialize CloudSessionBuilder")
                .expect("CLOUD_CONFIG_PATH environment variable is missing")
        }
    };

    // The reason why we enable so long waiting for TracingInfo is... Cassandra. (Yes, again.)
    // In Cassandra Java Driver, the wait time for tracing info is 10 seconds, so here we do the same.
    // However, as Scylla usually gets TracingInfo ready really fast (our default interval is hence 3ms),
    // we stick to a not-so-much-terribly-long interval here.
    session_builder
        .tracing_info_fetch_attempts(NonZeroU32::new(200).unwrap())
        .tracing_info_fetch_interval(Duration::from_millis(50))
}

// Shorthands for better readability.
// Copied from Scylla because we don't want to make it public there.
pub(crate) trait DeserializeOwnedValue:
    for<'frame, 'metadata> DeserializeValue<'frame, 'metadata>
{
}
impl<T> DeserializeOwnedValue for T where
    T: for<'frame, 'metadata> DeserializeValue<'frame, 'metadata>
{
}

// This LBP produces a predictable query plan - it order the nodes
// by position in the ring.
// This is to make sure that all DDL queries land on the same node,
// to prevent errors from concurrent DDL queries executed on different nodes.
#[derive(Debug)]
struct SchemaQueriesLBP;

impl LoadBalancingPolicy for SchemaQueriesLBP {
    fn pick<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        // I'm not sure if Scylla can handle concurrent DDL queries to different shard,
        // in other words if its local lock is per-node or per shard.
        // Just to be safe, let's use explicit shard.
        cluster.get_nodes_info().first().map(|node| (node, Some(0)))
    }

    fn fallback<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        Box::new(cluster.get_nodes_info().iter().map(|node| (node, Some(0))))
    }

    fn name(&self) -> String {
        "SchemaQueriesLBP".to_owned()
    }
}

fn apply_ddl_lbp(query: &mut Query) {
    let policy = query
        .get_execution_profile_handle()
        .map(|profile| profile.pointee_to_builder())
        .unwrap_or(ExecutionProfile::builder())
        .load_balancing_policy(Arc::new(SchemaQueriesLBP))
        .build();
    query.set_execution_profile_handle(Some(policy.into_handle()));
}

// This is just to make it easier to call the above function:
// we'll be able to do session.ddl(...) instead of perform_ddl(&session, ...)
// or something like that.
#[async_trait::async_trait]
pub(crate) trait PerformDDL {
    async fn ddl(&self, query: impl Into<Query> + Send) -> Result<(), ExecutionError>;
}

#[async_trait::async_trait]
impl PerformDDL for Session {
    async fn ddl(&self, query: impl Into<Query> + Send) -> Result<(), ExecutionError> {
        let mut query = query.into();
        apply_ddl_lbp(&mut query);
        self.query_unpaged(query, &[]).await.map(|_| ())
    }
}

#[derive(Debug)]
pub(crate) struct SingleTargetLBP {
    target: (Arc<scylla::cluster::Node>, Option<u32>),
}

impl LoadBalancingPolicy for SingleTargetLBP {
    fn pick<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<u32>)> {
        Some((&self.target.0, self.target.1))
    }

    fn fallback<'a>(
        &'a self,
        _query: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        Box::new(std::iter::empty())
    }

    fn name(&self) -> String {
        "SingleTargetLBP".to_owned()
    }
}

async fn for_each_target_execute<ExecuteFn, ExecuteFut>(
    cluster: &ClusterState,
    execute: ExecuteFn,
) -> Result<Vec<QueryResult>, ExecutionError>
where
    ExecuteFn: Fn(Arc<scylla::cluster::Node>, Option<Shard>) -> ExecuteFut,
    ExecuteFut: Future<Output = Result<QueryResult, ExecutionError>>,
{
    let tasks = cluster.get_nodes_info().iter().flat_map(|node| {
        let maybe_shard_count: Option<u16> = node.sharder().map(|sharder| sharder.nr_shards.into());
        match maybe_shard_count {
            Some(shard_count) => Either::Left(
                (0..shard_count).map(|shard| execute(node.clone(), Some(shard as u32))),
            ),
            None => Either::Right(std::iter::once(execute(node.clone(), None))),
        }
    });

    try_join_all(tasks).await
}

pub(crate) async fn execute_prepared_statement_everywhere(
    session: &Session,
    cluster: &ClusterState,
    statement: &PreparedStatement,
    values: &dyn SerializeRow,
) -> Result<Vec<QueryResult>, ExecutionError> {
    async fn send_to_target(
        session: &Session,
        node: Arc<scylla::cluster::Node>,
        shard: Option<Shard>,
        statement: &PreparedStatement,
        values: &dyn SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        let mut stmt = statement.clone();
        let values_ref = &values;
        let policy = SingleTargetLBP {
            target: (node, shard),
        };
        let execution_profile = ExecutionProfile::builder()
            .load_balancing_policy(Arc::new(policy))
            .build();
        stmt.set_execution_profile_handle(Some(execution_profile.into_handle()));

        session.execute_unpaged(&stmt, values_ref).await
    }

    for_each_target_execute(cluster, |node, shard| {
        send_to_target(session, node, shard, statement, values)
    })
    .await
}

pub(crate) async fn execute_unprepared_statement_everywhere(
    session: &Session,
    cluster: &ClusterState,
    query: &Query,
    values: &dyn SerializeRow,
) -> Result<Vec<QueryResult>, ExecutionError> {
    async fn send_to_target(
        session: &Session,
        node: Arc<scylla::cluster::Node>,
        shard: Option<Shard>,
        mut statement: Query,
        values: &dyn SerializeRow,
    ) -> Result<QueryResult, ExecutionError> {
        let policy = SingleTargetLBP {
            target: (node, shard),
        };
        let execution_profile = ExecutionProfile::builder()
            .load_balancing_policy(Arc::new(policy))
            .build();
        statement.set_execution_profile_handle(Some(execution_profile.into_handle()));

        session.query_unpaged(statement, values).await
    }

    for_each_target_execute(cluster, |node, shard| {
        send_to_target(session, node, shard, query.clone(), values)
    })
    .await
}
