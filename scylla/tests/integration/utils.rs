use futures::Future;
use scylla::client::caching_session::CachingSession;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::{GenericSessionBuilder, SessionBuilderKind};
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::deserialize::value::DeserializeValue;
use scylla::errors::{DbError, ExecutionError, RequestAttemptError};
use scylla::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use scylla::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use scylla::routing::Shard;
use scylla::statement::unprepared::Statement;
use std::collections::HashMap;
use std::env;
use std::net::{IpAddr, SocketAddr};
use std::num::NonZeroU32;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, warn};
use uuid::Uuid;

use scylla_proxy::{Node, Proxy, ProxyError, RunningProxy, ShardAwareness};

pub(crate) fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .try_init();
}

/// Finds the local IP address for a given destination IP address.
///
/// This function uses the `ip route get` command to get the routing information for the destination IP.
pub(crate) fn find_local_ip_for_destination(dest: IpAddr) -> Option<IpAddr> {
    let output = Command::new("ip")
        .arg("route")
        .arg("get")
        .arg(dest.to_string())
        .output()
        .ok()?;

    let output_str = std::str::from_utf8(&output.stdout).ok()?;

    // Example output for `ip route get 172.42.0.2`:
    //
    // 172.42.0.2 dev br-1e395ce79670 src 172.42.0.1 uid 1000
    //     cache
    let local_ip_str = output_str
        .split_whitespace()
        .skip_while(|s| *s != "src")
        .nth(1)?;

    IpAddr::from_str(local_ip_str).ok()
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
        .get_keyspace("system")
        .unwrap()
        .tables
        .get("local")
        .unwrap();

    if !system_local.columns.contains_key("supported_features") {
        return false;
    }

    let result = session
        .query_unpaged(
            "SELECT supported_features FROM system.local WHERE key='local'",
            (),
        )
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
            use scylla::client::session_builder::CloudSessionBuilder;
            use std::path::Path;

            std::env::var("CLOUD_CONFIG_PATH")
                .map(|config_path| {
                    CloudSessionBuilder::new(
                        Path::new(&config_path),
                        scylla::cloud::CloudTlsProvider::OpenSsl010,
                    )
                })
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

#[derive(Debug, Default)]
struct SchemaQueriesRetrySession {
    count: usize,
}

impl RetrySession for SchemaQueriesRetrySession {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        match request_info.error {
            RequestAttemptError::DbError(DbError::ServerError, s)
                if s == "Failed to apply group 0 change due to concurrent modification" =>
            {
                self.count += 1;
                // Give up if there are many failures.
                // In this case we really should do something about it in the
                // core, because it is absurd for DDL queries to fail this often.
                if self.count >= 10 {
                    error!("Received TENTH(!) group 0 concurrent modification error during DDL. Please fix Scylla Core.");
                    RetryDecision::DontRetry
                } else {
                    warn!("Received group 0 concurrent modification error during DDL. Performing retry #{}.", self.count);
                    RetryDecision::RetrySameTarget(None)
                }
            }
            _ => RetryDecision::DontRetry,
        }
    }

    fn reset(&mut self) {
        *self = Default::default()
    }
}

#[derive(Debug)]
struct SchemaQueriesRetryPolicy;

impl RetryPolicy for SchemaQueriesRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Box::new(SchemaQueriesRetrySession::default())
    }
}

fn apply_ddl_lbp(query: &mut Statement) {
    let policy = query
        .get_execution_profile_handle()
        .map(|profile| profile.pointee_to_builder())
        .unwrap_or(ExecutionProfile::builder())
        .load_balancing_policy(Arc::new(SchemaQueriesLBP))
        .retry_policy(Arc::new(SchemaQueriesRetryPolicy))
        .build();
    query.set_execution_profile_handle(Some(policy.into_handle()));
}

// This is just to make it easier to call the above function:
// we'll be able to do session.ddl(...) instead of perform_ddl(&session, ...)
// or something like that.
#[async_trait::async_trait]
pub(crate) trait PerformDDL {
    async fn ddl(&self, query: impl Into<Statement> + Send) -> Result<(), ExecutionError>;
}

#[async_trait::async_trait]
impl PerformDDL for Session {
    async fn ddl(&self, query: impl Into<Statement> + Send) -> Result<(), ExecutionError> {
        let mut query = query.into();
        apply_ddl_lbp(&mut query);
        self.query_unpaged(query, &[]).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl PerformDDL for CachingSession {
    async fn ddl(&self, query: impl Into<Statement> + Send) -> Result<(), ExecutionError> {
        let mut query = query.into();
        apply_ddl_lbp(&mut query);
        self.execute_unpaged(query, &[]).await.map(|_| ())
    }
}

/// Calculates a list of nodes host ids, in the same order as passed proxy_uris.
/// Useful if a test wants to set rules on some node, and then send requests to this node.
pub(crate) fn calculate_proxy_host_ids(
    proxy_uris: &[String],
    translation_map: &HashMap<SocketAddr, SocketAddr>,
    session: &Session,
) -> Vec<Uuid> {
    // First we calculate lists of proxy and real ips of nodes.
    // Why only ips? Because they are always unique (at least in tests, but it should be true as a general statement too),
    // and dealing with ports would complicate code.
    let proxy_ips: Vec<IpAddr> = proxy_uris
        .iter()
        .map(|uri| uri.as_str().parse::<SocketAddr>().unwrap().ip())
        .collect::<Vec<_>>();

    let real_node_ips: Vec<IpAddr> = {
        let reversed_translation_map = translation_map
            .iter()
            .map(|(a, b)| (b.ip(), a.ip()))
            .collect::<HashMap<_, _>>();

        proxy_uris
            .iter()
            .map(|uri| {
                *reversed_translation_map
                    .get(&uri.as_str().parse::<SocketAddr>().unwrap().ip())
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(proxy_ips.len(), real_node_ips.len());

    let state = session.get_cluster_state();
    let nodes = state.get_nodes_info();

    // Now we can generate a list of host ids, by iterating over IPs, finding matching `Node` object
    // and retrieving its `host_id`.
    // Each Node object has either translated or untranslated address inside, so we need to
    // compare against both when searching.
    let host_ids: Vec<Uuid> = proxy_ips
        .into_iter()
        .zip(real_node_ips)
        .map(|(proxy_ip, real_ip)| {
            let node = nodes
                .iter()
                .find(|n| n.address.ip() == proxy_ip || n.address.ip() == real_ip)
                .unwrap();
            node.host_id
        })
        .collect();

    assert_eq!(host_ids.len(), proxy_uris.len());
    host_ids
}
