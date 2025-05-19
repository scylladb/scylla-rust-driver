use scylla_cql::frame::response::error::DbError;
use tracing::{error, warn};

use crate::client::caching_session::CachingSession;
use crate::client::session::Session;
use crate::client::session_builder::{GenericSessionBuilder, SessionBuilderKind};
use crate::cluster::ClusterState;
use crate::cluster::NodeRef;
use crate::errors::{ExecutionError, RequestAttemptError};
use crate::network::Connection;
use crate::policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo};
use crate::policies::retry::{RequestInfo, RetryDecision, RetryPolicy, RetrySession};
use crate::routing::Shard;
use crate::statement::unprepared::Statement;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{num::NonZeroU32, time::Duration};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

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

// Just like resolve_hostname in session.rs
pub(crate) async fn resolve_hostname(hostname: &str) -> SocketAddr {
    match tokio::net::lookup_host(hostname).await {
        Ok(mut addrs) => addrs.next().unwrap(),
        Err(_) => {
            tokio::net::lookup_host((hostname, 9042)) // Port might not be specified, try default
                .await
                .unwrap()
                .next()
                .unwrap()
        }
    }
}

pub(crate) async fn supports_feature(session: &Session, feature: &str) -> bool {
    // Cassandra doesn't have a concept of features, so first detect
    // if there is the `supported_features` column in system.local

    let meta = session.get_cluster_state();
    let system_local = meta
        .keyspaces
        .get("system")
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

// Creates a generic session builder based on conditional compilation configuration
// For SessionBuilder of DefaultMode type, adds localhost to known hosts, as all of the tests
// connect to localhost.
pub(crate) fn create_new_session_builder() -> GenericSessionBuilder<impl SessionBuilderKind> {
    let session_builder = {
        #[cfg(not(scylla_cloud_tests))]
        {
            use crate::client::session_builder::SessionBuilder;

            let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

            SessionBuilder::new().known_node(uri)
        }

        #[cfg(scylla_cloud_tests)]
        {
            use crate::client::session_builder::CloudSessionBuilder;
            use std::path::Path;

            std::env::var("CLOUD_CONFIG_PATH")
                .map(|config_path| {
                    CloudSessionBuilder::new(
                        Path::new(&config_path),
                        crate::cloud::CloudTlsProvider::OpenSsl010,
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

pub(crate) async fn scylla_supports_tablets(session: &Session) -> bool {
    supports_feature(session, "TABLETS").await
}

pub(crate) fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(tracing_subscriber::fmt::TestWriter::new())
        .try_init();
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
        .unwrap_or_default()
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

#[async_trait::async_trait]
impl PerformDDL for Connection {
    async fn ddl(&self, query: impl Into<Statement> + Send) -> Result<(), ExecutionError> {
        let mut query = query.into();
        apply_ddl_lbp(&mut query);
        self.query_unpaged(query)
            .await
            .map(|_| ())
            .map_err(ExecutionError::LastAttemptError)
    }
}
