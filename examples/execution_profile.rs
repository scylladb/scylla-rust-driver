use anyhow::Result;
use scylla::load_balancing::{RoundRobinPolicy, TokenAwarePolicy};
use scylla::query::Query;
use scylla::retry_policy::{DefaultRetryPolicy, FallthroughRetryPolicy};
use scylla::speculative_execution::PercentileSpeculativeExecutionPolicy;
use scylla::statement::{Consistency, SerialConsistency};
use scylla::transport::session::Session;
use scylla::transport::ExecutionProfile;
use scylla::{SessionBuilder, SessionConfig};
use std::env;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);

    let profile1 = ExecutionProfile::builder()
        .consistency(Consistency::EachQuorum)
        .serial_consistency(Some(SerialConsistency::Serial))
        .request_timeout(Some(Duration::from_secs(42)))
        .load_balancing_policy(Arc::new(TokenAwarePolicy::new(Box::new(
            RoundRobinPolicy::new(),
        ))))
        .retry_policy(Box::new(FallthroughRetryPolicy::new()))
        .speculative_execution_policy(Some(Arc::new(PercentileSpeculativeExecutionPolicy {
            max_retry_count: 2,
            percentile: 42.0,
        })))
        .build();

    let profile2 = ExecutionProfile::builder()
        .consistency(Consistency::One)
        .serial_consistency(None)
        .request_timeout(Some(Duration::from_secs(3)))
        .load_balancing_policy(Arc::new(RoundRobinPolicy::new()))
        .retry_policy(Box::new(DefaultRetryPolicy::new()))
        .speculative_execution_policy(None)
        .build();

    let handle1 = profile1.clone().into_handle();
    let mut handle2 = profile2.into_handle();

    // It is even possible to use multiple sessions interleaved, having them configured with different profiles.
    let session1: Session = SessionBuilder::new()
        .known_node(&uri)
        .default_execution_profile_handle(handle1.clone())
        .build()
        .await?;

    let session2: Session = SessionBuilder::new()
        .known_node(&uri)
        .default_execution_profile_handle(handle2.clone())
        .build()
        .await?;

    // As default execution profile is not provided explicitly, session 3 uses a predefined one.
    let mut session_3_config = SessionConfig::new();
    session_3_config.add_known_node(uri);
    let session3: Session = Session::connect(session_3_config).await?;

    session1.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session2
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let mut query_insert: Query = "INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)".into();

    // As `query_insert` is set another handle than session1, the execution profile pointed by query's handle
    // will be preferred, so the query below will be executed with `profile2`, even though `session1` is set `profile1`.
    query_insert.set_execution_profile_handle(Some(handle2.clone()));
    session1.query(query_insert.clone(), (3, 4, "def")).await?;

    // One can, however, change the execution profile referred by a handle:
    handle2.map_to_another_profile(profile1);
    // And now the following queries are executed with profile1:
    session1.query(query_insert.clone(), (3, 4, "def")).await?;
    session2.query("SELECT * FROM ks.t", ()).await?;

    // One can unset a profile handle from a statement and, since then, execute it with session's default profile.
    query_insert.set_execution_profile_handle(None);
    session3.query("SELECT * FROM ks.t", ()).await?; // This executes with default session profile.
    session2.query("SELECT * FROM ks.t", ()).await?; // This executes with profile1.

    Ok(())
}
