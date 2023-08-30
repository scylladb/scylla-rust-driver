use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::{
    speculative_execution::PercentileSpeculativeExecutionPolicy,
    transport::execution_profile::ExecutionProfile,
};

use anyhow::Result;
use std::{env, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());
    println!("Connecting to {} ...", uri);

    let speculative = PercentileSpeculativeExecutionPolicy {
        max_retry_count: 2,
        percentile: 99.0,
    };

    let speculative_profile = ExecutionProfile::builder()
        .speculative_execution_policy(Some(Arc::new(speculative)))
        .build();

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .default_execution_profile_handle(speculative_profile.into_handle())
        .build()
        .await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.speculative_execution (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let mut select_stmt = session
        .prepare("SELECT a, b, c FROM examples_ks.speculative_execution")
        .await?;

    // This will allow for speculative execution
    select_stmt.set_is_idempotent(true);

    // This will trigger speculative execution
    session.execute_unpaged(&select_stmt, &[]).await?;

    Ok(())
}
