use scylla::{
    speculative_execution::PercentileSpeculativeExecutionPolicy, Session, SessionBuilder,
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

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .speculative_execution(Arc::new(speculative))
        .build()
        .await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    let mut select_stmt = session.prepare("SELECT a, b, c FROM ks.t").await?;

    // This will allow for speculative execution
    select_stmt.config.set_is_idempotent(true);

    // This will trigger speculative execution
    session.execute(&select_stmt, &[]).await?;

    Ok(())
}
