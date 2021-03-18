// CQL Tracing allows to see each step during execution of a query
// query() prepare() and execute() can be traced

use anyhow::{anyhow, Result};
use scylla::statement::{prepared_statement::PreparedStatement, query::Query, Consistency};
use scylla::tracing::{GetTracingConfig, TracingInfo};
use scylla::QueryResult;
use scylla::{Session, SessionBuilder};
use std::env;
use std::num::NonZeroU32;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    println!("Connecting to {} ...", uri);
    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.tracing_example (val text primary key)",
            &[],
        )
        .await?;

    // QUERY
    // Create a simple query and enable tracing for it
    let mut query: Query = Query::new("SELECT val from ks.tracing_example".to_string());
    query.set_tracing(true);

    // QueryResult will contain a tracing_id which can be used to query tracing information
    let query_result: QueryResult = session.query(query.clone(), &[]).await?;
    let query_tracing_id: Uuid = query_result
        .tracing_id
        .ok_or_else(|| anyhow!("Tracing id is None!"))?;

    // Get tracing information for this query and print it
    let tracing_info: TracingInfo = session.get_tracing_info(&query_tracing_id).await?;
    println!("Query tracing info: {:#?}\n", tracing_info);

    // PREPARE
    // Now prepare a query - query to be prepared has tracing set so the prepare will be traced
    let mut prepared: PreparedStatement = session.prepare(query).await?;

    // prepared.prepare_tracing_id contains tracing ids of all prepare requests
    let prepare_ids: &Vec<Uuid> = &prepared.prepare_tracing_ids;
    println!("Prepare tracing ids: {:?}\n", prepare_ids);

    // EXECUTE
    // To trace execution of a prepared statement tracing must be enabled for it
    prepared.set_tracing(true);

    let execute_result: QueryResult = session.execute(&prepared, &[]).await?;
    println!("Execute tracing id: {:?}", execute_result.tracing_id);

    // CUSTOM
    // GetTracingConfig allows to specify a custom settings for querying tracing info
    // Tracing info might not immediately be available on queried node
    // so the driver performs a few attempts with sleeps in between.

    let custom_config: GetTracingConfig = GetTracingConfig {
        attempts: NonZeroU32::new(8).unwrap(),
        interval: Duration::from_millis(100),
        consistency: Consistency::One,
    };

    let _custom_info: TracingInfo = session
        .get_tracing_info_custom(&query_tracing_id, &custom_config)
        .await?;

    Ok(())
}
