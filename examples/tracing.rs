// CQL Tracing allows to see each step during execution of a query
// query() prepare() execute() batch() query_iter() and execute_iter() can be traced

use anyhow::{anyhow, Result};
use scylla::batch::Batch;
use scylla::statement::{
    prepared_statement::PreparedStatement, query::Query, Consistency, SerialConsistency,
};
use scylla::tracing::{GetTracingConfig, TracingInfo};
use scylla::transport::iterator::RawIterator;
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
    let mut query: Query = Query::new("SELECT val from ks.tracing_example");
    query.set_tracing(true);
    query.set_serial_consistency(Some(SerialConsistency::LocalSerial));

    // QueryResult will contain a tracing_id which can be used to query tracing information
    let query_result: QueryResult = session.query(query.clone(), &[]).await?;
    let query_tracing_id: Uuid = query_result
        .tracing_id()
        .ok_or_else(|| anyhow!("Tracing id is None!"))?;

    // Get tracing information for this query and print it
    let tracing_info: TracingInfo = session.get_tracing_info(&query_tracing_id).await?;
    println!(
        "Tracing command {} performed by {:?}, which took {}µs total",
        tracing_info.command.as_deref().unwrap_or("?"),
        tracing_info.client,
        tracing_info.duration.unwrap_or(0)
    );
    println!("│  UUID   │ Elapsed  │ Command");
    println!("├─────────┼──────────┼──────────────────");
    for event in tracing_info.events {
        println!(
            "│{} │ {:6}µs │ {}",
            &event.event_id.to_string()[0..8],
            event.source_elapsed.unwrap_or(0),
            event.activity.as_deref().unwrap_or("?"),
        );
    }
    println!("└─────────┴──────────┴──────────────────");

    // PREPARE
    // Now prepare a query - query to be prepared has tracing set so the prepare will be traced
    let mut prepared: PreparedStatement = session.prepare(query.clone()).await?;

    // prepared.prepare_tracing_id contains tracing ids of all prepare requests
    let prepare_ids: &Vec<Uuid> = &prepared.prepare_tracing_ids;
    println!("Prepare tracing ids: {:?}\n", prepare_ids);

    // EXECUTE
    // To trace execution of a prepared statement tracing must be enabled for it
    prepared.set_tracing(true);

    let execute_result: QueryResult = session.execute(&prepared, &[]).await?;
    println!("Execute tracing id: {:?}", execute_result.tracing_id());

    // PAGED QUERY_ITER EXECUTE_ITER
    // It's also possible to trace paged queries like query_iter or execute_iter
    // After iterating through all rows iterator.get_tracing_ids() will give tracing ids
    // for all page queries
    let mut row_iterator: RawIterator = session.query_iter(query, &[]).await?;

    while let Some(_row) = row_iterator.next().await {
        // Receive rows
    }

    // Now print tracing ids for all page queries:
    println!(
        "Paged row iterator tracing ids: {:?}\n",
        row_iterator.get_tracing_ids()
    );

    // BATCH
    // Create a simple batch and enable tracing
    let mut batch: Batch = Batch::default();
    batch.append_statement("INSERT INTO ks.tracing_example (val) VALUES('val')");
    batch.set_tracing(true);

    // Run the batch and print its tracing_id
    let batch_result: QueryResult = session.batch(&batch, ((),)).await?;
    println!("Batch tracing id: {:?}\n", batch_result.tracing_id());

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
