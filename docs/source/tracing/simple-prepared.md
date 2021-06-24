# Tracing a simple/prepared query

Both [simple query](../queries/simple.md) and [prepared query](../queries/prepared.md)
return a `QueryResult` which contains a `tracing_id` if tracing was enabled.

### Tracing a simple query
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::QueryResult;
use scylla::tracing::TracingInfo;
use uuid::Uuid;

// Create a Query manually and enable tracing
let mut query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(4)".to_string());
query.set_tracing(true);

let res: QueryResult = session.query(query, &[]).await?;
let tracing_id: Option<Uuid> = res.tracing_id;

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```

### Tracing a prepared query
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;
use scylla::QueryResult;
use scylla::tracing::TracingInfo;
use uuid::Uuid;

// Prepare the query
let mut prepared: PreparedStatement = session
    .prepare("SELECT a FROM ks.tab")
    .await?;

// Enable tracing for the prepared query
prepared.set_tracing(true);

let res: QueryResult = session.execute(&prepared, &[]).await?;
let tracing_id: Option<Uuid> = res.tracing_id;

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```
