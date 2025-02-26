# Tracing a simple/prepared/batch query

[Simple query](../queries/simple.md), [prepared query](../queries/prepared.md) and [batch query](../queries/batch.md)
return a `QueryResult` which contains a `tracing_id` if tracing was enabled.

### Tracing a simple query
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::query::Query;
use scylla::response::query_result::QueryResult;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Create a Query manually and enable tracing
let mut query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(4)");
query.set_tracing(true);

let res: QueryResult = session.query_unpaged(query, &[]).await?;
let tracing_id: Option<Uuid> = res.tracing_id();

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
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::prepared_statement::PreparedStatement;
use scylla::response::query_result::QueryResult;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Prepare the query
let mut prepared: PreparedStatement = session
    .prepare("SELECT a FROM ks.tab")
    .await?;

// Enable tracing for the prepared query
prepared.set_tracing(true);

let res: QueryResult = session.execute_unpaged(&prepared, &[]).await?;
let tracing_id: Option<Uuid> = res.tracing_id();

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```

### Tracing a batch query
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::batch::Batch;
use scylla::response::query_result::QueryResult;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Create a batch statement
let mut batch: Batch = Default::default();
batch.append_statement("INSERT INTO ks.tab (a) VALUES(4)");

// Enable tracing
batch.set_tracing(true);

let res: QueryResult = session.batch(&batch, ((),)).await?;
let tracing_id: Option<Uuid> = res.tracing_id();

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```
