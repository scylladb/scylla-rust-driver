# Tracing a simple/prepared/batch query

[Unprepared statement](../statements/unprepared.md), [prepared statement](../statements/prepared.md) and [batch statement](../statements/batch.md)
execution return a `QueryResult` which contains a `tracing_id` if tracing was enabled.

### Tracing an unprepared statement execution
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::unprepared::Statement;
use scylla::response::query_result::QueryResult;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Create a Statement manually and enable tracing
let mut statement: Statement = Statement::new("INSERT INTO ks.tab (a) VALUES(4)");
statement.set_tracing(true);

let res: QueryResult = session.query_unpaged(statement, &[]).await?;
let tracing_id: Option<Uuid> = res.tracing_id();

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```

### Tracing a prepared statement
```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::prepared::PreparedStatement;
use scylla::response::query_result::QueryResult;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Prepare the statement
let mut prepared: PreparedStatement = session
    .prepare("SELECT a FROM ks.tab")
    .await?;

// Enable tracing for the prepared statement
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

### Tracing a batch statement
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
