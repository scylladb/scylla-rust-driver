# Tracing `Session::prepare`

`Session::prepare` prepares a query on all connections. If tracing is enabled for the `Query` to prepare, the resulting `PreparedStatement` will contain `prepare_tracing_ids`. `prepare_tracing_ids` is a list of tracing ids of prepare requests on all connections.

```rust
use scylla::statement::unprepared::Statement;
use scylla::statement::prepared::PreparedStatement;
use scylla::observability::tracing::TracingInfo;
use uuid::Uuid;

// Prepare the statement with tracing enabled
let mut to_prepare: Statement = Statement::new("SELECT a FROM ks.tab");
to_prepare.set_tracing(true);

let mut prepared: PreparedStatement = session
    .prepare(to_prepare)
    .await?;

// Now there are tracing ids for each prepare request
let tracing_ids: &[Uuid] = &prepared.prepare_tracing_ids;

for id in tracing_ids {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
```
