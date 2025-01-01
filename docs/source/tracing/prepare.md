# Tracing `Session::prepare`

`Session::prepare` prepares a query on all connections. If tracing is enabled for the `Query` to prepare, the resulting `PreparedStatement` will contain `prepare_tracing_ids`. `prepare_tracing_ids` is a list of tracing ids of prepare requests on all connections.

```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::prepared_statement::PreparedStatement;
use scylla::execution::tracing::TracingInfo;
use uuid::Uuid;

// Prepare the query with tracing enabled
let mut to_prepare: Query = Query::new("SELECT a FROM ks.tab");
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
# Ok(())
# }
```
