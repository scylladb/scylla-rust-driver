# Tracing a batch query
`Session::batch` returns a `BatchResult` which contains a `tracing_id` if tracing was enabled.

```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::batch::Batch;
use scylla::BatchResult;
use scylla::tracing::TracingInfo;
use uuid::Uuid;

// Create a batch statement
let mut batch: Batch = Default::default();
batch.append_statement("INSERT INTO ks.tab (a) VALUES(4)");

// Enable tracing
batch.set_tracing(true);

let res: BatchResult = session.batch(&batch, ((),)).await?;
let tracing_id: Option<Uuid> = res.tracing_id;

if let Some(id) = tracing_id {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(&id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```
