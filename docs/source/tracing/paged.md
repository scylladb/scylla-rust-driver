# Tracing a paged query

A paged query performs multiple simple/prepared queries to query subsequent pages.  
If tracing is enabled the row iterator will contain a list of tracing ids for all performed queries.


### Tracing `Session::query_iter`
```rust
# extern crate scylla;
# extern crate uuid;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::transport::iterator::RowIterator;
use scylla::tracing::TracingInfo;
use futures::StreamExt;
use uuid::Uuid;

// Create a Query manually and enable tracing
let mut query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(4)".to_string());
query.set_tracing(true);

// Create a paged query iterator and fetch pages
let mut row_iterator: RowIterator = session.query_iter(query, &[]).await?;
while let Some(_row) = row_iterator.next().await {
    // Receive rows
}

// Now there are tracing ids for each performed query
let tracing_ids: &[Uuid] = row_iterator.get_tracing_ids();

for id in tracing_ids {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```

### Tracing `Session::execute_iter`
```rust
# extern crate scylla;
# extern crate uuid;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::iterator::RowIterator;
use scylla::tracing::TracingInfo;
use futures::StreamExt;
use uuid::Uuid;

// Prepare the query
let mut prepared: PreparedStatement = session
    .prepare("SELECT a FROM ks.tab")
    .await?;

// Enable tracing for the prepared query
prepared.set_tracing(true);

// Create a paged query iterator and fetch pages
let mut row_iterator: RowIterator = session.execute_iter(prepared, &[]).await?;
while let Some(_row) = row_iterator.next().await {
    // Receive rows
}

// Now there are tracing ids for each performed query
let tracing_ids: &[Uuid] = row_iterator.get_tracing_ids();

for id in tracing_ids {
    // Query tracing info from system_traces.sessions and system_traces.events
    let tracing_info: TracingInfo = session.get_tracing_info(id).await?;
    println!("tracing_info: {:#?}", tracing_info);
}
# Ok(())
# }
```
