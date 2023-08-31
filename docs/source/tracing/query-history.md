# Query Execution History

The driver allows to collect history of query execution.\
This history includes all requests sent, decisions to retry and speculative execution fibers started.

## Example code

```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::execution::history::{HistoryCollector, StructuredHistory};
use std::sync::Arc;

// Create a query for which we would like to trace the history of its execution
let mut query: Query = Query::new("SELECT * FROM ks.t");

// Create a history collector and pass it to the query
let history_listener = Arc::new(HistoryCollector::new());
query.set_history_listener(history_listener.clone());

// Run the query, doesn't matter if it failed, the history will still be saved
let _ignore_error = session.query(query.clone(), ()).await;

// Access the collected history and print it
let structured_history: StructuredHistory = history_listener.clone_structured_history();
println!("Query history: {}", structured_history);
# Ok(())
# }
```
To see more check out the [example code](https://github.com/scylladb/scylla-rust-driver/blob/main/examples/query_history.rs)

## Output

Sample output for a query that didn't encounter any difficulties:
```none
=== Query #0 ===
| start_time: 2022-08-25 11:21:50.445075147 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.1:9042
|   request send time: 2022-08-25 11:21:50.445151628 UTC
|   Success at 2022-08-25 11:21:50.447444362 UTC
|
| Query successful at 2022-08-25 11:21:50.447447970 UTC
=================
```

Here's output for a query that had some trouble - nodes didn't respond and speculative execution decided to query others in parallel.
Finally the third node provided a response.
```none
=== Query #0 ===
| start_time: 2022-08-26 15:08:28.525367409 UTC
| Non-speculative attempts:
| - Attempt #0 sent to 127.0.0.219:9042
|   request send time: 2022-08-26 15:08:28.525409294 UTC
|   No result yet
|
|
| > Speculative fiber #0
| fiber start time: 2022-08-26 15:08:28.537074167 UTC
| - Attempt #0 sent to 127.0.0.217:9042
|   request send time: 2022-08-26 15:08:28.537126083 UTC
|   No result yet
|
|
| > Speculative fiber #1
| fiber start time: 2022-08-26 15:08:28.548050242 UTC
| - Attempt #0 sent to 127.0.0.218:9042
|   request send time: 2022-08-26 15:08:28.548089083 UTC
|   Success at 2022-08-26 15:08:28.590052778 UTC
|
| Query successful at 2022-08-26 15:08:28.590078119 UTC
=================
```

## How the driver executes queries

To read the output it's useful to understand more about how the driver executes queries.

### No speculative execution
Without speculative execution the driver performs many attempts sequentially until one of them succeeds.
A single attempt consists of sending a request to some node and waiting for the answer.
In case of an error the driver consults the retry policy to decide what to do next.
The decision might be to fail the query, retry on the same node, another node, change query parameters, etc.
Once the decision is made either the query fails or another attempt is started. This continues until the query ends.

### Speculative execution
When speculative execution is enabled at first the driver doesn't care about it - it does the attempts sequentially and tries to get an answer.
However once a specified amount of time has passed it will decide to try new attempts in parallel
hoping that another node will be able to answer quicker.
This is done by spawning a speculative fiber. Each spawned fiber performs sequential attempts just like in non-speculative execution.
Many fibers can be spawned if the answer wasn't acquired in time.

### StructuredHistory
[`StructuredHistory`](https://docs.rs/scylla/latest/scylla/history/struct.StructuredHistory.html)
is a history representation that represents the history by listing attempts for each speculative fiber.

## HistoryListener trait, custom history collecting

History can be collected by any struct implementing the
[`HistoryListener`](https://docs.rs/scylla/latest/scylla/history/trait.HistoryListener.html) trait.

The implementation of `HistoryListener` provided by this crate is the
[`HistoryCollector`](https://docs.rs/scylla/latest/scylla/history/struct.HistoryCollector.html).
`HistoryCollector` simply collects all events along with their timestamps.

Information collected by `HistoryCollector` is just a stream of events, in order to analyze it it's possible
to convert it to a structured representation.
[`StructuredHistory`](https://docs.rs/scylla/latest/scylla/history/struct.StructuredHistory.html)
can be created by calling `HistoryCollector::clone_structured_history()`.
