# Query timeouts

Query execution time can be limited by setting a request timeout. If a query does not complete
in the given time, then `QueryError::RequestTimeout` is returned by the driver immediately,
so that application logic can continue operating, but the query may still be in progress on the server.

As a side note, if one wishes custom server-side timeouts (i.e. actual interruption of query processing),
one can use a[`USING TIMEOUT <duration>` directive supported in ScyllaDB](https://github.com/scylladb/scylladb/blob/master/docs/cql/cql-extensions.md#using-timeout)
(but not in Cassandra).

Timeout can be set globally (per session) or locally (for given statement).
The default per-session timeout is currently 30s.
It is possible to turn off timeouts completely by providing `None` as timeout when building `Session`.
However, setting per-statement timeout to `None` results in falling back to per-session timeout.

```rust
# extern crate scylla;
# use std::error::Error;
# async fn timeouts() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder, query::Query};
use scylla::execution::ExecutionProfile;
use std::time::Duration;

let uri = std::env::var("SCYLLA_URI")
    .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

let no_timeout_profile_handle = ExecutionProfile::builder()
    .request_timeout(None) // no timeout
    .build()
    .into_handle();

let session: Session = SessionBuilder::new()
    .known_node(uri)
    .default_execution_profile_handle(no_timeout_profile_handle) // no per-session timeout
    .build()
    .await?;

// This query, having no timeout, could block indefinitely if a queried node hangs.
session
    .query("TRUNCATE keyspace.table", ())
    .await?;

let three_sec_timeout_profile_handle = ExecutionProfile::builder()
    .request_timeout(Some(Duration::from_secs(3))) // no timeout
    .build()
    .into_handle();

// The below query will last for no more than 3 seconds, yielding a RequestTimeout error
// if no response arrives until then.
let mut query: Query = "TRUNCATE keyspace.table".into();
query.set_execution_profile_handle(Some(three_sec_timeout_profile_handle));
session
    .query(query, ())
    .await?;

#    Ok(())
# }
```
