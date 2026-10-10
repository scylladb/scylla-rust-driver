# Request timeouts

CQL statement execution time can be limited by setting a request timeout. If request does not complete
in the given time, then `ExecutionError::RequestTimeout` is returned by the driver immediately,
so that application logic can continue operating, but the request may still be in progress on the server.

As a side note, if one wishes custom server-side timeouts (i.e. actual interruption of request processing),
one can use a[`USING TIMEOUT <duration>` directive supported in ScyllaDB](https://github.com/scylladb/scylladb/blob/master/docs/cql/cql-extensions.md#using-timeout)
(but not in Cassandra).

Timeout can be set globally (per session) or locally (for given statement).
The default per-session timeout is currently 30s.
It is possible to turn off timeouts completely by providing `None` as timeout when building `Session`.
However, setting per-statement timeout to `None` results in falling back to per-session timeout.

```rust
use scylla::client::execution_profile::ExecutionProfile;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::unprepared::Statement;
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

// This statement, having no timeout, could block indefinitely if a queried node hangs.
session
    .query_unpaged("TRUNCATE keyspace.table", ())
    .await?;

let three_sec_timeout_profile_handle = ExecutionProfile::builder()
    .request_timeout(Some(Duration::from_secs(3))) // no timeout
    .build()
    .into_handle();

// The below statement execution will last for no more than 3 seconds, yielding a RequestTimeout error
// if no response arrives until then.
let mut statement: Statement = "TRUNCATE keyspace.table".into();
statement.set_execution_profile_handle(Some(three_sec_timeout_profile_handle));
session
    .query_unpaged(statement, ())
    .await?;

```
