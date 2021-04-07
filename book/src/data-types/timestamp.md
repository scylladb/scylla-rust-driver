# Timestamp
`Timestamp` is represented as [`chrono::Duration`](https://docs.rs/chrono/0.4.19/chrono/struct.Duration.html)

Internally `Timestamp` is represented as `i64` describing number of milliseconds since unix epoch.
Driver converts this to `chrono::Duration`

When sending in a query it needs to be wrapped in `value::Timestamp` to differentiate from [`Time`](time.md)

```rust
# extern crate scylla;
# extern crate chrono;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::Timestamp;
use chrono::Duration;

// Insert some timestamp into the table
let to_insert: Duration = Duration::seconds(64);
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (Timestamp(to_insert),))
    .await?;

// Read timestamp from the table, no need for a wrapper here
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(Duration,)>() {
        let (timestamp_value,): (Duration,) = row?;
    }
}
# Ok(())
# }
```