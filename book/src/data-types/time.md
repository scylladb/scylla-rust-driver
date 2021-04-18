# Time
`Time` is represented as [`chrono::Duration`](https://docs.rs/chrono/0.4.19/chrono/struct.Duration.html)

Internally `Time` is represented as number of nanoseconds since midnight. 
It can't be negative or exceed `86399999999999` (24 hours).

When sending in a query it needs to be wrapped in `value::Time` to differentiate from [`Timestamp`](timestamp.md)

```rust
# extern crate scylla;
# extern crate chrono;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::Time;
use chrono::Duration;

// Insert some time into the table
let to_insert: Duration = Duration::seconds(64);
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (Time(to_insert),))
    .await?;

// Read time from the table, no need for a wrapper here
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(Duration,)>() {
        let (time_value,): (Duration,) = row?;
    }
}
# Ok(())
# }
```