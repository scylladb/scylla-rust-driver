# Duration
`Duration` is represented as [`CqlDuration`](https://docs.rs/scylla/latest/scylla/frame/value/struct.CqlDuration.html)\

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use scylla::frame::value::CqlDuration;

// Insert some duration into the table
let to_insert: CqlDuration = CqlDuration { months: 1, days: 2, nanoseconds: 3 };
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read duration from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(CqlDuration,)>()?;
while let Some((duration_value,)) = iter.try_next().await? {
    println!("{:?}", duration_value);
}
# Ok(())
# }
```