# Duration
`Duration` is represented as [`CqlDuration`](https://docs.rs/scylla/latest/scylla/frame/value/struct.CqlDuration.html)\

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::CqlDuration;

// Insert some duration into the table
let to_insert: CqlDuration = CqlDuration { months: 1, days: 2, nanoseconds: 3 };
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read duration from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(CqlDuration,)>()?;
while let Some((duration_value,)) = iter.next().transpose()? {
    println!("{:?}", duration_value);
}
# Ok(())
# }
```