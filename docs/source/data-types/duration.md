# Duration
`Duration` is represented as [`CqlDuration`](https://docs.rs/scylla/latest/scylla/cql/value/struct.CqlDuration.html)\

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::cql::value::CqlDuration;

// Insert some ip address into the table
let to_insert: CqlDuration = CqlDuration { months: 1, days: 2, nanoseconds: 3 };
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read inet from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(CqlDuration,)>() {
        let (cql_duration,): (CqlDuration,) = row?;
    }
}
# Ok(())
# }
```