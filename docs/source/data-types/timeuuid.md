# Timeuuid

`Timeuuid` is represented as `value::CqlTimeuuid`.
`value::CqlTimeuuid` is a wrapper  for `uuid::Uuid` with custom ordering logic
which follows Scylla/Cassandra semantics.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# use std::str::FromStr;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::CqlTimeuuid;

// Insert some timeuuid into the table
let to_insert: CqlTimeuuid = CqlTimeuuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001")?;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read timeuuid from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(CqlTimeuuid,)>() {
        let (timeuuid_value,): (CqlTimeuuid,) = row?;
    }
}
# Ok(())
# }
```