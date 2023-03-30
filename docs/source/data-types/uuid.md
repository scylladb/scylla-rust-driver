# Uuid, Timeuuid

`Uuid` and `Timeuuid` are represented as `uuid::Uuid`

```rust
# extern crate scylla;
# extern crate uuid;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use uuid::Uuid;

// Insert some uuid/timeuuid into the table
let to_insert: Uuid = Uuid::parse_str("8e14e760-7fa8-11eb-bc66-000000000001")?;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read uuid/timeuuid from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(Uuid,)>()?;
while let Some((uuid_value,)) = iter.next().transpose()? {
    println!("{:?}", uuid_value);
}
# Ok(())
# }
```