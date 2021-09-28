# Blob
`Blob` is represented as `Vec<u8>`


```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert some blob into the table as a Vec<u8>
// We can insert it by reference to not move the whole blob
let to_insert: Vec<u8> = vec![1, 2, 3, 4, 5];
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&to_insert,))
    .await?;

// Read blobs from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(Vec<u8>,)>() {
    let (blob_value,): (Vec<u8>,) = row?;
}
# Ok(())
# }
```