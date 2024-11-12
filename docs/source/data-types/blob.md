# Blob
`Blob` is represented as `Vec<u8>`


```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert some blob into the table as a Vec<u8>
// We can insert it by reference to not move the whole blob
let to_insert: Vec<u8> = vec![1, 2, 3, 4, 5];
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (&to_insert,))
    .await?;

// Read blobs from the table
let mut stream = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(Vec<u8>,)>()?;
while let Some((blob_value,)) = stream.try_next().await? {
    println!("{:?}", blob_value);
}
# Ok(())
# }
```