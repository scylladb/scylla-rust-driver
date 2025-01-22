## Vector
`Vector` is represented as `Vec<T>`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a vector of ints into the table
let my_vector: Vec<i32> = vec![1, 2, 3, 4, 5];
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (&my_vector,))
    .await?;

// Read a list of ints from the table
let mut stream = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(Vec<i32>,)>()?;
while let Some((vector_value,)) = stream.try_next().await? {
    println!("{:?}", vector);
}
# Ok(())
# }