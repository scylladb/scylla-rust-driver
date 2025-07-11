# Ascii, Text, Varchar
`Ascii`, `Text` and `Varchar` are represented as any of: `&str`, `String`, `Box<str>`, `Arc<str>`.

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert some text into the table as a &str
let to_insert_str: &str = "abcdef";
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert_str,))
    .await?;

// Insert some text into the table as a String
let to_insert_string: String = "abcdef".to_string();
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert_string,))
    .await?;

// Read ascii/text/varchar from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(String,)>()?;
while let Some((text_value,)) = iter.try_next().await? {
    println!("{}", text_value);
}
# Ok(())
# }
```
