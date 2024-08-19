# Ascii, Text, Varchar
`Ascii`, `Text` and `Varchar` are represented as `&str` and `String`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

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
let result = session.query_unpaged("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<(String,)>()?;
while let Some((text_value,)) = iter.next().transpose()? {
    println!("{}", text_value);
}
# Ok(())
# }
```