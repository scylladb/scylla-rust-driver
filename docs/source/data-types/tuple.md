# Tuple

`Tuple` is represented as rust tuples of max 16 elements.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a tuple of int and string into the table
let to_insert: (i32, String) = (1, "abc".to_string());
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a tuple of int and string from the table
let result = session.query_unpaged("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<((i32, String),)>()?;
while let Some((tuple_value,)) = iter.next().transpose()? {
    let int_value: i32 = tuple_value.0;
    let string_value: String = tuple_value.1;
    println!("({}, {})", int_value, string_value);
}
# Ok(())
# }
```
