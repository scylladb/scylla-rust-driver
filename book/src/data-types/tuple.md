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
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a tuple of int and string from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<((i32, String),)>() {
        let (tuple_value,): ((i32, String),) = row?;
        
        let int_value: i32 = tuple_value.0;
        let string_value: String = tuple_value.1;
    }
}
# Ok(())
# }
```