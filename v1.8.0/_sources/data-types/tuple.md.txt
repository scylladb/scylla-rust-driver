# Tuple

`Tuple` is represented as rust tuples of max 16 elements.

```rust
use futures::TryStreamExt;

// Insert a tuple of int and string into the table
let to_insert: (i32, String) = (1, "abc".to_string());
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a tuple of int and string from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<((i32, String),)>()?;
while let Some((tuple_value,)) = iter.try_next().await? {
    let int_value: i32 = tuple_value.0;
    let string_value: String = tuple_value.1;
    println!("({}, {})", int_value, string_value);
}
```
