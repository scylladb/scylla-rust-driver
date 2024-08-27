# Counter
`Counter` is represented as `struct Counter(pub i64)`\
`Counter` can't be inserted, it can only be read or updated.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::Counter;

// Read counter from the table
let result = session.query_unpaged("SELECT c FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<(Counter,)>()?;
while let Some((counter_value,)) = iter.next().transpose()? {
    let counter_int_value: i64 = counter_value.0;
    println!("{}", counter_int_value);
}
# Ok(())
# }
```