# Counter
`Counter` is represented as `struct Counter(pub i64)`  
`Counter` can't be inserted, it can only be read or updated.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::frame::value::Counter;

// Read counter from the table
if let Some(rows) = session.query("SELECT c FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(Counter,)>() {
        let (counter_value,): (Counter,) = row?;
        let counter_int_value: i64 = counter_value.0;
    }
}
# Ok(())
# }
```