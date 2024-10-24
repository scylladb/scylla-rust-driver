# Counter
`Counter` is represented as `struct Counter(pub i64)`\
`Counter` can't be inserted, it can only be read or updated.

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use scylla::frame::value::Counter;

// Add to counter value
let to_add: Counter = Counter(100);
session
    .query_unpaged("UPDATE keyspace.table SET c = c + ? WHERE pk = 15", (to_add,))
    .await?;

// Read counter from the table
let mut iter = session.query_iter("SELECT c FROM keyspace.table", &[])
    .await?
    .into_typed::<(Counter,)>();
while let Some((counter_value,)) = iter.try_next().await? {
    let counter_int_value: i64 = counter_value.0;
    println!("{}", counter_int_value);
}
# Ok(())
# }
```