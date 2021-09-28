# Varint
`Varint` is represented as [`num_bigint::BigInt`](https://docs.rs/num-bigint/0.4.0/num_bigint/struct.BigInt.html)

```rust
# extern crate scylla;
# extern crate num_bigint;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use num_bigint::BigInt;
use std::str::FromStr;

// Insert a varint into the table
let to_insert: BigInt = BigInt::from_str("12345")?;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a varint from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(BigInt,)>() {
    let (varint_value,): (BigInt,) = row?;
}
# Ok(())
# }
```