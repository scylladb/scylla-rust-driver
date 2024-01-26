# Varint
`Varint` is represented as `value::CqlVarint` or [`num_bigint::BigInt`](https://docs.rs/num-bigint/0.4.0/num_bigint/struct.BigInt.html).

## num_bigint::BigInt

To make use of `num_bigint::BigInt` type, user should enable one of the available feature flags (`num-bigint-03` or `num-bigint-04`). They enable support for `num_bigint::BigInt` v0.3 and v0.4 accordingly.

## value::CqlVarint

Without any feature flags, the user can interact with `Varint` type by making use of `value::CqlVarint` which
is a very simple wrapper representing the value as signed binary number in big-endian order.

## Example

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
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(BigInt,)>() {
        let (varint_value,): (BigInt,) = row?;
    }
}
# Ok(())
# }
```