# Decimal
`Decimal` is represented as [`bigdecimal::BigDecimal`](https://docs.rs/bigdecimal/0.2.0/bigdecimal/struct.BigDecimal.html)

```rust
# extern crate scylla;
# extern crate bigdecimal;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use bigdecimal::BigDecimal;
use std::str::FromStr;

// Insert a decimal into the table
let to_insert: BigDecimal = BigDecimal::from_str("12345.0")?;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a decimal from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(BigDecimal,)>() {
        let (decimal_value,): (BigDecimal,) = row?;
    }
}
# Ok(())
# }
```