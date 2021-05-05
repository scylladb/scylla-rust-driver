# Date

For most use cases `Date` can be represented as 
[`chrono::NaiveDate`](https://docs.rs/chrono/0.4.19/chrono/naive/struct.NaiveDate.html).  
`NaiveDate` supports dates from -262145-1-1 to 262143-12-31.

For dates outside of this range you can use the raw `u32` representation.

### Using `chrono::NaiveDate`:
```rust
# extern crate scylla;
# extern crate chrono;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use chrono::naive::NaiveDate;

// Insert some date into the table
let to_insert: NaiveDate = NaiveDate::from_ymd(2021, 3, 24);
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read NaiveDate from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(NaiveDate,)>() {
        let (date_value,): (NaiveDate,) = row?;
    }
}
# Ok(())
# }
```

### Using raw `u32` representation
Internally `Date` is represented as number of days since -5877641-06-23 i.e. 2^31 days before unix epoch.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::frame::value::Date;
use scylla::frame::response::result::CqlValue;

// Insert date using raw u32 representation
let to_insert: Date = Date(2_u32.pow(31)); // 1970-01-01 
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read raw Date from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows {
        let date_value: u32 = match row.columns[0] {
            Some(CqlValue::Date(date_value)) => date_value,
            _ => panic!("Should be a date!")
        };
    }
}
# Ok(())
# }
```