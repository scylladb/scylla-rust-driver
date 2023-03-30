# Date

For most use cases `Date` can be represented as
[`chrono::NaiveDate`](https://docs.rs/chrono/0.4.19/chrono/naive/struct.NaiveDate.html).\
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
let to_insert: NaiveDate = NaiveDate::from_ymd_opt(2021, 3, 24).unwrap();
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read NaiveDate from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(NaiveDate,)>()?;
while let Some((date_value,)) = iter.next().transpose()? {
    println!("{:?}", date_value);
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
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(Date,)>()?;
while let Some((date_value,)) = iter.next().transpose()? {
    println!("{:?}", date_value);
}
# Ok(())
# }
```
