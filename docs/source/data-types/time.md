# Time

Depending on feature flags used, three different types can be used to interact with time.

Internally [time](https://docs.scylladb.com/stable/cql/types.html#times) is represented as number of nanoseconds since
midnight. It can't be negative or exceed `86399999999999` (23:59:59.999999999).

## CqlTime

Without any extra features enabled, only `frame::value::CqlTime` is available. It's an
[`i64`](https://doc.rust-lang.org/std/primitive.i64.html) wrapper and it matches the internal time representation.

However, for most use cases other types are more practical. See following sections for `chrono` and `time`.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::frame::value::CqlTime;
use scylla::IntoTypedRows;

// 64 seconds since midnight
let to_insert = CqlTime(64 * 1_000_000_000);

// Insert time into the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read time from the table
if let Some(rows) = session
    .query("SELECT a FROM keyspace.table", &[])
    .await?
    .rows
{
    for row in rows.into_typed::<(CqlTime,)>() {
        let (time_value,): (CqlTime,) = row?;
    }
}
# Ok(())
# }
```

## chrono::NaiveTime

If `chrono` feature is enabled, [`chrono::NaiveTime`](https://docs.rs/chrono/0.4/chrono/naive/struct.NaiveDate.html)
can be used to interact with the database. Although chrono can represent leap seconds, they are not supported.
Attempts to convert [`chrono::NaiveTime`](https://docs.rs/chrono/0.4/chrono/naive/struct.NaiveDate.html) with leap
second to `CqlTime` or write it to the database will return an error.

```rust
# extern crate chrono;
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use chrono::NaiveTime;
use scylla::IntoTypedRows;

// 01:02:03.456,789,012
let to_insert = NaiveTime::from_hms_nano_opt(1, 2, 3, 456_789_012);

// Insert time into the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read time from the table
if let Some(rows) = session
    .query("SELECT a FROM keyspace.table", &[])
    .await?
    .rows
{
    for row in rows.into_typed::<(NaiveTime,)>() {
        let (time_value,): (NaiveTime,) = row?;
    }
}
# Ok(())
# }
```

## time::Time

If `time` feature is enabled, [`time::Time`](https://docs.rs/time/0.3/time/struct.Time.html) can be used to interact
with the database.

```rust
# extern crate scylla;
# extern crate time;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use time::Time;

// 01:02:03.456,789,012
let to_insert = Time::from_hms_nano(1, 2, 3, 456_789_012).unwrap();

// Insert time into the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read time from the table
if let Some(rows) = session
    .query("SELECT a FROM keyspace.table", &[])
    .await?
    .rows
{
    for row in rows.into_typed::<(Time,)>() {
        let (time_value,): (Time,) = row?;
    }
}
# Ok(())
# }
```
