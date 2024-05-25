# Timestamp

Depending on feature flags, three different types can be used to interact with timestamps.

Internally [timestamp](https://docs.scylladb.com/stable/cql/types.html#timestamps) is represented as
[`i64`](https://doc.rust-lang.org/std/primitive.i64.html) describing number of milliseconds since unix epoch.

## CqlTimestamp

Without any extra features enabled, only `frame::value::CqlTimestamp` is available. It's an
[`i64`](https://doc.rust-lang.org/std/primitive.i64.html) wrapper and it matches the internal time representation. It's
the only type that supports full range of values that database accepts.

However, for most use cases other types are more practical. See following sections for `chrono` and `time`.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::frame::value::CqlTimestamp;
use scylla::IntoTypedRows;

// 64 seconds since unix epoch, 1970-01-01 00:01:04
let to_insert = CqlTimestamp(64 * 1000);

// Write timestamp to the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read timestamp from the table
if let Some(rows) = session
    .query("SELECT a FROM keyspace.table", &[])
    .await?
    .rows
{
    for row in rows.into_typed::<(CqlTimestamp,)>() {
        let (timestamp_value,): (CqlTimestamp,) = row?;
    }
}
# Ok(())
# }
```

## chrono::DateTime

If full value range is not required, `chrono` feature can be used to enable support of
[`chrono::DateTime`](https://docs.rs/chrono/0.4/chrono/struct.DateTime.html). All values are expected to be converted
to UTC timezone explicitly, as [timestamp](https://docs.scylladb.com/stable/cql/types.html#timestamps) doesn't store
timezone information. Any precision finer than 1ms will be lost.

```rust
# extern crate chrono;
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use scylla::IntoTypedRows;

// 64.123 seconds since unix epoch, 1970-01-01 00:01:04.123
let to_insert = NaiveDateTime::new(
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
    NaiveTime::from_hms_milli_opt(0, 1, 4, 123).unwrap(),
)
.and_utc();

// Write timestamp to the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read timestamp from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<(DateTime<Utc>,)>()?;
while let Some((timestamp_value,)) = iter.next().transpose()? {
    println!("{:?}", timestamp_value);
}
# Ok(())
# }
```

## time::OffsetDateTime

Alternatively, `time` feature can be used to enable support of
[`time::OffsetDateTime`](https://docs.rs/time/0.3/time/struct.OffsetDateTime.html). As
[timestamp](https://docs.scylladb.com/stable/cql/types.html#timestamps) doesn't support timezone information, time will
be corrected to UTC and timezone info will be erased on write. On read, UTC timestamp is returned. Any precision finer
than 1ms will also be lost.

```rust
# extern crate scylla;
# extern crate time;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time};

// 64.123 seconds since unix epoch, 1970-01-01 00:01:04.123
let to_insert = PrimitiveDateTime::new(
    Date::from_calendar_date(1970, Month::January, 1).unwrap(),
    Time::from_hms_milli(0, 1, 4, 123).unwrap(),
)
.assume_utc();

// Write timestamp to the table
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read timestamp from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows_typed::<(OffsetDateTime,)>()?;
while let Some((timestamp_value,)) = iter.next().transpose()? {
    println!("{:?}", timestamp_value);
}
# Ok(())
# }
```
