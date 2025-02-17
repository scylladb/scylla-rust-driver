# Data Types

The driver maps database data types to matching Rust types
to achieve seamless sending and receiving of CQL values.

See the following chapters for examples on how to send and receive each data type.

See [Statement values](../statements/values.md) for more information about sending values along with statements.\
See [Query result](../statements/result.md) for more information about retrieving values from queries

Database types and their Rust equivalents:
* `Boolean` <----> `bool`
* `Tinyint`  <---->  `i8`
* `Smallint` <----> `i16`
* `Int` <----> `i32`
* `BigInt` <----> `i64`
* `Float` <----> `f32`
* `Double` <----> `f64`
* `Ascii`, `Text`, `Varchar` <----> `&str`, `String`
* `Counter` <----> `value::Counter`
* `Blob` <----> `Vec<u8>`
* `Inet` <----> `std::net::IpAddr`
* `Uuid` <----> `uuid::Uuid`
* `Timeuuid` <----> `value::CqlTimeuuid`
* `Date` <----> `value::CqlDate`, `chrono::NaiveDate`, `time::Date`
* `Time` <----> `value::CqlTime`, `chrono::NaiveTime`, `time::Time`
* `Timestamp` <----> `value::CqlTimestamp`, `chrono::DateTime<Utc>`, `time::OffsetDateTime`
* `Duration` <----> `value::CqlDuration`
* `Decimal` <----> `value::CqlDecimal`, `bigdecimal::Decimal`
* `Varint` <----> `value::CqlVarint`, `num_bigint::BigInt` (v0.3 and v0.4)
* `List` <----> `Vec<T>`
* `Set` <----> `Vec<T>`
* `Map` <----> `std::collections::HashMap<K, V>`
* `Tuple` <----> Rust tuples
* `UDT (User defined type)` <----> Custom user structs with macros


```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   primitive
   text
   counter
   blob
   inet
   uuid
   timeuuid
   date
   time
   timestamp
   duration
   decimal
   varint
   collections
   tuple
   udt

```
