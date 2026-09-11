# Data Types

The driver maps database data types to matching Rust types
to achieve seamless sending and receiving of CQL values.

See the following chapters for examples on how to send and receive each data type. Please note that using some of those types requires enabling respective feature flags. Details are available in chapters for such data types.

See [Statement values](../statements/values.md) for more information about sending values along with statements.

See [Query result](../statements/result.md) for more information about retrieving values from queries.

Database types and their Rust equivalents:
* `Boolean` <----> `bool`
* `Tinyint`  <---->  `i8`
* `Smallint` <----> `i16`
* `Int` <----> `i32`
* `BigInt` <----> `i64`
* `Float` <----> `f32`
* `Double` <----> `f64`
* `Ascii`, `Text`, `Varchar` <----> `&str`, `String`, `Box<str>`, `Arc<str>`
* `Counter` <----> `value::Counter`
* `Blob` <----> `&[u8]`, `Vec<u8>`, `Bytes`, (and `[u8; N]` for serialization only)
* `Inet` <----> `std::net::IpAddr`
* `Uuid` <----> `uuid::Uuid`
* `Timeuuid` <----> `value::CqlTimeuuid`
* `Date` <----> `value::CqlDate`, `chrono::NaiveDate`, `time::Date`
* `Time` <----> `value::CqlTime`, `chrono::NaiveTime`, `time::Time`
* `Timestamp` <----> `value::CqlTimestamp`, `chrono::DateTime<Utc>`, `time::OffsetDateTime`
* `Duration` <----> `value::CqlDuration`
* `Decimal` <----> `value::CqlDecimal`, `value::CqlDecimalBorrowed`, `bigdecimal::BigDecimal`
* `Varint` <----> `value::CqlVarint`, `value::CqlVarintBorrowed`, `num_bigint::BigInt` (v0.3 and v0.4)
* `List` <----> `Vec<T>`
* `Set` <----> `Vec<T>`
* `Map` <----> `std::collections::HashMap<K, V>`
* `Tuple` <----> Rust tuples
* `UDT (User defined type)` <----> Custom user structs with macros
* `Vector` <----> `Vec<T>`

Additionally, `Box`, `Arc`, and `Cow` serialization and deserialization is supported for all above types.

## Using the `value` types with serde

The `value::*` types above are plain data, so they can be embedded in your own
structures that are serialized with [serde](https://serde.rs) - to a cache, a
message queue, a config file, etc. Enabling the `serde` feature of the driver
derives serde's `Serialize` and `Deserialize` for `value::Counter`,
`value::CqlDate`, `value::CqlDecimal`, `value::CqlDuration`, `value::CqlTime`,
`value::CqlTimestamp`, `value::CqlTimeuuid`, `value::CqlVarint` and the borrowed
decimal/varint variants.

Note that this is unrelated to how values are represented on the wire - the CQL
binary format is always used when talking to the database.

The serde representations of these types are **not** covered by the driver's
semver guarantees - they may change between driver versions. Any such change is
announced in the release notes, so if you persist these values, check the release
notes before upgrading.

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
   vector

```
