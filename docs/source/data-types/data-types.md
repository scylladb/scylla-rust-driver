# Data Types

The driver maps database data types to matching Rust types
to achieve seamless sending and receiving of CQL values.

See the following chapters for examples on how to send and receive each data type.

See [Query values](../queries/values.md) for more information about sending values in queries.\
See [Query result](../queries/result.md) for more information about reading values from queries

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
* `Uuid`, `Timeuuid` <----> `uuid::Uuid`
* `Date` <----> `chrono::NaiveDate`, `u32`
* `Time` <----> `chrono::Duration`
* `Timestamp` <----> `chrono::Duration`
* `Decimal` <----> `bigdecimal::Decimal`
* `Varint` <----> `num_bigint::BigInt`
* `List` <----> `Vec<T>`
* `Set` <----> `Vec<T>`
* `Map` <----> `std::collections::HashMap<K, V>`
* `Tuple` <----> Rust tuples
* `UDT (User defined type)` <----> Custom user structs with macros


```eval_rst
.. toctree::
   :hidden:
   :glob:

   primitive
   text
   counter
   blob
   inet
   uuid
   date
   time
   timestamp
   decimal
   varint
   collections
   tuple
   udt

```