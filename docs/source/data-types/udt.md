# User defined types
Scylla allows users to define their own data types with named fields (See [the official documentation](https://opensource.docs.scylladb.com/stable/cql/types.html#user-defined-types))\
To use user defined types in the driver, you can create a corresponding struct in Rust, and use it to read and write UDT values.


For example let's say `my_type` was created using this query:
```sql
CREATE TYPE ks.my_type (int_val int, text_val text)
```

To use this type in the driver, create a matching struct and derive:
- `SerializeValue`: in order to be able to use this struct in query parameters. \
    This macro requires fields of UDT and struct to have matching names, but the order
    of the fields is not required to be the same. \
    Note: you can use different name using `rename` attribute - see `SerializeValue` macro documentation.
- `FromUserType`:  in order to be able to use this struct in query results. \
    This macro requires fields of UDT and struct to be in the same *ORDER*. \
    This mismatch between `SerializeValue` and `FromUserType` requirements is a temporary situation - in the future `FromUserType` (or  the macro that replaces it) will also require matching names.

```rust
# extern crate scylla;
# async fn check_only_compiles() {
use scylla::macros::{FromUserType, SerializeValue};

// Define a custom struct that matches the User Defined Type created earlier.
// Fields must be in the same order as they are in the database and also
// have the same names.
// Wrapping a field in Option will gracefully handle null field values.
#[derive(Debug, FromUserType, SerializeValue)]
struct MyType {
    int_val: i32,
    text_val: Option<String>,
}
# }
```

> ***Important***\
> For deserialization, fields in the Rust struct must be defined in the same order as they are in the database.
> When receiving values, the driver will (de)serialize fields one after another, without looking at field names.

> ***Important***\
> For serialization, by default fields in the Rust struct must be defined with the same names as they are in the database.
> The driver will serialize the fields in the order defined by the UDT, matching Rust fields by name.
> You can change this behaviour using macro attributes, see `SerializeValue` macro documentation for more information.

Now it can be sent and received just like any other CQL value:
```rust
# extern crate scylla;
# extern crate futures;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use scylla::macros::{FromUserType, SerializeValue};
use scylla::cql_to_rust::FromCqlVal;

#[derive(Debug, FromUserType, SerializeValue)]
struct MyType {
    int_val: i32,
    text_val: Option<String>,
}

// Insert my_type into the table
let to_insert = MyType {
    int_val: 17,
    text_val: Some("Some string".to_string()),
};

session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read MyType from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .into_typed::<(MyType,)>();
while let Some((my_type_value,)) = iter.try_next().await? {
    println!("{:?}", my_type_value);
}
# Ok(())
# }
```