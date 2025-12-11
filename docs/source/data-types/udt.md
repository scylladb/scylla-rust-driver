# User defined types
ScyllaDB allows users to define their own data types with named fields (See [the official documentation](https://opensource.docs.scylladb.com/stable/cql/types.html#user-defined-types))\
To use user defined types in the driver, you can create a corresponding struct in Rust, and use it to read and write UDT values.


For example let's say `my_type` was created using this query:
```sql
CREATE TYPE ks.my_type (int_val int, text_val text)
```

To use this type in the driver, create a matching struct and derive:
- `SerializeValue`: in order to be able to use this struct in query parameters. \
- `DeserializeValue`: in order to be able to use this struct in query results. \

Both macros require fields of UDT and struct to have matching names, but the order
of the fields is not required to be the same. \
Note: you can use different name using `rename` attribute - see `SerializeValue`
and `DeserializeValue` macros documentation.

```rust
# extern crate scylla;
# async fn check_only_compiles() {
use scylla::{DeserializeValue, SerializeValue};

// Define a custom struct that matches the User Defined Type created earlier.
// Fields don't have to be in the same order as they are in the database.
// By default, they must have the same names, but this can be worked around
// using `#[rename] field attribute.
// Wrapping a field in Option will gracefully handle null field values.
#[derive(Debug, DeserializeValue, SerializeValue)]
struct MyType {
    int_val: i32,
    text_val: Option<String>,
}
# }
```

> ***Important***\
> For (de)serialization, by default fields in the Rust struct must be defined with the same names as they are in the database.
> The driver will (de)serialize the fields in the order defined by the UDT, matching Rust fields by name.
> You can change this behaviour using macro attributes, see `SerializeValue`/`DeserializeValue` macro documentation for more information.

Now it can be sent and received just like any other CQL value:
```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use scylla::{DeserializeValue, SerializeValue};

#[derive(Debug, DeserializeValue, SerializeValue)]
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
    .rows_stream::<(MyType,)>()?;
while let Some((my_type_value,)) = iter.try_next().await? {
    println!("{:?}", my_type_value);
}
# Ok(())
# }
```

## Transparent Wrappers (Newtype Pattern)

Sometimes it is desirable to wrap a driver's type (like `i32` or `String`) in a custom struct for type safety, a pattern often called the "newtype pattern".
To avoid writing manual implementations of `SerializeValue` and `DeserializeValue`, you can use the `#[scylla(transparent)]` attribute.

This attribute delegates the serialization, deserialization, and type checking logic to the single field contained within the struct or enum. The wrapper will be stored in the database exactly as the inner type is stored (e.g., as an `Int` or `Text` column), not as a UDT (unless the inner type is a UDT, of course).

**Requirements:**
* The struct must have exactly one field.
* The enum must have exactly one variant with exactly one field.

```rust
# extern crate scylla;
# async fn check_only_compiles() {
use scylla::{DeserializeValue, SerializeValue};

// This struct will be serialized as a simple Int (4 bytes),
// strictly matching the inner `i32` type.
#[derive(Debug, SerializeValue, DeserializeValue)]
#[scylla(transparent)]
struct UserId(i32);

// Works with named fields too
#[derive(Debug, SerializeValue, DeserializeValue)]
#[scylla(transparent)]
struct OrderId {
    val: i64,
}

// This will be serialized as a CQL list<text>
#[derive(Debug, SerializeValue, DeserializeValue)]
#[scylla(transparent)]
struct Tags(Vec<String>);

// A standard UDT
#[derive(Debug, SerializeValue, DeserializeValue)]
struct Address {
    street: String,
    city: String,
}

// A transparent wrapper around the UDT.
// This matches the same CQL UDT layout as `Address`.
#[derive(Debug, SerializeValue, DeserializeValue)]
#[scylla(transparent)]
struct HomeAddress(Address);

# }
```