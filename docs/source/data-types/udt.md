# User defined types
Scylla allows users to define their own data types with named fields (See [the official documentation](https://opensource.docs.scylladb.com/stable/cql/types.html#user-defined-types))\
To use user defined types in the driver, you can create a corresponding struct in Rust, and use it to read and write UDT values.


For example let's say `my_type` was created using this query:
```sql
CREATE TYPE ks.my_type (int_val int, text_val text)
```

To use this type in the driver, create a matching struct and derive `IntoUserType` and `FromUserType`:

```rust
# extern crate scylla;
# async fn check_only_compiles() {
use scylla::macros::{FromUserType, IntoUserType};

// Define a custom struct that matches the User Defined Type created earlier.
// Fields must be in the same order as they are in the database.
// Wrapping a field in Option will gracefully handle null field values.
#[derive(Debug, IntoUserType, FromUserType)]
struct MyType {
    int_val: i32,
    text_val: Option<String>,
}
# }
```

> ***Important***\
> Fields in the Rust struct must be defined in the same order as they are in the database.
> When sending and receiving values, the driver will (de)serialize fields one after another, without looking at field names.

Now it can be sent and received just like any other CQL value:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::macros::{FromUserType, IntoUserType, SerializeCql};
use scylla::cql_to_rust::FromCqlVal;

#[derive(Debug, IntoUserType, FromUserType, SerializeCql)]
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
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read MyType from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(MyType,)>() {
        let (my_type_value,): (MyType,) = row?;
    }
}
# Ok(())
# }
```