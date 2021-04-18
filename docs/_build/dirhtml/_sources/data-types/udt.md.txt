# User defined types
Scylla allows users to define their own data types with named fields.  
The driver supports this by allowing to create a custom struct with derived functionality.

For example let's say `my_type` was created using this query:
```sql
CREATE TYPE ks.my_type (int_val int, text_val text)
```

To use this type in the driver create a matching struct and derive `IntoUserType` and `FromUserType`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::macros::{FromUserType, IntoUserType};
use scylla::cql_to_rust::FromCqlVal;

// Define custom struct that matches User Defined Type created earlier
// wrapping field in Option will gracefully handle null field values
#[derive(Debug, IntoUserType, FromUserType)]
struct MyType {
    int_val: i32,
    text_val: Option<String>,
}

// Now it can be sent and received like any other value

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