# Query result

`Session::query` and `Session::execute` return a `QueryResult` with rows represented as `Option<Vec<Row>>`.

### Basic representation
`Row` is a basic representation of a received row. It can be used by itself, but it's a bit awkward to use:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
if let Some(rows) = session.query("SELECT a from ks.tab", &[]).await?.rows {
    for row in rows {
        let int_value: i32 = row.columns[0].as_ref().unwrap().as_int().unwrap();
    }
}
# Ok(())
# }
```

### Parsing using `into_typed`
The driver provides a way to parse a row as a tuple of Rust types:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Parse row as a single column containing an int value
if let Some(rows) = session.query("SELECT a from ks.tab", &[]).await?.rows {
    for row in rows {
        let (int_value,): (i32,) = row.into_typed::<(i32,)>()?;
    }
}

// rows.into_typed() converts a Vec of Rows to an iterator of parsing results
if let Some(rows) = session.query("SELECT a from ks.tab", &[]).await?.rows {
    for row in rows.into_typed::<(i32,)>() {
        let (int_value,): (i32,) = row?;
    }
}

// Parse row as two columns containing an int and text columns
if let Some(rows) = session.query("SELECT a, b from ks.tab", &[]).await?.rows {
    for row in rows.into_typed::<(i32, String)>() {
        let (int_value, text_value): (i32, String) = row?;
    }
}
# Ok(())
# }
```

### `NULL` values
`NULL` values will return an error when parsed as a Rust type. 
To properly handle `NULL` values parse column as an `Option<>`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Parse row as two columns containing an int and text which might be null
if let Some(rows) = session.query("SELECT a, b from ks.tab", &[]).await?.rows {
    for row in rows.into_typed::<(i32, Option<String>)>() {
        let (int_value, str_or_null): (i32, Option<String>) = row?;
    }
}
# Ok(())
# }
```

### Parsing row as a custom struct
It is possible to receive row as a struct with fields matching the columns.  
The struct must:
* have the same number of fields as the number of queried columns
* have field types matching the columns being received
* derive `FromRow`

Field names don't need to match column names.
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use scylla::macros::FromRow;
use scylla::frame::response::cql_to_rust::FromRow;

#[derive(FromRow)]
struct MyRow {
    age: i32,
    name: Option<String>
}

// Parse row as two columns containing an int and text which might be null
if let Some(rows) = session.query("SELECT a, b from ks.tab", &[]).await?.rows {
    for row in rows.into_typed::<MyRow>() {
        let my_row: MyRow = row?;
    }
}
# Ok(())
# }
```

### Other data types
For parsing other data types see [Data Types](../data-types/data-types.md)