# Bool, Tinyint, Smallint, Int, Bigint, Float, Double

### Bool
`Bool` is represented as rust `bool`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a bool into the table
let to_insert: bool = true;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a bool from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(bool,)>() {
    let (bool_value,): (bool,) = row?;
}
# Ok(())
# }
```

### Tinyint
`Tinyint` is represented as rust `i8`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a tinyint into the table
let to_insert: i8 = 123;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a tinyint from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(i8,)>() {
    let (tinyint_value,): (i8,) = row?;
}
# Ok(())
# }
```

### Smallint
`Smallint` is represented as rust `i16`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a smallint into the table
let to_insert: i16 = 12345;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a smallint from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(i16,)>() {
    let (smallint_value,): (i16,) = row?;
}
# Ok(())
# }
```

### Int
`Int` is represented as rust `i32`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert an int into the table
let to_insert: i32 = 12345;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read an int from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(i32,)>() {
    let (int_value,): (i32,) = row?;
}
# Ok(())
# }
```

### Bigint
`Bigint` is represented as rust `i64`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a bigint into the table
let to_insert: i64 = 12345;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a bigint from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(i64,)>() {
    let (bigint_value,): (i64,) = row?;
}
# Ok(())
# }
```

### Float 
`Float` is represented as rust `f32`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a float into the table
let to_insert: f32 = 123.0;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a float from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(f32,)>() {
    let (float_value,): (f32,) = row?;
}
# Ok(())
# }
```

### Double
`Double` is represented as rust `f64`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a double into the table
let to_insert: f64 = 12345.0;
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a double from the table
let rows = session.query("SELECT a FROM keyspace.table", &[]).await?.rows()?;
for row in rows.into_typed::<(f64,)>() {
    let (double_value,): (f64,) = row?;
}
# Ok(())
# }
```