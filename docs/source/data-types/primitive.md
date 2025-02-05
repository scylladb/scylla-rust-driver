# Bool, Tinyint, Smallint, Int, Bigint, Float, Double

### Bool

`Bool` is represented as rust `bool`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a bool into the table
let to_insert: bool = true;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a bool from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(bool,)>()?;
while let Some((bool_value,)) = iter.try_next().await? {
    println!("{:?}", bool_value);
}
# Ok(())
# }
```

### Tinyint

`Tinyint` is represented as rust `i8`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a tinyint into the table
let to_insert: i8 = 123;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a tinyint from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(i8,)>()?;
while let Some((tinyint_value,)) = iter.try_next().await? {
    println!("{:?}", tinyint_value);
}
# Ok(())
# }
```

### Smallint

`Smallint` is represented as rust `i16`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a smallint into the table
let to_insert: i16 = 12345;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a smallint from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(i16,)>()?;
while let Some((smallint_value,)) = iter.try_next().await? {
    println!("{}", smallint_value);
}
# Ok(())
# }
```

### Int

`Int` is represented as rust `i32`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert an int into the table
let to_insert: i32 = 12345;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read an int from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(i32,)>()?;
while let Some((int_value,)) = iter.try_next().await? {
    println!("{}", int_value);
}
# Ok(())
# }
```

### Bigint

`Bigint` is represented as rust `i64`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a bigint into the table
let to_insert: i64 = 12345;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a bigint from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(i64,)>()?;
while let Some((bigint_value,)) = iter.try_next().await? {
    println!("{:?}", bigint_value);
}
# Ok(())
# }
```

### Float

`Float` is represented as rust `f32`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a float into the table
let to_insert: f32 = 123.0;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a float from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(f32,)>()?;
while let Some((float_value,)) = iter.try_next().await? {
    println!("{:?}", float_value);
}
# Ok(())
# }
```

### Double

`Double` is represented as rust `f64`

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;

// Insert a double into the table
let to_insert: f64 = 12345.0;
session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read a double from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(f64,)>()?;
while let Some((double_value,)) = iter.try_next().await? {
    println!("{:?}", double_value);
}
# Ok(())
# }
```
