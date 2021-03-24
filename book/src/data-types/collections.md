# List, Set, Map

## List
`List` is represented as `Vec<T>`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a list of ints into the table
let to_insert: Vec<i32> = vec![1, 2, 3, 4, 5];
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&to_insert,))
    .await?;

// Read a list of ints from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(Vec<i32>,)>() {
        let (list_value,): (Vec<i32>,) = row?;
    }
}
# Ok(())
# }
```

## Set
`Set` is represented as `Vec<T>`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a set of ints into the table
let to_insert: Vec<i32> = vec![1, 2, 3, 4, 5];
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&to_insert,))
    .await?;

// Read a set of ints from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(Vec<i32>,)>() {
        let (set_value,): (Vec<i32>,) = row?;
    }
}
# Ok(())
# }
```

## Map
`Map` is represented as `std::collections::HashMap<K, V>`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::collections::HashMap;

// Insert a map of text and int into the table
let mut to_insert: HashMap<String, i32> = HashMap::new();
to_insert.insert("abcd".to_string(), 16);

session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&to_insert,))
    .await?;

// Read a map from the table
if let Some(rows) = session.query("SELECT a FROM keyspace.table", &[]).await?.rows {
    for row in rows.into_typed::<(HashMap<String, i32>,)>() {
        let (map_value,): (HashMap<String, i32>,) = row?;
    }
}
# Ok(())
# }
```