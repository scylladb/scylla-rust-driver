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
let my_list: Vec<i32> = vec![1, 2, 3, 4, 5];
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_list,))
    .await?;

// Read a list of ints from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(Vec<i32>,)>()?;
while let Some((list_value,)) = iter.next().transpose()? {
    println!("{:?}", list_value);
}
# Ok(())
# }
```

## Set
`Set` is represented as `Vec<T>`, `HashSet<T>` or `BTreeSet<T>`:

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;

// Insert a set of ints into the table
let my_set: Vec<i32> = vec![1, 2, 3, 4, 5];
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_set,))
    .await?;

// Read a set of ints from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(Vec<i32>,)>()?;
while let Some((list_value,)) = iter.next().transpose()? {
    println!("{:?}", list_value);
}
# Ok(())
# }
```

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::collections::HashSet;

// Insert a set of ints into the table
let my_set: HashSet<i32> = vec![1, 2, 3, 4, 5].into_iter().collect();
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_set,))
    .await?;

// Read a set of ints from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(HashSet<i32>,)>()?;
while let Some((list_value,)) = iter.next().transpose()? {
    println!("{:?}", list_value);
}
# Ok(())
# }
```

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::collections::BTreeSet;

// Insert a set of ints into the table
let my_set: BTreeSet<i32> = vec![1, 2, 3, 4, 5].into_iter().collect();
session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_set,))
    .await?;

// Read a set of ints from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(BTreeSet<i32>,)>()?;
while let Some((list_value,)) = iter.next().transpose()? {
    println!("{:?}", list_value);
}
# Ok(())
# }
```

## Map
`Map` is represented as `HashMap<K, V>` or `BTreeMap<K, V>`

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::collections::HashMap;

// Insert a map of text and int into the table
let mut my_map: HashMap<String, i32> = HashMap::new();
my_map.insert("abcd".to_string(), 16);

session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_map,))
    .await?;

// Read a map from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(HashMap<String, i32>,)>()?;
while let Some((map_value,)) = iter.next().transpose()? {
    println!("{:?}", map_value);
}
# Ok(())
# }
```

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::IntoTypedRows;
use std::collections::BTreeMap;

// Insert a map of text and int into the table
let mut my_map: BTreeMap<String, i32> = BTreeMap::new();
my_map.insert("abcd".to_string(), 16);

session
    .query("INSERT INTO keyspace.table (a) VALUES(?)", (&my_map,))
    .await?;

// Read a map from the table
let result = session.query("SELECT a FROM keyspace.table", &[]).await?;
let mut iter = result.rows::<(BTreeMap<String, i32>,)>()?;
while let Some((map_value,)) = iter.next().transpose()? {
    println!("{:?}", map_value);
}
# Ok(())
# }
```