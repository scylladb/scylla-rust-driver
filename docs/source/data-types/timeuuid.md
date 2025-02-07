# Timeuuid

The `Timeuuid` type is represented as `value::CqlTimeuuid`.

Also, `value::CqlTimeuuid` is a wrapper for `uuid::Uuid` with custom ordering logic which follows Scylla/Cassandra semantics.

```rust
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# use std::str::FromStr;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use futures::TryStreamExt;
use scylla::value::CqlTimeuuid;

// Insert some timeuuid into the table
let to_insert: CqlTimeuuid = CqlTimeuuid::from_str("8e14e760-7fa8-11eb-bc66-000000000001")?;

session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))
    .await?;

// Read Timeuuid from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(CqlTimeuuid, )>()?;

while let Some((timeuuid,)) = iter.try_next().await? {
    println!("Read a value from row: {}", timeuuid);
}
# Ok(())
# }
```

## Creating your own Timeuuid

To create your own `Timeuuid` objects from timestamp-based `uuid` v1, you need to enable the feature `v1` of `uuid` crate using:

```shell
cargo add uuid -F v1
```

and now you're gonna be able to use the `uuid::v1` features: 

```rust
# extern crate uuid;
# extern crate scylla;
# extern crate futures;
# use scylla::client::session::Session;
# use std::error::Error;
# use std::str::FromStr;
use futures::TryStreamExt;
use scylla::value::CqlTimeuuid;
use uuid::Uuid;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {

// Tip: you can use random stable numbers or your MAC Address
let node_id = [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC];

// Build your Timeuuid with the current timestamp
let to_insert = CqlTimeuuid::from(Uuid::now_v1(&node_id));

session
    .query_unpaged("INSERT INTO keyspace.table (a) VALUES(?)", (to_insert,))  
    .await?;

// Read Timeuuid from the table
let mut iter = session.query_iter("SELECT a FROM keyspace.table", &[])
    .await?
    .rows_stream::<(CqlTimeuuid, )>()?;

while let Some((timeuuid,)) = iter.try_next().await? {
    println!("Read a value from row: {}", timeuuid);
}
# Ok(())
# }
```

Learn more about UUID::v1 [here](https://en.wikipedia.org/wiki/Universally_unique_identifier#Version_1_(date-time_and_MAC_address)).