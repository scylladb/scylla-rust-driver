# Schema agreement

Sometimes after performing queries some nodes have not been updated so we need a mechanism that checks if every node have agreed schema version.
There are four methods in `Session` that assist us. 
Every method raise `QueryError` if something goes wrong, but they should never raise any errors, unless there is a DB or connection malfunction.

### Checking schema version
`Session::fetch_schema_version` returns an `Uuid` of local node's schema version. 

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
println!("Local schema version is: {}", session.fetch_schema_version().await?);
# Ok(())
# }
```

### Awaiting schema agreement

`Session::await_schema_agreement` returns a `Future` that can be `await`ed on as long as schema is not in an agreement.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
session.await_schema_agreement().await?;
# Ok(())
# }
```

### Awaiting with timeout
We can also set timeout in miliseconds with `Session::await_timed_schema_agreement`.
It takes one argument, an `std::time::Duration` value that tells how long our driver should await for schema agreement. If the timeout is met the return value is `false` otherwise it is `true`.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# use std::time::Duration;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
if session.await_timed_schema_agreement(Duration::from_secs(5)).await? { // wait for 5 seconds
    println!("SCHEMA AGREED");
} else {
    println!("SCHEMA IS NOT IN AGREEMENT - TIMED OUT");
}
# Ok(())
# }
```

### Checking for schema interval
If schema is not agreed driver sleeps for a duration before checking it again. Default value is 200 miliseconds but it can be changed with `SessionBuilder::schema_agreement_interval`.


```rust
# extern crate scylla;
# use scylla::SessionBuilder;
# use std::error::Error;
# use std::time::Duration;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .schema_agreement_interval(Duration::from_secs(1))
    .build()
    .await?;
# Ok(())
# }
```

### Checking if schema is in agreement now
If you want to check if schema is in agreement now without retrying after failure you can use `Session::check_schema_agreement` function.


```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
if session.check_schema_agreement().await? { 
    println!("SCHEMA AGREED");
} else {
    println!("SCHEMA IS NOT IN AGREEMENT");
}
# Ok(())
# }
```


