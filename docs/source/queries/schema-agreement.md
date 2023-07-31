# Schema agreement

Sometimes after performing queries some nodes have not been updated, so we need a mechanism that checks if every node have agreed on schema version.
There is a number of methods in `Session` that assist us.
Every method raise `QueryError` if something goes wrong, but they should never raise any errors, unless there is a DB or connection malfunction.

### Awaiting schema agreement

`Session::await_schema_agreement` returns a `Future` that can be `await`ed as long as schema is not in an agreement.
However, it won't wait forever; `SessionConfig` defines a timeout that limits the time of waiting. If the timeout elapses,
the return value is `Err(QueryError::RequestTimeout)`, otherwise it is `Ok(schema_version)`.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
session.await_schema_agreement().await?;
# Ok(())
# }
```

### Interval of checking for schema agreement

If the schema is not agreed upon, the driver sleeps for a duration before checking it again. The default value is 200 milliseconds,
but it can be changed with `SessionBuilder::schema_agreement_interval`.

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

If you want to check if schema is in agreement now, without retrying after failure, you can use `Session::check_schema_agreement` function.

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
if session.check_schema_agreement().await?.is_some() {
    println!("SCHEMA AGREED");
} else {
    println!("SCHEMA IS NOT IN AGREEMENT");
}
# Ok(())
# }
```
