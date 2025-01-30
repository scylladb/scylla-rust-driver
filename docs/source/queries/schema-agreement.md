# Schema agreement

Sometimes after performing queries some nodes have not been updated, so we need a mechanism that checks if every node have agreed on schema version.

### Automated awaiting schema agreement

The driver automatically awaits schema agreement after a schema-altering query is executed.
Waiting for schema agreement more than necessary is never a bug, but might slow down applications which do a lot of schema changes (e.g. a migration).
For instance, in case where somebody wishes to create a keyspace and then a lot of tables in it, it makes sense only to wait after creating a keyspace
and after creating all the tables rather than after every query. Therefore, the said behaviour can be disabled:

```rust
# extern crate scylla;
# use scylla::client::session_builder::SessionBuilder;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
let session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .auto_await_schema_agreement(false)
    .build()
    .await?;
# Ok(())
# }
```

### Manually awaiting schema agreement

`Session::await_schema_agreement` returns a `Future` that can be `await`ed as long as schema is not in an agreement.
However, it won't wait forever; `SessionConfig` defines a timeout that limits the time of waiting. If the timeout elapses,
the return value is `Err(ExecutionError::SchemaAgreementTimeout)`, otherwise it is `Ok(schema_version)`.

```rust
# extern crate scylla;
# use scylla::client::session::Session;
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
# use scylla::client::session_builder::SessionBuilder;
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
# use scylla::client::session::Session;
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
