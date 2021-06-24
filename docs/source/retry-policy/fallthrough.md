# Fallthrough retry policy

The `FalthroughRetryPolicy` never retries, returns errors straight to the user. Useful for debugging.

### Examples
To use in `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::transport::retry_policy::FallthroughRetryPolicy;

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .retry_policy(Box::new(FallthroughRetryPolicy::new()))
    .build()
    .await?;
# Ok(())
# }
```

To use in a [simple query](../queries/simple.md):
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::transport::retry_policy::FallthroughRetryPolicy;

// Create a Query manually and set the retry policy
let mut my_query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(?)".to_string());
my_query.set_retry_policy(Box::new(FallthroughRetryPolicy::new()));

// Run the query using this retry policy
let to_insert: i32 = 12345;
session.query(my_query, (to_insert,)).await?;
# Ok(())
# }
```

To use in a [prepared query](../queries/prepared.md):
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::prepared_statement::PreparedStatement;
use scylla::transport::retry_policy::FallthroughRetryPolicy;

// Create PreparedStatement manually and set the retry policy
let mut prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

prepared.set_retry_policy(Box::new(FallthroughRetryPolicy::new()));

// Run the query using this retry policy
let to_insert: i32 = 12345;
session.execute(&prepared, (to_insert,)).await?;
# Ok(())
# }
```
