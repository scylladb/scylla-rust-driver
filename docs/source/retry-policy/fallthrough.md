# Fallthrough retry policy

The `FalthroughRetryPolicy` never retries, returns errors straight to the user. Useful for debugging.

### Examples
To use in `Session`:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# use std::sync::Arc;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::retry::FallthroughRetryPolicy;

let handle = ExecutionProfile::builder()
    .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
    .build()
    .into_handle();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .default_execution_profile_handle(handle)
    .build()
    .await?;
# Ok(())
# }
```

To use in an [unprepared statement](../statements/unprepared.md):
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# use std::sync::Arc;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::unprepared::Statement;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::retry::FallthroughRetryPolicy;

let handle = ExecutionProfile::builder()
    .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
    .build()
    .into_handle();

// Create a Statement manually and set the retry policy
let mut my_statement: Statement = Statement::new("INSERT INTO ks.tab (a) VALUES(?)");
my_statement.set_execution_profile_handle(Some(handle));

// Execute the statement using this retry policy
let to_insert: i32 = 12345;
session.query_unpaged(my_statement, (to_insert,)).await?;
# Ok(())
# }
```

To use in a [prepared statement](../statements/prepared.md):
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# use std::error::Error;
# use std::sync::Arc;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use scylla::statement::prepared::PreparedStatement;
use scylla::client::execution_profile::ExecutionProfile;
use scylla::policies::retry::FallthroughRetryPolicy;

let handle = ExecutionProfile::builder()
    .retry_policy(Arc::new(FallthroughRetryPolicy::new()))
    .build()
    .into_handle();

// Create PreparedStatement manually and set the retry policy
let mut prepared: PreparedStatement = session
    .prepare("INSERT INTO ks.tab (a) VALUES(?)")
    .await?;

prepared.set_execution_profile_handle(Some(handle));

// Run the query using this retry policy
let to_insert: i32 = 12345;
session.execute_unpaged(&prepared, (to_insert,)).await?;
# Ok(())
# }
```
