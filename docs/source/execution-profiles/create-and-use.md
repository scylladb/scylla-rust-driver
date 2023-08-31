# Creating a profile and setting it

### Example
To create an `ExecutionProfile` and attach it as default for `Session`:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::statement::Consistency;
use scylla::execution::ExecutionProfile;

let profile = ExecutionProfile::builder()
    .consistency(Consistency::LocalOne)
    .request_timeout(None) // no request timeout
    .build();

let handle = profile.into_handle();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .default_execution_profile_handle(handle)
    .build()
    .await?;
# Ok(())
# }
```

### Example
To create an `ExecutionProfile` and attach it to a `Query`:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::query::Query;
use scylla::statement::Consistency;
use scylla::execution::ExecutionProfile;
use std::time::Duration;

let profile = ExecutionProfile::builder()
    .consistency(Consistency::All)
    .request_timeout(Some(Duration::from_secs(30)))
    .build();

let handle = profile.into_handle();

let mut query1 = Query::from("SELECT * FROM ks.table");
query1.set_execution_profile_handle(Some(handle.clone()));

let mut query2 = Query::from("SELECT pk FROM ks.table WHERE pk = ?");
query2.set_execution_profile_handle(Some(handle));
# Ok(())
# }
```

### Example
To create an `ExecutionProfile` based on another profile:
```rust
# extern crate scylla;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::statement::Consistency;
use scylla::execution::ExecutionProfile;
use std::time::Duration;

let base_profile = ExecutionProfile::builder()
    .request_timeout(Some(Duration::from_secs(30)))
    .build();

let profile = base_profile.to_builder()
    .consistency(Consistency::All)
    .build();

# Ok(())
# }
```