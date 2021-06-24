# Percentile speculative execution

This policy has access to `Metrics` shared with session, and triggers
speculative execution when the request to the current host is above a
given percentile.


### Example
To use this policy in `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use std::{sync::Arc, time::Duration};
use scylla::{
    Session,
    SessionBuilder,
    speculative_execution::PercentileSpeculativeExecutionPolicy,
};

let policy = PercentileSpeculativeExecutionPolicy  {
    max_retry_count: 3,
    percentile: 99.0,
};

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .speculative_execution(Arc::new(policy))
    .build()
    .await?;
# Ok(())
# }
```
