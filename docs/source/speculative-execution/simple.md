# Simple speculative execution

The easiest speculative execution policy available.  It starts another
execution of a query after constant delay of `retry_interval` and does at most
`max_retry_count` speculative query executions (not counting the first,
non-speculative one).

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
    speculative_execution::SimpleSpeculativeExecutionPolicy
};

let policy = SimpleSpeculativeExecutionPolicy {
    max_retry_count: 3,
    retry_interval: Duration::from_millis(100),
};

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .speculative_execution(Arc::new(policy))
    .build()
    .await?;
# Ok(())
# }
```
