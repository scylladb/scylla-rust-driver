# Token aware Round robin

This policy will try to calculate a token to find replica nodes in which queried data is stored.  
After finding the replicas it performs a round robin on them.

### Example
To use this policy in `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::transport::load_balancing::{RoundRobinPolicy, TokenAwarePolicy};
use std::sync::Arc;

let robin = Box::new(RoundRobinPolicy::new());
let policy = Arc::new(TokenAwarePolicy::new(robin));

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .load_balancing(policy)
    .build()
    .await?;
# Ok(())
# }
```