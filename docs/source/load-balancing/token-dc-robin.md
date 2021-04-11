# Token aware DC Aware Round robin

This policy will try to calculate a token to find replica nodes in which queried data is stored.  
After finding the replicas it chooses the ones from the local datacenter and performs a round robin on them.

### Example
To use this policy in `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::transport::load_balancing::{DcAwareRoundRobinPolicy, TokenAwarePolicy};
use std::sync::Arc;

let local_dc: String = "us_east".to_string();
let dc_robin = Box::new(DcAwareRoundRobinPolicy::new(local_dc));
let policy = Arc::new(TokenAwarePolicy::new(dc_robin));

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .load_balancing(policy)
    .build()
    .await?;
# Ok(())
# }
```