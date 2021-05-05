# DC Aware Round robin

This is a more sophisticated version of [Round robin policy](robin.md).
It takes all nodes in the local datacenter and uses them one after another.  
If no nodes from the local datacenter are available it will fall back to other nodes.

For example if there are two datacenters:
* `us_east` with nodes: `A`, `B`, `C`
* `us_west` with nodes: `D`, `E`, `F`

this policy when set to `us_east` will only use `A`, `B`, `C`, `A`, `B`, ...

### Example
To use this policy in `Session`:
```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::{Session, SessionBuilder};
use scylla::transport::load_balancing::DcAwareRoundRobinPolicy;
use std::sync::Arc;

let local_dc_name: String = "us_east".to_string();

let session: Session = SessionBuilder::new()
    .known_node("127.0.0.1:9042")
    .load_balancing(Arc::new(DcAwareRoundRobinPolicy::new(local_dc_name)))
    .build()
    .await?;
# Ok(())
# }
```