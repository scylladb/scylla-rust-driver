# Connecting to the cluster

Scylla is a distributed database, which means that it operates on multiple nodes running independently.
When creating a `Session` you can specify a few known nodes to which the driver will try connecting:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::{Session, SessionBuilder};
use std::error::Error;
use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .known_node("127.0.0.72:4321")
        .known_node("localhost:8000")
        .connection_timeout(Duration::from_secs(3))
        .known_node_addr(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            9000,
        ))
        .build()
        .await?;

    Ok(())
}
```

After succesfully connecting to some specified node the driver will fetch topology information about
other nodes in this cluster and connect to them as well.

```eval_rst
.. toctree::
   :hidden:
   :glob:

   compression
   authentication
   tls

```