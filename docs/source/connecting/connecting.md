# Connecting to the cluster

ScyllaDB is a distributed database, which means that it operates on multiple nodes running independently.
When creating a `Session` you can specify a few known nodes to which the driver will try connecting:
```rust
# extern crate scylla;
# extern crate tokio;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use std::error::Error;
use std::time::Duration;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .known_node("127.0.0.72:4321")
        .known_node("localhost:8000")
        .connection_timeout(Duration::from_secs(3))
        .cluster_metadata_refresh_interval(Duration::from_secs(10))
        .known_node_addr(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            9000,
        ))
        .build()
        .await?;

    Ok(())
}
```

After successfully connecting to some specified node the driver will fetch topology information about
other nodes in this cluster and connect to them as well.

## Best practices for using Session

:::{warning}
Always try to use only a single Session object per apllication because creating them is very expensive!
:::

The driver maintains its own pool of connections to each node and each connection is capable of handling multiple requests in parallel. Driver will also route requests to nodes / shards that actually own the data (unless the load balancing policy that you use doesn't support it).

For those reasons, we recommend using one instance of `Session` per application.

Creating short-lived `Session`'s (e.g. `Session` per request) is strongly discouraged because it will result in great performance penalties because creating a `Session` is a costly process - it requires estabilishing a lot of TCP connections.
Creating many `Session`'s in one application (e.g. `Session` per thread / per Tokio task) is also discouraged, because it wastes resources - as mentioned before, `Session` maintains a connection pool itself and can handle parallel queries, so you would be holding a lot of connections unnecessarily.

If you need to share `Session` with different threads / Tokio tasks etc. use `Arc<Session>` - all methods of `Session` take `&self`, so it doesn't hinder the functionality in any way.

## Session identification

Every connection the driver opens sends a `SESSION_ID` option in the CQL `STARTUP` message - a UUID generated once per `Session`.
The server exposes it in `system.clients.client_options`, so all rows belonging to one session can be told apart from other clients.

`Session::session_id()` returns that UUID; log it to correlate client-side observations with `system.clients`:
```rust
# extern crate scylla;
# use scylla::client::session::Session;
# fn check_only_compiles(session: &Session) {
println!("session id: {}", session.session_id());
# }
```

## Metadata

The driver keeps an up-to-date view of the cluster topology (and client routes, if used) and of the cluster schema.
Topology and client routes are refreshed in reaction to the server events announcing a change.
Schema changes are different: the keyspaces that `SCHEMA_CHANGE` events name are accumulated and re-read by a periodic refresh, which batches the burst of events that a single DDL statement produces.

That refresh runs every 60 seconds by default; `cluster_metadata_refresh_interval` sets the interval. A shorter one picks schema changes up sooner, at the cost of more frequent metadata queries.

Only the affected keyspaces are re-read, not the whole schema.
If your cluster's events cannot be relied upon, `periodic_metadata_fetch_mode(PeriodicFetchMode::FullMetadata)` makes each refresh re-read the whole metadata instead.


```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   compression
   authentication
   tls
   client-routes

```
