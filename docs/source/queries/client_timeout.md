# Client timeouts

In order to make sure that your application can handle network failures and other disasters, database queries issued by
Scylla Rust Driver can time out on the client side, without waiting (potentially indefinitely) for a server to respond.

## Per-session settings

A default per-session client timeout is set to 30 seconds. This parameter can be modified via the query builder by using the `client_timeout` method:
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
        .client_timeout(Duration::from_millis(1500))
        .known_node_addr(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            9000,
        ))
        .build()
        .await?;

    Ok(())
}
```

## Per-query settings

Queries, prepared statements and batches can also have their own, unique client timeout parameter. That allows overriding the default session configuration and specifying a timeout for a single particular type of query. Example:

```rust
# extern crate scylla;
# use scylla::Session;
# use std::error::Error;
# async fn check_only_compiles(session: &Session) -> Result<(), Box<dyn Error>> {
use std::time::Duration;
use scylla::query::Query;

// Create a Query manually to change the client timeout
let mut my_query: Query = Query::new("INSERT INTO ks.tab (a) VALUES(?) IF NOT EXISTS".to_string());
my_query.set_client_timeout(Some(Duration::from_secs(3)));

// Insert a value into the table
let to_insert: i32 = 12345;
session.query(my_query, (to_insert,)).await?;
# Ok(())
# }
```
