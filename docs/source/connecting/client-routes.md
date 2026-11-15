# Client Routes (ScyllaDB Cloud Private Connectivity)

When connecting to clusters with ScyllaDB Cloud Private Connectivity, 
a ScyllaDB feature provided using AWS PrivateLink or GCP Private Service Connect,
nodes are not reachable at the addresses they broadcast to each other.
Instead, each node is reachable through a proxy endpoint whose address is stored in the
`system.client_routes` table. The driver needs to be told to use this table for routing —  that is
what the **Client Routes** feature provides.

## Overview

In a Client Routes setup:

1. Each proxy endpoint is identified by a **connection ID** —  a string that the cloud
   infrastructure assigns to a particular PrivateLink / Private Service Connect connection.
2. On session startup, as well as on each metadata refresh, the driver queries `system.client_routes`
   to discover which (address, port) pair to use for each cluster node, filtering by the configured
   connection IDs.
3. The driver subscribes to `CLIENT_ROUTES_CHANGE` events so it can update routing
   information without a full metadata refresh when the table contents change.

### Limitations

1. Mixed clusters (with both nodes reachable only through ClientRoutes and nodes reachable only directly)
   **are not supported**. All nodes must be reachable through ClientRoutes, in particular have corresponding
   entries in `system.client_routes`; otherwise, the driver will fail to contact them.
2. TLS **is not yet supported**.

## Prerequisites

Enable the `unstable-client-routes` Cargo feature:

```toml
[dependencies]
scylla = { version = "...", features = ["unstable-client-routes"] }
```

## Basic usage

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::client_routes::{ClientRoutesConfig, ClientRoutesProxy};
use scylla::client::session::Session;
use scylla::client::session_builder::ClientRoutesSessionBuilder;

// The connection ID assigned by your cloud provider for this PrivateLink /
// Private Service Connect connection.
let connection_id = "my-connection-id".to_string();

// Create a proxy configuration for this connection ID.
let proxy = ClientRoutesProxy::new_with_connection_id(connection_id);

// Build a ClientRoutesConfig (at least one proxy is required).
let config = ClientRoutesConfig::new(vec![proxy])?;

// Build the session.  Use the contact point(s) provided by your cloud setup.
let session: Session = ClientRoutesSessionBuilder::new(config)
    .known_node("my-privatelink-endpoint.amazonaws.com:9042")
    .build()
    .await?;

# Ok(())
# }
```

## Multiple connection IDs

If your deployment routes traffic through more than one proxy (e.g. one per availability zone),
pass all of them:

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::client_routes::{ClientRoutesConfig, ClientRoutesProxy};
use scylla::client::session::Session;
use scylla::client::session_builder::ClientRoutesSessionBuilder;

let proxies = vec![
    ClientRoutesProxy::new_with_connection_id("conn-id-az-1".into()),
    ClientRoutesProxy::new_with_connection_id("conn-id-az-2".into()),
];

let config = ClientRoutesConfig::new(proxies)?;

let session: Session = ClientRoutesSessionBuilder::new(config)
    .known_node("endpoint-az-1.amazonaws.com:9042")
    .known_node("endpoint-az-2.amazonaws.com:9042")
    .build()
    .await?;

# Ok(())
# }
```

## Overriding the hostname

By default, the hostname for each proxy is read from `system.client_routes`.
You can override it if your environment requires a different DNS name or IP
(e.g. for local testing):

```rust
# extern crate scylla;
# extern crate tokio;
# use std::error::Error;
# async fn check_only_compiles() -> Result<(), Box<dyn Error>> {
use scylla::client::client_routes::{ClientRoutesConfig, ClientRoutesProxy};
use scylla::client::session::Session;
use scylla::client::session_builder::ClientRoutesSessionBuilder;

let proxy = ClientRoutesProxy::new_with_connection_id("my-connection-id".into())
    .with_overridden_hostname("127.0.0.1".into());

let config = ClientRoutesConfig::new(vec![proxy])?;

let session: Session = ClientRoutesSessionBuilder::new(config)
    .known_node("127.0.0.1:9042")
    .build()
    .await?;

# Ok(())
# }
```

## Differences from a regular session

`ClientRoutesSessionBuilder` supports most of the same options as the regular `SessionBuilder`
(compression, authentication, execution profiles, etc.) with two exceptions:

* **Address translation** is managed internally by the Client Routes infrastructure,
  so `address_translator()` is not available.
* **TLS** is not yet supported on Client Routes sessions.

## Shard awareness

_Advanced shard awareness_ (the driver choosing a source port so that the node assigns a specific
shard to the connection) is enabled by default, as in a regular session. Such connections go to
the node's endpoint from `system.client_routes`, so they reach the requested shard only if that
endpoint forwards the source port to the node's shard-aware port, e.g. with Proxy Protocol v2.
If it does not, the driver notices that connections land on other shards, logs a warning and falls
back to letting the node choose the shard. Use `disallow_shard_aware_port(true)` to skip the
attempts in such deployments.

_Basic shard awareness_ (sending each request to the correct shard) works either way.
