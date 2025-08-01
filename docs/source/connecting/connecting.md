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
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

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

## Metadata

The driver refreshes the cluster metadata periodically, which contains information about cluster topology as well as the cluster schema. By default, the driver refreshes the cluster metadata every 60 seconds.
However, you can set the `cluster_metadata_refresh_interval` to a non-negative value to periodically refresh the cluster metadata. This is useful when you do not have unexpected amount of traffic or when you have an extra traffic causing topology to change frequently.

## ScyllaDB Cloud Serverless

ScyllaDB Serverless is an elastic and dynamic deployment model. When creating a `Session` you need to
specify the secure connection bundle as follows:

```rust
# extern crate scylla;
# extern crate tokio;
# fn check_only_compiles() {
use std::path::Path;
use std::error::Error;
use scylla::client::session_builder::CloudSessionBuilder;
use scylla::cloud::CloudTlsProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session =
        CloudSessionBuilder::new(Path::new("config_data.yaml"), CloudTlsProvider::OpenSsl010)
            .unwrap()
            .build()
            .await
            .unwrap();

    Ok(())
}
# }
```

It is also possible to load the secure connection bundle from anything
which implements read:

```rust
# extern crate scylla;
# extern crate tokio;
# fn check_only_compiles() {
use std::path::Path;
use std::error::Error;
use std::fs::File;
use scylla::client::session_builder::CloudSessionBuilder;
use scylla::cloud::{CloudConfig, CloudTlsProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut file = File::open("config_data.yaml").unwrap();
    let config = CloudConfig::from_reader(&mut file, CloudTlsProvider::OpenSsl010).unwrap();
    let session = CloudSessionBuilder::from_config(config)
        .build()
        .await
        .unwrap();

    Ok(())
}
# }
```

> ***Note***\
> `CloudSessionBuilder::new()` and `CloudConfig::from_reader()` accept
> two parameters. The first is `Read` or path to the configuration
> file. The second parameter is responsible for choosing the
> underlying TLS provider library. For more information about
> providers supported currently by the driver, see [TLS](tls.md).

Note that the bundle file will be provided after the serverless cluster is created. Here is an example of a
configuration file for a serverless cluster:

```yaml
datacenters:
  datacenter1:
    certificateAuthorityData: CERTIFICATE_DATA
    server: 127.0.1.1:9142
    nodeDomain: cql.cluster-id.scylla.com
    insecureSkipTlsVerify: false
authInfos:
  default:
    clientCertificateData: CERTIFICATE_DATA
    clientKeyData: KEY_DATA
    username: scylladb
    password: scylladb
contexts:
  default:
    datacenterName: datacenter1
    authInfoName: default
currentContext: default
```

```{eval-rst}
.. toctree::
   :hidden:
   :glob:

   compression
   authentication
   tls

```
