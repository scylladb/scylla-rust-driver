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
        .cluster_topology_refresh_interval(Duration::from_secs(10))
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

You can also set the `cluster_topology_refresh_interval` to a non-zero value to periodically refresh the
cluster topology. This is useful when you do not have unexpected amount of traffic or when you
have an extra traffic causing topology to change frequently. The default value is 60,
which means that the driver will refresh the topology every 60 seconds.

Scylla Serverless is an elastic and dynamic deployment model. When creating a `Session` you need to
specify the secure connection bundle as follows:

```rust
# extern crate scylla;
# extern crate tokio;
# fn check_only_compiles() {
use std::path::Path;
use std::error::Error;
use scylla::CloudSessionBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let session = CloudSessionBuilder::new(Path::new("config_data.yaml"))
        .unwrap()
        .build()
        .await
        .unwrap();

    Ok(())
}
# }
```

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

```eval_rst
.. toctree::
   :hidden:
   :glob:

   compression
   authentication
   tls

```