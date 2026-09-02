//! Connecting to a ScyllaDB cluster over TLS, using the `openssl` backend.
//!
//! The example starts its own throwaway TLS-enabled cluster, because talking TLS
//! requires a cluster whose certificates were generated up front — there is no
//! generic one to point at.

use anyhow::Result;
use openssl::ssl::{SslConnector, SslMethod};
use openssl::x509::X509;
use openssl::x509::store::X509StoreBuilder;
use scylla::client::session::{OpenSsl010Config, Session};

// Not an example itself: shared CI-only cluster setup, see `examples/ci/`.
#[path = "ci/tls_cluster.rs"]
mod ci_tls_cluster;

#[tokio::main]
async fn main() -> Result<()> {
    // --- CI setup ---------------------------------------------------------
    // Everything down to the matching banner exists only so that this example
    // can run unattended in CI, against a cluster nobody had to configure by
    // hand: it starts a throwaway TLS-enabled cluster with `scylla-ccm-bridge`.
    // It is not what the example teaches. If you already have such a cluster,
    // this is the block you replace with your own contact points.
    let cluster = ci_tls_cluster::start("examples_tls_openssl").await?;
    let ca_cert_der = cluster.ca_cert_der();
    let session_builder = cluster.session_builder().await;
    // --- end of CI setup --------------------------------------------------

    let mut builder = SslConnector::builder(SslMethod::tls())?;
    // Trust the certificate authority that signed the nodes' certificates, and
    // nothing else: `SslConnector` starts out trusting the system roots, which may not be
    // what you want if you use your own CA. `builder.set_ca_file("ca.crt")?` is the
    // shorter route when the CA is a file, but it adds to that default trust instead
    // of replacing it.
    let mut ca_store = X509StoreBuilder::new()?;
    ca_store.add_cert(X509::from_der(ca_cert_der)?)?;
    builder.set_cert_store(ca_store.build());
    // `SslConnector` verifies the node's certificate by default. The driver
    // additionally checks that the certificate covers the IP address it
    // connected to, so the nodes' certs must carry that address in their
    // subject alternative name.

    let session: Session = session_builder
        .tls_context(Some(OpenSsl010Config::new(builder)))
        .build()
        .await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    // `system.clients` is the server's own view of our connections, which is a
    // convenient way to confirm that they really are encrypted.
    let rows_result = session
        .query_unpaged(
            "SELECT ssl_enabled, ssl_protocol, ssl_cipher_suite FROM system.clients",
            &[],
        )
        .await?
        .into_rows_result()?;
    for row in rows_result.rows::<(Option<bool>, Option<String>, Option<String>)>()? {
        let (enabled, protocol, cipher) = row?;
        println!(
            "server sees a connection: ssl_enabled={enabled:?}, protocol={protocol:?}, cipher={cipher:?}"
        );
    }

    println!("Ok, talked to the cluster over TLS with openssl.");

    Ok(())
}
