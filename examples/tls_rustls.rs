//! Connecting to a ScyllaDB cluster over TLS, using the `rustls` backend.
//!
//! The example starts its own throwaway TLS-enabled cluster, because talking TLS
//! requires a cluster whose certificates were generated up front — there is no
//! generic one to point at.

use std::sync::Arc;

use anyhow::Result;
use rustls::ClientConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use scylla::client::session::Session;

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
    let cluster = ci_tls_cluster::start("examples_tls_rustls").await?;
    let ca_cert_der = cluster.ca_cert_der();
    let session_builder = cluster.session_builder().await;
    // --- end of CI setup --------------------------------------------------

    // A root store holding the certificate authority that signed the nodes'
    // certificates. Had the CA been handed to us as a `ca.crt` file, this would
    // be `CertificateDer::from_pem_file("ca.crt")?`, which needs
    // `rustls::pki_types::pem::PemObject` in scope.
    let mut root_store = RootCertStore::empty();
    root_store.add(CertificateDer::from(ca_cert_der))?;

    // The nodes don't ask us for a client certificate, so no client auth.
    // The driver checks that the node's certificate covers the IP address it
    // connected to, so the nodes' certs must carry that address in their
    // subject alternative name.
    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // An `Arc<ClientConfig>` is accepted by `tls_context` directly.
    let session: Session = session_builder
        .tls_context(Some(Arc::new(client_config)))
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

    println!("Ok, talked to the cluster over TLS with rustls.");

    Ok(())
}
