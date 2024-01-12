use anyhow::Result;
use scylla::transport::session::{IntoTypedRows, Session};
use scylla::SessionBuilder;
use std::env;
use std::sync::Arc;

use tokio_rustls::rustls::{
    self,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore,
};

// How to run scylla instance with TLS:
//
// Edit your scylla.yaml file and add paths to certificates
// ex:
// client_encryption_options:
//     enabled: true
//     certificate: /etc/scylla/db.crt
//     keyfile: /etc/scylla/db.key
//
// If using docker mount your scylla.yaml file and your cert files with option
// --volume $(pwd)/tls.yaml:/etc/scylla/scylla.yaml
//
// If python returns permission error 13 use "Z" flag
// --volume $(pwd)/tls.yaml:/etc/scylla/scylla.yaml:Z
//
// In your Rust program connect to port 9142 if it wasn't changed
// Create new a ClientConfig with your certificate added to it's
// root store.
//
// If your server is using a certifcate that does not have it's IP address
// as a Common Name or Subject Alternate Name you will need to skip
// name verification as part of rustls's configuration.
//
// Build it and add to scylla-rust-driver's SessionBuilder

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9142".to_string());

    println!("Connecting to {} ...", uri);
    let mut root_cert_store = RootCertStore::empty();
    let rustls_pemfile::Item::X509Certificate(cert) = rustls_pemfile::read_one_from_slice(
        &tokio::fs::read("./test/tls/ca.crt")
            .await
            .expect("Failed to load cert"),
    )
    .expect("Failed to parse pem")
    .expect("No certificates in file")
    .0
    else {
        panic!("not a certificate")
    };
    root_cert_store
        .add(cert)
        .expect("Failed to add cert to root cert");

    let mut config = ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();

    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification::default()));

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .rustls_config(Some(Arc::new(config)))
        .build()
        .await?;

    session.query("CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.t (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (?, ?, ?)", (3, 4, "def"))
        .await?;

    session
        .query("INSERT INTO ks.t (a, b, c) VALUES (1, 2, 'abc')", &[])
        .await?;

    let prepared = session
        .prepare("INSERT INTO ks.t (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    // Rows can be parsed as tuples
    if let Some(rows) = session.query("SELECT a, b, c FROM ks.t", &[]).await?.rows {
        for row in rows.into_typed::<(i32, i32, String)>() {
            let (a, b, c) = row?;
            println!("a, b, c: {}, {}, {}", a, b, c);
        }
    }
    println!("Ok.");

    Ok(())
}

#[derive(Debug)]
struct NoCertificateVerification {
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl Default for NoCertificateVerification {
    fn default() -> Self {
        Self {
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        }
    }
}

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        return Ok(ServerCertVerified::assertion());
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        return Ok(HandshakeSignatureValid::assertion());
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        return Ok(HandshakeSignatureValid::assertion());
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.supported.supported_schemes()
    }
}
