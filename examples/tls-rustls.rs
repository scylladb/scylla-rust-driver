use std::{env, sync::Arc};

use anyhow::Result;
use futures::TryStreamExt as _;

use rustls::pki_types::{pem::PemObject, CertificateDer};
use scylla::client::{session::Session, session_builder::SessionBuilder};

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
// In your Rust program connect to port 9142 if it wasn't changed.
// Create new rustls ConfigBuilder.
// Optionally, configure RootCertStore with your ca.crt.
// Build it and add to scylla-rust-driver's SessionBuilder.

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9142".to_string());

    println!("Connecting to {uri} ...");

    let rustls_ca = CertificateDer::from_pem_file("./test/tls/ca.crt")?;
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(rustls_ca)?;

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .tls_context(Some(Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        )))
        .build()
        .await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.tls (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.tls (a, b, c) VALUES (?, ?, ?)",
            (3, 4, "def"),
        )
        .await?;

    session
        .query_unpaged(
            "INSERT INTO examples_ks.tls (a, b, c) VALUES (1, 2, 'abc')",
            &[],
        )
        .await?;

    let prepared = session
        .prepare("INSERT INTO examples_ks.tls (a, b, c) VALUES (?, 7, ?)")
        .await?;
    session
        .execute_unpaged(&prepared, (42_i32, "I'm prepared!"))
        .await?;
    session
        .execute_unpaged(&prepared, (43_i32, "I'm prepared 2!"))
        .await?;
    session
        .execute_unpaged(&prepared, (44_i32, "I'm prepared 3!"))
        .await?;

    // Rows can be parsed as tuples
    let mut iter = session
        .query_iter("SELECT a, b, c FROM examples_ks.tls", &[])
        .await?
        .rows_stream::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = iter.try_next().await? {
        println!("a, b, c: {a}, {b}, {c}");
    }

    println!("Ok.");

    Ok(())
}
