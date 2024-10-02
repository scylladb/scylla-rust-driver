use anyhow::Result;
use scylla::transport::session::Session;
use scylla::SessionBuilder;
use std::env;
use std::fs;
use std::path::PathBuf;

use openssl::ssl::{SslContextBuilder, SslMethod, SslVerifyMode};

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
// Create new SslContextBuilder with SslMethod that is used in your connection
// Set verification mode
// if SslVerifyMode::PEER with self-signed certificate you have to
// use set_ca_file method with path to your ca.crt file as an argument
// if SslVerifyMode::NONE you don't need to use any additional methods
//
// Build it and add to scylla-rust-driver's SessionBuilder

#[tokio::main]
async fn main() -> Result<()> {
    // Create connection
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9142".to_string());

    println!("Connecting to {} ...", uri);

    let mut context_builder = SslContextBuilder::new(SslMethod::tls())?;
    let ca_dir = fs::canonicalize(PathBuf::from("./test/tls/ca.crt"))?;
    context_builder.set_ca_file(ca_dir.as_path())?;
    context_builder.set_verify(SslVerifyMode::PEER);

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .ssl_context(Some(context_builder.build()))
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
        .into_typed::<(i32, i32, String)>()?;
    while let Some((a, b, c)) = iter.try_next().await? {
        println!("a, b, c: {}, {}, {}", a, b, c);
    }

    println!("Ok.");

    Ok(())
}
