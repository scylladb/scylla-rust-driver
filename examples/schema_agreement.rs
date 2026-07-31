//! Shows how to check that every node in the cluster has caught up with a
//! schema change.
//!
//! After each schema-altering statement the driver waits, by itself, until the
//! nodes agree on the new schema. This example turns that off to take control
//! of when the waiting happens - what an application applying a batch of
//! migrations would do to avoid paying for a wait after every single one. The
//! price is that it must then wait explicitly wherever a statement depends on
//! an earlier schema change.

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::errors::SchemaAgreementError;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        // Important: by default schema agreement is automatically awaited after each DDL,
        // making our manual awaits no-op in this case.
        // DDL: https://docs.scylladb.com/manual/stable/cql/ddl.html
        .auto_await_schema_agreement(false)
        // How long to sleep between consecutive checks while the nodes disagree.
        .schema_agreement_interval(Duration::from_millis(500))
        .build()
        .await?;

    let version = session.await_schema_agreement().await?;
    println!("Schema version before any change: {version}");

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;

    // This wait is not optional. Automatic waiting is off, and the statement
    // below is sent to whichever node the load balancing policy picks - very
    // likely not the one that just created the keyspace. Without waiting for
    // the nodes to agree first, creating the table fails, sooner or later, with
    // "Can't find a keyspace examples_ks".
    session.await_schema_agreement().await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.schema_agreement (a int, b int, c text, primary key (a, b))",
            &[],
        )
        .await?;

    // A single check of the current state, which never retries and never waits.
    // Straight after a schema change the nodes may legitimately still disagree.
    match session.check_schema_agreement().await? {
        Some(version) => println!("Nodes already agree on schema version {version}"),
        None => println!("Nodes do not agree on the schema version yet"),
    }

    // Keep checking every `schema_agreement_interval` until the nodes agree or
    // `schema_agreement_timeout` elapses.
    match session.await_schema_agreement().await {
        Ok(version) => println!("Nodes agreed on schema version {version}"),
        // A timeout does not mean something went wrong: a cluster busy with
        // schema changes may simply need longer than we were willing to wait.
        Err(SchemaAgreementError::Timeout(waited)) => {
            println!("Nodes still disagree after {waited:?}")
        }
        Err(err) => return Err(err.into()),
    }

    println!("Ok.");

    Ok(())
}
