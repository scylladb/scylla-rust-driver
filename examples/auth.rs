//! Shows how to connect to a cluster that requires authentication, and what
//! happens when the credentials are wrong.
//!
//! Authentication is configured on the server, not in the driver: a cluster
//! running the default `AllowAllAuthenticator` accepts every connection and
//! silently ignores whatever credentials you send it. Demonstrating anything
//! at all therefore requires a cluster with `PasswordAuthenticator` enabled,
//! so this example starts a throwaway one of its own.
//!
//! Against such a cluster the driver side is a single builder call —
//! `SessionBuilder::user(username, password)` — and the example does two
//! things with it: it connects with the right password and runs a statement,
//! and then it connects with a wrong one and shows the connection being
//! refused. The second half is the point: it is what distinguishes "we
//! authenticated" from "authentication was switched off and nobody checked".

use anyhow::{Result, bail};
use scylla::errors::{
    ConnectionError, ConnectionPoolError, ConnectionSetupRequestErrorKind, DbError, MetadataError,
    NewSessionError,
};
use scylla_ccm_bridge::CLUSTER_VERSION;
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};

const USER: &str = "cassandra";
const PASSWORD: &str = "cassandra";

#[tokio::main]
async fn main() -> Result<()> {
    // --- CI setup ---------------------------------------------------------
    // Everything down to the matching banner exists only so that this example
    // can run unattended in CI, against a cluster nobody had to configure by
    // hand: it starts a throwaway password-authenticated cluster with
    // `scylla-ccm-bridge`.
    // It is not what the example teaches. If you already have such a cluster,
    // this is the block you replace with your own contact points.
    let mut cluster = Cluster::new(ClusterOptions {
        name: "examples_auth".to_string(),
        version: CLUSTER_VERSION.clone(),
        nodes_per_dc: vec![1],
        ..Default::default()
    })
    .await?;
    cluster.init().await?;
    // Swaps `AllowAllAuthenticator` for `PasswordAuthenticator` and seeds the
    // `cassandra`/`cassandra` superuser. Rewrites scylla.yaml, so it has to
    // happen before the nodes are started.
    cluster.enable_password_authentication().await?;
    cluster.start(None).await?;
    // --- end of CI setup --------------------------------------------------

    // The whole of the driver-side story: hand the credentials to the builder.
    // The driver replies to the server's AUTHENTICATE challenge on every
    // connection it opens, for the lifetime of the session.
    println!("Connecting as {USER} with the correct password ...");
    let session = cluster
        .make_session_builder()
        .await
        .user(USER, PASSWORD)
        .build()
        .await?;

    session.query_unpaged("CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}", &[]).await?;
    println!("Connected, and the keyspace was created.");

    // Now the same thing with a wrong password. Getting an error here is the
    // expected outcome, so it is matched on rather than propagated: it is the
    // proof that the successful connection above meant something.
    println!("Connecting as {USER} with a wrong password ...");
    let result = cluster
        .make_session_builder()
        .await
        .user(USER, "not-the-password")
        .build()
        .await;

    match result {
        Ok(_) => bail!("the server accepted a wrong password - it is not enforcing authentication"),
        Err(err) => match authentication_failure(&err) {
            Some(message) => println!("Rejected, as it should be. The server said: {message}"),
            None => bail!("connecting failed, but not because of the credentials: {err}"),
        },
    }

    Ok(())
}

/// Recognizes "the server rejected our credentials" among all the other ways
/// building a session can fail, and returns the message the server sent.
///
/// Bad credentials surface as a `DbError::AuthenticationError` raised while
/// setting up the control connection, which is what the nesting below spells
/// out. Anything else - an unreachable node, a broken pool, bad metadata - is
/// a different problem and is deliberately not swallowed here.
fn authentication_failure(err: &NewSessionError) -> Option<&str> {
    let NewSessionError::MetadataError(MetadataError::ConnectionPoolError(
        ConnectionPoolError::Broken {
            last_connection_error: ConnectionError::ConnectionSetupRequestError(setup_error),
        },
    )) = err
    else {
        return None;
    };

    match &setup_error.error {
        ConnectionSetupRequestErrorKind::DbError(DbError::AuthenticationError, message) => {
            Some(message)
        }
        _ => None,
    }
}
