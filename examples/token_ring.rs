//! Walking the cluster's token ring and, for each token range, listing the
//! replicas that own it.
//!
//! The token ring is a sorted, circular sequence of tokens. The range that ends
//! at a given token starts just after the previous token in the ring and is owned
//! by the replicas the cluster returns for any token inside it. Which nodes those
//! are depends on the keyspace's replication strategy, so replica sets are looked
//! up per keyspace.

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::cluster::NodeAddr;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let uri = env::var("SCYLLA_URI").unwrap_or_else(|_| "172.42.0.2:9042".to_string());

    println!("Connecting to {uri} ...");

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = \
             {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.token_ring (pk bigint PRIMARY KEY)",
            &[],
        )
        .await?;

    let cluster_state = session.get_cluster_state();

    // The ring is a sorted list of tokens; each entry belongs to one node. Walking
    // it in order visits every token range in the cluster once.
    let ring = cluster_state.replica_locator().ring();
    println!("The token ring has {} entries.", ring.len());

    // Seed the lower bound from the ring's last token so the first (wrap-around)
    // range prints its real start rather than a placeholder. An empty ring simply
    // skips the loop below.
    let mut previous_token: Option<i64> = ring.iter().last().map(|(token, _)| token.value());

    for (token, _owner) in ring.iter() {
        // Replicas that own the range ending at this token. `get_token_endpoints`
        // resolves them through the keyspace's replication strategy, so a token
        // range can have different replicas in different keyspaces. An empty table
        // name keeps the lookup on the keyspace-wide vnode ring instead of any
        // per-table tablet routing.
        let replicas: Vec<NodeAddr> = cluster_state
            .get_token_endpoints("examples_ks", "", *token)
            .into_iter()
            .map(|(node, _shard)| node.address)
            .collect();

        // `previous_token` is always `Some` here: it was seeded from the ring's
        // last token and the loop only runs when the ring is non-empty.
        if let Some(previous) = previous_token {
            println!(
                "Range ({}, {}] is owned by {:?}",
                previous,
                token.value(),
                replicas
            );
        }

        previous_token = Some(token.value());
    }

    println!("Ok.");

    Ok(())
}
