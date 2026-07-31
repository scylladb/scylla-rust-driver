//! Connects to a cluster whose nodes are reachable only through private
//! endpoints published in `system.client_routes` (as with AWS PrivateLink or
//! GCP Private Service Connect) rather than at the addresses they advertise.
//! Such a cluster is not something one has lying around, so the example starts
//! its own throwaway one, fronted by local load balancers, and then shows that
//! the driver really does route through the published endpoint.

// Not an example itself: CI-only cluster setup, see `examples/ci/`.
#[path = "ci/client_routes_cluster.rs"]
mod ci_client_routes_cluster;

use anyhow::{Result, bail};
use scylla::client::client_routes::{ClientRoutesConfig, ClientRoutesProxy};
use scylla::client::session_builder::ClientRoutesSessionBuilder;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // --- CI setup ---------------------------------------------------------
    // Everything down to the matching banner exists only so that this example
    // can run unattended in CI, against a cluster nobody had to configure by
    // hand: it starts a throwaway client-routes cluster with `scylla-ccm-bridge`.
    // It is not what the example teaches. If you already have such a cluster,
    // this is the block you replace with your own contact points.
    let cluster = ci_client_routes_cluster::ClientRoutesCluster::start().await?;
    let connection_id = ci_client_routes_cluster::CONNECTION_ID.to_string();
    let contact_point = cluster.contact_point().to_string();
    // --- end of CI setup --------------------------------------------------

    println!(
        "The node advertises its address as {}. In a private-networking setup that\n\
         address is not routable from here; all we are given is the private\n\
         endpoint {contact_point}.",
        cluster.advertised_node_address(),
    );

    // The connection ID identifies our PrivateLink / Private Service Connect
    // connection. The driver uses it to pick our rows out of
    // `system.client_routes`, and routes to every node accordingly.
    let proxy = ClientRoutesProxy::new_with_connection_id(connection_id);
    let config = ClientRoutesConfig::new(vec![proxy])?;

    let session = ClientRoutesSessionBuilder::new(config)
        .known_node(&contact_point)
        .build()
        .await?;

    // These are the routes the driver was handed on connecting. The address of
    // each node is neither the one it advertises nor the contact point we dialled.
    let routes = session
        .query_unpaged(
            "SELECT host_id, address, port FROM system.client_routes WHERE connection_id = ?",
            (ci_client_routes_cluster::CONNECTION_ID,),
        )
        .await?
        .into_rows_result()?;
    for route in routes.rows::<(Uuid, String, i32)>()? {
        let (host_id, address, port) = route?;
        println!("system.client_routes: node {host_id} is reachable at {address}:{port}");
    }

    // Ordinary work, carried over connections the driver opened to the routed
    // endpoint rather than to the node itself.
    session
        .query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS examples_ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;
    session
        .query_unpaged(
            "CREATE TABLE IF NOT EXISTS examples_ks.client_routes (a int primary key, b text)",
            &[],
        )
        .await?;
    session
        .query_unpaged(
            "INSERT INTO examples_ks.client_routes (a, b) VALUES (1, 'routed')",
            &[],
        )
        .await?;
    let (a, b) = session
        .query_unpaged(
            "SELECT a, b FROM examples_ks.client_routes WHERE a = 1",
            &[],
        )
        .await?
        .into_rows_result()?
        .single_row::<(i32, String)>()?;
    println!("Read back: ({a}, {b})");

    // What that looked like at the TCP level. The routed endpoint's address was
    // never configured into the driver, so every connection it accepted came
    // from a route the driver read out of `system.client_routes`.
    let via_contact_point = cluster.contact_point_connections();
    let via_route = cluster.routed_connections();
    println!(
        "Connections accepted by the contact point {}: {via_contact_point}",
        cluster.contact_point(),
    );
    println!(
        "Connections accepted by the routed endpoint {}: {via_route}",
        cluster.routed_endpoint(),
    );
    if via_route == 0 {
        bail!("the driver never used the endpoint from system.client_routes");
    }
    println!(
        "The routed endpoint is nowhere in this program's session configuration: the\n\
         driver reached it {via_route} time(s) purely on the strength of the route it read\n\
         from system.client_routes, instead of using the node's advertised address.",
    );

    println!("Ok.");

    Ok(())
}
