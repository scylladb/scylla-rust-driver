//! CI-only support code for `examples/client_routes.rs`.
//!
//! It recreates, in miniature, the topology that the Client Routes feature
//! exists for: a ScyllaDB node that is not reachable at the address it
//! advertises to clients, only through private endpoints published in
//! `system.client_routes`.
//!
//! ```text
//!            contact-point endpoint (127.0.0.1:A) --.
//!  driver --<                                        >-- ScyllaDB node (127.x.y.1:9042)
//!            routed endpoint        (127.0.0.1:B) --'
//! ```
//!
//! The two endpoints are deliberately distinct: the driver is handed only the
//! first one, so any traffic reaching the second proves that the driver picked
//! up the route from `system.client_routes`. Both are `scylla-proxy`'s
//! [`NlbFrontend`], the same stand-in for a cloud network load balancer that
//! the driver's own client-routes tests use.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use scylla_ccm_bridge::CLUSTER_VERSION;
use scylla_ccm_bridge::client_routes::{ClientRoute, publish_client_routes};
use scylla_ccm_bridge::cluster::{Cluster, ClusterOptions};
use scylla_proxy::nlb::{NlbFrontend, RunningNlbFrontend};
use uuid::Uuid;

/// The connection ID that a cloud provider would assign to our private
/// endpoint. It is what the driver filters `system.client_routes` by.
pub const CONNECTION_ID: &str = "examples-client-routes";

/// Starts a load balancer on an OS-assigned local port, forwarding to `backend`.
async fn start_nlb(backend: SocketAddr) -> Result<RunningNlbFrontend> {
    NlbFrontend::builder()
        .listen_addr("127.0.0.1:0".parse().unwrap())
        .backend(backend)
        .build()
        .run()
        .await
        .context("failed to start the load balancer")
}

/// A throwaway one-node CCM cluster reachable only through local endpoints,
/// with a route for its node registered in `system.client_routes`.
///
/// Dropping it destroys the CCM cluster; the load balancers, being plain tokio
/// tasks, go away with the runtime.
pub struct ClientRoutesCluster {
    /// Destroys the CCM cluster on drop, hence held for the whole run.
    _cluster: Cluster,
    contact_point: RunningNlbFrontend,
    routed: RunningNlbFrontend,
    advertised_node_address: String,
}

impl ClientRoutesCluster {
    pub async fn start() -> Result<Self> {
        let mut cluster = Cluster::new(ClusterOptions {
            name: "examples_client_routes".to_string(),
            version: CLUSTER_VERSION.clone(),
            nodes_per_dc: vec![1],
            ..Default::default()
        })
        .await?;
        cluster.init().await?;
        cluster.start(None).await?;

        let node = cluster
            .nodes()
            .iter()
            .next()
            .context("the cluster has no nodes")?;
        let node_ip = node.broadcast_rpc_address();
        let node_addr = SocketAddr::new(node_ip, node.native_transport_port());
        let advertised_node_address = node.contact_endpoint();

        // Routes are keyed by host ID, so ask the node for its own.
        let host_id = cluster
            .make_session_builder()
            .await
            .build()
            .await?
            .query_unpaged("SELECT host_id FROM system.local", &[])
            .await?
            .into_rows_result()?
            .single_row::<(Uuid,)>()?
            .0;

        // Two independent endpoints in front of the same node: one is given to
        // the driver as a contact point, the other can only be learned from
        // `system.client_routes`.
        let contact_point = start_nlb(node_addr).await?;
        let routed = start_nlb(node_addr).await?;

        // In a real deployment, the cloud provider publishes the route when the
        // private endpoint is created.
        publish_client_routes(
            node_ip,
            &[ClientRoute {
                connection_id: CONNECTION_ID.to_string(),
                host_id,
                endpoint: routed.listen_addr(),
            }],
        )
        .await?;

        Ok(Self {
            _cluster: cluster,
            contact_point,
            routed,
            advertised_node_address,
        })
    }

    /// The private endpoint handed to the driver as its contact point.
    pub fn contact_point(&self) -> SocketAddr {
        self.contact_point.listen_addr()
    }

    /// The endpoint registered for the node in `system.client_routes`.
    pub fn routed_endpoint(&self) -> SocketAddr {
        self.routed.listen_addr()
    }

    /// The address the node advertises to clients, which none of them can reach
    /// here.
    pub fn advertised_node_address(&self) -> &str {
        &self.advertised_node_address
    }

    /// TCP connections accepted by the contact-point endpoint.
    pub fn contact_point_connections(&self) -> u64 {
        self.contact_point.accepted_connections()
    }

    /// TCP connections accepted by the routed endpoint.
    pub fn routed_connections(&self) -> u64 {
        self.routed.accepted_connections()
    }
}
