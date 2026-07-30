use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::Error;

use super::cli_wrapper::node::NodeCcm;
pub use super::cli_wrapper::{DBType, NodeStartOptions, NodeStopOptions};
use super::cluster::ClusterOptions;
use super::ip_allocator::NetPrefix;

/// Unique identifier for a node within a cluster.
pub type NodeId = u16;

/// Configuration options for creating a CCM node.
#[derive(Debug, Clone)]
pub struct NodeOptions {
    /// Node ID, needed to compose ccm commands properly
    pub id: NodeId,
    /// Database Type: Cassandra, Scylla or Datastax
    pub db_type: DBType,
    /// Scylla or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub version: String,
    /// Datacenter ID (0-based index matching position in `ClusterOptions::nodes_per_dc`).
    pub datacenter_id: u16,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub ip_prefix: NetPrefix,
    /// Number of vCPU for Scylla to occupy
    pub smp: u16,
    /// Amount of MB for Scylla to occupy. Has to be bigger than `smp`*512.
    pub memory: u32,
}

impl NodeOptions {
    /// Returns the CCM node name (e.g., "node1", "node2").
    pub fn name(&self) -> String {
        format!("node{}", self.id)
    }

    pub(super) fn from_cluster_opts(value: &ClusterOptions) -> Self {
        NodeOptions {
            id: 0,
            datacenter_id: 1,
            db_type: value.db_type,
            version: value.version.clone(),
            ip_prefix: value.ip_prefix,
            smp: value.smp,
            memory: value.memory,
        }
    }
}

/// The operational status of a node in the cluster.
#[derive(PartialEq)]
pub enum NodeStatus {
    /// Node is stopped.
    Stopped,
    /// Node is started and running.
    Started,
    /// Node has been decommissioned.
    Decommissioned,
    /// Node has been deleted.
    Deleted,
}

/// Represents a single node in the CCM cluster.
pub struct Node {
    status: NodeStatus,
    opts: NodeOptions,
    ccm_cmd: NodeCcm,
    node_dir: PathBuf,
}

impl Node {
    pub(super) fn new(opts: NodeOptions, ccm_cmd: NodeCcm, cluster_dir: &Path) -> Self {
        let node_dir = cluster_dir.join(format!("node{}", opts.id));
        Node {
            opts,
            status: NodeStatus::Stopped,
            ccm_cmd,
            node_dir,
        }
    }

    /// Returns the name of this node (e.g., "node1").
    pub fn name(&self) -> String {
        self.opts.name()
    }

    /// Returns the unique ID of this node.
    pub fn id(&self) -> NodeId {
        self.opts.id
    }

    /// Returns the CQL contact endpoint (address:port) for this node.
    pub fn contact_endpoint(&self) -> String {
        format!(
            "{}:{}",
            self.broadcast_rpc_address(),
            self.native_transport_port()
        )
    }

    /// Returns the broadcast RPC address for this node.
    pub fn broadcast_rpc_address(&self) -> IpAddr {
        self.opts.ip_prefix.to_ipaddress(self.opts.id)
    }

    /// Returns the native transport port for this node (typically 9042).
    pub fn native_transport_port(&self) -> u16 {
        9042
    }

    /// Returns the 0-based datacenter index for this node.
    ///
    /// Corresponds to the position in [`ClusterOptions::nodes_per_dc`].
    /// CCM names these `dc1`, `dc2`, … (1-based), so CCM DC name = `dc{datacenter_id + 1}`.
    pub fn datacenter_id(&self) -> u16 {
        self.opts.datacenter_id
    }

    /// Executes `ccm updateconf` and applies it for this node.
    /// It accepts the key-value pairs to update the configuration.
    ///
    /// ### Example
    /// ```
    /// # use scylla_ccm_bridge::node::Node;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(node: &mut Node) -> Result<(), Box<dyn Error>> {
    /// let args = [
    ///     ("client_encryption_options.enabled", "true"),
    ///     ("client_encryption_options.certificate", "db.cert"),
    ///     ("client_encryption_options.keyfile", "db.key"),
    /// ];
    ///
    /// node.updateconf(args).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The code above is equivalent to the following scylla.yaml:
    /// ```yaml
    /// client_encryption_options:
    ///   enabled: true
    ///   certificate: db.cert
    ///   keyfile: db.key
    /// ```
    pub async fn updateconf<K, V>(
        &mut self,
        key_values: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), Error>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        self.ccm_cmd
            .node_updateconf()
            .config(key_values)
            .run()
            .await
            .map(|_| ())
    }

    /// This method starts the node. User can provide optional [`NodeStartOptions`] to control the behavior of the node start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        self.ccm_cmd
            .node_start()
            .wait_options(opts)
            .scylla_smp(self.opts.smp)
            .scylla_mem_megabytes(self.opts.memory)
            .run()
            .await?;
        self.set_status(NodeStatus::Started);
        Ok(())
    }

    /// Stops the node.
    pub async fn stop(&mut self, opts: Option<NodeStopOptions>) -> Result<(), Error> {
        self.ccm_cmd.node_stop().wait_options(opts).run().await?;
        self.set_status(NodeStatus::Stopped);
        Ok(())
    }

    /// Decommissions the node.
    pub async fn decommission(&mut self) -> Result<(), Error> {
        if self.status == NodeStatus::Deleted || self.status == NodeStatus::Decommissioned {
            return Ok(());
        }
        self.ccm_cmd.node_decommission().run().await?;
        self.set_status(NodeStatus::Decommissioned);
        Ok(())
    }

    /// Deletes the node from the cluster.
    pub async fn delete(&mut self) -> Result<(), Error> {
        if self.status == NodeStatus::Deleted {
            return Ok(());
        }
        self.ccm_cmd.node_remove().run().await?;
        self.set_status(NodeStatus::Deleted);
        Ok(())
    }

    pub(super) fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    /// Returns the current operational status of this node.
    pub fn status(self) -> NodeStatus {
        self.status
    }

    /// Returns the path to the node's configuration directory.
    pub fn node_dir(&self) -> &Path {
        &self.node_dir
    }
}
