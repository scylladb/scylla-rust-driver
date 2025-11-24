use std::net::IpAddr;
use std::path::{Path, PathBuf};

use anyhow::Error;

use super::cli_wrapper::node::NodeCcm;
use super::cli_wrapper::{DBType, NodeStartOptions, NodeStopOptions};
use super::cluster::ClusterOptions;
use super::ip_allocator::NetPrefix;

pub(crate) type NodeId = u16;

#[derive(Debug, Clone)]
pub(crate) struct NodeOptions {
    /// Node ID, needed to compose ccm commands properly
    pub(crate) id: NodeId,
    /// Database Type: Cassandra, Scylla or Datastax
    #[expect(dead_code)]
    pub(crate) db_type: DBType,
    /// Scylla or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    #[expect(dead_code)]
    pub(crate) version: String,
    /// Datacenter ID
    #[expect(dead_code)]
    pub(crate) datacenter_id: u16,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub(crate) ip_prefix: NetPrefix,
    /// Number of vCPU for Scylla to occupy
    pub(crate) smp: u16,
    /// Amount of MB for Scylla to occupy. Has to be bigger than `smp`*512.
    pub(crate) memory: u32,
}

impl NodeOptions {
    pub(crate) fn name(&self) -> String {
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

#[derive(PartialEq)]
pub(crate) enum NodeStatus {
    Stopped,
    Started,
    Deleted,
}

pub(crate) struct Node {
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

    #[expect(dead_code)]
    pub(crate) fn name(&self) -> String {
        self.opts.name()
    }

    pub(crate) fn id(&self) -> NodeId {
        self.opts.id
    }

    pub(crate) fn contact_endpoint(&self) -> String {
        format!(
            "{}:{}",
            self.broadcast_rpc_address(),
            self.native_transport_port()
        )
    }

    pub(crate) fn broadcast_rpc_address(&self) -> IpAddr {
        self.opts.ip_prefix.to_ipaddress(self.opts.id)
    }

    pub(crate) fn native_transport_port(&self) -> u16 {
        9042
    }

    /// Executes `ccm updateconf` and applies it for this node.
    /// It accepts the key-value pairs to update the configuration.
    ///
    /// ### Example
    /// ```
    /// # use crate::ccm::cluster::Node;
    /// # async fn check_only_compiles(node: &Node) -> Result<(), Box<dyn Error>> {
    /// let args = [
    ///     ("client_encryption_options.enabled", "true"),
    ///     ("client_encryption_options.certificate", "db.cert"),
    ///     ("client_encryption_options.keyfile", "db.key"),
    /// ];
    ///
    /// node.updateconf(args).await?
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
    #[cfg_attr(
        any(not(feature = "openssl-010"), not(feature = "rustls-023")),
        expect(dead_code)
    )]
    pub(crate) async fn updateconf<K, V>(
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
    #[expect(dead_code)]
    pub(crate) async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
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

    #[expect(dead_code)]
    pub(crate) async fn stop(&mut self, opts: Option<NodeStopOptions>) -> Result<(), Error> {
        self.ccm_cmd.node_stop().wait_options(opts).run().await?;
        self.set_status(NodeStatus::Stopped);
        Ok(())
    }

    #[expect(dead_code)]
    pub(crate) async fn delete(&mut self) -> Result<(), Error> {
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

    #[expect(dead_code)]
    pub(crate) fn status(self) -> NodeStatus {
        self.status
    }

    #[cfg_attr(
        any(not(feature = "openssl-010"), not(feature = "rustls-023")),
        expect(dead_code)
    )]
    pub(crate) fn node_dir(&self) -> &Path {
        &self.node_dir
    }
}
