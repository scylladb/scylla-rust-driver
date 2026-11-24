use crate::node::NodeOptions;
use crate::topology::Topology;

use super::cli_wrapper::DBType;
use super::cli_wrapper::NodeStartOptions;
use super::cli_wrapper::cluster::Ccm;
use super::ip_allocator::NetPrefix;
use super::logged_cmd::LoggedCmd;
use super::node::{Node, NodeId, NodeStatus};
use super::{IP_ALLOCATOR, ROOT_CCM_DIR, TEST_KEEP_CLUSTER_ON_FAILURE};

#[cfg(feature = "driver")]
use scylla::client::session_builder::SessionBuilder;

use anyhow::{Context, Error};
use tempfile::TempDir;
use tokio::fs::metadata;
use tracing::info;

use std::path::PathBuf;
use std::sync::Arc;

/// Default memory size in MB for cluster nodes.
pub const DEFAULT_MEMORY: u32 = 1024;
/// Default number of SMPs for cluster nodes.
pub const DEFAULT_SMP: u16 = 2;

/// Configuration options for creating a CCM cluster.
#[derive(Debug, Clone)]
pub struct ClusterOptions {
    /// Cluster Name
    pub name: String,
    /// What to database to run: ScyllaDB or Cassandra
    pub db_type: DBType,
    /// ScyllaDB or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub version: String,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub ip_prefix: NetPrefix,
    /// The datacenters and racks to create the cluster with, and the number of nodes in each.
    pub topology: Topology,
    /// Number of vCPU for Scylla to occupy
    pub smp: u16,
    /// Amount of MB for Scylla to occupy. Has to be bigger than `smp`*512.
    pub memory: u32,
    /// Don't call `ccm remove` when cluster instance is dropped
    pub keep_on_drop: bool,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            name: "".to_string(),
            db_type: DBType::Scylla,
            version: "".to_string(),
            ip_prefix: NetPrefix::empty(),
            topology: Topology::default(),
            smp: DEFAULT_SMP,
            memory: DEFAULT_MEMORY,
            keep_on_drop: false,
        }
    }
}

/// List of nodes in a cluster.
pub struct NodeList(Vec<Node>);

impl NodeList {
    fn get_free_node_id(&self) -> Option<NodeId> {
        for node_id in 1..=255 {
            match self.iter().find(|node| node.id() == node_id) {
                Some(_) => continue,
                None => return Some(node_id),
            }
        }
        None
    }

    fn push(&mut self, node: Node) {
        self.0.push(node);
    }

    /// Iterate over nodes.
    pub fn iter(&self) -> impl Iterator<Item = &Node> {
        self.0.iter()
    }

    /// Iterate mutably over nodes.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.0.iter_mut()
    }

    /// Get a node by ID.
    pub fn get_by_id(&self, id: NodeId) -> Option<&Node> {
        self.iter().find(|node| node.id() == id)
    }

    /// Get a mutable reference to a node by ID.
    pub fn get_mut_by_id(&mut self, id: NodeId) -> Option<&mut Node> {
        self.iter_mut().find(|node| node.id() == id)
    }

    fn new() -> Self {
        NodeList(Vec::new())
    }

    /// Get contact endpoints for all nodes.
    pub fn get_contact_endpoints(&self) -> Vec<String> {
        self.iter().map(|node| node.contact_endpoint()).collect()
    }

    /// Get the number of nodes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the node list is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// A CCM cluster instance.
pub struct Cluster {
    ccm_cmd: Ccm,
    // Needs to be held, because it removes the dir when dropped
    tmp_dir_guard: TempDir,
    nodes: NodeList,
    opts: ClusterOptions,
    destroyed: bool,
    allocated_prefix: bool,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.opts.keep_on_drop {
            self.destroy_sync()
                .expect("Automatic cluster descruction failed");
        }

        if self.allocated_prefix {
            // Return the IP prefix to the pool.
            IP_ALLOCATOR
                .lock()
                .expect("Failed to acquire IP_ALLOCATOR lock")
                .free_ip_prefix(&self.opts.ip_prefix)
                .expect("Failed to return ip prefix");
        }
    }
}

impl Cluster {
    /// Creates a new cluster instance with the given options.
    pub async fn new(opts: ClusterOptions) -> Result<Self, Error> {
        // Before creating anything that dropping the `Cluster` would try to tear down.
        opts.topology.validate_initial()?;

        let mut opts = opts.clone();
        let allocated_prefix = if opts.ip_prefix.is_empty() {
            opts.ip_prefix = IP_ALLOCATOR
                .lock()
                .expect("Failed to acquire IP_ALLOCATOR lock")
                .alloc_ip_prefix()?;
            true
        } else {
            false
        };

        let config_dir = TempDir::with_prefix_in(&opts.name, &*ROOT_CCM_DIR)
            .context("Failed to create temp dir for the cluster")?;
        let config_dir_path = config_dir.path();

        info!("Config dir: {:?}", config_dir.path());

        match metadata(config_dir_path).await {
            Ok(mt) => {
                if !mt.is_dir() {
                    anyhow::bail!(
                        "{:?} already exists and it is not a directory",
                        config_dir_path
                    );
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir_all(config_dir_path)
                        .await
                        .with_context(|| {
                            format!("failed to create root directory {config_dir_path:?}")
                        })?;
                }
                _ => {
                    return Err(Error::from(err).context(format!(
                        "failed to create root directory {config_dir_path:?}"
                    )));
                }
            },
        }

        let lcmd = Arc::new(LoggedCmd::new());

        let mut cluster = Cluster {
            ccm_cmd: Ccm::new(config_dir.path(), Arc::clone(&lcmd)),
            tmp_dir_guard: config_dir,
            nodes: NodeList::new(),
            opts: opts.clone(),
            destroyed: false,
            allocated_prefix,
        };

        for datacenter in opts.topology.datacenters() {
            for rack in datacenter.racks() {
                for _ in 0..rack.nodes() {
                    let node_options = NodeOptions::from_cluster_opts(
                        &cluster.opts,
                        cluster
                            .nodes
                            .get_free_node_id()
                            .expect("No available node id"),
                        datacenter.name().to_owned(),
                        rack.name().to_owned(),
                    );
                    cluster.append_node(node_options);
                }
            }
        }

        Ok(cluster)
    }

    /// Initializes the cluster by creating it via CCM.
    pub async fn init(&mut self) -> Result<(), Error> {
        // `ccm create --nodes` would populate the cluster too, but it only takes node counts, so
        // create an empty cluster and add the nodes one by one, each with its own placement.
        self.ccm_cmd
            .cluster_create(
                self.opts.name.clone(),
                self.opts.version.clone(),
                self.opts.db_type,
            )
            .run()
            .await?;

        // CCM writes each node's datacenter and rack to the topology file read by the snitch of
        // the cluster's type, but `ccm add` doesn't configure that snitch. Set it for the whole
        // cluster, so that every node added to it uses the snitch.
        let snitch = match self.opts.db_type {
            // Reads `cassandra-rackdc.properties`.
            DBType::Scylla => "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
            // Reads `cassandra-topology.properties`.
            DBType::Cassandra => "org.apache.cassandra.locator.PropertyFileSnitch",
        };
        self.updateconf([("endpoint_snitch", snitch)]).await?;

        for node in self.nodes.iter() {
            self.ccm_cmd
                .cluster_add_node(self.opts.db_type, node.name())
                .dc(node.datacenter().to_owned())
                .rack(node.rack().to_owned())
                .address(node.broadcast_rpc_address().to_string())
                // Like `ccm create --nodes`, which makes every node a seed.
                .seed()
                .run()
                .await?;
        }

        Ok(())
    }

    /// Executes `ccm updateconf` and applies it for all nodes in the cluster.
    /// It accepts the key-value pairs to update the configuration.
    ///
    /// ### Example
    /// ```
    /// # use scylla_ccm_bridge::cluster::Cluster;
    /// # use std::error::Error;
    /// # async fn check_only_compiles(cluster: &mut Cluster) -> Result<(), Box<dyn Error>> {
    /// let args = [
    ///     ("client_encryption_options.enabled", "true"),
    ///     ("client_encryption_options.certificate", "db.cert"),
    ///     ("client_encryption_options.keyfile", "db.key"),
    /// ];
    ///
    /// cluster.updateconf(args).await?;
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
            .cluster_updateconf()
            .config(key_values)
            .run()
            .await
            .map(|_| ())
    }

    /// Enables the `PasswordAuthenticator` for the cluster.
    // Consider making it accept an enum in the future. Supported authenticators:
    // https://github.com/scylladb/scylladb/blob/529ff3efa57553eef6b0239b03b81581b70fb9ed/db/config.cc#L1045-L1051.
    pub async fn enable_password_authentication(&mut self) -> Result<(), Error> {
        let args = [
            ("authenticator", "PasswordAuthenticator"),
            ("auth_superuser_name", "cassandra"),
            (
                "auth_superuser_salted_password",
                "$6$x7IFjiX5VCpvNiFk$2IfjTvSyGL7zerpV.wbY7mJjaRCrJ/68dtT3UpT.sSmNYz1bPjtn3mH.kJKFvaZ2T4SbVeBijjmwGjcb83LlV/",
            ),
        ];

        self.updateconf(args).await
    }

    /// This method starts the cluster. User can provide optional [`NodeStartOptions`] to control the behavior of the nodes start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        self.ccm_cmd
            .cluster_start()
            .wait_options(opts)
            .scylla_smp(self.opts.smp)
            .scylla_mem_megabytes(self.opts.memory)
            .run()
            .await?;

        for node in self.nodes.iter_mut() {
            node.set_status(NodeStatus::Started);
        }

        Ok(())
    }

    fn append_node(&mut self, node_options: NodeOptions) -> &mut Node {
        let node_name = node_options.name();
        let node = Node::new(
            node_options,
            self.ccm_cmd.for_node(node_name),
            &self.cluster_dir(),
        );

        self.nodes.push(node);
        self.nodes.0.last_mut().unwrap()
    }

    /// Add a new node to the cluster in the given datacenter and rack.
    ///
    /// Either may be new to the cluster, in which case adding the node creates it.
    pub async fn add_node(&mut self, datacenter: &str, rack: &str) -> Result<&mut Node, Error> {
        anyhow::ensure!(!datacenter.is_empty(), "the datacenter name is empty");
        anyhow::ensure!(!rack.is_empty(), "the rack name is empty");

        let id = self.nodes.get_free_node_id().expect("No available node id");
        let node_options =
            NodeOptions::from_cluster_opts(&self.opts, id, datacenter.to_owned(), rack.to_owned());
        // Pass the node's IP explicitly via `-i`. Without it, CCM
        // auto-assigns based on `len(nodelist) + 1`, which diverges from
        // our `get_free_node_id()` when there are gaps (e.g., after a
        // decommission reuses a lower ID).
        let node_ip = self.opts.ip_prefix.to_ipaddress(id).to_string();
        self.ccm_cmd
            .cluster_add_node(self.opts.db_type, node_options.name())
            .dc(datacenter.to_owned())
            .rack(rack.to_owned())
            .address(node_ip)
            .run()
            .await?;
        Ok(self.append_node(node_options))
    }

    /// Stops the cluster.
    pub async fn stop(&mut self) -> Result<(), Error> {
        self.ccm_cmd.cluster_stop().run().await.map(|_| ())
    }

    /// Sets whether to keep the cluster on drop.
    pub fn set_keep_on_drop(&mut self, value: bool) {
        self.opts.keep_on_drop = value;
        self.tmp_dir_guard.disable_cleanup(value);
    }

    /// Marks the cluster as failed and preserves it if `TEST_KEEP_CLUSTER_ON_FAILURE` is set.
    pub fn mark_as_failed(&mut self) {
        if *TEST_KEEP_CLUSTER_ON_FAILURE {
            println!("Test failed, keep cluster alive, TEST_KEEP_CLUSTER_ON_FAILURE=true");
            self.set_keep_on_drop(true);
        }
    }

    // Only for use in destructor - that is why it takes `&mut`.
    // This code is here, instead of directly in destructor, because
    // its logic is coupled with `destroy`. Putting those methods
    // close decreases the chances of forgetting to change one of them
    // in the future.
    fn destroy_sync(&mut self) -> Result<(), Error> {
        if !self.destroyed {
            self.ccm_cmd.cluster_remove().run_sync().map(|_| ())?;
        }
        Ok(())
    }

    /// Destroys the cluster by removing it via CCM.
    pub async fn destroy(mut self) -> Result<(), Error> {
        self.ccm_cmd.cluster_remove().run().await.map(|_| ())?;
        self.destroyed = true;
        Ok(())
    }

    /// Creates a new session builder with the cluster's contact endpoints.
    #[cfg(feature = "driver")]
    pub async fn make_session_builder(&self) -> SessionBuilder {
        let endpoints = self.nodes.get_contact_endpoints();
        SessionBuilder::new().known_nodes(endpoints)
    }

    /// Returns a reference to the list of nodes in the cluster.
    pub fn nodes(&self) -> &NodeList {
        &self.nodes
    }

    /// Returns a mutable reference to the list of nodes in the cluster.
    pub fn nodes_mut(&mut self) -> &mut NodeList {
        &mut self.nodes
    }

    /// Returns the cluster's configuration directory path.
    pub fn cluster_dir(&self) -> PathBuf {
        self.tmp_dir_guard.path().join(&self.opts.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::{Datacenter, Rack};

    #[tokio::test]
    async fn rejects_invalid_topology() {
        let result = Cluster::new(ClusterOptions {
            name: "invalid_topology".to_owned(),
            topology: Topology::new([Datacenter::new("dc1", [Rack::new("rack1", 0)])]),
            ..ClusterOptions::default()
        })
        .await;
        // Rejected before anything that dropping the `Cluster` would try to remove with
        // `ccm remove` is created.
        assert!(result.is_err());
    }
}
