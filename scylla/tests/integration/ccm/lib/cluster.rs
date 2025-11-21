use crate::ccm::lib::TEST_KEEP_CLUSTER_ON_FAILURE;
use crate::ccm::lib::node::NodeOptions;

use super::cli_wrapper::DBType;
use super::cli_wrapper::NodeStartOptions;
use super::cli_wrapper::cluster::Ccm;
use super::ip_allocator::NetPrefix;
use super::logged_cmd::LoggedCmd;
use super::node::{Node, NodeId, NodeStatus};
use super::{IP_ALLOCATOR, ROOT_CCM_DIR};

use scylla::client::session_builder::SessionBuilder;

use anyhow::{Context, Error};
use tempfile::TempDir;
use tokio::fs::metadata;
use tracing::info;

use std::path::PathBuf;
use std::sync::Arc;

pub(crate) const DEFAULT_MEMORY: u32 = 512;
pub(crate) const DEFAULT_SMP: u16 = 1;

#[derive(Debug, Clone)]
pub(crate) struct ClusterOptions {
    /// Cluster Name
    pub(crate) name: String,
    /// What to database to run: ScyllaDB or Cassandra
    pub(crate) db_type: DBType,
    /// ScyllaDB or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub(crate) version: String,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub(crate) ip_prefix: NetPrefix,
    /// Number of nodes to populate
    /// [1,2] - DC1 contains 1 node, DC2 contains 2 nodes
    pub(crate) nodes_per_dc: Vec<u8>,
    /// Number of vCPU for Scylla to occupy
    pub(crate) smp: u16,
    /// Amount of MB for Scylla to occupy. Has to be bigger than `smp`*512.
    pub(crate) memory: u32,
    /// Don't call `ccm remove` when cluster instance is dropped
    pub(crate) keep_on_drop: bool,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            name: "".to_string(),
            db_type: DBType::Scylla,
            version: "".to_string(),
            ip_prefix: NetPrefix::empty(),
            nodes_per_dc: Vec::new(),
            smp: DEFAULT_SMP,
            memory: DEFAULT_MEMORY,
            keep_on_drop: false,
        }
    }
}

pub(crate) struct NodeList(Vec<Node>);

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

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Node> {
        self.0.iter()
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut Node> {
        self.0.iter_mut()
    }

    #[expect(dead_code)]
    pub(crate) async fn get_by_id(&self, id: NodeId) -> Option<&Node> {
        self.iter().find(|node| node.id() == id)
    }

    #[expect(dead_code)]
    pub(crate) async fn get_mut_by_id(&mut self, id: NodeId) -> Option<&mut Node> {
        self.iter_mut().find(|node| node.id() == id)
    }

    fn new() -> Self {
        NodeList(Vec::new())
    }

    pub(crate) async fn get_contact_endpoints(&self) -> Vec<String> {
        self.iter().map(|node| node.contact_endpoint()).collect()
    }
}

pub(crate) struct Cluster {
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
    pub(crate) async fn new(opts: ClusterOptions) -> Result<Self, Error> {
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
                        .with_context(
                            || format! {"failed to create root directory {config_dir_path:?}"},
                        )?;
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

        for datacenter_id in 0..opts.nodes_per_dc.len() {
            for _ in 0..opts.nodes_per_dc[datacenter_id] {
                let node_options = NodeOptions {
                    id: cluster
                        .nodes
                        .get_free_node_id()
                        .expect("No available node id"),
                    datacenter_id: datacenter_id.try_into().expect("Too many datacenters"),
                    ..NodeOptions::from_cluster_opts(&cluster.opts)
                };
                cluster.append_node(node_options);
            }
        }

        Ok(cluster)
    }

    pub(crate) async fn init(&mut self) -> Result<(), Error> {
        self.ccm_cmd
            .cluster_create(
                self.opts.name.clone(),
                self.opts.version.clone(),
                self.opts.db_type,
            )
            .populate(&self.opts.nodes_per_dc, Some(self.opts.ip_prefix))
            .run()
            .await
            .map(|_| ())
    }

    /// Executes `ccm updateconf` and applies it for all nodes in the cluster.
    /// It accepts the key-value pairs to update the configuration.
    ///
    /// ### Example
    /// ```
    /// # use crate::ccm::cluster::Cluster;
    /// # async fn check_only_compiles(cluster: &Cluster) -> Result<(), Box<dyn Error>> {
    /// let args = [
    ///     ("client_encryption_options.enabled", "true"),
    ///     ("client_encryption_options.certificate", "db.cert"),
    ///     ("client_encryption_options.keyfile", "db.key"),
    /// ];
    ///
    /// cluster.updateconf(args).await?
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
    pub(crate) async fn updateconf<K, V>(
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
    pub(crate) async fn enable_password_authentication(&mut self) -> Result<(), Error> {
        let args = [("authenticator", "PasswordAuthenticator")];

        self.updateconf(args).await
    }

    /// This method starts the cluster. User can provide optional [`NodeStartOptions`] to control the behavior of the nodes start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub(crate) async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
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

    #[expect(dead_code)]
    pub(crate) async fn add_node(
        &mut self,
        datacenter_id: Option<u16>,
    ) -> Result<&mut Node, Error> {
        let id = self.nodes.get_free_node_id().expect("No available node id");
        let datacenter_id = datacenter_id.unwrap_or(1);
        let node_options = NodeOptions {
            id,
            datacenter_id,
            ..NodeOptions::from_cluster_opts(&self.opts)
        };
        let datacenter_name = format!("dc{datacenter_id}");
        self.ccm_cmd
            .cluster_add_node(self.opts.db_type, node_options.name())
            .dc(datacenter_name)
            .run()
            .await?;
        Ok(self.append_node(node_options))
    }

    #[expect(dead_code)]
    pub(crate) async fn stop(&mut self) -> Result<(), Error> {
        self.ccm_cmd.cluster_stop().run().await.map(|_| ())
    }

    pub(crate) fn set_keep_on_drop(&mut self, value: bool) {
        self.opts.keep_on_drop = value;
        self.tmp_dir_guard.disable_cleanup(value);
    }

    pub(crate) fn mark_as_failed(&mut self) {
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

    #[expect(dead_code)]
    pub(crate) async fn destroy(mut self) -> Result<(), Error> {
        self.ccm_cmd.cluster_remove().run().await.map(|_| ())?;
        self.destroyed = true;
        Ok(())
    }

    pub(crate) async fn make_session_builder(&self) -> SessionBuilder {
        let endpoints = self.nodes.get_contact_endpoints().await;
        SessionBuilder::new().known_nodes(endpoints)
    }

    pub(crate) fn nodes(&self) -> &NodeList {
        &self.nodes
    }

    #[expect(dead_code)]
    pub(crate) fn nodes_mut(&mut self) -> &mut NodeList {
        &mut self.nodes
    }

    pub(crate) fn cluster_dir(&self) -> PathBuf {
        self.tmp_dir_guard.path().join(&self.opts.name)
    }
}
