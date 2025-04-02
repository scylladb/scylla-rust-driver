use super::ccm_cmd::Ccm;
use super::ccm_cmd::DBType;
use super::ip_allocator::NetPrefix;
use super::logged_cmd::{LoggedCmd, RunOptions};
use super::{IP_ALLOCATOR, ROOT_CCM_DIR};

use scylla::client::session_builder::SessionBuilder;

use anyhow::{Context, Error};
use tempfile::TempDir;
use tokio::fs::metadata;
use tracing::{debug, info};

use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct ClusterOptions {
    /// Cluster Name
    pub(crate) name: String,
    /// What to database to run: Scylla or Cassandra
    pub(crate) db_type: DBType,
    /// Scylla or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub(crate) version: String,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub(crate) ip_prefix: NetPrefix,
    /// Number of nodes to populate
    /// [1,2] - DC1 contains 1 node, DC2 contains 2 nodes
    pub(crate) nodes: Vec<u8>,
    /// Number of vCPU for Scylla to occupy
    pub(crate) smp: u16,
    /// Amount of MB for Scylla to occupy has to be bigger than smp*512
    pub(crate) memory: u32,
    /// Don't call `ccm remove` when cluster instance is dropped
    pub(crate) do_not_remove_on_drop: bool,
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            name: "".to_string(),
            db_type: DBType::Scylla,
            version: "".to_string(),
            ip_prefix: NetPrefix::empty(),
            nodes: Vec::new(),
            smp: DEFAULT_SMP,
            memory: DEFAULT_MEMORY,
            do_not_remove_on_drop: false,
        }
    }
}

pub(crate) type NodeId = u16;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct NodeOptions {
    /// Node ID, needed to compose ccm commands properly
    pub(crate) id: NodeId,
    /// Database Type: Cassandra, Scylla or Datastax
    pub(crate) db_type: DBType,
    /// Scylla or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub(crate) version: String,
    /// Datacenter ID
    pub(crate) datacenter_id: u16,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub(crate) ip_prefix: NetPrefix,
    /// Number of vCPU for Scylla to occupy
    pub(crate) smp: u16,
    /// Amount of MB for Scylla to occupy has to be bigger than smp*512
    pub(crate) memory: u32,
}

#[allow(dead_code)]
impl NodeOptions {
    fn name(&self) -> String {
        format!("node{}", self.id)
    }

    fn from_cluster_opts(value: &ClusterOptions) -> Self {
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

/// Options to start the node with.
/// It controls `--no-wait`, `--wait-other-notice` and `--wait-for-binary-proto` ccm options.
#[allow(dead_code)]
pub(crate) struct NodeStartOptions {
    /// Don't wait for the node to start. Corresponds to `--no-wait` option in ccm.
    no_wait: bool,
    /// Wait till other nodes recognize started node. Corresponds to `--wait-other-notice` option in ccm.
    wait_other_notice: bool,
    /// Wait till started node report that client port is opened and operational.
    /// Corresponds to `--wait-for-binary-proto` option in ccm.
    wait_for_binary_proto: bool,
}

/// The default start options. Enable following ccm options:
/// - `--wait-other-notice`
/// - `--wait-for-binary-proto`
///
/// The `--no-wait` option is not enabled.
impl Default for NodeStartOptions {
    fn default() -> Self {
        Self {
            no_wait: false,
            wait_other_notice: true,
            wait_for_binary_proto: true,
        }
    }
}

impl NodeStartOptions {
    const NO_WAIT: &'static str = "--no-wait";
    const WAIT_OTHER_NOTICE: &'static str = "--wait-other-notice";
    const WAIT_FOR_BINARY_PROTO: &'static str = "--wait-for-binary-proto";

    /// Creates the default start options. Enables following ccm options:
    /// - `--wait-other-notice`
    /// - `--wait-for-binary-proto`
    ///
    /// The `--no-wait` option is not enabled.
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Enables or disables the `--no-wait` ccm option.
    #[allow(dead_code)]
    pub(crate) fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    /// Enables or disables the `--wait-other-notice` ccm option.
    #[allow(dead_code)]
    pub(crate) fn wait_other_notice(mut self, wait_other_notice: bool) -> Self {
        self.wait_other_notice = wait_other_notice;
        self
    }

    /// Enables or disables the `--wait-for-binary-proto` ccm option.
    #[allow(dead_code)]
    pub(crate) fn wait_for_binary_proto(mut self, wait_for_binary_proto: bool) -> Self {
        self.wait_for_binary_proto = wait_for_binary_proto;
        self
    }
}

/// Options to stop the node with.
/// It allows to control the value of `--no-wait` and `--not-gently` ccm options.
#[allow(dead_code)]
#[derive(Default)]
pub(crate) struct NodeStopOptions {
    /// Dont't wait for the node to properly stop.
    no_wait: bool,
    /// Force-terminate node with `kill -9`.
    not_gently: bool,
}

impl NodeStopOptions {
    const NO_WAIT: &'static str = "--no-wait";
    const NOT_GENTLY: &'static str = "--not-gently";

    /// Create a new `NodeStopOptions` with default values.
    /// All ccm options are disabled by default.
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        NodeStopOptions {
            no_wait: false,
            not_gently: false,
        }
    }

    /// Enables or disables the `--no-wait` cmm option.
    #[allow(dead_code)]
    pub(crate) fn no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    /// Enables or disables the `--not-gently` ccm option.
    #[allow(dead_code)]
    pub(crate) fn not_gently(mut self, not_gently: bool) -> Self {
        self.not_gently = not_gently;
        self
    }
}

#[allow(dead_code)]
pub(crate) struct Node {
    status: NodeStatus,
    opts: NodeOptions,
    logged_cmd: Arc<LoggedCmd>,
    /// A `--config-dir` for ccm
    config_dir: PathBuf,
}

#[allow(dead_code)]
impl Node {
    fn new(opts: NodeOptions, logged_cmd: Arc<LoggedCmd>, config_dir: PathBuf) -> Self {
        Node {
            opts,
            logged_cmd,
            status: NodeStatus::Stopped,
            config_dir,
        }
    }

    pub(crate) fn jmx_port(&self) -> u16 {
        7000 + self.opts.datacenter_id * 100 + self.opts.id
    }

    pub(crate) fn debug_port(&self) -> u16 {
        2000 + self.opts.datacenter_id * 100 + self.opts.id
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

    fn get_ccm_env(&self) -> HashMap<String, String> {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert(
            "SCYLLA_EXT_OPTS".to_string(),
            format!("--smp={} --memory={}M", self.opts.smp, self.opts.memory),
        );
        env
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
    pub(crate) async fn updateconf<K, V>(
        &self,
        key_values: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), Error>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let config_dir = &self.config_dir;
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "updateconf".to_string(),
            "--config-dir".to_string(),
            config_dir.to_string_lossy().into_owned(),
        ];
        for (k, v) in key_values.into_iter() {
            args.push(format!("{}:{}", k.as_ref(), v.as_ref()));
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        Ok(())
    }

    /// This method starts the node. User can provide optional [`NodeStartOptions`] to control the behavior of the node start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub(crate) async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "start".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
        ];

        let NodeStartOptions {
            no_wait,
            wait_other_notice,
            wait_for_binary_proto,
        } = opts.unwrap_or_default();
        if no_wait {
            args.push(NodeStartOptions::NO_WAIT.to_string());
        }
        if wait_other_notice {
            args.push(NodeStartOptions::WAIT_OTHER_NOTICE.to_string());
        }
        if wait_for_binary_proto {
            args.push(NodeStartOptions::WAIT_FOR_BINARY_PROTO.to_string());
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new().with_env(self.get_ccm_env()))
            .await?;
        self.set_status(NodeStatus::Started);
        Ok(())
    }

    pub(crate) async fn stop(&mut self, opts: Option<NodeStopOptions>) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "stop".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
        ];

        let NodeStopOptions {
            no_wait,
            not_gently,
        } = opts.unwrap_or_default();
        if no_wait {
            args.push(NodeStopOptions::NO_WAIT.to_string());
        }
        if not_gently {
            args.push(NodeStopOptions::NOT_GENTLY.to_string());
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new().with_env(self.get_ccm_env()))
            .await?;
        self.set_status(NodeStatus::Stopped);
        Ok(())
    }

    pub(crate) async fn delete(&mut self) -> Result<(), Error> {
        if self.status == NodeStatus::Deleted {
            return Ok(());
        }
        let args: Vec<String> = vec![
            self.opts.name(),
            "remove".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
        ];
        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        self.set_status(NodeStatus::Deleted);
        Ok(())
    }

    fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    pub(crate) fn status(self) -> NodeStatus {
        self.status
    }
}

pub(crate) struct NodeList(Vec<Node>);

#[allow(dead_code)]
impl NodeList {
    fn get_free_node_id(&self) -> Option<NodeId> {
        for node_id in 1..=255 {
            match self.iter().find(|node| node.opts.id == node_id) {
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

    pub(crate) async fn get_by_id(&self, id: NodeId) -> Option<&Node> {
        self.iter().find(|node| node.opts.id == id)
    }

    pub(crate) async fn get_mut_by_id(&mut self, id: NodeId) -> Option<&mut Node> {
        self.iter_mut().find(|node| node.opts.id == id)
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
    nodes: NodeList,
    destroyed: bool,
    logged_cmd: Arc<LoggedCmd>,
    opts: ClusterOptions,
    config_dir: TempDir,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.opts.do_not_remove_on_drop {
            self.destroy_sync().ok();

            // Return the IP prefix to the pool.
            IP_ALLOCATOR
                .lock()
                .expect("Failed to acquire IP_ALLOCATOR lock")
                .free_ip_prefix(&self.opts.ip_prefix)
                .expect("Failed to return ip prefix");
        }
    }
}

pub(crate) const DEFAULT_MEMORY: u32 = 512;
pub(crate) const DEFAULT_SMP: u16 = 1;

#[allow(dead_code)]
impl Cluster {
    /// A `--config-dir` for ccm
    /// Since ccm does not support parallel access to different cluster in the same `config-dir`
    /// we had to isolate each cluster into its own config directory
    fn config_dir(&self) -> &Path {
        self.config_dir.path()
    }

    pub(crate) async fn add_node(&mut self, datacenter_id: Option<u16>) -> &mut Node {
        let node = self.append_node(datacenter_id).await;
        {
            let datacenter = format!("dc{}", node.opts.datacenter_id);

            let mut args: Vec<String> = vec![
                "add".to_string(),
                node.opts.name(),
                "--data-center".to_string(),
                datacenter,
                "--jmx-port".to_string(),
                node.jmx_port().to_string(),
                "--remote-debug-port".to_string(),
                node.debug_port().to_string(),
                "--config-dir".to_string(),
                node.config_dir.to_string_lossy().to_string(),
            ];
            match node.opts.db_type {
                DBType::Scylla => {
                    args.push("--scylla".to_string());
                }
                DBType::Cassandra => {}
            }
            node.logged_cmd
                .run_command("ccm", &args, RunOptions::new().with_env(node.get_ccm_env()))
                .await
                .expect("failed to add node");
        }
        node
    }

    async fn append_node(&mut self, datacenter_id: Option<u16>) -> &mut Node {
        let id = self.nodes.get_free_node_id().expect("No available node id");
        let node = Node::new(
            NodeOptions {
                id,
                datacenter_id: datacenter_id.unwrap_or(1),
                ..NodeOptions::from_cluster_opts(&self.opts)
            },
            self.logged_cmd.clone(),
            self.config_dir().to_owned(),
        );
        self.nodes.push(node);
        self.nodes.0.last_mut().unwrap()
    }

    pub(crate) async fn new(opts: ClusterOptions) -> Result<Self, Error> {
        let mut opts = opts.clone();
        if opts.ip_prefix.is_empty() {
            opts.ip_prefix = IP_ALLOCATOR
                .lock()
                .expect("Failed to acquire IP_ALLOCATOR lock")
                .alloc_ip_prefix()?
        };

        let config_dir = TempDir::with_prefix_in(&opts.name, &*ROOT_CCM_DIR)
            .context("Failed to create temp dir for the cluster")?;
        let config_dir_path = config_dir.path();

        info!("Config dir: {:?}", config_dir.path());

        match metadata(config_dir_path).await {
            Ok(mt) => {
                if !mt.is_dir() {
                    return Err(Error::msg(format!(
                        "{:?} already exists and it is not a directory",
                        config_dir_path
                    )));
                }
            }
            Err(err) => {
                match err.kind() {
                    std::io::ErrorKind::NotFound => {
                        tokio::fs::create_dir_all(config_dir_path).await.with_context(
                        || format! {"failed to create root directory {:?}", config_dir_path},
                    )?;
                    }
                    _ => {
                        return Err(Error::from(err).context(format!(
                            "failed to create root directory {:?}",
                            config_dir_path
                        )));
                    }
                }
            }
        }

        let lcmd = Arc::new(LoggedCmd::new().await);

        let mut cluster = Cluster {
            ccm_cmd: Ccm::new(config_dir.path(), Arc::clone(&lcmd)),
            destroyed: false,
            nodes: NodeList::new(),
            logged_cmd: Arc::clone(&lcmd),
            opts: opts.clone(),
            config_dir,
        };

        for datacenter_id in 0..opts.nodes.len() {
            for _ in 0..opts.nodes[datacenter_id] {
                cluster.append_node(Some((datacenter_id + 1) as u16)).await;
            }
        }

        Ok(cluster)
    }

    pub(crate) async fn init(&mut self) -> Result<(), Error> {
        let config_dir = self.config_dir();
        debug!("Init cluster, config_dir: {:?}", config_dir);
        self.ccm_cmd
            .cluster_create(
                self.opts.name.clone(),
                self.opts.version.clone(),
                self.opts.ip_prefix,
                self.opts.db_type,
            )
            .run()
            .await?;

        self.ccm_cmd
            .cluster_populate(&self.opts.nodes, self.opts.ip_prefix)
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
        &self,
        key_values: impl IntoIterator<Item = (K, V)>,
    ) -> Result<(), Error>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let config_dir = self.config_dir();
        let mut args: Vec<String> = vec![
            "updateconf".to_string(),
            "--config-dir".to_string(),
            config_dir.to_string_lossy().into_owned(),
        ];
        for (k, v) in key_values.into_iter() {
            args.push(format!("{}:{}", k.as_ref(), v.as_ref()));
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        Ok(())
    }

    /// Enables the `PasswordAuthenticator` for the cluster.
    // Consider making it accept an enum in the future. Supported authenticators:
    // https://github.com/scylladb/scylladb/blob/529ff3efa57553eef6b0239b03b81581b70fb9ed/db/config.cc#L1045-L1051.
    pub(crate) async fn enable_password_authentication(&self) -> Result<(), Error> {
        let args = [("authenticator", "PasswordAuthenticator")];

        self.updateconf(args).await
    }

    fn get_ccm_env(&self) -> HashMap<String, String> {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert(
            "SCYLLA_EXT_OPTS".to_string(),
            format!("--smp={} --memory={}M", self.opts.smp, self.opts.memory),
        );
        env
    }

    /// This method starts the cluster. User can provide optional [`NodeStartOptions`] to control the behavior of the nodes start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub(crate) async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        let mut args = vec![
            "start".to_string(),
            "--config-dir".to_string(),
            self.config_dir().to_string_lossy().into_owned(),
        ];

        let NodeStartOptions {
            no_wait,
            wait_other_notice,
            wait_for_binary_proto,
        } = opts.unwrap_or_default();
        if no_wait {
            args.push(NodeStartOptions::NO_WAIT.to_string());
        }
        if wait_other_notice {
            args.push(NodeStartOptions::WAIT_OTHER_NOTICE.to_string());
        }
        if wait_for_binary_proto {
            args.push(NodeStartOptions::WAIT_FOR_BINARY_PROTO.to_string());
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new().with_env(self.get_ccm_env()))
            .await?;
        for node in self.nodes.iter_mut() {
            node.set_status(NodeStatus::Started);
        }
        Ok(())
    }

    pub(crate) async fn stop(&mut self) -> Result<(), Error> {
        if self.destroyed {
            return Ok(());
        }

        match self
            .logged_cmd
            .run_command(
                "ccm",
                &[
                    "stop",
                    &self.opts.name,
                    "--config-dir",
                    &self.config_dir().to_string_lossy(),
                ],
                RunOptions::new(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub(crate) fn set_keep_on_drop(&mut self, value: bool) {
        self.opts.do_not_remove_on_drop = value;
    }

    pub(crate) fn destroy_sync(&mut self) -> Result<(), Error> {
        if self.destroyed {
            return Ok(());
        }

        match Command::new("ccm")
            .args([
                "remove",
                &self.opts.name,
                "--config-dir",
                &self.config_dir().to_string_lossy(),
            ])
            .output()
        {
            Ok(_) => {
                self.destroyed = true;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) async fn destroy(&mut self) -> Result<(), Error> {
        if self.destroyed {
            return Ok(());
        }
        self.stop().await.ok();
        match self
            .logged_cmd
            .run_command(
                "ccm",
                &[
                    "remove",
                    &self.opts.name,
                    "--config-dir",
                    &self.config_dir().to_string_lossy(),
                ],
                RunOptions::new(),
            )
            .await
        {
            Ok(_) => {
                self.destroyed = true;
                for node in self.nodes.iter_mut() {
                    node.set_status(NodeStatus::Deleted);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn make_session_builder(&self) -> SessionBuilder {
        let endpoints = self.nodes.get_contact_endpoints().await;
        SessionBuilder::new().known_nodes(endpoints)
    }

    pub(crate) fn nodes(&self) -> &NodeList {
        &self.nodes
    }

    pub(crate) fn nodes_mut(&mut self) -> &mut NodeList {
        &mut self.nodes
    }
}
