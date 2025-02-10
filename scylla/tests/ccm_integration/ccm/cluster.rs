use super::logged_cmd::{LoggedCmd, RunOptions};
use super::node_config::NodeConfig;
use anyhow::{Context, Error};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::{metadata, File};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DBType {
    Scylla,
    Cassandra,
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterOptions {
    /// Cluster Name
    pub(crate) name: String,
    /// What to database to run: Scylla, Cassandra or Datastax
    pub(crate) db_type: DBType,
    /// Scylla or Cassandra version string that goes to CCM.
    /// Examples: `release:6.2.2`, `unstable:master/2021-05-24T17:16:53Z`
    pub(crate) version: String,
    /// CCM allocates node ip addresses based on this prefix:
    /// if ip_prefix = `127.0.1.`, then `node1` address is `127.0.1.1`, `node2` address is `127.0.1.2`
    pub(crate) ip_prefix: NetPrefix,
    /// CCM does not allow to have one active cluster within one config directory
    /// To have more than two active CCM cluster at the same time we isolate each cluster into separate
    /// config director, each config directory is created in `root_dir`.
    /// Example: root_dir = `/tmp/ccm`, config directory for cluster `test_cluster_1` is going be `/tmp/ccm/test_cluster_1`
    ///  and cluster directory (that is created and managed  by CCM) for this cluster is going to be `/tmp/ccm/test_cluster_1/test_cluster_1`
    pub(crate) root_dir: String,
    /// Number of nodes to populate
    /// [1,2] - DC1 contains 1 node, DC2 contains 2 nodes
    pub(crate) nodes: Vec<u8>,
    /// Number of vCPU for Scylla to occupy
    pub(crate) smp: u16,
    /// Amount of MB for Scylla to occupy has to be bigger than smp*512
    pub(crate) memory: u32,
    /// scylla.yaml or cassandra.yaml
    pub(crate) config: NodeConfig,
    /// Don't call `ccm remove` when cluster instance is dropped
    pub(crate) do_not_remove_on_drop: bool,
}

impl ClusterOptions {
    /// A `--config-dir` for ccm
    /// Since ccm does not support parallel access to different cluster in the same `config-dir`
    /// we had to isolate each cluster into its own config directory
    fn config_dir(&self) -> String {
        format!("{}/{}", self.root_dir, self.name)
    }

    /// A directory that is created by CCM where it stores this cluster
    fn cluster_dir(&self) -> String {
        format!("{}/{}/{}", self.root_dir, self.name, self.name)
    }
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            name: "".to_string(),
            db_type: DBType::Scylla,
            version: "".to_string(),
            ip_prefix: NetPrefix::empty(),
            root_dir: "/tmp/ccm".to_string(),
            nodes: Vec::new(),
            smp: DEFAULT_SMP,
            memory: DEFAULT_MEMORY,
            config: NodeConfig::default(),
            do_not_remove_on_drop: false,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct NodeOptions {
    /// Node ID, needed to compose ccm commands properly
    pub(crate) id: u16,
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
    /// A `--config-dir` for ccm
    pub(crate) config_dir: String,
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

    fn from(value: &ClusterOptions) -> Self {
        NodeOptions {
            id: 0,
            datacenter_id: 1,
            db_type: value.db_type.clone(),
            version: value.version.clone(),
            config_dir: value.config_dir().to_string(),
            ip_prefix: value.ip_prefix.clone(),
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
    const NO_WAIT: &str = "--no-wait";
    const WAIT_OTHER_NOTICE: &str = "--wait-other-notice";
    const WAIT_FOR_BINARY_PROTO: &str = "--wait-for-binary-proto";

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
pub(crate) struct NodeStopOptions {
    /// Dont't wait for the node to properly stop.
    no_wait: bool,
    /// Force-terminate node with `kill -9`.
    not_gently: bool,
}

impl NodeStopOptions {
    const NO_WAIT: &str = "--no-wait";
    const NOT_GENTLY: &str = "--not-gently";

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
    pub(crate) fn no_wait(&mut self, no_wait: bool) -> &mut Self {
        self.no_wait = no_wait;
        self
    }

    /// Enables or disables the `--not-gently` ccm option.
    #[allow(dead_code)]
    pub(crate) fn not_gently(&mut self, not_gently: bool) -> &mut Self {
        self.not_gently = not_gently;
        self
    }
}

#[allow(dead_code)]
pub(crate) struct Node {
    status: NodeStatus,
    opts: NodeOptions,
    config: NodeConfig,
    logged_cmd: Arc<LoggedCmd>,
}

#[allow(dead_code)]
impl Node {
    fn new(opts: NodeOptions, config: NodeConfig, logged_cmd: Arc<LoggedCmd>) -> Self {
        Node {
            opts,
            config,
            logged_cmd,
            status: NodeStatus::Stopped,
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
        match self.config.get("broadcast_rpc_address", NodeConfig::Null) {
            NodeConfig::Null => self.opts.ip_prefix.to_ipaddress(self.opts.id),
            NodeConfig::String(value) => IpAddr::from_str(value.as_str())
                .with_context(|| format!("unexpected content of broadcast_rpc_address: {value}"))
                .unwrap(),
            other => {
                panic!("unexpected content of broadcast_rpc_address: {other:#?}")
            }
        }
    }

    pub(crate) fn native_transport_port(&self) -> u16 {
        match self.config.get("native_transport_port", NodeConfig::Null) {
            NodeConfig::Null => 9042,
            NodeConfig::Int(value) => value as u16,
            other => {
                panic!("unexpected content of native_transport_port: {other:#?}")
            }
        }
    }

    fn get_ccm_env(&self) -> HashMap<String, String> {
        let mut env: HashMap<String, String> = HashMap::new();
        env.insert(
            "SCYLLA_EXT_OPTS".to_string(),
            format!("--smp={} --memory={}M", self.opts.smp, self.opts.memory),
        );
        env
    }

    /// This method starts the node. User can provide optional [`NodeStartOptions`] to control the behavior of the node start.
    /// If `None` is provided, the default options are used (see the implementation of Default for [`NodeStartOptions`]).
    pub(crate) async fn start(&mut self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "start".to_string(),
            "--config-dir".to_string(),
            self.opts.config_dir.clone(),
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

    pub(crate) async fn stop(&mut self, opts: NodeStopOptions) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "stop".to_string(),
            "--config-dir".to_string(),
            self.opts.config_dir.clone(),
        ];

        let NodeStopOptions {
            no_wait,
            not_gently,
        } = opts;
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
            self.opts.config_dir.clone(),
        ];
        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        self.set_status(NodeStatus::Deleted);
        Ok(())
    }

    pub(crate) async fn sync_config(&self) -> Result<(), Error> {
        let mut args = vec![
            self.opts.name(),
            "updateconf".to_string(),
            "--config-dir".to_string(),
            self.opts.config_dir.clone(),
        ];

        // converting config to space separated list of `key:value`
        // example:
        //      value: 1.1.1.1
        //      obj:
        //          value2: 2.2.2.2
        // should become "value:1.1.1.1 obj.value2:2.2.2.2"
        let additional: Vec<String> = self
            .config
            .to_flat_string()
            .split_whitespace()
            .map(String::from)
            .collect();

        args.extend(additional);
        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        Ok(())
    }

    fn set_status(&mut self, status: NodeStatus) {
        self.status = status;
    }

    pub(crate) fn status(self) -> NodeStatus {
        self.status
    }
}

pub(crate) struct NodeList(Vec<Arc<RwLock<Node>>>);

struct NodeListIter<'a> {
    list: &'a NodeList,
    index: usize,
}

impl Iterator for NodeListIter<'_> {
    type Item = Arc<RwLock<Node>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.list.0.len() {
            let node = self.list.0[self.index].clone();
            self.index += 1;
            Some(node)
        } else {
            None
        }
    }
}

#[allow(dead_code)]
impl NodeList {
    fn push(&mut self, node: Arc<RwLock<Node>>) {
        self.0.push(node);
    }

    fn iter(&self) -> NodeListIter {
        NodeListIter {
            list: self,
            index: 0,
        }
    }

    fn new() -> Self {
        NodeList(Vec::new())
    }

    pub(crate) async fn get_contact_endpoints(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for node in self.iter() {
            let node = node.read().await;
            let cp = node.contact_endpoint();
            out.push(cp);
        }
        out
    }
}

pub(crate) struct Cluster {
    pub(crate) nodes: NodeList,
    destroyed: bool,
    logged_cmd: Arc<LoggedCmd>,
    opts: ClusterOptions,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.opts.do_not_remove_on_drop {
            self.destroy_sync().ok();
        }
    }
}

pub(crate) const DEFAULT_MEMORY: u32 = 512;
pub(crate) const DEFAULT_SMP: u16 = 1;

#[allow(dead_code)]
impl Cluster {
    /// This method looks at all busy /24 networks in 127.0.0.0/8 and return first available
    /// network goes as busy if anything is listening on any port and any address in this network
    /// 127.0.0.0/24 is skipped and never returned
    async fn sniff_ipv4_prefix() -> Result<NetPrefix, Error> {
        let mut used_ips: HashSet<IpAddr> = HashSet::new();
        let file = File::open("/proc/net/tcp").await?;
        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines.next_line().await? {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(ip_hex) = parts.get(1) {
                let ip_port: Vec<&str> = ip_hex.split(':').collect();
                if let Some(ip_hex) = ip_port.first() {
                    if let Ok(ip) = u32::from_str_radix(ip_hex, 16) {
                        used_ips.insert(
                            Ipv4Addr::new(ip as u8, (ip >> 8) as u8, (ip >> 16) as u8, 0).into(),
                        );
                    }
                }
            }
        }

        for a in 0..=255 {
            for b in 0..=255 {
                if a == 0 && b == 0 {
                    continue;
                }
                let ip_prefix: IpAddr = Ipv4Addr::new(127, a, b, 0).into();
                if !used_ips.contains(&ip_prefix) {
                    return Ok(ip_prefix.into());
                }
            }
        }
        Err(Error::msg("All ip ranges are busy"))
    }

    async fn get_free_node_id(&self) -> u16 {
        'outer: for node_id in 1..=255 {
            for node in self.nodes.iter() {
                let node = node.read().await;
                if node.opts.id == node_id {
                    continue 'outer;
                }
            }
            return node_id;
        }
        256
    }

    pub(crate) async fn add_node(&mut self, datacenter_id: Option<u16>) -> Arc<RwLock<Node>> {
        let node_arc = self.append_node(datacenter_id).await;
        {
            let node = node_arc.read().await;
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
                node.opts.config_dir.clone(),
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
        node_arc
    }

    async fn append_node(&mut self, datacenter_id: Option<u16>) -> Arc<RwLock<Node>> {
        let node = Node::new(
            NodeOptions {
                id: self.get_free_node_id().await,
                datacenter_id: datacenter_id.unwrap_or(1),
                ..NodeOptions::from(&self.opts)
            },
            self.opts.config.clone(),
            self.logged_cmd.clone(),
        );
        let node = Arc::new(RwLock::new(node));
        self.nodes.push(node.clone());
        node
    }

    pub(crate) async fn new(opts: ClusterOptions) -> Result<Self, Error> {
        let mut opts = opts.clone();
        if opts.ip_prefix.is_empty() {
            opts.ip_prefix = Self::sniff_ipv4_prefix().await?
        };

        match metadata(opts.config_dir().as_str()).await {
            Ok(mt) => {
                if !mt.is_dir() {
                    return Err(Error::msg(format!(
                        "{} already exists and it is not a dictionary",
                        opts.config_dir()
                    )));
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir_all(opts.config_dir().as_str())
                        .await
                        .with_context(
                            || format! {"failed to create root directory {}", opts.config_dir()},
                        )?;
                }
                _ => {
                    return Err(Error::from(err).context(format!(
                        "failed to create root directory {}",
                        opts.config_dir()
                    )));
                }
            },
        }

        let lcmd = LoggedCmd::new().await;

        let mut cluster = Cluster {
            destroyed: false,
            nodes: NodeList::new(),
            logged_cmd: Arc::new(lcmd),
            opts: opts.clone(),
        };

        for datacenter_id in 0..opts.nodes.len() {
            for _ in 0..opts.nodes[datacenter_id] {
                cluster.append_node(Some((datacenter_id + 1) as u16)).await;
            }
        }

        Ok(cluster)
    }

    pub(crate) async fn init(&mut self) -> Result<(), Error> {
        let cluster_dir = PathBuf::from(self.opts.cluster_dir());
        let config_dir = self.opts.config_dir();
        if cluster_dir.exists() {
            tokio::fs::remove_dir_all(&cluster_dir)
                .await
                .with_context(|| {
                    format!(
                        "failed to remove config dir {}",
                        cluster_dir.to_str().unwrap()
                    )
                })?;
        }
        let mut args: Vec<String> = vec![
            "create".to_string(),
            self.opts.name.clone(),
            "-v".to_string(),
            self.opts.version.clone(),
            "-i".to_string(),
            self.opts.ip_prefix.to_string(),
            "--config-dir".to_string(),
            config_dir.clone(),
        ];
        if self.opts.db_type == DBType::Scylla {
            args.push("--scylla".to_string());
        }
        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        let nodes_str = self
            .opts
            .nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<String>>()
            .join(":");

        let args = vec![
            "populate".to_string(),
            "-i".to_string(),
            self.opts.ip_prefix.to_string(),
            "-n".to_string(),
            nodes_str,
            "--config-dir".to_string(),
            config_dir.clone(),
        ];
        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new())
            .await?;
        Ok(())
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
    pub(crate) async fn start(&self, opts: Option<NodeStartOptions>) -> Result<(), Error> {
        let mut args = vec![
            "start".to_string(),
            "--config-dir".to_string(),
            self.opts.config_dir(),
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
        for node in self.nodes.iter() {
            let mut node = node.write().await;
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
                    &self.opts.config_dir(),
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
                &self.opts.config_dir(),
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
                    &self.opts.config_dir(),
                ],
                RunOptions::new(),
            )
            .await
        {
            Ok(_) => {
                self.destroyed = true;
                for node in self.nodes.iter() {
                    node.clone().write().await.set_status(NodeStatus::Deleted);
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

// fn to_str<'a, 'b>(value: &'a String) -> &'b str {
//     let tmp = value.clone();
//     tmp.as_str()
//     // let s = String::from("hello");
//     // let boxed_str: Box<str> = s.into_boxed_str(); // Moves and converts the String
//     // let s_ref: &'a str = &boxed_str;
//     // s_ref
//
//     // println!("{}", s_ref);
//     // let tmp = value.into_boxed_str();
//     // let tmp2: &'a str = tmp;
//     // tmp2
// }

#[derive(Debug, Clone)]
pub(crate) struct NetPrefix(IpAddr);

impl NetPrefix {
    fn empty() -> Self {
        NetPrefix(IpAddr::V6(Ipv6Addr::UNSPECIFIED))
    }

    fn is_empty(&self) -> bool {
        self.0.is_unspecified()
    }

    fn from_string(value: String) -> Result<Self, AddrParseError> {
        Ok(IpAddr::from_str(&value)?.into())
    }

    fn to_str(&self) -> String {
        match self.0 {
            IpAddr::V4(v4) => {
                let octets = v4.octets();
                format!("{}.{}.{}.", octets[0], octets[1], octets[2])
            }
            IpAddr::V6(v6) => {
                let mut segments = v6.segments();
                segments[7] = 0; // Set last segment to 0
                let new_ip = Ipv6Addr::from(segments);
                let formatted = new_ip.to_string();
                formatted.rsplit_once(':').map(|x| x.0).unwrap().to_string() + ":"
            }
        }
    }

    fn to_ipaddress(&self, id: u16) -> IpAddr {
        match self.0 {
            IpAddr::V4(v4) => {
                let mut octets = v4.octets();
                octets[3] = id as u8;
                IpAddr::V4(Ipv4Addr::from(octets))
            }
            IpAddr::V6(v6) => {
                let mut segments = v6.segments();
                segments[7] = id;
                IpAddr::V6(Ipv6Addr::from(segments))
            }
        }
    }
}

impl Display for NetPrefix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<IpAddr> for NetPrefix {
    fn from(ip: IpAddr) -> Self {
        NetPrefix(ip)
    }
}

impl From<NetPrefix> for String {
    fn from(value: NetPrefix) -> Self {
        value.0.to_string()
    }
}

impl Default for NetPrefix {
    fn default() -> Self {
        NetPrefix::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ipv4_prefix() {
        let ip = NetPrefix::from_string("192.168.1.100".to_string()).unwrap();
        assert_eq!(ip.to_str(), "192.168.1.");
    }

    #[test]
    fn test_ipv4_loopback() {
        let ip = NetPrefix::from_string("127.0.0.1".to_string()).unwrap();
        assert_eq!(ip.to_str(), "127.0.0.");
    }

    #[test]
    fn test_ipv4_edge_case() {
        let ip = NetPrefix::from_string("0.0.0.0".to_string()).unwrap();
        assert_eq!(ip.to_str(), "0.0.0.");
    }

    #[test]
    fn test_ipv6_prefix() {
        let ip =
            NetPrefix::from_string("2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string()).unwrap();
        assert_eq!(ip.to_str(), "2001:db8:85a3::8a2e:370:");
    }

    #[test]
    fn test_ipv6_loopback() {
        let ip = NetPrefix::from_string("::1".to_string()).unwrap();
        assert_eq!(ip.to_str(), "::");
    }

    #[test]
    fn test_ipv6_shortened() {
        let ip = NetPrefix::from_string("2001:db8::ff00:42:8329".to_string()).unwrap();
        assert_eq!(ip.to_str(), "2001:db8::ff00:42:");
    }
}
