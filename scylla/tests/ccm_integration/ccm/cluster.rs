use crate::ccm::ROOT_CCM_DIR;

use super::logged_cmd::{LoggedCmd, RunOptions};
use super::node_config::NodeConfig;
use anyhow::{Context, Error};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::net::{AddrParseError, IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::fs::{metadata, File};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;
use tracing::debug;

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

#[allow(dead_code)]
pub(crate) enum NodeStartOption {
    // Don't wait node to start
    NoWait,
    // Wait till other nodes recognize started node
    WaitOtherNotice,
    // Wait till started node report that client port is opened and operational
    WaitForBinaryProto,
}

#[allow(dead_code)]
pub(crate) enum NodeStopOption {
    // Don't wait node to properly stop
    NoWait,
    // Force-terminate node with `kill -9`
    Kill,
}

#[allow(dead_code)]
pub(crate) struct Node {
    status: NodeStatus,
    opts: NodeOptions,
    config: NodeConfig,
    logged_cmd: Arc<LoggedCmd>,
    /// A `--config-dir` for ccm
    config_dir: PathBuf,
}

#[allow(dead_code)]
impl Node {
    fn new(
        opts: NodeOptions,
        config: NodeConfig,
        logged_cmd: Arc<LoggedCmd>,
        config_dir: PathBuf,
    ) -> Self {
        Node {
            opts,
            config,
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

    pub(crate) async fn start(&mut self, opts: Option<&[NodeStartOption]>) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "start".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
        ];
        for opt in opts.unwrap_or(&[
            NodeStartOption::WaitForBinaryProto,
            NodeStartOption::WaitOtherNotice,
        ]) {
            match opt {
                NodeStartOption::NoWait => args.push("--no-wait".to_string()),
                NodeStartOption::WaitOtherNotice => args.push("--wait-other-notice".to_string()),
                NodeStartOption::WaitForBinaryProto => {
                    args.push("--wait-for-binary-proto".to_string())
                }
            }
        }

        self.logged_cmd
            .run_command("ccm", &args, RunOptions::new().with_env(self.get_ccm_env()))
            .await?;
        self.set_status(NodeStatus::Started);
        Ok(())
    }

    pub(crate) async fn stop(&mut self, opts: Option<&[NodeStopOption]>) -> Result<(), Error> {
        let mut args: Vec<String> = vec![
            self.opts.name(),
            "stop".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
        ];
        for opt in opts.unwrap_or(&[]) {
            match opt {
                NodeStopOption::NoWait => args.push("--no-wait".to_string()),
                NodeStopOption::Kill => args.push("--not-gently".to_string()),
            }
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

    pub(crate) async fn sync_config(&self) -> Result<(), Error> {
        let mut args = vec![
            self.opts.name(),
            "updateconf".to_string(),
            "--config-dir".to_string(),
            self.config_dir.to_string_lossy().to_string(),
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
    ccm_dir: TempDir,
    cluster_dir: PathBuf,
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
    /// A `--config-dir` for ccm
    /// Since ccm does not support parallel access to different cluster in the same `config-dir`
    /// we had to isolate each cluster into its own config directory
    fn config_dir(&self) -> &Path {
        self.ccm_dir.path()
    }

    /// A directory that is created by CCM where it stores this cluster
    fn cluster_dir(&self) -> &Path {
        &self.cluster_dir
    }

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
            self.config_dir().to_owned(),
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

        let ccm_dir = TempDir::with_prefix_in(&opts.name, &*ROOT_CCM_DIR)
            .context("Failed to create temp dir for the cluster")?;
        let ccm_dir_path = ccm_dir.path();
        let cluster_dir = ccm_dir.path().join(&opts.name);

        println!("Config dir: {:?}", ccm_dir.path());

        match metadata(ccm_dir_path).await {
            Ok(mt) => {
                if !mt.is_dir() {
                    return Err(Error::msg(format!(
                        "{:?} already exists and it is not a directory",
                        ccm_dir_path
                    )));
                }
            }
            Err(err) => match err.kind() {
                std::io::ErrorKind::NotFound => {
                    tokio::fs::create_dir_all(ccm_dir_path).await.with_context(
                        || format! {"failed to create root directory {:?}", ccm_dir_path},
                    )?;
                }
                _ => {
                    return Err(Error::from(err).context(format!(
                        "failed to create root directory {:?}",
                        ccm_dir_path
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
            ccm_dir,
            cluster_dir,
        };

        for datacenter_id in 0..opts.nodes.len() {
            for _ in 0..opts.nodes[datacenter_id] {
                cluster.append_node(Some((datacenter_id + 1) as u16)).await;
            }
        }

        Ok(cluster)
    }

    pub(crate) async fn init(&mut self) -> Result<(), Error> {
        let cluster_dir = PathBuf::from(self.cluster_dir());
        let config_dir = self.config_dir();
        debug!(
            "Init cluster, cluster_dir: {:?}, config_dir: {:?}",
            cluster_dir, config_dir
        );
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
            config_dir.to_string_lossy().into_owned(),
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
            config_dir.to_string_lossy().into_owned(),
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

    pub(crate) async fn start(&self, opts: Option<&[NodeStartOption]>) -> Result<(), Error> {
        let mut args = vec![
            "start".to_string(),
            "--config-dir".to_string(),
            self.config_dir().to_string_lossy().into_owned(),
        ];
        for opt in opts.unwrap_or(&[
            NodeStartOption::WaitForBinaryProto,
            NodeStartOption::WaitOtherNotice,
        ]) {
            match opt {
                NodeStartOption::NoWait => args.push("--no-wait".to_string()),
                NodeStartOption::WaitOtherNotice => args.push("--wait-other-notice".to_string()),
                NodeStartOption::WaitForBinaryProto => {
                    args.push("--wait-for-binary-proto".to_string())
                }
            }
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
                    &self.config_dir().to_string_lossy().into_owned(),
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
                &self.config_dir().to_string_lossy().into_owned(),
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
                    &self.config_dir().to_string_lossy().into_owned(),
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
