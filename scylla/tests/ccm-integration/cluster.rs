use crate::logged_cli::{LoggedCmd, RunOptions};
use crate::run_options;
use crate::scylla_config::NodeConfig;
use anyhow::{Context, Error};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs::{metadata, File};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DBType {
    SCYLLA,
    CASSANDRA,
    DATASTAX,
}

#[derive(Debug, Clone)]
pub(crate) struct ClusterOptions {
    pub name: String,
    pub db_type: DBType,
    pub version: String,
    pub ip_prefix: String,
    pub root_dir: String,
    pub nodes: Vec<i32>,
    pub smp: i32,
    pub memory: i32,
    pub config: NodeConfig,
    pub keep_on_drop: bool,
}

impl ClusterOptions {
    fn as_ref(&self) -> &ClusterOptions {
        &self
    }

    // A `--config-dir` for ccm
    // Since ccm does not support parallel access to different cluster in the same `config-dir`
    // we had to isolate each cluster into its own config directory
    fn config_dir(&self) -> String {
        format!("{}/{}", self.root_dir, self.name)
    }

    // A directory that is created by CCM where it stores this cluster
    fn cluster_dir(&self) -> String {
        format!("{}/{}/{}", self.root_dir, self.name, self.name)
    }

    // A file to store all ccm logs
    fn ccm_log_file(&self) -> String {
        format!("{}/{}/ccm.log", self.root_dir, self.name)
    }
}

impl ClusterOptions {
    fn new(name: String, version: String) -> Self {
        ClusterOptions {
            name,
            version,
            ..ClusterOptions::default()
        }
    }
}

impl Default for ClusterOptions {
    fn default() -> Self {
        ClusterOptions {
            name: "".to_string(),
            db_type: DBType::SCYLLA,
            version: "".to_string(),
            ip_prefix: "".to_string(),
            root_dir: "/tmp/ccm".to_string(),
            nodes: Vec::new(),
            smp: 1,
            memory: 512,
            config: NodeConfig::default(),
            keep_on_drop: false,
        }
    }
}

#[derive(Debug, Clone)]
struct NodeOptions {
    id: i32,
    db_type: DBType,
    version: String,
    datacenter_id: i32,
    ip_prefix: String,
    config_dir: String,
    smp: i32,
    memory: i32,
}

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
    STOPPED,
    STARTED,
    DELETED,
}

pub(crate) enum NodeStartOption {
    NOWAIT,
    WaitOtherNotice,
    WaitForBinaryProto,
}

pub(crate) enum NodeStopOption {
    NOWAIT,
    KILL,
}

pub(crate) struct Node {
    status: NodeStatus,
    opts: NodeOptions,
    config: NodeConfig,
    logged_cmd: Arc<LoggedCmd>,
}

impl Node {
    fn new(opts: NodeOptions, config: NodeConfig, logged_cmd: Arc<LoggedCmd>) -> Self {
        Node {
            opts,
            config,
            logged_cmd,
            status: NodeStatus::STOPPED,
        }
    }

    pub(crate) fn jmx_port(&self) -> i32 {
        7000 + self.opts.datacenter_id * 100 + self.opts.id
    }

    pub(crate) fn debug_port(&self) -> i32 {
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
            NodeConfig::Null => {
                let ip_addr = format!("{}{}", self.opts.ip_prefix.to_string(), self.opts.id);
                IpAddr::from_str(ip_addr.as_str()).unwrap()
            }
            NodeConfig::String(value) => IpAddr::from_str(value.as_str()).unwrap(),
            _ => {
                panic!("unexpected content of broadcast_rpc_address")
            }
        }
    }

    pub(crate) fn native_transport_port(&self) -> i32 {
        match self.config.get("native_transport_port", NodeConfig::Null) {
            NodeConfig::Null => 9042,
            NodeConfig::Int(value) => value as i32,
            _ => {
                panic!("unexpected content of native_transport_port")
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
        let mut args = vec![
            to_str(self.opts.name()),
            "start",
            "--config-dir",
            &self.opts.config_dir,
        ];
        for opt in opts.unwrap_or(&[
            NodeStartOption::WaitForBinaryProto,
            NodeStartOption::WaitOtherNotice,
        ]) {
            match opt {
                NodeStartOption::NOWAIT => args.push("--no-wait"),
                NodeStartOption::WaitOtherNotice => args.push("--wait-other-notice"),
                NodeStartOption::WaitForBinaryProto => args.push("--wait-for-binary-proto"),
            }
        }

        self.logged_cmd
            .run_command("ccm", &args, run_options!(env = self.get_ccm_env()))
            .await?;
        self._set_status(NodeStatus::STARTED);
        Ok(())
    }

    pub(crate) async fn stop(&mut self, opts: Option<&[NodeStopOption]>) -> Result<(), Error> {
        let mut args = vec![
            to_str(self.opts.name()),
            "stop",
            "--config-dir",
            &self.opts.config_dir,
        ];
        for opt in opts.unwrap_or(&[]) {
            match opt {
                NodeStopOption::NOWAIT => args.push("--no-wait"),
                NodeStopOption::KILL => args.push("--not-gently"),
            }
        }
        self.logged_cmd
            .run_command("ccm", &args, run_options!(env = self.get_ccm_env()))
            .await?;
        self._set_status(NodeStatus::STOPPED);
        Ok(())
    }

    pub(crate) async fn delete(&mut self) -> Result<(), Error> {
        if self.status == NodeStatus::DELETED {
            return Ok(());
        }
        let args = vec![
            to_str(self.opts.name()),
            "remove",
            "--config-dir",
            &self.opts.config_dir,
        ];
        self.logged_cmd.run_command("ccm", &args, None).await?;
        self._set_status(NodeStatus::DELETED);
        Ok(())
    }

    pub(crate) async fn sync_config(&self) -> Result<(), Error> {
        let mut args = vec![
            to_str(self.opts.name()),
            "updateconf",
            "--config-dir",
            &self.opts.config_dir,
        ];
        let config = self.config.to_flat_string();
        let additional = config.split_whitespace().collect::<Vec<_>>();
        args.extend(additional);
        self.logged_cmd.run_command("ccm", &args, None).await?;
        Ok(())
    }

    fn _set_status(&mut self, status: NodeStatus) {
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

impl<'a> Iterator for NodeListIter<'a> {
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

impl NodeList {
    async fn filter<F>(self, filter: F) -> Self
    where
        F: Fn(&Node) -> bool,
    {
        let mut new = Self::new();
        for node in self.0.iter() {
            if filter(node.read().await.deref()) {
                new.push(node.clone());
            }
        }
        new
    }

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
    pub nodes: NodeList,
    destroyed: bool,
    logged_cmd: Arc<LoggedCmd>,
    opts: ClusterOptions,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if !self.opts.keep_on_drop {
            self.destroy_sync().ok();
        }
    }
}

impl Cluster {
    async fn sniff_ip_prefix() -> Result<String, Error> {
        let mut used_ips = HashSet::new();
        let file = File::open("/proc/net/tcp").await?;
        let mut lines = BufReader::new(file).lines();
        while let Some(line) = lines.next_line().await? {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(ip_hex) = parts.get(1) {
                let ip_port: Vec<&str> = ip_hex.split(':').collect();
                if let Some(ip_hex) = ip_port.get(0) {
                    if let Some(ip) = u32::from_str_radix(ip_hex, 16).ok() {
                        used_ips.insert(format!(
                            "{}.{}.{}.",
                            ip & 0xFF,
                            (ip >> 8) & 0xFF,
                            (ip >> 16) & 0xFF,
                        ));
                    }
                }
            }
        }

        for a in 0..=255 {
            for b in 0..=255 {
                if a == 0 && b == 0 {
                    continue;
                }
                let ip_prefix = format!("127.{}.{}.", a, b);
                if !used_ips.contains(&ip_prefix) {
                    return Ok(ip_prefix);
                }
            }
        }
        Err(Error::msg("All ip ranges are busy"))
    }

    async fn get_free_node_id(&self) -> i32 {
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

    pub(crate) async fn add_node(&mut self, datacenter_id: Option<i32>) -> Arc<RwLock<Node>> {
        let node_arc = self._add_node(datacenter_id).await;
        {
            let node = node_arc.read().await;
            let datacenter = format!("dc{}", node.opts.datacenter_id);
            let jmx_port = node.jmx_port().to_string();
            let debug_port = node.debug_port().to_string();
            let mut args: Vec<&str> = vec![
                "add",
                to_str(node.opts.name()),
                "--data-center",
                &datacenter,
                "--jmx-port",
                &jmx_port,
                "--remote-debug-port",
                &debug_port,
                "--config-dir",
                &node.opts.config_dir,
            ];
            if node.opts.db_type == DBType::SCYLLA {
                args.push("--scylla");
            }
            node.logged_cmd
                .run_command("ccm", &args, run_options!(env = node.get_ccm_env()))
                .await
                .expect("failed to add node");
        }
        node_arc
    }

    async fn _add_node(&mut self, datacenter_id: Option<i32>) -> Arc<RwLock<Node>> {
        let node = Node::new(
            NodeOptions {
                id: self.get_free_node_id().await,
                datacenter_id: datacenter_id.unwrap_or(1),
                ..NodeOptions::from(self.opts.as_ref())
            },
            self.opts.config.clone(),
            self.logged_cmd.clone(),
        );
        let node = Arc::new(RwLock::new(node));
        self.nodes.push(node.clone());
        node
    }

    const DEFAULT_MEMORY: i32 = 512;
    const DEFAULT_SMP: i32 = 1;

    pub(crate) async fn new(opts: ClusterOptions) -> Result<Self, Error> {
        let mut opts = opts.clone();
        if opts.ip_prefix.is_empty() {
            opts.ip_prefix = Self::sniff_ip_prefix().await?
        };

        if !opts.ip_prefix.ends_with(".") {
            opts.ip_prefix = format!("{}.", opts.ip_prefix);
        }

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
                        .context(
                            format! {"failed to create root directory {}", opts.config_dir()},
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

        let mut lcmd = LoggedCmd::new();
        let ccm_log_file = opts.ccm_log_file();
        lcmd.set_log_file(ccm_log_file.clone())
            .await
            .context(format!("failed to set log file {}", ccm_log_file))?;

        let mut cluster = Cluster {
            destroyed: false,
            nodes: NodeList::new(),
            logged_cmd: Arc::new(lcmd),
            opts: opts.clone(),
        };

        for datacenter_id in 0..opts.nodes.len() {
            for _ in 0..opts.nodes[datacenter_id] {
                cluster._add_node(Some((datacenter_id + 1) as i32)).await;
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
                .context(format!(
                    "failed to remove config dir {}",
                    cluster_dir.to_str().unwrap()
                ))?;
        }
        let mut args: Vec<&str> = vec![
            "create",
            &self.opts.name,
            "-v",
            &self.opts.version,
            "-i",
            &self.opts.ip_prefix,
            "--config-dir",
            &config_dir,
        ];
        if self.opts.db_type == DBType::SCYLLA {
            args.push("--scylla");
        }
        self.logged_cmd.run_command("ccm", &args, None).await?;
        let nodes_str = self
            .opts
            .nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<String>>()
            .join(":");

        let args: Vec<&str> = vec![
            "populate",
            "-i",
            &self.opts.ip_prefix,
            "-n",
            nodes_str.as_str(),
            "--config-dir",
            &config_dir,
        ];
        self.logged_cmd.run_command("ccm", &args, None).await?;
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
        let mut args = vec!["start", "--config-dir", to_str(self.opts.config_dir())];
        for opt in opts.unwrap_or(&[
            NodeStartOption::WaitForBinaryProto,
            NodeStartOption::WaitOtherNotice,
        ]) {
            match opt {
                NodeStartOption::NOWAIT => args.push("--no-wait"),
                NodeStartOption::WaitOtherNotice => args.push("--wait-other-notice"),
                NodeStartOption::WaitForBinaryProto => args.push("--wait-for-binary-proto"),
            }
        }

        self.logged_cmd
            .run_command("ccm", &args, run_options!(env = self.get_ccm_env()))
            .await?;
        for node in self.nodes.iter() {
            let mut node = node.write().await;
            node._set_status(NodeStatus::STARTED);
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
                None,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn set_keep_on_drop(&mut self, value: bool) {
        self.opts.keep_on_drop = value;
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
                None,
            )
            .await
        {
            Ok(_) => {
                self.destroyed = true;
                for node in self.nodes.iter() {
                    node.clone().write().await._set_status(NodeStatus::DELETED);
                }
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }
}

fn to_str<'a>(value: String) -> &'a str {
    Box::leak(value.into_boxed_str())
}
