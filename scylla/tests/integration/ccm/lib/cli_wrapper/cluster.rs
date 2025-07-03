use std::collections::HashMap;
use std::fmt::Write;
use std::path::Path;
use std::process::ExitStatus;
use std::sync::Arc;

use itertools::Itertools;

use crate::ccm::lib::ip_allocator::NetPrefix;
use crate::ccm::lib::logged_cmd::{LoggedCmd, RunOptions};

use super::node::NodeCcm;
use super::{DBType, NodeStartOptions};

pub(crate) struct Ccm {
    config_dir: String,
    cmd: Arc<LoggedCmd>,
}

impl Ccm {
    pub(crate) fn new(config_dir: impl AsRef<Path>, cmd: Arc<LoggedCmd>) -> Self {
        Self {
            config_dir: config_dir
                .as_ref()
                .to_str()
                .expect("Config dir not representable as str")
                .to_string(),
            cmd,
        }
    }

    pub(crate) fn for_node(&self, node_name: String) -> NodeCcm {
        NodeCcm::new(self.config_dir.clone(), Arc::clone(&self.cmd), node_name)
    }
}

///
/// ccm create
///
pub(crate) struct ClusterCreate<'ccm> {
    ccm: &'ccm mut Ccm,
    name: String,
    version: String,
    db_type: DBType,
    ip_prefix: Option<String>,
    nodes: Option<String>,
}

impl Ccm {
    pub(crate) fn cluster_create(
        &mut self,
        name: String,
        version: String,
        db_type: DBType,
    ) -> ClusterCreate<'_> {
        ClusterCreate {
            ccm: self,
            name,
            version,
            db_type,
            ip_prefix: None,
            nodes: None,
        }
    }
}

impl ClusterCreate<'_> {
    pub(crate) fn populate(mut self, nodes: &[u8], ipprefix: Option<NetPrefix>) -> Self {
        let nodes_string = nodes.iter().map(|node| node.to_string()).join(":");
        self.nodes = Some(nodes_string);
        self.ip_prefix = ipprefix.map(|p| p.to_string());
        self
    }

    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let mut args: Vec<&str> = vec![
            "create",
            self.name.as_str(),
            "--version",
            self.version.as_str(),
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];
        if let Some(nodes) = self.nodes.as_ref() {
            args.push("--nodes");
            args.push(nodes.as_str())
        }
        if let Some(ipprefix) = self.ip_prefix.as_ref() {
            args.push("--ipprefix");
            args.push(ipprefix.as_str());
        }
        if self.db_type == DBType::Scylla {
            args.push("--scylla");
        }
        self.ccm
            .cmd
            .run_command("ccm", &args, RunOptions::new())
            .await
    }
}

///
/// ccm updateconf
///
pub(crate) struct ClusterUpdateconf<'ccm> {
    ccm: &'ccm mut Ccm,
    args: Vec<String>,
}

impl Ccm {
    pub(crate) fn cluster_updateconf(&mut self) -> ClusterUpdateconf<'_> {
        let cfg_dir = self.config_dir.as_str().to_string();
        ClusterUpdateconf {
            ccm: self,
            args: vec![
                "updateconf".to_string(),
                "--config-dir".to_string(),
                cfg_dir,
            ],
        }
    }
}

impl ClusterUpdateconf<'_> {
    pub(crate) fn config<K, V>(mut self, key_values: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        for (k, v) in key_values.into_iter() {
            self.args.push(format!("{}:{}", k.as_ref(), v.as_ref()));
        }
        self
    }
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        self.ccm
            .cmd
            .run_command("ccm", &self.args, RunOptions::new())
            .await
    }
}

///
/// ccm start
///
pub(crate) struct ClusterStart<'ccm> {
    ccm: &'ccm mut Ccm,
    wait_opts: Option<NodeStartOptions>,
    scylla_smp: Option<u16>,
    scylla_mem_megabytes: Option<u32>,
}

impl Ccm {
    pub(crate) fn cluster_start(&mut self) -> ClusterStart<'_> {
        ClusterStart {
            ccm: self,
            wait_opts: None,
            scylla_smp: None,
            scylla_mem_megabytes: None,
        }
    }
}

impl ClusterStart<'_> {
    pub(crate) fn wait_options(mut self, options: Option<NodeStartOptions>) -> Self {
        self.wait_opts = options;
        self
    }
    pub(crate) fn scylla_smp(mut self, smp: u16) -> Self {
        self.scylla_smp = Some(smp);
        self
    }

    pub(crate) fn scylla_mem_megabytes(mut self, mem_megabytes: u32) -> Self {
        self.scylla_mem_megabytes = Some(mem_megabytes);
        self
    }

    fn get_ccm_env(&self) -> HashMap<String, String> {
        let mut value = String::new();
        if let Some(smp) = self.scylla_smp {
            write!(value, "--smp={smp} ").unwrap();
        }
        if let Some(mem_megabytes) = self.scylla_mem_megabytes {
            write!(value, "--memory={mem_megabytes}M ").unwrap();
        }

        let mut env = HashMap::new();
        if !value.is_empty() {
            env.insert("SCYLLA_EXT_OPTS".to_string(), value);
        }

        env
    }

    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let env = self.get_ccm_env();
        let mut args = vec!["start", "--config-dir", self.ccm.config_dir.as_str()];

        let NodeStartOptions {
            no_wait,
            wait_other_notice,
            wait_for_binary_proto,
        } = self.wait_opts.unwrap_or_default();
        if no_wait {
            args.push(NodeStartOptions::NO_WAIT);
        }
        if wait_other_notice {
            args.push(NodeStartOptions::WAIT_OTHER_NOTICE);
        }
        if wait_for_binary_proto {
            args.push(NodeStartOptions::WAIT_FOR_BINARY_PROTO);
        }

        self.ccm
            .cmd
            .run_command("ccm", &args, RunOptions::new().with_env(env))
            .await
    }
}

///
/// ccm remove
///
pub(crate) struct ClusterRemove<'ccm> {
    ccm: &'ccm mut Ccm,
}

impl Ccm {
    pub(crate) fn cluster_remove(&mut self) -> ClusterRemove<'_> {
        ClusterRemove { ccm: self }
    }
}

impl ClusterRemove<'_> {
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let args = &["remove", "--config-dir", self.ccm.config_dir.as_str()];

        self.ccm
            .cmd
            .run_command("ccm", args, RunOptions::new())
            .await
    }

    pub(crate) fn run_sync(self) -> Result<ExitStatus, anyhow::Error> {
        let args = &["remove", "--config-dir", self.ccm.config_dir.as_str()];

        self.ccm
            .cmd
            .run_command_sync("ccm", args, RunOptions::new())
    }
}

///
/// ccm stop
///
pub(crate) struct ClusterStop<'ccm> {
    ccm: &'ccm mut Ccm,
}

impl Ccm {
    pub(crate) fn cluster_stop(&mut self) -> ClusterStop<'_> {
        ClusterStop { ccm: self }
    }
}

impl ClusterStop<'_> {
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let args = &["stop", "--config-dir", self.ccm.config_dir.as_str()];

        self.ccm
            .cmd
            .run_command("ccm", args, RunOptions::new())
            .await
    }
}

///
/// ccm add
///
pub(crate) struct ClusterAddNode<'ccm> {
    ccm: &'ccm mut Ccm,
    name: String,
    db_type: DBType,
    dc: Option<String>,
}

impl Ccm {
    pub(crate) fn cluster_add_node(&mut self, db_type: DBType, name: String) -> ClusterAddNode<'_> {
        ClusterAddNode {
            ccm: self,
            db_type,
            name,
            dc: None,
        }
    }
}

impl ClusterAddNode<'_> {
    pub(crate) fn dc(mut self, dc: String) -> Self {
        self.dc = Some(dc);
        self
    }

    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let mut args = vec![
            "add",
            "--config-dir",
            self.ccm.config_dir.as_str(),
            self.name.as_str(),
        ];

        if let Some(dc) = self.dc.as_ref() {
            args.push("--data-center");
            args.push(dc.as_str())
        }

        match self.db_type {
            DBType::Scylla => {
                args.push("--scylla");
            }
            DBType::Cassandra => {}
        }

        self.ccm
            .cmd
            .run_command("ccm", args, RunOptions::new())
            .await
    }
}
