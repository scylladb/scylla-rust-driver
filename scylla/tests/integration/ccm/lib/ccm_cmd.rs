use std::path::Path;
use std::process::ExitStatus;
use std::sync::Arc;

use crate::ccm::lib::ip_allocator::NetPrefix;

use super::logged_cmd::LoggedCmd;
use super::logged_cmd::RunOptions;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum DBType {
    Scylla,
    #[allow(dead_code)]
    Cassandra,
}

pub(crate) struct Ccm {
    config_dir: String,
    cmd: Arc<LoggedCmd>,
}

impl Ccm {
    pub(crate) fn new(config_dir: &Path, cmd: Arc<LoggedCmd>) -> Self {
        Self {
            config_dir: config_dir
                .to_str()
                .expect("Config dir not representable as str")
                .to_string(),
            cmd,
        }
    }
}

pub(crate) struct ClusterCreate<'ccm> {
    ccm: &'ccm mut Ccm,
    name: String,
    version: String,
    ip_prefix: String,
    db_type: DBType,
}

impl Ccm {
    pub(crate) fn cluster_create(
        &mut self,
        name: String,
        version: String,
        ip_prefix: NetPrefix,
        db_type: DBType,
    ) -> ClusterCreate<'_> {
        ClusterCreate {
            ccm: self,
            name,
            version,
            ip_prefix: ip_prefix.to_string(),
            db_type,
        }
    }
}

impl ClusterCreate<'_> {
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let mut args: Vec<&str> = vec![
            "create",
            self.name.as_str(),
            "--version",
            self.version.as_str(),
            "--ipprefix",
            self.ip_prefix.as_str(),
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];
        if self.db_type == DBType::Scylla {
            args.push("--scylla");
        }
        self.ccm
            .cmd
            .run_command("ccm", &args, RunOptions::new())
            .await
    }
}

pub(crate) struct ClusterPopulate<'ccm> {
    ccm: &'ccm mut Ccm,
    nodes: String,
    ip_prefix: String,
}

impl Ccm {
    pub(crate) fn cluster_populate(
        &mut self,
        nodes: &[u8],
        ip_prefix: NetPrefix,
    ) -> ClusterPopulate<'_> {
        let nodes_string = nodes
            .iter()
            .map(|node| node.to_string())
            .collect::<Vec<String>>()
            .join(":");
        ClusterPopulate {
            ccm: self,
            nodes: nodes_string,
            ip_prefix: ip_prefix.to_string(),
        }
    }
}

impl ClusterPopulate<'_> {
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let args: Vec<&str> = vec![
            "populate",
            "--ipprefix",
            self.ip_prefix.as_str(),
            "--nodes",
            self.nodes.as_str(),
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];
        self.ccm
            .cmd
            .run_command("ccm", &args, RunOptions::new())
            .await
    }
}
