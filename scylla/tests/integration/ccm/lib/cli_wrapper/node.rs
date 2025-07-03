use std::collections::HashMap;
use std::fmt::Write;
use std::process::ExitStatus;
use std::sync::Arc;

use crate::ccm::lib::logged_cmd::{LoggedCmd, RunOptions};

use super::{NodeStartOptions, NodeStopOptions};

pub(crate) struct NodeCcm {
    config_dir: String,
    cmd: Arc<LoggedCmd>,
    node_name: String,
}

impl NodeCcm {
    pub(crate) fn new(config_dir: String, cmd: Arc<LoggedCmd>, node_name: String) -> Self {
        Self {
            config_dir,
            cmd,
            node_name,
        }
    }
}

///
/// ccm <node> start
///
pub(crate) struct NodeStart<'ccm> {
    ccm: &'ccm mut NodeCcm,
    wait_opts: Option<NodeStartOptions>,
    scylla_smp: Option<u16>,
    scylla_mem_megabytes: Option<u32>,
}

impl NodeCcm {
    pub(crate) fn node_start(&mut self) -> NodeStart<'_> {
        NodeStart {
            ccm: self,
            wait_opts: None,
            scylla_smp: None,
            scylla_mem_megabytes: None,
        }
    }
}

impl NodeStart<'_> {
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
        let mut args = vec![
            self.ccm.node_name.as_str(),
            "start",
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];

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
/// ccm <node> stop
///
pub(crate) struct NodeStop<'ccm> {
    ccm: &'ccm mut NodeCcm,
    wait_opts: Option<NodeStopOptions>,
}

impl NodeCcm {
    pub(crate) fn node_stop(&mut self) -> NodeStop<'_> {
        NodeStop {
            ccm: self,
            wait_opts: None,
        }
    }
}

impl NodeStop<'_> {
    pub(crate) fn wait_options(mut self, options: Option<NodeStopOptions>) -> Self {
        self.wait_opts = options;
        self
    }

    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let mut args = vec![
            self.ccm.node_name.as_str(),
            "stop",
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];

        let NodeStopOptions {
            no_wait,
            not_gently,
        } = self.wait_opts.unwrap_or_default();
        if no_wait {
            args.push(NodeStopOptions::NO_WAIT);
        }
        if not_gently {
            args.push(NodeStopOptions::NOT_GENTLY);
        }

        self.ccm
            .cmd
            .run_command("ccm", args, RunOptions::new())
            .await
    }
}

///
/// ccm <node> updateconf
///
pub(crate) struct NodeUpdateconf<'ccm> {
    ccm: &'ccm mut NodeCcm,
    args: Vec<String>,
}

impl NodeCcm {
    pub(crate) fn node_updateconf(&mut self) -> NodeUpdateconf<'_> {
        let cfg_dir = self.config_dir.as_str().to_string();
        let name = self.node_name.clone();
        NodeUpdateconf {
            ccm: self,
            args: vec![
                name,
                "updateconf".to_string(),
                "--config-dir".to_string(),
                cfg_dir,
            ],
        }
    }
}

impl NodeUpdateconf<'_> {
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
/// ccm <node> remove
///
pub(crate) struct NodeRemove<'ccm> {
    ccm: &'ccm mut NodeCcm,
}

impl NodeCcm {
    pub(crate) fn node_remove(&mut self) -> NodeRemove<'_> {
        NodeRemove { ccm: self }
    }
}

impl NodeRemove<'_> {
    pub(crate) async fn run(self) -> Result<ExitStatus, anyhow::Error> {
        let args = &[
            self.ccm.node_name.as_str(),
            "remove",
            "--config-dir",
            self.ccm.config_dir.as_str(),
        ];

        self.ccm
            .cmd
            .run_command("ccm", args, RunOptions::new())
            .await
    }
}
