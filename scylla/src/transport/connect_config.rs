use crate::transport::Compression;
use std::net::SocketAddr;

pub struct ConnectConfig {
    pub known_nodes: Vec<KnownNode>,
    pub compression: Option<Compression>,
    /*
    These configuration options will be added in the future:

    Option<String> is a just a placeholder value
    pub auth_username: Option<String>,
    pub auth_password: Option<String>,

    pub use_tls: bool,
    pub tls_certificate_path: Option<String>,

    pub tcp_nodelay: bool,
    pub tcp_keepalive: bool,

    pub load_balancing: Option<String>,
    pub retry_policy: Option<String>,

    pub default_consistency: Option<String>,

    pub keyspace: Option<String>,
    */
}

pub enum KnownNode {
    Hostname(String),
    Address(SocketAddr),
}

impl ConnectConfig {
    pub fn new() -> Self {
        ConnectConfig {
            known_nodes: Vec::new(),
            compression: None,
        }
    }

    pub fn add_known_node(&mut self, hostname: impl AsRef<str>) {
        self.known_nodes
            .push(KnownNode::Hostname(hostname.as_ref().to_string()));
    }

    pub fn add_known_node_addr(&mut self, node_addr: SocketAddr) {
        self.known_nodes.push(KnownNode::Address(node_addr));
    }

    pub fn add_known_nodes(&mut self, hostnames: &[impl AsRef<str>]) {
        for hostname in hostnames {
            self.add_known_node(hostname);
        }
    }

    pub fn add_known_nodes_addr(&mut self, node_addrs: &[SocketAddr]) {
        for address in node_addrs {
            self.add_known_node_addr(*address);
        }
    }
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self::new()
    }
}
