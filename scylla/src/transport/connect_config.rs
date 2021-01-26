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
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self::new()
    }
}
