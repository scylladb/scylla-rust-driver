use super::connect_config::ConnectConfig;
use super::errors::NewSessionError;
use super::session::Session;
use super::Compression;
use std::net::SocketAddr;

pub struct SessionBuilder {
    pub config: ConnectConfig,
}

impl SessionBuilder {
    pub fn new() -> Self {
        SessionBuilder {
            config: ConnectConfig::new(),
        }
    }

    pub fn known_node(mut self, hostname: impl AsRef<str>) -> Self {
        self.config.add_known_node(hostname);
        self
    }

    pub fn known_node_addr(mut self, node_addr: SocketAddr) -> Self {
        self.config.add_known_node_addr(node_addr);
        self
    }

    pub fn known_nodes(mut self, hostnames: &[impl AsRef<str>]) -> Self {
        self.config.add_known_nodes(hostnames);
        self
    }

    pub fn known_nodes_addr(mut self, node_addrs: &[SocketAddr]) -> Self {
        self.config.add_known_nodes_addr(node_addrs);
        self
    }

    pub fn compression(mut self, compression: Option<Compression>) -> Self {
        self.config.compression = compression;
        self
    }

    pub async fn build(self) -> Result<Session, NewSessionError> {
        Session::connect(self.config).await
    }
}

impl Default for SessionBuilder {
    fn default() -> Self {
        SessionBuilder::new()
    }
}
