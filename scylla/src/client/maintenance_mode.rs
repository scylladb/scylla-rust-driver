//! Maintenance mode: a session pinned to exactly one node.
//!
//! A maintenance-mode session is meant for operating on a single node directly -
//! typically an unhealthy one, or one reachable only over a local Unix domain
//! socket. It differs from a regular session in that it:
//!
//! - connects to exactly one endpoint, given up front, and never to any other -
//!   the peer list is never read, so no other node is ever discovered;

use std::net::{Ipv4Addr, SocketAddr};
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::sync::Arc;
use std::time::Duration;

use crate::cluster::metadata::MaintenanceEndpoint;
use crate::cluster::node::{Transport, resolve_hostname};
use crate::errors::NewSessionError;

/// The single endpoint a maintenance-mode session connects to.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MaintenanceModeEndpoint {
    /// The node is identified by its hostname, which the driver resolves.
    ///
    /// If the hostname resolves to several addresses, the driver connects to one
    /// of them and treats it as the one node of the session.
    Hostname(String),

    /// The node is identified by its IP address and port.
    Address(SocketAddr),

    /// The node is reachable over a Unix domain socket at the given path.
    #[cfg(unix)]
    UnixSocket(PathBuf),
}

impl MaintenanceModeEndpoint {
    /// Targets the node with the given hostname.
    pub fn hostname(hostname: impl Into<String>) -> Self {
        Self::Hostname(hostname.into())
    }

    /// Targets the node at the given address.
    pub fn address(address: SocketAddr) -> Self {
        Self::Address(address)
    }

    /// Targets the node listening on the Unix domain socket at the given path.
    ///
    /// # Caveats
    ///
    /// A CQL node is identified by a `SocketAddr` throughout the driver's API
    /// and a Unix socket has no such address. A placeholder of `0.0.0.0:0` therefore
    /// stands in for it.
    ///
    /// TLS is **silently ignored** for a Unix socket connection: such a socket
    /// is local by construction, so a TLS context configured on the builder is
    /// skipped.
    #[cfg(unix)]
    pub fn unix_socket(path: impl Into<PathBuf>) -> Self {
        Self::UnixSocket(path.into())
    }

    /// Turns the endpoint the user configured into the one the driver connects
    /// to, resolving a hostname if that is what was given.
    pub(crate) async fn resolve(
        self,
        hostname_resolution_timeout: Option<Duration>,
    ) -> Result<MaintenanceEndpoint, NewSessionError> {
        let (address, transport) = match self {
            Self::Address(address) => (address, Transport::Tcp),
            Self::Hostname(hostname) => {
                let address = resolve_hostname(&hostname, hostname_resolution_timeout)
                    .await
                    .ok()
                    .and_then(|addresses| addresses.into_iter().next())
                    // A hostname that resolves to nothing leaves the session with
                    // no node at all, which is fatal - there is no second contact
                    // point to fall back on.
                    .ok_or(NewSessionError::FailedToResolveAnyHostname(vec![hostname]))?;
                // A maintenance mode session talks to one node, so if the hostname
                // resolves to several addresses the first is taken and the session
                // is pinned to it, rather than treating them as a set to choose
                // among.
                (address, Transport::Tcp)
            }
            #[cfg(unix)]
            Self::UnixSocket(path) => (
                UNIX_SOCKET_PLACEHOLDER_ADDR,
                Transport::UnixSocket(Arc::from(path.as_path())),
            ),
        };

        Ok(MaintenanceEndpoint { address, transport })
    }
}

/// Stands in for the address of a node reached over a Unix domain socket, which
/// has no address of its own.
pub(crate) const UNIX_SOCKET_PLACEHOLDER_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
