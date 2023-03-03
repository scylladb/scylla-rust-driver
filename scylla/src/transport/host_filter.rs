//! Host filters.
//!
//! Host filters are essentially just a predicate over
//! [`Peer`](crate::transport::topology::Peer)s. Currently, they are used
//! by the [`Session`](crate::transport::session::Session) to determine whether
//! connections should be opened to a given node or not.

use std::collections::HashSet;
use std::io::Error;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::transport::topology::Peer;

/// The `HostFilter` trait.
pub trait HostFilter: Send + Sync {
    /// Returns whether a peer should be accepted or not.
    fn accept(&self, peer: &Peer) -> bool;
}

/// Unconditionally accepts all nodes.
pub struct AcceptAllHostFilter;

impl HostFilter for AcceptAllHostFilter {
    fn accept(&self, _peer: &Peer) -> bool {
        true
    }
}

/// Accepts nodes whose addresses are present in the allow list provided
/// during filter's construction.
pub struct AllowListHostFilter {
    allowed: HashSet<SocketAddr>,
}

impl AllowListHostFilter {
    /// Creates a new `AllowListHostFilter` which only accepts nodes from the
    /// list.
    pub fn new<I, A>(allowed_iter: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = A>,
        A: ToSocketAddrs,
    {
        // I couldn't get the iterator combinators to work
        let mut allowed = HashSet::new();
        for item in allowed_iter {
            for addr in item.to_socket_addrs()? {
                allowed.insert(addr);
            }
        }

        Ok(Self { allowed })
    }
}

impl HostFilter for AllowListHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        match peer.address {
            super::NodeAddr::Translatable(addr) => self.allowed.contains(&addr),
            // If the address is Untranslatable, then the node either was originally
            // an ContactPoint, or a Translatable node for which the host filter
            // returned true.
            super::NodeAddr::Untranslatable(_) => true,
        }
    }
}

/// Accepts nodes from given DC.
pub struct DcHostFilter {
    local_dc: String,
}

impl DcHostFilter {
    /// Creates a new `DcHostFilter` that accepts nodes only from the
    /// `local_dc`.
    pub fn new(local_dc: String) -> Self {
        Self { local_dc }
    }
}

impl HostFilter for DcHostFilter {
    fn accept(&self, peer: &Peer) -> bool {
        peer.datacenter.as_ref() == Some(&self.local_dc)
    }
}
