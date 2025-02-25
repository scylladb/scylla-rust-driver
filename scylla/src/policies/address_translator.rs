use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr as _;

use async_trait::async_trait;
use uuid::Uuid;

use crate::errors::TranslationError;

/// Data used to issue connections to a node that is possibly subject to address translation.
///
/// Built from `PeerEndpoint` if its `NodeAddr` variant implies address translation possibility.
#[derive(Debug)]
pub struct UntranslatedPeer<'a> {
    pub(crate) host_id: Uuid,
    pub(crate) untranslated_address: SocketAddr,
    pub(crate) datacenter: Option<&'a str>,
    pub(crate) rack: Option<&'a str>,
}

impl UntranslatedPeer<'_> {
    /// The unique identifier of the node in the cluster.
    #[inline]
    pub fn host_id(&self) -> Uuid {
        self.host_id
    }

    /// The address of the node in the cluster as broadcast by the node itself.
    /// It may be subject to address translation.
    #[inline]
    pub fn untranslated_address(&self) -> SocketAddr {
        self.untranslated_address
    }

    /// The datacenter the node resides in.
    #[inline]
    pub fn datacenter(&self) -> Option<&str> {
        self.datacenter
    }

    /// The rack the node resides in.
    #[inline]
    pub fn rack(&self) -> Option<&str> {
        self.rack
    }
}

/// Translates IP addresses received from ScyllaDB nodes into locally reachable addresses.
///
/// The driver auto-detects new ScyllaDB nodes added to the cluster through server side pushed
/// notifications and through checking the system tables. For each node, the address the driver
/// receives corresponds to the address set as `rpc_address` in the node yaml file. In most
/// cases, this is the correct address to use by the driver and that is what is used by default.
/// However, sometimes the addresses received through this mechanism will either not be reachable
/// directly by the driver or should not be the preferred address to use to reach the node (for
/// instance, the `rpc_address` set on ScyllaDB nodes might be a private IP, but some clients
/// may have to use a public IP, or pass by a router, e.g. through NAT, to reach that node).
/// This interface allows to deal with such cases, by allowing to translate an address as sent
/// by a ScyllaDB node to another address to be used by the driver for connection.
///
/// Please note that the "known nodes" addresses provided while creating
/// the [`Session`](crate::client::session::Session) instance are not translated,
/// only IP addresses retrieved from or sent by Cassandra nodes to the driver are.
#[async_trait]
pub trait AddressTranslator: Send + Sync {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError>;
}

#[async_trait]
impl AddressTranslator for HashMap<SocketAddr, SocketAddr> {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        match self.get(&untranslated_peer.untranslated_address()) {
            Some(&translated_addr) => Ok(translated_addr),
            None => Err(TranslationError::NoRuleForAddress(
                untranslated_peer.untranslated_address(),
            )),
        }
    }
}

#[async_trait]
// Notice: this is inefficient, but what else can we do with such poor representation as str?
// After all, the cluster size is small enough to make this irrelevant.
impl AddressTranslator for HashMap<&'static str, &'static str> {
    async fn translate_address(
        &self,
        untranslated_peer: &UntranslatedPeer,
    ) -> Result<SocketAddr, TranslationError> {
        for (&rule_addr_str, &translated_addr_str) in self.iter() {
            if let Ok(rule_addr) = SocketAddr::from_str(rule_addr_str) {
                if rule_addr == untranslated_peer.untranslated_address() {
                    return SocketAddr::from_str(translated_addr_str).map_err(|reason| {
                        TranslationError::InvalidAddressInRule {
                            translated_addr_str,
                            reason,
                        }
                    });
                }
            }
        }
        Err(TranslationError::NoRuleForAddress(
            untranslated_peer.untranslated_address(),
        ))
    }
}
