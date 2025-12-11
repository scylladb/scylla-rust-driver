#![cfg_attr(
    not(all(scylla_unstable, feature = "unstable-host-listener")),
    expect(
        unreachable_pub,
        dead_code // TODO: remove this when host listener is actually used in the driver
    )
)]

//! Host listeners can subscribe to events regarding hosts.
//!
//! This includes events when a host is added or removed from the cluster,
//! as well as when a host is marked as up or down.

use std::net::SocketAddr;

use uuid::Uuid;

/// Context provided to [HostListener] callbacks.
#[non_exhaustive]
#[derive(Debug)]
pub struct HostEventContext {
    pub(crate) host_id: Uuid,
    pub(crate) addr: SocketAddr,
}

impl HostEventContext {
    /// ID of the host related to the event.
    pub fn host_id(&self) -> Uuid {
        self.host_id
    }

    /// Address of the host related to the event.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Kind of host event being signalled by [HostListener].
#[non_exhaustive]
#[derive(Debug)]
pub enum HostEvent {
    /// Signalled when the driver learns that a new host is added to the cluster.
    ///
    /// This happens when the control connection discovers a new peer.
    Added,

    /// Signalled when the driver learns that a known host is removed from the cluster.
    ///
    /// This happens when the control connection discovers that a known peer is no longer
    /// in the cluster.
    Removed,

    /// Signalled when the driver learns that a known host has changed its address.
    AddressChanged {
        /// The old address of the host.
        old_address: SocketAddr,
        /// The new address of the host.
        new_address: SocketAddr,
    },

    /// Signalled when the driver learns that there exists connectivity to a known host.
    ///
    /// This happens in the following cases:
    /// 1. When the control connection discovers a new peer (optimistically assuming it's up).
    /// 2. When the connection pool associated with the host reports it as up (meaning that
    ///    it successfully opened at least one connection).
    Up,

    /// Signalled when the driver learns that there is NO connectivity to a known host.
    ///
    /// This happens in the following cases:
    /// 1. When the control connection discovers that a known peer is no longer in the cluster.
    ///    Then `host_down` event precedes the `host_removed` event.
    /// 2. When the connection pool associated with the host reports it as down (meaning that
    ///    it failed to open any connections).
    Down,
}

/// Allows listening to host events: ADD, REMOVE, UP, DOWN.
///
/// ADD and REMOVE are triggered when the driver discovers or removes a host from
/// its internal metadata (e.g., through control connection peer discovery or CQL events).
///
/// UP and DOWN are triggered when the connection pool associated with the host
/// reports it as up or down.
///
/// Note that UP/DOWN events are deduplicated, so if a host is already marked as UP,
/// there won't be another UP event until a DOWN event occurs, and vice versa.
/// Also, ADD/REMOVE events should not be duplicated, too.
pub trait HostListener: Send + Sync {
    /// Called when a host event occurs.
    fn on_event(&self, ctx: &HostEventContext, event: &HostEvent);
}
