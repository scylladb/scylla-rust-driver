//! This module holds entities that represent the whole configurable
//! driver's client of the cluster.
//! The following abstractions are involved:
//! - [Session](session::Session) - the main entity of the driver. It:
//!     - contains and manages all driver configuration,
//      - launches and communicates with [ClusterWorker] (see [cluster](crate::cluster) module for more info),
//!     - enables executing CQL requests, taking all configuration into consideration.
//! - [SessionBuilder](session_builder::SessionBuilder) - just a convenient builder for a `Session`.
//! - [CachingSession](caching_session::CachingSession) - a wrapper over a [Session](session::Session)
//!   that keeps and manages a cache of prepared statements, so that a user can be free of such considerations.
//! - [SelfIdentity] - configuresd driver and application self-identifying information,
//!   to be sent in STARTUP message.

pub mod caching_session;

mod self_identity;
pub use self_identity::SelfIdentity;

pub mod session;

pub mod session_builder;

#[cfg(test)]
mod session_test;

pub use scylla_cql::frame::Compression;

pub use crate::transport::connection_pool::PoolSize;
