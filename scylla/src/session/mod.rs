//! This module holds entities that represent the whole configurable
//! driver session with the cluster.
//! The following abstractions are involved:
//! - Session - the main entity of the driver. It:
//!     - contains and manages all driver configuration,
//!     - launches and communicates with ClusterWorker (see cluster module for more info),
//!     - enables executing CQL requests, taking all configuration into consideration.
//! - SessionBuilder - just a convenient builder for a Session.
//! - CachingSession - a wrapper over a Session that keeps and manages a cache
//!   of prepared statements, so that a user can be free of such considerations.

mod caching_session;
#[allow(clippy::module_inception)]
mod session;
pub mod session_builder;
#[cfg(test)]
mod session_test;

#[allow(deprecated)]
pub use caching_session::{CachingSession, GenericCachingSession, LegacyCachingSession};
pub use session::*;
#[cfg(feature = "cloud")]
pub use session_builder::CloudSessionBuilder;
pub use session_builder::SessionBuilder;
