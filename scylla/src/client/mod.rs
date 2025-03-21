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
//! - [ExecutionProfile](execution_profile::ExecutionProfile) - a profile that groups various configuration
//!   options relevant when executing a request against the DB.
//! - [QueryPager](pager::QueryPager) and [TypedRowStream](pager::TypedRowStream) - entities that provide
//!   automated transparent paging of a query.

pub mod execution_profile;

pub mod pager;

pub mod caching_session;

mod self_identity;
pub use self_identity::SelfIdentity;

pub mod session;

pub mod session_builder;

pub use scylla_cql::frame::Compression;

pub use crate::network::PoolSize;
