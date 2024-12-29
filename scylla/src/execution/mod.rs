//! This module holds entities that control, customize, trace, and measure statement execution.
//! This includes:
//! - automated query pager,
//! - execution profiles,
//! - policies:
//!     - load balancing,
//!     - retries,
//!     - speculative execution,
//! - cluster-side request tracing,
//! - driver metrics,
//! - request execution history,
//! - error types.

pub mod errors;
pub mod execution_profile;
pub mod history;
pub mod load_balancing;
pub(crate) mod metrics;
pub mod pager;
pub mod retries;
pub mod speculative_execution;
pub mod tracing;

pub use execution_profile::ExecutionProfile;
