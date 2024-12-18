//! This module holds entities that control, customize, trace, and measure statement execution.
//! This includes:
//! - execution profiles,
//! - policies:
//!     - load balancing,
//!     - retries,
//!     - speculative execution,
//! - cluster-side request tracing,
//! - request execution history,
//! - TODO.

pub mod execution_profile;
pub mod history;
pub mod load_balancing;
pub mod retries;
pub mod speculative_execution;
pub mod tracing;

pub use execution_profile::ExecutionProfile;
