//! This module holds entities that control, customize, trace, and measure statement execution.
//! This includes:
//! - execution profiles,
//! - policies:
//!     - load balancing,
//!     - retries,
//!     - speculative execution,
//! - TODO.

pub mod downgrading_consistency_retry_policy;
pub mod execution_profile;
pub mod load_balancing;
pub mod retry_policy;
pub mod speculative_execution;

pub use execution_profile::ExecutionProfile;
