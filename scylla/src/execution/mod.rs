//! This module holds entities that control, customize, trace, and measure statement execution.
//! This includes:
//! - execution profiles,
//! - policies:
//!     - load balancing,
//!     - speculative execution,
//! - TODO.

pub mod execution_profile;
pub mod load_balancing;
pub mod speculative_execution;

pub use execution_profile::ExecutionProfile;
