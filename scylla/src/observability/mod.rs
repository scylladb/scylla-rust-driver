//! This module holds entities that allow observing and measuring driver's and cluster's behaviour.
//! This includes:
//! - driver-side tracing,
//! - cluster-side tracing,
//! - request execution history,
//! - driver metrics.

pub(crate) mod driver_tracing;
pub mod history;
#[cfg(feature = "metrics")]
pub mod metrics;
mod rate_limiting;
pub mod tracing;

pub(crate) use rate_limiting::{rate_limited, warn_rate_limited, RateLimiter};
