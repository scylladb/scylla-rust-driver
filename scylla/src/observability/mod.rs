//! This module holds entities that allow observing and measuring driver's and cluster's behaviour.
//! This includes:
//! - driver-side tracing,
//! - cluster-side tracing,
//! - request execution history,
//! - driver metrics.

pub(crate) mod driver_tracing;
pub mod history;
// When the `metrics` feature is enabled, compile and expose the real metrics
// implementation as a public module.  When disabled, compile the zero-cost
// no-op implementation as a crate-internal module so that internal code can
// use it without any `#[cfg]` guards, while keeping the no-op hidden from
// library users.
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(not(feature = "metrics"))]
#[path = "metrics_noop.rs"]
pub(crate) mod metrics;
pub mod tracing;
