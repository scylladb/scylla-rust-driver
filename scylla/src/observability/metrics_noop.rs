//! No-op metrics module used when the `metrics` feature is disabled.
//!
//! All recording methods are zero-cost no-ops;
//! Nothing is public, to make sure we don't expose those no-op structs to the user.
//! Query methods are removed to make sure we don't use non-functioning methods by accident.

/// Error that occurred upon a metrics operation.
pub(crate) enum MetricsError {}

/// No-op metrics object used when the `metrics` feature is disabled.
#[derive(Debug)]
pub(crate) struct Metrics;

impl Metrics {
    #[inline(always)]
    pub(crate) fn new() -> Self {
        Self
    }

    #[inline(always)]
    pub(crate) fn inc_failed_nonpaged_queries(&self) {}

    #[inline(always)]
    pub(crate) fn inc_total_nonpaged_queries(&self) {}

    #[inline(always)]
    pub(crate) fn inc_failed_paged_queries(&self) {}

    #[inline(always)]
    pub(crate) fn inc_total_paged_queries(&self) {}

    #[inline(always)]
    pub(crate) fn inc_retries_num(&self) {}

    #[inline(always)]
    pub(crate) fn inc_total_connections(&self) {}

    #[inline(always)]
    pub(crate) fn dec_total_connections(&self) {}

    #[inline(always)]
    pub(crate) fn inc_connection_timeouts(&self) {}

    #[inline(always)]
    pub(crate) fn inc_request_timeouts(&self) {}

    #[inline(always)]
    pub(crate) fn log_query_latency(&self, _latency: u64) -> Result<(), MetricsError> {
        Ok(())
    }
}

#[cfg(test)]
impl Default for Metrics {
    fn default() -> Self {
        Self
    }
}
