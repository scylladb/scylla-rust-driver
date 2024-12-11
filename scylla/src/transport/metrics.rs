use crate::transport::histogram::{Histogram, Snapshot};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const ORDER_TYPE: Ordering = Ordering::Relaxed;

#[derive(Debug)]
pub struct MetricsError {
    cause: &'static str,
}

impl From<&'static str> for MetricsError {
    fn from(err: &'static str) -> MetricsError {
        MetricsError { cause: err }
    }
}

impl std::fmt::Display for MetricsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "metrics error: {}", self.cause)
    }
}

#[derive(Default, Debug)]
pub struct Metrics {
    errors_num: AtomicU64,
    queries_num: AtomicU64,
    errors_iter_num: AtomicU64,
    queries_iter_num: AtomicU64,
    retries_num: AtomicU64,
    histogram: Arc<Histogram>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            errors_num: AtomicU64::new(0),
            queries_num: AtomicU64::new(0),
            errors_iter_num: AtomicU64::new(0),
            queries_iter_num: AtomicU64::new(0),
            retries_num: AtomicU64::new(0),
            histogram: Arc::new(Histogram::new()),
        }
    }

    /// Increments counter for errors that occurred in nonpaged queries.
    pub(crate) fn inc_failed_nonpaged_queries(&self) {
        self.errors_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for nonpaged queries.
    pub(crate) fn inc_total_nonpaged_queries(&self) {
        self.queries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for errors that occurred in paged queries.
    pub(crate) fn inc_failed_paged_queries(&self) {
        self.errors_iter_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for page queries in paged queries.
    /// If query_iter would return 4 pages then this counter should be incremented 4 times.
    pub(crate) fn inc_total_paged_queries(&self) {
        self.queries_iter_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter measuring how many times a retry policy has decided to retry a query
    pub(crate) fn inc_retries_num(&self) {
        self.retries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Saves to histogram latency of completing single query.
    /// For paged queries it should log latency for every page.
    ///
    /// # Arguments
    ///
    /// * `latency` - time in milliseconds that should be logged
    pub(crate) fn log_query_latency(&self, latency: u64) -> Result<(), MetricsError> {
        self.histogram.increment(latency)?;
        Ok(())
    }

    /// Returns average latency in milliseconds
    pub fn get_latency_avg_ms(&self) -> Result<u64, MetricsError> {
        let mean = self.histogram.log_operation(Histogram::mean())?;
        Ok(mean)
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0 - 100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        let result = self
            .histogram
            .log_operation(Histogram::percentile(percentile))?;
        Ok(result)
    }

    /// Returns snapshot of histogram metrics taken at the moment of calling this function. \
    /// Available metrics: min, max, mean, std_dev, median,
    ///                    percentile_90, percentile_95, percentile_99, percentile_99_9.
    pub fn get_snapshot(&self) -> Result<Snapshot, MetricsError> {
        let snapshot = self.histogram.log_operation(Histogram::snapshot())?;
        Ok(snapshot)
    }

    /// Returns counter for errors occurred in nonpaged queries
    pub fn get_errors_num(&self) -> u64 {
        self.errors_num.load(ORDER_TYPE)
    }

    /// Returns counter for nonpaged queries
    pub fn get_queries_num(&self) -> u64 {
        self.queries_num.load(ORDER_TYPE)
    }

    /// Returns counter for errors occurred in paged queries
    pub fn get_errors_iter_num(&self) -> u64 {
        self.errors_iter_num.load(ORDER_TYPE)
    }

    /// Returns counter for pages requested in paged queries
    pub fn get_queries_iter_num(&self) -> u64 {
        self.queries_iter_num.load(ORDER_TYPE)
    }

    /// Returns counter measuring how many times a retry policy has decided to retry a query
    pub fn get_retries_num(&self) -> u64 {
        self.retries_num.load(ORDER_TYPE)
    }
}
