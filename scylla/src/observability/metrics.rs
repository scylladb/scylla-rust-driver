use histogram::{AtomicHistogram, Histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

const ORDER_TYPE: Ordering = Ordering::Relaxed;

/// Error that occured upon a metrics operation.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MetricsError {
    #[error("Histogram error: {0}")]
    HistogramError(#[from] Arc<dyn std::error::Error + Send + Sync>),
    #[error("Histogram is empty")]
    Empty,
}

/// Snapshot is a structure that contains histogram statistics such as
/// min, max, mean, standard deviation, median, and most common percentiles
/// collected in a certain moment.
#[non_exhaustive]
#[derive(Debug)]
pub struct Snapshot {
    pub min: u64,
    pub max: u64,
    pub mean: u64,
    pub stddev: u64,
    pub median: u64,
    pub percentile_75: u64,
    pub percentile_95: u64,
    pub percentile_98: u64,
    pub percentile_99: u64,
    pub percentile_99_9: u64,
}

pub struct Metrics {
    errors_num: AtomicU64,
    queries_num: AtomicU64,
    errors_iter_num: AtomicU64,
    queries_iter_num: AtomicU64,
    retries_num: AtomicU64,
    histogram: Arc<AtomicHistogram>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics::default()
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
        if let Err(err) = self.histogram.increment(latency) {
            Err(MetricsError::HistogramError(Arc::new(err)))
        } else {
            Ok(())
        }
    }

    /// Returns average latency in milliseconds
    pub fn get_latency_avg_ms(&self) -> Result<u64, MetricsError> {
        Self::mean(&self.histogram.load())
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0 - 100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        let res = self.histogram.load().percentile(percentile);

        match res {
            Err(err) => Err(MetricsError::HistogramError(Arc::new(err))),

            Ok(None) => Err(MetricsError::Empty),

            Ok(Some(p)) => Ok(p.count()),
        }
    }

    /// Returns snapshot of histogram metrics taken at the moment of calling this function. \
    /// Available metrics: min, max, mean, std_dev, median,
    ///                    percentile_75, percentile_95, percentile_98,
    ///                    percentile_99, and percentile_99_9.
    pub fn get_snapshot(&self) -> Result<Snapshot, MetricsError> {
        let h = self.histogram.load();

        let (min, max) = Self::minmax(&h)?;

        let percentile_args = [50.0, 75.0, 95.0, 98.0, 99.0, 99.9];
        let percentiles = Self::percentiles(&h, &percentile_args)?;

        Ok(Snapshot {
            min,
            max,
            mean: Self::mean(&h)?,
            stddev: Self::stddev(&h)?,
            median: percentiles[0],
            percentile_75: percentiles[1],
            percentile_95: percentiles[2],
            percentile_98: percentiles[3],
            percentile_99: percentiles[4],
            percentile_99_9: percentiles[5],
        })
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

    // Metric implementations

    fn mean(h: &Histogram) -> Result<u64, MetricsError> {
        // Compute the mean (count each bucket as its interval's center).
        let mut weighted_sum = 0_u128;
        let mut count = 0_u128;

        for bucket in h {
            let mid = ((bucket.start() + bucket.end()) / 2) as u128;
            weighted_sum += mid * bucket.count() as u128;
            count += bucket.count() as u128;
        }

        if count != 0 {
            Ok((weighted_sum / count) as u64)
        } else {
            Err(MetricsError::Empty)
        }
    }

    fn percentiles(h: &Histogram, percentiles: &[f64]) -> Result<Vec<u64>, MetricsError> {
        let res = h.percentiles(percentiles);

        match res {
            Err(err) => Err(MetricsError::HistogramError(Arc::new(err))),

            Ok(None) => Err(MetricsError::Empty),

            Ok(Some(ps)) => Ok(ps.into_iter().map(|(_, bucket)| bucket.count()).collect()),
        }
    }

    fn stddev(h: &Histogram) -> Result<u64, MetricsError> {
        let total_count = h
            .into_iter()
            .map(|bucket| bucket.count() as u128)
            .sum::<u128>();

        let mean = Self::mean(h)? as u128;
        let mut variance_sum = 0;
        for bucket in h {
            let count = bucket.count() as u128;
            let mid = ((bucket.start() + bucket.end()) / 2) as u128;

            variance_sum += count * (mid as i128 - mean as i128).pow(2) as u128;
        }
        let variance = variance_sum / total_count;

        Ok((variance as f64).sqrt() as u64)
    }

    fn minmax(h: &Histogram) -> Result<(u64, u64), MetricsError> {
        let mut min = u64::MAX;
        let mut max = 0;
        for bucket in h {
            if bucket.count() == 0 {
                continue;
            }
            let lower_bound = bucket.start();
            let upper_bound = bucket.end();

            min = u64::min(min, lower_bound);
            max = u64::max(max, upper_bound);
        }

        if min > max {
            Err(MetricsError::Empty)
        } else {
            Ok((min, max))
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        // Configuration:
        //  - exponent of max value: n = 16
        //  - inverse exponent of relative error: p = 12,
        //  - max value: N = 65535,
        //  - relative error: e = 0.000244,
        //  - total number of buckets: (n - p + 1) * 2^p = 20480,
        //  - histogram size: 1.7 MiB.
        // Reference for calculating these values:
        //  - https://observablehq.com/@iopsystems/h2histogram
        let max_value_power = 16;
        let grouping_power = 12;

        Self {
            errors_num: AtomicU64::new(0),
            queries_num: AtomicU64::new(0),
            errors_iter_num: AtomicU64::new(0),
            queries_iter_num: AtomicU64::new(0),
            retries_num: AtomicU64::new(0),
            histogram: Arc::new(AtomicHistogram::new(grouping_power, max_value_power).unwrap()),
        }
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h = self.histogram.load();
        f.debug_struct("Metrics")
            .field("errors_num", &self.errors_num)
            .field("queries_num", &self.queries_num)
            .field("errors_iter_num", &self.errors_iter_num)
            .field("queries_iter_num", &self.queries_iter_num)
            .field("retries_num", &self.retries_num)
            .field("histogram", &h)
            .finish()
    }
}
