//! Collecting metrics of driver operations.

use histogram::{AtomicHistogram, Histogram};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;

const ORDER_TYPE: Ordering = Ordering::Relaxed;

/// Error that occured upon a metrics operation.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MetricsError {
    /// Computing histogram statistics failed.
    #[error("Histogram error: {0}")]
    HistogramError(#[from] Arc<dyn std::error::Error + Send + Sync>),
    /// Histogram is empty, so statistics cannot be computed.
    #[error("Histogram is empty")]
    Empty,
}

/// Snapshot is a structure that contains histogram statistics such as
/// min, max, mean, standard deviation, median, and most common percentiles
/// collected in a certain moment.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Minimum value in the histogram.
    pub min: u64,
    /// Maximum value in the histogram.
    pub max: u64,
    /// Mean value in the histogram.
    pub mean: u64,
    /// Standard deviation of values in the histogram.
    pub stddev: u64,
    /// Median value in the histogram.
    pub median: u64,
    /// 75th percentile value in the histogram.
    pub percentile_75: u64,
    /// 95th percentile value in the histogram.
    pub percentile_95: u64,
    /// 98th percentile value in the histogram.
    pub percentile_98: u64,
    /// 99th percentile value in the histogram.
    pub percentile_99: u64,
    /// 99.9th percentile value in the histogram.
    pub percentile_99_9: u64,
}

/// The interval in seconds for which the rate is calculated.
const INTERVAL: u64 = 5;

/// An implementation of an Exponentially Weighted Moving Average (EWMA).
#[derive(Debug)]
struct ExponentiallyWeightedMovingAverage {
    /// Smoothing factor, a value between 0 and 1.
    alpha: f64,
    /// Atomic counter to keep track of the number of requests. \
    /// Indicates the number of requests that have not been accounted for EWMA yet.
    uncounted: AtomicU64,
    /// To check if the EWMA has been initialized.
    is_initialized: Mutex<bool>,
    ///  Atomic value representing the current rate of requests per second. \
    ///  AtomicU64 is used to store a floating point number as a bit representation.
    rate: AtomicU64,
}

impl ExponentiallyWeightedMovingAverage {
    fn new(alpha: f64) -> Self {
        Self {
            alpha,
            uncounted: AtomicU64::new(0),
            is_initialized: Mutex::new(false),
            rate: AtomicU64::new(0),
        }
    }

    /// Returns the current `rate` as a floating point number.
    fn rate(&self) -> f64 {
        f64::from_bits(self.rate.load(Ordering::Acquire))
    }

    /// Increments the `uncounted` requests counter. \
    /// Should be called every time a new request is made.
    fn update(&self) {
        self.uncounted.fetch_add(1, ORDER_TYPE);
    }

    /// Updates the `rate` based on the current number of requests. \
    /// Should be called every time the interval has passed.
    ///
    /// The rate is updated using the formula: \
    /// `rate = rate + alpha * (instant_rate - rate)` \
    /// where `instant_rate` is the number of requests in the last interval.
    ///
    /// The first time this function is called, the `rate` is set to the `instant_rate`.
    fn tick(&self) {
        let mut is_initialized = self.is_initialized.lock().unwrap();

        let count = self.uncounted.swap(0, ORDER_TYPE);
        let instant_rate = count as f64 / INTERVAL as f64;

        if *is_initialized {
            let rate = f64::from_bits(self.rate.load(Ordering::Acquire));
            self.rate.store(
                f64::to_bits(rate + self.alpha * (instant_rate - rate)),
                Ordering::Release,
            );
        } else {
            self.rate
                .store(f64::to_bits(instant_rate), Ordering::Release);
            *is_initialized = true;
        }
    }
}

#[derive(Debug)]
struct RequestRateMeter {
    one_minute_rate: ExponentiallyWeightedMovingAverage,
    five_minute_rate: ExponentiallyWeightedMovingAverage,
    fifteen_minute_rate: ExponentiallyWeightedMovingAverage,
    count: AtomicU64,
    start_time: std::time::Instant,
    last_tick: AtomicU64,
}

impl RequestRateMeter {
    fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            one_minute_rate: ExponentiallyWeightedMovingAverage::new(
                1.0 - (-(INTERVAL as f64) / 60.0 / 1.0).exp(),
            ),
            five_minute_rate: ExponentiallyWeightedMovingAverage::new(
                1.0 - (-(INTERVAL as f64) / 60.0 / 5.0).exp(),
            ),
            fifteen_minute_rate: ExponentiallyWeightedMovingAverage::new(
                1.0 - (-(INTERVAL as f64) / 60.0 / 15.0).exp(),
            ),
            count: AtomicU64::new(0),
            start_time: now,
            last_tick: AtomicU64::new(now.elapsed().as_nanos() as u64),
        }
    }

    fn mark(&self) {
        self.tick_if_necessary();
        self.count.fetch_add(1, ORDER_TYPE);
        self.one_minute_rate.update();
        self.five_minute_rate.update();
        self.fifteen_minute_rate.update();
    }

    fn one_minute_rate(&self) -> f64 {
        self.one_minute_rate.rate()
    }

    fn five_minute_rate(&self) -> f64 {
        self.five_minute_rate.rate()
    }

    fn fifteen_minute_rate(&self) -> f64 {
        self.fifteen_minute_rate.rate()
    }

    fn mean_rate(&self) -> f64 {
        let count = self.count();
        if count == 0 {
            0.0
        } else {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            count as f64 / elapsed
        }
    }

    fn count(&self) -> u64 {
        self.count.load(ORDER_TYPE)
    }

    fn tick_if_necessary(&self) {
        // Multiple threads could read the same `old_tick`...
        let old_tick = self.last_tick.load(ORDER_TYPE);
        let new_tick = self.start_time.elapsed().as_nanos() as u64;
        let elapsed = new_tick - old_tick;

        // _"Problematic"_ `if` - see a comment below.
        if elapsed > INTERVAL * 1_000_000_000 {
            let new_interval_start_tick = new_tick - elapsed % (INTERVAL * 1_000_000_000);
            // But then only one will succeed in the following COMPARE EXCHANGE operation.
            if self
                .last_tick
                .compare_exchange(old_tick, new_interval_start_tick, ORDER_TYPE, ORDER_TYPE)
                .is_ok()
            {
                let required_ticks = elapsed / (INTERVAL * 1_000_000_000);
                // So only one thread will do the following ticks.
                // The only concern is that this loop might take so long that another thread
                // enters the _"problematic"_ `if` and then we have two threads in `tick()`.
                // This is extremely unlikely, because then the loop would have to take
                // 5 seconds! (INTERVAL * 1e9), BUT even it it happens, we have a mutex
                // in ECMA to guard against logical race.
                for _ in 0..required_ticks {
                    self.one_minute_rate.tick();
                    self.five_minute_rate.tick();
                    self.fifteen_minute_rate.tick();
                }
            }
        }
    }
}

impl Default for RequestRateMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// Various metrics collected by the driver.
///
/// `Metrics` is cheaply cloneable — all clones share the same underlying
/// counters and histogram, so recording on any clone is visible through every
/// other clone.  This is achieved by wrapping the actual data in an `Arc`
/// internally; cloning `Metrics` costs one atomic reference-count increment.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

/// Storage for all metric counters and histograms.
struct MetricsInner {
    /// Number of errors that occurred in unpaged requests.
    errors_unpaged: AtomicU64,
    /// Number of executed unpaged requests.
    requests_unpaged: AtomicU64,
    /// Number of errors that occurred in manually paged requests.
    errors_single_page: AtomicU64,
    /// Number of executed manually paged requests.
    requests_single_page: AtomicU64,
    /// Number of errors that occurred in automatically paged requests.
    errors_iter: AtomicU64,
    /// Number of executed automatically paged requests.
    requests_iter: AtomicU64,
    /// Number of times a retry policy has decided to retry a query.
    retries_num: AtomicU64,
    /// Histogram that collects latencies of queries executed by the driver.
    histogram: Arc<AtomicHistogram>,
    /// Collects rates of queries executed by the driver.
    meter: Arc<RequestRateMeter>,
    /// Total number of active connections to the cluster (excluding control connections).
    total_connections: AtomicU64,
    connection_timeouts: AtomicU64,
    request_timeouts: AtomicU64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
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
            inner: Arc::new(MetricsInner {
                errors_unpaged: AtomicU64::new(0),
                requests_unpaged: AtomicU64::new(0),
                errors_single_page: AtomicU64::new(0),
                requests_single_page: AtomicU64::new(0),
                errors_iter: AtomicU64::new(0),
                requests_iter: AtomicU64::new(0),
                retries_num: AtomicU64::new(0),
                histogram: Arc::new(AtomicHistogram::new(grouping_power, max_value_power).unwrap()),
                meter: Arc::new(RequestRateMeter::new()),
                total_connections: AtomicU64::new(0),
                connection_timeouts: AtomicU64::new(0),
                request_timeouts: AtomicU64::new(0),
            }),
        }
    }

    /// Increments counter for errors that occurred in nonpaged execution
    /// (`Session::{query,execute}_unpaged()` and `Session::batch()`).
    pub(crate) fn inc_failed_nonpaged_queries(&self) {
        self.inner.errors_unpaged.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for nonpaged request execution
    /// (`Session::{query,execute}_unpaged()` and `Session::batch()`).
    pub(crate) fn inc_total_nonpaged_queries(&self) {
        self.inner.requests_unpaged.fetch_add(1, ORDER_TYPE);
        self.inner.meter.mark();
    }

    /// Increments counter for errors that occurred in manually paged execution
    /// (`Session::{query,execute}_single_page()`).
    pub(crate) fn inc_failed_manually_paged_queries(&self) {
        self.inner.errors_single_page.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for page queries in manually paged execution
    /// (`Session::{query,execute}_single_page()`).
    pub(crate) fn inc_total_manually_paged_queries(&self) {
        self.inner.requests_single_page.fetch_add(1, ORDER_TYPE);
        self.inner.meter.mark();
    }

    /// Increments counter for errors that occurred in automatically paged execution
    /// (`Session::{query,execute}_iter()`).
    pub(crate) fn inc_failed_automatically_paged_queries(&self) {
        self.inner.errors_iter.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for page queries in automatically paged execution
    /// (`Session::{query,execute}_iter()`).
    /// If `query_iter` would return 4 pages, then this counter should be incremented 4 times.
    pub(crate) fn inc_total_automatically_paged_queries(&self) {
        self.inner.requests_iter.fetch_add(1, ORDER_TYPE);
        self.inner.meter.mark();
    }

    /// Increments counter measuring how many times a retry policy has decided
    /// to retry a request.
    pub(crate) fn inc_retries_num(&self) {
        self.inner.retries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for active number of connections to the cluster.
    /// Should be called when opening new connections, once per connection.
    pub(crate) fn inc_total_connections(&self) {
        self.inner.total_connections.fetch_add(1, ORDER_TYPE);
    }

    /// Decrements counter for number of active connections to the cluster.
    /// Should be called when closing the connections, once per connection.
    pub(crate) fn dec_total_connections(&self) {
        self.inner.total_connections.fetch_sub(1, ORDER_TYPE);
    }

    /// Increments counter for timeouts for new connections to the cluster.
    pub(crate) fn inc_connection_timeouts(&self) {
        self.inner.connection_timeouts.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for client request timeouts.
    pub(crate) fn inc_request_timeouts(&self) {
        self.inner.request_timeouts.fetch_add(1, ORDER_TYPE);
    }

    /// Saves to histogram latency of completing single query.
    /// For paged queries it should log latency for every page.
    ///
    /// # Arguments
    ///
    /// * `latency` - time in milliseconds that should be logged
    pub(crate) fn log_query_latency(&self, latency: u64) -> Result<(), MetricsError> {
        if let Err(err) = self.inner.histogram.increment(latency) {
            Err(MetricsError::HistogramError(Arc::new(err)))
        } else {
            Ok(())
        }
    }

    /// Returns average latency in milliseconds
    pub fn get_latency_avg_ms(&self) -> Result<u64, MetricsError> {
        Self::mean(&self.inner.histogram.load())
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0..=100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        // `histogram` 1.x uses the 0.0..=1.0 quantile scale, whereas our
        // public API uses the 0.0..=100.0 percentile scale.
        let res = self.inner.histogram.load().quantile(percentile / 100.0);

        match res {
            Err(err) => Err(MetricsError::HistogramError(Arc::new(err))),

            Ok(None) => Err(MetricsError::Empty),

            // A single quantile was requested, so there is exactly one entry.
            Ok(Some(qr)) => match qr.entries().values().next() {
                Some(bucket) => Ok((bucket.start() + bucket.end()) / 2),
                None => Err(MetricsError::Empty),
            },
        }
    }

    /// Returns snapshot of histogram metrics taken at the moment of calling this function. \
    /// Available metrics: min, max, mean, std_dev, median,
    ///                    percentile_75, percentile_95, percentile_98,
    ///                    percentile_99, and percentile_99_9.
    pub fn get_snapshot(&self) -> Result<Snapshot, MetricsError> {
        let h = self.inner.histogram.load();

        let (min, max) = Self::minmax(&h)?;

        // `histogram` uses the 0.0..=1.0 quantile scale.
        let quantile_args = [0.50, 0.75, 0.95, 0.98, 0.99, 0.999];
        let mut percentiles = Self::percentiles(&h, &quantile_args)?;

        // SAFETY: `unwrap()`s are OK here, because `Self::percentiles()` returned iterator's length
        // is equal to number of elements in `quantile_args`.
        let median = percentiles.next().unwrap();
        let percentile_75 = percentiles.next().unwrap();
        let percentile_95 = percentiles.next().unwrap();
        let percentile_98 = percentiles.next().unwrap();
        let percentile_99 = percentiles.next().unwrap();
        let percentile_99_9 = percentiles.next().unwrap();

        Ok(Snapshot {
            min,
            max,
            mean: Self::mean(&h)?,
            stddev: Self::stddev(&h)?,
            median,
            percentile_75,
            percentile_95,
            percentile_98,
            percentile_99,
            percentile_99_9,
        })
    }

    /// Returns count of errors occurred in unpaged execution.
    pub fn get_errors_unpaged_num(&self) -> u64 {
        self.inner.errors_unpaged.load(ORDER_TYPE)
    }

    /// Returns count of errors occurred in manually paged execution.
    pub fn get_errors_manually_paged_num(&self) -> u64 {
        self.inner.errors_single_page.load(ORDER_TYPE)
    }

    #[deprecated(
        since = "1.8.0",
        note = "Method's naming is confusing. Call `get_errors_unpaged_num()`
        and `get_errors_manually_paged_num()` and add their results together instead."
    )]
    /// Returns count of errors occurred in **non** `*_iter()` queries (**non** `QueryPager` APIs).
    pub fn get_errors_num(&self) -> u64 {
        self.get_errors_unpaged_num() + self.get_errors_manually_paged_num()
    }

    /// Returns count of unpaged requests.
    pub fn get_requests_unpaged_num(&self) -> u64 {
        self.inner.requests_unpaged.load(ORDER_TYPE)
    }

    /// Returns count of manually paged requests.
    pub fn get_requests_manually_paged_num(&self) -> u64 {
        self.inner.requests_single_page.load(ORDER_TYPE)
    }

    #[deprecated(
        since = "1.8.0",
        note = "Method's naming is confusing. Call `get_requests_unpaged_num()`
        and `get_requests_manually_paged_num()` and add their results together instead."
    )]
    /// Returns count of **non** `*_iter()` queries (**non** `QueryPager` APIs).
    pub fn get_queries_num(&self) -> u64 {
        self.get_requests_unpaged_num() + self.get_requests_manually_paged_num()
    }

    /// Returns count of errors occurred in automatically paged requests
    /// (`QueryPager` APIs, `*_iter()` execution).
    pub fn get_errors_automatically_paged_num(&self) -> u64 {
        self.inner.errors_iter.load(ORDER_TYPE)
    }

    #[deprecated(
        since = "1.8.0",
        note = "Method's naming is confusing. Use `get_errors_automatically_paged_num()` instead."
    )]
    /// Returns counter for errors occurred in `*_iter()` queries (`QueryPager` APIs).
    pub fn get_errors_iter_num(&self) -> u64 {
        self.inner.errors_iter.load(ORDER_TYPE)
    }

    /// Returns count of automatically paged requests (`QueryPager` APIs, `*_iter()` execution).
    pub fn get_requests_automatically_paged_num(&self) -> u64 {
        self.inner.requests_iter.load(ORDER_TYPE)
    }

    #[deprecated(
        since = "1.8.0",
        note = "Method's naming is confusing. Use `get_requests_automatically_paged_num()` instead."
    )]
    /// Returns count of `*_iter()` queries (`QueryPager` APIs).
    pub fn get_queries_iter_num(&self) -> u64 {
        self.get_requests_automatically_paged_num()
    }

    /// Returns counter measuring how many times a retry policy has decided to retry a query
    pub fn get_retries_num(&self) -> u64 {
        self.inner.retries_num.load(ORDER_TYPE)
    }

    /// Returns mean rate of queries per second
    pub fn get_mean_rate(&self) -> f64 {
        self.inner.meter.mean_rate()
    }

    /// Returns one-minute rate of queries per second
    pub fn get_one_minute_rate(&self) -> f64 {
        self.inner.meter.one_minute_rate()
    }

    /// Returns five-minute rate of queries per second
    pub fn get_five_minute_rate(&self) -> f64 {
        self.inner.meter.five_minute_rate()
    }

    /// Returns fifteen-minute rate of queries per second
    pub fn get_fifteen_minute_rate(&self) -> f64 {
        self.inner.meter.fifteen_minute_rate()
    }

    /// Returns total number of active connections
    pub fn get_total_connections(&self) -> u64 {
        self.inner.total_connections.load(ORDER_TYPE)
    }

    /// Returns counter for connection timeouts
    pub fn get_connection_timeouts(&self) -> u64 {
        self.inner.connection_timeouts.load(ORDER_TYPE)
    }

    /// Returns counter for request timeouts
    pub fn get_request_timeouts(&self) -> u64 {
        self.inner.request_timeouts.load(ORDER_TYPE)
    }

    // Metric implementations

    // histogram crate used to implement Histogram::mean() method. Why did they remove it?
    // Answer of brayniac, the maintainer of histogram crate:
    //
    // > The histogram has no way of providing a true mean. Do we use the lower or upper end
    // > of the bucket range while calculating? Somewhere in the middle? It seems more appropriate
    // > to let the caller decide how they want to deal with this detail. Same when determining
    // > a percentile, the best we can do is return the Bucket where the percentile falls into its range.
    // > It may depend on your use-case on what value to report. Previous assumptions of over-reporting
    // > latencies by using the upper-edge of the bucket might not be appropriate for all use-cases.
    fn mean(h: &Histogram) -> Result<u64, MetricsError> {
        // Compute the mean (count each bucket as its interval's center).
        let mut weighted_sum = 0_u128;
        let mut count = 0_u128;

        for bucket in h {
            let mid = ((bucket.start() + bucket.end()) / 2) as u128;
            weighted_sum += mid * bucket.count() as u128;
            count += bucket.count() as u128;
        }

        weighted_sum
            .checked_div(count)
            .map(|result| result as u64)
            .ok_or(MetricsError::Empty)
    }

    /// Computes the bucket midpoints for the given `quantiles`, which must be
    /// on the 0.0..=1.0 scale used by the `histogram` crate.
    fn percentiles(
        h: &Histogram,
        quantiles: &[f64],
    ) -> Result<impl Iterator<Item = u64> + use<>, MetricsError> {
        let res = h.quantiles(quantiles);

        match res {
            Err(err) => Err(MetricsError::HistogramError(Arc::new(err))),

            Ok(None) => Err(MetricsError::Empty),

            // `entries()` is a `BTreeMap` keyed by quantile, so iterating its
            // values yields buckets in ascending quantile order.
            Ok(Some(qr)) => Ok(qr
                .entries()
                .values()
                // Get the mean value from the bucket.
                .map(|bucket| (bucket.start() + bucket.end()) / 2)
                // https://github.com/iopsystems/histogram/issues/23
                // Until `into_entries` is added, we need this additional Vec.
                // https://github.com/iopsystems/histogram/pull/24
                .collect::<Vec<_>>()
                .into_iter()),
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

#[cfg(test)]
impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h = self.inner.histogram.load();
        f.debug_struct("Metrics")
            .field("errors_unpaged", &self.inner.errors_unpaged)
            .field("requests_unpaged", &self.inner.requests_unpaged)
            .field("errors_single_page", &self.inner.errors_single_page)
            .field("requests_single_page", &self.inner.requests_single_page)
            .field("errors_iter", &self.inner.errors_iter)
            .field("requests_iter", &self.inner.requests_iter)
            .field("retries_num", &self.inner.retries_num)
            .field("histogram", &h)
            .field("meter", &self.inner.meter)
            .field("total_connections", &self.inner.total_connections)
            .field("connection_timeouts", &self.inner.connection_timeouts)
            .field("request_timeouts", &self.inner.request_timeouts)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, SeedableRng};

    use crate::observability::metrics::Snapshot;

    use super::Metrics;

    // Characterization test for a small, hand-verifiable distribution where
    // every bucket has width 1 (values are well below 2^(grouping_power + 1)),
    // so the bucket midpoints equal the inserted values exactly.
    #[test]
    fn characterization_known_distribution() {
        let metrics = Metrics::new();
        for v in 0..=100u64 {
            metrics.log_query_latency(v).unwrap();
        }

        let snapshot = metrics.get_snapshot().unwrap();
        assert_eq!(snapshot.min, 0);
        assert_eq!(snapshot.max, 100);
        assert_eq!(snapshot.mean, 50);
        assert_eq!(snapshot.stddev, 29);
        assert_eq!(snapshot.median, 50);
        assert_eq!(snapshot.percentile_75, 75);
        assert_eq!(snapshot.percentile_95, 95);
        assert_eq!(snapshot.percentile_98, 98);
        assert_eq!(snapshot.percentile_99, 99);
        assert_eq!(snapshot.percentile_99_9, 100);

        assert_eq!(metrics.get_latency_avg_ms().unwrap(), 50);
    }

    #[test]
    fn regression_get_latency_percentile_ms() {
        let metrics = Metrics::new();
        // One bucket (value 5) with 10 observations, another (value 1000) with 1.
        for _ in 0..10 {
            metrics.log_query_latency(5).unwrap();
        }
        metrics.log_query_latency(1000).unwrap();

        // The public API uses the 0.0..=100.0 percentile scale.
        assert_eq!(metrics.get_latency_percentile_ms(0.0).unwrap(), 5);
        assert_eq!(metrics.get_latency_percentile_ms(50.0).unwrap(), 5);
        assert_eq!(metrics.get_latency_percentile_ms(90.0).unwrap(), 5);
        assert_eq!(metrics.get_latency_percentile_ms(100.0).unwrap(), 1000);

        // Empty histogram yields the `Empty` error.
        let empty = Metrics::new();
        assert!(matches!(
            empty.get_latency_percentile_ms(50.0),
            Err(super::MetricsError::Empty)
        ));
    }

    // A regression test for a bug where we would return
    // the number of observations in the bucket for the given percentile.
    #[test]
    fn regression_test_snapshot_one_bucket() {
        let metrics = Metrics::new();

        // Histogram will have one non-empty bucket [0, 0] with 32 observations.
        for _ in 0..32 {
            metrics.log_query_latency(0).unwrap();
        }

        let Snapshot {
            min,
            max,
            mean,
            stddev,
            median,
            percentile_75,
            percentile_95,
            percentile_98,
            percentile_99,
            percentile_99_9,
        } = metrics.get_snapshot().unwrap();

        assert_eq!(min, 0);
        assert_eq!(max, 0);
        assert_eq!(mean, 0);
        assert_eq!(stddev, 0);

        // Before the fix, these would return 32.
        assert_eq!(median, 0);
        assert_eq!(percentile_75, 0);
        assert_eq!(percentile_95, 0);
        assert_eq!(percentile_98, 0);
        assert_eq!(percentile_99, 0);
        assert_eq!(percentile_99_9, 0);
    }

    #[test]
    fn test_snapshot_ordering() {
        fn test_with_seed(seed: u64) {
            let rng = rand_chacha::ChaCha8Rng::seed_from_u64(seed);
            let metrics = Metrics::new();

            for v in rng.random_iter::<u16>().take(100) {
                metrics.log_query_latency(v as u64).unwrap();
            }

            let Snapshot {
                min,
                max,
                median,
                percentile_75,
                percentile_95,
                percentile_98,
                percentile_99,
                percentile_99_9,
                ..
            } = metrics.get_snapshot().unwrap();

            assert!(min <= median);
            assert!(median <= percentile_75);
            assert!(percentile_75 <= percentile_95);
            assert!(percentile_95 <= percentile_98);
            assert!(percentile_98 <= percentile_99);
            assert!(percentile_99 <= percentile_99_9);
            assert!(percentile_99_9 <= max);
        }

        test_with_seed(u64::MIN);
        test_with_seed(u64::MAX);
        test_with_seed(42);
        test_with_seed(0xDEADCAFE);
    }
}
