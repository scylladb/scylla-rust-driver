use crate::observability::lock_free_histogram::{LockFreeHistogram, LockFreeHistogramError};
use histogram::Histogram;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use thiserror::Error;

const ORDER_TYPE: Ordering = Ordering::Relaxed;

#[derive(Error, Debug, PartialEq)]
pub enum MetricsError {
    #[error("Lock-free histogram error: {0}")]
    LockFreeHistogramError(#[from] LockFreeHistogramError),
    #[error("Histogram error: {0}")]
    HistogramError(#[from] histogram::Error),
    #[error("Histogram is empty")]
    Empty,
}

/// Snapshot is a structure that contains histogram statistics such as
/// min, max, mean, standard deviation, median, and most common percentiles
/// collected in a certain moment.
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

const INTERVAL: u64 = 5;

#[derive(Debug)]
struct ExponentiallyWeightedMovingAverage {
    alpha: f64,
    uncounted: AtomicU64,
    is_initialized: AtomicBool,
    rate: AtomicU64,
}

impl ExponentiallyWeightedMovingAverage {
    fn new(alpha: f64) -> Self {
        Self {
            alpha,
            uncounted: AtomicU64::new(0),
            is_initialized: AtomicBool::new(false),
            rate: AtomicU64::new(0),
        }
    }

    fn rate(&self) -> f64 {
        f64::from_bits(self.rate.load(Ordering::Acquire))
    }

    fn update(&self) {
        self.uncounted.fetch_add(1, ORDER_TYPE);
    }

    fn tick(&self) {
        let count = self.uncounted.swap(0, ORDER_TYPE);
        let instant_rate = count as f64 / INTERVAL as f64;

        if self.is_initialized.load(Ordering::Acquire) {
            let rate = f64::from_bits(self.rate.load(Ordering::Acquire));
            self.rate.store(
                f64::to_bits(rate + self.alpha * (instant_rate - rate)),
                Ordering::Release,
            );
        } else {
            self.rate
                .store(f64::to_bits(instant_rate), Ordering::Release);
            self.is_initialized.store(true, Ordering::Release);
        }
    }
}

#[derive(Debug)]
struct Meter {
    one_minute_rate: ExponentiallyWeightedMovingAverage,
    five_minute_rate: ExponentiallyWeightedMovingAverage,
    fifteen_minute_rate: ExponentiallyWeightedMovingAverage,
    count: AtomicU64,
    start_time: std::time::Instant,
    last_tick: AtomicU64,
}

impl Meter {
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
        let old_tick = self.last_tick.load(ORDER_TYPE);
        let new_tick = self.start_time.elapsed().as_nanos() as u64;
        let elapsed = new_tick - old_tick;

        if elapsed > INTERVAL * 1_000_000_000 {
            let new_interval_start_tick = new_tick - elapsed % (INTERVAL * 1_000_000_000);
            if self
                .last_tick
                .compare_exchange(old_tick, new_interval_start_tick, ORDER_TYPE, ORDER_TYPE)
                .is_ok()
            {
                let required_ticks = elapsed / (INTERVAL * 1_000_000_000);
                for _ in 0..required_ticks {
                    self.one_minute_rate.tick();
                    self.five_minute_rate.tick();
                    self.fifteen_minute_rate.tick();
                }
            }
        }
    }
}

impl Default for Meter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default, Debug)]
pub struct Metrics {
    errors_num: AtomicU64,
    queries_num: AtomicU64,
    errors_iter_num: AtomicU64,
    queries_iter_num: AtomicU64,
    retries_num: AtomicU64,
    histogram: Arc<LockFreeHistogram>,
    meter: Arc<Meter>,
    total_connections: AtomicU64,
    connection_timeouts: AtomicU64,
    request_timeouts: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            errors_num: AtomicU64::new(0),
            queries_num: AtomicU64::new(0),
            errors_iter_num: AtomicU64::new(0),
            queries_iter_num: AtomicU64::new(0),
            retries_num: AtomicU64::new(0),
            histogram: Arc::new(LockFreeHistogram::default()),
            meter: Arc::new(Meter::new()),
            total_connections: AtomicU64::new(0),
            connection_timeouts: AtomicU64::new(0),
            request_timeouts: AtomicU64::new(0),
        }
    }

    /// Increments counter for errors that occurred in nonpaged queries.
    pub(crate) fn inc_failed_nonpaged_queries(&self) {
        self.errors_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for nonpaged queries.
    pub(crate) fn inc_total_nonpaged_queries(&self) {
        self.queries_num.fetch_add(1, ORDER_TYPE);
        self.meter.mark();
    }

    /// Increments counter for errors that occurred in paged queries.
    pub(crate) fn inc_failed_paged_queries(&self) {
        self.errors_iter_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for page queries in paged queries.
    /// If query_iter would return 4 pages then this counter should be incremented 4 times.
    pub(crate) fn inc_total_paged_queries(&self) {
        self.queries_iter_num.fetch_add(1, ORDER_TYPE);
        self.meter.mark();
    }

    /// Increments counter measuring how many times a retry policy has decided to retry a query
    pub(crate) fn inc_retries_num(&self) {
        self.retries_num.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for total number of connections
    pub(crate) fn inc_total_connections(&self) {
        self.total_connections.fetch_add(1, ORDER_TYPE);
    }

    /// Decrements counter for total number of connections
    pub(crate) fn dec_total_connections(&self) {
        self.total_connections.fetch_sub(1, ORDER_TYPE);
    }

    /// Increments counter for connection timeouts
    pub(crate) fn inc_connection_timeouts(&self) {
        self.connection_timeouts.fetch_add(1, ORDER_TYPE);
    }

    /// Increments counter for request timeouts
    pub(crate) fn inc_request_timeouts(&self) {
        self.request_timeouts.fetch_add(1, ORDER_TYPE);
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
        Self::mean(self.histogram.snapshot()?)
    }

    /// Returns latency from histogram for a given percentile
    /// # Arguments
    ///
    /// * `percentile` - float value (0.0 - 100.0)
    pub fn get_latency_percentile_ms(&self, percentile: f64) -> Result<u64, MetricsError> {
        let bucket = self.histogram.snapshot()?.percentile(percentile)?;

        if let Some(p) = bucket {
            Ok(p.count())
        } else {
            Err(MetricsError::Empty)
        }
    }

    /// Returns snapshot of histogram metrics taken at the moment of calling this function. \
    /// Available metrics: min, max, mean, std_dev, median,
    ///                    percentile_75, percentile_95, percentile_98,
    ///                    percentile_99, and percentile_99_9.
    pub fn get_snapshot(&self) -> Result<Snapshot, MetricsError> {
        let h = self.histogram.snapshot()?;

        let (min, max) = Self::minmax(h.clone())?;

        let percentile_args = [50.0, 75.0, 95.0, 98.0, 99.0, 99.9];
        let percentiles = Self::percentiles(&h, &percentile_args)?;

        Ok(Snapshot {
            min,
            max,
            mean: Self::mean(h.clone())?,
            stddev: Self::stddev(h)?,
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

    /// Returns mean rate of queries per second
    pub fn get_mean_rate(&self) -> f64 {
        self.meter.mean_rate()
    }

    /// Returns one-minute rate of queries per second
    pub fn get_one_minute_rate(&self) -> f64 {
        self.meter.one_minute_rate()
    }

    /// Returns five-minute rate of queries per second
    pub fn get_five_minute_rate(&self) -> f64 {
        self.meter.five_minute_rate()
    }

    /// Returns fifteen-minute rate of queries per second
    pub fn get_fifteen_minute_rate(&self) -> f64 {
        self.meter.fifteen_minute_rate()
    }

    /// Returns total number of active connections
    pub fn get_total_connections(&self) -> u64 {
        self.total_connections.load(ORDER_TYPE)
    }

    /// Returns counter for connection timeouts
    pub fn get_connection_timeouts(&self) -> u64 {
        self.connection_timeouts.load(ORDER_TYPE)
    }

    /// Returns counter for request timeouts
    pub fn get_request_timeouts(&self) -> u64 {
        self.request_timeouts.load(ORDER_TYPE)
    }

    // Metric implementations

    fn mean(h: Histogram) -> Result<u64, MetricsError> {
        // Compute the mean (count each bucket as its interval's center).
        let mut weighted_sum = 0_u128;
        let mut count = 0_u128;

        for bucket in h.into_iter() {
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
        let res = h.percentiles(percentiles)?;
        if let Some(ps) = res {
            Ok(ps.into_iter().map(|(_, bucket)| bucket.count()).collect())
        } else {
            Err(MetricsError::Empty)
        }
    }

    fn stddev(h: Histogram) -> Result<u64, MetricsError> {
        let total_count = h
            .clone()
            .into_iter()
            .map(|bucket| bucket.count() as u128)
            .sum::<u128>();

        let mean = Self::mean(h.clone())? as u128;
        let mut variance_sum = 0;
        for bucket in h.into_iter() {
            let count = bucket.count() as u128;
            let mid = ((bucket.start() + bucket.end()) / 2) as u128;

            variance_sum += count * (mid as i128 - mean as i128).pow(2) as u128;
        }
        let variance = variance_sum / total_count;

        Ok((variance as f64).sqrt() as u64)
    }

    fn minmax(h: Histogram) -> Result<(u64, u64), MetricsError> {
        let mut min = u64::MAX;
        let mut max = 0;
        for bucket in h.into_iter() {
            if bucket.count() == 0 {
                continue;
            }
            let lower_bound = bucket.start();
            let upper_bound = bucket.end();

            if lower_bound < min {
                min = lower_bound;
            }
            if upper_bound > max {
                max = upper_bound;
            }
        }

        if min > max {
            Err(MetricsError::Empty)
        } else {
            Ok((min, max))
        }
    }
}
