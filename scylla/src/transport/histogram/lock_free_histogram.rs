use std::hint;
/// This file was inspired by a lock-free histogram implementation
/// from the Prometheus library in Go.
/// https://github.com/prometheus/client_golang/blob/main/prometheus/histogram.go
/// Note: in the current implementation, the histogram *may* incur a data race
/// after (1 << 63) increments (which is quite a lot).
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::Config;

const ORDER_TYPE: Ordering = Ordering::Relaxed;
const HIGH_BIT: u64 = 1 << 63;
const LOW_MASK: u64 = HIGH_BIT - 1;

#[derive(Debug)]
pub struct Histogram {
    /// Hot index - index of the hot pool.
    /// Count - number of all started observations.
    /// Use of both of these variables in one AtomicU64
    /// allows use of a lock-free algorithm.
    hot_idx_and_count: AtomicU64,
    /// Finished observations for each bucket pool.
    pool_counts: [AtomicU64; 2],
    /// Two bucket pools: hot and cold.
    bucket_pools: [Box<[AtomicU64]>; 2],
    /// Mutex for "writers" (logging operations)
    /// (logging time is not as critical).
    logger_mutex: Arc<Mutex<i8>>,
    /// Standard configuration for math operations.
    config: Config,
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
    pub percentile_90: u64,
    pub percentile_95: u64,
    pub percentile_99: u64,
    pub percentile_99_9: u64,
}

impl Histogram {
    pub fn new() -> Self {
        let grouping_power = 7;
        let max_value_power = 64;
        let config = Config::new(grouping_power, max_value_power)
            .expect("default histogram construction failure");

        Self::with_config(&config)
    }

    pub fn with_config(config: &Config) -> Self {
        let mut buckets1 = Vec::with_capacity(config.total_buckets());
        buckets1.resize_with(config.total_buckets(), || AtomicU64::new(0));
        let mut buckets2 = Vec::with_capacity(config.total_buckets());
        buckets2.resize_with(config.total_buckets(), || AtomicU64::new(0));

        Self {
            hot_idx_and_count: AtomicU64::new(0),
            pool_counts: [AtomicU64::new(0), AtomicU64::new(0)],
            bucket_pools: [buckets1.into(), buckets2.into()],
            logger_mutex: Arc::new(Mutex::new(0)),
            config: *config,
        }
    }

    pub fn increment(&self, value: u64) -> Result<(), &'static str> {
        // Increment started observations count.
        let n = self.hot_idx_and_count.fetch_add(1, ORDER_TYPE);
        let hot_idx = (n >> 63) as usize;

        // Increment the corresponding bucket value.
        let idx = self.config.value_to_index(value)?;
        self.bucket_pools[hot_idx][idx].fetch_add(1, ORDER_TYPE);

        // Increment finished observations count.
        self.pool_counts[hot_idx].fetch_add(1, ORDER_TYPE);
        Ok(())
    }

    pub fn mean() -> impl FnOnce(&[AtomicU64], &Config) -> Result<u64, &'static str> {
        |buckets, config| {
            let total_count = Histogram::get_total_count(buckets);

            let mut weighted_sum = 0;
            for (i, bucket) in buckets.iter().enumerate() {
                // Note: we choose index_to_lower_bound here
                // but that choice is arbitrary.
                weighted_sum +=
                    bucket.load(ORDER_TYPE) as u128 * config.index_to_lower_bound(i) as u128;
            }
            Ok((weighted_sum / total_count) as u64)
        }
    }

    pub fn percentile(
        percentile: f64,
    ) -> impl FnOnce(&[AtomicU64], &Config) -> Result<u64, &'static str> {
        move |buckets, config| {
            if !(0.0..100.0).contains(&percentile) {
                return Err("percentile out of bounds");
            }

            let total_count = Histogram::get_total_count(buckets);
            let count = (percentile / 100.0 * total_count as f64).ceil() as u128;

            let mut pref_sum = 0;
            for (i, bucket) in buckets.iter().enumerate() {
                if pref_sum >= count {
                    // Note: we choose index_to_lower_bound here and after the loop
                    // but that choice is arbitrary.
                    return Ok(config.index_to_lower_bound(i));
                }
                pref_sum += bucket.load(ORDER_TYPE) as u128;
            }
            Ok(config.index_to_lower_bound(buckets.len() - 1))
        }
    }

    pub fn snapshot() -> impl FnOnce(&[AtomicU64], &Config) -> Result<Snapshot, &'static str> {
        |buckets, config| {
            let total_count = Histogram::get_total_count(buckets);

            let mut min = u64::MAX;
            let mut max = 0;
            let mut weighted_sum = 0;
            let mut pref_sum = 0;
            let mut percentile_75 = 0;
            let mut percentile_90 = 0;
            let mut percentile_95 = 0;
            let mut percentile_99 = 0;
            let mut percentile_99_9 = 0;

            let percentile_75_threshold = (0.75 * total_count as f64).ceil() as u128;
            let percentile_90_threshold = (0.9 * total_count as f64).ceil() as u128;
            let percentile_95_threshold = (0.95 * total_count as f64).ceil() as u128;
            let percentile_99_threshold = (0.99 * total_count as f64).ceil() as u128;
            let percentile_99_9_threshold = (0.999 * total_count as f64).ceil() as u128;

            for (i, bucket) in buckets.iter().enumerate() {
                let count = bucket.load(ORDER_TYPE) as u128;
                if count == 0 {
                    continue;
                }

                let lower_bound = config.index_to_lower_bound(i);
                let upper_bound = config.index_to_upper_bound(i);

                if lower_bound < min {
                    min = lower_bound;
                }
                if upper_bound > max {
                    max = upper_bound;
                }

                weighted_sum += count * lower_bound as u128;

                let next_pref_sum = pref_sum + count;
                if pref_sum < percentile_75_threshold && next_pref_sum >= percentile_75_threshold {
                    percentile_75 = lower_bound;
                }
                if pref_sum < percentile_90_threshold && next_pref_sum >= percentile_90_threshold {
                    percentile_90 = lower_bound;
                }
                if pref_sum < percentile_95_threshold && next_pref_sum >= percentile_95_threshold {
                    percentile_95 = lower_bound;
                }
                if pref_sum < percentile_99_threshold && next_pref_sum >= percentile_99_threshold {
                    percentile_99 = lower_bound;
                }
                if pref_sum < percentile_99_9_threshold
                    && next_pref_sum >= percentile_99_9_threshold
                {
                    percentile_99_9 = lower_bound;
                }

                pref_sum = next_pref_sum;
            }

            let mean = (weighted_sum / total_count) as u64;
            let mut variance_sum = 0;
            for (i, bucket) in buckets.iter().enumerate() {
                let count = bucket.load(ORDER_TYPE) as u128;
                if count == 0 {
                    continue;
                }

                let lower_bound = config.index_to_lower_bound(i);
                variance_sum += count * (lower_bound as u128 - mean as u128).pow(2);
            }
            let variance = variance_sum / total_count;
            let stddev = (variance as f64).sqrt() as u64;

            Ok(Snapshot {
                min,
                max,
                mean,
                stddev,
                median: config.index_to_lower_bound(buckets.len() / 2),
                percentile_75,
                percentile_90,
                percentile_95,
                percentile_99,
                percentile_99_9,
            })
        }
    }

    pub fn get_total_count(buckets: &[AtomicU64]) -> u128 {
        buckets.iter().map(|v| v.load(ORDER_TYPE) as u128).sum()
    }

    pub fn log_operation<T>(
        &self,
        f: impl FnOnce(&[AtomicU64], &Config) -> Result<T, &'static str>,
    ) -> Result<T, &'static str> {
        // Lock the "writers" mutex.
        let _guard = self.logger_mutex.lock();
        if _guard.is_err() {
            return Err("couldn't lock the logger mutex");
        }
        let _guard = _guard.unwrap();

        // Switch the hot-cold index (wrapping add on highest bit).
        let n = self.hot_idx_and_count.fetch_add(HIGH_BIT, ORDER_TYPE);
        let started_count = n & LOW_MASK;

        let hot_idx = (n >> 63) as usize;
        let cold_idx = 1 - hot_idx;

        let hot_counts = &self.pool_counts[hot_idx];
        let cold_counts = &self.pool_counts[cold_idx];

        // Wait until the old hot observers (now working on the currently
        // cold bucket pool) finish their job.
        // (Since observer's job is fast, we can wait in a spin loop).
        while started_count != cold_counts.load(ORDER_TYPE) {
            hint::spin_loop();
        }

        // Now there are no active observers on the cold pool, so we can safely
        // access the data without a logical race.
        let result = f(&self.bucket_pools[cold_idx], &self.config);

        // Compund the cold histogram results onto the already running hot ones.
        // Note that no logging operation can run now as we still hold
        // the mutex, so it doesn't matter that the entire operation isn't atomic.

        // Update finished observations' counts.
        hot_counts.fetch_add(cold_counts.load(ORDER_TYPE), ORDER_TYPE);
        cold_counts.store(0, ORDER_TYPE);

        // Update bucket values (both pools have the same length).
        for i in 0..self.bucket_pools[0].len() {
            let hot_bucket = &self.bucket_pools[hot_idx][i];
            let cold_bucket = &self.bucket_pools[cold_idx][i];

            hot_bucket.fetch_add(cold_bucket.load(ORDER_TYPE), ORDER_TYPE);
            cold_bucket.store(0, ORDER_TYPE);
        }

        result
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram::new()
    }
}
