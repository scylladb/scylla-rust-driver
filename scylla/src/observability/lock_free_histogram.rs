//! This file was inspired by a lock-free histogram implementation
//! from the Prometheus library in Go.
//! https://github.com/prometheus/client_golang/blob/main/prometheus/histogram.go
//! Note: in the current implementation, the histogram *may* incur a data race
//! after (1 << 63) increments (which is quite a lot).

use histogram::{AtomicHistogram, Histogram};
use std::hint;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;

/// Bit mask for the highest bit in a 64-bit value.
/// Corresponds to the hot index.
const HIGH_BIT: u64 = 1 << 63;
/// Bit mask for the lower 63 bits in a 64-bit value.
/// Corresponds to the total increment counter.
const LOW_MASK: u64 = HIGH_BIT - 1;

/// Error that occured during lock-free histogram operations
#[derive(Error, Debug, PartialEq)]
pub enum LockFreeHistogramError {
    #[error("Invalid use of histogram: {0}")]
    HistogramError(#[from] histogram::Error),
    #[error("Could not lock the snapshot mutex")]
    Mutex,
}

/// # Lock-free histogram
/// Histogram wrapper with efficient synchronisation mechanisms allowing for
/// atomic reads (`increment` method) and writes (`snapshot` method).
///
/// ## Increments
/// Increments are lock-free and only consist of a few atomics modifications.
///
/// ## Snapshots
/// Snapshots are queued on a mutex and spin-locked for a brief period of time
/// upon synchronisation with increments.
///
/// ## Internals
/// The wrapper consists of:
/// - two `AtomicHistogram`'s (a cold and a hot one) which combined
///   hold the histogram's state,
/// - some synchronisation utilities.
///
/// ## Motivation
/// This struct was motivated by the lack of atomic reads
/// in `AtomicHistogram` from the histogram crate.
pub(crate) struct LockFreeHistogram {
    /// Hot index - index of the hot pool.
    /// Count - number of all started observations.
    /// Use of both of these variables in one AtomicU64
    /// allows use of a lock-free algorithm.
    hot_idx_and_count: AtomicU64,
    /// Finished observations for each bucket pool.
    pool_counts: [AtomicU64; 2],
    /// Two histograms in a pool: hot and cold.
    histogram_pool: [AtomicHistogram; 2],
    /// Mutex for "writers" (snapshot operations)
    /// (snapshot performance is not as critical).
    snapshot_mutex: Arc<Mutex<()>>,
}

impl LockFreeHistogram {
    pub(crate) fn new(grouping_power: u8, max_value_power: u8) -> Self {
        Self {
            hot_idx_and_count: AtomicU64::new(0),
            pool_counts: [AtomicU64::new(0), AtomicU64::new(0)],
            histogram_pool: [
                AtomicHistogram::new(grouping_power, max_value_power).unwrap(),
                AtomicHistogram::new(grouping_power, max_value_power).unwrap(),
            ],
            snapshot_mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Atomically increments the bucket corresponding to `value` in the histogram.
    pub(crate) fn increment(&self, value: u64) -> Result<(), LockFreeHistogramError> {
        // Increment started observations count.
        let n = self.hot_idx_and_count.fetch_add(1, Ordering::Relaxed);
        let hot_idx = (n >> 63) as usize;

        // Increment the corresponding bucket value.
        self.histogram_pool[hot_idx].increment(value)?;

        // Increment finished observations count.
        // Release ordering is needed to guarantee the bucket increment's happens-before
        // relationship with the acquiring side.
        self.pool_counts[hot_idx].fetch_add(1, Ordering::Release);
        Ok(())
    }

    /// Returns an atomic snapshot of the histogram
    pub(crate) fn snapshot(&self) -> Result<Histogram, LockFreeHistogramError> {
        // Lock the "writers" mutex.
        let _guard = self.snapshot_mutex.lock();
        if _guard.is_err() {
            return Err(LockFreeHistogramError::Mutex);
        }
        let _guard = _guard.unwrap();

        // Switch the hot-cold index (wrapping add on highest bit).
        // Note: n is the one from *before* the addition, so we repeat it.
        let n = self
            .hot_idx_and_count
            .fetch_add(HIGH_BIT, Ordering::Relaxed)
            .wrapping_add(HIGH_BIT);

        let started_count = n & LOW_MASK;

        let hot_idx = (n >> 63) as usize;
        let cold_idx = 1 - hot_idx;

        let hot_counts = &self.pool_counts[hot_idx];
        let cold_counts = &self.pool_counts[cold_idx];

        // Wait until the old hot observers (now working on the currently
        // cold bucket pool) finish their job.
        // (Since observer's job is fast, we can wait in a spin loop).
        // The acquire ordering guarantees that all relating bucket increments
        // happened before.
        while started_count != cold_counts.load(Ordering::Acquire) {
            hint::spin_loop();
        }

        // Now there are no active observers on the cold pool, so we can safely
        // access the data without a logical race.
        let result = self.histogram_pool[cold_idx].load();

        // Compound the cold histogram results onto the already running hot ones.
        // Note that no snapshot operation can run now as we still hold
        // the mutex, so it doesn't matter that the entire operation isn't atomic.

        // Update finished observations' counts.
        hot_counts.fetch_add(started_count, Ordering::Relaxed);
        cold_counts.store(0, Ordering::Relaxed);

        // Update hot bucket values.
        let snapshot = self.histogram_pool[cold_idx].drain();
        for bucket in snapshot.into_iter() {
            self.histogram_pool[hot_idx].add(bucket.start(), bucket.count())?;
        }

        Ok(result)
    }
}

impl Default for LockFreeHistogram {
    fn default() -> Self {
        // Config: 64ms error, values in range [0ms, ~262_000ms].
        // Size: (2^13 + 5 * 2^12) * 8B * 2 ~= 450kB.
        let grouping_power = 12;
        let max_value_power = 18;
        LockFreeHistogram::new(grouping_power, max_value_power)
    }
}

impl std::fmt::Debug for LockFreeHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h0 = self.histogram_pool[0].load();
        let h1 = self.histogram_pool[1].load();
        f.debug_struct("LockFreeHistogram")
            .field("hot_idx_and_count", &self.hot_idx_and_count)
            .field("pool_counts", &self.pool_counts)
            .field("histogram_pool", &[h0, h1])
            .field("snapshot_mutex", &self.snapshot_mutex)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::LockFreeHistogram;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    use std::thread;

    fn spawn_all(
        observer: impl Fn() + Clone + Send + 'static,
        observer_count: u64,
        snapshot: impl Fn() + Clone + Send + 'static,
        snapshot_count: u64,
    ) {
        // Spawn all threads.
        // Note: snapshots must be joined first!
        // That's because observers wait for snapshots to
        // finish correctly, so we need to catch the snapshot
        // panic before joining any observer thread.
        let mut handles = Vec::new();
        for _ in 0..snapshot_count {
            handles.push(thread::spawn(snapshot.clone()));
        }
        for _ in 0..observer_count {
            handles.push(thread::spawn(observer.clone()));
        }

        for thread in handles {
            thread.join().unwrap();
        }
    }

    /// Snapshot and increment liveness check.
    ///
    /// ## Setup:
    /// - observer threads atomically incrementing the histogram,
    /// - snapshot threads fighting for exclusive access.
    ///
    /// ## Expected behavior:
    /// - no deadlock occurs.
    fn liveness(observer_count: u64, snapshot_count: u64, rep: u64) {
        let h = Arc::new(LockFreeHistogram::default());
        // The number of finished snapshot *threads*.
        let finished_snapshots = Arc::new(AtomicU64::new(0));

        // Observer thread closure.
        let h_1 = h.clone();
        let fin_s_1 = finished_snapshots.clone();
        let observer = move || {
            while fin_s_1.load(Ordering::Relaxed) < snapshot_count {
                h_1.increment(0).unwrap();
            }
        };

        // Snapshot thread closure.
        let snapshot = move || {
            for _ in 0..rep {
                let _ = h.snapshot().unwrap();
            }
            // Mark as finished.
            finished_snapshots.fetch_add(1, Ordering::Relaxed);
        };

        spawn_all(observer, observer_count, snapshot, snapshot_count);
    }

    #[test]
    fn liveness_tests() {
        liveness(1, 2, 1000);
        liveness(2, 2, 1000);
        liveness(10, 10, 100);
    }

    /// Atomic snapshot test.
    ///
    /// ## Setup:
    /// - 1 observer alternating between incrementing two distinct values
    ///   of the histogram,
    /// - 1 snapshot.
    ///
    /// ## Expected behavior:
    /// - the difference between said values in each snapshot is either 1 or 0.
    #[test]
    fn atomic_tests() {
        // The number of snapshots taken.
        let rep = 10_000;
        let snapshot_count = 1;
        // A value of which increment affects a bucket other than the first one.
        let value_in_other_bucket = 2_u64.pow(10);

        // The histogram is non-empty for code simplicity.
        let h = Arc::new(LockFreeHistogram::default());
        h.increment(0).unwrap();
        h.increment(value_in_other_bucket).unwrap();

        // The number of finished snapshot *threads*.
        let finished_snapshots = Arc::new(AtomicU64::new(0));

        // Observer thread closure.
        let h_1 = h.clone();
        let fin_s_1 = finished_snapshots.clone();
        let observer = move || {
            while fin_s_1.load(Ordering::Relaxed) < snapshot_count {
                h_1.increment(0).unwrap();
                h_1.increment(value_in_other_bucket).unwrap();
            }
        };

        // Snapshot thread closure.
        let snapshot = move || {
            for _ in 0..rep {
                let s = h.snapshot().unwrap();
                // Get count at first bucket.
                let bucket_front = s.percentile(10.0).unwrap().unwrap();
                let count_front = bucket_front.count();
                // Get count at the other bucket.
                let bucket_back = s.percentile(100.0).unwrap().unwrap();
                let count_back = bucket_back.count();

                // Make sure the test actually works
                // (the other bucket is not the first one).
                assert!(bucket_front != bucket_back);
                // Make sure the snapshot was atomic.
                assert!(count_front >= count_back && count_front - count_back <= 1);
            }
            // Mark as finished.
            finished_snapshots.fetch_add(1, Ordering::Relaxed);
        };

        spawn_all(observer, 1, snapshot, 1);
    }
}
