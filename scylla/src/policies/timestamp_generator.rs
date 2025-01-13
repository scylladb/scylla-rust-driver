use std::{
    sync::atomic::AtomicI64,
    time::{SystemTime, UNIX_EPOCH},
};

use std::sync::atomic::Ordering;
use std::sync::Mutex;
use tokio::time::{Duration, Instant};
use tracing::warn;

/// Trait used to represent a timestamp generator
pub trait TimestampGenerator: Send + Sync {
    /// This generates a new timestamp
    fn next_timestamp(&self) -> i64;
}

/// Basic timestamp generator. Provides no guarantees, if system clock returns
/// time before UNIX epoch it panics.
#[derive(Default)]
pub struct SimpleTimestampGenerator {}

impl SimpleTimestampGenerator {
    pub fn new() -> Self {
        SimpleTimestampGenerator {}
    }
}

impl TimestampGenerator for SimpleTimestampGenerator {
    fn next_timestamp(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }
}

/// Warning configuration for MonotonicTimestampGenerator
struct MonotonicTimestampGeneratorWarningsCfg {
    warning_threshold: Duration,
    warning_interval: Duration,
}

/// Monotonic timestamp generator. Guarantees monotonicity of timestamps.
/// If system clock will not provide an increased timestamp, then the timestamp will
/// be artificially increased. If the config is provided and the clock skew is bigger than
/// warning_threshold (by default 1 second), then the user will be warned about
/// the skew repeatedly, with warning_interval provided in the settings (by default 1 second).
/// Remember that this generator only guarantees monotonicity within one instance of this struct!
/// If you create multiple instances the monotonicity guarantee becomes void.
pub struct MonotonicTimestampGenerator {
    last: AtomicI64,
    last_warning: Mutex<Instant>,
    config: Option<MonotonicTimestampGeneratorWarningsCfg>,
}

impl MonotonicTimestampGenerator {
    /// Creates a new monotonic timestamp generator with default settings
    pub fn new() -> Self {
        MonotonicTimestampGenerator {
            last: AtomicI64::new(0),
            last_warning: Mutex::new(Instant::now()),
            config: Some(MonotonicTimestampGeneratorWarningsCfg {
                warning_threshold: Duration::from_secs(1),
                warning_interval: Duration::from_secs(1),
            }),
        }
    }

    pub fn with_warning_times(
        mut self,
        warning_threshold: Duration,
        warning_interval: Duration,
    ) -> Self {
        self.config = Some(MonotonicTimestampGeneratorWarningsCfg {
            warning_threshold,
            warning_interval,
        });
        self
    }

    pub fn without_warnings(mut self) -> Self {
        self.config = None;
        self
    }

    // This is guaranteed to return a monotonic timestamp. If clock skew is detected
    // then this method will increment the last timestamp.
    fn compute_next(&self, last: i64) -> i64 {
        let current = SystemTime::now().duration_since(UNIX_EPOCH);
        if let Ok(cur_time) = current {
            // We have generated a valid timestamp
            let u_cur = cur_time.as_micros() as i64;
            if u_cur > last {
                // We have generated a valid, monotonic timestamp
                return u_cur;
            } else if let Some(cfg) = self.config.as_ref() {
                // We have detected clock skew, we will increment the last timestamp, and check if we should warn the user
                if last - u_cur > cfg.warning_threshold.as_micros() as i64 {
                    // We have detected a clock skew bigger than the threshold, we check if we warned the user recently
                    let mut last_warn = self.last_warning.lock().unwrap();
                    let now = Instant::now();
                    if now >= last_warn.checked_add(cfg.warning_interval).unwrap() {
                        // We have not warned the user recently, we will warn the user
                        *last_warn = now;
                        drop(last_warn);
                        warn!(
                            "Clock skew detected. The current time ({}) was {} \
                    microseconds behind the last generated timestamp ({}). \
                    The next generated timestamp will be artificially incremented \
                    to guarantee monotonicity.",
                            u_cur,
                            last - u_cur,
                            last
                        )
                    }
                }
            }
        } else {
            // We have generated a timestamp before UNIX epoch, we will warn the user and increment the last timestamp
            warn!("Clock skew detected. The current time was behind UNIX epoch.");
        }

        last + 1
    }
}

impl Default for MonotonicTimestampGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TimestampGenerator for MonotonicTimestampGenerator {
    fn next_timestamp(&self) -> i64 {
        loop {
            let last = self.last.load(Ordering::SeqCst);
            let cur = self.compute_next(last);
            if self
                .last
                .compare_exchange(last, cur, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return cur;
            }
        }
    }
}

#[test]
fn monotonic_timestamp_generator_is_monotonic() {
    const NUMBER_OF_ITERATIONS: u32 = 1000;

    let mut prev = None;
    let mut cur;
    let generator = MonotonicTimestampGenerator::new();
    for _ in 0..NUMBER_OF_ITERATIONS {
        cur = generator.next_timestamp();
        if let Some(prev_val) = prev {
            assert!(cur > prev_val);
        }
        prev = Some(cur);
    }
}

#[test]
fn monotonic_timestamp_generator_is_monotonic_with_concurrency() {
    use std::collections::HashSet;
    use std::sync::Arc;

    const NUMBER_OF_ITERATIONS: usize = 1000;
    const NUMBER_OF_THREADS: usize = 10;
    let generator = Arc::new(MonotonicTimestampGenerator::new());
    let timestamps_sets: Vec<_> = std::thread::scope(|s| {
        (0..NUMBER_OF_THREADS)
            .map(|_| {
                s.spawn(|| {
                    let timestamps: Vec<i64> = (0..NUMBER_OF_ITERATIONS)
                        .map(|_| generator.next_timestamp())
                        .collect();
                    assert!(timestamps.windows(2).all(|w| w[0] < w[1]));
                    let timestamps_set: HashSet<i64> = HashSet::from_iter(timestamps);
                    assert_eq!(
                        timestamps_set.len(),
                        NUMBER_OF_ITERATIONS,
                        "Colliding values in a single thread"
                    );
                    timestamps_set
                })
            })
            .map(|handle| handle.join().unwrap())
            .collect()
    });

    let full_set: HashSet<i64> = timestamps_sets.iter().flatten().copied().collect();
    assert_eq!(
        full_set.len(),
        NUMBER_OF_ITERATIONS * NUMBER_OF_THREADS,
        "Colliding values between threads"
    );
}
