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
    // This generates a new timestamp
    fn next_timestamp(&self) -> i64;
}

/// Basic timestamp generator. Provides no guarantees, if system clock returns
/// time before UNIX epoch it panics.
#[derive(Default)]
pub struct SimpleTimestampGenerator {}

#[allow(dead_code)]
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
pub struct MonotonicTimestampGeneratorCfg {
    warning_threshold: Duration,
    warning_interval: Duration,
}

/// Monotonic timestamp generator. Guarantees monotonicity of timestamps.
/// If system clock will not provide an increased timestamp, then the timestamp will
/// be artificially increased. If the config is provided and the clock skew is bigger than
/// warning_threshold (by default 1 second), then the user will be warned about
/// the skew repeatedly, with warning_interval provided in the settings (by default 1 second).
/// Remember that this generator only guarantees monotonicity within one client!
/// If you create multiple clients with MonotonicTimestampGenerators the monotonicity
/// guarantee becomes void.
pub struct MonotonicTimestampGenerator {
    last: AtomicI64,
    last_warning: Mutex<Instant>,
    config: Option<MonotonicTimestampGeneratorCfg>,
}

impl MonotonicTimestampGenerator {
    /// Creates a new monotonic timestamp generator with provided settings
    pub fn new_with_settings(config: Option<MonotonicTimestampGeneratorCfg>) -> Self {
        MonotonicTimestampGenerator {
            last: AtomicI64::new(0),
            last_warning: Mutex::new(Instant::now()),
            config,
        }
    }
    /// Creates a new monotonic timestamp generator with default settings
    pub fn new() -> Self {
        MonotonicTimestampGenerator::new_with_settings(Some(MonotonicTimestampGeneratorCfg {
            warning_threshold: Duration::from_secs(1),
            warning_interval: Duration::from_secs(1),
        }))
    }

    // This is guaranteed to return a monotonic timestamp. If clock skew is detected
    // then this method will increment the last timestamp.
    fn compute_next(&self, last: i64) -> i64 {
        let current = SystemTime::now().duration_since(UNIX_EPOCH);
        if let Ok(cur_time) = current {
            let u_cur = cur_time.as_micros() as i64;
            if u_cur > last {
                return u_cur;
            } else if let Some(cfg) = self.config.as_ref() {
                if last - u_cur > cfg.warning_threshold.as_micros() as i64 {
                    let mut last_warn = self.last_warning.lock().unwrap();
                    let now = Instant::now();
                    if now >= last_warn.checked_add(cfg.warning_interval).unwrap() {
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
