use std::{
    sync::atomic::AtomicI64,
    time::{SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::lock::Mutex;
use std::sync::atomic::Ordering;
use tokio::time::{Duration, Instant};
use tracing::warn;

/// Trait used to represent a timestamp generator
#[async_trait]
pub trait TimestampGenerator: Send + Sync {
    // This generates a new timestamp
    async fn next_timestamp(&self) -> i64;
}

/// Basic monotonic timestamp generator
pub struct MonotonicTimestampGenerator {
    last: AtomicI64,
    last_warning: Mutex<Instant>,
    warning_threshold_us: i64,
    warning_interval_ms: i64,
}

impl MonotonicTimestampGenerator {
    /// Creates a new monotonic timestamp generator with provided settings
    pub fn new_with_settings(warning_threshold_us: i64, warning_interval_ms: i64) -> Self {
        MonotonicTimestampGenerator {
            last: AtomicI64::new(0),
            last_warning: Mutex::new(Instant::now()),
            warning_threshold_us,
            warning_interval_ms,
        }
    }
    /// Creates a new monotonic timestamp generator with default settings
    pub fn new() -> Self {
        MonotonicTimestampGenerator::new_with_settings(1000000, 1000)
    }

    // This is guaranteed to return a monotonic timestamp. If clock skew is detected
    // then this method will increment the last timestamp.
    async fn compute_next(&self, last: i64) -> i64 {
        let current = SystemTime::now().duration_since(UNIX_EPOCH);
        if let Ok(cur_time) = current {
            let u_cur = cur_time.as_micros() as i64;
            if u_cur > last {
                return u_cur;
            } else if self.warning_threshold_us >= 0 && last - u_cur > self.warning_threshold_us {
                let mut last_warn = self.last_warning.lock().await;
                let now = Instant::now();
                if now
                    >= last_warn
                        .checked_add(Duration::from_millis(self.warning_interval_ms as u64))
                        .unwrap()
                {
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

#[async_trait]
impl TimestampGenerator for MonotonicTimestampGenerator {
    async fn next_timestamp(&self) -> i64 {
        loop {
            let last = self.last.load(Ordering::SeqCst);
            let cur = self.compute_next(last).await;
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
