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

/// Basic monotonic timestamp generator. Guarantees monotonicity of timestamps.
/// If system clock will not provide an increased timestamp, then the timestamp will
/// be artificially increased. If the warning_threshold (by default 1 second) is not None
/// and the clock skew is bigger than its value, then the user will be warned about
/// the skew repeatedly, with warning_interval provided in the settings (by default 1 second).
/// warning_interval should not be set to None if warning_threshold is not None, as it is
/// meaningless, if every generated timestamp over the skew threshold should warn, Duration::ZERO
/// should be used instead.
pub struct MonotonicTimestampGenerator {
    last: AtomicI64,
    last_warning: Mutex<Instant>,
    warning_threshold: Option<Duration>,
    warning_interval: Option<Duration>,
}

impl MonotonicTimestampGenerator {
    /// Creates a new monotonic timestamp generator with provided settings
    pub fn new_with_settings(
        warning_threshold: Option<Duration>,
        warning_interval: Option<Duration>,
    ) -> Self {
        let mut threshold = warning_threshold;
        if warning_interval.is_none() && warning_threshold.is_some() {
            warn!(
                "Do not set warning_interval to None when warning_threshold is not None. \
                Replacing with Duration::ZERO"
            );
            threshold = Some(Duration::ZERO);
        }
        MonotonicTimestampGenerator {
            last: AtomicI64::new(0),
            last_warning: Mutex::new(Instant::now()),
            warning_threshold,
            warning_interval: threshold,
        }
    }
    /// Creates a new monotonic timestamp generator with default settings
    pub fn new() -> Self {
        MonotonicTimestampGenerator::new_with_settings(
            Some(Duration::from_secs(1)),
            Some(Duration::from_secs(1)),
        )
    }

    // This is guaranteed to return a monotonic timestamp. If clock skew is detected
    // then this method will increment the last timestamp.
    async fn compute_next(&self, last: i64) -> i64 {
        let current = SystemTime::now().duration_since(UNIX_EPOCH);
        if let Ok(cur_time) = current {
            let u_cur = cur_time.as_micros() as i64;
            if u_cur > last {
                return u_cur;
            } else if self.warning_threshold.is_some()
                && last - u_cur > self.warning_threshold.unwrap().as_micros() as i64
            {
                let mut last_warn = self.last_warning.lock().await;
                let now = Instant::now();
                if now
                    >= last_warn
                        .checked_add(self.warning_interval.unwrap())
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
