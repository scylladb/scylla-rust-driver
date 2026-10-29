//! Utilities for rate limiting actions in a lock-free manner.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Permits an action at most once per configured interval.
///
/// Lock-free: a single relaxed atomic holds the timestamp of the last permit,
/// so that it is cheap enough to be queried on the hot path.
pub(crate) struct RateLimiter {
    interval_nanos: u64,
    last_permit_nanos: AtomicU64,
}

impl RateLimiter {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval_nanos: interval.as_nanos() as u64,
            last_permit_nanos: AtomicU64::new(0),
        }
    }

    /// Returns `true` if the action may be performed, i.e. if it was never permitted before
    /// or the last permit is older than the interval. Then, the last permit time is updated.
    ///
    /// Concurrent callers racing for the same permit are resolved by a CAS, so at most
    /// one of them succeeds.
    pub(crate) fn try_acquire(&self) -> bool {
        // Single global reference point for all rate limiters.
        // Used as a necessary absolute reference point for comparing [std::time::Instant].
        static GLOBAL_EPOCH: OnceLock<Instant> = OnceLock::new();

        let now = Instant::now();
        let epoch = *GLOBAL_EPOCH.get_or_init(|| now);

        let now_nanos = now.duration_since(epoch).as_nanos() as u64;

        let last_permit = self.last_permit_nanos.load(Ordering::Relaxed);

        // 0 is the sentinel for "never permitted", so the first call always succeeds.
        if last_permit == 0 {
            return self
                .last_permit_nanos
                .compare_exchange(
                    0,
                    now_nanos.max(1), // Ensure we never store the sentinel again.
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok();
        }

        now_nanos.saturating_sub(last_permit) >= self.interval_nanos
            && self
                .last_permit_nanos
                .compare_exchange(last_permit, now_nanos, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }
}

/// Logs a warning, unless `$limiter` (a [`RateLimiter`]) already permitted one
/// within its interval.
///
/// # Example
///
/// Ignored because `warn_rate_limited!` is not public API.
/// ```ignore
/// static LIMITER: RateLimiter = RateLimiter::new(Duration::from_secs(1));
///
/// // Warns at most once per second, no matter how often this is reached.
/// warn_rate_limited!(LIMITER, "Something is off with node {}", node);
/// ```
macro_rules! warn_rate_limited {
    ($limiter:expr, $($arg:tt)+) => {
        if $limiter.try_acquire() {
            ::tracing::warn!($($arg)+);
        }
    };
}

pub(crate) use warn_rate_limited;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rate_limit_state_basic() {
        let state = RateLimiter::new(Duration::from_millis(100));

        // First call should return true
        assert!(state.try_acquire());

        // Immediate second call should return false
        assert!(!state.try_acquire());

        // After waiting, should return true again
        std::thread::sleep(Duration::from_millis(150));
        assert!(state.try_acquire());
    }

    #[test]
    fn test_rate_limit_state_concurrent() {
        let state = Arc::new(RateLimiter::new(Duration::from_millis(10)));
        let success_count = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..10)
            .map(|_| {
                let state_clone = state.clone();
                let success_count_clone = success_count.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        if state_clone.try_acquire() {
                            success_count_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        // Should have significantly fewer successes than total attempts due to rate limiting
        let successes = success_count.load(Ordering::Relaxed);
        assert!(
            successes > 0,
            "Should have at least some successes (at least the first attempt)"
        );
        assert!(
            successes < 1000,
            "Should be rate limited, got {} successes",
            successes
        );
    }

    #[test]
    fn test_different_states_independent() {
        let state1 = RateLimiter::new(Duration::from_millis(100));
        let state2 = RateLimiter::new(Duration::from_millis(100));

        // Both should be able to acquire initially.
        assert!(state1.try_acquire());
        assert!(state2.try_acquire());

        // Both should be rate limited independently.
        assert!(!state1.try_acquire());
        assert!(!state2.try_acquire());
    }

    #[test]
    fn test_zero_interval() {
        let state = RateLimiter::new(Duration::ZERO);

        // With zero interval, every call should succeed
        assert!(state.try_acquire());
        assert!(state.try_acquire());
        assert!(state.try_acquire());
    }

    #[test]
    fn test_very_long_interval() {
        let state = RateLimiter::new(Duration::from_secs(3600)); // 1 hour

        // First call succeeds
        assert!(state.try_acquire());

        // Subsequent calls should fail
        assert!(!state.try_acquire());
        assert!(!state.try_acquire());
    }
}
