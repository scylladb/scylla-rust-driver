//! Utilities for rate limiting actions in a lock-free manner.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        OnceLock,
    },
    time::{Duration, Instant},
};

/// `RateLimiter` can be used to control the rate of permit acquisition.
/// It uses atomic operations to ensure that permits are acquired at most once per specified interval.
///
/// # Example
///
/// Ignored because `RateLimiter` is not public API.
/// ```ignore
/// let rate_limiter = RateLimiter::new();
/// // Try to acquire a permit with a 1 second interval.
/// if rate_limiter.try_acquire(Duration::from_secs(1)) {
///     println!("Permit acquired!");
/// } else {
///     println!("Rate limit exceeded, try again later.");
/// }
/// ```
pub(crate) struct RateLimiter {
    last_permit_nanos: AtomicU64,
}

impl RateLimiter {
    /// Creates a new `RateLimiter` instance, which is used to control the rate of permit acquisition.
    pub(crate) const fn new() -> Self {
        Self {
            last_permit_nanos: AtomicU64::new(0),
        }
    }

    /// Attempts to acquire a permit, which is subject of rate limiting,
    /// given the specified permit interval.
    ///
    /// If enough time has passed since the last permit was acquired,
    /// updates the last permit time and returns `true`. Otherwise,
    /// the rate is limited and `false` is returned.
    ///
    /// The first call to `try_acquire` will always succeed, allowing the first permit to be acquired immediately.
    pub(crate) fn try_acquire(&self, interval: Duration) -> bool {
        // Single global reference point for all rate limiters.
        // Used as a necessary absolute reference point for comparing [std::time::Instant].
        static GLOBAL_EPOCH: OnceLock<Instant> = OnceLock::new();

        let now = Instant::now();
        let epoch = *GLOBAL_EPOCH.get_or_init(|| now);

        let now_nanos = now.duration_since(epoch).as_nanos() as u64;
        let interval_nanos = interval.as_nanos() as u64;

        let last_permit = self.last_permit_nanos.load(Ordering::Relaxed);

        // Special case: if `last_permit` is 0, this is the first call, so allow it.
        if last_permit == 0 {
            // Try to update from 0 to current time.
            return self
                .last_permit_nanos
                .compare_exchange(
                    0,
                    now_nanos.max(1), // Ensure we never store 0 again.
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok();
        }

        // Normal case: check if enough time has passed.
        if now_nanos.saturating_sub(last_permit) >= interval_nanos {
            self.last_permit_nanos
                .compare_exchange(last_permit, now_nanos, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        } else {
            false
        }
    }
}

/// A macro to perform an action if the rate limit allows it.
///
/// This macro ensures that the given action is performed at most once per
/// specified interval, using efficient atomic operations for synchronization.
/// Each unique call site has its own independent rate limiting state.
///
/// # Arguments
///
/// * `$interval` - A `Duration` specifying the minimum time between actions.
/// * `$action` - A closure (of type `FnOnce()`) specifying an action executed
///   if the rate limit allows it.
///
/// # Example
///
/// Ignored because `rate_limited!` is not public API.
/// ```ignore
/// use std::time::Duration;
///
/// // This will only print a message once per second, no matter how many times it's called.
/// rate_limited!(Duration::from_secs(1), || println!("This is a rate-limited print"));
/// ```
///
/// # Implementation Details
///
/// - Uses `std::time::Instant` for monotonic time measurements immune to clock adjustments
/// - Uses `AtomicU64` for lock-free synchronization between threads
/// - Each macro call site gets its own static rate limiting state for independence
/// - Uses relaxed memory ordering for optimal performance
/// - Minimal macro expansion - just static definition + function call
///
/// # Thread Safety
///
/// This macro is fully thread-safe. Multiple threads can call the same rate-limited action
/// concurrently, and at most one thread will perform the action per interval.
macro_rules! rate_limited {
    ($interval:expr, $action:expr) => {{
        use $crate::observability::RateLimiter;

        // Each call site gets its own static rate limiting state.
        static RATE_LIMIT_STATE: RateLimiter = RateLimiter::new();

        // Check if we should warn and emit if so
        if RATE_LIMIT_STATE.try_acquire($interval) {
            $action();
        }
    }};
}

/// A rate-limited version of the `warn!()` macro that prevents spamming logs.
///
/// This macro ensures that warning messages are only emitted at most once per
/// specified interval, using efficient atomic operations for synchronization.
/// Each unique call site has its own independent rate limiting state.
///
/// # Arguments
///
/// * `$interval` - A `Duration` specifying the minimum time between warnings
/// * `$args` - Arguments passed to the `warn!` macro (format string and values)
///
/// # Examples
///
/// Ignored because `warn_rate_limited!` is not public API.
/// ```ignore
/// use std::time::Duration;
///
/// // This will only warn once per second, no matter how many times it's called
/// warn_rate_limited!(Duration::from_secs(1), "This is a rate-limited warning");
///
/// // With format arguments
/// warn_rate_limited!(
///     Duration::from_secs(5),
///     "Connection failed to {}: {}",
///     address,
///     error
/// );
/// ```
///
/// # Implementation Details
///
/// - Uses `std::time::Instant` for monotonic time measurements immune to clock adjustments
/// - Uses `AtomicU64` for lock-free synchronization between threads
/// - Each macro call site gets its own static rate limiting state for independence
/// - Uses relaxed memory ordering for optimal performance
/// - Minimal macro expansion - just static definition + function call
///
/// # Thread Safety
///
/// This macro is fully thread-safe. Multiple threads can call the same rate-limited warning
/// concurrently, and at most one thread will emit the warning per interval.
macro_rules! warn_rate_limited {
    ($interval:expr $(, $args:expr)* $(,)?) => {
        $crate::observability::rate_limited!($interval, || {
            // Use the tracing crate to log the warning.
            tracing::warn!($($args),*);
        });
    };
}

pub(crate) use rate_limited;
pub(crate) use warn_rate_limited;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rate_limit_state_basic() {
        let state = RateLimiter::new();

        // First call should return true
        assert!(state.try_acquire(Duration::from_millis(100)));

        // Immediate second call should return false
        assert!(!state.try_acquire(Duration::from_millis(100)));

        // After waiting, should return true again
        std::thread::sleep(Duration::from_millis(150));
        assert!(state.try_acquire(Duration::from_millis(100)));
    }

    #[test]
    fn test_rate_limit_state_concurrent() {
        let state = Arc::new(RateLimiter::new());
        let success_count = Arc::new(AtomicUsize::new(0));

        let threads: Vec<_> = (0..10)
            .map(|_| {
                let state_clone = state.clone();
                let success_count_clone = success_count.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        if state_clone.try_acquire(Duration::from_millis(10)) {
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
        let state1 = RateLimiter::new();
        let state2 = RateLimiter::new();

        // Both should be able to acquire initially.
        assert!(state1.try_acquire(Duration::from_millis(100)));
        assert!(state2.try_acquire(Duration::from_millis(100)));

        // Both should be rate limited independently.
        assert!(!state1.try_acquire(Duration::from_millis(100)));
        assert!(!state2.try_acquire(Duration::from_millis(100)));
    }

    #[test]
    fn test_zero_interval() {
        let state = RateLimiter::new();

        // With zero interval, every call should succeed
        assert!(state.try_acquire(Duration::ZERO));
        assert!(state.try_acquire(Duration::ZERO));
        assert!(state.try_acquire(Duration::ZERO));
    }

    #[test]
    fn test_very_long_interval() {
        let state = RateLimiter::new();

        // First call succeeds
        assert!(state.try_acquire(Duration::from_secs(3600))); // 1 hour

        // Subsequent calls should fail
        assert!(!state.try_acquire(Duration::from_secs(3600)));
        assert!(!state.try_acquire(Duration::from_secs(3600)));
    }
}
