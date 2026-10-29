//! Utilities for rate limiting actions in a lock-free manner.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;
// Rather than std's Instant, so that tests can pause and advance time.
use tokio::time::Instant;

/// Permits an action at most once per configured interval.
///
/// Lock-free: a single relaxed atomic holds the time of the next permit,
/// so that it is cheap enough to be queried on the hot path.
pub(crate) struct RateLimiter {
    interval: Duration,
    /// Earliest time of the next permit, as returned by `to_nanos()`.
    next_permit_nanos: AtomicI64,
}

impl RateLimiter {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval,
            // Permits the first call, whenever it happens.
            next_permit_nanos: AtomicI64::new(i64::MIN),
        }
    }

    /// Returns `true` if the action may be performed, i.e. if it was never permitted before
    /// or the last permit is at least the interval old.
    ///
    /// Concurrent callers racing for the same permit are resolved by a CAS, so at most
    /// one of them succeeds.
    pub(crate) fn try_acquire(&self) -> bool {
        let now = Self::to_nanos(Instant::now());
        let next_permit = self.next_permit_nanos.load(Ordering::Relaxed);
        now >= next_permit
            && self
                .next_permit_nanos
                .compare_exchange(
                    next_permit,
                    now.saturating_add(Self::saturating_nanos(self.interval)),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
    }

    /// Converts `instant` to nanoseconds since a process-wide epoch: the first converted instant.
    ///
    /// Needed because an `Instant` can't be stored in an atomic. Instants before the epoch
    /// are negative rather than clamped, so the result doesn't depend on when the epoch
    /// was set (in tests, tokio's paused clock can be behind it).
    fn to_nanos(instant: Instant) -> i64 {
        static EPOCH: OnceLock<Instant> = OnceLock::new();
        let epoch = *EPOCH.get_or_init(|| instant);
        match instant.checked_duration_since(epoch) {
            Some(since_epoch) => Self::saturating_nanos(since_epoch),
            // Can't overflow, because the operand is non-negative.
            None => -Self::saturating_nanos(epoch.duration_since(instant)),
        }
    }

    /// Saturation is theoretical: `i64` nanoseconds cover about 292 years.
    fn saturating_nanos(duration: Duration) -> i64 {
        i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_rate_limit_state_basic() {
        let state = RateLimiter::new(Duration::from_millis(100));

        // First call should return true
        assert!(state.try_acquire());

        // Calls within the interval should return false
        assert!(!state.try_acquire());
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(!state.try_acquire());

        // Once the interval has passed, should return true again
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(state.try_acquire());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_rate_limit_state_concurrent() {
        let state = RateLimiter::new(Duration::from_millis(10));
        let success_count = AtomicUsize::new(0);
        let runtime = tokio::runtime::Handle::current();

        thread::scope(|s| {
            for _ in 0..10 {
                s.spawn(|| {
                    // So that the thread sees the paused clock.
                    let _guard = runtime.enter();
                    for _ in 0..100 {
                        if state.try_acquire() {
                            success_count.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });

        // Time is paused, so only the first permit can be granted.
        assert_eq!(success_count.into_inner(), 1);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn test_different_states_independent() {
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
    fn test_to_nanos_before_epoch() {
        let now = Instant::now();
        let later = now + Duration::from_secs(3600);

        // With nextest, this sets the epoch, so that `now` is before it.
        let later_nanos = RateLimiter::to_nanos(later);
        assert_eq!(later_nanos - RateLimiter::to_nanos(now), 3_600_000_000_000);
    }
}
