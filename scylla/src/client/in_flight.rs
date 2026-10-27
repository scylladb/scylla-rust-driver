//! Counts the requests that a [`Session`](crate::client::session::Session)
//! executes, so that a graceful shutdown can wait for them.
//!
//! # Design
//!
//! Requests start and finish on many threads at the same time. A single
//! atomic counter would make all of them write to one cache line. Thus, the
//! counter is split into shards, each in its own cache line:
//! - A request increments the shard of the thread that it starts on.
//! - The request decrements the same shard when it finishes, even if its task
//!   moved to another thread meanwhile. Thus, each shard is a true count of the
//!   requests that started on it, and it never goes below zero.
//! - Only the shutdown reads all shards.
//!
//! Each shard also holds the `CLOSED` bit. Shutdown sets this bit in every
//! shard. The count and the bit share one atomic word. Thus, an admission and
//! a close on one shard are always ordered: either the request is counted
//! before the close, or the request sees the bit and is rejected.
//!
//! When a decrement makes a closed shard reach zero, it wakes the shutdown.
//! The shutdown then reads all shards again. Only these decrements touch
//! the shared [`Notify`], so it is not contended while the session runs.

use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

/// Set in every shard after [`InFlightRequests::close`].
/// The other bits hold the number of requests.
const CLOSED: usize = 1 << (usize::BITS - 1);

/// One shard of the counter.
///
/// The alignment puts each shard in its own cache line. 128 bytes cover
/// the adjacent-line prefetcher of x86 CPUs and the 128-byte cache lines
/// of some ARM CPUs.
#[repr(align(128))]
struct Shard(AtomicUsize);

/// A sharded counter of in-flight requests that can be closed.
///
/// After [`close`](Self::close), [`try_enter`](Self::try_enter) rejects all
/// new requests, and [`wait_idle`](Self::wait_idle) waits until the admitted
/// requests finish.
pub(crate) struct InFlightRequests {
    /// The number of shards is a power of two.
    shards: Box<[Shard]>,
    /// Woken when a closed shard reaches zero.
    idle: Notify,
}

impl InFlightRequests {
    pub(crate) fn new() -> Self {
        // Twice the number of CPUs makes collisions less probable. Collisions
        // happen when threads other than the runtime workers send requests.
        let parallelism = std::thread::available_parallelism().map_or(1, NonZero::get);
        Self::with_shard_count((parallelism * 2).next_power_of_two())
    }

    fn with_shard_count(shard_count: usize) -> Self {
        debug_assert!(shard_count.is_power_of_two());
        Self {
            shards: (0..shard_count)
                .map(|_| Shard(AtomicUsize::new(0)))
                .collect(),
            idle: Notify::new(),
        }
    }

    /// Admits a new request, unless the counter is closed.
    ///
    /// The request is in flight until the returned guard is dropped.
    pub(crate) fn try_enter(&self) -> Option<InFlightGuard<'_>> {
        let shard_idx = current_thread_index() & (self.shards.len() - 1);
        let shard = &self.shards[shard_idx].0;
        // Only a successful update admits the request, so a rejection
        // needs no ordering.
        // `fetch_update` is deprecated in favor of `try_update`, which is not
        // stable in our MSRV.
        // TODO: use `try_update` and remove this allow() once MSRV is 1.97+.
        #[allow(deprecated)]
        let admission = shard.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |value| {
            (value & CLOSED == 0).then_some(value + 1)
        });
        admission.ok()?;
        Some(InFlightGuard {
            requests: self,
            shard_idx,
        })
    }

    /// Rejects all requests that try to enter after this call.
    /// It is safe to call this more than once.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Session::shutdown() will use it")
    )]
    pub(crate) fn close(&self) {
        for shard in &self.shards {
            shard.0.fetch_or(CLOSED, Ordering::AcqRel);
        }
    }

    /// Waits until no request is in flight.
    ///
    /// Call [`close`](Self::close) first. On an open counter, this future
    /// does not wake when the count reaches zero.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Session::shutdown() will use it")
    )]
    pub(crate) async fn wait_idle(&self) {
        debug_assert!(self.is_closed());
        loop {
            // Create the future before the shards are read. A `Notified`
            // receives the wakeups of `notify_waiters()` from its creation.
            // Thus, either the read sees the last decrement, or the last
            // decrement wakes this future.
            let notified = self.idle.notified();
            if self.count() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Returns the number of requests in flight.
    pub(crate) fn count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.0.load(Ordering::Acquire) & !CLOSED)
            .sum()
    }

    fn is_closed(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.0.load(Ordering::Acquire) & CLOSED != 0)
    }
}

impl std::fmt::Debug for InFlightRequests {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InFlightRequests")
            .field("count", &self.count())
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Marks one request as in flight. Dropping it finishes the request.
pub(crate) struct InFlightGuard<'a> {
    requests: &'a InFlightRequests,
    /// The shard that was incremented. The guard can be dropped on another
    /// thread, so it decrements this shard and not the shard of that thread.
    shard_idx: usize,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        let previous = self.requests.shards[self.shard_idx]
            .0
            .fetch_sub(1, Ordering::AcqRel);
        if previous == CLOSED | 1 {
            // This shard is closed and has no more requests.
            self.requests.idle.notify_waiters();
        }
    }
}

/// Returns a number that is unique to the current thread.
///
/// Threads get consecutive numbers, in the order of their first request.
fn current_thread_index() -> usize {
    static NEXT_THREAD_INDEX: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static THREAD_INDEX: usize = NEXT_THREAD_INDEX.fetch_add(1, Ordering::Relaxed);
    }
    // The access fails only while the thread-local storage is destroyed.
    // Any shard is correct, so use the first one.
    THREAD_INDEX.try_with(|index| *index).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::InFlightRequests;

    #[test]
    fn enter_and_exit_update_the_count() {
        let requests = InFlightRequests::with_shard_count(4);
        assert_eq!(requests.count(), 0);

        let first = requests.try_enter().unwrap();
        let second = requests.try_enter().unwrap();
        assert_eq!(requests.count(), 2);

        drop(first);
        assert_eq!(requests.count(), 1);
        drop(second);
        assert_eq!(requests.count(), 0);
    }

    #[test]
    fn close_rejects_new_requests() {
        let requests = InFlightRequests::with_shard_count(4);
        let admitted = requests.try_enter().unwrap();

        requests.close();
        assert!(requests.try_enter().is_none());
        assert_eq!(requests.count(), 1);

        // A second close changes nothing.
        requests.close();
        assert!(requests.try_enter().is_none());
        assert_eq!(requests.count(), 1);

        drop(admitted);
        assert_eq!(requests.count(), 0);
    }

    #[test]
    fn guard_dropped_on_another_thread_decrements_its_own_shard() {
        let requests = InFlightRequests::with_shard_count(4);
        std::thread::scope(|s| {
            let guard = requests.try_enter().unwrap();
            s.spawn(move || drop(guard));
        });
        assert_eq!(requests.count(), 0);
        for shard in &requests.shards {
            assert_eq!(shard.0.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn wait_idle_returns_at_once_when_idle() {
        let requests = InFlightRequests::with_shard_count(4);
        drop(requests.try_enter().unwrap());
        requests.close();
        assert!(futures::poll!(pin!(requests.wait_idle())).is_ready());
    }

    #[tokio::test]
    async fn wait_idle_wakes_when_the_last_request_finishes() {
        let requests = Arc::new(InFlightRequests::with_shard_count(4));
        let first = requests.try_enter().unwrap();
        let second = requests.try_enter().unwrap();
        requests.close();

        let waiter = tokio::spawn({
            let requests = Arc::clone(&requests);
            async move { requests.wait_idle().await }
        });

        drop(first);
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        // If this drop did not wake the waiter, the timeout would expire.
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_and_wait_idle_under_load() {
        let requests = Arc::new(InFlightRequests::new());
        let close_returned = Arc::new(AtomicBool::new(false));

        let tasks: Vec<_> = (0..16)
            .map(|_| {
                let requests = Arc::clone(&requests);
                let close_returned = Arc::clone(&close_returned);
                tokio::spawn(async move {
                    loop {
                        // Read the flag before the admission. If close() had
                        // returned by then, the admission must fail.
                        let was_closed = close_returned.load(Ordering::SeqCst);
                        let Some(guard) = requests.try_enter() else {
                            return;
                        };
                        assert!(!was_closed, "a request entered after close()");
                        // Lets the task move to another worker thread.
                        tokio::task::yield_now().await;
                        drop(guard);
                    }
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(20)).await;
        requests.close();
        close_returned.store(true, Ordering::SeqCst);

        tokio::time::timeout(Duration::from_secs(1), requests.wait_idle())
            .await
            .unwrap();
        assert_eq!(requests.count(), 0);

        for task in tasks {
            task.await.unwrap();
        }
    }
}
