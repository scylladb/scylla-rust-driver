//! A single-producer single-consumer, capacity-one, merge-on-send channel.
//!
//! This is the primitive over which the metadata worker (producer) talks to the
//! cluster worker (consumer). The consumer can be busy for a long time - it
//! builds a new `ClusterState` and awaits connection pools - while the producer
//! keeps generating updates: freshly fetched metadata, partial client-routes
//! snapshots, node UP/DOWN hints, replies owed to explicit refresh requests.
//!
//! Queueing all of those would make the consumer apply stale intermediate
//! states one after another; dropping them would lose information. Instead,
//! this channel holds *at most one* in-flight value and hands the producer
//! mutable access to it, so that a new update can be **merged** into the
//! pending one ([`Sender::modify`]).
//!
//! The single-producer/single-consumer discipline is enforced statically: both
//! [`Sender::modify`] and [`Receiver::recv`] take `&mut self`, and neither
//! endpoint is `Clone`.
//!
//! # Implementation notes
//!
//! - The slot is guarded by a [`std::sync::Mutex`], not a `tokio` one: the
//!   critical section is purely synchronous (it never awaits). Lock poisoning
//!   is handled the same way as elsewhere in this crate - with `unwrap()`. The
//!   only code inside the critical section that can panic is the
//!   caller-provided merge closure; the producer of this channel is a driver
//!   worker task, so a panicking closure means the worker is already dead and
//!   the session is unusable. Propagating the poison as a panic to the consumer
//!   is then strictly better than silently continuing with a possibly
//!   half-merged update.
//! - Wakeups go through [`tokio::sync::Notify`].
//! - Endpoint liveness is tracked with explicit `AtomicBool` flags rather than
//!   `Arc::strong_count`. `Arc`'s refcount is only decremented *after* our
//!   `Drop` impl returns, so a receiver woken from `Drop for Sender` would
//!   still observe a strong count of 2 and park again, losing the wakeup
//!   forever. An explicit flag set *before* notifying has no such race.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;
use tokio::sync::Notify;

/// State shared by the two endpoints of the channel.
struct Shared<T> {
    /// The single in-flight value, if any.
    slot: Mutex<Option<T>>,
    /// Signals "the slot may have changed" or "the sender was dropped".
    notify: Notify,
    /// Set by `Drop for Sender`, before notifying the receiver.
    sender_dropped: AtomicBool,
    /// Set by `Drop for Receiver`.
    receiver_dropped: AtomicBool,
}

/// The producing endpoint of a [`merge_channel`].
pub(crate) struct Sender<T> {
    shared: Arc<Shared<T>>,
}

/// The consuming endpoint of a [`merge_channel`].
pub(crate) struct Receiver<T> {
    shared: Arc<Shared<T>>,
}

/// Returned by [`Sender::modify`] when the [`Receiver`] has been dropped,
/// so that no update can ever be observed again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the receiving end of the channel has been dropped")]
pub(crate) struct SendError;

/// Creates a capacity-one, merge-on-send channel.
pub(crate) fn merge_channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        slot: Mutex::new(None),
        notify: Notify::new(),
        sender_dropped: AtomicBool::new(false),
        receiver_dropped: AtomicBool::new(false),
    });
    (
        Sender {
            shared: Arc::clone(&shared),
        },
        Receiver { shared },
    )
}

impl<T> Sender<T> {
    /// Merges an update into the pending value.
    ///
    /// `f` is applied to the currently pending value, which is `None` if the
    /// receiver already took the previous one (or if nothing was sent yet).
    ///
    /// If, after `f` returns, the slot holds a value, the receiver is notified.
    /// If `f` leaves the slot as `None` - i.e. it decided there is nothing to
    /// send, or it cancelled a previously pending update - no notification is
    /// emitted: there would be nothing for the receiver to take, and a spurious
    /// wakeup would only make it re-check the slot and park again.
    ///
    /// Returns [`SendError`] if the receiver has been dropped. In that case `f`
    /// is not applied at all.
    pub(crate) fn modify<F>(&mut self, f: F) -> Result<(), SendError>
    where
        F: FnOnce(&mut Option<T>),
    {
        if self.shared.receiver_dropped.load(Ordering::Acquire) {
            return Err(SendError);
        }

        let has_value = {
            let mut slot = self.shared.slot.lock().unwrap();
            f(&mut slot);
            slot.is_some()
        };

        if has_value {
            self.shared.notify.notify_one();
        }
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // The flag must be set before notifying, so that a receiver woken by
        // this notification is guaranteed to observe it.
        self.shared.sender_dropped.store(true, Ordering::Release);
        self.shared.notify.notify_one();
    }
}

impl<T> Receiver<T> {
    /// Takes the pending value without waiting.
    #[cfg_attr(not(test), expect(dead_code,))]
    pub(crate) fn try_recv(&mut self) -> Option<T> {
        self.shared.slot.lock().unwrap().take()
    }

    /// Takes the pending value, awaiting until one is available.
    ///
    /// Returns `None` once the sender has been dropped *and* the slot is empty,
    /// so an update produced right before the sender was dropped is still
    /// delivered.
    ///
    /// This method is cancel-safe: the value is only removed from the slot when
    /// it is actually returned, and the `Notified` future is enabled (i.e.
    /// registered in the wait list) before the slot is inspected, so no wakeup
    /// can be lost by dropping the returned future.
    pub(crate) async fn recv(&mut self) -> Option<T> {
        // Borrow the shared state through a local `Arc` reference so that the
        // `Notified` future (which borrows `notify`) does not conflict with the
        // `&mut self` receiver.
        let shared: &Shared<T> = &self.shared;
        let take = || shared.slot.lock().unwrap().take();

        loop {
            let mut notified = std::pin::pin!(shared.notify.notified());
            // Register in the wait list *before* looking at the slot, so that a
            // concurrent `modify` either is seen below or wakes us up.
            notified.as_mut().enable();

            if let Some(value) = take() {
                return Some(value);
            }

            if shared.sender_dropped.load(Ordering::Acquire) {
                // The sender fills the slot before setting the flag, but we
                // read the slot before the flag - so re-check it once more to
                // avoid losing that last update.
                return take();
            }

            notified.as_mut().await;
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared.receiver_dropped.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::{SendError, merge_channel};

    #[tokio::test]
    async fn merges_updates_sent_before_recv() {
        let (mut tx, mut rx) = merge_channel::<Vec<u32>>();

        tx.modify(|slot| slot.get_or_insert_default().push(1))
            .unwrap();
        tx.modify(|slot| {
            // The second closure must see what the first one left behind.
            assert_eq!(slot.as_deref(), Some(&[1][..]));
            slot.get_or_insert_default().push(2);
        })
        .unwrap();

        assert_eq!(rx.recv().await, Some(vec![1, 2]));
        assert_eq!(rx.try_recv(), None);
    }

    #[tokio::test]
    async fn recv_wakes_on_later_modify() {
        let (mut tx, mut rx) = merge_channel::<u32>();

        let receiving = tokio::task::spawn(async move { rx.recv().await });

        // Let the spawned task reach the await point. Even if it does not, the
        // test still passes - the value simply is already pending.
        tokio::task::yield_now().await;

        tx.modify(|slot| *slot = Some(42)).unwrap();
        assert_eq!(receiving.await.unwrap(), Some(42));
    }

    #[tokio::test]
    async fn try_recv_is_non_blocking() {
        let (mut tx, mut rx) = merge_channel::<u32>();

        assert_eq!(rx.try_recv(), None);
        tx.modify(|slot| *slot = Some(7)).unwrap();
        assert_eq!(rx.try_recv(), Some(7));
        assert_eq!(rx.try_recv(), None);
    }

    #[tokio::test]
    async fn modify_fails_after_receiver_dropped() {
        let (mut tx, rx) = merge_channel::<u32>();
        drop(rx);

        assert_eq!(tx.modify(|slot| *slot = Some(1)), Err(SendError));
    }

    #[tokio::test]
    async fn pending_value_survives_sender_drop() {
        let (mut tx, mut rx) = merge_channel::<u32>();

        tx.modify(|slot| *slot = Some(5)).unwrap();
        drop(tx);

        assert_eq!(rx.recv().await, Some(5));
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn parked_recv_wakes_on_sender_drop() {
        let (tx, mut rx) = merge_channel::<u32>();

        let receiving = tokio::task::spawn(async move { rx.recv().await });
        tokio::task::yield_now().await;

        drop(tx);
        assert_eq!(receiving.await.unwrap(), None);
    }

    #[tokio::test]
    async fn cancelled_recv_does_not_lose_value() {
        let (mut tx, mut rx) = merge_channel::<u32>();

        // Poll `recv` once so that it actually parks: it registers in the
        // wait list and enables its `Notified` future.
        {
            let mut receiving = std::pin::pin!(rx.recv());
            assert!(futures::poll!(receiving.as_mut()).is_pending());

            // The parked waiter consumes the notification emitted here...
            tx.modify(|slot| *slot = Some(9)).unwrap();

            // ...and then the future is cancelled without ever being polled
            // again, so the value is still in the slot.
        }

        assert_eq!(rx.recv().await, Some(9));
    }

    #[tokio::test]
    async fn modify_may_decide_not_to_send() {
        let (mut tx, mut rx) = merge_channel::<u32>();

        tx.modify(|slot| *slot = None).unwrap();
        assert_eq!(rx.try_recv(), None);

        // A pending update may also be retracted before it is observed.
        tx.modify(|slot| *slot = Some(1)).unwrap();
        tx.modify(|slot| *slot = None).unwrap();
        assert_eq!(rx.try_recv(), None);
    }
}
