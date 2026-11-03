//! Counts the requests that a [`Session`](crate::client::session::Session)
//! executes, so that a graceful shutdown can wait for them.
//!
//! # Design
//!
//! A [`TaskTracker`] does the counting. Each request holds a
//! [`TaskTrackerToken`]. A graceful shutdown closes the tracker and waits
//! until all tokens are dropped.
//!
//! A closed `TaskTracker` still gives tokens. Thus, [`InFlightRequests::try_enter`]
//! takes a token first, and then rejects the request if the tracker is closed.
//! The token and the close change the same atomic word, so one of them is
//! first:
//! - If the close is first, the check sees it, and the request is rejected.
//! - If the token is first, [`InFlightRequests::wait_idle`] counts it.
//!
//! `try_enter` also checks the close before it takes a token. Thus, after a
//! close, rejected requests do not touch the counter, and they can not keep
//! it above zero.

use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

/// A counter of in-flight requests that can be closed.
///
/// After [`close`](Self::close), [`try_enter`](Self::try_enter) rejects all
/// new requests, and [`wait_idle`](Self::wait_idle) waits until the admitted
/// requests finish.
#[derive(Debug, Default)]
pub(crate) struct InFlightRequests {
    /// Never reopened. A reopen would admit requests after a shutdown.
    tracker: TaskTracker,
}

impl InFlightRequests {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Admits a new request, unless the counter is closed.
    ///
    /// The request is in flight until the returned guard is dropped.
    pub(crate) fn try_enter(&self) -> Option<InFlightGuard> {
        // After a close, do not touch the counter. Otherwise, rejected
        // requests could keep it above zero, and the shutdown would never end.
        if self.tracker.is_closed() {
            return None;
        }
        let token = self.tracker.token();
        // The close can come between the two checks. See the module docs.
        if self.tracker.is_closed() {
            return None;
        }
        Some(InFlightGuard { _token: token })
    }

    /// Rejects all requests that try to enter after this call.
    /// It is safe to call this more than once.
    #[expect(dead_code, reason = "Session::shutdown() will use it")]
    pub(crate) fn close(&self) {
        self.tracker.close();
    }

    /// Waits until the counter is closed and no request is in flight.
    #[expect(dead_code, reason = "Session::shutdown() will use it")]
    pub(crate) async fn wait_idle(&self) {
        self.tracker.wait().await;
    }
}

/// Marks one request as in flight. Dropping it finishes the request.
pub(crate) struct InFlightGuard {
    _token: TaskTrackerToken,
}
