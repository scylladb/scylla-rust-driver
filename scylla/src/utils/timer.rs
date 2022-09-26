use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use tokio::time::{Instant, Sleep};

/// A resettable timer.
///
/// It can be in one of two states: armed or unarmed.
/// Waiting for an armed timer resolves when the timer's deadline is reached.
/// Waiting for an unarmed timer resolves after the timer is reset and its new
/// deadline is reached.
pub struct ArmableTimer {
    state: ArmableTimerState,
}

enum ArmableTimerState {
    /// The timer is not set, but somebody may be waiting for it.
    Unarmed(Option<Waker>),

    /// The timer is set.
    /// The pin box wrapper could be avoided it we used pin_project to generate
    /// pin projections, but it's a heavy dependency and our current use cases
    /// of the ArmableTimer don't really require pulling it in.
    Armed(Pin<Box<Sleep>>),
}

impl Default for ArmableTimer {
    fn default() -> Self {
        Self {
            state: ArmableTimerState::Unarmed(None),
        }
    }
}

impl ArmableTimer {
    /// Creates a new, unarmed timer.
    pub fn new() -> Self {
        Default::default()
    }

    /// Arms the timer to fire at the specified moment.
    ///
    /// If the moment has already expired, the timer will become ready at once.
    /// If somebody waited for the timer, they will start waiting for the new
    /// deadline.
    pub fn arm(&mut self, deadline: Instant) {
        match &mut self.state {
            ArmableTimerState::Unarmed(maybe_waker) => {
                if let Some(waker) = maybe_waker.take() {
                    // We can't really re-register the waker on the new Sleep
                    // future, so wake it and let it be re-registered naturally.
                    waker.wake();
                }
                self.state = ArmableTimerState::Armed(Box::pin(tokio::time::sleep_until(deadline)));
            }
            ArmableTimerState::Armed(sleep) => sleep.as_mut().reset(deadline),
        }
    }

    /// Gets the current deadline of the driver, or returns `None`
    /// if it is unarmed.
    pub fn deadline(&self) -> Option<Instant> {
        match &self.state {
            ArmableTimerState::Unarmed(_) => None,
            ArmableTimerState::Armed(sleep) => Some(sleep.deadline()),
        }
    }
}

impl Future for ArmableTimer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            ArmableTimerState::Unarmed(maybe_waker) => {
                *maybe_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            ArmableTimerState::Armed(sleep) => match sleep.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(_) => {
                    self.state = ArmableTimerState::Unarmed(None);
                    Poll::Ready(())
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::Poll;
    use std::time::Duration;

    use futures::future;
    use tokio::time::Instant;

    use super::ArmableTimer;

    fn make_timer_with_awaitee() -> (Arc<Mutex<ArmableTimer>>, impl Future<Output = ()>) {
        let timer = Arc::new(Mutex::new(ArmableTimer::new()));
        let awaitee = future::poll_fn({
            let timer = timer.clone();
            move |cx| {
                let mut lock = timer.lock().unwrap();
                Pin::new(&mut *lock).poll(cx)
            }
        });
        (timer, awaitee)
    }

    #[tokio::test]
    async fn test_armable_timer() {
        let (timer, mut awaitee) = make_timer_with_awaitee();

        // Check that the timer is usable multiple times
        for _ in 0..3 {
            // Initially/after firing, the timer has no deadline
            // and does not fire
            assert_eq!(timer.lock().unwrap().deadline(), None);
            assert_eq!(futures::poll(&mut awaitee).await, Poll::Pending);

            // Set the deadline and make sure that the timer expires
            let deadline = Instant::now() + Duration::from_millis(50);
            timer.lock().unwrap().arm(deadline);
            assert_eq!(timer.lock().unwrap().deadline(), Some(deadline));

            // Wait until the timer resolves and make sure that the necessary
            // time has elapsed
            (&mut awaitee).await;
            assert!(Instant::now() >= deadline);
        }
    }
}
