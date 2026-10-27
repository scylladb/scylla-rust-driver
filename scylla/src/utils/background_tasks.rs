//! Lets a graceful shutdown of a [`Session`](crate::client::session::Session)
//! stop the background tasks of the session, and wait until they stop.

use std::future::Future;

use tokio_util::sync::CancellationToken;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

/// The background tasks of one session.
///
/// The tasks that this type spawns stop when [`stop_all`](Self::stop_all)
/// is called, and `stop_all` waits until they are gone.
///
/// The connection config holds a clone, so that pools and connections can
/// spawn their tasks here.
#[derive(Clone, Debug, Default)]
pub(crate) struct BackgroundTasks {
    stop: CancellationToken,
    tracker: TaskTracker,
}

impl BackgroundTasks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Spawns `fut` as a task that stops when [`stop_all`](Self::stop_all)
    /// is called. Dropping the returned handle aborts the task.
    pub(crate) fn spawn_stoppable<F>(&self, fut: F) -> AbortOnDropHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn(self.stoppable(fut))
    }

    /// Spawns `fut` as a task that [`stop_all`](Self::stop_all) waits for.
    /// Dropping the returned handle aborts the task.
    ///
    /// The task must end by itself after `stop_all` is called. Use
    /// [`stoppable`](Self::stoppable) inside it.
    pub(crate) fn spawn<F>(&self, fut: F) -> AbortOnDropHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        AbortOnDropHandle::new(self.tracker.spawn(fut))
    }

    /// Wraps `fut` so that it stops when [`stop_all`](Self::stop_all) is called.
    ///
    /// The returned future completes when `fut` completes, or when `stop_all`
    /// is called, whichever is first. It then drops `fut`.
    pub(crate) fn stoppable<F>(&self, fut: F) -> impl Future<Output = ()> + use<F>
    where
        F: Future<Output = ()>,
    {
        let stop = self.stop.child_token();
        async move {
            // Not `CancellationToken::run_until_cancelled`: it prefers the
            // future when both are ready. Prefer the stop: a task that a
            // closed channel of another stopped task woke must not run its
            // body again.
            tokio::select! {
                biased;
                () = stop.cancelled() => {}
                () = fut => {}
            }
        }
    }

    /// Stops all tasks, including the tasks that are spawned after this call,
    /// and waits until they are gone.
    ///
    /// It is safe to call this more than once, also concurrently.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Session::shutdown() will use it")
    )]
    pub(crate) async fn stop_all(&self) {
        self.stop.cancel();
        self.tracker.close();
        self.tracker.wait().await;
    }

    /// Returns the number of tasks that have not ended yet.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tracker.len()
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::time::Duration;

    use super::BackgroundTasks;

    const TEST_TIMEOUT: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn stop_all_stops_stoppable_tasks() {
        let tasks = BackgroundTasks::new();
        let _handle = tasks.spawn_stoppable(std::future::pending());
        assert_eq!(tasks.len(), 1);

        tokio::time::timeout(TEST_TIMEOUT, tasks.stop_all())
            .await
            .unwrap();
        assert_eq!(tasks.len(), 0);
    }

    #[tokio::test]
    async fn stop_all_waits_for_tasks_that_did_not_run_yet() {
        let tasks = BackgroundTasks::new();
        // On this single-threaded runtime, the task does not run before the
        // test yields.
        let _handle = tasks.spawn_stoppable(std::future::pending());

        let mut stop_all = pin!(tasks.stop_all());
        assert!(futures::poll!(stop_all.as_mut()).is_pending());
        tokio::time::timeout(TEST_TIMEOUT, stop_all).await.unwrap();
        assert_eq!(tasks.len(), 0);
    }

    #[tokio::test]
    async fn task_spawned_after_stop_all_stops_at_once() {
        let tasks = BackgroundTasks::new();
        tokio::time::timeout(TEST_TIMEOUT, tasks.stop_all())
            .await
            .unwrap();

        let handle = tasks.spawn_stoppable(std::future::pending());
        tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .unwrap()
            .unwrap();
    }
}
