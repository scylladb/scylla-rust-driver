//! Lets a graceful shutdown of a [`Session`](crate::client::session::Session)
//! stop the background tasks of the session, and wait until they stop.

use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::task::AbortHandle;
use tokio_util::task::{AbortOnDropHandle, TaskTracker};

/// The background tasks of one session.
///
/// A task stops in two ways, and both abort it:
/// - Its owner drops the handle that [`spawn`](Self::spawn) returns. This
///   stops one task, for example the router of a connection that is dropped.
/// - [`stop_all`](Self::stop_all) aborts all tasks, and waits until they are gone.
///
/// The connection config holds a clone, so that pools and connections can
/// spawn their tasks here.
#[derive(Clone, Debug, Default)]
pub(crate) struct BackgroundTasks {
    registry: Arc<Mutex<Registry>>,
    tracker: TaskTracker,
}

#[derive(Debug, Default)]
struct Registry {
    stopped: bool,
    /// Can also hold handles of tasks that ended. `push` removes them.
    handles: Vec<AbortHandle>,
    /// The length of `handles` after `push` last removed the ended tasks.
    len_after_cleanup: usize,
}

impl Registry {
    /// Tasks end all the time, for example when a connection breaks. Without
    /// a cleanup, `handles` would grow with each connection that the session
    /// ever opened.
    fn push(&mut self, handle: AbortHandle) {
        const MIN_CLEANUP_LEN: usize = 64;
        if self.handles.len() >= MIN_CLEANUP_LEN.max(2 * self.len_after_cleanup) {
            self.handles.retain(|handle| !handle.is_finished());
            self.len_after_cleanup = self.handles.len();
        }
        self.handles.push(handle);
    }
}

impl BackgroundTasks {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Spawns `fut` as a task that [`stop_all`](Self::stop_all) aborts, and
    /// waits for. Dropping the returned handle aborts the task.
    ///
    /// An abort drops `fut` at its next poll. If the task must do something
    /// when it stops, do it in the `Drop` of a value that `fut` owns.
    ///
    /// After a stop, this still spawns `fut` but aborts it at once. `fut` can
    /// run until its first `.await`. This is acceptable: only other background
    /// tasks spawn, and `stop_all` waits until all of them are gone.
    pub(crate) fn spawn<F>(&self, fut: F) -> AbortOnDropHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = self.tracker.spawn(fut);
        let mut registry = self.registry.lock().unwrap();
        if registry.stopped {
            handle.abort();
        } else {
            registry.push(handle.abort_handle());
        }
        AbortOnDropHandle::new(handle)
    }

    /// Stops all tasks, including the tasks that are spawned after this call,
    /// and waits until they are gone.
    ///
    /// It is safe to call this more than once, also concurrently.
    pub(crate) async fn stop_all(&self) {
        let handles = {
            let mut registry = self.registry.lock().unwrap();
            registry.stopped = true;
            std::mem::take(&mut registry.handles)
        };
        for handle in handles {
            handle.abort();
        }
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
    async fn stop_all_stops_tasks() {
        let tasks = BackgroundTasks::new();
        let _handle = tasks.spawn(std::future::pending());
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
        let _handle = tasks.spawn(std::future::pending());

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

        let handle = tasks.spawn(std::future::pending());
        let result = tokio::time::timeout(TEST_TIMEOUT, handle).await.unwrap();
        assert!(result.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn handles_of_ended_tasks_are_removed() {
        let tasks = BackgroundTasks::new();
        for _ in 0..1000 {
            tasks.spawn(async {}).await.unwrap();
        }
        assert!(tasks.registry.lock().unwrap().handles.len() <= 64);
    }
}
