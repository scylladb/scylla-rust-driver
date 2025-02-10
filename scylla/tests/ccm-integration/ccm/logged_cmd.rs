use anyhow::{Context, Error};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::ops::Deref;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::AtomicI32;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

// A simple abstraction to run commands, log command, exit code its stderr, stdout to a file
//  and optionally to own stderr/stdout
// It should allow to run multiple commands in parallel
pub(crate) struct LoggedCmd {
    run_id: AtomicI32,
}

/// A set of options for the command to run.
pub(crate) struct RunOptions {
    /// Environment variables for the command.
    env: HashMap<String, String>,
    /// A flag telling whether the command is allowed to fail.
    /// If set to true, and command fails, the error is not propagated.
    allow_failure: bool,
}

impl RunOptions {
    /// The default run options. With empty environment and `allow_failure` set to false.
    pub(crate) fn new() -> Self {
        RunOptions {
            env: HashMap::new(),
            allow_failure: false,
        }
    }

    pub(crate) fn with_env(mut self, env: HashMap<String, String>) -> Self {
        self.env = env;
        self
    }

    pub(crate) fn allow_failure(mut self, allow: bool) -> Self {
        self.allow_failure = allow;
        self
    }
}

impl LoggedCmd {
    pub(crate) async fn new() -> Self {
        LoggedCmd {
            run_id: AtomicI32::new(1),
        }
    }

    async fn process_child_result(
        status: io::Result<ExitStatus>,
        allow_failure: bool,
        run_id: i32,
        stderr: Vec<u8>,
        command_with_args: String,
    ) -> Result<ExitStatus, Error> {
        match status {
            Ok(status) => {
                match status.code() {
                    Some(code) => {
                        tracing::info!("{:15} -> status = {}", format!("exited[{}]", run_id), code);
                    }
                    None => {
                        tracing::info!("{:15} -> status = unknown", format!("exited[{}]", run_id));
                    }
                }
                if !allow_failure && !status.success() {
                    let tmp = stderr.deref();
                    return Err(Error::msg(format!(
                        "Command `{}` failed: {}, stderr: \n{}",
                        command_with_args,
                        status,
                        std::str::from_utf8(tmp)?
                    )));
                }
                Ok(status)
            }
            Err(e) => {
                tracing::info!(
                    "{:15} -> failed to wait on child process: = {}",
                    format!("exited[{}]", run_id),
                    e
                );
                Err(Error::from(e).context(format!("Command `{}` failed", command_with_args,)))
            }
        }
    }

    pub(crate) async fn run_command<A, B, C>(
        &self,
        command: C,
        args: A,
        opts: RunOptions,
    ) -> Result<ExitStatus, Error>
    where
        A: IntoIterator<Item = B> + Clone,
        B: AsRef<OsStr> + Clone,
        C: AsRef<OsStr> + Clone,
    {
        let run_id = self
            .run_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut cmd = Command::new(command.clone());
        cmd.args(args.clone())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let command_with_args = std::iter::once(command.as_ref().to_string_lossy().into_owned())
            .chain(
                args.clone()
                    .into_iter()
                    .map(|s| s.as_ref().to_string_lossy().into_owned()),
            )
            .collect::<Vec<String>>()
            .join(" ");

        let RunOptions { env, allow_failure } = opts;
        if !env.is_empty() {
            for (key, value) in &env {
                tracing::info!("{:15} -> {}={}", format!("env[{}]", run_id), key, value);
            }
            cmd.envs(env);
        }

        let mut child = cmd.spawn().with_context(|| {
            format!("failed to spawn child process for command {command_with_args}",)
        })?;
        tracing::info!(
            "{:15} -> {}",
            format!("started[{}]", run_id),
            command_with_args,
        );

        let stdout_task = Self::stream_reader(
            child.stdout.take().expect("Failed to capture stdout"),
            format!("{:15} -> ", format!("stdout[{}]", run_id)),
            None,
        );

        let mut stderr: Vec<u8> = Vec::new();
        let stderr_task = Self::stream_reader(
            child.stderr.take().expect("Failed to capture stderr"),
            format!("{:15} -> ", format!("stderr[{}]", run_id)),
            Some(&mut stderr),
        );

        let (_, _, status) = tokio::join!(stdout_task, stderr_task, child.wait());
        LoggedCmd::process_child_result(status, allow_failure, run_id, stderr, command_with_args)
            .await
    }

    async fn stream_reader<T>(stream: T, prefix: String, buffer: Option<&mut Vec<u8>>)
    where
        T: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        match buffer {
            Some(buffer) => {
                while let Some(line) = lines.next_line().await.ok().flatten() {
                    tracing::debug!("{} {}", prefix, line);
                    buffer.extend_from_slice(line.as_bytes());
                }
            }
            None => {
                while let Some(line) = lines.next_line().await.ok().flatten() {
                    tracing::debug!("{} {}", prefix, line);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fmt::Write;
    use std::sync::{Arc, Mutex};
    use tracing::field::Field;
    use tracing::subscriber::DefaultGuard;
    use tracing::Subscriber;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::{Layer, Registry};

    /// Collects the log message from an event.
    /// Created for test purposes, to test the logs emitted by LoggedCmd API.
    struct PrintlnVisitor {
        log_message: String,
    }

    impl tracing::field::Visit for PrintlnVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if self.log_message.is_empty() {
                write!(self.log_message, "{:?}", value).unwrap();
            } else {
                write!(self.log_message, ", {}: {:?}", field, value).unwrap();
            }
        }
    }

    struct VecCollector {
        logs: Arc<Mutex<Vec<String>>>,
    }

    impl<S> Layer<S> for VecCollector
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            let mut logs = self.logs.lock().unwrap();

            let mut visitor = PrintlnVisitor {
                log_message: String::new(),
            };
            event.record(&mut visitor);

            logs.push(visitor.log_message);
        }
    }

    impl VecCollector {
        fn new() -> (Self, Arc<Mutex<Vec<String>>>) {
            let logs = Arc::new(Mutex::new(Vec::new()));
            (Self { logs: logs.clone() }, logs)
        }
    }

    fn setup_tracing_collected_logs() -> (Arc<Mutex<Vec<String>>>, DefaultGuard) {
        let (vec_collector, logs) = VecCollector::new();
        let subscriber = Registry::default().with(vec_collector);
        let guard = tracing::subscriber::set_default(subscriber);

        (logs, guard)
    }

    #[tokio::test]
    async fn test_run_command_success() {
        let (logs, _guard) = setup_tracing_collected_logs();
        let runner = LoggedCmd::new().await;

        // Run a simple echo command
        runner
            .run_command("echo", &["Test Success"], RunOptions::new())
            .await
            .unwrap();

        let log_contents = logs.lock().unwrap().join("\n");
        assert_eq!(log_contents, "started[1]      -> echo Test Success\nstdout[1]       ->  Test Success\nexited[1]       -> status = 0");
    }

    #[tokio::test]
    async fn test_run_command_failure() {
        let (logs, _guard) = setup_tracing_collected_logs();
        let runner = LoggedCmd::new().await;

        // Run a command that will fail
        let err = runner
            .run_command("ls", &["/nonexistent_path"], RunOptions::new())
            .await
            .err();

        assert!(err.is_some());
        assert!(err
            .unwrap()
            .to_string()
            .contains("No such file or directory"));

        let log_contents = logs.lock().unwrap().join("\n");
        assert_eq!(log_contents, "started[1]      -> ls /nonexistent_path\nstderr[1]       ->  ls: cannot access '/nonexistent_path': No such file or directory\nexited[1]       -> status = 2");
    }

    #[tokio::test]
    async fn test_run_command_allow_failure() {
        let (logs, _guard) = setup_tracing_collected_logs();
        let runner = LoggedCmd::new().await;

        // Run a command that will fail
        let status = runner
            .run_command(
                "ls",
                &["/nonexistent_path"],
                RunOptions::new().allow_failure(true),
            )
            .await
            .unwrap();

        assert_eq!(status.code(), Some(2));

        let log_contents = logs.lock().unwrap().join("\n");
        assert_eq!(log_contents, "started[1]      -> ls /nonexistent_path\nstderr[1]       ->  ls: cannot access '/nonexistent_path': No such file or directory\nexited[1]       -> status = 2");
    }

    #[tokio::test]
    async fn test_run_command_with_env() {
        let (logs, _guard) = setup_tracing_collected_logs();
        let runner = LoggedCmd::new().await;

        let env_vars: HashMap<String, String> =
            [("TEST_ENV".to_string(), "12345".to_string())].into();

        runner
            .run_command(
                "printenv",
                &["TEST_ENV"],
                RunOptions::new().with_env(env_vars),
            )
            .await
            .unwrap();

        let log_contents = logs.lock().unwrap().join("\n");
        assert_eq!(log_contents, "env[1]          -> TEST_ENV=12345\nstarted[1]      -> printenv TEST_ENV\nstdout[1]       ->  12345\nexited[1]       -> status = 0");
    }
}
