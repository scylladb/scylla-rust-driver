use anyhow::{Context, Error};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::ops::Deref;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::task;

// A simple abstraction to run commands, log command, exit code its stderr, stdout to a file
//  and optionally to own stderr/stdout
// It should allow to run multiple commands in parallel
pub(crate) struct LoggedCmd {
    file: Arc<Mutex<File>>,
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
    pub(crate) async fn new(file_name: String) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name.as_str())
            .await
            .with_context(|| format!("failed to open ccm log file {}", file_name))?;

        Ok(LoggedCmd {
            file: Arc::new(Mutex::new(file)),
            run_id: AtomicI32::new(1),
        })
    }

    async fn process_child_result(
        status: io::Result<ExitStatus>,
        allow_failure: bool,
        run_id: i32,
        stderr: Vec<u8>,
        command_with_args: String,
        writer: Arc<Mutex<File>>,
    ) -> Result<ExitStatus, Error> {
        match status {
            Ok(status) => {
                match status.code() {
                    Some(code) => {
                        writer
                            .lock()
                            .await
                            .write_all(
                                format!(
                                    "{:15} -> status = {}\n",
                                    format!("exited[{}]", run_id),
                                    code
                                )
                                .as_bytes(),
                            )
                            .await
                            .ok();
                    }
                    None => {
                        writer
                            .lock()
                            .await
                            .write_all(
                                format!(
                                    "{:15} -> status = unknown\n",
                                    format!("exited[{}]", run_id)
                                )
                                .as_bytes(),
                            )
                            .await
                            .ok();
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
                writer
                    .lock()
                    .await
                    .write_all(
                        format!(
                            "{:15} -> failed to wait on child process: = {}\n",
                            format!("exited[{}]", run_id),
                            e
                        )
                        .as_bytes(),
                    )
                    .await
                    .ok();
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
                self.file
                    .lock()
                    .await
                    .write_all(
                        format!("{:15} -> {}={}\n", format!("env[{}]", run_id), key, value)
                            .as_bytes(),
                    )
                    .await
                    .ok();
            }
            cmd.envs(env);
        }

        let mut child = cmd.spawn().with_context(|| {
            format!("failed to spawn child process for command {command_with_args}",)
        })?;
        self.file
            .lock()
            .await
            .write_all(
                format!(
                    "{:15} -> {}\n",
                    format!("started[{}]", run_id),
                    command_with_args,
                )
                .as_bytes(),
            )
            .await
            .ok();

        let stdout_task = Self::stream_reader(
            child.stdout.take().expect("Failed to capture stdout"),
            self.file.clone(),
            format!("{:15} -> ", format!("stdout[{}]", run_id)),
            None,
        );

        let mut stderr: Vec<u8> = Vec::new();
        let stderr_task = Self::stream_reader(
            child.stderr.take().expect("Failed to capture stderr"),
            self.file.clone(),
            format!("{:15} -> ", format!("stderr[{}]", run_id)),
            Some(&mut stderr),
        );

        let (_, _, status) = tokio::join!(stdout_task, stderr_task, child.wait());
        LoggedCmd::process_child_result(
            status,
            allow_failure,
            run_id,
            stderr,
            command_with_args,
            self.file.clone(),
        )
        .await
    }

    async fn stream_reader<T>(
        stream: T,
        writer: Arc<Mutex<File>>,
        prefix: String,
        buffer: Option<&mut Vec<u8>>,
    ) where
        T: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        match buffer {
            Some(buffer) => {
                while let Some(line) = lines.next_line().await.ok().flatten() {
                    let _ = writer
                        .lock()
                        .await
                        .write_all(format!("{} {}\n", prefix, line).as_bytes())
                        .await;
                    buffer.extend_from_slice(line.as_bytes());
                }
            }
            None => {
                while let Some(line) = lines.next_line().await.ok().flatten() {
                    let _ = writer
                        .lock()
                        .await
                        .write_all(format!("{} {}\n", prefix, line).as_bytes())
                        .await;
                }
            }
        }
    }
}

impl Drop for LoggedCmd {
    fn drop(&mut self) {
        let file = self.file.clone();
        task::spawn(async move {
            if let Err(e) = file.lock().await.sync_all().await {
                eprintln!("Failed to sync file: {}", e);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::fs;

    #[tokio::test]
    async fn test_run_command_success() {
        let log_file = "/tmp/test_log_success.txt";
        let _ = fs::remove_file(log_file).await;
        let runner = LoggedCmd::new(log_file.to_string())
            .await
            .expect("Failed to set log file");

        // Run a simple echo command
        runner
            .run_command("echo", &["Test Success"], RunOptions::new())
            .await
            .unwrap();

        drop(runner);

        let log_contents = fs::read_to_string(log_file).await.unwrap();
        assert_eq!(log_contents, "started[1]      -> echo Test Success\nstdout[1]       ->  Test Success\nexited[1]       -> status = 0\n");

        let _ = fs::remove_file(log_file).await;
    }

    #[tokio::test]
    async fn test_run_command_failure() {
        let log_file = "/tmp/test_log_failure.txt";
        let _ = fs::remove_file(log_file).await;
        let runner = LoggedCmd::new(log_file.to_string())
            .await
            .expect("Failed to set log file");

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

        drop(runner);

        let log_contents = fs::read_to_string(log_file).await.unwrap();
        assert_eq!(log_contents, "started[1]      -> ls /nonexistent_path\nstderr[1]       ->  ls: cannot access '/nonexistent_path': No such file or directory\nexited[1]       -> status = 2\n");
        let _ = fs::remove_file(log_file).await;
    }

    #[tokio::test]
    async fn test_run_command_with_env() {
        let log_file = "/tmp/test_log_env.txt";
        let _ = fs::remove_file(log_file).await;
        let runner = LoggedCmd::new(log_file.to_string())
            .await
            .expect("Failed to set log file");

        let mut env_vars: HashMap<String, String> = HashMap::new();
        env_vars.insert("TEST_ENV".to_string(), "12345".to_string());

        runner
            .run_command(
                "printenv",
                &["TEST_ENV"],
                RunOptions::new().with_env(env_vars),
            )
            .await
            .unwrap();

        drop(runner);

        let log_contents = fs::read_to_string(log_file).await.unwrap();
        assert_eq!(log_contents, "env[1]          -> TEST_ENV=12345\nstarted[1]      -> printenv TEST_ENV\nstdout[1]       ->  12345\nexited[1]       -> status = 0\n");
        let _ = fs::remove_file(log_file).await;
    }
}
