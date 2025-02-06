use anyhow::{Context, Error};
use std::collections::HashMap;
use std::ops::Deref;
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::AtomicI32;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex};
use tokio::task;

pub(crate) struct LoggedCmd {
    log_file: String,
    file: Option<Arc<Mutex<File>>>,
    run_id: AtomicI32,
}

#[macro_export]
macro_rules! run_options {
    ($($key:ident = $value:expr),* $(,)?) => {
        Some(RunOptions {
            $($key: $value,)*
            ..Default::default()
        })
    };
}

#[derive(Default, Debug)]
pub(crate) struct RunOptions {
    pub env: HashMap<String, String>,
    pub allow_failure: Option<bool>,
}

impl LoggedCmd {
    pub(crate) fn new() -> Self {
        LoggedCmd {
            log_file: "".to_string(),
            file: None,
            run_id: AtomicI32::new(1),
        }
    }

    pub(crate) async fn set_log_file(&mut self, file_name: String) -> Result<(), Error> {
        self.log_file = file_name.clone();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_file.as_str())
            .await
            .context(format!("failed to open ccm log file {}", self.log_file))?;
        self.file = Some(Arc::new(Mutex::new(file)));
        Ok(())
    }

    pub(crate) async fn run_command(
        &self,
        command: &str,
        args: &[&str],
        opts: Option<RunOptions>,
    ) -> Result<ExitStatus, Error> {
        let run_id = self
            .run_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut cmd = Command::new(command);
        cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());

        let writer = self.file.as_ref().unwrap();
        let mut allow_failure = false;

        if let Some(opts) = opts {
            if let Some(allow) = opts.allow_failure {
                allow_failure = allow;
            }
            if !opts.env.is_empty() {
                cmd.envs(opts.env.clone());
                for (key, value) in opts.env {
                    writer
                        .lock()
                        .await
                        .write_all(
                            format!("{:15} -> {}={}\n", format!("env[{}]", run_id), key, value)
                                .as_bytes(),
                        )
                        .await
                        .ok();
                }
            }
        }

        let mut child = cmd.spawn().context(format!(
            "failed to spawn child process for command {} {}",
            command,
            args.join(" ")
        ))?;
        writer
            .lock()
            .await
            .write_all(
                format!(
                    "{:15} -> {} {}\n",
                    format!("started[{}]", run_id),
                    command,
                    args.join(" ")
                )
                .as_bytes(),
            )
            .await
            .ok();

        let stdout_task = Self::stream_reader(
            child.stdout.take().expect("Failed to capture stdout"),
            self.file.as_ref().unwrap().clone(),
            format!("{:15} -> ", format!("stdout[{}]", run_id)),
            None,
        );

        let mut stderr: Vec<u8> = Vec::new();
        let stderr_task = Self::stream_reader(
            child.stderr.take().expect("Failed to capture stderr"),
            self.file.as_ref().unwrap().clone(),
            format!("{:15} -> ", format!("stderr[{}]", run_id)),
            Some(&mut stderr),
        );

        let (_, _, status) = tokio::join!(stdout_task, stderr_task, child.wait());
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
                        "Command `{} {}` failed: {}, stderr: \n{}",
                        command,
                        args.join(" "),
                        status.to_string(),
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
                Err(Error::from(e).context(format!(
                    "Command `{} {}` failed",
                    command,
                    args.join("")
                )))
            }
        }
    }

    async fn stream_reader<T>(
        stream: T,
        writer: Arc<Mutex<File>>,
        prefix: String,
        mut buffer: Option<&mut Vec<u8>>,
    ) where
        T: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        while let Some(line) = tokio::select! {
            line = lines.next_line() => line.unwrap_or(None),
        } {
            let _ = writer
                .lock()
                .await
                .write_all(format!("{} {}\n", prefix, line).as_bytes())
                .await;
            if let Some(ref mut buffer) = buffer {
                buffer.extend_from_slice(line.as_bytes());
            }
        }
    }
}

impl Drop for LoggedCmd {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            task::spawn(async move {
                if let Err(e) = file.lock().await.sync_all().await {
                    eprintln!("Failed to sync file: {}", e);
                }
            });
        }
    }
}

#[tokio::main]
async fn main() {
    let mut runner = LoggedCmd::new();
    runner
        .set_log_file("command_log.txt".to_string())
        .await
        .expect("Failed to set log file");

    if let Err(e) = runner
        .run_command("ls", &["-l", "/nonexistent_path"], None)
        .await
    {
        eprintln!("Failed to run command: {}", e);
    }

    let mut env_vars: HashMap<String, String> = HashMap::new();
    env_vars.insert("GREETING".to_string(), "Hello".to_string());

    if let Err(e) = runner
        .run_command("printenv", &["GREETING"], run_options!(env = env_vars))
        .await
    {
        eprintln!("Failed to run command: {}", e);
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
        fs::remove_file(log_file).await.ok();
        let mut runner = LoggedCmd::new();

        runner
            .set_log_file(log_file.to_string())
            .await
            .expect("Failed to set log file");

        // Run a simple echo command
        runner
            .run_command("echo", &["Test Success"], None)
            .await
            .unwrap();

        drop(runner);

        let log_contents = fs::read_to_string(log_file).await.unwrap();
        assert_eq!(log_contents, "started[1]      -> echo Test Success\nstdout[1]       ->  Test Success\nexited[1]       -> status = 0\n");

        fs::remove_file(log_file).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_command_failure() {
        let log_file = "/tmp/test_log_failure.txt";
        fs::remove_file(log_file).await.ok();
        let mut runner = LoggedCmd::new();

        runner
            .set_log_file(log_file.to_string())
            .await
            .expect("Failed to set log file");

        // Run a command that will fail
        let err = runner
            .run_command("ls", &["/nonexistent_path"], None)
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
        fs::remove_file(log_file).await.unwrap();
    }

    #[tokio::test]
    async fn test_run_command_with_env() {
        let log_file = "/tmp/test_log_env.txt";
        fs::remove_file(log_file).await.ok();
        let mut runner = LoggedCmd::new();

        runner
            .set_log_file(log_file.to_string())
            .await
            .expect("Failed to set log file");

        let mut env_vars: HashMap<String, String> = HashMap::new();
        env_vars.insert("TEST_ENV".to_string(), "12345".to_string());

        runner
            .run_command("printenv", &["TEST_ENV"], run_options!(env = env_vars))
            .await
            .unwrap();

        drop(runner);

        let log_contents = fs::read_to_string(log_file).await.unwrap();
        assert_eq!(log_contents, "env[1]          -> TEST_ENV=12345\nstarted[1]      -> printenv TEST_ENV\nstdout[1]       ->  12345\nexited[1]       -> status = 0\n");
        fs::remove_file(log_file).await.unwrap();
    }
}
