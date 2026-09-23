use std::{
    path::PathBuf,
    process::{ExitStatus, Stdio},
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    process::{Child, Command},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};

const TERMINATION_GRACE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}

impl RunStatus {
    pub fn failed(self) -> bool {
        matches!(self, Self::Failed | Self::TimedOut)
    }
}

#[derive(Debug)]
pub struct RunResult {
    pub status: RunStatus,
    pub duration: Duration,
    pub detail: Option<String>,
}

impl RunResult {
    pub fn new(status: RunStatus, duration: Duration) -> Self {
        Self {
            status,
            duration,
            detail: None,
        }
    }

    fn failed(started: Instant, detail: impl Into<String>) -> Self {
        Self {
            status: RunStatus::Failed,
            duration: started.elapsed(),
            detail: Some(detail.into()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Runner {
    executable: PathBuf,
    cwd: Option<PathBuf>,
    model: Option<String>,
    timeout: Option<Duration>,
    verbose: bool,
}

impl Runner {
    pub fn new(executable: impl Into<PathBuf>) -> Self {
        Self {
            executable: executable.into(),
            cwd: None,
            model: None,
            timeout: None,
            verbose: false,
        }
    }

    pub fn cwd(mut self, cwd: Option<PathBuf>) -> Self {
        self.cwd = cwd;
        self
    }

    pub fn model(mut self, model: Option<String>) -> Self {
        self.model = model;
        self
    }

    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn verbose(mut self, verbose: bool) -> Self {
        self.verbose = verbose;
        self
    }

    pub async fn run(&self, prompt: &str, mut cancelled: watch::Receiver<bool>) -> RunResult {
        let started = Instant::now();
        if *cancelled.borrow() {
            return RunResult::new(RunStatus::Cancelled, started.elapsed());
        }

        let mut command = self.command(prompt);
        if self.verbose {
            eprintln!(
                "[codex-loop] starting {} exec --json",
                self.executable.display()
            );
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                return RunResult::failed(
                    started,
                    format!("could not start {}: {error}", self.executable.display()),
                );
            }
        };

        let stdout_task = child.stdout.take().map(|stdout| {
            let verbose = self.verbose;
            tokio::spawn(async move { consume_stdout(BufReader::new(stdout), verbose).await })
        });
        let stderr_task = child
            .stderr
            .take()
            .map(|stderr| tokio::spawn(consume_stderr(BufReader::new(stderr))));

        let wait_result = if let Some(limit) = self.timeout {
            tokio::select! {
                biased;
                changed = cancelled.changed() => {
                    let _ = changed;
                    terminate_child(&mut child).await;
                    WaitResult::Cancelled
                }
                result = timeout(limit, child.wait()) => match result {
                    Ok(status) => WaitResult::Exited(status),
                    Err(_) => {
                        terminate_child(&mut child).await;
                        WaitResult::TimedOut
                    }
                }
            }
        } else {
            tokio::select! {
                biased;
                changed = cancelled.changed() => {
                    let _ = changed;
                    terminate_child(&mut child).await;
                    WaitResult::Cancelled
                }
                status = child.wait() => WaitResult::Exited(status)
            }
        };

        let stream = finish_output(stdout_task).await;
        finish_stderr(stderr_task).await;

        match wait_result {
            WaitResult::Cancelled => RunResult::new(RunStatus::Cancelled, started.elapsed()),
            WaitResult::TimedOut => RunResult {
                status: RunStatus::TimedOut,
                duration: started.elapsed(),
                detail: self
                    .timeout
                    .map(|limit| format!("exceeded {}", humantime::format_duration(limit))),
            },
            WaitResult::Exited(Err(error)) => {
                RunResult::failed(started, format!("could not wait for Codex: {error}"))
            }
            WaitResult::Exited(Ok(status)) => classify_exit(status, stream, started.elapsed()),
        }
    }

    fn command(&self, prompt: &str) -> Command {
        let mut command = Command::new(&self.executable);
        command.arg("exec").arg("--json");
        if let Some(cwd) = &self.cwd {
            command.arg("-C").arg(cwd);
        }
        if let Some(model) = &self.model {
            command.arg("--model").arg(model);
        }
        command
            .arg(prompt)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        #[cfg(unix)]
        command.process_group(0);

        command
    }
}

impl Default for Runner {
    fn default() -> Self {
        Self::new("codex")
    }
}

enum WaitResult {
    Exited(std::io::Result<ExitStatus>),
    TimedOut,
    Cancelled,
}

#[derive(Default)]
struct StreamResult {
    completed: bool,
    failure: Option<String>,
}

fn classify_exit(status: ExitStatus, stream: StreamResult, duration: Duration) -> RunResult {
    if !status.success() {
        return RunResult {
            status: RunStatus::Failed,
            duration,
            detail: Some(match status.code() {
                Some(code) => format!("Codex exited with status {code}"),
                None => "Codex was terminated by a signal".to_owned(),
            }),
        };
    }
    if let Some(failure) = stream.failure {
        return RunResult {
            status: RunStatus::Failed,
            duration,
            detail: Some(failure),
        };
    }
    if stream.completed {
        RunResult::new(RunStatus::Completed, duration)
    } else {
        RunResult {
            status: RunStatus::Failed,
            duration,
            detail: Some("Codex exited without a terminal completion event".to_owned()),
        }
    }
}

async fn consume_stdout<R>(reader: R, verbose: bool) -> StreamResult
where
    R: AsyncBufRead + Unpin,
{
    let mut result = StreamResult::default();
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        match serde_json::from_str::<Value>(&line) {
            Ok(event) => handle_event(&event, &mut result, verbose),
            Err(error) => {
                println!("{line}");
                if verbose {
                    eprintln!("[codex-loop] ignored malformed JSON event: {error}");
                }
            }
        }
    }
    result
}

fn handle_event(event: &Value, result: &mut StreamResult, verbose: bool) {
    let kind = event.get("type").and_then(Value::as_str).unwrap_or("");
    match kind {
        "turn.completed" => result.completed = true,
        "turn.failed" | "error" => {
            let message = event_message(event).unwrap_or_else(|| kind.to_owned());
            eprintln!("[codex] error: {message}");
            result.failure = Some(message);
        }
        "item.started" | "item.completed" => {
            if let Some(item) = event.get("item") {
                handle_item(kind, item, verbose);
            }
        }
        _ if verbose => eprintln!("[codex-loop] Codex event: {kind}"),
        _ => {}
    }
}

fn event_message(event: &Value) -> Option<String> {
    event
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| {
            event
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
        .map(ToOwned::to_owned)
}

fn handle_item(event_kind: &str, item: &Value, verbose: bool) {
    let item_kind = item.get("type").and_then(Value::as_str).unwrap_or("");
    match (event_kind, item_kind) {
        ("item.completed", "agent_message") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                println!("{text}");
            }
        }
        ("item.completed", "reasoning") => {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                eprintln!("[codex] {text}");
            }
        }
        ("item.started", "command_execution") => {
            if let Some(command) = item.get("command").and_then(Value::as_str) {
                eprintln!("[codex] $ {command}");
            }
        }
        ("item.completed", "command_execution") => {
            if let Some(output) = item.get("aggregated_output").and_then(Value::as_str)
                && !output.is_empty()
            {
                print!("{output}");
                if !output.ends_with('\n') {
                    println!();
                }
            }
        }
        (_, item_kind) if verbose => {
            eprintln!("[codex-loop] Codex item: {item_kind}");
        }
        _ => {}
    }
}

async fn consume_stderr<R>(reader: R)
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        eprintln!("[codex] {line}");
    }
}

async fn finish_output(task: Option<JoinHandle<StreamResult>>) -> StreamResult {
    match task {
        Some(task) => task.await.unwrap_or_default(),
        None => StreamResult::default(),
    }
}

async fn finish_stderr(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        let _ = task.await;
    }
}

async fn terminate_child(child: &mut Child) {
    send_signal(child, libc::SIGTERM);
    if timeout(TERMINATION_GRACE, child.wait()).await.is_ok() {
        return;
    }

    send_signal(child, libc::SIGKILL);
    let _ = child.start_kill();
    let _ = child.wait().await;
}

#[cfg(unix)]
fn send_signal(child: &Child, signal: libc::c_int) {
    if let Some(id) = child.id()
        && let Ok(process_group) = i32::try_from(id)
    {
        // The child is its own process-group leader, so a negative PID reaches it and its children.
        unsafe {
            libc::kill(-process_group, signal);
        }
    }
}

#[cfg(not(unix))]
fn send_signal(child: &Child, _signal: libc::c_int) {
    let _ = child.start_kill();
}

#[cfg(all(test, unix))]
mod tests {
    use std::{os::unix::fs::PermissionsExt, time::Duration};

    use tempfile::tempdir;
    use tokio::sync::watch;

    use super::{RunStatus, Runner};

    fn fake_script(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("fake-codex");
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
        (directory, path)
    }

    #[tokio::test]
    async fn times_out_a_child_process() {
        let (_directory, executable) = fake_script("sleep 30");
        let runner = Runner::new(executable).timeout(Some(Duration::from_millis(30)));
        let (_cancel, receiver) = watch::channel(false);

        let result = runner.run("prompt", receiver).await;

        assert_eq!(result.status, RunStatus::TimedOut);
        assert!(result.duration < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn terminal_failure_event_overrides_a_zero_exit_status() {
        let (_directory, executable) = fake_script(
            r#"printf '%s\n' '{"type":"turn.failed","error":{"message":"agent failed"}}'"#,
        );
        let runner = Runner::new(executable);
        let (_cancel, receiver) = watch::channel(false);

        let result = runner.run("prompt", receiver).await;

        assert_eq!(result.status, RunStatus::Failed);
        assert_eq!(result.detail.as_deref(), Some("agent failed"));
    }

    #[tokio::test]
    async fn cancellation_terminates_a_running_child() {
        let (_directory, executable) = fake_script("sleep 30");
        let runner = Runner::new(executable);
        let (cancel, receiver) = watch::channel(false);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel.send(true).unwrap();
        });

        let result = runner.run("prompt", receiver).await;

        assert_eq!(result.status, RunStatus::Cancelled);
        assert!(result.duration < Duration::from_secs(3));
    }
}
