use std::{future::Future, time::Duration};

use tokio::sync::watch;

use crate::runner::{RunResult, RunStatus};

#[derive(Clone, Copy, Debug)]
pub struct LoopOptions {
    pub interval: Duration,
    pub max_runs: Option<u64>,
    pub stop_on_error: bool,
    pub verbose: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoopExit {
    Finished,
    Cancelled,
}

pub async fn run_loop<F, Fut>(
    options: LoopOptions,
    mut cancelled: watch::Receiver<bool>,
    mut run: F,
) -> LoopExit
where
    F: FnMut(u64, watch::Receiver<bool>) -> Fut,
    Fut: Future<Output = RunResult>,
{
    let mut run_number = 0;
    loop {
        if *cancelled.borrow() {
            return LoopExit::Cancelled;
        }

        run_number += 1;
        eprintln!("[codex-loop] iteration {run_number}");
        let result = run(run_number, cancelled.clone()).await;

        if result.status == RunStatus::Cancelled {
            return LoopExit::Cancelled;
        }
        report_result(&result);

        if options.stop_on_error && result.status.failed() {
            if options.verbose {
                eprintln!("[codex-loop] stopping after failed iteration");
            }
            return LoopExit::Finished;
        }
        if options
            .max_runs
            .is_some_and(|maximum| run_number >= maximum)
        {
            if options.verbose {
                eprintln!("[codex-loop] reached maximum of {run_number} runs");
            }
            return LoopExit::Finished;
        }

        eprintln!(
            "[codex-loop] next run in {}",
            humantime::format_duration(options.interval)
        );
        tokio::select! {
            biased;
            changed = cancelled.changed() => {
                let _ = changed;
                return LoopExit::Cancelled;
            }
            () = tokio::time::sleep(options.interval) => {}
        }
    }
}

fn report_result(result: &RunResult) {
    let duration = humantime::format_duration(result.duration);
    match result.status {
        RunStatus::Completed => eprintln!("[codex-loop] completed in {duration}"),
        RunStatus::Failed => {
            report_problem("failed", duration.to_string(), result.detail.as_deref())
        }
        RunStatus::TimedOut => {
            report_problem("timed out", duration.to_string(), result.detail.as_deref());
        }
        RunStatus::Cancelled => {}
    }
}

fn report_problem(label: &str, duration: String, detail: Option<&str>) {
    if let Some(detail) = detail {
        eprintln!("[codex-loop] {label} after {duration}: {detail}");
    } else {
        eprintln!("[codex-loop] {label} after {duration}");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;
    use tokio::sync::watch;

    use super::{LoopExit, LoopOptions, run_loop};
    use crate::runner::{RunResult, RunStatus, Runner};

    fn options(max_runs: Option<u64>, stop_on_error: bool) -> LoopOptions {
        LoopOptions {
            interval: Duration::from_millis(1),
            max_runs,
            stop_on_error,
            verbose: false,
        }
    }

    #[tokio::test]
    async fn stops_at_max_runs_without_an_extra_sleep() {
        let (_sender, receiver) = watch::channel(false);
        let calls = Arc::new(Mutex::new(0_u64));
        let calls_for_run = Arc::clone(&calls);

        let exit = run_loop(options(Some(3), false), receiver, move |_, _| {
            let calls = Arc::clone(&calls_for_run);
            async move {
                *calls.lock().unwrap() += 1;
                RunResult::new(RunStatus::Completed, Duration::ZERO)
            }
        })
        .await;

        assert_eq!(exit, LoopExit::Finished);
        assert_eq!(*calls.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn stop_on_error_prevents_another_run() {
        let (_sender, receiver) = watch::channel(false);
        let calls = Arc::new(Mutex::new(0_u64));
        let calls_for_run = Arc::clone(&calls);

        run_loop(options(Some(4), true), receiver, move |iteration, _| {
            let calls = Arc::clone(&calls_for_run);
            async move {
                *calls.lock().unwrap() += 1;
                let status = if iteration == 2 {
                    RunStatus::Failed
                } else {
                    RunStatus::Completed
                };
                RunResult::new(status, Duration::ZERO)
            }
        })
        .await;

        assert_eq!(*calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn failure_continues_by_default() {
        let (_sender, receiver) = watch::channel(false);
        let calls = Arc::new(Mutex::new(0_u64));
        let calls_for_run = Arc::clone(&calls);

        run_loop(options(Some(3), false), receiver, move |_, _| {
            let calls = Arc::clone(&calls_for_run);
            async move {
                *calls.lock().unwrap() += 1;
                RunResult::new(RunStatus::Failed, Duration::ZERO)
            }
        })
        .await;

        assert_eq!(*calls.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn cancellation_interrupts_interval_sleep() {
        let (sender, receiver) = watch::channel(false);
        let calls = Arc::new(Mutex::new(0_u64));
        let calls_for_run = Arc::clone(&calls);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            sender.send(true).unwrap();
        });
        let started = Instant::now();

        let exit = run_loop(
            LoopOptions {
                interval: Duration::from_secs(30),
                max_runs: None,
                stop_on_error: false,
                verbose: false,
            },
            receiver,
            move |_, _| {
                let calls = Arc::clone(&calls_for_run);
                async move {
                    *calls.lock().unwrap() += 1;
                    RunResult::new(RunStatus::Completed, Duration::ZERO)
                }
            },
        )
        .await;

        assert_eq!(exit, LoopExit::Cancelled);
        assert_eq!(*calls.lock().unwrap(), 1);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn fake_child_runs_then_sleeps_then_runs_again() {
        let directory = tempdir().unwrap();
        let executable = directory.path().join("fake-codex");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"turn.completed\"}'\n",
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();

        let runner = Runner::new(executable);
        let starts = Arc::new(Mutex::new(Vec::new()));
        let starts_for_run = Arc::clone(&starts);
        let interval = Duration::from_millis(30);
        let (_sender, receiver) = watch::channel(false);

        run_loop(
            LoopOptions {
                interval,
                max_runs: Some(2),
                stop_on_error: true,
                verbose: false,
            },
            receiver,
            move |_, cancelled| {
                starts_for_run.lock().unwrap().push(Instant::now());
                let runner = runner.clone();
                async move { runner.run("prompt", cancelled).await }
            },
        )
        .await;

        let starts = starts.lock().unwrap();
        assert_eq!(starts.len(), 2);
        assert!(starts[1].duration_since(starts[0]) >= interval);
    }
}
