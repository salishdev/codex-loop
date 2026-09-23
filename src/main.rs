use clap::Parser;
use codex_loop::{
    cli::Cli,
    r#loop::{LoopExit, LoopOptions, run_loop},
    runner::Runner,
};
use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let runner = Runner::default()
        .cwd(cli.cwd)
        .model(cli.model)
        .timeout(cli.timeout)
        .verbose(cli.verbose);

    let options = LoopOptions {
        interval: cli.interval,
        max_runs: cli.max_runs,
        stop_on_error: cli.stop_on_error,
        verbose: cli.verbose,
    };
    let prompt = cli.prompt;

    let (cancel_sender, cancel_receiver) = watch::channel(false);
    let signal_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = cancel_sender.send(true);
        }
    });

    let exit = run_loop(options, cancel_receiver, move |_, cancelled| {
        let runner = runner.clone();
        let prompt = prompt.clone();
        async move { runner.run(&prompt, cancelled).await }
    })
    .await;
    signal_task.abort();

    if exit == LoopExit::Cancelled {
        eprintln!("[codex-loop] stopping");
    }
}
