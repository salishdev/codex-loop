use std::{path::PathBuf, time::Duration};

use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "codex-loop",
    version,
    about = "Run a fresh Codex prompt repeatedly at a fixed interval"
)]
pub struct Cli {
    /// Interval between runs, e.g. 30s, 5m, 1h
    #[arg(value_parser = parse_nonzero_duration)]
    pub interval: Duration,

    /// Prompt passed to Codex
    pub prompt: String,

    /// Working directory
    #[arg(short = 'C', long = "cwd", value_name = "DIR", value_parser = parse_directory)]
    pub cwd: Option<PathBuf>,

    /// Codex model override
    #[arg(short = 'm', long)]
    pub model: Option<String>,

    /// Stop after N iterations
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(u64).range(1..))]
    pub max_runs: Option<u64>,

    /// Maximum duration of an individual Codex run
    #[arg(long, value_name = "DURATION", value_parser = parse_nonzero_duration)]
    pub timeout: Option<Duration>,

    /// Stop the loop if Codex fails
    #[arg(long)]
    pub stop_on_error: bool,

    /// Print additional lifecycle information
    #[arg(long)]
    pub verbose: bool,
}

fn parse_nonzero_duration(value: &str) -> Result<Duration, String> {
    let duration = humantime::parse_duration(value).map_err(|error| error.to_string())?;
    if duration.is_zero() {
        return Err("duration must be greater than zero".to_owned());
    }
    Ok(duration)
}

fn parse_directory(value: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(value);
    if path.is_dir() {
        Ok(path)
    } else {
        Err(format!("not a directory: {}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;

    use super::Cli;

    #[test]
    fn parses_human_duration() {
        let cli = Cli::try_parse_from(["codex-loop", "5m", "do work"]).unwrap();

        assert_eq!(cli.interval, Duration::from_secs(300));
    }

    #[test]
    fn rejects_invalid_and_zero_durations() {
        assert!(Cli::try_parse_from(["codex-loop", "later", "do work"]).is_err());
        assert!(Cli::try_parse_from(["codex-loop", "0s", "do work"]).is_err());
        assert!(Cli::try_parse_from(["codex-loop", "1s", "do work", "--timeout", "0s"]).is_err());
    }

    #[test]
    fn validates_max_runs() {
        let cli = Cli::try_parse_from(["codex-loop", "--max-runs", "3", "30s", "do work"]).unwrap();
        assert_eq!(cli.max_runs, Some(3));

        assert!(Cli::try_parse_from(["codex-loop", "--max-runs", "0", "30s", "do work"]).is_err());
    }

    #[test]
    fn rejects_a_non_directory_cwd() {
        assert!(
            Cli::try_parse_from([
                "codex-loop",
                "--cwd",
                "/a/path/that/does/not/exist",
                "30s",
                "do work",
            ])
            .is_err()
        );
    }
}
