# codex-loop

`codex-loop` runs the same prompt in a fresh `codex exec` process at a fixed
interval. The interval starts after each Codex process exits.

```console
cargo install --path .

codex-loop 5m \
  "Review open PRs that need my attention. Handle anything actionable. If nothing needs attention, exit."
```

The wrapper reads Codex's JSONL event stream, shows useful agent and command
output, and reports each iteration's terminal status. Failures are retried after
the normal interval unless `--stop-on-error` is supplied.

```text
Usage: codex-loop [OPTIONS] <INTERVAL> <PROMPT>

Options:
  -C, --cwd <DIR>           Working directory
  -m, --model <MODEL>       Codex model override
      --max-runs <N>        Stop after N iterations
      --timeout <DURATION>  Maximum duration of an individual Codex run
      --stop-on-error       Stop the loop if Codex fails
      --verbose             Print additional lifecycle information
  -h, --help                Print help
```

Press Ctrl-C to stop. During an active iteration, the wrapper terminates the
Codex process and its child processes before exiting. During the interval, it
cancels the sleep immediately.

## Development

```console
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
