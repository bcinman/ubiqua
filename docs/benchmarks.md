# Benchmarks

Criterion benchmarks in `examples/bank/benches` measure the bank example through the public API: what a command, a read, a restart or a round trip costs, not how the crates do it inside. They stay valid as the internals and the API change.

```sh
cargo bench -p bank                                  # everything, about 5 minutes
cargo bench -p bank --bench commands                 # one file
cargo bench -p bank -- send/deposit                  # benchmarks whose name contains this
cargo bench -p bank -- --quick                       # a rough pass in under a minute
cargo bench -p bank -- --save-baseline main          # record a baseline, say on main
cargo bench -p bank -- --baseline main               # compare this branch against it
cargo test -p bank --benches                         # run each benchmark once, as CI does
```

`commands`, `views` and `storage` host only the bank's three views, so "with views" measures them and nothing else. `workflows` and `client` run the whole `Bank` model, policies and saga included, as the app does. Every command is decided at a fixed time. Each sample starts from a fresh bank every 10,000 commands (1,000 with SQLite or sagas), so memory stays bounded and samples see the same history sizes. Compare numbers from the same machine only, and close other work while they run. On a shared cloud VM, two runs of the same code can differ by 5 to 35%, so criterion reports changes that aren't there; compare there with `-- --baseline main --noise-threshold 0.1`, or use a quiet dedicated machine.

## What they measure

| Metric | Benchmarks | Why it matters |
| --- | --- | --- |
| A command's cost in memory | `commands`: `send/deposit`, `send/rejected`, `send/retry` | The floor under everything: decide, evolve, commit. A refusal should cost about a decision; a retry, a lookup. |
| What views add to a command | `send/deposit_with_views` | Each command is applied to every view that reads it before `send` returns. |
| Scale with many accounts | `send/deposit_with_views_across_10k_accounts`, `read/summary_among_10k_accounts` | Finding one instance or key should not slow down as there are more. |
| Parallel throughput | `send_parallel/4_threads/*` | Commands to different accounts should run in parallel; to one account, one at a time. Compare with the single-threaded `send/*`. |
| Reads | `views`: `read/*` | What a UI pays for each `get` and each page of a list. |
| Replaying history | `replay/bank_views/*` | How long a view added to existing events, or changed, takes to catch up. Throughput is events per second, so it should stay flat as the history grows. |
| A durable command | `storage`: `sqlite/send/*` | With SQLite, the transactions per command, with and without views saving theirs. |
| The first command after a restart | `sqlite/first_read/*` | An account reads its events from the file the first time it's used. |
| Restart time | `sqlite/restart/*` | Opening a bank with views stored, or rebuilding them from every event. |
| Reactions | `workflows`: `workflows/scored_withdrawal`, `workflows/transfer` | A policy calling an external system, and a saga's full transfer: request, debit, credit, complete. |
| What an app waits for | `client`: `client/in_process/*`, `client/websocket/*` | A receipt or a read through a `Client`, from the runtime's thread or over a WebSocket on loopback, and how long a change takes to reach another client's live view. |
| Commands in flight | `client/in_process/send_*_in_flight`, `client/sqlite/*` | How many commands go through when an app or several apps send without waiting, in memory and with SQLite, where a runtime commits each batch in one transaction. |

`storage` writes real files in the system's temporary directory, so it measures that disk too. `send_parallel` uses 4 threads; it needs at least 4 cores to show parallelism.
