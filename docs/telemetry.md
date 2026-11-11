# Watch it run

Telemetry is what a dashboard reads: the model's parts, each aggregate as it stood at any version, each view's keys, and a trace of every command, event and view change. A trace can replay from the start or follow live.

```rust
let trace = bank.telemetry().trace(Since::Start)?;     // a TraceFeed: try_next, notify, recv().await
bank.send(&sam, DepositMoney { amount: 100 })?;
for step in trace.drain() { draw(step) }
```

```diagram
replay:  Event Event View View … ──▶ CaughtUp { streams } ──▶ live:  Command Event View View …
         what's recorded, run through                            each one as it happens,
         fresh copies of the views                               in one order for every observer
```

## A trace

One JSON object per step. A deposit, then a refused withdrawal:

```text
{"command":{"command":"DepositMoney","stream":"Account/sam-main","command_id":"memory-2","actor":null,"input":{"amount":100},"at":1762819200000,"outcome":{"committed":{"version":2}}}}
{"event":{"aggregate":"Account","stream":"Account/sam-main","seq":2,"id":"sam-main","command_id":"memory-2","event":{"MoneyDeposited":{"amount":100,"balance_after":100}},..}}
{"view":{"view":"AccountSummary","key":"sam-main","state":{"balance":100,"owner":"sam"},"cause":{"stream":"Account/sam-main","seq":2},..}}
{"view":{"view":"OwnerHoldings","key":"sam","state":{"balances":{"sam-main":100}},"cause":{"stream":"AccountSummary/sam-main","seq":2},..}}
{"view":{"view":"Statement","key":"sam-main","emitted":[{"cursor":1,"event":{"StatementLine":{"amount":100,"balance":100,"kind":"Deposit"}}}],..}}
{"command":{"command":"WithdrawMoney","input":{"amount":500},"outcome":{"rejected":{"InsufficientFunds":{"balance":100,"requested":500}}},..}}
```

| Step | Says |
| --- | --- |
| `command` | a command was decided: its input, who sent it, `committed` at a version or `rejected` with why |
| `event` | an aggregate recorded an event. A policy's commands name the event they reacted to: `FraudReview:Account/sam-main:3:1` |
| `view` | a view applied an event to a key: the key's state after it, what it emitted, and the `cause` |
| `caught_up` | the replay is over; `streams` is how far it went in each stream |
| `failed` | the trace can't go on. Nothing follows |

Follow `cause` to draw the chain: `OwnerHoldings` changed because of `AccountSummary/sam-main` #2, which changed because of `Account/sam-main` #2.

## Replay

```rust
bank.telemetry().trace(Since::Start)        // everything, then live
bank.telemetry().trace(Since::Now)          // only what happens from now
bank.telemetry().trace(Since::After(streams))   // after a `caught_up`'s streams, then live
```

A replay runs the recorded events, here or in the store, through fresh copies of the views. It shows the same view changes the live trace did, and the app's views aren't touched. Commands aren't recorded, so a replay shows only their events. An event that commits while a trace starts shows once.

## Look around

```rust
let telemetry = bank.telemetry();
telemetry.catalog();                              // the parts, and what each reads
telemetry.status()?;                              // every stream's version, and what's failing
telemetry.aggregate("Account/sam-main", Some(1))?; // the instance as it stood at version 1
telemetry.view("Statement");                       // every key, its state and cursor
```

```text
catalog   {"parts":[{"kind":"aggregate","name":"Account","reads":[],"version":0},
                    {"kind":"view","name":"OwnerHoldings","reads":["AccountSummary"],"version":0},
                    {"kind":"view","name":"Statement","reads":["Account"],"version":1},
                    {"kind":"policy","name":"FraudReview","reads":["Account"],"version":0},
                    {"kind":"saga","name":"ExecuteTransfer","reads":[],"version":0}, ..]}
aggregate Open { owner: "sam", balance: 0, frozen: false }        its state, as its Debug
view      [{"key":"sam-main","stream":"Statement/sam-main","state":"sam","cursor":1}]
```

Step `at` from 1 to the version to scrub through an account's history.

## Serve it

Telemetry shows everything, whoever may read it, so a server serves it only to the connections you allow:

```rust
Server::<BankApi>::new(bank)
    .sessions(|token| users.check(token))
    .telemetry(|user: Option<&User>| user.is_some_and(|u| u.is_staff()))
    .listen("0.0.0.0:7878")?;
```

Then a dashboard reads it through a client, the same way in process or connected:

```rust
let ops = Client::<BankApi>::connect(&url, Some(&staff_token));
let catalog = ops.telemetry().catalog();           // Pending<Result<Catalog, String>>
let trace = ops.telemetry().trace(Since::Start);   // carries on after a reconnect
```

After a reconnect the trace replays only what it missed, then sends `caught_up` again. Anyone else gets an error:

```text
→ {"id":7,"op":"trace","since":"start"}
← {"id":7,"err":{"invalid":"this connection may not read telemetry"}}
```

A dashboard in another language speaks [the wire protocol](../crates/domain/src/wire.rs): `catalog`, `status`, `aggregate` (`stream`, `at`), `view_keys` (`view`) and `trace` (`since`), ended with `stop`.

```text
→ {"id":3,"op":"trace","since":{"after":{"Account/sam-main":2}}}
← {"id":3,"trace":{"event":{"aggregate":"Account","stream":"Account/sam-main","seq":3,..}}}
← {"id":3,"trace":{"caught_up":{"streams":{"Account/sam-main":3,..}}}}
```

## What it costs

Nothing much while nobody watches: one atomic load per command, event and view change. With a trace open, each step is encoded as JSON when it happens. A replay reads every recorded event and runs it through every view.

## Limits

- Aggregate states show as their `Debug`: they don't serialize.
- A replay applies events in the order a view added later receives them, by time. Two events recorded in the same millisecond on different streams can replay in another order than they happened.
- Projections and sagas show through the commands and events they cause, not their own steps.

Next: back to the [guides](README.md#guides).
