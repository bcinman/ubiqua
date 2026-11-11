# Running a model

`Memory` runs every aggregate in memory. Aggregates need no setup; views come from a model.

```rust
use domain_memory::Memory;

let bank = Memory::new().with::<Bank>();
let receipt = bank.send(&sam, DepositMoney { amount: 100 })?;
let summary = bank.view::<AccountSummary>().get(&sam);
```

## What `send` returns

```rust
Ok(Receipt {
    stream: "Account/sam-main",
    version: 2,                      // seq of the last event
    command_id: "memory-2",
    events: [MoneyDeposited { amount: 100, balance_after: 100 }],
    reply: Balance { balance: 100 },
})

Err(SendError::Rejected(WithdrawMoneyRejection::InsufficientFunds(..)))
```

Handle each rejection by name:

```rust
match bank.send(&sam, WithdrawMoney { amount: 500 }) {
    Ok(receipt) => show(receipt.reply.balance),
    Err(SendError::Rejected(WithdrawMoneyRejection::InsufficientFunds(r))) => {
        say(format!("you have {}, you asked for {}", r.balance, r.requested))
    }
    Err(SendError::Rejected(WithdrawMoneyRejection::NotOpen(_))) => say("open an account first"),
    Err(other) => fail(other),
}
```

## Retry safely

```rust
let first = bank.send_with_id(&sam, WithdrawMoney { amount: 30 }, "req-1")?;
let retry = bank.send_with_id(&sam, WithdrawMoney { amount: 30 }, "req-1")?;

assert_eq!(first, retry);           // same receipt; the money left once
```

Reusing an id for a different command on the same stream is an error:

```rust
Err(SendError::CommandIdReused { stream, command_id })
```

## All or nothing

A command's events, new state, receipt and deliveries to views commit together. A rejection records nothing; so does a panic in `evolve` or `reply`. A panic in a view doesn't undo the command: the view falls behind and retries on the next read.

```diagram
send ──▶ decide ──▶ commit ──────────────────▶ apply to views ──▶ return receipt
                    events, state, receipt,     in order,
                    queued deliveries           after the commit
```

## Send as someone

```rust
bank.acting_as(User("sam".into())).send_with_id(&sam, WithdrawMoney { amount: 30 }, "req-2")?;
```

`decide` reads it with `cx.actor::<User>()`. See [Who is asking](sessions.md).

## Read

Views: see [Views](views.md#read-it). The aggregates themselves:

```rust
bank.state(&sam);     // AccountState::Open { owner: "sam", balance: 70, frozen: false }
bank.events(&sam);    // Vec<Envelope<AccountEvent>>, oldest first
bank.versions();      // [("Account/sam-main", 3)]
```

```rust
Envelope {
    stream: "Account/sam-main",
    seq: 3,
    at: Timestamp(1762819200000),
    command_id: Some("memory-3"),
    event: AccountEvent::MoneyWithdrawn(..),
}
```

## Import history

```rust
bank.append(&sam, [AccountOpened { owner: "sam".into() }]);   // no decide; evolve runs
```

## One thread changes it

An app or a server hands its `Memory` to a `Runtime`. Its thread makes every change: commands, views, policies, schedules and the store's writes. Everything else sends it jobs:

```rust
let bank = Runtime::spawn(Memory::new().with::<Bank>());   // Clone to share
let client = Client::<BankApi>::in_process(bank.clone());
let server = Server::<BankApi>::new(bank.clone()).listen("0.0.0.0:7878")?;

bank.exec(|memory| memory.send(&sam, DepositMoney { amount: 1 }), |sent| show(sent));
let events = bank.call(|memory| memory.events(&sam))?;    // waits; for tests and scripts
```

Reads don't queue: view readers and telemetry read from the caller's thread.

The runtime runs the jobs that queued up together, as one batch. With [SQLite](storage.md), a batch is one transaction, so many commands reach the disk with one write. Each job's answer goes out once its batch is stored. If a batch can't be stored, the runtime answers `Unavailable` from then on: restart to carry on from what the file holds.

`Runtime::inline(memory)` runs each job on the thread that hands it in, for tests and the web.

`Memory` itself also works from several threads, as tests do: commands to one instance queue, and commands to different instances run in parallel.

## Keep it on disk

```rust
let bank = Memory::new().persist_to(Sqlite::open("bank.db")?).with::<Bank>();
```

See [Keep it on disk](storage.md).

## Policies and schedules

```rust
let bank = Arc::new(Memory::new().with::<Bank>().external(FraudCheck, Vendor));   // Bank lists FraudReview
let next = bank.run_due();        // runs what's due; Some(when) to call it again
```

A `Runtime` calls it for you. See [Policies](policies.md#who-runs-them).

## Set the clock

```rust
let bank = Memory::with_clock(|| Timestamp::from_millis(1_762_819_200_000));
```

## On the web

Everything builds for `wasm32-unknown-unknown`:

```sh
cargo check --target wasm32-unknown-unknown -p bank --lib
```

There's no system clock there, so use `Memory::with_clock`. And state there doesn't have to be `Send`:

```rust
#[aggregate(id = NoteId, state = Rc<Note>, ..)]   // builds for wasm, refused natively
```

That's `MaybeSend` and `MaybeSync`: `Send` and `Sync` natively, nothing on wasm. Bound on them in code meant for both.

Next: [Keep it on disk](storage.md).
