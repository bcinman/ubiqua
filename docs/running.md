# Running a model

`Memory` runs every aggregate in memory. No setup, no registration.

```rust
use domain_memory::Memory;

let bank = Memory::new();
let receipt = bank.send(&sam, DepositMoney { amount: 100 })?;
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

A command's events, new state and receipt commit together. A rejection records nothing; so does a panic in `evolve` or `reply`.

## Read

```rust
bank.state(&sam);     // AccountState::Open { owner: "sam", balance: 70 }
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

## Share between threads

```rust
let bank = Arc::new(Memory::new());

for _ in 0..8 {
    let bank = bank.clone();
    std::thread::spawn(move || bank.send(&sam, DepositMoney { amount: 1 }));
}
```

Commands to one instance queue. Commands to different instances run in parallel.

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

Next: [Stories](stories.md).
