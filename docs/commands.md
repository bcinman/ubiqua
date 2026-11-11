# Commands and events

## A command

```rust
#[derive(Command, Debug)]
#[command(
    target = AccountId,                        // which instance it goes to
    events(MoneyWithdrawn),                    // what it may record
    rejects(NotOpen, InsufficientFunds),       // why it may be refused (optional)
    reply = Balance,                           // what the caller gets back (optional, default `()`)
)]
pub struct WithdrawMoney {
    pub amount: u64,                           // the intent, not the outcome
}
```

The derive generates two enums:

```rust
pub enum WithdrawMoneyEvent { MoneyWithdrawn(MoneyWithdrawn) }
pub enum WithdrawMoneyRejection { NotOpen(NotOpen), InsufficientFunds(InsufficientFunds) }
```

Each member converts into its enum with `.into()`. Rejections also convert with `?`.

## Name it after the intent

| Good | Bad |
| --- | --- |
| `FreezeAccount`, `ChangeOwner` | `UpdateAccount { frozen, owner }` |
| `WithdrawMoney { amount }` | `SetBalance { balance }` |

## Carry the intent, record the outcome

```rust
WithdrawMoney { amount: 30 }                        // what the caller wants
MoneyWithdrawn { amount: 30, balance_after: 70 }    // what the account decided
```

## An event

```rust
#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MoneyWithdrawn {
    pub amount: u64,
    pub balance_after: u64,
}

assert_eq!(<MoneyWithdrawn as Event>::NAME, "MoneyWithdrawn");
```

Name events in the past tense. They're facts: `evolve` must accept them. They're stored as JSON, `{"MoneyWithdrawn": {"amount": 30, "balance_after": 70}}`, and kept forever, so a new field needs `#[serde(default)]`.

## A rejection

```rust
#[derive(Rejection, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InsufficientFunds {
    pub balance: u64,
    pub requested: u64,
}

assert_eq!(InsufficientFunds { balance: 50, requested: 80 }.to_string(),
           "InsufficientFunds { balance: 50, requested: 80 }");
```

Carry what the caller needs to act on it. A rejection records nothing. It serializes, so a client across a network gets it by name.

## A fact that can't be refused

Leave out `rejects`. The rejection enum is then empty, so `decide` can't return one.

```rust
#[derive(Command, Debug)]
#[command(target = AccountId, events(SettlementReceived, SettlementOnClosedAccount))]
pub struct RecordSettlement {
    pub settlement_id: String,
    pub amount: u64,
}
```

## Where things go

```text
examples/bank/src/
  commands.rs    each command next to its events and rejections
  account.rs     the aggregate
  lib.rs         ids and re-exports
```

Split `commands.rs` into one file per command as it grows.

Next: [Aggregates](aggregates.md).
