# Projections

A projection writes events into a store you own: a database table, a search index, a file. A [view](views.md) is kept for you. A projection's store is yours to query however you like.

```diagram
events ──▶ projection ──▶ your store
           per key, in order, in batches
```

From [`examples/bank/src/ledger.rs`](../examples/bank/src/ledger.rs):

```rust
#[domain::projection(key = AccountId, from(Account(MoneyDeposited, MoneyWithdrawn)))]
pub struct Ledger {
    pub store: LedgerStore,
}

impl Project for Ledger {
    async fn project(&self, account: &AccountId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> {
        self.store.record(batch.into_iter().map(|input| {
            let (kind, amount, balance_after) = match input.event {
                LedgerInput::MoneyDeposited(e) => (LineKind::Deposit, e.amount, e.balance_after),
                LedgerInput::MoneyWithdrawn(e) => (LineKind::Withdrawal, e.amount, e.balance_after),
            };
            ((input.stream.to_string(), input.seq), LedgerRow { account: account.clone(), kind, amount, balance_after })
        }));
        Ok(())
    }
}
```

Run it:

```rust
let bank = Memory::new().with::<Bank>().projection(Ledger { store });
```

## Declare one

Like a view: a key, the events it reads, and `key_of(..)` to route them. No state.

```rust
#[domain::projection(key = AccountId, from(Account(MoneyDeposited, MoneyWithdrawn)))]
pub struct Ledger { .. }

// generated:
pub enum LedgerInput {
    MoneyDeposited(MoneyDeposited),
    MoneyWithdrawn(MoneyWithdrawn),
}
```

## What each input carries

| `input.` | |
| --- | --- |
| `event` | the event, as `LedgerInput` |
| `stream`, `seq` | where it was recorded: `Account/sam-main`, `4` |
| `at` | when |
| `source::<Account>()` | the id that recorded it |
| `state::<V>()` | if it came from the view `V`, that key's state after it |

## Write every row once

An input can arrive twice, say after a crash. Key each row by `stream` and `seq`, so writing it again changes nothing:

```sql
INSERT INTO ledger (stream, seq, account, amount) VALUES (?, ?, ?, ?)
ON CONFLICT (stream, seq) DO NOTHING
```

## Batches

Each call gets one key's inputs, oldest first, at most `BATCH` of them:

```rust
impl Project for Ledger {
    const BATCH: usize = 500;     // default 100
    ..
}
```

## When it fails

Return `Err`, or panic, and the same batch runs again later: after 1s, 2s, 4s, up to a minute apart. Other keys carry on meanwhile.

```text
sam-main  ✗ ─ 1s ─ ✗ ─ 2s ─ ✓
kim-main  ✓ ✓ ✓
```

## History

| | Gets |
| --- | --- |
| a new projection | all of history first |
| after a restart, with [SQLite](storage.md) | only what it missed |
| a new `version` | all of history again |

```rust
#[domain::projection(key = AccountId, from(..), version = 2)]    // rebuild your store from scratch
```

Unlike a [policy](policies.md), a projection never sends commands, so replaying history is safe.

## Read your own write

After a command, check the projection has written it before reading your store:

```rust
let receipt = bank.send(&sam, DepositMoney { amount: 100 })?;
bank.caught_up::<Ledger>(&receipt)?;      // Err(NotCaughtUp { .. }) until it has
let rows = store.for_account(&sam);
```

## Test it

Give it a store you can read back:

```rust
let store = LedgerStore::default();
Story::new()
    .projection(Ledger { store: store.clone() })
    .given(&sam(), (opened(), MoneyDeposited { amount: 100, balance_after: 100 }))
    .when(&sam(), WithdrawMoney { amount: 40 })
    .then(&sam(), [MoneyWithdrawn { amount: 40, balance_after: 60 }]);

assert_eq!(store.for_account(&sam()), [
    LedgerRow { account: sam(), kind: LineKind::Deposit, amount: 100, balance_after: 100 },
    LedgerRow { account: sam(), kind: LineKind::Withdrawal, amount: 40, balance_after: 60 },
]);
```

Next: [Sagas](sagas.md).
