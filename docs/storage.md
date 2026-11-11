# Keep it on disk

```rust
use domain_sqlite::Sqlite;

let bank = Memory::new().persist_to(Sqlite::open("bank.db")?).with::<Bank>();
```

Everything else stays the same. On the next run, the bank carries on where it stopped:

```text
$ cargo run -p bank -- bank.db
withdraw 500: rejected: InsufficientFunds { balance: 70, requested: 500 }
$ cargo run -p bank -- bank.db
showing: Some(Summary { owner: Some("sam"), balance: 70 })
redraw #1: Some(Summary { owner: Some("sam"), balance: 170 })
redraw #2: Some(Summary { owner: Some("sam"), balance: 140 })
withdraw 500: rejected: InsufficientFunds { balance: 140, requested: 500 }
```

## What's stored

| Table | Holds |
| --- | --- |
| `events` | every event, in commit order |
| `receipts` | each command's receipt, by command id |
| `views`, `view_keys`, `view_entries`, `view_progress` | each view's version, state per key, emitted events, and how far it has read |

Values are JSON, so any SQLite tool can read them:

```text
sqlite> SELECT stream, seq, event FROM events;
Account/sam-main|1|{"AccountOpened":{"owner":"sam"}}
Account/sam-main|2|{"MoneyDeposited":{"amount":100,"balance_after":100}}
```

## What happens on start

```diagram
open ──▶ each view: stored at this version? ──yes──▶ load it, then apply the events it missed
                                          └──no───▶ rebuild it from every event
first command to an account ──▶ read its events, evolve its state
```

A command commits its events and receipt in one transaction, before anything in memory changes. A view saves its state with how far it has read, so after a crash it picks up from there. Nothing is applied twice.

## Retry after a restart

```rust
let first = bank.send_with_id(&sam, WithdrawMoney { amount: 30 }, "req-1")?;
// the app restarts
let retry = bank.send_with_id(&sam, WithdrawMoney { amount: 30 }, "req-1")?;

assert_eq!(first, retry);           // the stored receipt; the money left once
```

When the database fails mid-command, `send` returns `SendError::Unavailable`. Retry with the same id.

## Change a view

Change its version. See [Views](views.md#change-a-view).

## Change an event

Events are kept forever, so old ones must still decode:

```rust
#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MoneyDeposited {
    pub amount: u64,
    pub balance_after: u64,
    #[serde(default)]
    pub note: String,               // added later; old events decode with ""
}
```

An event that doesn't decode stops the read with a message naming its stream and seq.

## Limits

- One process opens a file at a time.
- Views and the accounts in use are held in memory.
- `domain-sqlite` doesn't build for the web.

Write your own store by implementing [`Store`](../crates/domain-memory/src/store.rs).

Next: [The client](client.md).
