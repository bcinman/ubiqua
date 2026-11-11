# Keep it on disk

```rust
use domain_sqlite::Sqlite;

let bank = Memory::new().persist_to(Sqlite::open("bank.db")?).with::<Bank>();
```

Everything else stays the same. On the next run, the bank carries on where it stopped:

```text
$ cargo run -p bank -- bank.db
..
withdraw 10: rejected: Frozen
statement #1: StatementLine(StatementLine { kind: Deposit, amount: 1000, balance: 1000 })
statement #2: StatementLine(StatementLine { kind: Withdrawal, amount: 30, balance: 970 })
statement #3: StatementLine(StatementLine { kind: Withdrawal, amount: 50, balance: 920 })
statement #4: StatementLine(StatementLine { kind: Withdrawal, amount: 900, balance: 20 })
$ cargo run -p bank -- bank.db
redraw #1: Ok(Some(Summary { owner: Some("sam"), balance: 20 }))
redraw #2: Ok(Some(Summary { owner: Some("sam"), balance: 1020 }))
redraw #3: Ok(Some(Summary { owner: Some("sam"), balance: 990 }))
redraw #4: Ok(Some(Summary { owner: Some("sam"), balance: 1020 }))
transfer: rejected: AlreadyRequested
redraw #5: Ok(Some(Summary { owner: Some("sam"), balance: 120 }))
redraw #6: Ok(Some(Summary { owner: Some("sam"), balance: 1020 }))
withdraw 900: rejected: Frozen
redraw #7: Ok(Some(Summary { owner: Some("sam"), balance: 1010 }))
redraw #8: Ok(Some(Summary { owner: Some("sam"), balance: 1020 }))
withdraw 10: rejected: Frozen
statement #5: StatementLine(StatementLine { kind: Deposit, amount: 1000, balance: 1020 })
```

The second run finds the account still frozen and the transfer already requested. Only its deposit lands, as statement line #5; each withdrawal shows [as predicted](offline.md) and then reverts when it's refused.

## What's stored

| Table | Holds |
| --- | --- |
| `events` | every event, in commit order |
| `receipts` | each command's receipt, by command id |
| `views`, `view_keys`, `view_entries`, `view_progress` | each view's version, state per key, emitted events, and how far it has read |
| `schedules` | each [scheduled command](policies.md#schedules): when, and whether it ran or was cancelled |

Policies, projections and sagas keep theirs in the view tables:

| Name | Holds |
| --- | --- |
| `policy:FraudReview` | how far the [policy](policies.md) has read |
| `projection:Ledger` | the [projection](projections.md)'s version, and how far it has written |
| `saga:ExecuteTransfer` | each [saga](sagas.md) run: its input, step results and signals, by key |

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
each policy: stored before? ──yes──▶ act on the events it missed
                            └──no───▶ start from now
schedules ──▶ load the ones still waiting; overdue ones run at once
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
#[domain::event]
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

Write your own store by implementing [`Store`](../crates/domain-memory/src/store.rs), with the row types in `domain_memory::store`.

Next: [The client](client.md).
