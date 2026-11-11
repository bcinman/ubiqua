# Sagas

A saga carries out something that takes several commands, on several aggregates, such as moving money between two accounts. No one command can do it, so the saga takes steps, and if a later step is refused, it undoes the earlier ones.

```diagram
TransferRequested ──▶ policy ──▶ saga
                                  debit sam     ✓   undo: refund sam
                                  credit kim    ✗
                                  refund sam    ◀── runs because the credit was refused
```

Shortened from [`examples/bank/src/transfers.rs`](../examples/bank/src/transfers.rs):

```rust
#[domain::saga(
    key = TransferId,
    rejects(NotOpen, NotOwner, Frozen, InsufficientFunds, DestinationClosed),
    sends(WithdrawMoney)
)]
pub struct ExecuteTransfer {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u64,
    pub by: Option<String>,
}

impl Run for ExecuteTransfer {
    async fn run(self, cx: &SagaCx<Self>) -> Result<(), ExecuteTransferRejection> {
        let moved = self.move_money(cx).await;
        // Both always record, so there is no rejection to handle.
        match &moved {
            Ok(()) => {
                let _ = cx.send("complete", cx.key(), CompleteTransfer).await;
            }
            Err(refused) => {
                let _ = cx.send("fail", cx.key(), FailTransfer { reason: refused.to_string() }).await;
            }
        }
        moved
    }
}

impl ExecuteTransfer {
    async fn move_money(&self, cx: &SagaCx<Self>) -> Result<(), ExecuteTransferRejection> {
        cx.send("debit", &self.from, WithdrawMoney { amount: self.amount })
            .compensate("refund", &self.from, RefundMoney { amount: self.amount })
            .await?;
        cx.send("credit", &self.to, DepositMoney { amount: self.amount }).await.map_err(|_| DestinationClosed)?;
        Ok(())
    }
}
```

The struct is the run's input. A policy starts it:

```rust
impl React for TransferExecution {
    async fn react(&self, cx: &PolicyCx<Self>, input: TransferExecutionInput) -> Result<(), PolicyError> {
        let TransferExecutionInput::TransferRequested(requested) = input;
        let transfer = cx.source_id().clone();
        let TransferRequested { from, to, amount, by } = requested;
        cx.start_saga(&transfer, ExecuteTransfer { from, to, amount, by })?;
        Ok(())
    }
}
```

List both in the model:

```rust
#[domain::model(aggregates(Account, Transfer), policies(TransferExecution), sagas(ExecuteTransfer))]
pub struct Bank;

let bank = Memory::new().with::<Bank>();
```

Without a model, `.saga::<ExecuteTransfer>()` hosts one.

## Declare one

| `#[domain::saga(..)]` | |
| --- | --- |
| `key = TransferId` | one run per key |
| `rejects(..)` | how a run can be refused |
| `sends(WithdrawMoney)` | commands whose rejections `?` passes on |

The attribute writes `ExecuteTransferRejection`. With `sends(WithdrawMoney)`, `?` on a debit turns `InsufficientFunds` into `ExecuteTransferRejection::InsufficientFunds`.

## Steps

| `cx.` | |
| --- | --- |
| `send("name", &id, cmd).await` | a step: the reply, or the rejection |
| `.acting_as(user)` | send it as someone, for a `decide` that checks [who is asking](sessions.md) |
| `.compensate("name", &id, cmd)` | what undoes it if the run is refused later |
| `signal::<T>("name").await` | wait for a signal |
| `key()` / `now()` | the run's key / the time now |

Give every step its own name. A name used twice kills the run:

```text
ExecuteTransfer t-1 died: step debit ran twice; give each step its own name
```

## Each step happens once

Every step's result is kept. When a run is resumed, after a retry or a restart, its code runs again from the top, and each step it already took returns the kept result instead of sending again:

```text
first run         debit ✓   credit … the store is down
second run        debit (kept ✓)   credit ✓   complete ✓
```

So a run's code must take the same steps, in the same order, given the same input and results. Don't decide on `cx.now()`; decide in an aggregate.

Each step's command is sent under an id made from the run and its name:

```text
ExecuteTransfer:t-1:debit
ExecuteTransfer:t-1:credit
```

## When it's refused

Return `Err`, and the compensations of the steps that succeeded run, last first:

```text
one ✓   two ✓   refuse ✗    →    undo two, undo one
```

A compensation can't be rejected, so undoing can't get stuck:

```rust
#[domain::command(target = AccountId, events(MoneyDeposited), reply = Balance)]   // no rejects(..)
pub struct RefundMoney { pub amount: u64 }
```

When the store is down, a step or compensation runs again later: after 1s, 2s, 4s, up to a minute apart. A panic kills the run.

## Signals

A run can wait for something outside it, such as a person approving:

```rust
let approved: bool = cx.signal("approved").await;
```

```rust
bank.signal::<ExecuteTransfer>(&transfer, "approved", true)?;
```

A signal sent before the run waits for it is kept. The first value sent under a name wins.

## Where it stands

```rust
bank.saga_status::<ExecuteTransfer>(&transfer)
```

| | |
| --- | --- |
| `NotStarted` | no run under this key |
| `Running` | taking steps, waiting for a signal, or undoing |
| `Completed` | done |
| `Rejected(r)` | refused, and every compensation ran |
| `Died(why)` | it panicked, or its kept results don't fit its code. It won't run again |

Starting a run under a key already used does nothing, so a policy that runs twice starts one run.

With [SQLite](storage.md), unfinished runs carry on after a restart.

## Test it

```rust
Story::new()
    .with::<Bank>()                       // hosts the model's sagas, but runs no policy
    .policy(TransferExecution)
    .given(&sam(), (opened(), MoneyDeposited { amount: 100, balance_after: 100 }))
    .when(&t1(), RequestTransfer { from: sam(), to: kim(), amount: 30 })       // kim has no account
    .then(&t1(), (requested(30), failed(DestinationClosed)))
    .then(&sam(), (MoneyWithdrawn { amount: 30, balance_after: 70 }, MoneyDeposited { amount: 30, balance_after: 100 }))
    .then_none(&kim())
    .saga_status::<ExecuteTransfer>(&t1(), SagaStatus::Rejected(DestinationClosed.into()));
```

Send a signal with `.signal::<S>(&key, "name", value)`. A run that dies fails the story.

Next: [Who is asking](sessions.md).
