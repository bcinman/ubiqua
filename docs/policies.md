# Policies, outside systems and schedules

A policy reacts to events by sending commands. It's how one rule leads to the next: "whenever someone withdraws a large amount, score it for fraud; if it's risky, freeze the account for a day".

```diagram
event ──▶ policy ──▶ command          now
                 ──▶ call             to an outside system, and wait
                 ──▶ schedule         a command for later
```

From [`examples/bank/src/fraud.rs`](../examples/bank/src/fraud.rs):

```rust
#[derive(Policy)]
#[policy(from(Account(MoneyWithdrawn)))]
pub struct FraudReview;

impl React for FraudReview {
    async fn react(&self, cx: &PolicyCx<Self>, input: FraudReviewInput) -> Result<(), PolicyError> {
        let FraudReviewInput::MoneyWithdrawn(withdrawn) = input;
        if withdrawn.amount < SCORED_FROM {
            return Ok(());
        }
        let account = cx.source::<Account>().expect("reads accounts").clone();
        let Risk(risk) = cx.call(ScoreWithdrawal { account: account.clone(), amount: withdrawn.amount }).await?;
        if risk >= 90 {
            cx.send(&account, FreezeAccount)?;
            cx.schedule(&account, ThawAccount, cx.now().after(FREEZE_FOR))?;
        }
        Ok(())
    }
}
```

Run it:

```rust
let bank = Memory::new()
    .with::<Bank>()
    .policy(FraudReview)
    .external(FraudCheck, Vendor);
```

```text
withdrew 900, balance 70
withdraw 10: rejected: Frozen
```

## Declare one

`from(..)` lists the events it reacts to, as in a [view](views.md). The derive writes the input enum:

```rust
#[policy(from(Account(MoneyWithdrawn, MoneyDeposited), Customer(CustomerRegistered)))]
pub struct Notify;

// generated:
pub enum NotifyInput {
    MoneyWithdrawn(MoneyWithdrawn),
    MoneyDeposited(MoneyDeposited),
    CustomerRegistered(CustomerRegistered),
}
```

## What it can do

| `cx.` | |
| --- | --- |
| `source::<Account>()` | the id of the aggregate that recorded the event |
| `state::<Account>()` | that aggregate's state, just after the event |
| `at()` / `now()` | when the event happened / the time now |
| `send(&id, cmd)` | send a command |
| `call(call).await` | ask an outside system |
| `schedule(&id, cmd, at)` | send a command later |
| `schedule_with_id(&id, cmd, at, "id")` / `unschedule(&id, "id")` | a schedule you can cancel |
| `send_as(&actor, &id, cmd)` | send as someone, for a `decide` that checks [who is asking](sessions.md) |
| `start_saga(&key, saga)` / `signal::<S>(&key, "name", value)` | start a [saga](sagas.md) / signal one |

## Running twice is safe

A policy that fails runs again for the same event. Each `send` and `schedule` gets an id made from the event, the same on every run:

```text
FraudReview:Account/sam-main:4:1      the freeze
FraudReview:Account/sam-main:4:2      the thaw
```

So on the second run, the freeze returns its first receipt instead of freezing again.

## When it fails

Return `Err`, or panic, and it runs again later: after 1s, 2s, 4s, up to a minute apart. Events after it wait their turn.

```rust
Err(PolicyError::new("the ledger is down"))
cx.send(&account, FreezeAccount)?;     // any error converts with `?`

bank.failures();                // ["FraudReview failed on Account/sam-main #4: the ledger is down"]
```

To give up on an event instead, handle the error and return `Ok(())`.

## What it reacts to

Only events recorded after the policy was added. History doesn't trigger it:

```rust
bank.append(&sam, [MoneyWithdrawn { .. }]);    // no fraud check
```

With [SQLite](storage.md), a policy remembers how far it got. After a restart it catches up on what it missed.

## Outside systems

Name the service and its calls in the bank's words. An adapter talks to the vendor:

```rust
#[derive(Call, Clone, Debug, PartialEq)]
#[call(system = FraudCheck, returns = Risk, errors(Blocked))]
pub struct ScoreWithdrawal {
    pub account: AccountId,
    pub amount: u64,
}

#[derive(ExternalSystem)]
#[external_system(calls(ScoreWithdrawal))]
pub struct FraudCheck;

struct Vendor { http: HttpClient }

impl Answer<ScoreWithdrawal> for Vendor {
    async fn answer(&self, call: ScoreWithdrawal) -> Answered<ScoreWithdrawal> {
        match self.http.score(call.account, call.amount).await {
            Ok(score) => Ok(Risk(score)),
            Err(e) if e.blocked() => Err(CallError::Refused(Blocked.into())),
            Err(e) => Err(CallError::Failed(e.to_string())),
        }
    }
}
```

| The call returns | Means |
| --- | --- |
| `Ok(Risk(95))` | it answered |
| `Err(CallError::Refused(..))` | it said no, with one of `errors(..)`. Handle it by name |
| `Err(CallError::Failed(..))` | it broke. With `?`, the policy runs again later |

With no adapter, every call fails:

```text
nothing answers FraudCheck.ScoreWithdrawal: pass an adapter to `Memory::external`
```

The adapter can use any async library. Its future runs on the domain thread, so it shouldn't block.

## Schedules

```rust
bank.schedule(&sam, ThawAccount, bank.now().after(Duration::from_secs(86_400)), "thaw-sam")?;
bank.schedule(&sam, ThawAccount, bank.now().after(Duration::from_secs(3_600)), "thaw-sam")?;  // replaces it
bank.unschedule(&sam, "thaw-sam")?;
```

| `unschedule` returns | |
| --- | --- |
| `Ok(())` | it won't run |
| `Err(AlreadyExecuted)` | too late, it ran |
| `Err(AlreadyCancelled)` / `Err(NotScheduled)` | nothing to cancel |

Only commands that can't be rejected can be scheduled, because nobody is there to handle a rejection:

```rust
#[command(target = AccountId, events(AccountThawed))]       // no rejects(..)
pub struct ThawAccount;

impl Decide<ThawAccount> for Account {
    fn decide(state: &AccountState, _: &ThawAccount, _: &Cx) -> Decision<ThawAccount> {
        Ok(match state {
            AccountState::Open { frozen: true, .. } => vec![AccountThawed.into()],
            _ => vec![],                                     // nothing to do
        })
    }
}
```

With [SQLite](storage.md), schedules survive a restart.

## Who runs them

| | Policies | Schedules |
| --- | --- | --- |
| `Client::in_process` | on the domain thread | on the domain thread, when due |
| `Client::inline` | on the thread that sent the event | when you call `run_due` |
| `Memory` on its own | when you call `run_due` | when you call `run_due` |

`run_due` does what's due and says when to call it again:

```rust
let bank = Arc::new(bank);
bank.on_wake(move || wake_my_loop());       // something new to run

loop {
    let next = bank.run_due();               // Option<Timestamp>
    wait_until(next);                        // or until woken
}
```

On the web, call `run_due` from a timer.

## Test it

```rust
Story::new()
    .policy(FraudReview)
    .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 1000, balance_after: 1000 }.into()])
    .when(&a1(), WithdrawMoney { amount: 900 })
    .then(&a1(), [MoneyWithdrawn { amount: 900, balance_after: 100 }])
    .call(ScoreWithdrawal { account: a1(), amount: 900 })
    .succeed(Risk(95))
    .then(&a1(), [AccountFrozen])
    .advance(FREEZE_FOR)
    .then(&a1(), [AccountThawed]);
```

See [Stories](stories.md#policies).

Next: [Projections](projections.md).
