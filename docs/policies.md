# Policies, outside systems and schedules

A policy reacts to events by sending commands. It's how one rule leads to the next: "whenever someone withdraws a large amount, score it for fraud; if it's risky, freeze the account for a day".

```diagram
event ──▶ policy ──▶ command          now
                 ──▶ call             to an outside system, and wait
                 ──▶ schedule         a command for later
```

From [`examples/bank/src/fraud.rs`](../examples/bank/src/fraud.rs):

```rust
#[domain::policy(from(Account(MoneyWithdrawn)))]
pub struct FraudReview;

impl React for FraudReview {
    async fn react(&self, cx: &PolicyCx<Self>, input: FraudReviewInput) -> Result<(), PolicyError> {
        let FraudReviewInput::MoneyWithdrawn(withdrawn) = input;
        if withdrawn.amount < SCORED_FROM {
            return Ok(());
        }
        let account = cx.source_id().clone();
        let Risk(risk) = cx.call(ScoreWithdrawal { account: account.clone(), amount: withdrawn.amount }).await?;
        if risk >= 90 {
            cx.send(&account, FreezeAccount)?;
            cx.schedule(&account, ThawAccount, cx.now().after(FREEZE_FOR))?;
        }
        Ok(())
    }
}
```

List it in the model, and give its outside system an adapter:

```rust
#[domain::model(aggregates(Account), views(..), policies(FraudReview))]
pub struct Bank;

let bank = Memory::new().with::<Bank>().external(FraudCheck, Vendor);
```

`with` builds each listed policy with `Default`. A policy that needs values, such as a client
for another service, stays out of `policies(..)` and is added with `.policy(..)`:

```rust
let bank = Memory::new().with::<Bank>().policy(Notify { mailer });
```

Listing a policy and adding it too panics: it would run twice.

```text
withdrew 900, balance 70
withdraw 10: rejected: Frozen
```

## Declare one

`from(..)` lists the events it reacts to, as in a [view](views.md). The attribute writes the input enum:

```rust
#[domain::policy(from(Account(MoneyWithdrawn, MoneyDeposited), Customer(CustomerRegistered)))]
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
| `source_id()` | the id of the aggregate that recorded the event, for a policy with one source |
| `source::<Account>()` | the same, as `Some` when `Account` recorded it, for a policy with several |
| `state::<Account>()` | that aggregate's state, just after the event |
| `at()` / `now()` | when the event happened / the time now |
| `send(&id, cmd)` | send a command |
| `call(call).await` | ask an outside system |
| `schedule(&id, cmd, at)` | send a command later |
| `schedule_with_id(&id, cmd, at, "id")` / `unschedule(&id, "id")` | a schedule you can cancel |
| `acting_as(user).send(&id, cmd)` | send as someone, for a `decide` that checks [who is asking](sessions.md) |
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
#[domain::call(system = FraudCheck, returns = Risk, errors(Blocked))]
pub struct ScoreWithdrawal {
    pub account: AccountId,
    pub amount: u64,
}

#[domain::external_system(calls(ScoreWithdrawal))]
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

The adapter can use any async library. Its future runs on the runtime's thread, so it shouldn't block.

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
#[domain::command(target = AccountId, events(AccountThawed))]       // no rejects(..)
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
| `Runtime::spawn` | on the runtime's thread | on the runtime's thread, when due |
| `Runtime::spawn` on the web | on the page's thread, as events arrive | when a job calls `run_due` |
| `Memory` on its own | when you call `run_due` | when you call `run_due` |

`run_due` does what's due and says when to call it again:

```rust
let bank = Rc::new(bank);
let next = bank.run_due();                   // Option<Timestamp>
```

On the web, call `run_due` from a timer.

## Test it

A story runs only the policies it adds, even with `.with::<Bank>()`, so each story is about the
rules it names:

```rust
Story::new()
    .policy(FraudReview)
    .given(&a1(), (opened(), MoneyDeposited { amount: 1000, balance_after: 1000 }))
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
