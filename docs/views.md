# Views

A view answers a question, like "what's in this account?". It folds events into state per key, and keeps it current as events arrive.

```diagram
Account a-1 ──events──▶ AccountSummary a-1 ──▶ OwnerHoldings sam   { a-1: 70, a-2: 500 }
Account a-2 ──events──▶ AccountSummary a-2 ──┘
            └─────────▶ Statement a-1          [deposit 100, withdrawal 30]
```

## Fold events into state

```rust
#[derive(View)]
#[view(key = AccountId, from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)), state = Summary)]
pub struct AccountSummary;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub owner: Option<String>,
    pub balance: u64,
}

impl Apply for AccountSummary {
    fn apply(summary: &mut Summary, input: AccountSummaryInput, _: &mut ViewCx<Self>) {
        match input {
            AccountSummaryInput::AccountOpened(e) => summary.owner = Some(e.owner),
            AccountSummaryInput::MoneyDeposited(e) => summary.balance = e.balance_after,
            AccountSummaryInput::MoneyWithdrawn(e) => summary.balance = e.balance_after,
        }
    }
}
```

| Attribute | Means |
| --- | --- |
| `key = AccountId` | one state per account |
| `from(Account(..))` | the source aggregate, and the events read from it |
| `state = Summary` | `Default` is the state before the first event; omit for `()` |
| `version = 1` | see [Change a view](#change-a-view); omit for `0` |
| `predicts(DepositMoney)` | show a command before its answer; see [Offline and instant](offline.md) |

The derive generates `AccountSummaryInput`, an enum of exactly the events listed. Other events pass by.

## Emit events of your own

```rust
#[derive(View)]
#[view(key = AccountId, from(Account(MoneyDeposited, MoneyWithdrawn)), events(StatementLine))]
pub struct Statement;

impl Apply for Statement {
    fn apply(_: &mut (), input: StatementInput, cx: &mut ViewCx<Self>) {
        cx.emit(match input {
            StatementInput::MoneyDeposited(e) => StatementLine { kind: LineKind::Deposit, amount: e.amount, balance: e.balance_after },
            StatementInput::MoneyWithdrawn(e) => StatementLine { kind: LineKind::Withdrawal, amount: e.amount, balance: e.balance_after },
        });
    }
}
```

Without `events(..)`, a view emits every input it applies.

## Read several sources into one key

```rust
#[derive(View)]
#[view(key = OwnerId, from(Wallet(Credited), Club(Joined)), key_of(Wallet, Club), state = Activity)]
struct PerOwner;

impl KeyOf<Wallet> for PerOwner {
    fn key_of(_: Origin<'_, Wallet>, input: &PerOwnerInput) -> Option<OwnerId> {
        match input {
            PerOwnerInput::Credited(e) => Some(OwnerId::new(e.owner.clone())),
            _ => None,                                   // belongs to no key
        }
    }
}
```

A source in `key_of(..)` is routed by its `KeyOf`. Any other source's id is the key.

`ViewCx` also tells you where the event came from:

```rust
cx.source::<Wallet>()   // Some(&WalletId) when a wallet recorded it
cx.at()                 // when it was recorded
cx.key()                // the key being updated
```

## Build on other views

A view can read another view's emitted events. Each arrives with that view's state after it.

```rust
#[derive(View)]
#[view(
    key = OwnerId,
    from(AccountSummary(AccountOpened, MoneyDeposited, MoneyWithdrawn)),
    key_of(AccountSummary),
    state = Holdings
)]
pub struct OwnerHoldings;

impl KeyOf<AccountSummary> for OwnerHoldings {
    fn key_of(from: Origin<'_, AccountSummary>, _: &OwnerHoldingsInput) -> Option<OwnerId> {
        from.state.owner.clone().map(OwnerId::new)   // MoneyDeposited has no owner; the summary does
    }
}

impl Apply for OwnerHoldings {
    fn apply(holdings: &mut Holdings, _: OwnerHoldingsInput, cx: &mut ViewCx<Self>) {
        let account = cx.source::<AccountSummary>().unwrap().clone();   // the summary's key
        let summary = cx.state::<AccountSummary>().unwrap();            // its state after the event
        holdings.balances.insert(account, summary.balance);
    }
}
```

```rust
bank.view::<OwnerHoldings>().get(&OwnerId::new("sam"))
// Some(Holdings { balances: {"sam-main": 70, "sam-savings": 500} })
```

| From an aggregate | From a view |
| --- | --- |
| `from.id`: the instance's id | `from.id`: the view's key |
| `from.state`: `()` | `from.state`: the key's state after the event |
| the events it records | the events it emits |

A view without `events(..)` emits every input it applies, so `OwnerHoldings` reads the summary's own inputs.

Views chain as deep as you like. Hosting a view hosts the views it reads first, so a model can list them in any order, or list only the last one. A view that reads itself through others is refused when the model is hosted.

## Put it in a model

```rust
#[derive(Model)]
#[model(aggregates(Account), views(AccountSummary, OwnerHoldings, Statement))]   // and policies(..), sagas(..)
pub struct Bank;

let bank = Memory::new().with::<Bank>();
```

A view added when events already exist receives them first, oldest first. So does a view built on views.

## Read it

```rust
let summaries = bank.view::<AccountSummary>();

summaries.get(&sam)                      // Some(Summary { owner: Some("sam"), balance: 70 })
summaries.get(&nobody)                   // None
summaries.snapshot(&sam)                 // Snapshot { state: Some(..), cursor: Cursor(3) }
bank.view::<Statement>().list(&sam, Cursor::START, 10)   // [Entry { cursor: 1, event, state }, ..]
```

## Read your own write

```rust
let receipt = bank.send(&sam, WithdrawMoney { amount: 30 })?;
let summary = summaries.get_at_least(&sam, &receipt)?;
```

`get_at_least` fails with `NotCaughtUp` when the view hasn't applied that version, or that stream doesn't reach it. A stream reaches `OwnerHoldings` through `AccountSummary`, so an account's receipt works there too.

## Follow it live

```rust
let feed = bank.view::<Statement>().events(&sam, Cursor::START);
```

```text
CaughtUp { cursor: 0 }                          history ends here
Entry #1: StatementLine { Deposit, 100, 100 }   then new entries, as they're recorded
Entry #2: StatementLine { Withdrawal, 30, 70 }
```

Render a snapshot, then follow from its cursor. Nothing arrives twice; nothing is missed:

```rust
let Snapshot { state, cursor } = summaries.snapshot(&sam);
render(state);
let feed = summaries.events(&sam, cursor);
```

To redraw a key's state whenever it changes, use a client's [`live`](client.md#follow-it-live).

## Use it from a UI

Feeds need no async runtime:

```rust
while let Some(frame) = feed.try_next() { .. }   // each frame, in an immediate-mode UI
let frame = feed.recv().await;                   // on any executor
```

Dropping a feed unsubscribes.

## Change a view

A stored view keeps its state between runs. When you change what it computes (its `apply`, `key_of`, state, or the events it reads), change its version:

```rust
#[view(key = AccountId, from(Account(..)), state = Summary, version = 1)]
pub struct AccountSummary;
```

On the next start it's rebuilt from the events, and so is every view that reads it:

```text
AccountSummary  stored at 0, now 1   rebuilt
OwnerHoldings   reads AccountSummary rebuilt
Statement       unchanged            loaded, then applies what it missed
```

Without a new version, the old state stays. If it no longer decodes, startup stops:

```text
AccountSummary's stored state doesn't decode (missing field `currency` ..); change its version to rebuild it
```

## Test it

```rust
Story::new()
    .with::<Bank>()
    .given(&a1(), [opened()])
    .when(&a1(), WithdrawMoney { amount: 30 })
    .rejected(InsufficientFunds { balance: 0, requested: 30 })
    .view::<AccountSummary>(&a1(), Summary { owner: Some("sam".into()), balance: 0 })
    .emitted::<Statement>(&a1(), Vec::<StatementLine>::new());
```

See [Stories](stories.md).

## Decide who may read it

```rust
#[view(key = AccountId, from(Account(..)), state = Summary, rejects(NotOwner))]
pub struct AccountSummary;

impl Validate for AccountSummary { .. }
```

See [Who is asking](sessions.md#guard-a-view).

## Guarantees

- A view applies each source's events in the order they were recorded, each once.
- Between sources there's no order. Use `cx.at()` for business time.
- A view that panics keeps its pending events; the next read or command applies them.
- Reading a view first applies what's pending in the views it reads.

Next: [Running a model](running.md).
