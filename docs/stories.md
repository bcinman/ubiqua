# Stories

Given this history, when this command, then these events.

```rust
use domain_story::Story;

#[test]
fn an_overdraft_is_rejected_with_the_evidence() {
    Story::new()
        .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 50, balance_after: 50 }.into()])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 50, requested: 80 });
}
```

## Steps

| Step | Checks |
| --- | --- |
| `.given(&id, [events])` | nothing; writes history without deciding |
| `.when(&id, command)` | sends the command |
| `.then(&id, [events])` | the events recorded on `id` since the last check |
| `.then_none(&id)` | no events recorded on `id` |
| `.rejected(r)` | the command was refused with exactly `r` |
| `.replied(reply)` | the command's reply |
| `.at(timestamp)` | sets the clock for the next command |
| `.view::<V>(&key, state)` | a view's state for one key |
| `.emitted::<V>(&key, [events])` | every event a view emitted for one key |

## Success

```rust
Story::new()
    .given(&a1(), [opened()])
    .when(&a1(), DepositMoney { amount: 100 })
    .then(&a1(), [MoneyDeposited { amount: 100, balance_after: 100 }])
    .replied(Balance { balance: 100 });
```

## Several commands

```rust
Story::new()
    .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 100, balance_after: 100 }.into()])
    .when(&a1(), WithdrawMoney { amount: 80 })
    .then(&a1(), [MoneyWithdrawn { amount: 80, balance_after: 20 }])
    .when(&a1(), WithdrawMoney { amount: 80 })
    .rejected(InsufficientFunds { balance: 20, requested: 80 });
```

## Views

```rust
Story::new()
    .with::<Bank>()                                  // run the model's views
    .given(&a1(), [opened()])
    .when(&a1(), DepositMoney { amount: 100 })
    .then(&a1(), [MoneyDeposited { amount: 100, balance_after: 100 }])
    .when(&a1(), WithdrawMoney { amount: 30 })
    .then(&a1(), [MoneyWithdrawn { amount: 30, balance_after: 70 }])
    .view::<AccountSummary>(&a1(), Summary { owner: Some("sam".into()), balance: 70 })
    .emitted::<Statement>(&a1(), [
        StatementLine { kind: LineKind::Deposit, amount: 100, balance: 100 },
        StatementLine { kind: LineKind::Withdrawal, amount: 30, balance: 70 },
    ]);
```

`given` events reach the views too.

## Time

```rust
Story::new()
    .at(Timestamp::from_millis(5))
    .when(&lamp(), Toggle)
    .then(&lamp(), [Switched { on: true, at: Timestamp::from_millis(5) }]);
```

## Everything must be checked

A story fails when a command does something no step checks:

```rust
Story::new().when(&lamp(), Toggle).replied(true);
// panicked: story: Toggle left unchecked: 1 event(s) on Light/hall

Story::new().given(&lamp(), [Smashed]).when(&lamp(), Toggle);
// panicked: story: Toggle left unchecked: rejection Broken; check it with .rejected(..)
```

Each command is checked before the next `when` or `given`.

## Only outcomes the command can have

```rust
.when(&a1(), DepositMoney { amount: 1 })
.rejected(InsufficientFunds { .. })
// error: the trait bound `DepositMoneyRejection: From<InsufficientFunds>` is not satisfied

.replied(5u64)
// error[E0308]: mismatched types
```

## One story per rule

```rust
#[test] fn an_account_opens_once() { .. }
#[test] fn an_unopened_account_refuses_deposits() { .. }
#[test] fn an_overdraft_is_rejected_with_the_evidence() { .. }
```

For each command: one story for success, one per rejection.

Next: [Compile-time checks](compile-time-checks.md).
