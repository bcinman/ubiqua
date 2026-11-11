# Aggregates

An aggregate guards a rule: "a balance never goes below zero". It holds just the state that rule needs.

```rust
#[derive(Aggregate)]
#[aggregate(
    id = AccountId,
    state = AccountState,
    events(AccountOpened, MoneyDeposited, MoneyWithdrawn),
    commands(OpenAccount, DepositMoney, WithdrawMoney),
)]
pub struct Account;
```

The derive generates `AccountEvent`, an enum of the listed events, and checks that `Account` implements `Evolve` and a `Decide` for each command.

## State

```rust
#[derive(Clone, Debug, Default, PartialEq)]
pub enum AccountState {
    #[default]
    Unopened,                                  // every id starts here
    Open { owner: String, balance: u64 },
}
```

`Default` is the state before the first event. There's no "create": opening is the first command an account accepts.

## evolve: apply a fact

```rust
impl Evolve for Account {
    fn evolve(state: &mut AccountState, event: &AccountEvent) {
        match (state, event) {
            (state, AccountEvent::AccountOpened(e)) => {
                *state = AccountState::Open { owner: e.owner.clone(), balance: 0 };
            }
            (AccountState::Open { balance, .. }, AccountEvent::MoneyDeposited(e)) => *balance = e.balance_after,
            (AccountState::Open { balance, .. }, AccountEvent::MoneyWithdrawn(e)) => *balance = e.balance_after,
            _ => {}
        }
    }
}
```

Never fails, never panics, never does arithmetic the event already did.

## decide: one per command

```rust
impl Decide<WithdrawMoney> for Account {
    fn decide(state: &AccountState, cmd: &WithdrawMoney, cx: &Cx) -> Decision<WithdrawMoney> {
        let balance = open(state)?;
        if cmd.amount > balance {
            return Err(InsufficientFunds { balance, requested: cmd.amount }.into());
        }
        Ok(vec![MoneyWithdrawn { amount: cmd.amount, balance_after: balance - cmd.amount }.into()])
    }

    fn reply(state: &AccountState) -> Balance {
        Balance { balance: open(state).unwrap_or(0) }
    }
}
```

Three possible answers:

| Return | Means |
| --- | --- |
| `Ok(vec![event, ..])` | accepted, record these |
| `Ok(vec![])` | accepted, nothing to record (already in that state) |
| `Err(rejection.into())` | refused, nothing recorded |

`reply` runs after `evolve`, on the new state.

## Share a check with `?`

```rust
fn open(state: &AccountState) -> Result<u64, NotOpen> {
    match state {
        AccountState::Open { balance, .. } => Ok(*balance),
        AccountState::Unopened => Err(NotOpen),
    }
}

let balance = open(state)?;   // NotOpen becomes WithdrawMoneyRejection::NotOpen
```

Works in any `decide` whose command lists `NotOpen` in `rejects`.

## Read the clock

```rust
fn decide(state: &Lamp, _: &Toggle, cx: &Cx) -> Decision<Toggle> {
    Ok(vec![Switched { on: !state.on, at: cx.now() }.into()])
}
```

`cx.now()` is the time the command is decided. Never read the system clock in `decide`: stories control `cx.now()`.

## One command at a time

```text
WithdrawMoney 80 ─┐
                  ├─▶ Account a-1 (balance 100) ─▶ MoneyWithdrawn, balance 20
WithdrawMoney 80 ─┘                             ─▶ InsufficientFunds { balance: 20 }
```

Each instance decides one command at a time. The second withdrawal sees the balance the first one left.

## Draw it small

A rule that spans instances gets its own aggregate. "Usernames are unique" is about one username:

```rust
#[derive(Aggregate)]
#[aggregate(id = Username, state = Claim, events(UsernameClaimed), commands(ClaimUsername))]
pub struct UsernameClaim;
```

Next: [Views](views.md).
