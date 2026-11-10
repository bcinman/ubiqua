# Getting started

Build a bank account that can't be overdrawn. The finished code is in [`examples/bank`](../examples/bank).

```sh
cargo run -p bank
cargo test -p bank
```

## 1. Depend on the crates

```toml
[dependencies]
domain = { path = "../../crates/domain" }
domain-memory = { path = "../../crates/domain-memory" }

[dev-dependencies]
domain-story = { path = "../../crates/domain-story" }
```

## 2. Name the id

```rust
domain::id!(pub struct AccountId;);

let id = AccountId::new("sam-main");
```

## 3. Declare a command and its outcomes

```rust
#[derive(Command, Debug)]
#[command(target = AccountId, events(MoneyWithdrawn), rejects(NotOpen, InsufficientFunds), reply = Balance)]
pub struct WithdrawMoney {
    pub amount: u64,
}

#[derive(Event, Clone, Debug, PartialEq)]
pub struct MoneyWithdrawn {
    pub amount: u64,
    pub balance_after: u64,
}

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct InsufficientFunds {
    pub balance: u64,
    pub requested: u64,
}
```

`OpenAccount` and `DepositMoney` follow the same shape: see [`commands.rs`](../examples/bank/src/commands.rs).

## 4. Write the aggregate

```rust
#[derive(Aggregate)]
#[aggregate(
    id = AccountId,
    state = AccountState,
    events(AccountOpened, MoneyDeposited, MoneyWithdrawn),
    commands(OpenAccount, DepositMoney, WithdrawMoney),
)]
pub struct Account;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum AccountState {
    #[default]
    Unopened,
    Open { owner: String, balance: u64 },
}

impl Decide<WithdrawMoney> for Account {
    fn decide(state: &AccountState, cmd: &WithdrawMoney, _: &Cx) -> Decision<WithdrawMoney> {
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

`Evolve`, `open` and the other two `Decide`s are in [`account.rs`](../examples/bank/src/account.rs).

## 5. Send commands

```rust
let bank = Memory::new();
let sam = AccountId::new("sam-main");

bank.send(&sam, OpenAccount { owner: "sam".into() }).unwrap();
let receipt = bank.send(&sam, DepositMoney { amount: 100 }).unwrap();
println!("balance {}", receipt.reply.balance);
```

```text
$ cargo run -p bank
deposited 100, balance 100
withdrew 30, balance 70
withdraw 500: rejected: InsufficientFunds { balance: 70, requested: 500 }
Account/sam-main #1: AccountOpened(AccountOpened { owner: "sam" })
Account/sam-main #2: MoneyDeposited(MoneyDeposited { amount: 100, balance_after: 100 })
Account/sam-main #3: MoneyWithdrawn(MoneyWithdrawn { amount: 30, balance_after: 70 })
```

## 6. Test it with a story

```rust
#[test]
fn an_overdraft_is_rejected_with_the_evidence() {
    Story::new()
        .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 50, balance_after: 50 }.into()])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 50, requested: 80 });
}
```

Next: [Commands and events](commands.md).
