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
domain-client = { path = "../../crates/domain-client" }
domain-memory = { path = "../../crates/domain-memory" }
domain-sqlite = { path = "../../crates/domain-sqlite" }
serde = { version = "1", features = ["derive"] }

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
#[domain::command(target = AccountId, events(MoneyWithdrawn), rejects(NotOpen, InsufficientFunds), reply = Balance)]
pub struct WithdrawMoney {
    pub amount: u64,
}

#[domain::event]
pub struct MoneyWithdrawn {
    pub amount: u64,
    pub balance_after: u64,
}

#[domain::rejection]
pub struct InsufficientFunds {
    pub balance: u64,
    pub requested: u64,
}
```

Events are stored and rejections cross the network, so each attribute also derives `Serialize` and `Deserialize`, with `Clone`, `Debug` and `PartialEq` where they're needed. Replies and view state are plain types: derive `Serialize` and `Deserialize` on them yourself.

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
}

impl Reply<Account> for WithdrawMoney {
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
showing: None
redraw #1: Some(Summary { owner: Some("sam"), balance: 0 })
redraw #2: Some(Summary { owner: Some("sam"), balance: 100 })
redraw #3: Some(Summary { owner: Some("sam"), balance: 70 })
withdraw 500: rejected: InsufficientFunds { balance: 70, requested: 500 }
statement #1: StatementLine(StatementLine { kind: Deposit, amount: 100, balance: 100 })
statement #2: StatementLine(StatementLine { kind: Withdrawal, amount: 30, balance: 70 })
```

The example sends through a [client](client.md), as an app would. `cargo run -p bank -- bank.db` [keeps it on disk](storage.md).

## 6. Add a view

```rust
#[derive(View)]
#[view(key = AccountId, from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)), state = Summary)]
pub struct AccountSummary;

impl Apply for AccountSummary {
    fn apply(summary: &mut Summary, input: AccountSummaryInput, _: &mut ViewCx<Self>) {
        match input {
            AccountSummaryInput::AccountOpened(e) => summary.owner = Some(e.owner),
            AccountSummaryInput::MoneyDeposited(e) => summary.balance = e.balance_after,
            AccountSummaryInput::MoneyWithdrawn(e) => summary.balance = e.balance_after,
        }
    }
}

#[derive(Model)]
#[model(aggregates(Account), views(AccountSummary, OwnerHoldings, Statement))]
pub struct Bank;
```

```rust
let bank = Memory::new().with::<Bank>();
let receipt = bank.send(&sam, WithdrawMoney { amount: 30 }).unwrap();
let summary = bank.view::<AccountSummary>().at_least(&receipt).unwrap().get(&sam);
```

`Statement`, `OwnerHoldings` (built on the summaries) and the rest are in [`views.rs`](../examples/bank/src/views.rs). More in [Views](views.md).

## 7. Test it with a story

```rust
#[test]
fn an_overdraft_is_rejected_with_the_evidence() {
    Story::new()
        .given(&a1(), (opened(), MoneyDeposited { amount: 50, balance_after: 50 }))
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 50, requested: 80 });
}
```

Next: [Commands and events](commands.md).
