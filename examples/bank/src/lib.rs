//! A bank account: open it, deposit, withdraw, and never overdraw.

mod account;
mod commands;
mod fraud;
mod ledger;
mod transfers;
mod users;
mod views;

pub use account::{Account, AccountEvent, AccountState};
pub use commands::*;
pub use fraud::*;
pub use ledger::*;
pub use transfers::*;
pub use users::*;
pub use views::*;

domain::id!(
    pub struct AccountId;
);
domain::id!(
    pub struct OwnerId;
);
domain::id!(
    pub struct TransferId;
);

/// Everything that runs together. The fraud review also needs a `FraudCheck` adapter.
#[derive(domain::Model)]
#[model(
    aggregates(Account, Transfer),
    views(AccountSummary, OwnerHoldings, Statement),
    policies(FraudReview, TransferExecution),
    sagas(ExecuteTransfer)
)]
pub struct Bank;

/// What an app may do with a bank.
#[derive(domain::Api)]
#[api(
    commands(OpenAccount, DepositMoney, WithdrawMoney, RequestTransfer),
    views(AccountSummary, OwnerHoldings, Statement)
)]
pub struct BankApi;
