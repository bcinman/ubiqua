//! A bank account: open it, deposit, withdraw, and never overdraw.

mod account;
mod commands;
mod fraud;
mod views;

pub use account::{Account, AccountEvent, AccountState};
pub use commands::*;
pub use fraud::*;
pub use views::*;

domain::id!(
    pub struct AccountId;
);
domain::id!(
    pub struct OwnerId;
);

/// Everything that runs together.
#[derive(domain::Model)]
#[model(aggregates(Account), views(AccountSummary, OwnerHoldings, Statement))]
pub struct Bank;

/// What an app may do with a bank.
#[derive(domain::Api)]
#[api(commands(OpenAccount, DepositMoney, WithdrawMoney), views(AccountSummary, OwnerHoldings, Statement))]
pub struct BankApi;
