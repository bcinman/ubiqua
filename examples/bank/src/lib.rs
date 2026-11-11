//! A bank account: open it, deposit, withdraw, and never overdraw.

mod account;
mod commands;
mod views;

pub use account::{Account, AccountEvent, AccountState};
pub use commands::*;
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
