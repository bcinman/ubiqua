//! A bank account: open it, deposit, withdraw, and never overdraw.

mod account;
mod commands;

pub use account::{Account, AccountEvent, AccountState};
pub use commands::*;

domain::id!(
    pub struct AccountId;
);
