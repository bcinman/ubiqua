use crate::AccountId;
use domain::{Command, Event, Rejection};

// Open an account.

#[derive(Command, Debug)]
#[command(target = AccountId, events(AccountOpened), rejects(AlreadyOpen))]
pub struct OpenAccount {
    pub owner: String,
}

#[derive(Event, Clone, Debug, PartialEq)]
pub struct AccountOpened {
    pub owner: String,
}

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct AlreadyOpen;

// Deposit money.

#[derive(Command, Debug)]
#[command(target = AccountId, events(MoneyDeposited), rejects(NotOpen), reply = Balance)]
pub struct DepositMoney {
    pub amount: u64,
}

#[derive(Event, Clone, Debug, PartialEq)]
pub struct MoneyDeposited {
    pub amount: u64,
    pub balance_after: u64,
}

// Withdraw money.

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

// Shared by deposits and withdrawals.

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct NotOpen;

#[derive(Clone, Debug, PartialEq)]
pub struct Balance {
    pub balance: u64,
}
