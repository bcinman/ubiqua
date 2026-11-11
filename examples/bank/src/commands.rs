use crate::AccountId;
use domain::{Command, Event, Rejection};
use serde::{Deserialize, Serialize};

// Open an account.

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = AccountId, events(AccountOpened), rejects(AlreadyOpen))]
pub struct OpenAccount {
    pub owner: String,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountOpened {
    pub owner: String,
}

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct AlreadyOpen;

// Deposit money.

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = AccountId, events(MoneyDeposited), rejects(NotOpen), reply = Balance)]
pub struct DepositMoney {
    pub amount: u64,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MoneyDeposited {
    pub amount: u64,
    pub balance_after: u64,
}

// Withdraw money.

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = AccountId, events(MoneyWithdrawn), rejects(NotOpen, InsufficientFunds, Frozen), reply = Balance)]
pub struct WithdrawMoney {
    pub amount: u64,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MoneyWithdrawn {
    pub amount: u64,
    pub balance_after: u64,
}

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct InsufficientFunds {
    pub balance: u64,
    pub requested: u64,
}

/// The fraud review froze the account.
#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct Frozen;

// Freeze and thaw: the fraud review's commands. They never reject, so they can be scheduled; each
// records nothing when it no longer applies.

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = AccountId, events(AccountFrozen))]
pub struct FreezeAccount;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountFrozen;

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = AccountId, events(AccountThawed))]
pub struct ThawAccount;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountThawed;

// Shared by deposits and withdrawals.

#[derive(Rejection, Clone, Debug, PartialEq)]
pub struct NotOpen;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    pub balance: u64,
}
