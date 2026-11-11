use crate::{AccountId, NotOwner};
use serde::{Deserialize, Serialize};

// Open an account.

#[domain::command(target = AccountId, events(AccountOpened), rejects(AlreadyOpen, NotOwner))]
pub struct OpenAccount {
    pub owner: String,
}

#[domain::event]
pub struct AccountOpened {
    pub owner: String,
}

#[domain::rejection]
pub struct AlreadyOpen;

// Deposit money.

#[domain::command(target = AccountId, events(MoneyDeposited), rejects(NotOpen), reply = Balance)]
#[derive(Clone)]
pub struct DepositMoney {
    pub amount: u64,
}

#[domain::event]
pub struct MoneyDeposited {
    pub amount: u64,
    pub balance_after: u64,
}

// Withdraw money.

#[domain::command(target = AccountId, events(MoneyWithdrawn), rejects(NotOpen, NotOwner, InsufficientFunds, Frozen), reply = Balance)]
#[derive(Clone)]
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

/// The fraud review froze the account.
#[domain::rejection]
pub struct Frozen;

// Freeze and thaw: the fraud review's commands. They never reject, so they can be scheduled; each
// records nothing when it no longer applies.

#[domain::command(target = AccountId, events(AccountFrozen))]
pub struct FreezeAccount;

#[domain::event]
pub struct AccountFrozen;

#[domain::command(target = AccountId, events(AccountThawed))]
pub struct ThawAccount;

#[domain::event]
pub struct AccountThawed;

/// Gives back money a transfer took, when the transfer can't finish. It never rejects, so undoing
/// a transfer can't get stuck; it records nothing on an account that isn't open.
#[domain::command(target = AccountId, events(MoneyDeposited), reply = Balance)]
pub struct RefundMoney {
    pub amount: u64,
}

// Shared by deposits and withdrawals.

#[domain::rejection]
pub struct NotOpen;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Balance {
    pub balance: u64,
}
