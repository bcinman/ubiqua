use crate::*;
use domain::{Apply, Event, KeyOf, Origin, ReadCx, Validate, View, ViewCx};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Who owns an account and what's in it. Only its owner may read it.
#[derive(View)]
#[view(
    key = AccountId,
    from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)),
    state = Summary,
    rejects(NotOwner)
)]
pub struct AccountSummary;

impl Validate for AccountSummary {
    fn validate(cx: &ReadCx, _: &AccountId, summary: Option<&Summary>) -> Result<(), AccountSummaryRejection> {
        // No owner yet reads as empty, so a new user can watch for their account to open.
        Ok(must_own(cx.actor::<User>(), summary.and_then(|s| s.owner.as_deref()))?)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub owner: Option<String>,
    pub balance: u64,
}

impl Apply for AccountSummary {
    fn apply(summary: &mut Summary, input: AccountSummaryInput, _: &mut ViewCx<Self>) {
        match input {
            AccountSummaryInput::AccountOpened(e) => summary.owner = Some(e.owner),
            AccountSummaryInput::MoneyDeposited(e) => summary.balance = e.balance_after,
            AccountSummaryInput::MoneyWithdrawn(e) => summary.balance = e.balance_after,
        }
    }
}

/// Every account an owner holds, with its balance. Built from the summaries, which know the owner.
#[derive(View)]
#[view(
    key = OwnerId,
    from(AccountSummary(AccountOpened, MoneyDeposited, MoneyWithdrawn)),
    key_of(AccountSummary),
    state = Holdings,
    rejects(NotOwner)
)]
pub struct OwnerHoldings;

impl Validate for OwnerHoldings {
    fn validate(cx: &ReadCx, owner: &OwnerId, _: Option<&Holdings>) -> Result<(), OwnerHoldingsRejection> {
        Ok(must_own(cx.actor::<User>(), Some(owner.as_str()))?)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Holdings {
    pub balances: BTreeMap<AccountId, u64>,
}

impl Holdings {
    pub fn total(&self) -> u64 {
        self.balances.values().sum()
    }
}

impl KeyOf<AccountSummary> for OwnerHoldings {
    fn key_of(from: Origin<'_, AccountSummary>, _: &OwnerHoldingsInput) -> Option<OwnerId> {
        from.state.owner.clone().map(OwnerId::new)
    }
}

impl Apply for OwnerHoldings {
    fn apply(holdings: &mut Holdings, _: OwnerHoldingsInput, cx: &mut ViewCx<Self>) {
        let account = cx.source::<AccountSummary>().expect("reads summaries only").clone();
        let summary = cx.state::<AccountSummary>().expect("reads summaries only");
        holdings.balances.insert(account, summary.balance);
    }
}

/// One line per movement of money. Only the account's owner may read it, so it keeps the owner.
#[derive(View)]
#[view(
    key = AccountId,
    from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)),
    state = Option<String>,
    events(StatementLine),
    rejects(NotOwner),
    version = 1
)]
pub struct Statement;

impl Validate for Statement {
    fn validate(cx: &ReadCx, _: &AccountId, owner: Option<&Option<String>>) -> Result<(), StatementRejection> {
        Ok(must_own(cx.actor::<User>(), owner.and_then(Option::as_deref))?)
    }
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatementLine {
    pub kind: LineKind,
    pub amount: u64,
    pub balance: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum LineKind {
    Deposit,
    Withdrawal,
}

impl Apply for Statement {
    fn apply(owner: &mut Option<String>, input: StatementInput, cx: &mut ViewCx<Self>) {
        cx.emit(match input {
            StatementInput::AccountOpened(e) => {
                *owner = Some(e.owner);
                return;
            }
            StatementInput::MoneyDeposited(e) => {
                StatementLine { kind: LineKind::Deposit, amount: e.amount, balance: e.balance_after }
            }
            StatementInput::MoneyWithdrawn(e) => {
                StatementLine { kind: LineKind::Withdrawal, amount: e.amount, balance: e.balance_after }
            }
        });
    }
}
