use crate::*;
use domain::{Apply, Event, KeyOf, Origin, View, ViewCx};
use std::collections::BTreeMap;

/// Who owns an account and what's in it.
#[derive(View)]
#[view(key = AccountId, from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)), state = Summary)]
pub struct AccountSummary;

#[derive(Clone, Debug, Default, PartialEq)]
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
    state = Holdings
)]
pub struct OwnerHoldings;

#[derive(Clone, Debug, Default, PartialEq)]
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

/// One line per movement of money.
#[derive(View)]
#[view(key = AccountId, from(Account(MoneyDeposited, MoneyWithdrawn)), events(StatementLine))]
pub struct Statement;

#[derive(Event, Clone, Debug, PartialEq)]
pub struct StatementLine {
    pub kind: LineKind,
    pub amount: u64,
    pub balance: u64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LineKind {
    Deposit,
    Withdrawal,
}

impl Apply for Statement {
    fn apply(_: &mut (), input: StatementInput, cx: &mut ViewCx<Self>) {
        cx.emit(match input {
            StatementInput::MoneyDeposited(e) => {
                StatementLine { kind: LineKind::Deposit, amount: e.amount, balance: e.balance_after }
            }
            StatementInput::MoneyWithdrawn(e) => {
                StatementLine { kind: LineKind::Withdrawal, amount: e.amount, balance: e.balance_after }
            }
        });
    }
}
