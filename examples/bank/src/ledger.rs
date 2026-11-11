//! Every movement of money on every account, in a store the bank owns rather than a view: the
//! kind of table an accountant queries across accounts. A projection writes it.

use crate::*;
use domain::Projection;
use domain_memory::{Project, ProjectionError, ProjectionInput};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Projection)]
#[projection(key = AccountId, from(Account(MoneyDeposited, MoneyWithdrawn)))]
pub struct Ledger {
    pub store: LedgerStore,
}

/// One movement of money.
#[derive(Clone, Debug, PartialEq)]
pub struct LedgerRow {
    pub account: AccountId,
    pub kind: LineKind,
    pub amount: u64,
    pub balance_after: u64,
}

/// Stands in for a database table. Rows are keyed by the event they came from, so writing one
/// again changes nothing.
#[derive(Clone, Default)]
pub struct LedgerStore(Arc<Mutex<BTreeMap<(String, u64), LedgerRow>>>);

impl LedgerStore {
    pub fn record(&self, rows: impl IntoIterator<Item = ((String, u64), LedgerRow)>) {
        let mut table = self.0.lock().unwrap();
        for (id, row) in rows {
            table.entry(id).or_insert(row);
        }
    }

    pub fn for_account(&self, account: &AccountId) -> Vec<LedgerRow> {
        self.0.lock().unwrap().values().filter(|row| row.account == *account).cloned().collect()
    }
}

impl Project for Ledger {
    async fn project(&self, account: &AccountId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> {
        self.store.record(batch.into_iter().map(|input| {
            let (kind, amount, balance_after) = match input.event {
                LedgerInput::MoneyDeposited(e) => (LineKind::Deposit, e.amount, e.balance_after),
                LedgerInput::MoneyWithdrawn(e) => (LineKind::Withdrawal, e.amount, e.balance_after),
            };
            ((input.stream.to_string(), input.seq), LedgerRow { account: account.clone(), kind, amount, balance_after })
        }));
        Ok(())
    }
}
