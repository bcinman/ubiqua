//! Whenever someone withdraws a large amount, score it for fraud. A risky withdrawal freezes the
//! account for a day.

use crate::*;
use domain_memory::prelude::*;
use std::time::Duration;

/// Withdrawals from this amount are scored.
pub const SCORED_FROM: u64 = 500;

/// How long a risky withdrawal freezes the account.
pub const FREEZE_FOR: Duration = Duration::from_secs(24 * 60 * 60);

/// Asks the fraud scorer how risky a withdrawal is.
#[domain::call(system = FraudCheck, returns = Risk)]
pub struct ScoreWithdrawal {
    pub account: AccountId,
    pub amount: u64,
}

/// From 0, safe, to 100.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Risk(pub u8);

/// The fraud-scoring service, in the bank's words. An adapter talks to the vendor.
#[domain::external_system(calls(ScoreWithdrawal))]
pub struct FraudCheck;

#[domain::policy(from(Account(MoneyWithdrawn)))]
#[derive(Default)]
pub struct FraudReview;

impl React for FraudReview {
    async fn react(&self, cx: &PolicyCx<Self>, input: FraudReviewInput) -> Result<(), PolicyError> {
        let FraudReviewInput::MoneyWithdrawn(withdrawn) = input;
        if withdrawn.amount < SCORED_FROM {
            return Ok(());
        }
        let account = cx.source_id().clone();
        let Risk(risk) = cx.call(ScoreWithdrawal { account: account.clone(), amount: withdrawn.amount }).await?;
        if risk >= 90 {
            cx.send(&account, FreezeAccount)?;
            cx.schedule(&account, ThawAccount, cx.now().after(FREEZE_FOR))?;
        }
        Ok(())
    }
}
