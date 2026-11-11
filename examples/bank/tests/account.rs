use bank::*;
use domain_story::Story;

fn a1() -> AccountId {
    AccountId::new("a-1")
}

fn opened() -> AccountOpened {
    AccountOpened { owner: "sam".into() }
}

#[test]
fn an_account_opens_once() {
    Story::new().given(&a1(), [opened()]).when(&a1(), OpenAccount { owner: "kim".into() }).rejected(AlreadyOpen);
}

#[test]
fn deposits_into_an_open_account_and_replies_with_the_balance() {
    Story::new()
        .given(&a1(), [opened()])
        .when(&a1(), DepositMoney { amount: 100 })
        .then(&a1(), [MoneyDeposited { amount: 100, balance_after: 100 }])
        .replied(Balance { balance: 100 });
}

#[test]
fn an_unopened_account_refuses_deposits() {
    Story::new().when(&a1(), DepositMoney { amount: 100 }).rejected(NotOpen).then_none(&a1());
}

#[test]
fn each_withdrawal_is_decided_against_the_balance_the_last_one_left() {
    Story::new()
        .given(&a1(), (opened(), MoneyDeposited { amount: 100, balance_after: 100 }))
        .when(&a1(), WithdrawMoney { amount: 80 })
        .then(&a1(), [MoneyWithdrawn { amount: 80, balance_after: 20 }])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 20, requested: 80 });
}

#[test]
fn the_summary_and_statement_follow_the_account() {
    Story::new()
        .with::<Bank>()
        .given(&a1(), [opened()])
        .when(&a1(), DepositMoney { amount: 100 })
        .then(&a1(), [MoneyDeposited { amount: 100, balance_after: 100 }])
        .when(&a1(), WithdrawMoney { amount: 30 })
        .then(&a1(), [MoneyWithdrawn { amount: 30, balance_after: 70 }])
        .view::<AccountSummary>(&a1(), Summary { owner: Some("sam".into()), balance: 70 })
        .emitted::<Statement>(
            &a1(),
            [
                StatementLine { kind: LineKind::Deposit, amount: 100, balance: 100 },
                StatementLine { kind: LineKind::Withdrawal, amount: 30, balance: 70 },
            ],
        );
}

#[test]
fn a_story_with_the_model_runs_only_the_policies_it_adds() {
    // The fraud review is in the model; without `.policy(FraudReview)` it doesn't score this.
    Story::new()
        .with::<Bank>()
        .given(&a1(), (opened(), MoneyDeposited { amount: 1000, balance_after: 1000 }))
        .when(&a1(), WithdrawMoney { amount: 900 })
        .then(&a1(), [MoneyWithdrawn { amount: 900, balance_after: 100 }]);
}

#[test]
fn an_owner_sees_every_account_they_hold() {
    let savings = AccountId::new("a-2");
    Story::new()
        .with::<Bank>()
        .given(&a1(), (opened(), MoneyDeposited { amount: 50, balance_after: 50 }))
        .given(&savings, [opened()])
        .when(&savings, DepositMoney { amount: 200 })
        .then(&savings, [MoneyDeposited { amount: 200, balance_after: 200 }])
        .view::<OwnerHoldings>(
            &OwnerId::new("sam"),
            Holdings { balances: [(a1(), 50), (savings.clone(), 200)].into_iter().collect() },
        );
}

#[test]
fn a_risky_withdrawal_freezes_the_account_for_a_day() {
    let day = FREEZE_FOR;
    Story::new()
        .policy(FraudReview)
        .given(&a1(), (opened(), MoneyDeposited { amount: 1000, balance_after: 1000 }))
        .when(&a1(), WithdrawMoney { amount: 900 })
        .then(&a1(), [MoneyWithdrawn { amount: 900, balance_after: 100 }])
        .call(ScoreWithdrawal { account: a1(), amount: 900 })
        .succeed(Risk(90))
        .then(&a1(), [AccountFrozen])
        .when(&a1(), WithdrawMoney { amount: 10 })
        .rejected(Frozen)
        .advance(day)
        .then(&a1(), [AccountThawed])
        .when(&a1(), WithdrawMoney { amount: 10 })
        .then(&a1(), [MoneyWithdrawn { amount: 10, balance_after: 90 }]);
}

#[test]
fn small_withdrawals_are_not_scored() {
    Story::new()
        .policy(FraudReview)
        .given(&a1(), (opened(), MoneyDeposited { amount: 1000, balance_after: 1000 }))
        .when(&a1(), WithdrawMoney { amount: SCORED_FROM - 1 })
        .then(&a1(), [MoneyWithdrawn { amount: SCORED_FROM - 1, balance_after: 1001 - SCORED_FROM }])
        .advance(FREEZE_FOR)
        .then_none(&a1());
}

#[test]
fn a_withdrawal_of_the_scored_amount_is_scored_and_a_risk_below_90_changes_nothing() {
    Story::new()
        .policy(FraudReview)
        .given(&a1(), (opened(), MoneyDeposited { amount: 1000, balance_after: 1000 }))
        .when(&a1(), WithdrawMoney { amount: SCORED_FROM })
        .then(&a1(), [MoneyWithdrawn { amount: SCORED_FROM, balance_after: 1000 - SCORED_FROM }])
        .call(ScoreWithdrawal { account: a1(), amount: SCORED_FROM })
        .succeed(Risk(89))
        .advance(FREEZE_FOR)
        .then_none(&a1());
}

#[test]
fn freezing_and_thawing_record_nothing_when_they_no_longer_apply() {
    Story::new()
        .given(&a1(), (opened(), AccountFrozen))
        .when(&a1(), FreezeAccount)
        .then_none(&a1())
        .when(&a1(), ThawAccount)
        .then(&a1(), [AccountThawed])
        .when(&a1(), ThawAccount)
        .then_none(&a1());
}

#[test]
fn a_refund_to_an_unopened_account_records_nothing() {
    Story::new().when(&a1(), RefundMoney { amount: 30 }).replied(Balance { balance: 0 }).then_none(&a1());
}

#[test]
fn only_the_owner_may_withdraw() {
    Story::new()
        .given(&a1(), (opened(), MoneyDeposited { amount: 100, balance_after: 100 }))
        .acting_as(User("kim".into()))
        .when(&a1(), WithdrawMoney { amount: 10 })
        .rejected(NotOwner)
        .acting_as(User("sam".into()))
        .when(&a1(), WithdrawMoney { amount: 10 })
        .then(&a1(), [MoneyWithdrawn { amount: 10, balance_after: 90 }]);
}
