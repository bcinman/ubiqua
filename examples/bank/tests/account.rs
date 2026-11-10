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
fn an_overdraft_is_rejected_with_the_evidence() {
    Story::new()
        .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 50, balance_after: 50 }.into()])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 50, requested: 80 });
}

#[test]
fn each_withdrawal_is_decided_against_the_balance_the_last_one_left() {
    Story::new()
        .given(&a1(), [AccountEvent::from(opened()), MoneyDeposited { amount: 100, balance_after: 100 }.into()])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .then(&a1(), [MoneyWithdrawn { amount: 80, balance_after: 20 }])
        .when(&a1(), WithdrawMoney { amount: 80 })
        .rejected(InsufficientFunds { balance: 20, requested: 80 });
}
