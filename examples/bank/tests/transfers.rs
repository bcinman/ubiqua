use bank::*;
use domain_memory::SagaStatus;
use domain_story::Story;

fn sam() -> AccountId {
    AccountId::new("sam-main")
}

fn kim() -> AccountId {
    AccountId::new("kim-main")
}

fn t1() -> TransferId {
    TransferId::new("t-1")
}

fn bank() -> Story {
    Story::new()
        .policy(TransferExecution)
        .saga::<ExecuteTransfer>()
        .given(&sam(), (AccountOpened { owner: "sam".into() }, MoneyDeposited { amount: 100, balance_after: 100 }))
}

fn request(amount: u64) -> RequestTransfer {
    RequestTransfer { from: sam(), to: kim(), amount }
}

fn requested(amount: u64) -> TransferRequested {
    TransferRequested { from: sam(), to: kim(), amount, by: None }
}

fn failed(why: impl ToString) -> TransferEvent {
    TransferFailed { reason: why.to_string() }.into()
}

#[test]
fn a_transfer_moves_the_money_and_completes() {
    bank()
        .given(&kim(), [AccountOpened { owner: "kim".into() }])
        .when(&t1(), request(30))
        .then(&t1(), (requested(30), TransferCompleted))
        .then(&sam(), [MoneyWithdrawn { amount: 30, balance_after: 70 }])
        .then(&kim(), [MoneyDeposited { amount: 30, balance_after: 30 }])
        .saga_status::<ExecuteTransfer>(&t1(), SagaStatus::Completed);
}

#[test]
fn a_refused_credit_gives_the_money_back() {
    bank()
        .when(&t1(), request(30))
        .then(&t1(), (requested(30), failed(DestinationClosed)))
        .then(
            &sam(),
            (MoneyWithdrawn { amount: 30, balance_after: 70 }, MoneyDeposited { amount: 30, balance_after: 100 }),
        )
        .then_none(&kim())
        .saga_status::<ExecuteTransfer>(&t1(), SagaStatus::Rejected(DestinationClosed.into()));
}

#[test]
fn a_refused_debit_takes_nothing() {
    let short = InsufficientFunds { balance: 100, requested: 500 };
    bank()
        .given(&kim(), [AccountOpened { owner: "kim".into() }])
        .when(&t1(), request(500))
        .then(&t1(), (requested(500), failed(short.clone())))
        .then_none(&sam())
        .then_none(&kim())
        .saga_status::<ExecuteTransfer>(&t1(), SagaStatus::Rejected(short.into()));
}

#[test]
fn only_the_owner_can_move_their_money() {
    bank()
        .given(&kim(), [AccountOpened { owner: "kim".into() }])
        .acting_as(User("kim".into()))
        .when(&t1(), request(30))
        .then(&t1(), (TransferRequested { by: Some("kim".into()), ..requested(30) }, failed(NotOwner)))
        .saga_status::<ExecuteTransfer>(&t1(), SagaStatus::Rejected(NotOwner.into()));
}

#[test]
fn the_ledger_records_every_movement_once() {
    let store = LedgerStore::default();
    Story::new()
        .projection(Ledger { store: store.clone() })
        .given(&sam(), (AccountOpened { owner: "sam".into() }, MoneyDeposited { amount: 100, balance_after: 100 }))
        .when(&sam(), WithdrawMoney { amount: 40 })
        .then(&sam(), [MoneyWithdrawn { amount: 40, balance_after: 60 }]);
    let row = |kind, amount, balance_after| LedgerRow { account: sam(), kind, amount, balance_after };
    assert_eq!(store.for_account(&sam()), [row(LineKind::Deposit, 100, 100), row(LineKind::Withdrawal, 40, 60)]);
}
