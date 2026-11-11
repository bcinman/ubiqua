use domain::{Aggregate, Call, Cx, Decide, Decision, Evolve, ExternalSystem, Policy, Timestamp};
use domain_memory::{PolicyCx, PolicyError, React};
use domain_story::Story;
use std::time::Duration;

// Registering a customer opens their main account. A withdrawal is scored for fraud: a risky one
// freezes the account for a day, and a blocked one freezes it for good.

domain::id!(
    pub struct CustomerId;
);
domain::id!(
    pub struct AccountId;
);

#[domain::command(target = CustomerId, events(CustomerRegistered))]
pub struct Register;

#[domain::event]
pub struct CustomerRegistered;

#[derive(Aggregate)]
#[aggregate(id = CustomerId, state = (), events(CustomerRegistered), commands(Register))]
pub struct Customer;

impl Evolve for Customer {
    fn evolve(_: &mut (), _: &CustomerEvent) {}
}

impl Decide<Register> for Customer {
    fn decide(_: &(), _: &Register, _: &Cx) -> Decision<Register> {
        Ok(vec![CustomerRegistered.into()])
    }
}

#[domain::command(target = AccountId, events(AccountOpened), rejects(AlreadyOpen))]
pub struct OpenAccount {
    owner: CustomerId,
}

#[domain::command(target = AccountId, events(Withdrawn))]
pub struct Withdraw {
    amount: u64,
}

/// Never rejects, so it can be scheduled. Records nothing when already frozen.
#[domain::command(target = AccountId, events(Frozen))]
pub struct Freeze;

#[domain::command(target = AccountId, events(Thawed))]
pub struct Thaw;

#[domain::event]
pub struct AccountOpened {
    owner: CustomerId,
}

#[domain::event]
pub struct Withdrawn {
    amount: u64,
}

#[domain::event]
pub struct Frozen;

#[domain::event]
pub struct Thawed;

#[domain::rejection]
pub struct AlreadyOpen;

#[derive(Clone, Debug, Default)]
pub struct AccountState {
    open: bool,
    frozen: bool,
}

#[derive(Aggregate)]
#[aggregate(
    id = AccountId,
    state = AccountState,
    events(AccountOpened, Withdrawn, Frozen, Thawed),
    commands(OpenAccount, Withdraw, Freeze, Thaw)
)]
pub struct Account;

impl Evolve for Account {
    fn evolve(state: &mut AccountState, event: &AccountEvent) {
        match event {
            AccountEvent::AccountOpened(_) => state.open = true,
            AccountEvent::Withdrawn(_) => {}
            AccountEvent::Frozen(_) => state.frozen = true,
            AccountEvent::Thawed(_) => state.frozen = false,
        }
    }
}

impl Decide<OpenAccount> for Account {
    fn decide(state: &AccountState, cmd: &OpenAccount, _: &Cx) -> Decision<OpenAccount> {
        if state.open {
            return Err(AlreadyOpen.into());
        }
        Ok(vec![AccountOpened { owner: cmd.owner.clone() }.into()])
    }
}

impl Decide<Withdraw> for Account {
    fn decide(_: &AccountState, cmd: &Withdraw, _: &Cx) -> Decision<Withdraw> {
        Ok(vec![Withdrawn { amount: cmd.amount }.into()])
    }
}

impl Decide<Freeze> for Account {
    fn decide(state: &AccountState, _: &Freeze, _: &Cx) -> Decision<Freeze> {
        Ok(if state.frozen { vec![] } else { vec![Frozen.into()] })
    }
}

impl Decide<Thaw> for Account {
    fn decide(state: &AccountState, _: &Thaw, _: &Cx) -> Decision<Thaw> {
        Ok(if state.frozen { vec![Thawed.into()] } else { vec![] })
    }
}

// The fraud scorer, in the model's words.

#[derive(Call, Clone, Debug, PartialEq)]
#[call(system = FraudCheck, returns = Risk, errors(Blocked))]
pub struct Score {
    account: AccountId,
    amount: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Risk(u32);

#[domain::rejection]
pub struct Blocked;

#[derive(ExternalSystem)]
#[external_system(calls(Score))]
pub struct FraudCheck;

// Policies.

#[derive(Policy)]
#[policy(from(Customer(CustomerRegistered)))]
pub struct MainAccount;

impl React for MainAccount {
    async fn react(&self, cx: &PolicyCx<Self>, _: MainAccountInput) -> Result<(), PolicyError> {
        let owner = cx.source::<Customer>().expect("from customers").clone();
        let main = AccountId::new(format!("{owner}-main"));
        match cx.send(&main, OpenAccount { owner }) {
            Ok(_) | Err(domain::SendError::Rejected(OpenAccountRejection::AlreadyOpen(_))) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

pub const FREEZE_FOR: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Policy)]
#[policy(from(Account(Withdrawn)))]
pub struct FraudReview;

impl React for FraudReview {
    async fn react(&self, cx: &PolicyCx<Self>, input: FraudReviewInput) -> Result<(), PolicyError> {
        let FraudReviewInput::Withdrawn(withdrawn) = input;
        let account = cx.source::<Account>().expect("from accounts").clone();
        match cx.call(Score { account: account.clone(), amount: withdrawn.amount }).await {
            Ok(Risk(risk)) if risk >= 90 => {
                cx.send(&account, Freeze)?;
                cx.schedule(&account, Thaw, cx.now().after(FREEZE_FOR))?;
            }
            Ok(_) => {}
            Err(domain::CallError::Refused(ScoreError::Blocked(_))) => {
                cx.send(&account, Freeze)?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }
}

fn c1() -> CustomerId {
    CustomerId::new("c-1")
}

fn a1() -> AccountId {
    AccountId::new("a-1")
}

fn bank() -> Story {
    Story::new().policy(MainAccount).policy(FraudReview)
}

fn hours(n: u64) -> Duration {
    Duration::from_secs(n * 60 * 60)
}

#[test]
fn registering_opens_the_main_account() {
    bank()
        .when(&c1(), Register)
        .then(&c1(), [CustomerRegistered])
        .then(&AccountId::new("c-1-main"), [AccountOpened { owner: c1() }]);
}

#[test]
fn history_is_not_acted_on() {
    bank()
        .given(&c1(), [CustomerRegistered])
        .when(&CustomerId::new("c-2"), Register)
        .then(&CustomerId::new("c-2"), [CustomerRegistered])
        .then(&AccountId::new("c-2-main"), [AccountOpened { owner: CustomerId::new("c-2") }])
        .then_none(&AccountId::new("c-1-main"));
}

#[test]
fn a_risky_withdrawal_freezes_the_account_for_a_day() {
    bank()
        .at(Timestamp::from_millis(1_000))
        .when(&a1(), Withdraw { amount: 900 })
        .then(&a1(), [Withdrawn { amount: 900 }])
        .call(Score { account: a1(), amount: 900 })
        .succeed(Risk(95))
        .then(&a1(), [Frozen])
        .advance(hours(23))
        .then_none(&a1())
        .advance(hours(1))
        .then(&a1(), [Thawed]);
}

#[test]
fn a_safe_withdrawal_changes_nothing() {
    bank()
        .when(&a1(), Withdraw { amount: 5 })
        .then(&a1(), [Withdrawn { amount: 5 }])
        .call(Score { account: a1(), amount: 5 })
        .succeed(Risk(3))
        .advance(hours(48))
        .then_none(&a1());
}

#[test]
fn a_blocked_withdrawal_freezes_the_account_for_good() {
    bank()
        .when(&a1(), Withdraw { amount: 900 })
        .then(&a1(), [Withdrawn { amount: 900 }])
        .call(Score { account: a1(), amount: 900 })
        .fail(Blocked)
        .then(&a1(), [Frozen])
        .advance(hours(48))
        .then_none(&a1());
}

#[test]
#[should_panic(expected = "call FraudCheck.Score Score { account: AccountId(\"a-1\"), amount: 900 }")]
fn a_story_fails_when_a_call_is_left_unchecked() {
    bank().when(&a1(), Withdraw { amount: 900 }).then(&a1(), [Withdrawn { amount: 900 }]);
}

#[test]
#[should_panic(expected = "1 event(s) on Account/c-1-main")]
fn a_story_fails_when_a_policys_events_are_left_unchecked() {
    bank().when(&c1(), Register).then(&c1(), [CustomerRegistered]);
}
