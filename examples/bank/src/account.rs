use crate::*;
use domain::{Aggregate, Cx, Decide, Decision, Evolve};

#[derive(Aggregate)]
#[aggregate(
    id = AccountId,
    state = AccountState,
    events(AccountOpened, MoneyDeposited, MoneyWithdrawn),
    commands(OpenAccount, DepositMoney, WithdrawMoney),
)]
pub struct Account;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum AccountState {
    #[default]
    Unopened,
    Open {
        owner: String,
        balance: u64,
    },
}

impl Evolve for Account {
    fn evolve(state: &mut AccountState, event: &AccountEvent) {
        match (state, event) {
            (state, AccountEvent::AccountOpened(e)) => {
                *state = AccountState::Open { owner: e.owner.clone(), balance: 0 };
            }
            (AccountState::Open { balance, .. }, AccountEvent::MoneyDeposited(e)) => *balance = e.balance_after,
            (AccountState::Open { balance, .. }, AccountEvent::MoneyWithdrawn(e)) => *balance = e.balance_after,
            _ => {}
        }
    }
}

/// The balance of an open account, or `NotOpen`.
fn open(state: &AccountState) -> Result<u64, NotOpen> {
    match state {
        AccountState::Open { balance, .. } => Ok(*balance),
        AccountState::Unopened => Err(NotOpen),
    }
}

fn balance(state: &AccountState) -> Balance {
    Balance { balance: open(state).unwrap_or(0) }
}

impl Decide<OpenAccount> for Account {
    fn decide(state: &AccountState, cmd: &OpenAccount, _: &Cx) -> Decision<OpenAccount> {
        match state {
            AccountState::Unopened => Ok(vec![AccountOpened { owner: cmd.owner.clone() }.into()]),
            AccountState::Open { .. } => Err(AlreadyOpen.into()),
        }
    }

    fn reply(_: &AccountState) {}
}

impl Decide<DepositMoney> for Account {
    fn decide(state: &AccountState, cmd: &DepositMoney, _: &Cx) -> Decision<DepositMoney> {
        let balance = open(state)?;
        Ok(vec![MoneyDeposited { amount: cmd.amount, balance_after: balance + cmd.amount }.into()])
    }

    fn reply(state: &AccountState) -> Balance {
        balance(state)
    }
}

impl Decide<WithdrawMoney> for Account {
    fn decide(state: &AccountState, cmd: &WithdrawMoney, _: &Cx) -> Decision<WithdrawMoney> {
        let balance = open(state)?;
        if cmd.amount > balance {
            return Err(InsufficientFunds { balance, requested: cmd.amount }.into());
        }
        Ok(vec![MoneyWithdrawn { amount: cmd.amount, balance_after: balance - cmd.amount }.into()])
    }

    fn reply(state: &AccountState) -> Balance {
        balance(state)
    }
}
