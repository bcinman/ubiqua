use crate::*;
use domain::prelude::*;

#[derive(Aggregate)]
#[aggregate(
    id = AccountId,
    state = AccountState,
    events(AccountOpened, MoneyDeposited, MoneyWithdrawn, AccountFrozen, AccountThawed),
    commands(OpenAccount, DepositMoney, WithdrawMoney, RefundMoney, FreezeAccount, ThawAccount),
)]
pub struct Account;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum AccountState {
    #[default]
    Unopened,
    Open {
        owner: String,
        balance: u64,
        frozen: bool,
    },
}

impl Evolve for Account {
    fn evolve(state: &mut AccountState, event: &AccountEvent) {
        match (state, event) {
            (state, AccountEvent::AccountOpened(e)) => {
                *state = AccountState::Open { owner: e.owner.clone(), balance: 0, frozen: false };
            }
            (AccountState::Open { balance, .. }, AccountEvent::MoneyDeposited(e)) => *balance = e.balance_after,
            (AccountState::Open { balance, .. }, AccountEvent::MoneyWithdrawn(e)) => *balance = e.balance_after,
            (AccountState::Open { frozen, .. }, AccountEvent::AccountFrozen(_)) => *frozen = true,
            (AccountState::Open { frozen, .. }, AccountEvent::AccountThawed(_)) => *frozen = false,
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

fn owner(state: &AccountState) -> Option<&str> {
    match state {
        AccountState::Open { owner, .. } => Some(owner),
        AccountState::Unopened => None,
    }
}

fn balance(state: &AccountState) -> Balance {
    Balance { balance: open(state).unwrap_or(0) }
}

impl Decide<OpenAccount> for Account {
    fn decide(state: &AccountState, cmd: &OpenAccount, cx: &Cx) -> Decision<OpenAccount> {
        // Signed-in users open accounts in their own name.
        must_own(cx.actor::<User>(), Some(&cmd.owner))?;
        match state {
            AccountState::Unopened => Ok(vec![AccountOpened { owner: cmd.owner.clone() }.into()]),
            AccountState::Open { .. } => Err(AlreadyOpen.into()),
        }
    }
}

impl Decide<DepositMoney> for Account {
    fn decide(state: &AccountState, cmd: &DepositMoney, _: &Cx) -> Decision<DepositMoney> {
        let balance = open(state)?;
        Ok(vec![MoneyDeposited { amount: cmd.amount, balance_after: balance + cmd.amount }.into()])
    }
}

impl Reply<Account> for DepositMoney {
    fn reply(state: &AccountState) -> Balance {
        balance(state)
    }
}

impl Decide<RefundMoney> for Account {
    fn decide(state: &AccountState, cmd: &RefundMoney, _: &Cx) -> Decision<RefundMoney> {
        Ok(match open(state) {
            Ok(balance) => vec![MoneyDeposited { amount: cmd.amount, balance_after: balance + cmd.amount }.into()],
            Err(NotOpen) => vec![],
        })
    }
}

impl Reply<Account> for RefundMoney {
    fn reply(state: &AccountState) -> Balance {
        balance(state)
    }
}

impl Decide<WithdrawMoney> for Account {
    fn decide(state: &AccountState, cmd: &WithdrawMoney, cx: &Cx) -> Decision<WithdrawMoney> {
        let balance = open(state)?;
        must_own(cx.actor::<User>(), owner(state))?;
        if matches!(state, AccountState::Open { frozen: true, .. }) {
            return Err(Frozen.into());
        }
        if cmd.amount > balance {
            return Err(InsufficientFunds { balance, requested: cmd.amount }.into());
        }
        Ok(vec![MoneyWithdrawn { amount: cmd.amount, balance_after: balance - cmd.amount }.into()])
    }
}

impl Reply<Account> for WithdrawMoney {
    fn reply(state: &AccountState) -> Balance {
        balance(state)
    }
}

impl Decide<FreezeAccount> for Account {
    fn decide(state: &AccountState, _: &FreezeAccount, _: &Cx) -> Decision<FreezeAccount> {
        Ok(match state {
            AccountState::Open { frozen: false, .. } => vec![AccountFrozen.into()],
            _ => vec![],
        })
    }
}

impl Decide<ThawAccount> for Account {
    fn decide(state: &AccountState, _: &ThawAccount, _: &Cx) -> Decision<ThawAccount> {
        Ok(match state {
            AccountState::Open { frozen: true, .. } => vec![AccountThawed.into()],
            _ => vec![],
        })
    }
}
