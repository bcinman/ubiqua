//! Moving money between two accounts. No transaction spans two accounts, so a transfer is a saga:
//! debit one, credit the other, and refund the debit if the credit is refused. A `Transfer`
//! records the request and how it ended; a policy starts the saga when one is requested.

use crate::*;
use domain::{Aggregate, Cx, Decide, Decision, Evolve, Policy, Saga};
use domain_memory::{PolicyCx, PolicyError, React, Run, SagaCx};
use serde::{Deserialize, Serialize};

#[domain::command(target = TransferId, events(TransferRequested), rejects(AlreadyRequested))]
pub struct RequestTransfer {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u64,
}

#[domain::event]
pub struct TransferRequested {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u64,
    /// Who asked, so the debit checks they own the account.
    pub by: Option<String>,
}

#[domain::rejection]
pub struct AlreadyRequested;

// How it ended. The saga sends these; they never reject.

#[domain::command(target = TransferId, events(TransferCompleted))]
pub struct CompleteTransfer;

#[domain::event]
pub struct TransferCompleted;

#[domain::command(target = TransferId, events(TransferFailed))]
pub struct FailTransfer {
    pub reason: String,
}

#[domain::event]
pub struct TransferFailed {
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub enum TransferState {
    #[default]
    Unrequested,
    Pending,
    Ended,
}

#[derive(Aggregate)]
#[aggregate(
    id = TransferId,
    state = TransferState,
    events(TransferRequested, TransferCompleted, TransferFailed),
    commands(RequestTransfer, CompleteTransfer, FailTransfer)
)]
pub struct Transfer;

impl Evolve for Transfer {
    fn evolve(state: &mut TransferState, event: &TransferEvent) {
        *state = match event {
            TransferEvent::TransferRequested(_) => TransferState::Pending,
            TransferEvent::TransferCompleted(_) | TransferEvent::TransferFailed(_) => TransferState::Ended,
        };
    }
}

impl Decide<RequestTransfer> for Transfer {
    fn decide(state: &TransferState, cmd: &RequestTransfer, cx: &Cx) -> Decision<RequestTransfer> {
        if *state != TransferState::Unrequested {
            return Err(AlreadyRequested.into());
        }
        let by = cx.actor::<User>().map(|User(name)| name.clone());
        Ok(vec![TransferRequested { from: cmd.from.clone(), to: cmd.to.clone(), amount: cmd.amount, by }.into()])
    }

    fn reply(_: &TransferState) {}
}

impl Decide<CompleteTransfer> for Transfer {
    fn decide(state: &TransferState, _: &CompleteTransfer, _: &Cx) -> Decision<CompleteTransfer> {
        Ok(if *state == TransferState::Pending { vec![TransferCompleted.into()] } else { vec![] })
    }

    fn reply(_: &TransferState) {}
}

impl Decide<FailTransfer> for Transfer {
    fn decide(state: &TransferState, cmd: &FailTransfer, _: &Cx) -> Decision<FailTransfer> {
        let failed = TransferFailed { reason: cmd.reason.clone() };
        Ok(if *state == TransferState::Pending { vec![failed.into()] } else { vec![] })
    }

    fn reply(_: &TransferState) {}
}

/// Starts a transfer's saga once it's requested. Nobody waits for it.
#[derive(Policy)]
#[policy(from(Transfer(TransferRequested)))]
pub struct TransferExecution;

impl React for TransferExecution {
    async fn react(&self, cx: &PolicyCx<Self>, input: TransferExecutionInput) -> Result<(), PolicyError> {
        let TransferExecutionInput::TransferRequested(requested) = input;
        let transfer = cx.source::<Transfer>().expect("reads transfers").clone();
        let TransferRequested { from, to, amount, by } = requested;
        // Keyed by the transfer, so running twice starts one run.
        cx.start_saga(&transfer, ExecuteTransfer { from, to, amount, by })?;
        Ok(())
    }
}

/// The destination account can't take the money.
#[domain::rejection]
pub struct DestinationClosed;

/// Carries out one transfer.
#[derive(Saga, Debug, Serialize, Deserialize)]
#[saga(
    key = TransferId,
    rejects(NotOpen, NotOwner, Frozen, InsufficientFunds, DestinationClosed),
    sends(WithdrawMoney)
)]
pub struct ExecuteTransfer {
    pub from: AccountId,
    pub to: AccountId,
    pub amount: u64,
    pub by: Option<String>,
}

impl Run for ExecuteTransfer {
    async fn run(self, cx: &SagaCx<Self>) -> Result<(), ExecuteTransferRejection> {
        let moved = self.move_money(cx).await;
        // Both always record, so there's no rejection to handle.
        match &moved {
            Ok(()) => {
                let _ = cx.send("complete", cx.key(), CompleteTransfer).await;
            }
            Err(refused) => {
                let _ = cx.send("fail", cx.key(), FailTransfer { reason: refused.to_string() }).await;
            }
        }
        moved
    }
}

impl ExecuteTransfer {
    async fn move_money(&self, cx: &SagaCx<Self>) -> Result<(), ExecuteTransferRejection> {
        // Debit first: if the credit is refused, the money goes back. It's never made up.
        let debit = cx.send("debit", &self.from, WithdrawMoney { amount: self.amount });
        let debit = match &self.by {
            Some(name) => debit.acting_as(User(name.clone())),
            None => debit,
        };
        debit.compensate("refund", &self.from, RefundMoney { amount: self.amount }).await?;
        cx.send("credit", &self.to, DepositMoney { amount: self.amount }).await.map_err(|_| DestinationClosed)?;
        Ok(())
    }
}
