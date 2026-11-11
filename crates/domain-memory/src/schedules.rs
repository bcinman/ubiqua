//! Scheduled commands: sent later, under their own command id, so each runs once. They're kept
//! in the store when there is one, so they fire after a restart too.

use crate::store::ScheduleRow;
use crate::{AggregateOf, Memory, decode_json, json, lock};
use domain::{
    Aggregate, AggregateId, Command, CommandId, Evolve, Handles, NeverRejects, SendError, StreamId, Timestamp,
};
use std::fmt;
use std::time::Duration;

/// Sends a stored schedule's command.
pub(crate) type FireFn = fn(&Memory, &ScheduleRow) -> Result<(), Unfired>;

/// Why a due command wasn't sent.
pub(crate) enum Unfired {
    /// Try again shortly: the store failed.
    Retry,
    /// It doesn't decode, so it waits for code that can read it.
    Undecodable,
}

pub(crate) const PENDING: &str = "pending";
pub(crate) const FIRED: &str = "fired";
pub(crate) const CANCELLED: &str = "cancelled";

/// How long a schedule whose command couldn't be sent waits before it's tried again.
const RETRY: Duration = Duration::from_secs(1);

/// Why a command couldn't be scheduled.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScheduleError {
    /// A command already ran under this id, so it would never run again.
    AlreadyExecuted { stream: StreamId, command_id: CommandId },
    /// The store failed; nothing was scheduled.
    Unavailable(String),
}

/// Why a schedule couldn't be cancelled.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum UnscheduleError {
    /// The command already ran.
    AlreadyExecuted,
    /// It was cancelled before.
    AlreadyCancelled,
    /// Nothing was scheduled under this id.
    NotScheduled,
    /// The store failed; the schedule stands.
    Unavailable(String),
}

impl fmt::Display for ScheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExecuted { stream, command_id } => {
                write!(f, "command {command_id} already ran on {stream}")
            }
            Self::Unavailable(reason) => write!(f, "unavailable: {reason}"),
        }
    }
}

impl fmt::Display for UnscheduleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExecuted => f.write_str("the command already ran"),
            Self::AlreadyCancelled => f.write_str("it was already cancelled"),
            Self::NotScheduled => f.write_str("nothing is scheduled under that id"),
            Self::Unavailable(reason) => write!(f, "unavailable: {reason}"),
        }
    }
}

impl std::error::Error for ScheduleError {}
impl std::error::Error for UnscheduleError {}

impl Memory {
    /// Sends `command` to `id` at `at`, under `command_id`. Scheduling the same id again replaces
    /// the schedule. Only commands that never reject can be scheduled: nobody waits for the answer.
    pub fn schedule<C>(
        &self,
        id: &C::Target,
        command: C,
        at: Timestamp,
        command_id: impl Into<CommandId>,
    ) -> Result<(), ScheduleError>
    where
        C: NeverRejects,
        AggregateOf<C::Target>: Handles<C>,
    {
        let command_id = command_id.into();
        let stream = StreamId::of::<AggregateOf<C::Target>>(id);
        self.register_firer::<C>();
        let executed = || ScheduleError::AlreadyExecuted { stream: stream.clone(), command_id: command_id.clone() };
        if self.ran(id, &stream, &command_id).map_err(ScheduleError::Unavailable)? {
            return Err(executed());
        }
        let row = ScheduleRow {
            stream: stream.to_string(),
            command_id: command_id.to_string(),
            command: command_key::<C>(),
            id: json(id).map_err(ScheduleError::Unavailable)?,
            payload: json(&command).map_err(ScheduleError::Unavailable)?,
            at: at.as_millis(),
            state: PENDING.into(),
        };
        {
            // A fired schedule left a receipt, so `ran` caught it above.
            let mut schedules = lock(&self.schedules);
            let key = (row.stream.clone(), row.command_id.clone());
            self.save_schedule(&row).map_err(ScheduleError::Unavailable)?;
            lock(&self.stuck).remove(&key);
            schedules.insert(key, row);
        }
        self.wake();
        Ok(())
    }

    /// Cancels the command scheduled on `id` under `command_id`. Once it succeeds, the command
    /// never runs.
    pub fn unschedule<I>(&self, id: &I, command_id: impl Into<CommandId>) -> Result<(), UnscheduleError>
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        let command_id = command_id.into();
        let stream = StreamId::of::<AggregateOf<I>>(id);
        let key = (stream.to_string(), command_id.to_string());
        let mut schedules = lock(&self.schedules);
        let Some(row) = schedules.get(&key) else {
            drop(schedules);
            let ran = self.ran(id, &stream, &command_id).map_err(UnscheduleError::Unavailable)?;
            return Err(if ran { UnscheduleError::AlreadyExecuted } else { UnscheduleError::NotScheduled });
        };
        match row.state.as_str() {
            FIRED => Err(UnscheduleError::AlreadyExecuted),
            CANCELLED => Err(UnscheduleError::AlreadyCancelled),
            _ => {
                let row = ScheduleRow { state: CANCELLED.into(), ..row.clone() };
                self.save_schedule(&row).map_err(UnscheduleError::Unavailable)?;
                schedules.insert(key, row);
                Ok(())
            }
        }
    }

    /// Sends every pending command that's due, oldest first.
    pub(crate) fn fire_due(&self) {
        let now = self.now();
        let mut due: Vec<ScheduleRow> = {
            let stuck = lock(&self.stuck);
            lock(&self.schedules)
                .values()
                .filter(|row| row.state == PENDING && row.at <= now.as_millis())
                .filter(|row| !stuck.contains(&(row.stream.clone(), row.command_id.clone())))
                .cloned()
                .collect()
        };
        due.sort_by(|a, b| (a.at, &a.stream, &a.command_id).cmp(&(b.at, &b.stream, &b.command_id)));
        for row in due {
            let key = (row.stream.clone(), row.command_id.clone());
            // Cancelled since it was picked: keep it that way.
            if lock(&self.schedules).get(&key).is_none_or(|current| current.state != PENDING) {
                continue;
            }
            let fire = lock(&self.firers).get(&row.command).copied();
            let Some(fire) = fire else {
                // Its aggregate isn't hosted here; it fires once a model with it is.
                lock(&self.stuck).insert(key);
                continue;
            };
            let next = match fire(self, &row) {
                Ok(()) => ScheduleRow { state: FIRED.into(), ..row },
                Err(Unfired::Retry) => ScheduleRow { at: now.after(RETRY).as_millis(), ..row },
                Err(Unfired::Undecodable) => {
                    lock(&self.stuck).insert(key);
                    continue;
                }
            };
            if self.save_schedule(&next).is_ok() {
                lock(&self.schedules).insert(key, next);
            }
        }
    }

    /// When the next pending command is due.
    pub(crate) fn next_schedule(&self) -> Option<Timestamp> {
        let stuck = lock(&self.stuck);
        lock(&self.schedules)
            .values()
            .filter(|row| row.state == PENDING && !stuck.contains(&(row.stream.clone(), row.command_id.clone())))
            .map(|row| Timestamp::from_millis(row.at))
            .min()
    }

    pub(crate) fn register_firer<C>(&self)
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        lock(&self.firers).entry(command_key::<C>()).or_insert(fire::<C>);
    }

    fn save_schedule(&self, row: &ScheduleRow) -> Result<(), String> {
        match &self.store {
            Some(store) => store.save_schedule(row).map_err(|e| format!("the store failed: {e}")),
            None => Ok(()),
        }
    }

    /// Whether a command already ran under `command_id` on `stream`.
    fn ran<I>(&self, id: &I, stream: &StreamId, command_id: &CommandId) -> Result<bool, String>
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        let instance = self.instance(id);
        let mut instance = lock(&instance);
        self.load(id, &mut instance)?;
        if instance.receipts.contains_key(command_id) {
            return Ok(true);
        }
        match &self.store {
            Some(store) => store
                .receipt(stream.as_str(), command_id.as_str())
                .map(|row| row.is_some())
                .map_err(|e| format!("the store failed: {e}")),
            None => Ok(false),
        }
    }
}

/// Names a command across aggregates: `Transfer.ExpireTransfer`.
pub(crate) fn command_key<C>() -> String
where
    C: Command,
    AggregateOf<C::Target>: Aggregate,
{
    format!("{}.{}", <AggregateOf<C::Target> as Aggregate>::NAME, C::NAME)
}

fn fire<C>(memory: &Memory, row: &ScheduleRow) -> Result<(), Unfired>
where
    C: Command,
    AggregateOf<C::Target>: Handles<C>,
{
    let id: C::Target = decode_json(&row.id).map_err(|_| Unfired::Undecodable)?;
    let command: C = decode_json(&row.payload).map_err(|_| Unfired::Undecodable)?;
    match memory.send_with_id(&id, command, CommandId::new(row.command_id.clone())) {
        Err(SendError::Unavailable { .. }) => Err(Unfired::Retry),
        // It ran, or another command already used the id: either way it's done.
        _ => Ok(()),
    }
}
