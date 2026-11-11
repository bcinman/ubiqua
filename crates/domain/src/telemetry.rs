//! What an observer, such as a developer's dashboard, sees of a running model: its parts, where
//! each stream stands, and a trace of what happens, replayed from the start or followed live.
//!
//! Values are in their serde JSON form, as in storage and on the wire, so an observer needs none
//! of the model's types. Aggregate states aren't serializable, so they're shown as their `Debug`.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// One thing that happened in the model.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trace {
    /// A command was decided. Commands aren't recorded, so a replay doesn't show them.
    Command(CommandTrace),
    /// An aggregate recorded an event.
    Event(EventTrace),
    /// A view applied an event to one key, changing its state or emitting events.
    View(ViewTrace),
    /// Everything before now has been replayed; what follows is live. `streams` is how far the
    /// trace has gone in each stream: pass it as [`Since::After`] to pick up from here.
    CaughtUp { streams: BTreeMap<String, u64> },
    /// The trace can't go on, for this reason, such as a server that doesn't allow it. Nothing
    /// follows.
    Failed(String),
}

/// Where a trace starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Since {
    /// Replay everything recorded, then follow live.
    Start,
    /// Only what happens from now on.
    Now,
    /// Replay what came after these positions, such as a `CaughtUp`'s `streams`, then follow live.
    /// A stream it doesn't name replays from its start.
    After(BTreeMap<String, u64>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandTrace {
    /// The command's name, such as `DepositMoney`.
    pub command: String,
    pub stream: String,
    pub command_id: String,
    /// Who sent it, as its `Debug`; `None` for code in the app, policies and schedules.
    pub actor: Option<String>,
    pub input: Value,
    /// When it was decided.
    pub at: u64,
    pub outcome: Outcome,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Its events committed; the stream is now at `version`. They follow as `Event`s.
    Committed { version: u64 },
    /// The aggregate refused it, for this reason. Nothing was recorded.
    Rejected(Value),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventTrace {
    /// The aggregate's name, such as `Account`.
    pub aggregate: String,
    pub stream: String,
    pub seq: u64,
    /// The instance's id.
    pub id: Value,
    pub at: u64,
    /// The command that recorded it. A policy's commands name the event it reacted to:
    /// `FraudReview:Account/sam-main:3:1`. `None` for imported history.
    pub command_id: Option<String>,
    pub event: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewTrace {
    /// The view's name, such as `AccountSummary`.
    pub view: String,
    pub key: Value,
    /// The key's stream of emitted events, such as `Statement/sam-main`.
    pub stream: String,
    /// The key's state after it.
    pub state: Value,
    /// The events it emitted, each with its cursor in `stream`.
    pub emitted: Vec<Emitted>,
    /// The event it applied: an aggregate's, or one another view emitted.
    pub cause: Position,
    pub at: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Emitted {
    pub cursor: u64,
    pub event: Value,
}

/// An event's place: its stream and seq.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub stream: String,
    pub seq: u64,
}

/// The parts a model runs, and what each reads: enough to draw it.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Catalog {
    pub parts: Vec<Part>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Part {
    pub kind: Kind,
    pub name: String,
    /// The aggregates and views it reads.
    pub reads: Vec<String>,
    /// A view's or projection's version; 0 otherwise.
    pub version: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Aggregate,
    View,
    Policy,
    Projection,
    Saga,
}

/// Where the model stands now.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// Every stream that has events, with its version.
    pub streams: BTreeMap<String, u64>,
    /// What's failing: each policy's or projection's last failure, and saga runs that died.
    pub failures: Vec<String>,
}

/// One aggregate instance, as it stood at a version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateTrace {
    pub aggregate: String,
    pub stream: String,
    pub id: Value,
    pub version: u64,
    /// The state after `version`'s event, as its `Debug`.
    pub state: String,
    /// Its events up to `version`, oldest first.
    pub events: Vec<EventTrace>,
}

/// One key of a view, now.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewKey {
    pub key: Value,
    pub stream: String,
    pub state: Value,
    /// How many events it has emitted.
    pub cursor: u64,
}
