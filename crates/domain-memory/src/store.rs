//! Where a [`Memory`](crate::Memory) keeps what it must not lose: events, receipts and views.
//! Values arrive as JSON text, so a store needs none of the model's types. `domain-sqlite`
//! implements it.

use domain::{MaybeSend, MaybeSync};
use std::fmt;

/// Durable storage for a `Memory`. Each method is one transaction.
pub trait Store: MaybeSend + MaybeSync {
    /// Records a command's events and receipt together, or neither. Fails if an event's
    /// `(stream, seq)` is already taken.
    fn commit(&self, commit: &Commit) -> Result<(), StoreError>;
    /// One stream's events, oldest first.
    fn stream(&self, stream: &str) -> Result<Vec<EventRow>, StoreError>;
    /// The receipt a command stored under `command_id` on `stream`.
    fn receipt(&self, stream: &str, command_id: &str) -> Result<Option<ReceiptRow>, StoreError>;
    /// Every event an aggregate recorded, in the order they were committed, skipping those `keep`
    /// refuses. `keep` gets each event's stream and seq.
    fn history(&self, aggregate: &str, keep: &dyn Fn(&str, u64) -> bool) -> Result<Vec<EventRow>, StoreError>;
    /// Every stream with its version.
    fn versions(&self) -> Result<Vec<(String, u64)>, StoreError>;
    /// A number no earlier call returned, so command ids made now differ from earlier runs'.
    fn new_session(&self) -> Result<u64, StoreError>;

    /// The version a view was stored at, or `None` for a view never stored.
    fn view_version(&self, view: &str) -> Result<Option<u64>, StoreError>;
    /// Everything stored for a view.
    fn load_view(&self, view: &str) -> Result<ViewRows, StoreError>;
    /// Adds or replaces these keys, entries and progress, together.
    fn save_view(&self, view: &str, rows: &ViewRows) -> Result<(), StoreError>;
    /// Deletes what's stored for a view and records it at `version`.
    fn reset_view(&self, view: &str, version: u64) -> Result<(), StoreError>;

    /// Adds or replaces a scheduled command, by its stream and command id.
    fn save_schedule(&self, row: &ScheduleRow) -> Result<(), StoreError>;
    /// Every scheduled command, including those that ran or were cancelled.
    fn schedules(&self) -> Result<Vec<ScheduleRow>, StoreError>;
}

/// A command to send later.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleRow {
    pub stream: String,
    pub command_id: String,
    /// The aggregate and command, such as `Transfer.ExpireTransfer`.
    pub command: String,
    /// The instance's id, as JSON.
    pub id: String,
    /// The command, as JSON.
    pub payload: String,
    /// When it's due, in milliseconds since the Unix epoch.
    pub at: u64,
    /// `pending`, `fired` or `cancelled`.
    pub state: String,
}

/// What one command or import records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    /// The aggregate's name, such as `Account`.
    pub aggregate: String,
    pub stream: String,
    pub events: Vec<EventRow>,
    pub receipt: Option<ReceiptRow>,
}

/// One recorded event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRow {
    pub stream: String,
    pub seq: u64,
    /// The aggregate instance's id, as JSON.
    pub id: String,
    /// Milliseconds since the Unix epoch.
    pub at: u64,
    pub command_id: Option<String>,
    /// The event, as JSON.
    pub event: String,
}

/// A command's receipt, for retries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptRow {
    pub command_id: String,
    /// The command's name, so a reused id for another command is refused.
    pub command: String,
    pub receipt: String,
}

/// A view's keys, emitted entries and progress, or some of them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewRows {
    pub keys: Vec<KeyRow>,
    pub entries: Vec<EntryRow>,
    /// How far the view has applied each source stream.
    pub progress: Vec<(String, u64)>,
}

/// One key's state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyRow {
    pub key: String,
    pub state: String,
}

/// One event a view emitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryRow {
    pub key: String,
    pub cursor: u64,
    pub event: String,
    /// The key's state after it.
    pub state: String,
    /// The source event that caused it.
    pub stream: String,
    pub seq: u64,
    pub at: u64,
}

/// Why a store failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreError(pub String);

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for StoreError {}
