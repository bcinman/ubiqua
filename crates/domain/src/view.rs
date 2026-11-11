//! Views: read models folded from events, and the read protocol hosts serve them with.

use crate::{Command, Events, Evolve, MaybeSend, MaybeSync, ReadCx, Receipt, Rejections, StreamId, Timestamp};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::hash::Hash;
use std::sync::Arc;

/// Something a view can read: an aggregate, or another view. The derives implement it.
pub trait Source: Sized + MaybeSend + MaybeSync + 'static {
    /// The instance that recorded an event: an aggregate's id, or a view's key.
    type SourceId: Clone + Eq + Hash + Debug + MaybeSend + MaybeSync + 'static;
    /// The events it records: an aggregate's events, or the events a view emits.
    type SourceEvent: Events;
    /// What arrives with each event: `()` from an aggregate, the key's state after it from a view.
    type SourceState: Clone + Debug + MaybeSend + MaybeSync + 'static;

    /// Hosts the source itself, so a view's sources are hosted before it.
    #[doc(hidden)]
    fn register_source<R: Registry>(registry: &mut R);
}

/// What a host delivers from a source `S`: who recorded the event, the event, and the state with it.
#[doc(hidden)]
pub type SourcePayload<S> = (<S as Source>::SourceId, <S as Source>::SourceEvent, <S as Source>::SourceState);

/// Where an event came from, for [`KeyOf`].
pub struct Origin<'a, S: Source> {
    /// The aggregate instance or view key that recorded it.
    pub id: &'a S::SourceId,
    /// `()` from an aggregate; from a view, the key's state after the event.
    pub state: &'a S::SourceState,
}

/// A read model: state per key, folded from the events of aggregates and other views.
/// Derive it with `#[derive(View)]`, then implement [`Apply`].
pub trait View: Sized + MaybeSend + MaybeSync + 'static {
    const NAME: &'static str;
    /// Change it whenever you change what the view computes, so a stored view is rebuilt from the
    /// events, along with every view that reads it. `#[view(version = 1)]` sets it.
    const VERSION: u64 = 0;
    /// What one instance of the view is about, such as one account.
    type Key: Clone + Eq + Hash + Debug + Display + Serialize + DeserializeOwned + MaybeSend + MaybeSync + 'static;
    /// `Default` is the state before the key's first event.
    type State: Clone + Debug + Default + PartialEq + Serialize + DeserializeOwned + MaybeSend + MaybeSync + 'static;
    /// The events the view reads, as an enum. The derive generates it.
    type Input: Events;
    /// The events the view emits. Without `events(..)`, every input it applies.
    type Event: Events;
    /// Why a read may be refused: `#[view(rejects(..))]`. Empty unless the view guards its keys.
    type Rejection: Rejections;

    /// Calls `sources.source::<S>()` for each aggregate or view it reads.
    fn sources<S: Sources<Self>>(sources: &mut S);

    #[doc(hidden)]
    fn auto_emit(input: &Self::Input) -> Option<Self::Event>;
}

/// How a view applies one input.
#[diagnostic::on_unimplemented(message = "`{Self}` has no `impl Apply`", label = "implement `Apply` for `{Self}`")]
pub trait Apply: View {
    /// Update the key's state, and emit events with `cx.emit(..)`. Never fails: the input already happened.
    fn apply(state: &mut Self::State, input: Self::Input, cx: &mut ViewCx<'_, Self>);
}

/// Who may read a view's keys. A view with `rejects(..)` implements it; without, the derive does,
/// allowing every read.
#[diagnostic::on_unimplemented(
    message = "`{Self}` declares `rejects(..)` but has no `impl Validate`",
    label = "implement `Validate` for `{Self}`, or remove its `rejects(..)`"
)]
pub trait Validate: View {
    /// Allow the read, or refuse it. Runs on every read, and again on each change while a
    /// client watches the key: refusing ends the watch.
    fn validate(cx: &ReadCx<'_>, key: &Self::Key, state: Option<&Self::State>) -> Result<(), Self::Rejection>;
}

/// Routes a source's events to the view's keys. `#[derive(View)]` implements it per source.
pub trait Consumes<S: Source>: View {
    /// The key and input for this event, or `None` when the view doesn't read it.
    fn route(id: &S::SourceId, event: &S::SourceEvent, state: &S::SourceState) -> Option<(Self::Key, Self::Input)>;
}

/// Visits the sources a view reads. Hosts implement it.
pub trait Sources<V: View> {
    fn source<S: Source>(&mut self)
    where
        V: Consumes<S>;
}

/// Picks the key for events from a source whose id isn't the view's key.
/// List the source in `#[view(key_of(..))]`, then implement this.
pub trait KeyOf<S: Source>: View {
    /// `None` means the event belongs to no key.
    fn key_of(from: Origin<'_, S>, input: &Self::Input) -> Option<Self::Key>;
}

/// The default route: a source's id is the view's key.
#[diagnostic::on_unimplemented(
    message = "a `{Id}` source can't key a view keyed by `{Self}`",
    label = "list this source in `key_of(..)` and implement `KeyOf` for it"
)]
pub trait KeyFrom<Id> {
    fn key_from(id: &Id) -> Self;
}

impl<K: Clone> KeyFrom<K> for K {
    fn key_from(id: &K) -> K {
        id.clone()
    }
}

/// One event type in an event enum. The derives implement it for every member.
#[diagnostic::on_unimplemented(
    message = "`{E}` isn't one of the events in `{Self}`",
    label = "this source doesn't record `{E}`"
)]
pub trait Member<E> {
    fn get(&self) -> Option<&E>;
}

/// What [`Apply::apply`] may read besides the state and the input, and where it emits events.
pub struct ViewCx<'a, V: View> {
    key: &'a V::Key,
    stream: &'a StreamId,
    seq: u64,
    at: Timestamp,
    payload: &'a dyn Any,
    emitted: Vec<V::Event>,
}

impl<'a, V: View> ViewCx<'a, V> {
    /// For hosts: `payload` is the delivery's [`SourcePayload`].
    pub fn new(key: &'a V::Key, stream: &'a StreamId, seq: u64, at: Timestamp, payload: &'a dyn Any) -> Self {
        Self { key, stream, seq, at, payload, emitted: Vec::new() }
    }

    /// The key being updated.
    pub fn key(&self) -> &V::Key {
        self.key
    }

    /// The stream that recorded the event, and its position there.
    pub fn stream(&self) -> (&StreamId, u64) {
        (self.stream, self.seq)
    }

    /// When the event was recorded.
    pub fn at(&self) -> Timestamp {
        self.at
    }

    /// The aggregate instance or view key that recorded the event, if it came from `S`.
    pub fn source<S: Source>(&self) -> Option<&S::SourceId> {
        self.payload.downcast_ref::<SourcePayload<S>>().map(|(id, _, _)| id)
    }

    /// If the event came from the view `S`, that key's state after it.
    pub fn state<S: Source>(&self) -> Option<&S::SourceState> {
        self.payload.downcast_ref::<SourcePayload<S>>().map(|(_, _, state)| state)
    }

    /// Emit an event on this key, after the state update.
    pub fn emit(&mut self, event: impl Into<V::Event>) {
        self.emitted.push(event.into());
    }

    /// For hosts: the events emitted so far.
    pub fn into_emitted(self) -> Vec<V::Event> {
        self.emitted
    }
}

/// A position in one key's emitted events. `Cursor::START` is before the first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor(u64);

impl Cursor {
    pub const START: Cursor = Cursor(0);

    pub const fn new(seq: u64) -> Self {
        Self(seq)
    }

    /// The number of the last entry this cursor includes.
    pub const fn seq(self) -> u64 {
        self.0
    }
}

/// One event a view emitted, with the key's state after it.
pub struct Entry<V: View> {
    pub cursor: Cursor,
    pub event: V::Event,
    pub state: Arc<V::State>,
    /// The source event that caused it.
    pub stream: StreamId,
    pub seq: u64,
    pub at: Timestamp,
}

/// What a live feed delivers: history, then `CaughtUp`, then new entries as they're recorded.
pub enum Frame<V: View> {
    Entry(Entry<V>),
    /// Everything recorded so far has been sent.
    CaughtUp {
        cursor: Cursor,
    },
    /// The view's `validate` refused the reader. Nothing follows.
    Rejected(V::Rejection),
}

impl<V: View> Frame<V> {
    pub fn entry(&self) -> Option<&Entry<V>> {
        match self {
            Frame::Entry(entry) => Some(entry),
            _ => None,
        }
    }

    pub fn is_caught_up(&self) -> bool {
        matches!(self, Frame::CaughtUp { .. })
    }
}

/// A key's state with the cursor it is valid at. Follow it with `events(key, snapshot.cursor)`.
pub struct Snapshot<V: View> {
    /// `None` until the key's first event.
    pub state: Option<V::State>,
    pub cursor: Cursor,
}

/// A stream at a version, such as the one a command committed at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    pub stream: StreamId,
    pub version: u64,
}

impl<C: Command> From<&Receipt<C>> for Position {
    fn from(receipt: &Receipt<C>) -> Self {
        Self { stream: receipt.stream.clone(), version: receipt.version }
    }
}

/// A view hasn't applied a stream up to the version a read asked for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotCaughtUp {
    pub view: &'static str,
    pub stream: StreamId,
    pub wanted: u64,
    pub applied: u64,
}

impl fmt::Display for NotCaughtUp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} has applied {} up to {}, not {}", self.view, self.stream, self.applied, self.wanted)
    }
}

impl std::error::Error for NotCaughtUp {}

/// Aggregates and views that run together. Derive it with `#[derive(Model)]`.
pub trait Model: 'static {
    const NAME: &'static str;
    fn register<R: Registry>(registry: &mut R);
}

/// Receives a model's parts. Hosts implement it.
pub trait Registry {
    fn aggregate<A: Evolve>(&mut self);
    fn view<V: Apply>(&mut self);
    /// A command an aggregate decides, so a host can run it later, as a schedule does.
    fn command<C: Command>(&mut self)
    where
        <C::Target as crate::AggregateId>::Aggregate: crate::Handles<C>;
}

impl<V: View> Clone for Entry<V> {
    fn clone(&self) -> Self {
        Self {
            cursor: self.cursor,
            event: self.event.clone(),
            state: self.state.clone(),
            stream: self.stream.clone(),
            seq: self.seq,
            at: self.at,
        }
    }
}

impl<V: View> Debug for Entry<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Entry")
            .field("cursor", &self.cursor)
            .field("event", &self.event)
            .field("state", &self.state)
            .field("stream", &self.stream)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

impl<V: View> Clone for Frame<V> {
    fn clone(&self) -> Self {
        match self {
            Frame::Entry(entry) => Frame::Entry(entry.clone()),
            Frame::CaughtUp { cursor } => Frame::CaughtUp { cursor: *cursor },
            Frame::Rejected(rejection) => Frame::Rejected(rejection.clone()),
        }
    }
}

impl<V: View> Debug for Frame<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Entry(entry) => entry.fmt(f),
            Frame::CaughtUp { cursor } => f.debug_struct("CaughtUp").field("cursor", cursor).finish(),
            Frame::Rejected(rejection) => f.debug_tuple("Rejected").field(rejection).finish(),
        }
    }
}

impl<V: View> Clone for Snapshot<V> {
    fn clone(&self) -> Self {
        Self { state: self.state.clone(), cursor: self.cursor }
    }
}

impl<V: View> Debug for Snapshot<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot").field("state", &self.state).field("cursor", &self.cursor).finish()
    }
}

impl<V: View> PartialEq for Snapshot<V> {
    fn eq(&self, other: &Self) -> bool {
        self.state == other.state && self.cursor == other.cursor
    }
}
