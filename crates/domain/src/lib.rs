//! Commands ask for changes, aggregates decide them, and events record what happened.
//!
//! This crate holds the declarations: traits, derive macros and the values that cross between a
//! model and whatever hosts it. It has no runtime; `domain-memory` runs a model in memory.
//! See the `docs/` directory for a guide.

extern crate self as domain;

mod maybe;
mod values;

#[doc(hidden)]
pub mod compile_checks;

use std::fmt::{Debug, Display};
use std::hash::Hash;

pub use domain_macros::{Aggregate, Command, Event, Rejection};
pub use maybe::{MaybeSend, MaybeSync};
pub use values::{CommandId, Envelope, Receipt, SendError, StreamId, Timestamp};

/// A fact a command recorded. Derive it with `#[derive(Event)]`.
pub trait Event: Clone + Debug + PartialEq + MaybeSend + MaybeSync + 'static {
    const NAME: &'static str;
}

/// A reason a command was refused. Derive it with `#[derive(Rejection)]`.
pub trait Rejection: Clone + Debug + PartialEq + Display + std::error::Error + MaybeSend + MaybeSync + 'static {
    const NAME: &'static str;
}

/// A set of events, as an enum with one variant per event. The derives generate these.
pub trait Events: Clone + Debug + PartialEq + MaybeSend + MaybeSync + 'static {
    /// The `Event::NAME` of the variant.
    fn name(&self) -> &'static str;
}

/// A set of rejections, as an enum with one variant per rejection. `#[derive(Command)]` generates these.
pub trait Rejections:
    Clone + Debug + PartialEq + Display + std::error::Error + MaybeSend + MaybeSync + 'static
{
    /// The `Rejection::NAME` of the variant.
    fn name(&self) -> &'static str;
}

/// Converts a command's events into an aggregate's events. `#[derive(Command)]` implements it for
/// every aggregate whose events include all of the command's.
pub trait Widen<E>: Events {
    fn widen(self) -> E;
}

/// The id of one aggregate instance. Each id type belongs to exactly one aggregate, so a command's
/// target names the aggregate that decides it. `#[derive(Aggregate)]` implements it.
pub trait AggregateId: Clone + Eq + Hash + Debug + Display + MaybeSend + MaybeSync + 'static {
    type Aggregate: Aggregate<Id = Self>;
}

/// A consistency boundary: one instance per id, deciding one command at a time.
/// Derive it with `#[derive(Aggregate)]`, then implement [`Evolve`] and one [`Decide`] per command.
pub trait Aggregate: Sized + MaybeSend + MaybeSync + 'static {
    const NAME: &'static str;
    type Id: AggregateId<Aggregate = Self>;
    /// `Default` is the state of every instance before its first event.
    type State: Clone + Debug + Default + MaybeSend + MaybeSync + 'static;
    type Event: Events;
}

/// Applies a recorded event to the state. It never fails: the event already happened.
#[diagnostic::on_unimplemented(message = "`{Self}` has no `impl Evolve`", label = "implement `Evolve` for `{Self}`")]
pub trait Evolve: Aggregate {
    fn evolve(state: &mut Self::State, event: &Self::Event);
}

/// A request for a change to one aggregate instance. Derive it with `#[derive(Command)]`.
pub trait Command: Debug + MaybeSend + 'static {
    const NAME: &'static str;
    type Target: AggregateId;
    /// The events it may record.
    type Event: Events;
    /// The reasons it may be refused.
    type Rejection: Rejections;
    /// What the caller gets back once its events commit.
    type Reply: Clone + Debug + PartialEq + MaybeSend + MaybeSync + 'static;
}

/// What a [`Decide`] handler returns: the events to record, or why the command is refused.
pub type Decision<C> = Result<Vec<<C as Command>::Event>, <C as Command>::Rejection>;

/// How an aggregate decides one command.
pub trait Decide<C: Command>: Evolve {
    /// Decide the command against the current state. Return `Ok(vec![])` when there's nothing to record.
    fn decide(state: &Self::State, command: &C, cx: &Cx) -> Decision<C>;
    /// The reply, computed from the state after the events are applied.
    fn reply(state: &Self::State) -> C::Reply;
}

/// Everything a host needs to run `C` on this aggregate: it decides `C`, `C` targets its id, and
/// `C`'s events are among its own. Implemented automatically; use it as a bound.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't handle `{C}`",
    label = "needs `impl Decide<{C}> for {Self}`, with every event of `{C}` in its `events(..)`"
)]
pub trait Handles<C: Command>: Decide<C> {
    fn widen(event: C::Event) -> Self::Event;
}

impl<A, C> Handles<C> for A
where
    A: Decide<C>,
    C: Command<Target = A::Id>,
    C::Event: Widen<A::Event>,
{
    fn widen(event: C::Event) -> A::Event {
        event.widen()
    }
}

/// What a decision may read besides the state and the command.
#[derive(Clone, Debug)]
pub struct Cx {
    now: Timestamp,
    command_id: CommandId,
}

impl Cx {
    pub fn new(now: Timestamp, command_id: CommandId) -> Self {
        Self { now, command_id }
    }

    /// The time the command is decided. Its events record the same time.
    pub fn now(&self) -> Timestamp {
        self.now
    }

    pub fn command_id(&self) -> &CommandId {
        &self.command_id
    }
}

/// Declares a string id type: `domain::id!(pub struct AccountId);`
#[macro_export]
macro_rules! id {
    ($(#[$meta:meta])* $vis:vis struct $name:ident;) => {
        $(#[$meta])*
        #[derive(::std::clone::Clone, ::std::fmt::Debug, ::std::cmp::PartialEq, ::std::cmp::Eq,
                 ::std::cmp::PartialOrd, ::std::cmp::Ord, ::std::hash::Hash)]
        $vis struct $name(::std::string::String);

        impl $name {
            pub fn new(id: impl ::std::convert::Into<::std::string::String>) -> Self {
                Self(id.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl ::std::convert::From<&str> for $name {
            fn from(id: &str) -> Self {
                Self(id.into())
            }
        }

        impl ::std::convert::From<::std::string::String> for $name {
            fn from(id: ::std::string::String) -> Self {
                Self(id)
            }
        }
    };
}

#[doc(hidden)]
pub fn __assert_handles<A: Handles<C>, C: Command>() {}

#[doc(hidden)]
pub fn __assert_evolve<A: Evolve>() {}
