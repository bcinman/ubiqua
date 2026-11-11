//! What hosts and the derives use to run a model: registries, routing, conversions and the
//! actor behind a request. A model's own code doesn't name these.

pub use crate::actor::CurrentActor;
pub use crate::api::{ApiRegistry, SentVia};
pub use crate::external::{Adapter, Calls};
pub use crate::policy::{PolicySources, Reacts};
pub use crate::projection::WidenRejection;
pub use crate::view::{
    Consumes, HostPolicy, HostSaga, Hosts, KeyFrom, Keyed, Member, Predictions, Registry, SourcePayload, Sources,
};

/// A policy or view that reads exactly one source, so its context knows whose event it is.
/// `#[derive(Policy)]` and `#[derive(View)]` implement it when `from(..)` lists one source.
#[diagnostic::on_unimplemented(
    message = "`{Self}` reads more than one source, so `source_id()` can't say which",
    label = "call `source::<S>()` with the source you expect"
)]
pub trait OneSource {
    type Source: crate::Source;
}

/// Converts a command's events into an aggregate's events. `#[derive(Command)]` implements it for
/// every aggregate whose events include all of the command's.
pub trait Widen<E>: crate::Events {
    fn widen(self) -> E;
}
