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

/// Converts a command's events into an aggregate's events. `#[derive(Command)]` implements it for
/// every aggregate whose events include all of the command's.
pub trait Widen<E>: crate::Events {
    fn widen(self) -> E;
}
