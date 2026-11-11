//! Projections and sagas: what reads events into a store you own, and what carries out a process
//! that spans several aggregates.

use crate::Rejections;
use crate::host::{Keyed, Sources};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::fmt::{Debug, Display};
use std::hash::Hash;

/// Reads events into a store you own: a table, a search index, a file. It never sends commands,
/// so it can always run again over the same events. Declare it with `#[domain::projection(..)]`, then
/// implement the host's handler trait (`domain_memory::Project`).
pub trait Projection: Keyed {
    const NAME: &'static str;
    /// Change it to run the projection over the whole history again, such as after its store
    /// was lost or it writes a new shape. `#[domain::projection(version = 1)]` sets it.
    const VERSION: u64 = 0;

    /// Calls `sources.source::<S>()` for each aggregate or view it reads.
    fn sources<S: Sources<Self>>(sources: &mut S);
}

/// A process across several aggregates, run as steps that each commit on their own. When a step
/// is refused after earlier ones succeeded, the steps' compensations undo them, last first.
/// The struct is the run's input. Declare it with `#[domain::saga(..)]`, then implement the host's
/// handler trait (`domain_memory::Run`).
pub trait Saga: Serialize + DeserializeOwned + 'static {
    const NAME: &'static str;
    /// Identifies one run: starting it again with the same key returns the same run.
    type Key: Clone + Eq + Hash + Debug + Display + Serialize + DeserializeOwned + 'static;
    /// Every way a run can be refused: `#[domain::saga(rejects(..))]`.
    type Rejection: Rejections;
}

/// Converts a command's rejections into a saga's. `#[domain::command(..)]` implements it for every
/// rejection set that includes all of the command's, and a saga's rejections convert from it with `?`.
#[diagnostic::on_unimplemented(label = "add `{Self}` to the saga's `rejects(..)`, or handle it before `?`")]
pub trait WidenRejection<R>: Rejections {
    fn widen(self) -> R;
}
