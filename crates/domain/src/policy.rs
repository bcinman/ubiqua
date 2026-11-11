//! Policies: business rules that run after events. "Whenever a customer registers, open their
//! main account." A host delivers each event to the policies that react to it.

use crate::{Events, MaybeSend, MaybeSync, Source};

/// Reacts to events from aggregates or views, after they commit. Derive it with
/// `#[derive(Policy)]`, then implement the host's handler trait (`domain_memory::React`).
pub trait Policy: Sized + MaybeSend + MaybeSync + 'static {
    const NAME: &'static str;
    /// The events it reacts to, as an enum. The derive generates it.
    type Input: Events;

    /// Calls `sources.source::<S>()` for each aggregate or view it reacts to.
    fn sources<S: PolicySources<Self>>(sources: &mut S);
}

/// Picks out the events a policy reacts to from one source. `#[derive(Policy)]` implements it.
pub trait Reacts<S: Source>: Policy {
    /// The input for this event, or `None` when the policy ignores it.
    fn route(event: &S::SourceEvent) -> Option<Self::Input>;
}

/// Visits the sources a policy reacts to. Hosts implement it.
pub trait PolicySources<P: Policy> {
    fn source<S: Source>(&mut self)
    where
        P: Reacts<S>;
}
