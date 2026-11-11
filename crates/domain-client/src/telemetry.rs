//! Telemetry through a client: the same reads in process and over a connection.

use crate::pending::Pending;
use crate::{Api, Backend, Client};
use domain::telemetry::{AggregateTrace, Catalog, Since, Status, Trace, ViewKey};
use domain_memory::{Memory, TraceFeed};
use std::sync::Arc;

/// Reads a model's [telemetry](domain::telemetry), for a dashboard. Get it with
/// [`Client::telemetry`]. A server serves it only to the connections its `Server::telemetry`
/// allows; others get `Err`.
#[derive(Clone)]
pub struct ClientTelemetry {
    backend: Backend,
}

impl<A: Api> Client<A> {
    /// The model's telemetry: its parts, where it stands, and a trace of what happens in it.
    pub fn telemetry(&self) -> ClientTelemetry {
        ClientTelemetry { backend: self.backend.clone() }
    }
}

impl ClientTelemetry {
    /// The aggregates, views, policies, projections and sagas, and what each reads.
    pub fn catalog(&self) -> Pending<Result<Catalog, String>> {
        self.read(|memory| Ok(memory.telemetry().catalog()), domain::wire::Op::Catalog)
    }

    /// Every stream's version, and what's failing.
    pub fn status(&self) -> Pending<Result<Status, String>> {
        self.read(|memory| memory.telemetry().status(), domain::wire::Op::Status)
    }

    /// The aggregate instance recording `stream`, such as `Account/sam-main`, at version `at`, or now.
    pub fn aggregate(&self, stream: &str, at: Option<u64>) -> Pending<Result<Option<AggregateTrace>, String>> {
        let op = domain::wire::Op::Aggregate { stream: stream.into(), at };
        self.read(|memory| memory.telemetry().aggregate(stream, at), op)
    }

    /// Every key of the view named `view` that has a state.
    pub fn view(&self, view: &str) -> Pending<Result<Vec<ViewKey>, String>> {
        let op = domain::wire::Op::ViewKeys { view: view.into() };
        self.read(|memory| memory.telemetry().view(view).ok_or_else(|| format!("no view {view} is hosted here")), op)
    }

    /// What happened since `since`, then [`Trace::CaughtUp`], then what happens, until dropped.
    /// Over a connection it carries on after a reconnect, replaying only what it missed, then
    /// sends `CaughtUp` again. It ends with [`Trace::Failed`] when it can't go on.
    pub fn trace(&self, since: Since) -> TraceFeed {
        match &self.backend {
            Backend::Local { memory, .. } => memory.telemetry().trace(since).unwrap_or_else(|reason| {
                let (publisher, feed) = domain_memory::channel(());
                publisher.push(Trace::Failed(reason));
                feed
            }),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => remote.trace(since),
        }
    }

    #[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
    fn read<T: serde::de::DeserializeOwned + Send + 'static>(
        &self,
        local: impl FnOnce(&Arc<Memory>) -> Result<T, String>,
        op: domain::wire::Op,
    ) -> Pending<Result<T, String>> {
        match &self.backend {
            Backend::Local { memory, .. } => Pending::ready(local(memory)),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => remote.telemetry(op),
        }
    }
}
