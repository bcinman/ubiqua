//! Running projections: each key's inputs, in order and in batches, into a store you own. A batch
//! that fails or panics runs again later; other keys carry on meanwhile. A projection added when
//! events already exist receives them first, and so does one whose version changed.

use crate::calls::BoxFuture;
use crate::policies::{Next, PolicyRun};
use crate::store::{Store, StoreError, ViewRows};
use crate::views::{Consumer, Delivery};
use crate::{HostSources, Memory, Registrar, lock, reason};
use domain::host::{Consumes, Keyed, SourcePayload, Sources};
use domain::{MaybeSend, NotCaughtUp, Position, Projection, Source, StreamId, Timestamp};
use std::any::TypeId;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// How a projection writes its inputs to its store. Write it as an `async fn`:
///
/// ```text
/// impl Project for Ledger {
///     async fn project(&self, account: &AccountId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> { .. }
/// }
/// ```
pub trait Project: Projection {
    /// The most inputs one call gets.
    const BATCH: usize = 100;

    /// Writes one key's inputs, oldest first. Return `Err` to have the same batch run again
    /// later. Inputs arrive at least once, so every write must be safe to repeat: key what you
    /// write by the input's `stream` and `seq`.
    fn project(
        &self,
        key: &Self::Key,
        batch: Vec<ProjectionInput<Self>>,
    ) -> impl Future<Output = Result<(), ProjectionError>> + MaybeSend;
}

/// Why a projection's handler failed. Any error converts with `?`.
pub type ProjectionError = crate::PolicyError;

/// One event for a projection, with where and when it was recorded.
pub struct ProjectionInput<P: Keyed> {
    pub event: P::Input,
    pub stream: StreamId,
    pub seq: u64,
    pub at: Timestamp,
    delivery: Arc<Delivery>,
}

impl<P: Keyed> ProjectionInput<P> {
    /// The aggregate instance or view key that recorded the event, if it came from `S`.
    pub fn source<S: Source>(&self) -> Option<&S::SourceId> {
        (*self.delivery.payload).downcast_ref::<SourcePayload<S>>().map(|(id, _, _)| id)
    }

    /// If the event came from the view `S`, that key's state after it.
    pub fn state<S: Source>(&self) -> Option<&S::SourceState> {
        (*self.delivery.payload).downcast_ref::<SourcePayload<S>>().map(|(_, _, state)| state)
    }
}

impl<P: Keyed> Clone for ProjectionInput<P> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            stream: self.stream.clone(),
            seq: self.seq,
            at: self.at,
            delivery: self.delivery.clone(),
        }
    }
}

impl<P: Keyed> fmt::Debug for ProjectionInput<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionInput")
            .field("event", &self.event)
            .field("stream", &self.stream)
            .field("seq", &self.seq)
            .finish_non_exhaustive()
    }
}

/// The longest a failed batch waits before it runs again.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

type RouteFn<P> = fn(&Delivery) -> Option<(<P as Keyed>::Key, <P as Keyed>::Input)>;

fn route_of<P: Consumes<S>, S: Source>(delivery: &Delivery) -> Option<(P::Key, P::Input)> {
    let (id, event, state) = (*delivery.payload).downcast_ref::<SourcePayload<S>>()?;
    P::route(id, event, state)
}

pub(crate) struct ProjectionHost<P: Project> {
    projection: Arc<P>,
    routes: HashMap<TypeId, RouteFn<P>>,
    inbox: Mutex<Inbox<P>>,
    /// Each key's batch in flight, and its retries.
    running: Mutex<HashMap<P::Key, KeyRun>>,
    store: Option<Arc<dyn Store>>,
}

struct Inbox<P: Keyed> {
    queues: HashMap<P::Key, VecDeque<ProjectionInput<P>>>,
    /// Each stream's routed inputs not written yet. A stream's progress stops before the first.
    outstanding: HashMap<StreamId, BTreeSet<u64>>,
    /// The last seq received from each stream, routed or not.
    seen: HashMap<StreamId, u64>,
    /// Progress the store has.
    saved: HashMap<StreamId, u64>,
}

#[derive(Default)]
struct KeyRun {
    /// The batch in flight and how many inputs it holds.
    current: Option<(BoxFuture<Result<(), ProjectionError>>, usize)>,
    attempts: u32,
    retry_at: Option<Timestamp>,
    failure: Option<String>,
}

impl<P: Keyed> Inbox<P> {
    /// How far the projection has written `stream`: up to the first input not written yet.
    fn progress(&self, stream: &StreamId) -> u64 {
        match self.outstanding.get(stream).and_then(|seqs| seqs.first()) {
            Some(first) => first - 1,
            None => self.seen.get(stream).copied().unwrap_or(0),
        }
    }
}

impl<P: Project> ProjectionHost<P> {
    fn stored_name() -> String {
        format!("projection:{}", P::NAME)
    }

    /// Takes a written batch off its key's queue and saves the progress it made.
    fn written(&self, key: &P::Key, count: usize) {
        let mut inbox = lock(&self.inbox);
        let queue = inbox.queues.get_mut(key).expect("a batch comes from its key's queue");
        let written: Vec<(StreamId, u64)> = queue.drain(..count).map(|input| (input.stream, input.seq)).collect();
        if queue.is_empty() {
            inbox.queues.remove(key);
        }
        for (stream, seq) in &written {
            if let Some(seqs) = inbox.outstanding.get_mut(stream) {
                seqs.remove(seq);
            }
        }
        let Some(store) = &self.store else { return };
        let streams: BTreeSet<StreamId> = written.into_iter().map(|(stream, _)| stream).collect();
        let progress: Vec<(StreamId, u64)> = streams
            .into_iter()
            .map(|stream| {
                let at = inbox.progress(&stream);
                (stream, at)
            })
            .filter(|(stream, at)| inbox.saved.get(stream) != Some(at))
            .collect();
        let rows = ViewRows {
            progress: progress.iter().map(|(stream, at)| (stream.to_string(), *at)).collect(),
            ..ViewRows::default()
        };
        // If this fails, a restart writes these inputs again, which is safe.
        if !rows.progress.is_empty() && store.save_view(&Self::stored_name(), &rows).is_ok() {
            inbox.saved.extend(progress);
        }
    }
}

impl<P: Project> PolicyRun for ProjectionHost<P> {
    fn run(&self, memory: &Arc<Memory>, waker: &Waker) -> Next {
        let mut running = lock(&self.running);
        let keys: Vec<P::Key> = lock(&self.inbox).queues.keys().cloned().collect();
        let mut next = Next::Idle;
        let retry = |at: Timestamp, next: &mut Next| {
            *next = match next {
                Next::RetryAt(earlier) if *earlier <= at => Next::RetryAt(*earlier),
                _ => Next::RetryAt(at),
            }
        };
        for key in keys {
            let run = running.entry(key.clone()).or_default();
            loop {
                if run.current.is_none() {
                    if let Some(at) = run.retry_at
                        && memory.now() < at
                    {
                        retry(at, &mut next);
                        break;
                    }
                    let batch: Vec<ProjectionInput<P>> = match lock(&self.inbox).queues.get(&key) {
                        Some(queue) => queue.iter().take(P::BATCH.max(1)).cloned().collect(),
                        None => Vec::new(),
                    };
                    if batch.is_empty() {
                        break;
                    }
                    let (projection, key, count) = (self.projection.clone(), key.clone(), batch.len());
                    run.current = Some((Box::pin(async move { projection.project(&key, batch).await }), count));
                }
                let (future, count) = run.current.as_mut().expect("set above");
                let count = *count;
                let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    future.as_mut().poll(&mut Context::from_waker(waker))
                }));
                let failed = match polled {
                    Ok(Poll::Pending) => {
                        if matches!(next, Next::Idle) {
                            next = Next::Waiting;
                        }
                        break;
                    }
                    Ok(Poll::Ready(Ok(()))) => None,
                    Ok(Poll::Ready(Err(error))) => Some(error.to_string()),
                    Err(panic) => Some(reason(&*panic)),
                };
                run.current = None;
                match failed {
                    None => {
                        *run = KeyRun::default();
                        self.written(&key, count);
                    }
                    Some(error) => {
                        run.attempts += 1;
                        let backoff = Duration::from_secs(1 << (run.attempts - 1).min(6)).min(MAX_BACKOFF);
                        let at = memory.now().after(backoff);
                        run.retry_at = Some(at);
                        run.failure = Some(format!("{} failed on {key}: {error}", P::NAME));
                        retry(at, &mut next);
                        break;
                    }
                }
            }
        }
        running.retain(|_, run| run.current.is_some() || run.retry_at.is_some());
        next
    }

    fn failures(&self) -> Vec<String> {
        lock(&self.running).values().filter_map(|run| run.failure.clone()).collect()
    }
}

impl<P: Project> Consumer for ProjectionHost<P> {
    fn push(&self, delivery: Arc<Delivery>) {
        let mut inbox = lock(&self.inbox);
        let seen = inbox.seen.entry(delivery.stream.clone()).or_default();
        *seen = (*seen).max(delivery.seq);
        let Some((key, event)) = self.routes[&delivery.source](&delivery) else { return };
        inbox.outstanding.entry(delivery.stream.clone()).or_default().insert(delivery.seq);
        let input = ProjectionInput {
            event,
            stream: delivery.stream.clone(),
            seq: delivery.seq,
            at: delivery.at,
            delivery: delivery.clone(),
        };
        inbox.queues.entry(key).or_default().push_back(input);
    }

    // A projection runs in `Memory::run_due`, not in the commit's thread.
    fn drain(&self) {}

    fn catch_up(&self) {}

    fn applied(&self, _: &StreamId) -> Option<u64> {
        None
    }

    fn history(&self) -> Vec<Arc<Delivery>> {
        Vec::new()
    }

    fn add_downstream(&self, _: Arc<dyn Consumer>) {}
}

struct Routes<P: Keyed> {
    routes: HashMap<TypeId, RouteFn<P>>,
}

impl<P: Keyed> Sources<P> for Routes<P> {
    fn source<S: Source>(&mut self)
    where
        P: Consumes<S>,
    {
        self.routes.insert(TypeId::of::<S>(), route_of::<P, S>);
    }
}

impl Memory {
    /// Runs `projection` on the events it reads, hosting the views it reads. It receives what
    /// it hasn't written yet first, oldest first: all of history when it's new, or when its
    /// [`VERSION`](Projection::VERSION) changed; with a store, only what it missed otherwise.
    /// Projections run in [`run_due`](Self::run_due).
    ///
    /// # Panics
    /// When the store fails.
    pub fn projection<P: Project>(mut self, projection: P) -> Self {
        P::sources(&mut HostSources {
            registrar: &mut Registrar { memory: &mut self, hosting: Default::default(), rebuilt: Default::default() },
        });
        let mut routes = Routes::<P> { routes: HashMap::new() };
        P::sources(&mut routes);
        let name = ProjectionHost::<P>::stored_name();
        fn or_panic<T>(result: Result<T, StoreError>, name: &str) -> T {
            result.unwrap_or_else(|e| panic!("reading {name} from the store failed: {e}"))
        }
        let mut saved = HashMap::new();
        if let Some(store) = &self.store {
            if or_panic(store.view_version(&name), &name) == Some(P::VERSION) {
                let rows = or_panic(store.load_view(&name), &name);
                saved = rows.progress.into_iter().map(|(stream, seq)| (StreamId::from(stream), seq)).collect();
            } else {
                or_panic(store.reset_view(&name, P::VERSION), &name);
            }
        }
        let host = Arc::new(ProjectionHost {
            projection: Arc::new(projection),
            routes: routes.routes,
            inbox: Mutex::new(Inbox {
                queues: HashMap::new(),
                outstanding: HashMap::new(),
                seen: saved.clone(),
                saved: saved.clone(),
            }),
            running: Mutex::default(),
            store: self.store.clone(),
        });

        let sources: Vec<TypeId> = host.routes.keys().copied().collect();
        let keep = |stream: &str, seq: u64| seq > saved.get(stream).copied().unwrap_or(0);
        let mut history = Vec::new();
        for source in &sources {
            if let Some(upstream) = self.view_consumers.get(source) {
                history.extend(upstream.history().into_iter().filter(|d| keep(d.stream.as_str(), d.seq)));
            } else if let Some(read) = self.histories.get(source) {
                history.extend(read(&self, &keep));
            }
        }
        history.sort_by(|a, b| a.order().cmp(&b.order()));
        history.into_iter().for_each(|delivery| host.push(delivery));

        self.add_part(domain::telemetry::Kind::Projection, TypeId::of::<P>(), P::NAME, &sources, P::VERSION);
        for source in sources {
            match self.view_consumers.get(&source) {
                Some(upstream) => upstream.add_downstream(host.clone()),
                None => self.consumers.entry(source).or_default().push(host.clone()),
            }
        }
        self.hosts.insert(TypeId::of::<P>(), Box::new(host.clone()));
        self.runners.push(host);
        self
    }

    /// Checks `P` has written everything up to `position`, such as a command's receipt, so a
    /// read of its store sees that command. Fails when it hasn't, or when the stream doesn't
    /// reach it directly.
    ///
    /// # Panics
    /// When `P` isn't hosted here.
    pub fn caught_up<P: Project>(&self, position: impl Into<Position>) -> Result<(), NotCaughtUp> {
        let host =
            self.hosts.get(&TypeId::of::<P>()).and_then(|host| (**host).downcast_ref::<Arc<ProjectionHost<P>>>());
        let host = host.unwrap_or_else(|| panic!("{} isn't hosted here: add it with `Memory::projection`", P::NAME));
        let Position { stream, version } = position.into();
        let applied = lock(&host.inbox).progress(&stream);
        if applied >= version { Ok(()) } else { Err(NotCaughtUp { view: P::NAME, stream, wanted: version, applied }) }
    }
}
