//! Running policies: each one has an inbox of deliveries, handled one at a time in order, after
//! the commit that recorded them. A handler that fails or panics runs again later for the same
//! event, and every command it sends gets an id derived from that event, so running twice is safe.

use crate::calls::{BoxFuture, unbox};
use crate::store::{Store, StoreError, ViewRows};
use crate::views::{Consumer, Delivery};
use crate::{Acting, AggregateOf, Memory, Registrar, Run, ScheduleError, SignalError, UnscheduleError, lock, reason};
use domain::host::{CurrentActor, PolicySources, Reacts, SourcePayload};
use domain::{
    AggregateId, Answered, Call, Command, CommandId, Evolve, Handles, MaybeSend, MaybeSync, NeverRejects, Policy,
    Receipt, SendError, Source, StreamId, Timestamp,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// How a policy handles its events. Write it as an `async fn`:
///
/// ```ignore
/// impl React for FraudReview {
///     async fn react(&self, cx: &PolicyCx<Self>, input: FraudReviewInput) -> Result<(), PolicyError> { .. }
/// }
/// ```
pub trait React: Policy {
    /// Acts on one event. Return `Err` to have it run again later for the same event.
    fn react(
        &self,
        cx: &PolicyCx<Self>,
        input: Self::Input,
    ) -> impl Future<Output = Result<(), PolicyError>> + MaybeSend;
}

/// Why a policy's handler failed. Any error converts with `?`.
pub struct PolicyError(String);

impl PolicyError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

impl<E: std::error::Error> From<E> for PolicyError {
    fn from(error: E) -> Self {
        Self(error.to_string())
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PolicyError({:?})", self.0)
    }
}

/// What a policy may read and do while it handles one event.
pub struct PolicyCx<P: Policy> {
    memory: Weak<Memory>,
    delivery: Arc<Delivery>,
    /// Counts the command ids handed out, so a second run derives the same ones.
    ids: AtomicU32,
    _policy: PhantomData<fn() -> P>,
}

impl<P: Policy> PolicyCx<P> {
    fn memory(&self) -> Arc<Memory> {
        self.memory.upgrade().expect("a policy runs while its memory is alive")
    }

    /// The aggregate instance or view key that recorded the event, if it came from `S`.
    pub fn source<S: Source>(&self) -> Option<&S::SourceId> {
        (*self.delivery.payload).downcast_ref::<SourcePayload<S>>().map(|(id, _, _)| id)
    }

    /// The aggregate instance or view key that recorded the event, for a policy that reacts to one
    /// source.
    pub fn source_id(&self) -> &<P::Source as Source>::SourceId
    where
        P: domain::host::OneSource,
    {
        self.source::<P::Source>().expect("a policy's events come from the sources it reacts to")
    }

    /// If the event came from the view `S`, that key's state after it.
    pub fn state<S: Source>(&self) -> Option<&S::SourceState> {
        (*self.delivery.payload).downcast_ref::<SourcePayload<S>>().map(|(_, _, state)| state)
    }

    /// The stream that recorded the event, and its position there.
    pub fn stream(&self) -> (&StreamId, u64) {
        (&self.delivery.stream, self.delivery.seq)
    }

    /// When the event was recorded.
    pub fn at(&self) -> Timestamp {
        self.delivery.at
    }

    /// The time now, by the memory's clock.
    pub fn now(&self) -> Timestamp {
        self.memory().now()
    }

    /// The next command id derived from this event: the same, in the same order, every time the
    /// policy runs for it. [`send`](Self::send) and [`schedule`](Self::schedule) use these.
    pub fn next_command_id(&self) -> CommandId {
        let n = self.ids.fetch_add(1, Ordering::Relaxed) + 1;
        CommandId::new(format!("{}:{}:{}:{n}", P::NAME, self.delivery.stream, self.delivery.seq))
    }

    /// Sends a command under the next derived id, so a second run returns the first receipt.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.memory().send_with_id(id, command, self.next_command_id())
    }

    /// Sends the commands after it on behalf of `actor`, for a `decide` that checks who sent it.
    /// A policy has no actor otherwise.
    pub fn acting_as<T: Any + fmt::Debug + MaybeSend + MaybeSync>(&self, actor: T) -> Acting<'_, Self> {
        Acting { host: self, actor: CurrentActor::new(actor) }
    }

    /// Sends `command` to `id` at `at`, under the next derived id. A second run after it fired
    /// fails with [`ScheduleError::AlreadyExecuted`].
    pub fn schedule<C>(&self, id: &C::Target, command: C, at: Timestamp) -> Result<CommandId, ScheduleError>
    where
        C: NeverRejects,
        AggregateOf<C::Target>: Handles<C>,
    {
        let command_id = self.next_command_id();
        self.memory().schedule(id, command, at, command_id.clone())?;
        Ok(command_id)
    }

    /// Like [`schedule`](Self::schedule), under an id you can name again to cancel it.
    pub fn schedule_with_id<C>(
        &self,
        id: &C::Target,
        command: C,
        at: Timestamp,
        command_id: impl Into<CommandId>,
    ) -> Result<(), ScheduleError>
    where
        C: NeverRejects,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.memory().schedule(id, command, at, command_id)
    }

    /// Cancels a scheduled command.
    pub fn unschedule<I>(&self, id: &I, command_id: impl Into<CommandId>) -> Result<(), UnscheduleError>
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        self.memory().unschedule(id, command_id)
    }

    /// Starts a run of the saga `S` under `key`. Starting under a key already started does
    /// nothing, so a policy that runs twice starts one run: key it by what the run is about.
    pub fn start_saga<S: Run>(&self, key: &S::Key, input: S) -> Result<(), StoreError> {
        self.memory().start_saga(key, input)
    }

    /// Sends a saga's run a signal. The first value sent under a name is the one it gets.
    pub fn signal<S: Run>(&self, key: &S::Key, name: &str, value: impl serde::Serialize) -> Result<(), SignalError> {
        self.memory().signal::<S>(key, name, value)
    }

    /// Calls an external system. Pass it something that identifies the request, so the vendor
    /// acts once however many times the policy runs.
    pub async fn call<C: Call>(&self, call: C) -> Answered<C> {
        let answer = self.memory().start_call(call);
        unbox::<C>(answer.await)
    }
}

/// The longest a failed policy waits before it runs again.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// What a policy is doing after a run.
pub(crate) enum Next {
    /// Nothing left to handle.
    Idle,
    /// Waiting for a call to answer.
    Waiting,
    /// Failed; it runs the event again at this time.
    RetryAt(Timestamp),
}

/// A policy as `Memory::run_due` sees it.
pub(crate) trait PolicyRun: MaybeSend + MaybeSync {
    /// Handles deliveries in order until one waits or fails, or none is left.
    fn run(&self, memory: &Arc<Memory>, waker: &Waker) -> Next;
    /// What's failing now, until it succeeds.
    fn failures(&self) -> Vec<String>;
}

type RouteFn<P> = fn(&Delivery) -> Option<<P as Policy>::Input>;

fn route_of<P: Reacts<S>, S: Source>(delivery: &Delivery) -> Option<P::Input> {
    let (_, event, _) = (*delivery.payload).downcast_ref::<SourcePayload<S>>()?;
    P::route(event)
}

pub(crate) struct PolicyHost<P: React> {
    policy: Arc<P>,
    routes: HashMap<TypeId, RouteFn<P>>,
    inbox: Mutex<VecDeque<Arc<Delivery>>>,
    running: Mutex<Running>,
    store: Option<Arc<dyn Store>>,
}

struct Running {
    /// The handler for the event at the front of the inbox, while it waits.
    current: Option<BoxFuture<Result<(), PolicyError>>>,
    attempts: u32,
    retry_at: Option<Timestamp>,
    failure: Option<String>,
    progress: HashMap<StreamId, u64>,
}

impl<P: React> PolicyHost<P> {
    /// The name its progress is stored under.
    fn stored_name() -> String {
        format!("policy:{}", P::NAME)
    }

    /// Marks the front delivery handled.
    fn done(&self, running: &mut Running, delivery: &Delivery) {
        lock(&self.inbox).pop_front();
        running.progress.insert(delivery.stream.clone(), delivery.seq);
        if let Some(store) = &self.store {
            // If this fails, a restart runs the event again, which is safe.
            let rows = ViewRows { progress: vec![(delivery.stream.to_string(), delivery.seq)], ..ViewRows::default() };
            let _ = store.save_view(&Self::stored_name(), &rows);
        }
    }
}

impl<P: React> PolicyRun for PolicyHost<P> {
    fn run(&self, memory: &Arc<Memory>, waker: &Waker) -> Next {
        let mut running = lock(&self.running);
        loop {
            let Some(delivery) = lock(&self.inbox).front().cloned() else { return Next::Idle };
            if running.current.is_none() {
                if let Some(at) = running.retry_at
                    && memory.now() < at
                {
                    return Next::RetryAt(at);
                }
                // History, such as a story's `given`, already happened: it isn't acted on.
                let input = if delivery.history { None } else { self.routes[&delivery.source](&delivery) };
                let Some(input) = input else {
                    self.done(&mut running, &delivery);
                    continue;
                };
                let cx = PolicyCx::<P> {
                    memory: Arc::downgrade(memory),
                    delivery: delivery.clone(),
                    ids: AtomicU32::new(0),
                    _policy: PhantomData,
                };
                let policy = self.policy.clone();
                running.current = Some(Box::pin(async move { policy.react(&cx, input).await }));
            }
            let future = running.current.as_mut().expect("set above");
            let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future.as_mut().poll(&mut Context::from_waker(waker))
            }));
            let failed = match polled {
                Ok(Poll::Pending) => return Next::Waiting,
                Ok(Poll::Ready(Ok(()))) => None,
                Ok(Poll::Ready(Err(error))) => Some(error.to_string()),
                Err(panic) => Some(reason(&*panic)),
            };
            running.current = None;
            match failed {
                None => {
                    running.attempts = 0;
                    running.retry_at = None;
                    running.failure = None;
                    self.done(&mut running, &delivery);
                }
                Some(error) => {
                    running.attempts += 1;
                    let backoff = Duration::from_secs(1 << (running.attempts - 1).min(6)).min(MAX_BACKOFF);
                    let at = memory.now().after(backoff);
                    running.retry_at = Some(at);
                    running.failure =
                        Some(format!("{} failed on {} #{}: {error}", P::NAME, delivery.stream, delivery.seq));
                    return Next::RetryAt(at);
                }
            }
        }
    }

    fn failures(&self) -> Vec<String> {
        lock(&self.running).failure.iter().cloned().collect()
    }
}

impl<P: React> Consumer for PolicyHost<P> {
    fn push(&self, delivery: Arc<Delivery>) {
        lock(&self.inbox).push_back(delivery);
    }

    // A policy runs in `Memory::run_due`, not in the commit's thread.
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

impl Memory {
    /// Runs `policy` on the events it reacts to from now on, hosting the views it reads.
    ///
    /// A policy new to the store doesn't act on the past. One stored before acts on what it
    /// missed since. Policies run in [`run_due`](Self::run_due).
    ///
    /// # Panics
    /// When the store fails.
    pub fn policy<P: React>(mut self, policy: P) -> Self {
        assert!(
            self.hosts.insert(TypeId::of::<P>(), Box::new(())).is_none(),
            "{} is hosted twice: list it in the model's `policies(..)` or add it with `policy`, not both",
            P::NAME
        );
        P::sources(&mut HostSources {
            registrar: &mut Registrar { memory: &mut self, hosting: Default::default(), rebuilt: Default::default() },
        });
        let mut routes = Routes::<P> { routes: HashMap::new() };
        P::sources(&mut routes);
        let host = Arc::new(PolicyHost {
            policy: Arc::new(policy),
            routes: routes.routes,
            inbox: Mutex::default(),
            running: Mutex::new(Running {
                current: None,
                attempts: 0,
                retry_at: None,
                failure: None,
                progress: HashMap::new(),
            }),
            store: self.store.clone(),
        });

        let name = PolicyHost::<P>::stored_name();
        fn or_panic<T>(result: Result<T, StoreError>, name: &str) -> T {
            result.unwrap_or_else(|e| panic!("reading {name} from the store failed: {e}"))
        }
        let stored = match &self.store {
            Some(store) => {
                let version = or_panic(store.view_version(&name), &name);
                version.map(|_| or_panic(store.load_view(&name), &name).progress)
            }
            None => None,
        };
        let sources: Vec<TypeId> = host.routes.keys().copied().collect();
        let progress: HashMap<StreamId, u64> =
            stored.iter().flatten().map(|(stream, seq)| (StreamId::from(stream.clone()), *seq)).collect();
        let keep = |stream: &str, seq: u64| seq > progress.get(stream).copied().unwrap_or(0);
        let mut history = Vec::new();
        for source in &sources {
            if let Some(upstream) = self.view_consumers.get(source) {
                history.extend(upstream.history().into_iter().filter(|d| keep(d.stream.as_str(), d.seq)));
            } else if let Some(read) = self.histories.get(source) {
                history.extend(read(&self, &keep));
            }
        }

        {
            let mut running = lock(&host.running);
            running.progress = progress.clone();
            if stored.is_some() {
                // Act on what it missed, oldest first.
                history.sort_by(|a, b| a.order().cmp(&b.order()));
                lock(&host.inbox).extend(history);
            } else {
                // New: start from now.
                for delivery in &history {
                    let seq = running.progress.entry(delivery.stream.clone()).or_default();
                    *seq = (*seq).max(delivery.seq);
                }
                if let Some(store) = &self.store {
                    or_panic(store.reset_view(&name, 0), &name);
                    let rows = ViewRows {
                        progress: running.progress.iter().map(|(s, seq)| (s.to_string(), *seq)).collect(),
                        ..ViewRows::default()
                    };
                    or_panic(store.save_view(&name, &rows), &name);
                }
            }
        }

        for source in sources {
            match self.view_consumers.get(&source) {
                Some(upstream) => upstream.add_downstream(host.clone()),
                None => self.consumers.entry(source).or_default().push(host.clone()),
            }
        }
        self.runners.push(host);
        self
    }
}

/// Hosts each source of a policy that's a view.
struct HostSources<'r, 'm> {
    registrar: &'r mut Registrar<'m>,
}

impl<P: Policy> PolicySources<P> for HostSources<'_, '_> {
    fn source<S: Source>(&mut self)
    where
        P: Reacts<S>,
    {
        S::register_source(self.registrar);
    }
}

struct Routes<P: Policy> {
    routes: HashMap<TypeId, RouteFn<P>>,
}

impl<P: Policy> PolicySources<P> for Routes<P> {
    fn source<S: Source>(&mut self)
    where
        P: Reacts<S>,
    {
        self.routes.insert(TypeId::of::<S>(), route_of::<P, S>);
    }
}

impl<P: Policy> Acting<'_, PolicyCx<P>> {
    /// [`PolicyCx::send`], on behalf of the actor.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.host.memory().send_as(Some(&self.actor), id, command, self.host.next_command_id())
    }
}

/// A policy in a model's `policies(..)`, built with `Default`.
impl<P: React + Default> domain::host::HostPolicy<P> for Memory {
    fn host_policy(self) -> Self {
        self.policy(P::default())
    }
}
