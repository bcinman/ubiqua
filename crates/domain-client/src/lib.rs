//! What a UI uses to talk to a domain model: a typed [`Client`] for the model's Api.
//!
//! Nothing here depends on a UI framework or an async runtime. Results arrive as [`Pending`]
//! (check it each frame, or `.await` it on any executor) or through a callback, and changing state
//! arrives as a [`LiveView`] (read it each frame, get callbacks, or await changes).
//!
//! The model can run in the same process ([`Client::in_process`]) or on a server
//! ([`Client::connect`]). The methods are the same either way.

mod live;
mod pending;
mod predict;
#[cfg(not(target_arch = "wasm32"))]
mod remote;
mod telemetry;

#[doc(hidden)]
pub mod compile_checks;

pub use domain_memory::{Feed, Recv, Subscription};
#[cfg(not(target_arch = "wasm32"))]
pub use domain_websocket::ClientTls;
pub use live::{Changed, Listener, LiveView};
pub use pending::Pending;
pub use telemetry::ClientTelemetry;

use domain::host::{CurrentActor, SentVia};
use domain::{
    AggregateId, Api, Apply, Command, CommandId, Cursor, Entry, Frame, Handles, MaybeSend, MaybeSync, NotCaughtUp,
    Position, ReadCx, Reads, Receipt, SendError, Snapshot, Validate, View,
};
use domain_memory::{Memory, Publisher, ViewReader, Watch};
use live::Shared;
use pending::{lock, pending};
use predict::{Overlay, Predictor};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

type AggregateOf<I> = <I as AggregateId>::Aggregate;

/// What a send resolves to: the receipt, or why it didn't commit.
pub type Sent<C> = Result<Receipt<C>, SendError<<C as Command>::Rejection>>;

/// What a read of `V` resolves to: a `T`, or why the read failed.
pub type ReadResult<V, T> = Result<T, ReadError<<V as View>::Rejection>>;

/// What reading one key of `V` resolves to: its state (`None` before its first event), or why
/// the read failed.
pub type Read<V> = ReadResult<V, Option<<V as View>::State>>;

/// A typed client for the Api `A`. It sends only the commands `A` lists, and reads only its views.
pub struct Client<A: Api> {
    backend: Backend,
    actor: Option<CurrentActor>,
    /// Shared by clones, so a command sent through one shows on another's live views.
    predictor: Arc<Predictor>,
    _api: PhantomData<fn() -> A>,
}

impl<A: Api> Clone for Client<A> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            actor: self.actor.clone(),
            predictor: self.predictor.clone(),
            _api: PhantomData,
        }
    }
}

#[derive(Clone)]
enum Backend {
    Local {
        memory: Arc<Memory>,
        runner: Runner,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Remote(Arc<remote::Remote>),
}

#[derive(Clone)]
enum Runner {
    /// Commands run on the caller's thread.
    Inline,
    /// Commands run, in order, on the client's domain thread.
    #[cfg(not(target_arch = "wasm32"))]
    Thread(Arc<std::sync::mpsc::Sender<Job>>),
}

#[cfg(not(target_arch = "wasm32"))]
type Job = Box<dyn FnOnce(&Memory) + Send>;

/// How a client is connected to its model.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Connection {
    Connecting,
    /// In process, or connected to the server.
    Connected,
    /// The connection dropped or couldn't be made; it's trying again.
    Reconnecting {
        reason: String,
    },
    /// The server turned the client away with this HTTP status, such as 401 for a token it
    /// doesn't accept. It won't try again.
    Refused {
        status: u16,
    },
}

impl<A: Api> Client<A> {
    /// Runs commands on a domain thread of their own, so the UI thread never waits on one. The
    /// model's policies and scheduled commands run there too, between commands. On wasm, which
    /// has no threads, this is [`inline`](Self::inline).
    pub fn in_process(memory: Arc<Memory>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            use std::sync::mpsc::RecvTimeoutError;
            let (jobs, queue) = std::sync::mpsc::channel::<Job>();
            let jobs = Arc::new(jobs);
            // Wake the thread when there's work for policies. Weak, so the thread still stops
            // once every client is gone.
            let wake = Arc::downgrade(&jobs);
            memory.on_wake(move || {
                if let Some(jobs) = wake.upgrade() {
                    let _ = jobs.send(Box::new(|_| {}));
                }
            });
            let worker = memory.clone();
            std::thread::Builder::new()
                .name("domain".into())
                .spawn(move || {
                    let guard = std::panic::AssertUnwindSafe(&worker);
                    loop {
                        let due = std::panic::catch_unwind(|| guard.run_due()).ok().flatten();
                        let next = match due {
                            Some(at) => queue.recv_timeout(worker.now().until(at)),
                            None => queue.recv().map_err(|_| RecvTimeoutError::Disconnected),
                        };
                        match next {
                            // A panicking `send_then` callback mustn't stop the commands after it.
                            Ok(job) => {
                                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&worker)));
                            }
                            Err(RecvTimeoutError::Timeout) => {}
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }
                })
                .expect("spawn the domain thread");
            Self::local(memory, Runner::Thread(jobs))
        }
        #[cfg(target_arch = "wasm32")]
        Self::inline(memory)
    }

    /// Runs commands on the caller's thread: for tests, and anywhere waiting is fine. Policies
    /// run on whichever thread wakes them. Scheduled commands fire when something calls
    /// `Memory::run_due`; on the web, call it from a timer.
    pub fn inline(memory: Arc<Memory>) -> Self {
        let weak = Arc::downgrade(&memory);
        memory.on_wake(move || {
            if let Some(memory) = weak.upgrade() {
                memory.run_due();
            }
        });
        Self::local(memory, Runner::Inline)
    }

    fn local(memory: Arc<Memory>, runner: Runner) -> Self {
        Self { backend: Backend::Local { memory, runner }, actor: None, predictor: Arc::default(), _api: PhantomData }
    }

    /// Talks to a model a `domain-server` serves at `url`, such as `ws://127.0.0.1:7878/`, or
    /// `wss://bank.example.com/` over TLS, trusting the servers the system trusts. The server
    /// signs the connection in with `token`, if it checks sessions.
    ///
    /// Returns at once and connects in the background, reconnecting whenever the connection
    /// drops. Commands sent while it's down, or that it was carrying when it dropped, wait and
    /// are sent again once it's back, under the same command id, so each takes effect once; see
    /// [`unsent`](Self::unsent). Reads it was carrying fail with `Unavailable`. Live views and
    /// feeds carry on after a reconnect. Native only for now.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn connect(url: &str, token: Option<&str>) -> Self {
        let backend = Backend::Remote(remote::Remote::start(url, token, None));
        Self { backend, actor: None, predictor: Arc::default(), _api: PhantomData }
    }

    /// Like [`connect`](Self::connect), trusting the servers `tls` trusts on `wss://`, such as a
    /// server whose certificate your own authority signed.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn connect_with(url: &str, token: Option<&str>, tls: ClientTls) -> Self {
        let backend = Backend::Remote(remote::Remote::start(url, token, Some(tls)));
        Self { backend, actor: None, predictor: Arc::default(), _api: PhantomData }
    }

    /// The same client, sending and reading on behalf of `actor`, as a signed-in user would.
    /// `decide` reads it with `cx.actor::<T>()`, and views' `validate` with `cx.actor::<T>()`.
    ///
    /// # Panics
    /// On a client from [`connect`](Self::connect): the server decides who that is, from its token.
    pub fn acting_as<T: std::any::Any + fmt::Debug + MaybeSend + MaybeSync>(&self, actor: T) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        assert!(
            matches!(self.backend, Backend::Local { .. }),
            "a connected client acts as whoever its token signs in; pass the token to `connect`"
        );
        Self { actor: Some(CurrentActor::new(actor)), ..self.clone() }
    }

    /// How the client is connected. Always `Connected` in process.
    pub fn connection(&self) -> LiveView<Connection> {
        match &self.backend {
            Backend::Local { .. } => LiveView::new(Shared::new(Connection::Connected), Arc::new(())),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => remote.connection(),
        }
    }

    /// How many commands are waiting for an answer, such as while offline. Always 0 in process.
    /// They're kept in memory, so quitting the app drops them.
    pub fn unsent(&self) -> LiveView<usize> {
        match &self.backend {
            Backend::Local { .. } => LiveView::new(Shared::new(0), Arc::new(())),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => remote.unsent(),
        }
    }

    /// Sends a command. Live views of views that predict it (`#[view(predicts(..))]`) show its
    /// change at once, until the answer arrives. Retrying after [`SendError::Unavailable`] with
    /// [`send_with_id`](Self::send_with_id) and the same id takes effect at most once.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Pending<Sent<C>>
    where
        C: Command,
        C: SentVia<A>,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.send_with_id(id, command, self.new_command_id())
    }

    /// Like [`send`](Self::send), under your own command id.
    pub fn send_with_id<C>(&self, id: &C::Target, command: C, command_id: impl Into<CommandId>) -> Pending<Sent<C>>
    where
        C: Command,
        C: SentVia<A>,
        AggregateOf<C::Target>: Handles<C>,
    {
        let (pending, resolver) = pending();
        self.run(id, command, command_id.into(), move |sent| resolver.resolve(sent));
        pending
    }

    /// Sends a command and calls `then` with the result, on the thread that ran it: the domain
    /// thread for [`in_process`](Self::in_process), the connection's thread for
    /// [`connect`](Self::connect). For UIs whose handlers can't await.
    pub fn send_then<C>(&self, id: &C::Target, command: C, then: impl FnOnce(Sent<C>) + MaybeSend + 'static)
    where
        C: Command,
        C: SentVia<A>,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.run(id, command, self.new_command_id(), then);
    }

    /// A command id no other command has used.
    pub fn new_command_id(&self) -> CommandId {
        match &self.backend {
            Backend::Local { memory, .. } => memory.new_command_id(),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => remote.new_command_id(),
        }
    }

    /// Reads the view `V`.
    pub fn view<V: Apply + Validate>(&self) -> ClientView<V>
    where
        A: Reads<V>,
    {
        let inner = match &self.backend {
            Backend::Local { memory, .. } => ViewInner::Local { reader: memory.view::<V>(), actor: self.actor.clone() },
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => ViewInner::Remote(remote.clone()),
        };
        ClientView { inner, predictor: self.predictor.clone() }
    }

    fn run<C>(
        &self,
        id: &C::Target,
        command: C,
        command_id: CommandId,
        then: impl FnOnce(Sent<C>) + MaybeSend + 'static,
    ) where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        // Taken off the live views once the answer is in, before `then` sees it.
        let prediction = self.predictor.predict(id, &command);
        let then = move |sent| {
            drop(prediction);
            then(sent);
        };
        let (memory, runner) = match &self.backend {
            Backend::Local { memory, runner } => (memory, runner),
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => return remote.send(id, command, command_id, then),
        };
        let (id, actor) = (id.clone(), self.actor.clone());
        let job = move |memory: &Memory| {
            let sent = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                memory.send_as(actor.as_ref(), &id, command, command_id.clone())
            }));
            then(sent.unwrap_or_else(|panic| Err(SendError::Unavailable { command_id, reason: reason(&*panic) })));
        };
        match runner {
            Runner::Inline => job(memory),
            #[cfg(not(target_arch = "wasm32"))]
            Runner::Thread(jobs) => jobs.send(Box::new(job)).expect("the domain thread outlives its clients"),
        }
    }
}

fn reason(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic.downcast_ref::<&str>().copied().or(panic.downcast_ref::<String>().map(String::as_str));
    format!("the command panicked: {}", message.unwrap_or("no message"))
}

/// Why a read failed. `R` is the view's rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadError<R> {
    /// The view hasn't applied the position the read asked for.
    NotCaughtUp(NotCaughtUp),
    /// The view's `validate` refused the reader.
    Rejected(R),
    /// The server couldn't be reached, or failed.
    Unavailable(String),
}

impl<R: fmt::Display> fmt::Display for ReadError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::NotCaughtUp(e) => e.fmt(f),
            ReadError::Rejected(r) => write!(f, "rejected: {r}"),
            ReadError::Unavailable(reason) => write!(f, "unavailable: {reason}"),
        }
    }
}

impl<R: fmt::Debug + fmt::Display> std::error::Error for ReadError<R> {}

/// Reads one view through a [`Client`].
pub struct ClientView<V: Apply> {
    inner: ViewInner<V>,
    predictor: Arc<Predictor>,
}

enum ViewInner<V: Apply> {
    Local {
        reader: ViewReader<V>,
        actor: Option<CurrentActor>,
    },
    #[cfg(not(target_arch = "wasm32"))]
    Remote(Arc<remote::Remote>),
}

impl<V: Apply> Clone for ClientView<V> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            ViewInner::Local { reader, actor } => ViewInner::Local { reader: reader.clone(), actor: actor.clone() },
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => ViewInner::Remote(remote.clone()),
        };
        Self { inner, predictor: self.predictor.clone() }
    }
}

impl<V: Apply + Validate> ClientView<V> {
    /// The key's state, or `None` before its first event.
    pub fn get(&self, key: &V::Key) -> Pending<Read<V>> {
        match &self.inner {
            ViewInner::Local { reader, actor } => Pending::ready(checked::<V>(actor, key, reader.get(key))),
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.get::<V>(key, None),
        }
    }

    /// The key's state, once the view has applied `position`, such as a command's receipt.
    pub fn get_at_least(&self, key: &V::Key, position: impl Into<Position>) -> Pending<Read<V>> {
        match &self.inner {
            ViewInner::Local { reader, actor } => Pending::ready(match reader.get_at_least(key, position) {
                Ok(state) => checked::<V>(actor, key, state),
                Err(e) => Err(ReadError::NotCaughtUp(e)),
            }),
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.get::<V>(key, Some(position.into())),
        }
    }

    /// The key's state with the cursor it's valid at. Follow it with [`events`](Self::events).
    pub fn snapshot(&self, key: &V::Key) -> Pending<ReadResult<V, Snapshot<V>>> {
        match &self.inner {
            ViewInner::Local { reader, actor } => {
                let snapshot = reader.snapshot(key);
                Pending::ready(validate::<V>(actor.as_ref(), key, snapshot.state.as_ref()).map(|()| snapshot))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.snapshot::<V>(key),
        }
    }

    /// Up to `take` of the key's emitted events after `after`, oldest first.
    pub fn list(&self, key: &V::Key, after: Cursor, take: usize) -> Pending<ReadResult<V, Vec<Entry<V>>>> {
        match &self.inner {
            ViewInner::Local { reader, actor } => Pending::ready(
                validate::<V>(actor.as_ref(), key, reader.get(key).as_ref()).map(|()| reader.list(key, after, take)),
            ),
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.list::<V>(key, after, take),
        }
    }

    /// The key's emitted events after `after`, then `CaughtUp`, then new ones as they're
    /// recorded. If `validate` refuses the reader, now or on a later entry, the feed ends with
    /// `Frame::Rejected`. Over a connection, it carries on from the last entry after a
    /// reconnect, with another `CaughtUp`.
    pub fn events(&self, key: &V::Key, after: Cursor) -> Feed<V> {
        match &self.inner {
            ViewInner::Local { reader, actor } => {
                if let Err(ReadError::Rejected(r)) = validate::<V>(actor.as_ref(), key, reader.get(key).as_ref()) {
                    let (publisher, feed) = domain_memory::channel::<Frame<V>>(());
                    publisher.push(Frame::Rejected(r));
                    return feed;
                }
                let relay = Arc::new(FeedRelay {
                    feed: reader.events(key, after),
                    order: Mutex::new(()),
                    out: Mutex::new(None),
                    key: key.clone(),
                    actor: actor.clone(),
                    ended: AtomicBool::new(false),
                });
                // The caller's feed keeps the relay alive; the relay fills it.
                let (publisher, out) = domain_memory::channel::<Frame<V>>(relay.clone());
                *lock(&relay.out) = Some(publisher);
                let weak = Arc::downgrade(&relay);
                relay.feed.notify(move || {
                    if let Some(relay) = weak.upgrade() {
                        relay.pump();
                    }
                });
                out
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.events::<V>(key, after),
        }
    }

    /// The key's state, kept current: what [`get`](Self::get) returns, as it changes. If
    /// `validate` refuses the reader, now or after a change, it holds the rejection and stops.
    /// Commands the view predicts show at once, until their answers arrive.
    ///
    /// In process it starts at the state now. Over a connection it starts at `Ok(None)`, version
    /// 0, until the server's first answer arrives.
    pub fn live(&self, key: &V::Key) -> LiveView<Read<V>> {
        match &self.inner {
            ViewInner::Local { reader, actor } => {
                let watch = reader.watch(key);
                let initial = watch.try_next().expect("a watch starts with the state now").state;
                let initial = checked::<V>(actor, key, initial);
                let ended = AtomicBool::new(initial.is_err());
                let overlay = Overlay::<V>::new(key.clone(), initial);
                self.predictor.register(&overlay);
                let target = overlay.target();
                let pump = Arc::new(Pump {
                    watch: Mutex::new(watch),
                    overlay,
                    requested: AtomicBool::new(false),
                    key: key.clone(),
                    actor: actor.clone(),
                    ended,
                });
                let live = LiveView::new(target, pump.clone());
                // Weak, so the watch doesn't keep its own pump alive once the LiveView is gone.
                let weak = Arc::downgrade(&pump);
                lock(&pump.watch).notify(move || {
                    if let Some(pump) = weak.upgrade() {
                        pump.pump();
                    }
                });
                live
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => {
                let overlay = Overlay::<V>::new(key.clone(), Ok(None));
                self.predictor.register(&overlay);
                remote.watch::<V>(key, overlay)
            }
        }
    }
}

fn validate<V: Validate>(
    actor: Option<&CurrentActor>,
    key: &V::Key,
    state: Option<&V::State>,
) -> Result<(), ReadError<V::Rejection>> {
    V::validate(&ReadCx::new(actor), key, state).map_err(ReadError::Rejected)
}

fn checked<V: Validate>(actor: &Option<CurrentActor>, key: &V::Key, state: Option<V::State>) -> Read<V> {
    validate::<V>(actor.as_ref(), key, state.as_ref()).map(|()| state)
}

/// Moves states from a view's watch into a LiveView. The memory host calls `pump` with no lock
/// held, from whichever thread committed the change.
struct Pump<V: Apply> {
    watch: Mutex<Watch<V>>,
    overlay: Arc<Overlay<V>>,
    /// Set by a `pump` that found another one running; that one runs again for it.
    requested: AtomicBool,
    key: V::Key,
    actor: Option<CurrentActor>,
    /// `validate` refused the reader, so nothing more is published.
    ended: AtomicBool,
}

impl<V: Apply + Validate> Pump<V> {
    fn pump(&self) {
        self.requested.store(true, Ordering::SeqCst);
        loop {
            // One pump at a time, so states are published in the order they were applied.
            let watch = match self.watch.try_lock() {
                Ok(watch) => watch,
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(TryLockError::WouldBlock) => return,
            };
            self.requested.store(false, Ordering::SeqCst);
            let mut latest = None;
            while let Some(snapshot) = watch.try_next() {
                latest = Some(snapshot.state);
            }
            if let Some(state) = latest
                && !self.ended.load(Ordering::SeqCst)
            {
                let read = checked::<V>(&self.actor, &self.key, state);
                self.ended.store(read.is_err(), Ordering::SeqCst);
                self.overlay.confirm(read);
            }
            drop(watch);
            if !self.requested.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}

/// Moves frames from a view's feed into the caller's, validating each entry.
struct FeedRelay<V: Apply> {
    feed: domain_memory::Feed<V>,
    /// Held while pumping, so frames go out in the order they were recorded.
    order: Mutex<()>,
    out: Mutex<Option<Publisher<Frame<V>>>>,
    key: V::Key,
    actor: Option<CurrentActor>,
    ended: AtomicBool,
}

impl<V: Apply + Validate> FeedRelay<V> {
    fn pump(&self) {
        let _order = lock(&self.order);
        let out = lock(&self.out).clone();
        let Some(out) = out else { return };
        for frame in self.feed.drain() {
            if self.ended.load(Ordering::SeqCst) {
                return;
            }
            let frame = match frame {
                Frame::Entry(entry) => match validate::<V>(self.actor.as_ref(), &self.key, Some(&entry.state)) {
                    Ok(()) => Frame::Entry(entry),
                    Err(ReadError::Rejected(r)) => {
                        self.ended.store(true, Ordering::SeqCst);
                        Frame::Rejected(r)
                    }
                    Err(_) => continue,
                },
                other => other,
            };
            out.push(frame);
        }
    }
}
