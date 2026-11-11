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
use domain_memory::{Memory, Publisher, Runtime, Watch};
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
    Local(Runtime),
    #[cfg(not(target_arch = "wasm32"))]
    Remote(Arc<remote::Remote>),
}

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
    /// Talks to the model `runtime` runs in this process. Commands and reads run on its thread,
    /// so the UI thread never waits on one.
    pub fn in_process(runtime: Runtime) -> Self {
        Self { backend: Backend::Local(runtime), actor: None, predictor: Arc::default(), _api: PhantomData }
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

    /// Sends a command. Live views of views that predict it (`#[domain::view(predicts(..))]`) show its
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
            Backend::Local(runtime) => runtime.new_command_id(),
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
            Backend::Local(runtime) => ViewInner::Local { runtime: runtime.clone(), actor: self.actor.clone() },
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => ViewInner::Remote(remote.clone()),
        };
        ClientView { inner, predictor: self.predictor.clone(), _view: PhantomData }
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
        let runtime = match &self.backend {
            Backend::Local(runtime) => runtime,
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Remote(remote) => return remote.send(id, command, command_id, then),
        };
        let (id, actor, answered_id) = (id.clone(), self.actor.clone(), command_id.clone());
        runtime.exec(
            move |memory: &Memory| memory.send_as(actor.as_ref(), &id, command, command_id),
            move |done| {
                then(done.unwrap_or_else(|reason| {
                    Err(SendError::Unavailable { command_id: answered_id, reason: format!("the command {reason}") })
                }))
            },
        );
    }
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
    inner: ViewInner,
    predictor: Arc<Predictor>,
    _view: PhantomData<fn() -> V>,
}

enum ViewInner {
    /// Reads run as jobs on the runtime's thread.
    Local { runtime: Runtime, actor: Option<CurrentActor> },
    #[cfg(not(target_arch = "wasm32"))]
    Remote(Arc<remote::Remote>),
}

impl<V: Apply> Clone for ClientView<V> {
    fn clone(&self) -> Self {
        let inner = match &self.inner {
            ViewInner::Local { runtime, actor } => ViewInner::Local { runtime: runtime.clone(), actor: actor.clone() },
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => ViewInner::Remote(remote.clone()),
        };
        Self { inner, predictor: self.predictor.clone(), _view: PhantomData }
    }
}

impl<V: Apply + Validate> ClientView<V> {
    /// The key's state, or `None` before its first event.
    pub fn get(&self, key: &V::Key) -> Pending<Read<V>> {
        match &self.inner {
            ViewInner::Local { runtime, actor } => {
                let (key, actor) = (key.clone(), actor.clone());
                read::<V, _>(runtime, move |memory| checked::<V>(&actor, &key, memory.view::<V>().get(&key)))
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.get::<V>(key, None),
        }
    }

    /// The key's state, once the view has applied `position`, such as a command's receipt.
    pub fn get_at_least(&self, key: &V::Key, position: impl Into<Position>) -> Pending<Read<V>> {
        match &self.inner {
            ViewInner::Local { runtime, actor } => {
                let (key, actor, position) = (key.clone(), actor.clone(), position.into());
                read::<V, _>(runtime, move |memory| match memory.view::<V>().get_at_least(&key, position) {
                    Ok(state) => checked::<V>(&actor, &key, state),
                    Err(e) => Err(ReadError::NotCaughtUp(e)),
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.get::<V>(key, Some(position.into())),
        }
    }

    /// The key's state with the cursor it's valid at. Follow it with [`events`](Self::events).
    pub fn snapshot(&self, key: &V::Key) -> Pending<ReadResult<V, Snapshot<V>>> {
        match &self.inner {
            ViewInner::Local { runtime, actor } => {
                let (key, actor) = (key.clone(), actor.clone());
                read::<V, _>(runtime, move |memory| {
                    let snapshot = memory.view::<V>().snapshot(&key);
                    validate::<V>(actor.as_ref(), &key, snapshot.state.as_ref()).map(|()| snapshot)
                })
            }
            #[cfg(not(target_arch = "wasm32"))]
            ViewInner::Remote(remote) => remote.snapshot::<V>(key),
        }
    }

    /// Up to `take` of the key's emitted events after `after`, oldest first.
    pub fn list(&self, key: &V::Key, after: Cursor, take: usize) -> Pending<ReadResult<V, Vec<Entry<V>>>> {
        match &self.inner {
            ViewInner::Local { runtime, actor } => {
                let (key, actor) = (key.clone(), actor.clone());
                read::<V, _>(runtime, move |memory| {
                    let reader = memory.view::<V>();
                    validate::<V>(actor.as_ref(), &key, reader.get(&key).as_ref())
                        .map(|()| reader.list(&key, after, take))
                })
            }
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
            ViewInner::Local { runtime, actor } => {
                // The caller's feed keeps the relay alive, once the runtime's thread has made it.
                let slot = Arc::new(Mutex::new(None::<Arc<FeedRelay<V>>>));
                let (publisher, out) = domain_memory::channel::<Frame<V>>(slot.clone());
                let (key, actor) = (key.clone(), actor.clone());
                runtime.exec(
                    move |memory: &Memory| {
                        let reader = memory.view::<V>();
                        if let Err(ReadError::Rejected(r)) =
                            validate::<V>(actor.as_ref(), &key, reader.get(&key).as_ref())
                        {
                            publisher.push(Frame::Rejected(r));
                            return;
                        }
                        let relay = Arc::new(FeedRelay {
                            feed: reader.events(&key, after),
                            out: Mutex::new((publisher, key)),
                            actor,
                            ended: AtomicBool::new(false),
                        });
                        let weak = Arc::downgrade(&relay);
                        relay.feed.notify(move || {
                            if let Some(relay) = weak.upgrade() {
                                relay.pump();
                            }
                        });
                        *lock(&slot) = Some(relay);
                    },
                    |_| {},
                );
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
    /// It starts at `Ok(None)`, version 0, until the first state arrives: from the runtime's
    /// thread in process, or from the server over a connection.
    ///
    /// Readers on other threads share each value it shows, so it must be `Sync`.
    pub fn live(&self, key: &V::Key) -> LiveView<Read<V>>
    where
        Read<V>: MaybeSync,
    {
        match &self.inner {
            ViewInner::Local { runtime, actor } => {
                let overlay = Overlay::<V>::new(key.clone(), Ok(None));
                self.predictor.register(&overlay);
                let target = overlay.target();
                let pump = Arc::new(Pump {
                    watch: Mutex::new(None),
                    overlay,
                    requested: AtomicBool::new(false),
                    actor: actor.clone(),
                    ended: AtomicBool::new(false),
                });
                let live = LiveView::new(target, pump.clone());
                // Weak, so neither the job nor the watch keeps the pump alive once the LiveView
                // is gone.
                let weak = Arc::downgrade(&pump);
                let key = key.clone();
                runtime.exec(
                    move |memory: &Memory| {
                        let Some(pump) = weak.upgrade() else { return };
                        let watch = memory.view::<V>().watch(&key);
                        let weak = Arc::downgrade(&pump);
                        watch.notify(move || {
                            if let Some(pump) = weak.upgrade() {
                                pump.pump();
                            }
                        });
                        *lock(&pump.watch) = Some((watch, key));
                        pump.pump();
                    },
                    |_| {},
                );
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

/// Runs `job` on the runtime's thread and resolves with what it returns.
fn read<V: View, T: MaybeSend + 'static>(
    runtime: &Runtime,
    job: impl FnOnce(&Memory) -> ReadResult<V, T> + MaybeSend + 'static,
) -> Pending<ReadResult<V, T>> {
    let (pending, resolver) = pending();
    runtime.exec(job, move |read| {
        resolver.resolve(read.unwrap_or_else(|reason| Err(ReadError::Unavailable(format!("the read {reason}")))))
    });
    pending
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

/// Moves states from a view's watch into a LiveView. The runtime's thread calls `pump` once the
/// watch has news.
struct Pump<V: Apply> {
    /// The watch and its key, once the runtime's thread has started it.
    watch: Mutex<Option<(Watch<V>, V::Key)>>,
    overlay: Arc<Overlay<V>>,
    /// Set by a `pump` that found another one running; that one runs again for it.
    requested: AtomicBool,
    actor: Option<CurrentActor>,
    /// `validate` refused the reader, so nothing more is published.
    ended: AtomicBool,
}

impl<V: Apply + Validate> Pump<V> {
    fn pump(&self) {
        self.requested.store(true, Ordering::SeqCst);
        loop {
            // One pump at a time, so states are published in the order they were applied.
            let guard = match self.watch.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(TryLockError::WouldBlock) => return,
            };
            self.requested.store(false, Ordering::SeqCst);
            let Some((watch, key)) = &*guard else { return };
            let mut latest = None;
            while let Some(snapshot) = watch.try_next() {
                latest = Some(snapshot.state);
            }
            if let Some(state) = latest
                && !self.ended.load(Ordering::SeqCst)
            {
                let read = checked::<V>(&self.actor, key, state);
                self.ended.store(read.is_err(), Ordering::SeqCst);
                self.overlay.confirm(read);
            }
            drop(guard);
            if !self.requested.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}

/// Moves frames from a view's feed into the caller's, validating each entry.
struct FeedRelay<V: Apply> {
    feed: domain_memory::Feed<V>,
    /// The caller's feed and the key. Held while pumping, so frames go out in the order they
    /// were recorded.
    out: Mutex<(Publisher<Frame<V>>, V::Key)>,
    actor: Option<CurrentActor>,
    ended: AtomicBool,
}

impl<V: Apply + Validate> FeedRelay<V> {
    fn pump(&self) {
        let out = lock(&self.out);
        let (out, key) = &*out;
        for frame in self.feed.drain() {
            if self.ended.load(Ordering::SeqCst) {
                return;
            }
            let frame = match frame {
                Frame::Entry(entry) => match validate::<V>(self.actor.as_ref(), key, Some(&entry.state)) {
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
