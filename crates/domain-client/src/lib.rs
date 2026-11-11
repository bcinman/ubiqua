//! What a UI uses to talk to a domain model: a typed [`Client`] for the model's Api.
//!
//! Nothing here depends on a UI framework or an async runtime. Results arrive as [`Pending`]
//! (check it each frame, or `.await` it on any executor) or through a callback, and changing state
//! arrives as a [`LiveView`] (read it each frame, get callbacks, or await changes).
//!
//! Today the client runs the model in the same process, on a `Memory`. A client for a model on a
//! server will have the same methods.

mod live;
mod pending;

#[doc(hidden)]
pub mod compile_checks;

pub use domain_memory::{Feed, Recv, Subscription};
pub use live::{Changed, Listener, LiveView};
pub use pending::Pending;

use domain::{
    AggregateId, Api, Apply, Command, CommandId, Cursor, Entry, Handles, MaybeSend, NotCaughtUp, Position, Reads,
    Receipt, SendError, SentVia, Snapshot,
};
use domain_memory::{Memory, ViewReader, Watch};
use live::Shared;
use pending::{lock, pending};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, TryLockError};

type AggregateOf<I> = <I as AggregateId>::Aggregate;

/// What a send resolves to: the receipt, or why it didn't commit.
pub type Sent<C> = Result<Receipt<C>, SendError<<C as Command>::Rejection>>;

/// A typed client for the Api `A`. It sends only the commands `A` lists, and reads only its views.
pub struct Client<A: Api> {
    memory: Arc<Memory>,
    runner: Runner,
    _api: PhantomData<fn() -> A>,
}

impl<A: Api> Clone for Client<A> {
    fn clone(&self) -> Self {
        Self { memory: self.memory.clone(), runner: self.runner.clone(), _api: PhantomData }
    }
}

#[derive(Clone)]
enum Runner {
    /// Commands run on the caller's thread.
    Inline,
    /// Commands run, in order, on the client's domain thread.
    #[cfg(not(target_arch = "wasm32"))]
    Thread(std::sync::mpsc::Sender<Job>),
}

#[cfg(not(target_arch = "wasm32"))]
type Job = Box<dyn FnOnce(&Memory) + Send>;

impl<A: Api> Client<A> {
    /// Runs commands on a domain thread of their own, so the UI thread never waits on one. On wasm,
    /// which has no threads, they run on the caller's thread.
    pub fn in_process(memory: Arc<Memory>) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let (jobs, queue) = std::sync::mpsc::channel::<Job>();
            let worker = memory.clone();
            std::thread::Builder::new()
                .name("domain".into())
                .spawn(move || {
                    for job in queue {
                        // A panicking `send_then` callback mustn't stop the commands after it.
                        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(&worker)));
                    }
                })
                .expect("spawn the domain thread");
            Self { memory, runner: Runner::Thread(jobs), _api: PhantomData }
        }
        #[cfg(target_arch = "wasm32")]
        Self::inline(memory)
    }

    /// Runs commands on the caller's thread: for tests, and anywhere waiting is fine.
    pub fn inline(memory: Arc<Memory>) -> Self {
        Self { memory, runner: Runner::Inline, _api: PhantomData }
    }

    /// Sends a command. Retrying after [`SendError::Unavailable`] with
    /// [`send_with_id`](Self::send_with_id) and the same id takes effect at most once.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Pending<Sent<C>>
    where
        C: Command,
        C: SentVia<A>,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.send_with_id(id, command, self.memory.new_command_id())
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
    /// thread for [`in_process`](Self::in_process). For UIs whose handlers can't await.
    pub fn send_then<C>(&self, id: &C::Target, command: C, then: impl FnOnce(Sent<C>) + MaybeSend + 'static)
    where
        C: Command,
        C: SentVia<A>,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.run(id, command, self.memory.new_command_id(), then);
    }

    /// Reads the view `V`.
    pub fn view<V: Apply>(&self) -> ClientView<V>
    where
        A: Reads<V>,
    {
        ClientView { reader: self.memory.view::<V>() }
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
        let id = id.clone();
        let job = move |memory: &Memory| {
            let sent = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                memory.send_with_id(&id, command, command_id.clone())
            }));
            then(sent.unwrap_or_else(|panic| Err(SendError::Unavailable { command_id, reason: reason(&*panic) })));
        };
        match &self.runner {
            Runner::Inline => job(&self.memory),
            #[cfg(not(target_arch = "wasm32"))]
            Runner::Thread(jobs) => jobs.send(Box::new(job)).expect("the domain thread outlives its clients"),
        }
    }
}

fn reason(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic.downcast_ref::<&str>().copied().or(panic.downcast_ref::<String>().map(String::as_str));
    format!("the command panicked: {}", message.unwrap_or("no message"))
}

/// Why a read failed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReadError {
    /// The view hasn't applied the position the read asked for.
    NotCaughtUp(NotCaughtUp),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::NotCaughtUp(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for ReadError {}

/// Reads one view through a [`Client`].
pub struct ClientView<V: Apply> {
    reader: ViewReader<V>,
}

impl<V: Apply> Clone for ClientView<V> {
    fn clone(&self) -> Self {
        Self { reader: self.reader.clone() }
    }
}

impl<V: Apply> ClientView<V> {
    /// The key's state, or `None` before its first event.
    pub fn get(&self, key: &V::Key) -> Pending<Result<Option<V::State>, ReadError>> {
        Pending::ready(Ok(self.reader.get(key)))
    }

    /// The key's state, once the view has applied `position`, such as a command's receipt.
    pub fn get_at_least(
        &self,
        key: &V::Key,
        position: impl Into<Position>,
    ) -> Pending<Result<Option<V::State>, ReadError>> {
        let read = self.reader.at_least(position).map(|reader| reader.get(key)).map_err(ReadError::NotCaughtUp);
        Pending::ready(read)
    }

    /// The key's state with the cursor it's valid at. Follow it with [`events`](Self::events).
    pub fn snapshot(&self, key: &V::Key) -> Pending<Result<Snapshot<V>, ReadError>> {
        Pending::ready(Ok(self.reader.snapshot(key)))
    }

    /// Up to `take` of the key's emitted events after `after`, oldest first.
    pub fn list(&self, key: &V::Key, after: Cursor, take: usize) -> Pending<Result<Vec<Entry<V>>, ReadError>> {
        Pending::ready(Ok(self.reader.list(key, after, take)))
    }

    /// The key's emitted events after `after`, then `CaughtUp`, then new ones as they're recorded.
    pub fn events(&self, key: &V::Key, after: Cursor) -> Feed<V> {
        self.reader.events(key, after)
    }

    /// The key's state, kept current. It starts at the state now: `None` before the key's first event.
    pub fn live(&self, key: &V::Key) -> LiveView<Option<V::State>> {
        let watch = self.reader.watch(key);
        let initial = watch.try_next().expect("a watch starts with the state now").state;
        let target = Shared::new(initial);
        let pump =
            Arc::new(Pump { watch: Mutex::new(watch), target: target.clone(), requested: AtomicBool::new(false) });
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
}

/// Moves states from a view's watch into a LiveView. The memory host calls `pump` with no lock
/// held, from whichever thread committed the change.
struct Pump<V: Apply> {
    watch: Mutex<Watch<V>>,
    target: Arc<Shared<Option<V::State>>>,
    /// Set by a `pump` that found another one running; that one runs again for it.
    requested: AtomicBool,
}

impl<V: Apply> Pump<V> {
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
            if let Some(state) = latest {
                self.target.publish(state);
            }
            drop(watch);
            if !self.requested.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}
