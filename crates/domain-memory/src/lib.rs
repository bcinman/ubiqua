//! Runs a domain model in memory: for tests, prototypes and apps that don't need to persist yet.
//!
//! Each aggregate instance decides one command at a time; different instances run in parallel.
//! A command's events, new state, receipt and deliveries to views commit together or not at all.
//! Views apply each source's events in order, at least once, after the commit. A view that panics
//! doesn't fail the command: it falls behind, and reading it panics until it catches up.
//!
//! With [`Memory::persist_to`] it also keeps everything in a [`Store`], such as `domain-sqlite`'s,
//! and picks up where it left off on the next run.

mod store;
mod views;

pub use store::{Commit, EntryRow, EventRow, KeyRow, ReceiptRow, Store, StoreError, ViewRows};
pub use views::{Feed, Recv, Subscription, ViewReader, Watch};

use domain::{
    Aggregate, AggregateId, Apply, Command, CommandId, Consumes, Cx, Envelope, Evolve, Handles, MaybeSend, MaybeSync,
    Model, Receipt, Registry, SendError, Source, Sources, StreamId, Timestamp,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use views::{AnyBox, Consumer, Delivery, ViewHost};

/// Reads one aggregate's history as deliveries, skipping the `(stream, seq)`s `keep` refuses.
type HistoryFn = fn(&Memory, &dyn Fn(&str, u64) -> bool) -> Vec<Arc<Delivery>>;

/// The aggregate an id belongs to.
type AggregateOf<I> = <I as AggregateId>::Aggregate;
type StateOf<I> = <AggregateOf<I> as Aggregate>::State;
type EventOf<I> = <AggregateOf<I> as Aggregate>::Event;

/// Every aggregate's instances, in memory. Share it between threads with an `Arc`.
pub struct Memory {
    stores: Mutex<HashMap<TypeId, Box<dyn AnyStore>>>,
    clock: Box<dyn Clock>,
    sent: AtomicU64,
    /// Starts every command id `new_command_id` makes.
    id_prefix: String,
    store: Option<Arc<dyn Store>>,
    /// The views that read each aggregate, by the aggregate's `TypeId`.
    consumers: HashMap<TypeId, Vec<Arc<dyn Consumer>>>,
    /// Each view's `Arc<ViewHost<V>>`, by the view's `TypeId`.
    views: HashMap<TypeId, AnyBox>,
    /// The same hosts, for other views that read them.
    view_consumers: HashMap<TypeId, Arc<dyn Consumer>>,
    /// How to read each registered aggregate's history, for views added later.
    histories: HashMap<TypeId, HistoryFn>,
}

trait Clock: Fn() -> Timestamp + MaybeSend + MaybeSync {}
impl<F: Fn() -> Timestamp + MaybeSend + MaybeSync> Clock for F {}

#[cfg(not(target_arch = "wasm32"))]
impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

impl Memory {
    /// Uses the system clock. On wasm, use [`Memory::with_clock`].
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new() -> Self {
        Self::with_clock(Timestamp::now)
    }

    /// Uses `clock` for the time each command is decided.
    pub fn with_clock(clock: impl Fn() -> Timestamp + MaybeSend + MaybeSync + 'static) -> Self {
        Self {
            stores: Mutex::default(),
            clock: Box::new(clock),
            sent: AtomicU64::new(0),
            id_prefix: "memory".into(),
            store: None,
            consumers: HashMap::new(),
            views: HashMap::new(),
            view_consumers: HashMap::new(),
            histories: HashMap::new(),
        }
    }

    /// Keeps events, receipts and views in `store` too, so the next run picks up where this one
    /// left off. Instances read their events the first time they're used; views load what was
    /// stored and apply what they missed. Call it before [`with`](Self::with).
    ///
    /// # Panics
    /// After `with` or a command, or when the store fails.
    pub fn persist_to(mut self, store: impl Store + 'static) -> Self {
        assert!(
            self.views.is_empty() && self.histories.is_empty() && lock(&self.stores).is_empty(),
            "call `persist_to` before `with` and before sending commands"
        );
        let session = store.new_session().unwrap_or_else(|e| panic!("the store failed: {e}"));
        self.id_prefix = format!("s{session}");
        self.store = Some(Arc::new(store));
        self
    }

    /// Hosts the model's views. A view added when events already exist receives them first, in
    /// the order they were recorded.
    ///
    /// With a store, a view stored at its current [`View::VERSION`](domain::View::VERSION) loads
    /// and applies only what it missed. Otherwise it's rebuilt from the events, and so is every
    /// view that reads it.
    pub fn with<M: Model>(mut self) -> Self {
        M::register(&mut Registrar { memory: &mut self, hosting: HashSet::new(), rebuilt: HashSet::new() });
        self
    }

    /// Reads a view.
    ///
    /// # Panics
    /// When `V` isn't in a model passed to [`with`](Self::with).
    pub fn view<V: Apply>(&self) -> ViewReader<V> {
        let host = self.views.get(&TypeId::of::<V>()).and_then(|host| (**host).downcast_ref::<Arc<ViewHost<V>>>());
        match host {
            Some(host) => ViewReader { host: host.clone() },
            None => panic!("{} isn't hosted here: list it in a model's `views(..)` and use `Memory::with`", V::NAME),
        }
    }

    /// Decides `command` on the instance `id`, commits its events and returns the receipt.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        self.send_with_id(id, command, self.new_command_id())
    }

    /// A command id no other command sent here has used.
    pub fn new_command_id(&self) -> CommandId {
        let n = self.sent.fetch_add(1, Ordering::Relaxed) + 1;
        CommandId::new(format!("{}-{n}", self.id_prefix))
    }

    /// Like [`send`](Self::send), under your own command id. Sending the same id again returns the
    /// first receipt without deciding again, so a retry takes effect once.
    pub fn send_with_id<C>(
        &self,
        id: &C::Target,
        command: C,
        command_id: impl Into<CommandId>,
    ) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let command_id = command_id.into();
        let stream = StreamId::of::<AggregateOf<C::Target>>(id);
        let receipt = self.send_locked(id, command, command_id, stream)?;
        self.deliver::<AggregateOf<C::Target>>();
        Ok(receipt)
    }

    fn send_locked<C>(
        &self,
        id: &C::Target,
        command: C,
        command_id: CommandId,
        stream: StreamId,
    ) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let unavailable = |reason: String| SendError::Unavailable { command_id: command_id.clone(), reason };
        let instance = self.instance(id);
        let mut instance = lock(&instance);
        self.load(id, &mut instance).map_err(unavailable)?;

        if let Some(stored) = instance.receipts.get(&command_id) {
            return match stored.as_any().downcast_ref::<Receipt<C>>() {
                Some(receipt) => Ok(receipt.clone()),
                None => Err(SendError::CommandIdReused { stream, command_id }),
            };
        }
        if let Some(stored) = self.stored_receipt::<C>(&stream, &command_id) {
            return stored;
        }

        let at = (self.clock)();
        let cx = Cx::new(at, command_id.clone());
        let events = <AggregateOf<C::Target> as domain::Decide<C>>::decide(&instance.state, &command, &cx)
            .map_err(SendError::Rejected)?;

        // Work on a copy, so a panic in evolve or reply leaves the instance as it was.
        let mut state = instance.state.clone();
        let mut recorded = Vec::with_capacity(events.len());
        for event in events.iter().cloned() {
            let event = <AggregateOf<C::Target> as Handles<C>>::widen(event);
            AggregateOf::<C::Target>::evolve(&mut state, &event);
            let seq = instance.events.len() as u64 + recorded.len() as u64 + 1;
            let command_id = Some(command_id.clone());
            recorded.push(Envelope { stream: stream.clone(), seq, at, command_id, event });
        }
        let reply = <AggregateOf<C::Target> as domain::Decide<C>>::reply(&state);
        let version = instance.events.len() as u64 + recorded.len() as u64;
        let receipt = Receipt { stream, version, command_id: command_id.clone(), events, reply };

        if self.store.is_some() {
            let row = ReceiptRow {
                command_id: command_id.to_string(),
                command: C::NAME.into(),
                receipt: encode_receipt(&receipt)?,
            };
            self.persist::<AggregateOf<C::Target>>(id, &recorded, Some(row)).map_err(unavailable)?;
        }
        instance.receipts.insert(command_id, Box::new(receipt.clone()));
        self.commit(id, &mut instance, state, recorded);
        Ok(receipt)
    }

    /// Writes the events and receipt to the store, if there is one. Runs before memory changes, so a
    /// failed write changes nothing.
    fn persist<A: Aggregate>(
        &self,
        id: &A::Id,
        recorded: &[Envelope<A::Event>],
        receipt: Option<ReceiptRow>,
    ) -> Result<(), String> {
        let Some(store) = &self.store else { return Ok(()) };
        let id_json = json(id)?;
        let events = recorded
            .iter()
            .map(|envelope| {
                Ok(EventRow {
                    stream: envelope.stream.to_string(),
                    seq: envelope.seq,
                    id: id_json.clone(),
                    at: envelope.at.as_millis(),
                    command_id: envelope.command_id.as_ref().map(ToString::to_string),
                    event: json(&envelope.event)?,
                })
            })
            .collect::<Result<_, String>>()?;
        let commit = Commit { aggregate: A::NAME.into(), stream: StreamId::of::<A>(id).to_string(), events, receipt };
        store.commit(&commit).map_err(|e| format!("the store failed: {e}"))
    }

    /// Reads the instance's events from the store the first time it's used.
    fn load<A: Evolve>(&self, id: &A::Id, instance: &mut Instance<A>) -> Result<(), String> {
        if instance.loaded {
            return Ok(());
        }
        if let Some(store) = &self.store {
            let stream = StreamId::of::<A>(id);
            let rows = store.stream(stream.as_str()).map_err(|e| format!("reading {stream} failed: {e}"))?;
            // Rebuild on the side, so a failure leaves the instance unloaded.
            let mut state = A::State::default();
            let mut events = Vec::with_capacity(rows.len());
            for row in &rows {
                let (_, envelope) = decode_event::<A>(row)?;
                A::evolve(&mut state, &envelope.event);
                events.push(envelope);
            }
            instance.state = state;
            instance.events = events;
        }
        instance.loaded = true;
        Ok(())
    }

    /// The receipt the store holds for `command_id` on `stream`, from an earlier run.
    fn stored_receipt<C: Command>(
        &self,
        stream: &StreamId,
        command_id: &CommandId,
    ) -> Option<Result<Receipt<C>, SendError<C::Rejection>>> {
        let store = self.store.as_ref()?;
        let unavailable = |reason: String| SendError::Unavailable { command_id: command_id.clone(), reason };
        let row = match store.receipt(stream.as_str(), command_id.as_str()) {
            Ok(row) => row?,
            Err(e) => return Some(Err(unavailable(format!("the store failed: {e}")))),
        };
        if row.command != C::NAME {
            return Some(Err(SendError::CommandIdReused { stream: stream.clone(), command_id: command_id.clone() }));
        }
        let decoded = serde_json::from_str::<(u64, Vec<C::Event>, C::Reply)>(&row.receipt);
        Some(match decoded {
            Ok((version, events, reply)) => {
                Ok(Receipt { stream: stream.clone(), version, command_id: command_id.clone(), events, reply })
            }
            Err(e) => Err(unavailable(format!("its stored receipt doesn't decode: {e}"))),
        })
    }

    /// Stores the new state and events, and queues them for the views, under the instance's lock.
    fn commit<A: Aggregate>(
        &self,
        id: &A::Id,
        instance: &mut Instance<A>,
        state: A::State,
        recorded: Vec<Envelope<A::Event>>,
    ) {
        let consumers = self.consumers.get(&TypeId::of::<A>()).map_or(&[][..], |c| &c[..]);
        if !consumers.is_empty() {
            for envelope in &recorded {
                let delivery = Arc::new(delivery_of::<A>(id, envelope));
                consumers.iter().for_each(|consumer| consumer.push(delivery.clone()));
            }
        }
        instance.state = state;
        instance.events.extend(recorded);
    }

    /// Applies queued deliveries to the views that read `A`. Runs after the instance is unlocked.
    fn deliver<A: Aggregate>(&self) {
        for consumer in self.consumers.get(&TypeId::of::<A>()).into_iter().flatten() {
            // The command has committed, so a view that panics doesn't fail it. The view stays
            // behind with its deliveries, and tries them again on the next read or command.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| consumer.drain()));
        }
    }

    /// The instance's current state.
    ///
    /// # Panics
    /// When the store fails.
    pub fn state<I: AggregateId>(&self, id: &I) -> StateOf<I>
    where
        AggregateOf<I>: Evolve,
    {
        self.read(id, |instance| instance.state.clone())
    }

    /// The instance's events, oldest first.
    ///
    /// # Panics
    /// When the store fails.
    pub fn events<I: AggregateId>(&self, id: &I) -> Vec<Envelope<EventOf<I>>>
    where
        AggregateOf<I>: Evolve,
    {
        self.read(id, |instance| instance.events.clone())
    }

    fn read<I: AggregateId, T>(&self, id: &I, f: impl FnOnce(&Instance<AggregateOf<I>>) -> T) -> T
    where
        AggregateOf<I>: Evolve,
    {
        let instance = self.instance(id);
        let mut instance = lock(&instance);
        self.load(id, &mut instance).unwrap_or_else(|e| panic!("{e}"));
        f(&instance)
    }

    /// Records events without deciding a command, as history: for tests and imports.
    ///
    /// # Panics
    /// When the store fails.
    pub fn append<I, E>(&self, id: &I, events: impl IntoIterator<Item = E>)
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
        E: Into<EventOf<I>>,
    {
        let stream = StreamId::of::<AggregateOf<I>>(id);
        let at = (self.clock)();
        {
            let instance = self.instance(id);
            let mut instance = lock(&instance);
            self.load(id, &mut instance).unwrap_or_else(|e| panic!("{e}"));
            let mut state = instance.state.clone();
            let mut recorded = Vec::new();
            for event in events {
                let event = event.into();
                AggregateOf::<I>::evolve(&mut state, &event);
                let seq = instance.events.len() as u64 + recorded.len() as u64 + 1;
                recorded.push(Envelope { stream: stream.clone(), seq, at, command_id: None, event });
            }
            self.persist::<AggregateOf<I>>(id, &recorded, None).unwrap_or_else(|e| panic!("{e}"));
            self.commit(id, &mut instance, state, recorded);
        }
        self.deliver::<AggregateOf<I>>();
    }

    /// Every event `A` has recorded that `keep` accepts, as deliveries.
    fn history<A: Aggregate>(&self, keep: &dyn Fn(&str, u64) -> bool) -> Vec<Arc<Delivery>> {
        if let Some(store) = &self.store {
            // Read the store: it has every instance, loaded or not.
            let rows = store.history(A::NAME, keep).unwrap_or_else(|e| panic!("reading {} failed: {e}", A::NAME));
            return rows
                .iter()
                .map(|row| {
                    let (id, envelope) = decode_event::<A>(row).unwrap_or_else(|e| panic!("{e}"));
                    Arc::new(delivery_of::<A>(&id, &envelope))
                })
                .collect();
        }
        let stores = lock(&self.stores);
        let Some(store) = stores.get(&TypeId::of::<A>()) else { return Vec::new() };
        let store = store.as_any().downcast_ref::<Instances<A>>().expect("store matches its TypeId");
        let mut history = Vec::new();
        for (id, instance) in &store.instances {
            for envelope in &lock(instance).events {
                if keep(envelope.stream.as_str(), envelope.seq) {
                    history.push(Arc::new(delivery_of::<A>(id, envelope)));
                }
            }
        }
        history
    }

    /// Every stream that has events, with its version.
    ///
    /// # Panics
    /// When the store fails.
    pub fn versions(&self) -> Vec<(StreamId, u64)> {
        if let Some(store) = &self.store {
            let mut versions: Vec<_> = store
                .versions()
                .unwrap_or_else(|e| panic!("the store failed: {e}"))
                .into_iter()
                .map(|(stream, version)| (StreamId::from(stream), version))
                .collect();
            versions.sort();
            return versions;
        }
        let stores = lock(&self.stores);
        let mut versions: Vec<_> = stores.values().flat_map(|store| store.versions()).collect();
        versions.sort();
        versions
    }

    fn instance<I: AggregateId>(&self, id: &I) -> Arc<Mutex<Instance<AggregateOf<I>>>> {
        let mut stores = lock(&self.stores);
        let store = stores
            .entry(TypeId::of::<AggregateOf<I>>())
            .or_insert_with(|| Box::new(Instances::<AggregateOf<I>>::default()));
        let store = store.as_any_mut().downcast_mut::<Instances<AggregateOf<I>>>().expect("store matches its TypeId");
        store.instances.entry(id.clone()).or_default().clone()
    }
}

fn delivery_of<A: Aggregate>(id: &A::Id, envelope: &Envelope<A::Event>) -> Delivery {
    let payload = (id.clone(), envelope.event.clone(), ());
    Delivery::new(TypeId::of::<A>(), envelope.stream.clone(), envelope.seq, envelope.at, payload)
}

/// Reads an event the store holds, with the id of the instance that recorded it.
fn decode_event<A: Aggregate>(row: &EventRow) -> Result<(A::Id, Envelope<A::Event>), String> {
    let undecodable = |what: &str, e: serde_json::Error| {
        format!("the stored {what} of {} #{} doesn't decode as {}: {e}", row.stream, row.seq, A::NAME)
    };
    let id = serde_json::from_str(&row.id).map_err(|e| undecodable("id", e))?;
    let event = serde_json::from_str(&row.event).map_err(|e| undecodable("event", e))?;
    let envelope = Envelope {
        stream: StreamId::from(row.stream.clone()),
        seq: row.seq,
        at: Timestamp::from_millis(row.at),
        command_id: row.command_id.clone().map(CommandId::new),
        event,
    };
    Ok((id, envelope))
}

fn encode_receipt<C: Command>(receipt: &Receipt<C>) -> Result<String, SendError<C::Rejection>> {
    json(&(receipt.version, &receipt.events, &receipt.reply))
        .map_err(|reason| SendError::Unavailable { command_id: receipt.command_id.clone(), reason })
}

pub(crate) fn json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| format!("{} doesn't encode as JSON: {e}", std::any::type_name::<T>()))
}

/// A poisoned lock only means a decision panicked before it committed; the data is consistent.
fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Instance<A: Aggregate> {
    state: A::State,
    events: Vec<Envelope<A::Event>>,
    /// Receipts of commands decided in this run; earlier runs' are in the store.
    receipts: HashMap<CommandId, Box<dyn AnyReceipt>>,
    /// Whether the store's events have been read.
    loaded: bool,
}

impl<A: Aggregate> Default for Instance<A> {
    fn default() -> Self {
        Self { state: A::State::default(), events: Vec::new(), receipts: HashMap::new(), loaded: false }
    }
}

struct Instances<A: Aggregate> {
    instances: HashMap<A::Id, Arc<Mutex<Instance<A>>>>,
}

impl<A: Aggregate> Default for Instances<A> {
    fn default() -> Self {
        Self { instances: HashMap::new() }
    }
}

trait AnyStore: MaybeSend + MaybeSync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn versions(&self) -> Vec<(StreamId, u64)>;
}

impl<A: Aggregate> AnyStore for Instances<A> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn versions(&self) -> Vec<(StreamId, u64)> {
        self.instances
            .iter()
            .map(|(id, instance)| (StreamId::of::<A>(id), lock(instance).events.len() as u64))
            .filter(|(_, version)| *version > 0)
            .collect()
    }
}

trait AnyReceipt: MaybeSend + MaybeSync {
    fn as_any(&self) -> &dyn Any;
}

impl<C: Command> AnyReceipt for Receipt<C> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Hosts a model's views on a `Memory`, each after the sources it reads.
struct Registrar<'m> {
    memory: &'m mut Memory,
    /// Views being hosted right now, to catch a view that reads itself through others.
    hosting: HashSet<TypeId>,
    /// Views rebuilt from the events, so the views that read them are rebuilt too.
    rebuilt: HashSet<TypeId>,
}

impl Registry for Registrar<'_> {
    fn aggregate<A: Evolve>(&mut self) {
        self.memory.histories.insert(TypeId::of::<A>(), Memory::history::<A>);
    }

    fn view<V: Apply>(&mut self) {
        let id = TypeId::of::<V>();
        if self.memory.views.contains_key(&id) {
            return;
        }
        assert!(self.hosting.insert(id), "{} reads itself through other views", V::NAME);
        V::sources(&mut HostSources { registrar: self });

        let memory = &*self.memory;
        let mut host = ViewHost::<V>::new(memory.store.clone());
        V::sources(&mut Wire { host: &mut host, memory });
        let sources: Vec<TypeId> = host.sources().collect();

        if let Some(store) = &memory.store {
            fn or_panic<T>(result: Result<T, StoreError>, view: &str) -> T {
                result.unwrap_or_else(|e| panic!("reading {view} from the store failed: {e}"))
            }
            let stored = or_panic(store.view_version(V::NAME), V::NAME);
            let upstream_rebuilt = sources.iter().any(|source| self.rebuilt.contains(source));
            if stored == Some(V::VERSION) && !upstream_rebuilt {
                host.load(or_panic(store.load_view(V::NAME), V::NAME));
            } else {
                or_panic(store.reset_view(V::NAME, V::VERSION), V::NAME);
                self.rebuilt.insert(id);
            }
        }

        // Receive what it hasn't applied yet, oldest first across sources: everything, unless it
        // was loaded from the store.
        let progress = host.progress();
        let keep = |stream: &str, seq: u64| seq > progress.get(stream).copied().unwrap_or(0);
        let mut history = Vec::new();
        for source in &sources {
            if let Some(upstream) = memory.view_consumers.get(source) {
                history.extend(upstream.history().into_iter().filter(|d| keep(d.stream.as_str(), d.seq)));
            } else if let Some(read) = memory.histories.get(source) {
                history.extend(read(memory, &keep));
            }
        }
        history.sort_by(|a, b| a.order().cmp(&b.order()));
        let host = Arc::new(host);
        history.into_iter().for_each(|delivery| host.push(delivery));
        host.drain();

        for source in sources {
            match self.memory.view_consumers.get(&source) {
                Some(upstream) => upstream.add_downstream(host.clone()),
                None => self.memory.consumers.entry(source).or_default().push(host.clone()),
            }
        }
        self.memory.view_consumers.insert(id, host.clone());
        self.memory.views.insert(id, Box::new(host));
        self.hosting.remove(&id);
    }
}

/// Hosts each source of a view before the view itself.
struct HostSources<'r, 'm> {
    registrar: &'r mut Registrar<'m>,
}

impl<V: Apply> Sources<V> for HostSources<'_, '_> {
    fn source<S: Source>(&mut self)
    where
        V: Consumes<S>,
    {
        S::register_source(self.registrar);
    }
}

/// Routes a view's sources, and connects it to the views it reads.
struct Wire<'h, 'm, V: Apply> {
    host: &'h mut ViewHost<V>,
    memory: &'m Memory,
}

impl<V: Apply> Sources<V> for Wire<'_, '_, V> {
    fn source<S: Source>(&mut self)
    where
        V: Consumes<S>,
    {
        self.host.add_source::<S>();
        if let Some(upstream) = self.memory.view_consumers.get(&TypeId::of::<S>()) {
            self.host.add_upstream(upstream.clone());
        }
    }
}
