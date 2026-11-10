//! Runs a domain model in memory: for tests, prototypes and apps that don't need to persist yet.
//!
//! Each aggregate instance decides one command at a time; different instances run in parallel.
//! A command's events, new state and receipt commit together or not at all.

use domain::{
    Aggregate, AggregateId, Command, CommandId, Cx, Envelope, Evolve, Handles, MaybeSend, MaybeSync, Receipt,
    SendError, StreamId, Timestamp,
};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// The aggregate an id belongs to.
type AggregateOf<I> = <I as AggregateId>::Aggregate;
type StateOf<I> = <AggregateOf<I> as Aggregate>::State;
type EventOf<I> = <AggregateOf<I> as Aggregate>::Event;

/// Every aggregate's instances, in memory. Share it between threads with an `Arc`.
pub struct Memory {
    stores: Mutex<HashMap<TypeId, Box<dyn AnyStore>>>,
    clock: Box<dyn Clock>,
    sent: AtomicU64,
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
        Self { stores: Mutex::default(), clock: Box::new(clock), sent: AtomicU64::new(0) }
    }

    /// Decides `command` on the instance `id`, commits its events and returns the receipt.
    pub fn send<C>(&self, id: &C::Target, command: C) -> Result<Receipt<C>, SendError<C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let n = self.sent.fetch_add(1, Ordering::Relaxed) + 1;
        self.send_with_id(id, command, CommandId::new(format!("memory-{n}")))
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
        let instance = self.instance(id);
        let mut instance = lock(&instance);

        if let Some(stored) = instance.receipts.get(&command_id) {
            return match stored.as_any().downcast_ref::<Receipt<C>>() {
                Some(receipt) => Ok(receipt.clone()),
                None => Err(SendError::CommandIdReused { stream, command_id }),
            };
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

        instance.state = state;
        instance.events.extend(recorded);
        instance.receipts.insert(command_id, Box::new(receipt.clone()));
        Ok(receipt)
    }

    /// The instance's current state.
    pub fn state<I: AggregateId>(&self, id: &I) -> StateOf<I> {
        lock(&self.instance(id)).state.clone()
    }

    /// The instance's events, oldest first.
    pub fn events<I: AggregateId>(&self, id: &I) -> Vec<Envelope<EventOf<I>>> {
        lock(&self.instance(id)).events.clone()
    }

    /// Records events without deciding a command, as history: for tests and imports.
    pub fn append<I, E>(&self, id: &I, events: impl IntoIterator<Item = E>)
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
        E: Into<EventOf<I>>,
    {
        let stream = StreamId::of::<AggregateOf<I>>(id);
        let at = (self.clock)();
        let instance = self.instance(id);
        let mut instance = lock(&instance);
        let mut state = instance.state.clone();
        let mut recorded = Vec::new();
        for event in events {
            let event = event.into();
            AggregateOf::<I>::evolve(&mut state, &event);
            let seq = instance.events.len() as u64 + recorded.len() as u64 + 1;
            recorded.push(Envelope { stream: stream.clone(), seq, at, command_id: None, event });
        }
        instance.state = state;
        instance.events.extend(recorded);
    }

    /// Every stream that has events, with its version.
    pub fn versions(&self) -> Vec<(StreamId, u64)> {
        let stores = lock(&self.stores);
        let mut versions: Vec<_> = stores.values().flat_map(|store| store.versions()).collect();
        versions.sort();
        versions
    }

    fn instance<I: AggregateId>(&self, id: &I) -> Arc<Mutex<Instance<AggregateOf<I>>>> {
        let mut stores = lock(&self.stores);
        let store = stores
            .entry(TypeId::of::<AggregateOf<I>>())
            .or_insert_with(|| Box::new(Store::<AggregateOf<I>>::default()));
        let store = store.as_any_mut().downcast_mut::<Store<AggregateOf<I>>>().expect("store matches its TypeId");
        store.instances.entry(id.clone()).or_default().clone()
    }
}

/// A poisoned lock only means a decision panicked before it committed; the data is consistent.
fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Instance<A: Aggregate> {
    state: A::State,
    events: Vec<Envelope<A::Event>>,
    receipts: HashMap<CommandId, Box<dyn AnyReceipt>>,
}

impl<A: Aggregate> Default for Instance<A> {
    fn default() -> Self {
        Self { state: A::State::default(), events: Vec::new(), receipts: HashMap::new() }
    }
}

struct Store<A: Aggregate> {
    instances: HashMap<A::Id, Arc<Mutex<Instance<A>>>>,
}

impl<A: Aggregate> Default for Store<A> {
    fn default() -> Self {
        Self { instances: HashMap::new() }
    }
}

trait AnyStore: MaybeSend + MaybeSync {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn versions(&self) -> Vec<(StreamId, u64)>;
}

impl<A: Aggregate> AnyStore for Store<A> {
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
