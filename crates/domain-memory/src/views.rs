//! Hosting views: each view has an inbox of deliveries, applied in order, and a store per key.
//! A view's emitted entries are delivered to the views that read it, the same way.

use crate::lock;
use domain::{
    Apply, Consumes, Cursor, Entry, Frame, MaybeSend, MaybeSync, NotCaughtUp, Position, Snapshot, Source,
    SourcePayload, StreamId, Timestamp, View, ViewCx,
};
use std::any::{Any, TypeId};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

/// A boxed value of any type that can cross threads where there are threads.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type AnyBox = Box<dyn Any + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub(crate) type AnyBox = Box<dyn Any>;

// A view's deliveries carry `(V::Key, V::Event, V::State)`, which is its `SourcePayload`: the View
// derive makes its `Source` types those three.

/// One recorded event on its way to the views that read its source.
pub(crate) struct Delivery {
    /// The `TypeId` of the aggregate or view that recorded it.
    pub source: TypeId,
    pub stream: StreamId,
    pub seq: u64,
    pub at: Timestamp,
    /// The source's [`SourcePayload`]: who recorded it, the event, and the state with it.
    pub payload: AnyBox,
}

impl Delivery {
    pub fn new<T: Any + MaybeSend + MaybeSync>(
        source: TypeId,
        stream: StreamId,
        seq: u64,
        at: Timestamp,
        payload: T,
    ) -> Self {
        Self { source, stream, seq, at, payload: Box::new(payload) }
    }

    /// The order a view added later applies history in.
    pub fn order(&self) -> (Timestamp, &StreamId, u64) {
        (self.at, &self.stream, self.seq)
    }
}

/// A view as the commit path and other views see it.
pub(crate) trait Consumer: MaybeSend + MaybeSync {
    /// Queue a delivery. Called while the source is locked, so each source's order holds.
    fn push(&self, delivery: Arc<Delivery>);
    /// Apply every queued delivery in order, then let the views that read this one apply what it emitted.
    fn drain(&self);
    /// Drain the views this one reads, then this one, so a read sees everything already committed.
    fn catch_up(&self);
    /// How far this view has applied `stream`, directly or through the views it reads.
    fn applied(&self, stream: &StreamId) -> Option<u64>;
    /// Every entry this view has emitted, as deliveries for a view added later that reads it.
    fn history(&self) -> Vec<Arc<Delivery>>;
    /// Deliver this view's emitted entries to `consumer` from now on.
    fn add_downstream(&self, consumer: Arc<dyn Consumer>);
}

type RouteFn<V> = fn(&Delivery) -> Option<(<V as View>::Key, <V as View>::Input)>;

fn route_of<V: Consumes<S> + Apply, S: Source>(delivery: &Delivery) -> Option<(V::Key, V::Input)> {
    let (id, event, state) = (*delivery.payload).downcast_ref::<SourcePayload<S>>()?;
    V::route(id, event, state)
}

pub(crate) struct ViewHost<V: Apply> {
    routes: HashMap<TypeId, RouteFn<V>>,
    /// The views this one reads.
    upstream: Vec<Arc<dyn Consumer>>,
    /// The views that read this one.
    downstream: Mutex<Vec<Arc<dyn Consumer>>>,
    inbox: Mutex<VecDeque<Arc<Delivery>>>,
    data: Mutex<Data<V>>,
}

struct Data<V: Apply> {
    keys: HashMap<V::Key, Key<V>>,
    /// The last seq applied from each source stream, whether or not the view read it.
    progress: HashMap<StreamId, u64>,
}

struct Key<V: Apply> {
    state: Option<Arc<V::State>>,
    entries: Vec<Entry<V>>,
    feeds: Vec<Weak<Queue<Frame<V>>>>,
    watches: Vec<Weak<Queue<Snapshot<V>>>>,
}

impl<V: Apply> Default for Key<V> {
    fn default() -> Self {
        Self { state: None, entries: Vec::new(), feeds: Vec::new(), watches: Vec::new() }
    }
}

impl<V: Apply> Key<V> {
    fn snapshot(&self) -> Snapshot<V> {
        Snapshot { state: self.state.as_deref().cloned(), cursor: Cursor::new(self.entries.len() as u64) }
    }
}

impl<V: Apply> ViewHost<V> {
    pub fn new() -> Self {
        Self {
            routes: HashMap::new(),
            upstream: Vec::new(),
            downstream: Mutex::default(),
            inbox: Mutex::default(),
            data: Mutex::new(Data { keys: HashMap::new(), progress: HashMap::new() }),
        }
    }

    pub fn add_source<S: Source>(&mut self)
    where
        V: Consumes<S>,
    {
        self.routes.insert(TypeId::of::<S>(), route_of::<V, S>);
    }

    pub fn add_upstream(&mut self, upstream: Arc<dyn Consumer>) {
        self.upstream.push(upstream);
    }

    pub fn sources(&self) -> impl Iterator<Item = TypeId> + '_ {
        self.routes.keys().copied()
    }

    fn apply_one(&self, data: &mut Data<V>, delivery: &Delivery) {
        let route = self.routes[&delivery.source];
        if let Some((key, input)) = route(delivery) {
            // Run the view's code on copies, so a panic leaves the key as it was.
            let before = data.keys.get(&key).and_then(|k| k.state.clone());
            let mut state = before.as_deref().cloned().unwrap_or_default();
            let auto = V::auto_emit(&input);
            let mut cx = ViewCx::new(&key, &delivery.stream, delivery.seq, delivery.at, &*delivery.payload);
            V::apply(&mut state, input, &mut cx);
            let emitted: Vec<V::Event> = auto.into_iter().chain(cx.into_emitted()).collect();

            let changed = before.as_deref() != Some(&state);
            let state = Arc::new(state);
            let stream = StreamId::new(V::NAME, &key);
            let downstream = lock(&self.downstream);
            let k = data.keys.entry(key.clone()).or_default();
            k.state = Some(state.clone());
            for event in emitted {
                let cursor = Cursor::new(k.entries.len() as u64 + 1);
                if !downstream.is_empty() {
                    let payload: (V::Key, V::Event, V::State) = (key.clone(), event.clone(), (*state).clone());
                    let next =
                        Arc::new(Delivery::new(TypeId::of::<V>(), stream.clone(), cursor.seq(), delivery.at, payload));
                    downstream.iter().for_each(|consumer| consumer.push(next.clone()));
                }
                let entry = Entry {
                    cursor,
                    event,
                    state: state.clone(),
                    stream: delivery.stream.clone(),
                    seq: delivery.seq,
                    at: delivery.at,
                };
                k.entries.push(entry.clone());
                publish(&mut k.feeds, Frame::Entry(entry));
            }
            if changed {
                let snapshot = k.snapshot();
                publish(&mut k.watches, snapshot);
            }
        }
        data.progress.insert(delivery.stream.clone(), delivery.seq);
    }

    fn read<T>(&self, f: impl FnOnce(&mut Data<V>) -> T) -> T {
        self.catch_up();
        f(&mut lock(&self.data))
    }
}

impl<V: Apply> Consumer for ViewHost<V> {
    fn push(&self, delivery: Arc<Delivery>) {
        lock(&self.inbox).push_back(delivery);
    }

    fn drain(&self) {
        {
            // Holding `data` makes this the only drainer; a delivery leaves the inbox once applied.
            let mut data = lock(&self.data);
            loop {
                let next = lock(&self.inbox).front().cloned();
                let Some(delivery) = next else { break };
                self.apply_one(&mut data, &delivery);
                lock(&self.inbox).pop_front();
            }
        }
        let downstream = lock(&self.downstream).clone();
        downstream.iter().for_each(|consumer| consumer.drain());
    }

    fn catch_up(&self) {
        self.upstream.iter().for_each(|upstream| upstream.catch_up());
        self.drain();
    }

    fn applied(&self, stream: &StreamId) -> Option<u64> {
        let own = lock(&self.data).progress.get(stream).copied();
        // A stream reaches this view along several paths; it's applied as far as the slowest one.
        own.into_iter().chain(self.upstream.iter().filter_map(|upstream| upstream.applied(stream))).min()
    }

    fn history(&self) -> Vec<Arc<Delivery>> {
        let data = lock(&self.data);
        let mut history = Vec::new();
        for (key, k) in &data.keys {
            let stream = StreamId::new(V::NAME, key);
            for entry in &k.entries {
                let payload: (V::Key, V::Event, V::State) = (key.clone(), entry.event.clone(), (*entry.state).clone());
                let delivery = Delivery::new(TypeId::of::<V>(), stream.clone(), entry.cursor.seq(), entry.at, payload);
                history.push(Arc::new(delivery));
            }
        }
        history
    }

    fn add_downstream(&self, consumer: Arc<dyn Consumer>) {
        lock(&self.downstream).push(consumer);
    }
}

/// Reads one view: state, emitted events, live feeds. Get it with [`Memory::view`](crate::Memory::view).
pub struct ViewReader<V: Apply> {
    pub(crate) host: Arc<ViewHost<V>>,
}

impl<V: Apply> Clone for ViewReader<V> {
    fn clone(&self) -> Self {
        Self { host: self.host.clone() }
    }
}

impl<V: Apply> ViewReader<V> {
    /// The key's state, or `None` before its first event.
    pub fn get(&self, key: &V::Key) -> Option<V::State> {
        self.host.read(|data| data.keys.get(key).and_then(|k| k.state.as_deref().cloned()))
    }

    /// The key's state with the cursor it's valid at.
    pub fn snapshot(&self, key: &V::Key) -> Snapshot<V> {
        self.host.read(|data| match data.keys.get(key) {
            Some(k) => k.snapshot(),
            None => Snapshot { state: None, cursor: Cursor::START },
        })
    }

    /// Up to `take` of the key's emitted events after `after`, oldest first.
    pub fn list(&self, key: &V::Key, after: Cursor, take: usize) -> Vec<Entry<V>> {
        self.host.read(|data| {
            let entries = data.keys.get(key).map_or(&[][..], |k| &k.entries[..]);
            entries.iter().skip(after.seq() as usize).take(take).cloned().collect()
        })
    }

    /// The key's emitted events after `after`, then `CaughtUp`, then new ones as they're recorded.
    pub fn events(&self, key: &V::Key, after: Cursor) -> Feed<V> {
        self.host.read(|data| {
            let k = data.keys.entry(key.clone()).or_default();
            let queue = Arc::new(Queue::default());
            {
                let mut q = lock(&queue.inner);
                q.items.extend(k.entries.iter().skip(after.seq() as usize).cloned().map(Frame::Entry));
                let cursor = Cursor::new(k.entries.len().max(after.seq() as usize) as u64);
                q.items.push_back(Frame::CaughtUp { cursor });
            }
            k.feeds.push(Arc::downgrade(&queue));
            Subscription { queue }
        })
    }

    /// The key's state now, then again each time it changes.
    pub fn watch(&self, key: &V::Key) -> Watch<V> {
        self.host.read(|data| {
            let k = data.keys.entry(key.clone()).or_default();
            let queue = Arc::new(Queue::default());
            lock(&queue.inner).items.push_back(k.snapshot());
            k.watches.push(Arc::downgrade(&queue));
            Subscription { queue }
        })
    }

    /// Checks the view has applied `position`, such as a command's receipt, so a read sees that
    /// command. Counts what it reads through other views. Fails when it hasn't applied it, or when
    /// the stream doesn't reach this view.
    pub fn at_least(&self, position: impl Into<Position>) -> Result<&Self, NotCaughtUp> {
        let Position { stream, version } = position.into();
        self.host.catch_up();
        let applied = self.host.applied(&stream).unwrap_or(0);
        if applied >= version { Ok(self) } else { Err(NotCaughtUp { view: V::NAME, stream, wanted: version, applied }) }
    }
}

/// A live feed of a view key's emitted events.
pub type Feed<V> = Subscription<Frame<V>>;

/// A live feed of a view key's state.
pub type Watch<V> = Subscription<Snapshot<V>>;

/// Items arriving from a view. Read them each frame with `try_next`, or `.recv().await` them on
/// any executor. Dropping it unsubscribes.
pub struct Subscription<T> {
    queue: Arc<Queue<T>>,
}

impl<T> Subscription<T> {
    /// The next item, if one has arrived.
    pub fn try_next(&self) -> Option<T> {
        lock(&self.queue.inner).items.pop_front()
    }

    /// Every item that has arrived.
    pub fn drain(&self) -> Vec<T> {
        lock(&self.queue.inner).items.drain(..).collect()
    }

    /// Waits for the next item. Uses only the waker, so it runs on any executor.
    pub fn recv(&mut self) -> Recv<'_, T> {
        Recv { queue: &self.queue }
    }
}

/// The future [`Subscription::recv`] returns.
pub struct Recv<'a, T> {
    queue: &'a Queue<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut q = lock(&self.queue.inner);
        match q.items.pop_front() {
            Some(item) => Poll::Ready(item),
            None => {
                q.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

struct Queue<T> {
    inner: Mutex<QueueInner<T>>,
}

struct QueueInner<T> {
    items: VecDeque<T>,
    waker: Option<Waker>,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self { inner: Mutex::new(QueueInner { items: VecDeque::new(), waker: None }) }
    }
}

/// Sends `item` to every live subscriber, dropping the ones that are gone.
fn publish<T: Clone>(subscribers: &mut Vec<Weak<Queue<T>>>, item: T) {
    subscribers.retain(|weak| {
        let Some(queue) = weak.upgrade() else { return false };
        let waker = {
            let mut q = lock(&queue.inner);
            q.items.push_back(item.clone());
            q.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        true
    });
}
