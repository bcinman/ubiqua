//! Telemetry: what an observer sees of a running memory. While nobody observes, it costs one
//! check per command, event and view update. With an observer, each is encoded as JSON
//! as it happens and queued for every observer, in one order.
//!
//! A replay runs the recorded events through a memory of its own that hosts the same views, so
//! it shows the same view changes the live trace did, without touching this one's views.

use crate::views::{Publisher, Queue, Subscription, Wakeup};
use crate::{Instances, Memory, decode_event, lock};
use domain::telemetry::{AggregateTrace, Catalog, EventTrace, Kind, Part, Since, Status, Trace, ViewKey};
use domain::{Aggregate, Envelope, Evolve, StreamId, Timestamp};
use serde::Serialize;
use serde_json::Value;
use std::any::TypeId;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::sync::Weak;

/// A live trace: what happened since it started, then [`Trace::CaughtUp`], then what happens.
/// Read it like any [`Subscription`]; dropping it stops it.
pub type TraceFeed = Subscription<Trace>;

/// Reads a running memory's telemetry. Get it with [`Memory::telemetry`].
///
/// It sees every aggregate, event and view, whoever may read them: serve it only to people
/// trusted with all of it.
pub struct Telemetry<'m> {
    pub(crate) memory: &'m Memory,
}

impl Memory {
    /// What an observer, such as a dashboard, reads: the model's parts, where it stands, and a
    /// trace of what happens in it, replayed or live.
    pub fn telemetry(&self) -> Telemetry<'_> {
        Telemetry { memory: self }
    }
}

impl Telemetry<'_> {
    /// The aggregates, views, policies, projections and sagas hosted here, and what each reads.
    pub fn catalog(&self) -> Catalog {
        Catalog { parts: self.memory.parts.clone() }
    }

    /// Every stream's version, and what's failing.
    pub fn status(&self) -> Result<Status, String> {
        let streams = self.memory.try_versions()?.into_iter().map(|(s, v)| (s.to_string(), v)).collect();
        Ok(Status { streams, failures: self.memory.failures() })
    }

    /// The aggregate instance recording `stream`, such as `Account/sam-main`, as it stood at
    /// version `at`, or now. `None` when the stream has no events or its aggregate isn't hosted.
    pub fn aggregate(&self, stream: &str, at: Option<u64>) -> Result<Option<AggregateTrace>, String> {
        let Some((name, _)) = stream.split_once('/') else { return Ok(None) };
        match self.memory.aggregates().into_iter().find(|traced| traced.name == name) {
            Some(traced) => (traced.inspect)(self.memory, stream, at),
            None => Ok(None),
        }
    }

    /// Every key of the view named `view` that has a state, by stream. `None` when it isn't hosted.
    pub fn view(&self, view: &str) -> Option<Vec<ViewKey>> {
        self.memory.view_keys.get(view).map(|keys| keys(self.memory))
    }

    /// What happened since `since`, oldest first, then [`Trace::CaughtUp`], then each command,
    /// event and view change as it happens. Each event shows once, however the replay and the
    /// live part overlap.
    ///
    /// A replay reads every event recorded, here or in the store, and runs it through fresh
    /// copies of the views, in the order a view added later receives them. Commands aren't
    /// recorded, so it shows only their events.
    pub fn trace(&self, since: Since) -> Result<TraceFeed, String> {
        let (publisher, feed) = crate::channel(());
        self.trace_to(since, &publisher)?;
        Ok(feed)
    }

    /// Like [`trace`](Self::trace), into a feed made elsewhere with [`channel`](crate::channel),
    /// such as on a thread that hands the runtime a job to start it. It runs until that feed is
    /// dropped.
    pub fn trace_to(&self, since: Since, feed: &Publisher<Trace>) -> Result<(), String> {
        let hub = &self.memory.hub;
        // Observe first, then read what's recorded: whatever commits in between shows in both,
        // and the live copy is skipped.
        hub.register(&feed.queue);
        let (replayed, streams) = match since {
            Since::Now => {
                let versions = self.memory.try_versions()?;
                (Vec::new(), versions.into_iter().map(|(s, v)| (s.to_string(), v)).collect())
            }
            Since::Start => self.replay(&BTreeMap::new())?,
            Since::After(after) => self.replay(&after)?,
        };
        hub.join(&feed.queue, replayed, streams);
        Ok(())
    }

    /// The traces of every recorded event and the view changes it caused, skipping those at or
    /// before `after`, and how far they reach in each stream.
    fn replay(&self, after: &BTreeMap<String, u64>) -> Result<(Vec<Trace>, BTreeMap<String, u64>), String> {
        let mut shadow = Memory::with_clock(Timestamp::default);
        for host in &self.memory.replay_views {
            host(&mut shadow);
        }
        let watching = shadow.telemetry().trace(Since::Now)?;
        for past in self.memory.past()? {
            (past.replay)(&shadow);
        }
        let mut streams = after.clone();
        let mut replayed = Vec::new();
        for trace in watching.drain() {
            match &trace {
                Trace::Event(e) => reach(&mut streams, &e.stream, e.seq),
                Trace::View(v) => v.emitted.iter().for_each(|e| reach(&mut streams, &v.stream, e.cursor)),
                Trace::Command(_) => {}
                Trace::CaughtUp { .. } | Trace::Failed(_) => continue,
            }
            if !covered(after, &trace) {
                replayed.push(trace);
            }
        }
        Ok((replayed, streams))
    }
}

fn reach(streams: &mut BTreeMap<String, u64>, stream: &str, seq: u64) {
    let at = streams.entry(stream.to_string()).or_default();
    *at = (*at).max(seq);
}

/// Whether `trace` is an event, or a view change caused by one, at or before `streams`.
fn covered(streams: &BTreeMap<String, u64>, trace: &Trace) -> bool {
    let (stream, seq) = match trace {
        Trace::Event(e) => (&e.stream, e.seq),
        Trace::View(v) => (&v.cause.stream, v.cause.seq),
        Trace::Command(_) | Trace::CaughtUp { .. } | Trace::Failed(_) => return false,
    };
    streams.get(stream).is_some_and(|&at| seq <= at)
}

/// Hands traces to observers. Shared by a memory and its views, on the memory's thread.
#[derive(Default)]
pub(crate) struct Hub {
    inner: RefCell<HubInner>,
}

#[derive(Default)]
struct HubInner {
    observers: Vec<Observer>,
    /// Observers to wake once the publisher is done: see [`Hub::flush`].
    wakeups: Vec<Wakeup>,
}

impl Hub {
    /// Whether anyone observes. Check it before building traces.
    pub(crate) fn observed(&self) -> bool {
        !self.inner.borrow().observers.is_empty()
    }

    fn register(&self, queue: &Weak<Queue<Trace>>) {
        let observer = Observer { queue: queue.clone(), join: Join::Replaying(Vec::new()) };
        self.inner.borrow_mut().observers.push(observer);
    }

    /// Queues `traces` for every observer, in this order for all of them. Observers are woken
    /// later, by [`flush`](Self::flush). Drops those whose feed was dropped.
    pub(crate) fn publish(&self, traces: &[Trace]) {
        let mut inner = self.inner.borrow_mut();
        let HubInner { observers, wakeups } = &mut *inner;
        observers.retain_mut(|observer| match observer.queue.upgrade() {
            Some(queue) => {
                traces.iter().for_each(|trace| observer.join.offer(&queue, trace, wakeups));
                true
            }
            None => false,
        });
    }

    /// Wakes the observers that have new traces. Call it once the publisher is done, so an
    /// observer's callback may send commands.
    pub(crate) fn flush(&self) {
        let wakeups = std::mem::take(&mut self.inner.borrow_mut().wakeups);
        wakeups.into_iter().for_each(Wakeup::wake);
    }

    /// Queues the replay, `CaughtUp`, and the live traces that arrived meanwhile for the feed
    /// `queue`, then takes it live.
    fn join(&self, queue: &Weak<Queue<Trace>>, replayed: Vec<Trace>, streams: BTreeMap<String, u64>) {
        let mut inner = self.inner.borrow_mut();
        let Some(observer) = inner.observers.iter_mut().find(|o| o.queue.ptr_eq(queue)) else {
            unreachable!("a feed joins the hub it registered with")
        };
        // Dropped meanwhile: the next publish forgets it.
        let Some(queue) = queue.upgrade() else { return };
        let Join::Replaying(waiting) = std::mem::replace(&mut observer.join, Join::Live(BTreeMap::new())) else {
            unreachable!("an observer joins once")
        };
        let mut q = lock(&queue.inner);
        q.items.extend(replayed);
        q.items.push_back(Trace::CaughtUp { streams: streams.clone() });
        q.items.extend(waiting.into_iter().filter(|trace| !covered(&streams, trace)));
        observer.join = Join::Live(streams);
    }
}

/// One trace feed, as the hub sees it.
struct Observer {
    /// The feed's queue; gone once the feed is dropped.
    queue: Weak<Queue<Trace>>,
    join: Join,
}

enum Join {
    /// The replay is being read: live traces wait here.
    Replaying(Vec<Trace>),
    /// Live, skipping what the replay covered, up to these positions.
    Live(BTreeMap<String, u64>),
}

impl Join {
    fn offer(&mut self, queue: &Queue<Trace>, trace: &Trace, wakeups: &mut Vec<Wakeup>) {
        match self {
            Join::Replaying(waiting) => waiting.push(trace.clone()),
            Join::Live(replayed) if covered(replayed, trace) => {}
            Join::Live(_) => {
                let mut q = lock(&queue.inner);
                q.items.push_back(trace.clone());
                wakeups.extend(q.waker.take().map(Wakeup::Waker));
                wakeups.extend(q.notify.clone().map(Wakeup::Notify));
            }
        }
    }
}

/// A recorded event, ready to run through a replay's views.
pub(crate) struct Past {
    at: Timestamp,
    stream: StreamId,
    seq: u64,
    replay: Box<dyn FnOnce(&Memory)>,
}

type Recorded<A> = Vec<(<A as Aggregate>::Id, Envelope<<A as Aggregate>::Event>)>;
type Inspect = fn(&Memory, &str, Option<u64>) -> Result<Option<AggregateTrace>, String>;

/// How telemetry reads one aggregate.
#[derive(Clone, Copy)]
pub(crate) struct Traced {
    name: &'static str,
    /// Every event it recorded, here or in the store.
    past: fn(&Memory) -> Result<Vec<Past>, String>,
    /// The instance recording a stream, at a version.
    inspect: Inspect,
}

impl Traced {
    pub(crate) fn of<A: Evolve>() -> Self {
        Self { name: A::NAME, past: Memory::past_of::<A>, inspect: Memory::inspect::<A> }
    }
}

impl Memory {
    /// Lists a part for the catalog, naming the sources it reads.
    pub(crate) fn add_part(&mut self, kind: Kind, id: TypeId, name: &'static str, reads: &[TypeId], version: u64) {
        self.names.insert(id, name);
        let mut reads: Vec<String> =
            reads.iter().filter_map(|source| self.names.get(source)).map(|n| n.to_string()).collect();
        reads.sort();
        self.parts.push(Part { kind, name: name.into(), reads, version });
    }

    /// Every aggregate hosted: the model's, and those used without one.
    fn aggregates(&self) -> Vec<Traced> {
        let mut all: HashMap<TypeId, Traced> = self.traced.clone();
        for (id, store) in self.stores.borrow_mut().iter() {
            all.entry(*id).or_insert_with(|| store.traced());
        }
        all.into_values().collect()
    }

    /// Every event recorded, in the order a view added later receives them.
    fn past(&self) -> Result<Vec<Past>, String> {
        let mut past = Vec::new();
        for traced in self.aggregates() {
            past.extend((traced.past)(self)?);
        }
        past.sort_by(|a, b| (a.at, &a.stream, a.seq).cmp(&(b.at, &b.stream, b.seq)));
        Ok(past)
    }

    /// Every event `A` recorded, with the id of the instance that recorded it.
    fn recorded<A: Aggregate>(&self, stream: Option<&str>) -> Result<Recorded<A>, String> {
        if let Some(store) = &self.store {
            let rows = match stream {
                Some(stream) => store.stream(stream),
                None => store.history(A::NAME, &|_, _| true),
            };
            let rows = rows.map_err(|e| format!("the store failed: {e}"))?;
            return rows.iter().map(decode_event::<A>).collect();
        }
        let instances: Vec<_> = {
            let stores = self.stores.borrow_mut();
            let Some(store) = stores.get(&std::any::TypeId::of::<A>()) else { return Ok(Vec::new()) };
            let store = store.as_any().downcast_ref::<Instances<A>>().expect("store matches its TypeId");
            store
                .instances
                .iter()
                .filter(|(id, _)| stream.is_none_or(|s| StreamId::of::<A>(id).as_str() == s))
                .map(|(id, instance)| (id.clone(), instance.clone()))
                .collect()
        };
        Ok(instances
            .into_iter()
            .flat_map(|(id, instance)| {
                instance.borrow_mut().events.iter().map(|e| (id.clone(), e.clone())).collect::<Vec<_>>()
            })
            .collect())
    }

    pub(crate) fn past_of<A: Evolve>(&self) -> Result<Vec<Past>, String> {
        Ok(self
            .recorded::<A>(None)?
            .into_iter()
            .map(|(id, envelope)| Past {
                at: envelope.at,
                stream: envelope.stream.clone(),
                seq: envelope.seq,
                replay: Box::new(move |shadow: &Memory| shadow.replay_event::<A>(&id, envelope)),
            })
            .collect())
    }

    /// Records an event exactly as it was recorded elsewhere, for a replay's views.
    fn replay_event<A: Evolve>(&self, id: &A::Id, envelope: Envelope<A::Event>) {
        {
            let instance = self.instance(id);
            let mut instance = instance.borrow_mut();
            // Views don't read the aggregate's state, so the replay doesn't evolve it.
            let state = instance.state.clone();
            self.commit(id, &mut instance, state, vec![envelope], false);
        }
        self.deliver::<A>();
    }

    pub(crate) fn inspect<A: Evolve>(&self, stream: &str, at: Option<u64>) -> Result<Option<AggregateTrace>, String> {
        let recorded = self.recorded::<A>(Some(stream))?;
        let Some((id, _)) = recorded.first() else { return Ok(None) };
        let id = value(id);
        let version = at.unwrap_or(u64::MAX).min(recorded.len() as u64);
        let mut state = A::State::default();
        let mut events = Vec::new();
        for (id, envelope) in recorded.iter().take(version as usize) {
            A::evolve(&mut state, &envelope.event);
            events.push(event_trace::<A>(id, envelope));
        }
        let (aggregate, stream, state) = (A::NAME.into(), stream.into(), format!("{state:#?}"));
        Ok(Some(AggregateTrace { aggregate, stream, id, version, state, events }))
    }
}

pub(crate) fn event_trace<A: Aggregate>(id: &A::Id, envelope: &Envelope<A::Event>) -> EventTrace {
    EventTrace {
        aggregate: A::NAME.into(),
        stream: envelope.stream.to_string(),
        seq: envelope.seq,
        id: value(id),
        at: envelope.at.as_millis(),
        command_id: envelope.command_id.as_ref().map(ToString::to_string),
        event: value(&envelope.event),
    }
}

/// `value` as JSON. Everything traced is also stored, so this fails only for a value the store
/// couldn't keep either; the trace then says why.
pub(crate) fn value<T: Serialize + ?Sized>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|e| Value::String(format!("doesn't encode as JSON: {e}")))
}
