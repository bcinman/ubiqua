//! Stories: given this history, when this command, then these events.
//!
//! A story runs the real model in memory and must account for everything it does. It panics when a
//! command records events no `then` checks, is rejected and no `rejected` checks it, or an external
//! call is left unanswered. Policies run after every step, and calls to external systems without an
//! adapter wait until the story answers them.

use domain::{
    Adapter, Aggregate, AggregateId, Apply, Call, CallError, Command, CurrentActor, Cursor, Evolve, Handles,
    HostPolicy, HostSaga, Hosts, IntoEvents, Model, Receipt, SendError, StreamId, Timestamp,
};
use domain_memory::{Memory, Project, React, Run, SagaStatus, StubbedCall};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

type AggregateOf<I> = <I as AggregateId>::Aggregate;
type EventOf<I> = <AggregateOf<I> as Aggregate>::Event;

/// The model's history and clock. Start one with [`Story::new`].
pub struct Story {
    memory: Arc<Memory>,
    clock: Arc<AtomicU64>,
    /// How far each stream's events have been accounted for.
    seen: HashMap<StreamId, u64>,
    /// Who sends the commands, from `acting_as`.
    actor: Option<CurrentActor>,
}

impl Default for Story {
    fn default() -> Self {
        Self::new()
    }
}

/// Hosts a model's sagas but not its policies, for [`Story::with`].
#[doc(hidden)]
pub struct Sagas(Memory);

impl<P: domain::Policy> HostPolicy<P> for Sagas {
    fn host_policy(self) -> Self {
        self
    }
}

impl<S: Run> HostSaga<S> for Sagas {
    fn host_saga(self) -> Self {
        Sagas(self.0.saga::<S>())
    }
}

impl Story {
    /// An empty history, with the clock at the Unix epoch.
    pub fn new() -> Self {
        let clock = Arc::new(AtomicU64::new(0));
        let now = clock.clone();
        let memory = Memory::with_clock(move || Timestamp::from_millis(now.load(Ordering::Relaxed))).stub_calls();
        Self { memory: Arc::new(memory), clock, seen: HashMap::new(), actor: None }
    }

    /// Runs the model's views and sagas too, so `view` and `emitted` can check them. Its policies
    /// don't run: add each one a story is about with [`policy`](Self::policy).
    pub fn with<M: Model + Hosts<Sagas>>(self) -> Self {
        self.build(|memory| M::host(Sagas(memory.with_views::<M>())).0)
    }

    /// Runs a policy after every step. Its commands' events must be checked like any others.
    pub fn policy<P: React>(self, policy: P) -> Self {
        self.build(|memory| memory.policy(policy))
    }

    /// Runs a projection after every step. Give it a store you can read back.
    pub fn projection<P: Project>(self, projection: P) -> Self {
        self.build(|memory| memory.projection(projection))
    }

    /// Runs the saga `S`'s runs after every step. A run that dies fails the story.
    pub fn saga<S: Run>(self) -> Self {
        self.build(Memory::saga::<S>)
    }

    /// Answers `system`'s calls with a real adapter instead of the story.
    pub fn external<S: Adapter<I>, I>(self, system: S, adapter: I) -> Self {
        self.build(|memory| memory.external(system, adapter))
    }

    fn build(self, f: impl FnOnce(Memory) -> Memory) -> Self {
        let Story { memory, clock, seen, actor } = self;
        let memory = Arc::try_unwrap(memory).unwrap_or_else(|_| panic!("story: set it up before `given` and `when`"));
        Story { memory: Arc::new(f(memory)), clock, seen, actor }
    }

    /// History: events already recorded on `id`, written without deciding any command.
    pub fn given<I>(mut self, id: &I, events: impl IntoEvents<EventOf<I>>) -> Self
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        self.memory.append(id, events);
        self.catch_up();
        self.run();
        self
    }

    /// Sets the clock that later commands are decided at. Scheduled commands now due run.
    ///
    /// # Panics
    /// When `now` is earlier than the clock.
    pub fn at(self, now: Timestamp) -> Self {
        let before = Timestamp::from_millis(self.clock.load(Ordering::Relaxed));
        assert!(now >= before, "story: the clock only moves forward, from {before:?} to {now:?}");
        self.clock.store(now.as_millis(), Ordering::Relaxed);
        self.run();
        self
    }

    /// Moves the clock forward. Scheduled commands now due run.
    pub fn advance(self, by: Duration) -> Self {
        let now = Timestamp::from_millis(self.clock.load(Ordering::Relaxed)).after(by);
        self.at(now)
    }

    /// Sends the commands after it on behalf of `actor`, as a signed-in user would. `decide`
    /// reads it with `cx.actor::<T>()`.
    pub fn acting_as<T: std::any::Any + std::fmt::Debug + domain::MaybeSend + domain::MaybeSync>(
        mut self,
        actor: T,
    ) -> Self {
        self.actor = Some(CurrentActor::new(actor));
        self
    }

    /// Sends a command. Check what it did with the returned [`When`].
    pub fn when<C>(self, id: &C::Target, command: C) -> When<C>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let outcome = self.memory.send_as(self.actor.as_ref(), id, command, self.memory.new_command_id());
        self.run();
        When { story: self, outcome, checked_rejection: false, finished: false }
    }

    /// The memory the story runs on, for checks a story step doesn't cover.
    pub fn memory(&self) -> &Memory {
        &self.memory
    }

    /// Runs the policies and anything due, and fails on a policy that failed.
    fn run(&self) {
        self.memory.run_due();
        let failures = self.memory.failures();
        if !failures.is_empty() {
            panic!("story: {}", failures.join("; "));
        }
    }

    fn catch_up(&mut self) {
        self.seen.extend(self.memory.versions());
    }

    fn unchecked(&self) -> Vec<String> {
        self.memory
            .versions()
            .into_iter()
            .filter_map(|(stream, version)| {
                let seen = self.seen.get(&stream).copied().unwrap_or(0);
                (version > seen).then(|| format!("{} event(s) on {stream}", version - seen))
            })
            .chain(self.memory.waiting_calls().into_iter().map(|call| format!("call {call}; check it with .call(..)")))
            .collect()
    }
}

/// The outcome of one command, to check with `then`, `rejected` and `replied`. When it's
/// dropped, it panics if anything the command did went unchecked.
pub struct When<C: Command> {
    story: Story,
    outcome: Result<Receipt<C>, SendError<C::Rejection>>,
    checked_rejection: bool,
    finished: bool,
}

impl<C: Command> When<C> {
    /// Checks the events recorded on `id` since the last check.
    pub fn then<I>(mut self, id: &I, expected: impl IntoEvents<EventOf<I>>) -> Self
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        let stream = StreamId::of::<AggregateOf<I>>(id);
        let seen = self.story.seen.get(&stream).copied().unwrap_or(0) as usize;
        let recorded: Vec<_> = self.story.memory.events(id).into_iter().skip(seen).map(|e| e.event).collect();
        assert_eq!(recorded, expected.into_events(), "events recorded on {stream}");
        self.story.seen.insert(stream, (seen + recorded.len()) as u64);
        self
    }

    /// Checks that no events were recorded on `id` since the last check.
    pub fn then_none<I: AggregateId>(self, id: &I) -> Self
    where
        AggregateOf<I>: Evolve,
    {
        self.then(id, Vec::<EventOf<I>>::new())
    }

    /// Checks that the command was refused with exactly this rejection.
    pub fn rejected(mut self, rejection: impl Into<C::Rejection>) -> Self {
        let expected = rejection.into();
        match &self.outcome {
            Err(SendError::Rejected(actual)) => assert_eq!(actual, &expected, "{} was rejected", C::NAME),
            Err(other) => panic!("{} failed: {other:?}, expected rejection {expected:?}", C::NAME),
            Ok(receipt) => panic!("{} succeeded with {:?}, expected rejection {expected:?}", C::NAME, receipt.events),
        }
        self.checked_rejection = true;
        self
    }

    /// Checks the command's reply.
    pub fn replied(self, reply: C::Reply) -> Self {
        match &self.outcome {
            Ok(receipt) => assert_eq!(receipt.reply, reply, "{} replied", C::NAME),
            Err(error) => panic!("{} failed: {error:?}, expected reply {reply:?}", C::NAME),
        }
        self
    }

    /// Checks a view's state for one key.
    pub fn view<V: Apply>(self, key: &V::Key, expected: V::State) -> Self {
        let actual = self.story.memory.view::<V>().get(key);
        assert_eq!(actual.as_ref(), Some(&expected), "{} for {key:?}", V::NAME);
        self
    }

    /// Checks every event a view has emitted for one key, oldest first.
    pub fn emitted<V: Apply>(self, key: &V::Key, expected: impl IntoEvents<V::Event>) -> Self {
        let entries = self.story.memory.view::<V>().list(key, Cursor::START, usize::MAX);
        let actual: Vec<V::Event> = entries.into_iter().map(|entry| entry.event).collect();
        assert_eq!(actual, expected.into_events(), "{} emitted for {key:?}", V::NAME);
        self
    }

    /// Checks where the run of `S` under `key` stands.
    pub fn saga_status<S: Run>(self, key: &S::Key, expected: SagaStatus<S::Rejection>) -> Self {
        assert_eq!(self.story.memory.saga_status::<S>(key), expected, "{} {key}", S::NAME);
        self
    }

    /// Sends the run of `S` under `key` a signal, then runs what it does next.
    pub fn signal<S: Run>(self, key: &S::Key, name: &str, value: impl serde::Serialize) -> Self {
        if let Err(e) = self.story.memory.signal::<S>(key, name, value) {
            panic!("story: signal {name} to {} {key}: {e}", S::NAME);
        }
        self.story.run();
        self
    }

    /// Sets the clock. Scheduled commands now due run; check their events with `then`.
    pub fn at(mut self, now: Timestamp) -> Self {
        let story = std::mem::take(&mut self.story);
        self.story = story.at(now);
        self
    }

    /// Moves the clock forward. Scheduled commands now due run.
    pub fn advance(mut self, by: Duration) -> Self {
        let story = std::mem::take(&mut self.story);
        self.story = story.advance(by);
        self
    }

    /// Checks the next call a policy made to an external system, which waits for an answer:
    /// follow it with `succeed` or `fail`.
    pub fn call<K: Call>(self, expected: K) -> Called<C, K> {
        let Some(call) = self.story.memory.next_call() else {
            panic!("story: expected {}.{} {expected:?}, but nothing called", K::SYSTEM, K::NAME);
        };
        match call.args::<K>() {
            Some(args) => assert_eq!(args, &expected, "{}.{} was called with", K::SYSTEM, K::NAME),
            None => panic!("story: expected {}.{} {expected:?}, but the next call is {call}", K::SYSTEM, K::NAME),
        }
        Called { when: Some(self), call: Some(call), _call: PhantomData }
    }

    /// Adds history, after checking this command was accounted for.
    pub fn given<I>(mut self, id: &I, events: impl IntoEvents<EventOf<I>>) -> Story
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
    {
        self.finish();
        std::mem::take(&mut self.story).given(id, events)
    }

    /// Sends the commands after it on behalf of `actor`, after checking this one was accounted for.
    pub fn acting_as<T: std::any::Any + std::fmt::Debug + domain::MaybeSend + domain::MaybeSync>(
        mut self,
        actor: T,
    ) -> Story {
        self.finish();
        std::mem::take(&mut self.story).acting_as(actor)
    }

    /// Sends the next command, after checking this one was accounted for.
    pub fn when<D>(mut self, id: &D::Target, command: D) -> When<D>
    where
        D: Command,
        AggregateOf<D::Target>: Handles<D>,
    {
        self.finish();
        std::mem::take(&mut self.story).when(id, command)
    }

    /// The receipt or error, for checks a story step doesn't cover.
    pub fn outcome(&self) -> &Result<Receipt<C>, SendError<C::Rejection>> {
        &self.outcome
    }

    fn finish(&mut self) {
        self.finished = true;
        self.story.run();
        let mut problems = self.story.unchecked();
        if !self.checked_rejection {
            match &self.outcome {
                Err(SendError::Rejected(r)) => problems.push(format!("rejection {r:?}; check it with .rejected(..)")),
                Err(other) => problems.push(format!("error {other:?}")),
                Ok(_) => {}
            }
        }
        if !problems.is_empty() {
            panic!("story: {} left unchecked: {}", C::NAME, problems.join(", "));
        }
    }
}

impl<C: Command> Drop for When<C> {
    fn drop(&mut self) {
        if !self.finished && !std::thread::panicking() {
            self.finish();
        }
    }
}

/// A call the story checked, waiting for its answer.
pub struct Called<C: Command, K: Call> {
    when: Option<When<C>>,
    call: Option<StubbedCall>,
    _call: PhantomData<fn() -> K>,
}

impl<C: Command, K: Call> Called<C, K> {
    /// Answers the call with what it returns. The policy carries on.
    pub fn succeed(self, returns: K::Returns) -> When<C> {
        self.answer(Ok(returns))
    }

    /// Answers the call with one of its declared errors. The policy carries on.
    pub fn fail(self, error: impl Into<K::Error>) -> When<C> {
        self.answer(Err(CallError::Refused(error.into())))
    }

    fn answer(mut self, answer: domain::Answered<K>) -> When<C> {
        self.call.take().expect("answered once").answer::<K>(answer);
        let when = self.when.take().expect("answered once");
        when.story.run();
        when
    }
}

impl<C: Command, K: Call> Drop for Called<C, K> {
    fn drop(&mut self) {
        if self.call.is_some() && !std::thread::panicking() {
            panic!(
                "story: {}.{} was checked but not answered; follow .call(..) with .succeed(..) or .fail(..)",
                K::SYSTEM,
                K::NAME
            );
        }
    }
}
