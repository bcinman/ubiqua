//! Stories: given this history, when this command, then these events.
//!
//! A story runs the real model in memory and must account for everything it does. It panics when a
//! command records events no `then` checks, or is rejected and no `rejected` checks it.

use domain::{
    Aggregate, AggregateId, Apply, Command, Cursor, Evolve, Handles, Model, Receipt, SendError, StreamId, Timestamp,
};
use domain_memory::Memory;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

type AggregateOf<I> = <I as AggregateId>::Aggregate;
type EventOf<I> = <AggregateOf<I> as Aggregate>::Event;

/// The model's history and clock. Start one with [`Story::new`].
pub struct Story {
    memory: Memory,
    clock: Arc<AtomicU64>,
    /// How far each stream's events have been accounted for.
    seen: HashMap<StreamId, u64>,
}

impl Default for Story {
    fn default() -> Self {
        Self::new()
    }
}

impl Story {
    /// An empty history, with the clock at the Unix epoch.
    pub fn new() -> Self {
        let clock = Arc::new(AtomicU64::new(0));
        let now = clock.clone();
        let memory = Memory::with_clock(move || Timestamp::from_millis(now.load(Ordering::Relaxed)));
        Self { memory, clock, seen: HashMap::new() }
    }

    /// Runs the model's views too, so `view` and `emitted` can check them.
    pub fn with<M: Model>(self) -> Self {
        let Story { memory, clock, seen } = self;
        Story { memory: memory.with::<M>(), clock, seen }
    }

    /// History: events already recorded on `id`, written without deciding any command.
    pub fn given<I, E>(mut self, id: &I, events: impl IntoIterator<Item = E>) -> Self
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
        E: Into<EventOf<I>>,
    {
        self.memory.append(id, events);
        self.catch_up();
        self
    }

    /// Sets the clock that later commands are decided at.
    pub fn at(self, now: Timestamp) -> Self {
        self.clock.store(now.as_millis(), Ordering::Relaxed);
        self
    }

    /// Sends a command. Check what it did with the returned [`When`].
    pub fn when<C>(self, id: &C::Target, command: C) -> When<C>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let outcome = self.memory.send(id, command);
        When { story: self, outcome, checked_rejection: false, finished: false }
    }

    /// The memory the story runs on, for checks a story step doesn't cover.
    pub fn memory(&self) -> &Memory {
        &self.memory
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
    pub fn then<I, E>(mut self, id: &I, expected: impl IntoIterator<Item = E>) -> Self
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
        E: Into<EventOf<I>>,
    {
        let stream = StreamId::of::<AggregateOf<I>>(id);
        let seen = self.story.seen.get(&stream).copied().unwrap_or(0) as usize;
        let recorded: Vec<_> = self.story.memory.events(id).into_iter().skip(seen).map(|e| e.event).collect();
        let expected: Vec<EventOf<I>> = expected.into_iter().map(Into::into).collect();
        assert_eq!(recorded, expected, "events recorded on {stream}");
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
    pub fn emitted<V: Apply>(self, key: &V::Key, expected: impl IntoIterator<Item = impl Into<V::Event>>) -> Self {
        let entries = self.story.memory.view::<V>().list(key, Cursor::START, usize::MAX);
        let actual: Vec<V::Event> = entries.into_iter().map(|entry| entry.event).collect();
        let expected: Vec<V::Event> = expected.into_iter().map(Into::into).collect();
        assert_eq!(actual, expected, "{} emitted for {key:?}", V::NAME);
        self
    }

    /// Sets the clock for the next command.
    pub fn at(self, now: Timestamp) -> Self {
        self.story.clock.store(now.as_millis(), Ordering::Relaxed);
        self
    }

    /// Adds history, after checking this command was accounted for.
    pub fn given<I, E>(mut self, id: &I, events: impl IntoIterator<Item = E>) -> Story
    where
        I: AggregateId,
        AggregateOf<I>: Evolve,
        E: Into<EventOf<I>>,
    {
        self.finish();
        std::mem::take(&mut self.story).given(id, events)
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
