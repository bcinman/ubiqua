use domain::{
    Aggregate, Answer, Answered, Call, Command, CommandId, Cx, Decide, Decision, Event, Evolve, ExternalSystem, Model,
    Policy, StreamId, Timestamp,
};
use domain_memory::{Memory, PolicyCx, PolicyError, React, ScheduleError, UnscheduleError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

// A counter is bumped; `Echo` pings `<id>-echo` for each bump.

domain::id!(
    pub struct CounterId;
);

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = CounterId, events(Bumped))]
pub struct Bump;

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = CounterId, events(Pinged))]
pub struct Ping;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Bumped;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pinged;

#[derive(Aggregate)]
#[aggregate(id = CounterId, state = (), events(Bumped, Pinged), commands(Bump, Ping))]
pub struct Counter;

impl Evolve for Counter {
    fn evolve(_: &mut (), _: &CounterEvent) {}
}

impl Decide<Bump> for Counter {
    fn decide(_: &(), _: &Bump, _: &Cx) -> Decision<Bump> {
        Ok(vec![Bumped.into()])
    }
    fn reply(_: &()) {}
}

impl Decide<Ping> for Counter {
    fn decide(_: &(), _: &Ping, _: &Cx) -> Decision<Ping> {
        Ok(vec![Pinged.into()])
    }
    fn reply(_: &()) {}
}

#[derive(Model)]
#[model(aggregates(Counter))]
pub struct Counting;

/// Pings, then fails while `failures` is above zero.
#[derive(Policy)]
#[policy(from(Counter(Bumped)))]
pub struct Echo {
    failures: AtomicU32,
}

impl React for Echo {
    async fn react(&self, cx: &PolicyCx<Self>, _: EchoInput) -> Result<(), PolicyError> {
        let id = cx.source::<Counter>().expect("from counters");
        cx.send(&CounterId::new(format!("{id}-echo")), Ping)?;
        let left = self.failures.load(Ordering::SeqCst);
        if left > 0 {
            self.failures.store(left - 1, Ordering::SeqCst);
            return Err(PolicyError::new("not yet"));
        }
        Ok(())
    }
}

// A lookup service: pings only when it says so.

#[derive(Call, Debug, PartialEq)]
#[call(system = Lookup, returns = bool)]
pub struct ShouldPing {
    counter: CounterId,
}

#[derive(ExternalSystem)]
#[external_system(calls(ShouldPing))]
pub struct Lookup;

pub struct Always;

impl Answer<ShouldPing> for Always {
    async fn answer(&self, _: ShouldPing) -> Answered<ShouldPing> {
        Ok(true)
    }
}

#[derive(Policy)]
#[policy(from(Counter(Bumped)))]
pub struct AskFirst;

impl React for AskFirst {
    async fn react(&self, cx: &PolicyCx<Self>, _: AskFirstInput) -> Result<(), PolicyError> {
        let id = cx.source::<Counter>().expect("from counters").clone();
        if cx.call(ShouldPing { counter: id.clone() }).await? {
            cx.send(&CounterId::new(format!("{id}-echo")), Ping)?;
        }
        Ok(())
    }
}

fn clocked() -> (Memory, Arc<AtomicU64>) {
    let clock = Arc::new(AtomicU64::new(1_000));
    let now = clock.clone();
    (Memory::with_clock(move || Timestamp::from_millis(now.load(Ordering::SeqCst))), clock)
}

fn c(id: &str) -> CounterId {
    CounterId::new(id)
}

fn version(memory: &Memory, stream: &str) -> u64 {
    memory.versions().into_iter().find(|(s, _)| s.as_str() == stream).map_or(0, |(_, v)| v)
}

#[test]
fn a_failed_policy_runs_again_later_and_its_commands_take_effect_once() {
    let (memory, clock) = clocked();
    let memory = Arc::new(memory.with::<Counting>().policy(Echo { failures: AtomicU32::new(2) }));
    memory.send(&c("a"), Bump).unwrap();

    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(2_000)), "retries after a second");
    assert_eq!(memory.policy_failures(), ["Echo failed on Counter/a #1: not yet"]);
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(2_000)), "not before its time");

    clock.store(2_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(4_000)), "then after two");
    clock.store(4_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), None);
    assert!(memory.policy_failures().is_empty());
    assert_eq!(version(&memory, "Counter/a-echo"), 1, "three runs, one ping");
}

#[test]
fn a_policy_acts_on_events_after_it_was_added_only() {
    let memory = Memory::new().with::<Counting>();
    memory.send(&c("a"), Bump).unwrap();
    let memory = Arc::new(memory.policy(Echo { failures: AtomicU32::new(0) }));
    memory.send(&c("b"), Bump).unwrap();
    memory.run_due();
    assert_eq!((version(&memory, "Counter/a-echo"), version(&memory, "Counter/b-echo")), (0, 1));
}

#[test]
fn an_adapter_answers_calls_and_without_one_the_policy_fails() {
    let memory = Arc::new(Memory::new().with::<Counting>().policy(AskFirst).external(Lookup, Always));
    memory.send(&c("a"), Bump).unwrap();
    memory.run_due();
    assert_eq!(version(&memory, "Counter/a-echo"), 1);

    let memory = Arc::new(Memory::new().with::<Counting>().policy(AskFirst));
    memory.send(&c("a"), Bump).unwrap();
    memory.run_due();
    let failures = memory.policy_failures();
    assert!(failures[0].contains("nothing answers Lookup.ShouldPing"), "{failures:?}");
}

#[test]
fn a_scheduled_command_runs_once_at_its_time_unless_cancelled() {
    let (memory, clock) = clocked();
    let memory = Arc::new(memory.with::<Counting>());
    let at = |ms| Timestamp::from_millis(ms);

    memory.schedule(&c("a"), Ping, at(5_000), "ping-1").unwrap();
    memory.schedule(&c("a"), Ping, at(3_000), "ping-1").unwrap(); // replaces
    memory.schedule(&c("b"), Ping, at(3_000), "ping-2").unwrap();
    assert_eq!(memory.run_due(), Some(at(3_000)));
    assert_eq!(memory.unschedule(&c("b"), "ping-2"), Ok(()));
    assert_eq!(memory.unschedule(&c("b"), "ping-2"), Err(UnscheduleError::AlreadyCancelled));
    assert_eq!(memory.unschedule(&c("b"), "ping-3"), Err(UnscheduleError::NotScheduled));

    clock.store(3_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), None);
    clock.store(9_000, Ordering::SeqCst);
    memory.run_due();
    assert_eq!(memory.versions(), [(StreamId::new("Counter", "a"), 1)], "a fired once; b was cancelled");
    assert_eq!(memory.events(&c("a"))[0].command_id, Some(CommandId::new("ping-1")));

    let executed =
        ScheduleError::AlreadyExecuted { stream: StreamId::new("Counter", "a"), command_id: "ping-1".into() };
    assert_eq!(memory.schedule(&c("a"), Ping, at(10_000), "ping-1"), Err(executed));
    assert_eq!(memory.unschedule(&c("a"), "ping-1"), Err(UnscheduleError::AlreadyExecuted));
}

#[test]
fn an_id_already_used_by_a_command_cant_be_scheduled() {
    let memory = Memory::new().with::<Counting>();
    memory.send_with_id(&c("a"), Ping, "done").unwrap();
    assert!(matches!(
        memory.schedule(&c("a"), Ping, Timestamp::from_millis(0), "done"),
        Err(ScheduleError::AlreadyExecuted { .. })
    ));
    assert_eq!(memory.unschedule(&c("a"), "done"), Err(UnscheduleError::AlreadyExecuted));
}
