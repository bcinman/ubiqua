use domain::{Answer, Answered, CommandId, Cx, Decide, Decision, Evolve, StreamId, Timestamp};
use domain_memory::{Memory, PolicyCx, PolicyError, React, ScheduleError, UnscheduleError};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

// A counter is bumped; `Echo` pings `<id>-echo` for each bump.

domain::id!(
    pub struct CounterId;
);

#[domain::command(target = CounterId, events(Bumped))]
pub struct Bump;

#[domain::command(target = CounterId, events(Pinged))]
pub struct Ping;

#[domain::event]
pub struct Bumped;

#[domain::event]
pub struct Pinged;

#[domain::aggregate(id = CounterId, state = (), events(Bumped, Pinged), commands(Bump, Ping))]
pub struct Counter;

impl Evolve for Counter {
    fn evolve(_: &mut (), _: &CounterEvent) {}
}

impl Decide<Bump> for Counter {
    fn decide(_: &(), _: &Bump, _: &Cx) -> Decision<Bump> {
        Ok(vec![Bumped.into()])
    }
}

impl Decide<Ping> for Counter {
    fn decide(_: &(), _: &Ping, _: &Cx) -> Decision<Ping> {
        Ok(vec![Pinged.into()])
    }
}

#[domain::model(aggregates(Counter))]
pub struct Counting;

#[domain::model(aggregates(Counter), policies(Echo))]
pub struct Echoing;

/// Pings, then fails while `failures` is above zero.
#[domain::policy(from(Counter(Bumped)))]
#[derive(Default)]
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

#[domain::call(system = Lookup, returns = bool)]
pub struct ShouldPing {
    counter: CounterId,
}

#[domain::external_system(calls(ShouldPing))]
pub struct Lookup;

pub struct Always;

impl Answer<ShouldPing> for Always {
    async fn answer(&self, _: ShouldPing) -> Answered<ShouldPing> {
        Ok(true)
    }
}

#[domain::policy(from(Counter(Bumped)))]
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
    let memory = Rc::new(memory.with::<Counting>().policy(Echo { failures: AtomicU32::new(2) }));
    memory.send(&c("a"), Bump).unwrap();

    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(2_000)), "retries after a second");
    assert_eq!(memory.failures(), ["Echo failed on Counter/a #1: not yet"]);
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(2_000)), "not before its time");

    clock.store(2_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(4_000)), "then after two");
    clock.store(4_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), None);
    assert!(memory.failures().is_empty());
    assert_eq!(version(&memory, "Counter/a-echo"), 1, "three runs, one ping");
}

#[test]
fn a_policy_acts_on_events_after_it_was_added_only() {
    let memory = Memory::new().with::<Counting>();
    memory.send(&c("a"), Bump).unwrap();
    let memory = Rc::new(memory.policy(Echo { failures: AtomicU32::new(0) }));
    memory.send(&c("b"), Bump).unwrap();
    memory.run_due();
    assert_eq!((version(&memory, "Counter/a-echo"), version(&memory, "Counter/b-echo")), (0, 1));
}

#[test]
fn an_adapter_answers_calls_and_without_one_the_policy_fails() {
    let memory = Rc::new(Memory::new().with::<Counting>().policy(AskFirst).external(Lookup, Always));
    memory.send(&c("a"), Bump).unwrap();
    memory.run_due();
    assert_eq!(version(&memory, "Counter/a-echo"), 1);

    let memory = Rc::new(Memory::new().with::<Counting>().policy(AskFirst));
    memory.send(&c("a"), Bump).unwrap();
    memory.run_due();
    let failures = memory.failures();
    assert!(failures[0].contains("nothing answers Lookup.ShouldPing"), "{failures:?}");
}

#[test]
fn a_scheduled_command_runs_once_at_its_time_unless_cancelled() {
    let (memory, clock) = clocked();
    let memory = Rc::new(memory.with::<Counting>());
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

// Who signs a sheet: an app signs as its user, and `Countersign` signs a copy as the clerk.

#[derive(Debug)]
struct Name(&'static str);

domain::id!(
    pub struct SheetId;
);

#[domain::command(target = SheetId, events(Signed))]
pub struct Sign;

#[domain::event]
pub struct Signed {
    by: Option<String>,
}

#[domain::aggregate(id = SheetId, state = (), events(Signed), commands(Sign))]
pub struct Sheet;

impl Evolve for Sheet {
    fn evolve(_: &mut (), _: &SheetEvent) {}
}

impl Decide<Sign> for Sheet {
    fn decide(_: &(), _: &Sign, cx: &Cx) -> Decision<Sign> {
        Ok(vec![Signed { by: cx.actor::<Name>().map(|Name(name)| name.to_string()) }.into()])
    }
}

#[domain::policy(from(Sheet(Signed)))]
pub struct Countersign;

impl React for Countersign {
    async fn react(&self, cx: &PolicyCx<Self>, input: CountersignInput) -> Result<(), PolicyError> {
        let CountersignInput::Signed(signed) = input;
        if signed.by.as_deref() != Some("clerk") {
            let sheet = cx.source::<Sheet>().expect("from sheets");
            cx.acting_as(Name("clerk")).send(&SheetId::new(format!("{sheet}-copy")), Sign)?;
        }
        Ok(())
    }
}

#[test]
fn a_command_carries_who_sent_it() {
    let memory = Rc::new(Memory::new().policy(Countersign));
    memory.acting_as(Name("sam")).send(&SheetId::new("s-1"), Sign).unwrap();
    memory.send(&SheetId::new("s-2"), Sign).unwrap();
    memory.run_due();

    let signers = |id: &str| -> Vec<Option<String>> {
        memory
            .events(&SheetId::new(id))
            .into_iter()
            .map(|e| match e.event {
                SheetEvent::Signed(s) => s.by,
            })
            .collect()
    };
    assert_eq!(signers("s-1"), [Some("sam".to_string())]);
    assert_eq!(signers("s-2"), [None]);
    assert_eq!(signers("s-1-copy"), [Some("clerk".to_string())]);
    assert_eq!(signers("s-1-copy-copy"), Vec::<Option<String>>::new());
}

#[test]
fn a_model_hosts_the_policies_it_lists() {
    let memory = Rc::new(Memory::new().with::<Echoing>());
    memory.send(&CounterId::new("c"), Bump).unwrap();
    memory.run_due();
    assert_eq!(memory.events(&CounterId::new("c-echo")).len(), 1);
}

#[test]
#[should_panic(expected = "Echo is hosted twice")]
fn a_listed_policy_cant_be_added_again() {
    let _ = Memory::new().with::<Echoing>().policy(Echo::default());
}
