use domain::{Aggregate, Command, Cx, Decide, Decision, Event, Evolve, Rejection, SendError};
use domain_memory::Memory;
use std::sync::Arc;

domain::id!(
    struct CounterId;
);

#[derive(Command, Debug)]
#[command(target = CounterId, events(Incremented), reply = u64)]
struct Increment;

#[derive(Command, Debug)]
#[command(target = CounterId, events(Incremented), rejects(TooHigh))]
struct IncrementBelow {
    limit: u64,
}

#[derive(Event, Clone, Debug, PartialEq)]
struct Incremented {
    value_after: u64,
}

#[derive(Rejection, Clone, Debug, PartialEq)]
struct TooHigh;

#[derive(Aggregate)]
#[aggregate(id = CounterId, state = u64, events(Incremented), commands(Increment, IncrementBelow))]
struct Counter;

impl Evolve for Counter {
    fn evolve(state: &mut u64, event: &CounterEvent) {
        let CounterEvent::Incremented(e) = event;
        // Fails mid-way through a commit, to check that nothing of it is kept.
        assert!(e.value_after != BOOM, "evolve panics on {BOOM}");
        *state = e.value_after;
    }
}

impl Decide<Increment> for Counter {
    fn decide(state: &u64, _: &Increment, _: &Cx) -> Decision<Increment> {
        Ok(vec![Incremented { value_after: state + 1 }.into()])
    }

    fn reply(state: &u64) -> u64 {
        *state
    }
}

impl Decide<IncrementBelow> for Counter {
    fn decide(state: &u64, cmd: &IncrementBelow, _: &Cx) -> Decision<IncrementBelow> {
        if state + 1 >= cmd.limit {
            return Err(TooHigh.into());
        }
        // Two events, so a failure on the second shows whether the first was kept.
        Ok(vec![Incremented { value_after: state + 1 }.into(), Incremented { value_after: state + 2 }.into()])
    }

    fn reply(_: &u64) {}
}

const BOOM: u64 = 1_000_013;

fn counter() -> CounterId {
    CounterId::new("c-1")
}

#[test]
fn a_retried_command_id_is_decided_once_and_returns_the_first_receipt() {
    let memory = Memory::new();
    memory.send(&counter(), Increment).unwrap();
    let first = memory.send_with_id(&counter(), Increment, "req-1").unwrap();
    let retry = memory.send_with_id(&counter(), Increment, "req-1").unwrap();

    assert_eq!(first, retry);
    assert_eq!(first.version, 2);
    assert_eq!(memory.state(&counter()), 2);
    assert_eq!(memory.events(&counter()).len(), 2);
}

#[test]
fn a_command_id_used_for_another_command_is_refused() {
    let memory = Memory::new();
    memory.send_with_id(&counter(), Increment, "req-1").unwrap();
    let reused = memory.send_with_id(&counter(), IncrementBelow { limit: 10 }, "req-1");

    assert!(matches!(reused, Err(SendError::CommandIdReused { .. })), "{reused:?}");
    assert_eq!(memory.state(&counter()), 1);
}

#[test]
fn a_rejection_records_nothing() {
    let memory = Memory::new();
    memory.append(&counter(), [Incremented { value_after: 5 }]);
    let refused = memory.send(&counter(), IncrementBelow { limit: 6 });

    assert_eq!(refused.unwrap_err().rejection(), Some(&IncrementBelowRejection::TooHigh(TooHigh)));
    assert_eq!(memory.state(&counter()), 5);
    assert_eq!(memory.events(&counter()).len(), 1);
}

#[test]
fn a_command_that_panics_part_way_through_commits_nothing() {
    let memory = Memory::new();
    memory.append(&counter(), [Incremented { value_after: BOOM - 2 }]);

    // Applies BOOM - 1, then panics applying BOOM.
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        memory.send(&counter(), IncrementBelow { limit: BOOM + 10 })
    }));

    assert!(panicked.is_err());
    assert_eq!(memory.state(&counter()), BOOM - 2);
    assert_eq!(memory.events(&counter()).len(), 1);
    assert_eq!(memory.send(&counter(), Increment).unwrap().reply, BOOM - 1);
}

#[test]
fn an_instance_decides_one_command_at_a_time() {
    let memory = Arc::new(Memory::new());
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let memory = memory.clone();
            std::thread::spawn(move || {
                for _ in 0..200 {
                    memory.send(&counter(), Increment).unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());

    // Each decision saw the state the previous one left, so the values count up with no repeats.
    let values: Vec<u64> = memory
        .events(&counter())
        .into_iter()
        .map(|e| match e.event {
            CounterEvent::Incremented(e) => e.value_after,
        })
        .collect();
    assert_eq!(values, (1..=1600).collect::<Vec<_>>());
    assert_eq!(memory.state(&counter()), 1600);
}
