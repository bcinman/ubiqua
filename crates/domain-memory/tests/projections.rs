use domain::{Aggregate, Cx, Decide, Decision, Evolve, Model, Position, Projection, Timestamp};
use domain_memory::{Memory, Project, ProjectionError, ProjectionInput};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

domain::id!(
    pub struct CounterId;
);

#[domain::command(target = CounterId, events(Bumped))]
pub struct Bump;

#[domain::event]
pub struct Bumped;

#[derive(Aggregate)]
#[aggregate(id = CounterId, state = (), events(Bumped), commands(Bump))]
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

#[derive(Model)]
#[model(aggregates(Counter))]
pub struct Counting;

/// Writes `(key, seq)` per input and each batch's size. Fails on key `bad` while `failing`.
#[derive(Projection, Default)]
#[projection(key = CounterId, from(Counter(Bumped)))]
pub struct Tally {
    rows: Arc<Mutex<Vec<(String, u64)>>>,
    batches: Arc<Mutex<Vec<usize>>>,
    failing: Arc<AtomicBool>,
}

impl Project for Tally {
    const BATCH: usize = 2;

    async fn project(&self, key: &CounterId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> {
        if key.as_str() == "bad" && self.failing.load(Ordering::SeqCst) {
            return Err(ProjectionError::new("down"));
        }
        self.batches.lock().unwrap().push(batch.len());
        self.rows.lock().unwrap().extend(batch.iter().map(|input| (key.to_string(), input.seq)));
        Ok(())
    }
}

fn c(id: &str) -> CounterId {
    CounterId::new(id)
}

fn rows(tally: &Tally) -> Vec<(String, u64)> {
    tally.rows.lock().unwrap().clone()
}

#[test]
fn a_new_projection_gets_all_of_history_in_order_and_in_batches() {
    let memory = Memory::new().with::<Counting>();
    for _ in 0..5 {
        memory.send(&c("a"), Bump).unwrap();
    }
    let tally = Tally::default();
    let (written, batches) = (tally.rows.clone(), tally.batches.clone());
    let memory = Arc::new(memory.projection(tally));
    memory.run_due();
    let seqs: Vec<u64> = written.lock().unwrap().iter().map(|(_, seq)| *seq).collect();
    assert_eq!(seqs, [1, 2, 3, 4, 5]);
    assert_eq!(*batches.lock().unwrap(), [2, 2, 1]);
}

#[test]
fn a_failing_key_runs_again_later_while_other_keys_carry_on() {
    let clock = Arc::new(AtomicU64::new(1_000));
    let now = clock.clone();
    let tally = Tally::default();
    let (failing, view) = (tally.failing.clone(), Tally { rows: tally.rows.clone(), ..Tally::default() });
    failing.store(true, Ordering::SeqCst);
    let memory = Memory::with_clock(move || Timestamp::from_millis(now.load(Ordering::SeqCst)));
    let memory = Arc::new(memory.with::<Counting>().projection(tally));

    memory.send(&c("bad"), Bump).unwrap();
    memory.send(&c("good"), Bump).unwrap();
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(2_000)), "retries after a second");
    assert_eq!(rows(&view), [("good".to_string(), 1)]);
    assert_eq!(memory.failures().len(), 1);

    memory.send(&c("good"), Bump).unwrap();
    memory.run_due();
    assert_eq!(rows(&view).len(), 2, "good isn't held up");

    failing.store(false, Ordering::SeqCst);
    clock.store(2_000, Ordering::SeqCst);
    assert_eq!(memory.run_due(), None);
    assert!(memory.failures().is_empty());
    assert!(rows(&view).contains(&("bad".to_string(), 1)));
    assert_eq!(rows(&view).len(), 3, "each input written once");
}

#[test]
fn caught_up_tells_when_a_command_is_written() {
    let memory = Arc::new(Memory::new().with::<Counting>().projection(Tally::default()));
    let receipt = memory.send(&c("a"), Bump).unwrap();
    assert!(memory.caught_up::<Tally>(&receipt).is_err());
    memory.run_due();
    assert_eq!(memory.caught_up::<Tally>(&receipt), Ok(()));
    let ahead = Position { stream: receipt.stream.clone(), version: 2 };
    assert!(memory.caught_up::<Tally>(ahead).is_err());
}
