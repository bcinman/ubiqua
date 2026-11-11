use domain::{Aggregate, Cx, Decide, Decision, Evolve, Model, Saga};
use domain_memory::{Memory, Run, SagaCx, SagaStatus, SignalError};
use serde::{Deserialize, Serialize};
use std::rc::Rc;

// A log records notes. `Refuse` is always rejected.

domain::id!(
    pub struct LogId;
);

#[domain::command(target = LogId, events(Noted))]
pub struct Note(String);

#[domain::command(target = LogId, events(Noted), rejects(No))]
pub struct Refuse;

#[domain::event]
pub struct Noted(String);

#[domain::rejection]
pub struct No;

#[derive(Aggregate)]
#[aggregate(id = LogId, state = (), events(Noted), commands(Note, Refuse))]
pub struct Log;

impl Evolve for Log {
    fn evolve(_: &mut (), _: &LogEvent) {}
}

impl Decide<Note> for Log {
    fn decide(_: &(), cmd: &Note, _: &Cx) -> Decision<Note> {
        Ok(vec![Noted(cmd.0.clone()).into()])
    }
}

impl Decide<Refuse> for Log {
    fn decide(_: &(), _: &Refuse, _: &Cx) -> Decision<Refuse> {
        Err(No.into())
    }
}

#[derive(Model)]
#[model(aggregates(Log))]
pub struct Logging;

/// Notes `one` and `two`, each undone by a note, then waits for `go` and, if told to, ends
/// with a refused step.
#[derive(Saga, Debug, Serialize, Deserialize)]
#[saga(key = LogId, rejects(No), sends(Refuse))]
pub struct Steps {
    pub tag: String,
}

impl Run for Steps {
    async fn run(self, cx: &SagaCx<Self>) -> Result<(), StepsRejection> {
        let log = cx.key();
        let note = |what: &str| Note(format!("{}{what}", self.tag));
        let _ = cx.send("one", log, note("one")).compensate("undo-one", log, note("undo one")).await;
        let _ = cx.send("two", log, note("two")).compensate("undo-two", log, note("undo two")).await;
        let refuse: bool = cx.signal("go").await;
        if refuse {
            cx.send("refuse", log, Refuse).await?;
        }
        Ok(())
    }
}

/// Uses a step name twice.
#[derive(Saga, Debug, Serialize, Deserialize)]
#[saga(key = LogId)]
pub struct Twice;

impl Run for Twice {
    async fn run(self, cx: &SagaCx<Self>) -> Result<(), TwiceRejection> {
        let _ = cx.send("note", cx.key(), Note("a".into())).await;
        let _ = cx.send("note", cx.key(), Note("b".into())).await;
        Ok(())
    }
}

fn l(id: &str) -> LogId {
    LogId::new(id)
}

fn notes(memory: &Memory, id: &str) -> Vec<String> {
    memory.events(&l(id)).into_iter().map(|e| e.event).map(|LogEvent::Noted(Noted(n))| n).collect()
}

fn hosted() -> Rc<Memory> {
    Rc::new(Memory::new().with::<Logging>().saga::<Steps>().saga::<Twice>())
}

#[test]
fn a_run_waits_for_its_signal_and_completes() {
    let memory = hosted();
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::NotStarted);
    assert_eq!(memory.signal::<Steps>(&l("a"), "go", false), Err(SignalError::NotStarted));

    memory.start_saga(&l("a"), Steps { tag: String::new() }).unwrap();
    memory.run_due();
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::Running);
    assert_eq!(notes(&memory, "a"), ["one", "two"]);

    memory.signal::<Steps>(&l("a"), "go", false).unwrap();
    memory.run_due();
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::Completed);
    assert_eq!(notes(&memory, "a"), ["one", "two"]);
}

#[test]
fn a_signal_sent_before_the_wait_is_kept_and_the_first_value_wins() {
    let memory = hosted();
    memory.start_saga(&l("a"), Steps { tag: String::new() }).unwrap();
    memory.signal::<Steps>(&l("a"), "go", true).unwrap();
    memory.signal::<Steps>(&l("a"), "go", false).unwrap();
    memory.run_due();
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::Rejected(No.into()));
}

#[test]
fn a_refused_run_undoes_its_steps_last_first() {
    let memory = hosted();
    memory.start_saga(&l("a"), Steps { tag: String::new() }).unwrap();
    memory.run_due();
    memory.signal::<Steps>(&l("a"), "go", true).unwrap();
    memory.run_due();
    assert_eq!(notes(&memory, "a"), ["one", "two", "undo two", "undo one"]);
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::Rejected(No.into()));
    assert!(memory.failures().is_empty());
}

#[test]
fn starting_twice_starts_one_run() {
    let memory = hosted();
    memory.start_saga(&l("a"), Steps { tag: "first ".into() }).unwrap();
    memory.run_due();
    memory.start_saga(&l("a"), Steps { tag: "second ".into() }).unwrap();
    memory.signal::<Steps>(&l("a"), "go", false).unwrap();
    memory.run_due();
    memory.start_saga(&l("a"), Steps { tag: "third ".into() }).unwrap();
    memory.run_due();
    assert_eq!(memory.saga_status::<Steps>(&l("a")), SagaStatus::Completed, "a finished run stays finished");
    assert_eq!(notes(&memory, "a"), ["first one", "first two"]);
}

#[test]
fn a_step_name_used_twice_kills_the_run() {
    let memory = hosted();
    memory.start_saga(&l("a"), Twice).unwrap();
    memory.run_due();
    assert!(matches!(memory.saga_status::<Twice>(&l("a")), SagaStatus::Died(why) if why.contains("ran twice")));
    assert_eq!(notes(&memory, "a"), ["a"]);
    assert_eq!(memory.failures().len(), 1);
}
