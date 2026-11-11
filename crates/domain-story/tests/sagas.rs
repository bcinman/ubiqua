use domain::{Aggregate, Cx, Decide, Decision, Evolve, Policy, Saga};
use domain_memory::{PolicyCx, PolicyError, React, Run, SagaCx, SagaStatus};
use domain_story::Story;
use serde::{Deserialize, Serialize};

// Ringing a door starts a visit: the door opens, and closes once the visitor has left.

domain::id!(
    pub struct DoorId;
);

#[domain::command(target = DoorId, events(Rang))]
pub struct Ring;

#[domain::command(target = DoorId, events(Opened))]
pub struct Open;

#[domain::command(target = DoorId, events(Closed))]
pub struct Close;

#[domain::event]
pub struct Rang;

#[domain::event]
pub struct Opened;

#[domain::event]
pub struct Closed;

#[derive(Aggregate)]
#[aggregate(id = DoorId, state = (), events(Rang, Opened, Closed), commands(Ring, Open, Close))]
pub struct Door;

impl Evolve for Door {
    fn evolve(_: &mut (), _: &DoorEvent) {}
}

impl Decide<Ring> for Door {
    fn decide(_: &(), _: &Ring, _: &Cx) -> Decision<Ring> {
        Ok(vec![Rang.into()])
    }
}

impl Decide<Open> for Door {
    fn decide(_: &(), _: &Open, _: &Cx) -> Decision<Open> {
        Ok(vec![Opened.into()])
    }
}

impl Decide<Close> for Door {
    fn decide(_: &(), _: &Close, _: &Cx) -> Decision<Close> {
        Ok(vec![Closed.into()])
    }
}

#[derive(Policy)]
#[policy(from(Door(Rang)))]
pub struct Answer;

impl React for Answer {
    async fn react(&self, cx: &PolicyCx<Self>, _: AnswerInput) -> Result<(), PolicyError> {
        let door = cx.source_id().clone();
        cx.start_saga(&door, Visit)?;
        Ok(())
    }
}

/// Opens the door, waits for `left`, then closes it.
#[derive(Saga, Debug, Serialize, Deserialize)]
#[saga(key = DoorId)]
pub struct Visit;

impl Run for Visit {
    async fn run(self, cx: &SagaCx<Self>) -> Result<(), VisitRejection> {
        let _ = cx.send("open", cx.key(), Open).await;
        cx.signal::<()>("left").await;
        let _ = cx.send("close", cx.key(), Close).await;
        Ok(())
    }
}

fn front() -> DoorId {
    DoorId::new("front")
}

#[test]
fn a_signal_runs_what_the_saga_does_next() {
    Story::new()
        .policy(Answer)
        .saga::<Visit>()
        .when(&front(), Ring)
        .then(&front(), (Rang, Opened))
        .saga_status::<Visit>(&front(), SagaStatus::Running)
        .signal::<Visit>(&front(), "left", ())
        .then(&front(), [Closed])
        .saga_status::<Visit>(&front(), SagaStatus::Completed);
}

#[test]
#[should_panic(expected = "story: signal left to Visit front")]
fn a_signal_to_a_run_that_never_started_fails_the_story() {
    Story::new().saga::<Visit>().when(&front(), Open).then(&front(), [Opened]).signal::<Visit>(&front(), "left", ());
}
