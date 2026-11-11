use domain::{Aggregate, Command, Cx, Decide, Decision, Event, Evolve, Rejection, Timestamp};
use domain_story::Story;
use serde::{Deserialize, Serialize};

domain::id!(
    struct LampId;
);

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = LampId, events(Switched), rejects(Broken), reply = bool)]
struct Toggle;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Switched {
    on: bool,
    at: Timestamp,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Smashed;

#[derive(Rejection, Clone, Debug, PartialEq)]
struct Broken;

#[derive(Clone, Debug, Default)]
struct Lamp {
    on: bool,
    broken: bool,
}

#[derive(Aggregate)]
#[aggregate(id = LampId, state = Lamp, events(Switched, Smashed), commands(Toggle))]
struct Light;

impl Evolve for Light {
    fn evolve(state: &mut Lamp, event: &LightEvent) {
        match event {
            LightEvent::Switched(e) => state.on = e.on,
            LightEvent::Smashed(_) => state.broken = true,
        }
    }
}

impl Decide<Toggle> for Light {
    fn decide(state: &Lamp, _: &Toggle, cx: &Cx) -> Decision<Toggle> {
        if state.broken {
            return Err(Broken.into());
        }
        Ok(vec![Switched { on: !state.on, at: cx.now() }.into()])
    }

    fn reply(state: &Lamp) -> bool {
        state.on
    }
}

fn lamp() -> LampId {
    LampId::new("hall")
}

fn on_at(millis: u64) -> Switched {
    Switched { on: true, at: Timestamp::from_millis(millis) }
}

#[test]
fn checks_events_rejections_replies_and_the_clock() {
    Story::new()
        .at(Timestamp::from_millis(5))
        .when(&lamp(), Toggle)
        .then(&lamp(), [on_at(5)])
        .replied(true)
        .when(&lamp(), Toggle)
        .then(&lamp(), [Switched { on: false, at: Timestamp::from_millis(5) }])
        .given(&lamp(), [Smashed])
        .when(&lamp(), Toggle)
        .rejected(Broken)
        .then_none(&lamp());
}

#[test]
#[should_panic(expected = "1 event(s) on Light/hall")]
fn fails_when_events_go_unchecked() {
    Story::new().when(&lamp(), Toggle).replied(true);
}

#[test]
#[should_panic(expected = "rejection Broken")]
fn fails_when_a_rejection_goes_unchecked() {
    Story::new().given(&lamp(), [Smashed]).when(&lamp(), Toggle);
}

#[test]
#[should_panic(expected = "1 event(s) on Light/hall")]
fn checks_each_command_before_the_next() {
    Story::new()
        .when(&lamp(), Toggle)
        .when(&lamp(), Toggle)
        .then(&lamp(), [on_at(0), Switched { on: false, at: Timestamp::from_millis(0) }]);
}

#[test]
#[should_panic(expected = "events recorded on Light/hall")]
fn fails_when_events_differ() {
    Story::new().when(&lamp(), Toggle).then(&lamp(), [on_at(1)]);
}

#[test]
fn given_events_need_no_check() {
    Story::new()
        .given(&lamp(), [on_at(0)])
        .when(&lamp(), Toggle)
        .then(&lamp(), [Switched { on: false, at: Timestamp::from_millis(0) }]);
}
