use domain::{
    Aggregate, Apply, Command, Cursor, Cx, Decide, Decision, Event, Evolve, Frame, KeyOf, Model, Origin, Timestamp,
    View, ViewCx,
};
use domain_memory::Memory;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

// A wallet records credits and notes; views read credits only.

domain::id!(
    struct WalletId;
);
domain::id!(
    struct ClubId;
);
domain::id!(
    struct OwnerId;
);

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = WalletId, events(Credited))]
struct Credit {
    owner: String,
    amount: u64,
}

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = WalletId, events(Noted))]
struct Note;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Credited {
    owner: String,
    amount: u64,
    total_after: u64,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Noted;

#[derive(Aggregate)]
#[aggregate(id = WalletId, state = u64, events(Credited, Noted), commands(Credit, Note))]
struct Wallet;

impl Evolve for Wallet {
    fn evolve(total: &mut u64, event: &WalletEvent) {
        if let WalletEvent::Credited(e) = event {
            *total = e.total_after;
        }
    }
}

impl Decide<Credit> for Wallet {
    fn decide(total: &u64, cmd: &Credit, _: &Cx) -> Decision<Credit> {
        Ok(vec![Credited { owner: cmd.owner.clone(), amount: cmd.amount, total_after: total + cmd.amount }.into()])
    }
    fn reply(_: &u64) {}
}

impl Decide<Note> for Wallet {
    fn decide(_: &u64, _: &Note, _: &Cx) -> Decision<Note> {
        Ok(vec![Noted.into()])
    }
    fn reply(_: &u64) {}
}

#[derive(Command, Debug, Serialize, Deserialize)]
#[command(target = ClubId, events(Joined))]
struct Join {
    member: String,
}

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Joined {
    member: String,
}

#[derive(Aggregate)]
#[aggregate(id = ClubId, state = (), events(Joined), commands(Join))]
struct Club;

impl Evolve for Club {
    fn evolve(_: &mut (), _: &ClubEvent) {}
}

impl Decide<Join> for Club {
    fn decide(_: &(), cmd: &Join, _: &Cx) -> Decision<Join> {
        Ok(vec![Joined { member: cmd.member.clone() }.into()])
    }
    fn reply(_: &()) {}
}

/// Each wallet's total. Emits every credit it applies.
#[derive(View)]
#[view(key = WalletId, from(Wallet(Credited)), state = u64)]
struct Totals;

impl Apply for Totals {
    fn apply(total: &mut u64, input: TotalsInput, _: &mut ViewCx<Self>) {
        let TotalsInput::Credited(e) = input;
        *total = e.total_after;
    }
}

/// Per owner, across wallets and clubs: what they were credited, and in what order things happened.
#[derive(View)]
#[view(key = OwnerId, from(Wallet(Credited), Club(Joined)), key_of(Wallet, Club), state = Activity)]
struct PerOwner;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Activity {
    credited: u64,
    log: Vec<String>,
}

impl KeyOf<Wallet> for PerOwner {
    fn key_of(_: Origin<'_, Wallet>, input: &PerOwnerInput) -> Option<OwnerId> {
        match input {
            PerOwnerInput::Credited(e) => Some(OwnerId::new(e.owner.clone())),
            PerOwnerInput::Joined(_) => None,
        }
    }
}

impl KeyOf<Club> for PerOwner {
    fn key_of(_: Origin<'_, Club>, input: &PerOwnerInput) -> Option<OwnerId> {
        match input {
            PerOwnerInput::Joined(e) => Some(OwnerId::new(e.member.clone())),
            PerOwnerInput::Credited(_) => None,
        }
    }
}

impl Apply for PerOwner {
    fn apply(activity: &mut Activity, input: PerOwnerInput, cx: &mut ViewCx<Self>) {
        match input {
            PerOwnerInput::Credited(e) => {
                activity.credited += e.amount;
                activity.log.push(format!("{} +{}", cx.source::<Wallet>().unwrap(), e.amount));
            }
            PerOwnerInput::Joined(_) => activity.log.push(format!("joined {}", cx.source::<Club>().unwrap())),
        }
    }
}

#[derive(Model)]
#[model(aggregates(Wallet, Club), views(Totals, PerOwner))]
struct Bank;

fn w(id: &str) -> WalletId {
    WalletId::new(id)
}

fn credit(amount: u64) -> Credit {
    Credit { owner: "sam".into(), amount }
}

fn totals(entries: &[domain::Entry<Totals>]) -> Vec<(u64, u64)> {
    entries.iter().map(|e| (e.cursor.seq(), *e.state)).collect()
}

/// A clock that ticks once per reading, so every commit has its own time.
fn ticking() -> impl Fn() -> Timestamp {
    let now = AtomicU64::new(0);
    move || Timestamp::from_millis(now.fetch_add(1, Ordering::Relaxed) + 1)
}

#[test]
fn a_view_folds_its_events_and_skips_the_rest() {
    let memory = Memory::new().with::<Bank>();
    memory.send(&w("a"), credit(5)).unwrap();
    memory.send(&w("a"), Note).unwrap();
    memory.send(&w("a"), credit(7)).unwrap();
    memory.send(&w("b"), credit(1)).unwrap();

    let view = memory.view::<Totals>();
    assert_eq!(view.get(&w("a")), Some(12));
    assert_eq!(view.get(&w("c")), None);
    assert_eq!(totals(&view.list(&w("a"), Cursor::START, 10)), [(1, 5), (2, 12)]);
    assert_eq!(totals(&view.list(&w("a"), Cursor::new(1), 10)), [(2, 12)]);
}

#[test]
fn a_feed_sends_history_after_the_cursor_then_caught_up_then_live_entries() {
    let memory = Memory::new().with::<Bank>();
    memory.send(&w("a"), credit(1)).unwrap();
    let snapshot = memory.view::<Totals>().snapshot(&w("a"));
    assert_eq!((snapshot.state, snapshot.cursor), (Some(1), Cursor::new(1)));

    // Written between the snapshot and the feed: must arrive once, not be missed.
    memory.send(&w("a"), credit(2)).unwrap();
    memory.send(&w("a"), credit(3)).unwrap();
    let feed = memory.view::<Totals>().events(&w("a"), snapshot.cursor);
    memory.send(&w("a"), credit(4)).unwrap();
    memory.send(&w("b"), credit(100)).unwrap();

    let frames: Vec<String> = feed
        .drain()
        .into_iter()
        .map(|frame| match frame {
            Frame::Entry(e) => format!("{}:{}", e.cursor.seq(), e.state),
            Frame::CaughtUp { cursor } => format!("caught up {}", cursor.seq()),
        })
        .collect();
    assert_eq!(frames, ["2:3", "3:6", "caught up 3", "4:10"]);
}

#[test]
fn a_watch_sends_the_state_now_and_on_each_change_only() {
    let memory = Memory::new().with::<Bank>();
    let watch = memory.view::<Totals>().watch(&w("a"));
    memory.send(&w("a"), credit(5)).unwrap();
    memory.send(&w("a"), Note).unwrap(); // not read
    memory.send(&w("a"), credit(0)).unwrap(); // read, state unchanged
    memory.send(&w("a"), credit(2)).unwrap();

    let states: Vec<_> = watch.drain().into_iter().map(|s| (s.state, s.cursor.seq())).collect();
    assert_eq!(states, [(None, 0), (Some(5), 1), (Some(7), 3)]);
}

#[test]
fn notify_fires_for_items_already_waiting_then_on_each_arrival() {
    let memory = Memory::new().with::<Bank>();
    let watch = memory.view::<Totals>().watch(&w("a"));
    memory.send(&w("a"), credit(5)).unwrap(); // arrives before anyone listens

    let calls = Arc::new(AtomicU64::new(0));
    let counter = calls.clone();
    watch.notify(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    memory.send(&w("a"), credit(2)).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let states: Vec<_> = watch.drain().into_iter().map(|s| s.state).collect();
    assert_eq!(states, [None, Some(5), Some(7)]);
}

#[test]
fn at_least_counts_events_the_view_skips_and_refuses_other_streams() {
    let memory = Memory::new().with::<Bank>();
    memory.send(&w("a"), credit(5)).unwrap();
    let noted = memory.send(&w("a"), Note).unwrap();
    let joined = memory.send(&ClubId::new("chess"), Join { member: "sam".into() }).unwrap();

    let view = memory.view::<Totals>();
    assert_eq!(view.at_least(&noted).unwrap().get(&w("a")), Some(5));
    let Err(error) = view.at_least(&joined) else { panic!("Club/chess isn't a source of Totals") };
    assert_eq!((error.stream.as_str(), error.wanted, error.applied), ("Club/chess", 1, 0));
}

#[test]
fn key_of_routes_several_sources_to_one_key() {
    let memory = Memory::with_clock(ticking()).with::<Bank>();
    memory.send(&w("a"), credit(5)).unwrap();
    memory.send(&ClubId::new("chess"), Join { member: "sam".into() }).unwrap();
    memory.send(&w("b"), credit(2)).unwrap();
    memory.send(&w("c"), Credit { owner: "kim".into(), amount: 9 }).unwrap();

    let sam = memory.view::<PerOwner>().get(&OwnerId::new("sam")).unwrap();
    assert_eq!(sam, Activity { credited: 7, log: vec!["a +5".into(), "joined chess".into(), "b +2".into()] });
    assert_eq!(memory.view::<PerOwner>().get(&OwnerId::new("kim")).unwrap().credited, 9);
}

#[test]
fn a_view_added_later_receives_the_history_in_the_order_it_was_recorded() {
    let memory = Memory::with_clock(ticking());
    memory.send(&w("b"), credit(1)).unwrap();
    memory.send(&ClubId::new("chess"), Join { member: "sam".into() }).unwrap();
    memory.send(&w("a"), credit(2)).unwrap();
    memory.send(&w("b"), credit(3)).unwrap();

    let memory = memory.with::<Bank>();
    let sam = memory.view::<PerOwner>().get(&OwnerId::new("sam")).unwrap();
    assert_eq!(sam.log, ["b +1", "joined chess", "a +2", "b +3"]);
    assert_eq!(memory.view::<Totals>().get(&w("b")), Some(4));

    // And then live, without applying the history twice.
    memory.send(&w("b"), credit(10)).unwrap();
    assert_eq!(totals(&memory.view::<Totals>().list(&w("b"), Cursor::START, 10)), [(1, 1), (2, 4), (3, 14)]);
}

#[test]
fn views_apply_each_source_in_order_under_concurrent_commands() {
    let memory = Arc::new(Memory::new().with::<Bank>());
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let memory = memory.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    memory.send(&w(&format!("w{}", t % 4)), credit(1)).unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());

    for wallet in 0..4 {
        let entries = memory.view::<Totals>().list(&w(&format!("w{wallet}")), Cursor::START, usize::MAX);
        let expected: Vec<(u64, u64)> = (1..=200).map(|n| (n, n)).collect();
        assert_eq!(totals(&entries), expected, "w{wallet}");
    }
    assert_eq!(memory.view::<PerOwner>().get(&OwnerId::new("sam")).unwrap().credited, 800);
}

#[test]
fn a_waiting_reader_wakes_when_an_entry_arrives() {
    let memory = Arc::new(Memory::new().with::<Bank>());
    let mut feed = memory.view::<Totals>().events(&w("a"), Cursor::START);
    assert!(block_on(feed.recv()).is_caught_up());

    let writer = memory.clone();
    let thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        writer.send(&w("a"), credit(3)).unwrap();
    });
    let frame = block_on(feed.recv());
    assert_eq!(*frame.entry().unwrap().state, 3);
    thread.join().unwrap();
}

// A view that panics the first time it sees an amount of 13.

static FLAKY_PANICKED: AtomicBool = AtomicBool::new(false);
static FLAKY_APPLIED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

#[derive(View)]
#[view(key = WalletId, from(Wallet(Credited)), state = u64)]
struct Flaky;

impl Apply for Flaky {
    fn apply(total: &mut u64, input: FlakyInput, _: &mut ViewCx<Self>) {
        let FlakyInput::Credited(e) = input;
        if e.amount == 13 && !FLAKY_PANICKED.swap(true, Ordering::SeqCst) {
            panic!("flaky view fails once");
        }
        FLAKY_APPLIED.lock().unwrap().push(e.amount);
        *total = e.total_after;
    }
}

#[derive(Model)]
#[model(aggregates(Wallet), views(Flaky))]
struct FlakyBank;

#[test]
fn a_view_that_panics_falls_behind_without_failing_the_command_and_applies_each_event_once() {
    let memory = Memory::new().with::<FlakyBank>();
    memory.send(&w("a"), credit(1)).unwrap();
    let receipt = memory.send(&w("a"), credit(13)).expect("the command commits though its view fails");
    assert_eq!((receipt.version, memory.state(&w("a"))), (2, 14));
    assert_eq!(*FLAKY_APPLIED.lock().unwrap(), [1], "the view is behind");

    // The next read drains the inbox and catches up.
    let view = memory.view::<Flaky>();
    assert_eq!(view.at_least(&receipt).unwrap().get(&w("a")), Some(14));
    memory.send(&w("a"), credit(2)).unwrap();
    assert_eq!(view.get(&w("a")), Some(16));
    assert_eq!(*FLAKY_APPLIED.lock().unwrap(), [1, 13, 2]);
}

/// Polls again only when woken, so a feed that forgets its waker fails instead of being re-polled.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct Flag {
        thread: std::thread::Thread,
        woken: AtomicBool,
    }
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::SeqCst);
            self.thread.unpark();
        }
    }
    let flag = Arc::new(Flag { thread: std::thread::current(), woken: AtomicBool::new(false) });
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        while !flag.woken.swap(false, Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline, "nothing woke the reader");
            std::thread::park_timeout(Duration::from_millis(50));
        }
    }
}
