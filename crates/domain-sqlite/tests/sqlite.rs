use domain::{
    Aggregate, Apply, Cursor, Cx, Decide, Decision, Evolve, Model, Policy, Reply, SendError, StreamId, Timestamp,
    ViewCx,
};
use domain_memory::{Memory, PolicyCx, PolicyError, React};
use domain_sqlite::Sqlite;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

// A wallet records credits. `Summary` sums them, `Overview` copies the summary's state, and
// `Flaky` fails while `FLAKY_FAILS` is set.

domain::id!(
    pub struct WalletId;
);

#[domain::command(target = WalletId, events(Credited), reply = u64)]
pub struct Credit {
    amount: u64,
}

#[domain::command(target = WalletId, events(Credited))]
pub struct Note;

/// Succeeds only on a wallet with money in it, and records nothing.
#[domain::command(target = WalletId, events(Credited), rejects(Empty))]
pub struct Claim;

#[derive(domain::Rejection, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Empty;

#[domain::event]
pub struct Credited {
    amount: u64,
    total: u64,
}

#[derive(Aggregate)]
#[aggregate(id = WalletId, state = u64, events(Credited), commands(Credit, Note, Claim))]
pub struct Wallet;

impl Evolve for Wallet {
    fn evolve(total: &mut u64, event: &WalletEvent) {
        let WalletEvent::Credited(e) = event;
        *total = e.total;
    }
}

impl Decide<Credit> for Wallet {
    fn decide(total: &u64, cmd: &Credit, _: &Cx) -> Decision<Credit> {
        Ok(vec![Credited { amount: cmd.amount, total: total + cmd.amount }.into()])
    }
}

impl Reply<Wallet> for Credit {
    fn reply(total: &u64) -> u64 {
        *total
    }
}

impl Decide<Claim> for Wallet {
    fn decide(total: &u64, _: &Claim, _: &Cx) -> Decision<Claim> {
        if *total == 0 { Err(Empty.into()) } else { Ok(vec![]) }
    }
}

impl Decide<Note> for Wallet {
    fn decide(_: &u64, _: &Note, _: &Cx) -> Decision<Note> {
        Ok(vec![])
    }
}

#[derive(Model)]
#[model(aggregates(Wallet))]
pub struct WalletsOnly;

/// The first version of the views: the summary sums the amounts.
mod v0 {
    use super::*;

    #[derive(domain::View)]
    #[view(key = WalletId, from(Wallet(Credited)), state = u64)]
    pub struct Summary;

    impl Apply for Summary {
        fn apply(sum: &mut u64, input: SummaryInput, _: &mut ViewCx<Self>) {
            let SummaryInput::Credited(e) = input;
            *sum += e.amount; // applying an event twice shows
        }
    }

    #[derive(domain::View)]
    #[view(key = WalletId, from(Summary(Credited)), state = u64)]
    pub struct Overview;

    impl Apply for Overview {
        fn apply(copy: &mut u64, _: OverviewInput, cx: &mut ViewCx<Self>) {
            *copy = *cx.state::<Summary>().expect("reads the summary");
        }
    }

    #[derive(Model)]
    #[model(aggregates(Wallet), views(Summary, Overview))]
    pub struct Wallets;
}

/// The summary now counts double and skips wallet `b`, under a new version. `Overview` is unchanged.
mod v1 {
    use super::*;
    use domain::{KeyOf, Origin};

    #[derive(domain::View)]
    #[view(key = WalletId, from(Wallet(Credited)), key_of(Wallet), state = u64, version = 1)]
    pub struct Summary;

    impl KeyOf<Wallet> for Summary {
        fn key_of(from: Origin<'_, Wallet>, _: &SummaryInput) -> Option<WalletId> {
            (from.id.as_str() != "b").then(|| from.id.clone())
        }
    }

    impl Apply for Summary {
        fn apply(sum: &mut u64, input: SummaryInput, _: &mut ViewCx<Self>) {
            let SummaryInput::Credited(e) = input;
            *sum += 2 * e.amount;
        }
    }

    #[derive(domain::View)]
    #[view(key = WalletId, from(Summary(Credited)), state = u64)]
    pub struct Overview;

    impl Apply for Overview {
        fn apply(copy: &mut u64, _: OverviewInput, cx: &mut ViewCx<Self>) {
            *copy = *cx.state::<Summary>().expect("reads the summary");
        }
    }

    #[derive(Model)]
    #[model(aggregates(Wallet), views(Summary, Overview))]
    pub struct Wallets;
}

static FLAKY_FAILS: AtomicBool = AtomicBool::new(false);

#[derive(domain::View)]
#[view(key = WalletId, from(Wallet(Credited)), state = u64)]
pub struct Flaky;

impl Apply for Flaky {
    fn apply(sum: &mut u64, input: FlakyInput, _: &mut ViewCx<Self>) {
        assert!(!FLAKY_FAILS.load(Ordering::SeqCst), "flaky view fails");
        let FlakyInput::Credited(e) = input;
        *sum += e.amount;
    }
}

#[derive(Model)]
#[model(aggregates(Wallet), views(Flaky))]
pub struct FlakyWallets;

/// A database file that's deleted when the test ends.
struct TempDb(PathBuf);

impl TempDb {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("domain-sqlite-{}-{name}.db", std::process::id()));
        let db = TempDb(path);
        db.remove();
        db
    }

    fn open<M: domain::host::Hosts<Memory>>(&self) -> Memory {
        Memory::new().persist_to(Sqlite::open(&self.0).unwrap()).with::<M>()
    }

    fn remove(&self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        self.remove();
    }
}

fn w(id: &str) -> WalletId {
    WalletId::new(id)
}

#[test]
fn events_receipts_and_views_survive_a_restart() {
    let db = TempDb::new("restart");
    let (paid, events_before) = {
        let memory = db.open::<v0::Wallets>();
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        memory.send(&w("a"), Credit { amount: 7 }).unwrap();
        let paid = memory.send_with_id(&w("b"), Credit { amount: 1 }, "pay-1").unwrap();
        (paid, memory.events(&w("a")))
    };

    let memory = db.open::<v0::Wallets>();
    assert_eq!(memory.state(&w("a")), 12);
    assert_eq!(memory.events(&w("a")), events_before);
    assert_eq!(memory.versions(), [(StreamId::new("Wallet", "a"), 2), (StreamId::new("Wallet", "b"), 1)]);
    let summaries = memory.view::<v0::Summary>();
    assert_eq!((summaries.get(&w("a")), summaries.list(&w("a"), Cursor::START, 10).len()), (Some(12), 2));
    assert_eq!(memory.view::<v0::Overview>().get(&w("a")), Some(12));

    // A retry from before the restart returns the first receipt and records nothing.
    assert_eq!(memory.send_with_id(&w("b"), Credit { amount: 1 }, "pay-1"), Ok(paid));
    assert_eq!(memory.state(&w("b")), 1);
    assert!(matches!(memory.send_with_id(&w("b"), Note, "pay-1"), Err(SendError::CommandIdReused { .. })));

    // New command ids don't collide with the first run's.
    let receipt = memory.send(&w("a"), Credit { amount: 1 }).unwrap();
    assert_eq!((receipt.version, receipt.reply), (3, 13));
    assert_eq!(memory.view::<v0::Summary>().get(&w("a")), Some(13));
}

#[test]
fn a_view_applies_each_event_once_across_restarts() {
    let db = TempDb::new("once");
    {
        let memory = db.open::<WalletsOnly>();
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        memory.send(&w("b"), Credit { amount: 2 }).unwrap();
    }
    // Added later: it reads the stored history, though no instance is loaded.
    assert_eq!(db.open::<v0::Wallets>().view::<v0::Summary>().get(&w("a")), Some(5));
    {
        let memory = db.open::<v0::Wallets>();
        memory.send(&w("a"), Credit { amount: 1 }).unwrap();
    }
    let memory = db.open::<v0::Wallets>();
    let summaries = memory.view::<v0::Summary>();
    assert_eq!((summaries.get(&w("a")), summaries.get(&w("b"))), (Some(6), Some(2)));
    assert_eq!(summaries.list(&w("a"), Cursor::START, 10).len(), 2);
}

#[test]
fn a_view_that_fell_behind_catches_up_on_the_next_run() {
    let db = TempDb::new("behind");
    {
        let memory = db.open::<FlakyWallets>();
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        FLAKY_FAILS.store(true, Ordering::SeqCst);
        memory.send(&w("a"), Credit { amount: 7 }).expect("the command commits though its view fails");
    }
    FLAKY_FAILS.store(false, Ordering::SeqCst);
    let memory = db.open::<FlakyWallets>();
    assert_eq!(memory.view::<Flaky>().get(&w("a")), Some(12));
}

#[test]
fn changing_a_views_version_rebuilds_it_and_the_views_that_read_it() {
    let db = TempDb::new("version");
    {
        let memory = db.open::<v0::Wallets>();
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        memory.send(&w("a"), Credit { amount: 7 }).unwrap();
        memory.send(&w("b"), Credit { amount: 3 }).unwrap();
    }
    let memory = db.open::<v1::Wallets>();
    assert_eq!(memory.view::<v1::Summary>().get(&w("a")), Some(24));
    assert_eq!(memory.view::<v1::Overview>().get(&w("a")), Some(24), "rebuilt, though its own version is the same");
    assert_eq!(memory.view::<v1::Summary>().list(&w("a"), Cursor::START, 10).len(), 2);
    memory.send(&w("a"), Credit { amount: 1 }).unwrap();
    drop(memory);

    let memory = db.open::<v1::Wallets>();
    assert_eq!(memory.view::<v1::Overview>().get(&w("a")), Some(26));
    assert_eq!(memory.view::<v1::Summary>().get(&w("b")), None, "the old version's keys are gone");

    // Going back is a change too.
    let memory = db.open::<v0::Wallets>();
    assert_eq!(memory.view::<v0::Overview>().get(&w("a")), Some(13));
}

/// Credits `<id>-bonus` with 1 for every credit, except to bonus wallets.
#[derive(Policy)]
#[policy(from(Wallet(Credited)))]
pub struct Bonus;

impl React for Bonus {
    async fn react(&self, cx: &PolicyCx<Self>, _: BonusInput) -> Result<(), PolicyError> {
        let id = cx.source::<Wallet>().expect("from wallets");
        if !id.as_str().ends_with("-bonus") {
            cx.send(&w(&format!("{id}-bonus")), Credit { amount: 1 })?;
        }
        Ok(())
    }
}

#[test]
fn a_new_policy_ignores_the_past_and_a_stored_one_acts_on_what_it_missed() {
    let db = TempDb::new("policies");
    {
        let memory = db.open::<WalletsOnly>();
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        memory.send(&w("c"), Credit { amount: 5 }).unwrap(); // never credited again
    }

    let with_bonus = || Rc::new(db.open::<WalletsOnly>().policy(Bonus));
    let memory = with_bonus();
    memory.run_due();
    assert_eq!(memory.state(&w("a-bonus")), 0, "new: the past isn't acted on");
    memory.send(&w("a"), Credit { amount: 1 }).unwrap();
    memory.run_due();
    assert_eq!(memory.state(&w("a-bonus")), 1);
    drop(memory);

    // While the policy isn't running, b is credited.
    db.open::<WalletsOnly>().send(&w("b"), Credit { amount: 2 }).unwrap();
    let memory = with_bonus();
    memory.run_due();
    assert_eq!((memory.state(&w("a-bonus")), memory.state(&w("b-bonus"))), (1, 1), "it caught up, once");
    assert_eq!(memory.state(&w("c-bonus")), 0, "and still not on the past");
}

#[test]
fn a_schedule_fires_after_a_restart_once() {
    let db = TempDb::new("schedules");
    let open_at = |ms: u64| {
        let memory = Memory::with_clock(move || Timestamp::from_millis(ms));
        Rc::new(memory.persist_to(Sqlite::open(&db.0).unwrap()).with::<WalletsOnly>())
    };
    let memory = open_at(1_000);
    memory.schedule(&w("a"), Note, Timestamp::from_millis(5_000), "note-1").unwrap();
    memory.schedule(&w("a"), Note, Timestamp::from_millis(5_000), "note-2").unwrap();
    memory.unschedule(&w("a"), "note-2").unwrap();
    assert_eq!(memory.run_due(), Some(Timestamp::from_millis(5_000)));
    drop(memory);

    let memory = open_at(6_000);
    assert_eq!(memory.run_due(), None);
    assert!(memory.send_with_id(&w("a"), Note, "note-1").is_ok(), "note-1 ran");
    assert!(matches!(
        memory.send_with_id(&w("a"), Credit { amount: 1 }, "note-1"),
        Err(SendError::CommandIdReused { .. })
    ));
    assert!(matches!(memory.unschedule(&w("a"), "note-2"), Err(domain_memory::UnscheduleError::AlreadyCancelled)));
    drop(memory);

    let memory = open_at(7_000);
    assert_eq!(memory.run_due(), None, "nothing left to fire");
}

/// Copies each credit's `(wallet, seq)` to a list the test reads, as a projection would to a
/// table. `p1` is the same projection under a new version.
mod p0 {
    use super::*;
    use domain::Projection;
    use domain_memory::{Project, ProjectionError, ProjectionInput};

    #[derive(Projection)]
    #[projection(key = WalletId, from(Wallet(Credited)))]
    pub struct Rows(pub Arc<std::sync::Mutex<Vec<(String, u64)>>>);

    impl Project for Rows {
        async fn project(&self, key: &WalletId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> {
            self.0.lock().unwrap().extend(batch.iter().map(|input| (key.to_string(), input.seq)));
            Ok(())
        }
    }
}

mod p1 {
    use super::*;
    use domain::Projection;
    use domain_memory::{Project, ProjectionError, ProjectionInput};

    #[derive(Projection)]
    #[projection(key = WalletId, from(Wallet(Credited)), version = 1)]
    pub struct Rows(pub Arc<std::sync::Mutex<Vec<(String, u64)>>>);

    impl Project for Rows {
        async fn project(&self, key: &WalletId, batch: Vec<ProjectionInput<Self>>) -> Result<(), ProjectionError> {
            self.0.lock().unwrap().extend(batch.iter().map(|input| (key.to_string(), input.seq)));
            Ok(())
        }
    }
}

#[test]
fn a_projection_writes_what_it_missed_and_all_of_history_when_its_version_changes() {
    let db = TempDb::new("projections");
    let written = Arc::new(std::sync::Mutex::new(Vec::new()));
    let take = || std::mem::take(&mut *written.lock().unwrap());
    {
        let memory = Rc::new(db.open::<WalletsOnly>().projection(p0::Rows(written.clone())));
        memory.send(&w("a"), Credit { amount: 5 }).unwrap();
        memory.run_due();
    }
    db.open::<WalletsOnly>().send(&w("a"), Credit { amount: 1 }).unwrap();
    assert_eq!(take(), [("a".to_string(), 1)]);

    Rc::new(db.open::<WalletsOnly>().projection(p0::Rows(written.clone()))).run_due();
    assert_eq!(take(), [("a".to_string(), 2)], "only what it missed");

    Rc::new(db.open::<WalletsOnly>().projection(p1::Rows(written.clone()))).run_due();
    assert_eq!(take(), [("a".to_string(), 1), ("a".to_string(), 2)], "a new version gets all of history");
}

/// Credits `<key>-paid` with 1, tries to claim `<key>`, waits for `go`, then credits
/// `<key>-paid` with 10 if the claim succeeded, or 100 if it was refused.
#[derive(domain::Saga, Debug, Serialize, Deserialize)]
#[saga(key = WalletId)]
pub struct Pay;

impl domain_memory::Run for Pay {
    async fn run(self, cx: &domain_memory::SagaCx<Self>) -> Result<(), PayRejection> {
        let paid = w(&format!("{}-paid", cx.key()));
        let _ = cx.send("first", &paid, Credit { amount: 1 }).await;
        let claimed = cx.send("claim", cx.key(), Claim).await.is_ok();
        cx.signal::<()>("go").await;
        let _ = cx.send("second", &paid, Credit { amount: if claimed { 10 } else { 100 } }).await;
        Ok(())
    }
}

#[test]
fn a_saga_run_carries_on_after_a_restart_without_repeating_its_steps() {
    use domain_memory::SagaStatus;
    let db = TempDb::new("sagas");
    let open = || Rc::new(db.open::<WalletsOnly>().saga::<Pay>());
    {
        let memory = open();
        memory.start_saga(&w("a"), Pay).unwrap();
        memory.run_due();
        assert_eq!(memory.state(&w("a-paid")), 1);
    }

    let memory = open();
    assert_eq!(memory.saga_status::<Pay>(&w("a")), SagaStatus::Running);
    memory.send(&w("a"), Credit { amount: 5 }).unwrap(); // a claim would succeed now
    memory.signal::<Pay>(&w("a"), "go", ()).unwrap();
    memory.run_due();
    assert_eq!(memory.saga_status::<Pay>(&w("a")), SagaStatus::Completed);
    assert_eq!(memory.versions().into_iter().find(|(s, _)| s.as_str() == "Wallet/a-paid").unwrap().1, 2);
    drop(memory);

    let memory = open();
    memory.run_due();
    assert_eq!(memory.saga_status::<Pay>(&w("a")), SagaStatus::Completed);
    assert_eq!(memory.state(&w("a-paid")), 101, "one credit per step, and the claim stays refused");
}

#[test]
fn in_a_batch_a_failed_write_is_dropped_alone_and_the_rest_commit_together() {
    use domain_memory::store::{Commit, EventRow, Store};
    let db = TempDb::new("batch");
    let credit = |seq: u64| Commit {
        aggregate: "Wallet".into(),
        stream: "Wallet/a".into(),
        events: vec![EventRow {
            stream: "Wallet/a".into(),
            seq,
            id: "\"a\"".into(),
            at: 0,
            command_id: None,
            event: format!(r#"{{"Credited":{{"amount":1,"total":{seq}}}}}"#),
        }],
        receipt: None,
    };
    {
        let store = Sqlite::open(&db.0).unwrap();
        store.begin_batch().unwrap();
        store.commit(&credit(1)).unwrap();
        assert!(store.commit(&credit(1)).is_err(), "seq 1 is taken");
        store.commit(&credit(2)).unwrap();
        store.commit_batch().unwrap();
    }
    let memory = db.open::<WalletsOnly>();
    assert_eq!((memory.state(&w("a")), memory.events(&w("a")).len()), (2, 2));
}

#[test]
fn commands_sent_through_a_runtime_without_waiting_are_all_stored() {
    let db = TempDb::new("runtime");
    let path = db.0.clone();
    let runtime = domain_memory::Runtime::spawn(move || {
        Memory::new().persist_to(Sqlite::open(&path).unwrap()).with::<v0::Wallets>()
    });
    let (answer, answers) = std::sync::mpsc::channel();
    for _ in 0..200 {
        let answer = answer.clone();
        runtime.exec(
            |memory| memory.send(&w("a"), Credit { amount: 1 }).map(|receipt| receipt.reply),
            move |sent| answer.send(sent).unwrap(),
        );
    }
    let mut totals: Vec<u64> = answers.iter().take(200).map(|sent| sent.unwrap().unwrap()).collect();
    totals.sort();
    assert_eq!(totals, (1..=200).collect::<Vec<_>>());

    // Every answer came after its command was stored: a second run reads them all.
    let memory = db.open::<v0::Wallets>();
    assert_eq!(memory.state(&w("a")), 200);
    assert_eq!(memory.view::<v0::Summary>().get(&w("a")), Some(200));
}
