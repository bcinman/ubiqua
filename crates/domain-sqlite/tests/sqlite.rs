use domain::{
    Aggregate, Apply, Command, Cursor, Cx, Decide, Decision, Event, Evolve, Model, SendError, StreamId, ViewCx,
};
use domain_memory::Memory;
use domain_sqlite::Sqlite;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

// A wallet records credits. `Summary` sums them, `Overview` copies the summary's state, and
// `Flaky` fails while `FLAKY_FAILS` is set.

domain::id!(
    pub struct WalletId;
);

#[derive(Command, Debug)]
#[command(target = WalletId, events(Credited), reply = u64)]
pub struct Credit {
    amount: u64,
}

#[derive(Command, Debug)]
#[command(target = WalletId, events(Credited))]
pub struct Note;

#[derive(Event, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Credited {
    amount: u64,
    total: u64,
}

#[derive(Aggregate)]
#[aggregate(id = WalletId, state = u64, events(Credited), commands(Credit, Note))]
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
    fn reply(total: &u64) -> u64 {
        *total
    }
}

impl Decide<Note> for Wallet {
    fn decide(_: &u64, _: &Note, _: &Cx) -> Decision<Note> {
        Ok(vec![])
    }
    fn reply(_: &u64) {}
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

    fn open<M: Model>(&self) -> Memory {
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
