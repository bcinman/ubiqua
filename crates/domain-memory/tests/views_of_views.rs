use domain::{Aggregate, Apply, Cursor, Cx, Decide, Decision, Evolve, KeyOf, Model, Origin, View, ViewCx};
use domain_memory::{Memory, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

domain::id!(
    struct AccountId;
);
domain::id!(
    struct OwnerId;
);
domain::id!(
    struct Everyone;
);

#[domain::command(target = AccountId, events(Opened))]
struct Open {
    owner: String,
}

#[domain::command(target = AccountId, events(Moved))]
struct Move {
    amount: i64,
}

#[domain::command(target = AccountId, events(Closed))]
struct Close;

#[domain::event]
struct Opened {
    owner: String,
}

/// Carries no owner: a view keyed by owner must find it in the summary's state.
#[domain::event]
struct Moved {
    amount: i64,
    balance_after: i64,
}

#[domain::event]
struct Closed;

#[derive(Aggregate)]
#[aggregate(id = AccountId, state = i64, events(Opened, Moved, Closed), commands(Open, Move, Close))]
struct Account;

impl Evolve for Account {
    fn evolve(balance: &mut i64, event: &AccountEvent) {
        if let AccountEvent::Moved(e) = event {
            *balance = e.balance_after;
        }
    }
}

impl Decide<Open> for Account {
    fn decide(_: &i64, cmd: &Open, _: &Cx) -> Decision<Open> {
        Ok(vec![Opened { owner: cmd.owner.clone() }.into()])
    }
}

impl Decide<Move> for Account {
    fn decide(balance: &i64, cmd: &Move, _: &Cx) -> Decision<Move> {
        Ok(vec![Moved { amount: cmd.amount, balance_after: balance + cmd.amount }.into()])
    }
}

impl Decide<Close> for Account {
    fn decide(_: &i64, _: &Close, _: &Cx) -> Decision<Close> {
        Ok(vec![Closed.into()])
    }
}

/// Per account. Emits every input it applies, with its state after it.
#[derive(View)]
#[view(key = AccountId, from(Account(Opened, Moved, Closed)), state = Summary)]
struct AccountSummary;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Summary {
    owner: Option<String>,
    balance: i64,
    closed: bool,
}

impl Apply for AccountSummary {
    fn apply(s: &mut Summary, input: AccountSummaryInput, _: &mut ViewCx<Self>) {
        match input {
            AccountSummaryInput::Opened(e) => s.owner = Some(e.owner),
            AccountSummaryInput::Moved(e) => s.balance = e.balance_after,
            AccountSummaryInput::Closed(_) => s.closed = true,
        }
    }
}

/// Per owner: the balance of each open account, from the summaries.
#[derive(View)]
#[view(key = OwnerId, from(AccountSummary(Opened, Moved, Closed)), key_of(AccountSummary), state = Dashboard)]
struct OwnerDashboard;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Dashboard {
    balances: BTreeMap<String, i64>,
}

impl KeyOf<AccountSummary> for OwnerDashboard {
    fn key_of(from: Origin<'_, AccountSummary>, _: &OwnerDashboardInput) -> Option<OwnerId> {
        from.state.owner.clone().map(OwnerId::new)
    }
}

impl Apply for OwnerDashboard {
    fn apply(d: &mut Dashboard, _: OwnerDashboardInput, cx: &mut ViewCx<Self>) {
        let account = cx.source::<AccountSummary>().unwrap().to_string();
        let summary = cx.state::<AccountSummary>().unwrap();
        if summary.closed {
            d.balances.remove(&account);
        } else {
            d.balances.insert(account, summary.balance);
        }
    }
}

/// One key for everyone: each owner's total, from the dashboards. Three levels deep.
#[derive(View)]
#[view(key = Everyone, from(OwnerDashboard(Opened, Moved, Closed)), key_of(OwnerDashboard), state = Totals)]
struct Leaderboard;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Totals {
    by_owner: BTreeMap<String, i64>,
}

impl KeyOf<OwnerDashboard> for Leaderboard {
    fn key_of(_: Origin<'_, OwnerDashboard>, _: &LeaderboardInput) -> Option<Everyone> {
        Some(everyone())
    }
}

impl Apply for Leaderboard {
    fn apply(t: &mut Totals, _: LeaderboardInput, cx: &mut ViewCx<Self>) {
        let owner = cx.source::<OwnerDashboard>().unwrap().to_string();
        let total = cx.state::<OwnerDashboard>().unwrap().balances.values().sum();
        t.by_owner.insert(owner, total);
    }
}

#[derive(Model)]
#[model(aggregates(Account), views(AccountSummary, OwnerDashboard, Leaderboard))]
struct Bank;

/// Lists the views last-first, and leaves the summary out: hosting a view hosts its sources.
#[derive(Model)]
#[model(aggregates(Account), views(Leaderboard))]
struct Backwards;

fn a(id: &str) -> AccountId {
    AccountId::new(id)
}

fn owner(id: &str) -> OwnerId {
    OwnerId::new(id)
}

fn everyone() -> Everyone {
    Everyone::new("all")
}

fn balances(pairs: &[(&str, i64)]) -> Dashboard {
    Dashboard { balances: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect() }
}

fn totals(pairs: &[(&str, i64)]) -> Totals {
    Totals { by_owner: pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect() }
}

/// Every commit at its own time, so history order is unambiguous.
fn ticking() -> impl Fn() -> domain::Timestamp {
    let now = AtomicU64::new(0);
    move || domain::Timestamp::from_millis(now.fetch_add(1, Ordering::Relaxed) + 1)
}

fn activity(memory: &Memory) {
    memory.send(&a("s1"), Open { owner: "sam".into() }).unwrap();
    memory.send(&a("s1"), Move { amount: 50 }).unwrap();
    memory.send(&a("k1"), Open { owner: "kim".into() }).unwrap();
    memory.send(&a("s2"), Open { owner: "sam".into() }).unwrap();
    memory.send(&a("s2"), Move { amount: 7 }).unwrap();
    memory.send(&a("k1"), Move { amount: 3 }).unwrap();
    memory.send(&a("s1"), Move { amount: -20 }).unwrap();
}

#[test]
fn a_view_routes_by_its_source_views_state() {
    let memory = Memory::new().with::<Bank>();
    activity(&memory);

    let dashboards = memory.view::<OwnerDashboard>();
    assert_eq!(dashboards.get(&owner("sam")), Some(balances(&[("s1", 30), ("s2", 7)])));
    assert_eq!(dashboards.get(&owner("kim")), Some(balances(&[("k1", 3)])));

    memory.send(&a("s1"), Close).unwrap();
    assert_eq!(dashboards.get(&owner("sam")), Some(balances(&[("s2", 7)])));
}

#[test]
fn the_model_hosts_a_views_sources_whatever_order_it_lists_them_in() {
    let memory = Memory::new().with::<Backwards>();
    activity(&memory);
    assert_eq!(memory.view::<Leaderboard>().get(&everyone()), Some(totals(&[("kim", 3), ("sam", 37)])));
    assert_eq!(memory.view::<AccountSummary>().get(&a("s1")).unwrap().balance, 30);
}

#[test]
fn a_view_of_views_added_later_receives_the_history() {
    let memory = Memory::with_clock(ticking());
    activity(&memory);
    let memory = memory.with::<Bank>();
    assert_eq!(memory.view::<OwnerDashboard>().get(&owner("sam")), Some(balances(&[("s1", 30), ("s2", 7)])));
    assert_eq!(memory.view::<Leaderboard>().get(&everyone()), Some(totals(&[("kim", 3), ("sam", 37)])));

    // Then live, without applying the history twice.
    memory.send(&a("s2"), Move { amount: 1 }).unwrap();
    assert_eq!(memory.view::<Leaderboard>().get(&everyone()), Some(totals(&[("kim", 3), ("sam", 38)])));
    let entries = memory.view::<OwnerDashboard>().list(&owner("sam"), Cursor::START, usize::MAX);
    assert_eq!(entries.len(), 6, "2 opened + 3 moves before, 1 after");
}

#[test]
fn a_summary_added_after_its_dashboard_existed_is_not_applied_twice() {
    // The summary gets the history once when the dashboard hosts it; listing it again changes nothing.
    let memory = Memory::with_clock(ticking());
    activity(&memory);
    let memory = memory.with::<Backwards>().with::<Bank>();
    let summary = memory.view::<AccountSummary>().list(&a("s1"), Cursor::START, usize::MAX);
    assert_eq!(summary.len(), 3);
    assert_eq!(memory.view::<Leaderboard>().get(&everyone()), Some(totals(&[("kim", 3), ("sam", 37)])));
}

#[test]
fn reading_your_own_write_counts_what_arrives_through_other_views() {
    let memory = Memory::new().with::<Bank>();
    memory.send(&a("s1"), Open { owner: "sam".into() }).unwrap();
    let receipt = memory.send(&a("s1"), Move { amount: 9 }).unwrap();

    let board = memory.view::<Leaderboard>();
    assert_eq!(board.get_at_least(&everyone(), &receipt).unwrap(), Some(totals(&[("sam", 9)])));
}

#[test]
fn a_live_feed_on_a_view_of_views_gets_each_change() {
    let memory = Memory::new().with::<Bank>();
    let feed = memory.view::<OwnerDashboard>().events(&owner("sam"), Cursor::START);
    activity(&memory);

    let states: Vec<i64> =
        feed.drain().iter().filter_map(|f| f.entry()).map(|e| e.state.balances.values().sum()).collect();
    assert_eq!(states, [0, 50, 50, 57, 37]);
}

#[test]
fn views_of_views_stay_exact_under_concurrent_commands() {
    let runtime = Runtime::spawn(|| Memory::new().with::<Bank>());
    runtime
        .call(|memory| {
            for n in 0..4 {
                memory.send(&a(&format!("a{n}")), Open { owner: format!("o{}", n % 2) }).unwrap();
            }
        })
        .unwrap();
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let runtime = runtime.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    runtime
                        .call(move |m| m.send(&a(&format!("a{}", t % 4)), Move { amount: 1 }).map(drop))
                        .unwrap()
                        .unwrap();
                }
            })
        })
        .collect();
    threads.into_iter().for_each(|t| t.join().unwrap());

    runtime
        .call(|memory| {
            assert_eq!(memory.view::<Leaderboard>().get(&everyone()), Some(totals(&[("o0", 400), ("o1", 400)])));
            let summary = memory.view::<AccountSummary>().list(&a("a0"), Cursor::START, usize::MAX);
            let balances: Vec<i64> = summary.iter().skip(1).map(|e| e.state.balance).collect();
            assert_eq!(balances, (1..=200).collect::<Vec<_>>());
        })
        .unwrap();
}

// Two views that read each other.

#[derive(View)]
#[view(key = AccountId, from(Account(Opened), Pong(Opened)))]
struct Ping;

#[derive(View)]
#[view(key = AccountId, from(Ping(Opened)))]
struct Pong;

impl Apply for Ping {
    fn apply(_: &mut (), _: PingInput, _: &mut ViewCx<Self>) {}
}

impl Apply for Pong {
    fn apply(_: &mut (), _: PongInput, _: &mut ViewCx<Self>) {}
}

#[derive(Model)]
#[model(aggregates(Account), views(Ping))]
struct Loop;

#[test]
#[should_panic(expected = "Ping reads itself through other views")]
fn a_view_that_reads_itself_is_refused() {
    let _ = Memory::new().with::<Loop>();
}

// A summary that panics the first time it sees a move of 13.

static SHAKY_PANICKED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(View)]
#[view(key = AccountId, from(Account(Opened, Moved)), state = Summary)]
struct ShakySummary;

impl Apply for ShakySummary {
    fn apply(s: &mut Summary, input: ShakySummaryInput, _: &mut ViewCx<Self>) {
        match input {
            ShakySummaryInput::Opened(e) => s.owner = Some(e.owner),
            ShakySummaryInput::Moved(e) => {
                assert!(e.amount != 13 || SHAKY_PANICKED.swap(true, Ordering::SeqCst), "shaky fails once");
                s.balance = e.balance_after;
            }
        }
    }
}

#[derive(View)]
#[view(key = OwnerId, from(ShakySummary(Opened, Moved)), key_of(ShakySummary), state = i64)]
struct ShakyTotal;

impl KeyOf<ShakySummary> for ShakyTotal {
    fn key_of(from: Origin<'_, ShakySummary>, _: &ShakyTotalInput) -> Option<OwnerId> {
        from.state.owner.clone().map(OwnerId::new)
    }
}

impl Apply for ShakyTotal {
    fn apply(total: &mut i64, _: ShakyTotalInput, cx: &mut ViewCx<Self>) {
        *total = cx.state::<ShakySummary>().unwrap().balance;
    }
}

#[derive(Model)]
#[model(aggregates(Account), views(ShakyTotal))]
struct Shaky;

#[test]
fn reading_a_view_first_catches_up_the_views_it_reads() {
    let memory = Memory::new().with::<Shaky>();
    memory.send(&a("s1"), Open { owner: "sam".into() }).unwrap();
    memory.send(&a("s1"), Move { amount: 13 }).expect("the command commits though its view fails");

    // Nothing else is sent: the read alone must apply the summary's pending move and pass it on.
    assert_eq!(memory.view::<ShakyTotal>().get(&owner("sam")), Some(13));
}
