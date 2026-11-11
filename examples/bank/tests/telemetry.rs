//! What a dashboard sees of the bank: its parts, its accounts as they stood, and a trace of
//! commands, events and view changes, replayed or live, in process and over a connection.

use bank::*;
use domain::Timestamp;
use domain::telemetry::{Kind, Outcome, Since, Trace};
use domain_client::{Client, Pending};
use domain_memory::{Memory, TraceFeed};
use domain_server::Server;
use domain_sqlite::Sqlite;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A clock that moves on with every reading, so the order events were recorded in is also the
/// order of their times.
fn ticking() -> impl Fn() -> Timestamp + Send + Sync + 'static {
    let now = AtomicU64::new(1_762_819_200_000);
    move || Timestamp::from_millis(now.fetch_add(1, Ordering::Relaxed))
}

fn sam() -> AccountId {
    AccountId::new("sam-main")
}

fn kim() -> AccountId {
    AccountId::new("kim-main")
}

/// Sam opens, deposits, overdraws and withdraws; kim opens and deposits.
fn busy_day(bank: &Memory) {
    bank.send(&sam(), OpenAccount { owner: "sam".into() }).unwrap();
    bank.send(&sam(), DepositMoney { amount: 100 }).unwrap();
    bank.send(&sam(), WithdrawMoney { amount: 500 }).unwrap_err();
    bank.send(&sam(), WithdrawMoney { amount: 30 }).unwrap();
    bank.send(&kim(), OpenAccount { owner: "kim".into() }).unwrap();
    bank.send(&kim(), DepositMoney { amount: 5 }).unwrap();
}

fn without_commands(traces: &[Trace]) -> Vec<Trace> {
    traces.iter().filter(|t| !matches!(t, Trace::Command(_) | Trace::CaughtUp { .. })).cloned().collect()
}

#[test]
fn a_live_trace_shows_each_command_its_events_and_the_view_changes_they_caused() {
    let bank = Memory::with_clock(ticking()).with::<Bank>();
    let live = bank.telemetry().trace(Since::Now).unwrap();
    busy_day(&bank);
    let traces = live.drain();

    assert_eq!(traces[0], Trace::CaughtUp { streams: BTreeMap::new() }, "nothing to replay yet");
    let commands: Vec<(String, Outcome)> = traces
        .iter()
        .filter_map(|t| match t {
            Trace::Command(c) => Some((format!("{} {}", c.command, c.stream), c.outcome.clone())),
            _ => None,
        })
        .collect();
    let committed = |version| Outcome::Committed { version };
    let refused = serde_json::json!({ "InsufficientFunds": { "balance": 100, "requested": 500 } });
    assert_eq!(
        commands,
        [
            ("OpenAccount Account/sam-main".into(), committed(1)),
            ("DepositMoney Account/sam-main".into(), committed(2)),
            ("WithdrawMoney Account/sam-main".into(), Outcome::Rejected(refused)),
            ("WithdrawMoney Account/sam-main".into(), committed(3)),
            ("OpenAccount Account/kim-main".into(), committed(1)),
            ("DepositMoney Account/kim-main".into(), committed(2)),
        ]
    );

    // The deposit, then its event, then each view it changed, a view of views last.
    let deposit = traces
        .iter()
        .position(|t| matches!(t, Trace::Command(c) if c.command == "DepositMoney" && c.stream == "Account/sam-main"))
        .unwrap();
    let Trace::Event(event) = &traces[deposit + 1] else { panic!("the event follows its command: {traces:#?}") };
    assert_eq!((event.stream.as_str(), event.seq), ("Account/sam-main", 2));
    assert_eq!(event.event, serde_json::json!({ "MoneyDeposited": { "amount": 100, "balance_after": 100 } }));
    let views: Vec<_> = traces[deposit + 2..]
        .iter()
        .map_while(|t| match t {
            Trace::View(v) => Some(v),
            _ => None,
        })
        .collect();
    let names: Vec<&str> = views.iter().map(|v| v.view.as_str()).collect();
    assert_eq!(names, ["AccountSummary", "OwnerHoldings", "Statement"]);
    assert_eq!(views[0].state, serde_json::json!({ "owner": "sam", "balance": 100 }));
    assert_eq!((views[1].cause.stream.as_str(), views[1].cause.seq), ("AccountSummary/sam-main", 2));
    assert_eq!(views[1].key, serde_json::json!("sam"));
    assert_eq!(views[2].emitted.len(), 1);
    assert_eq!(views[2].emitted[0].cursor, 1);
    assert_eq!(
        views[2].emitted[0].event,
        serde_json::json!({ "StatementLine": { "kind": "Deposit", "amount": 100, "balance": 100 } })
    );
}

#[test]
fn a_replay_shows_the_same_events_and_view_changes_as_the_live_trace_did() {
    let bank = Memory::with_clock(ticking()).with::<Bank>();
    let live = bank.telemetry().trace(Since::Now).unwrap();
    busy_day(&bank);
    let shown = without_commands(&live.drain());

    let replay = bank.telemetry().trace(Since::Start).unwrap().drain();
    let Some(Trace::CaughtUp { streams }) = replay.last() else { panic!("ends caught up: {replay:#?}") };
    assert_eq!(without_commands(&replay), shown, "the same changes, in the same order");
    assert_eq!(streams["Account/sam-main"], 3);
    assert_eq!(streams["Account/kim-main"], 2);
    assert_eq!(streams["Statement/sam-main"], 2);

    // Picking up from there replays nothing old.
    let resumed = bank.telemetry().trace(Since::After(streams.clone())).unwrap();
    bank.send(&kim(), DepositMoney { amount: 7 }).unwrap();
    let resumed = resumed.drain();
    assert!(matches!(&resumed[0], Trace::CaughtUp { streams: s } if s == streams), "{resumed:#?}");
    let events: Vec<(&str, u64)> = resumed
        .iter()
        .filter_map(|t| match t {
            Trace::Event(e) => Some((e.stream.as_str(), e.seq)),
            _ => None,
        })
        .collect();
    assert_eq!(events, [("Account/kim-main", 3)]);

    // The live views weren't touched by the replays.
    assert_eq!(bank.view::<Statement>().list(&sam(), domain::Cursor::START, 10).len(), 2);
}

#[test]
fn a_trace_started_while_commands_commit_shows_each_event_once() {
    let bank = Arc::new(Memory::with_clock(ticking()).with::<Bank>());
    bank.send(&sam(), OpenAccount { owner: "sam".into() }).unwrap();
    const DEPOSITS: u64 = 300;
    let sending = {
        let bank = bank.clone();
        std::thread::spawn(move || {
            for _ in 0..DEPOSITS {
                bank.send(&sam(), DepositMoney { amount: 1 }).unwrap();
            }
        })
    };
    std::thread::sleep(Duration::from_millis(2));
    let trace = bank.telemetry().trace(Since::Start).unwrap();
    sending.join().unwrap();

    let traces = trace.drain();
    let seqs: Vec<u64> = traces
        .iter()
        .filter_map(|t| match t {
            Trace::Event(e) => Some(e.seq),
            _ => None,
        })
        .collect();
    assert_eq!(seqs, (1..=DEPOSITS + 1).collect::<Vec<_>>());
    let lines: Vec<u64> = traces
        .iter()
        .filter_map(|t| match t {
            Trace::View(v) if v.view == "Statement" => v.emitted.first().map(|e| e.cursor),
            _ => None,
        })
        .collect();
    assert_eq!(lines, (1..=DEPOSITS).collect::<Vec<_>>());
    assert_eq!(traces.iter().filter(|t| matches!(t, Trace::CaughtUp { .. })).count(), 1);
}

#[test]
fn an_account_shows_as_it_stood_at_each_version() {
    let bank = Memory::with_clock(ticking()).with::<Bank>();
    busy_day(&bank);
    let telemetry = bank.telemetry();

    let then = telemetry.aggregate("Account/sam-main", Some(2)).unwrap().unwrap();
    assert_eq!((then.version, then.events.len(), then.id.clone()), (2, 2, serde_json::json!("sam-main")));
    assert!(then.state.contains("balance: 100,"), "{}", then.state);
    let now = telemetry.aggregate("Account/sam-main", None).unwrap().unwrap();
    assert!(now.state.contains("balance: 70,"), "{}", now.state);
    assert_eq!(telemetry.aggregate("Account/nobody", None), Ok(None));
    assert_eq!(telemetry.aggregate("Nothing/sam-main", None), Ok(None));

    let lines = telemetry.view("Statement").unwrap();
    let keys: Vec<(&str, u64)> = lines.iter().map(|k| (k.stream.as_str(), k.cursor)).collect();
    assert_eq!(keys, [("Statement/kim-main", 1), ("Statement/sam-main", 2)]);
    assert_eq!(telemetry.view("Nothing"), None);

    let catalog = telemetry.catalog();
    let part = |name: &str| catalog.parts.iter().find(|p| p.name == name).unwrap_or_else(|| panic!("{name}"));
    assert_eq!((part("Account").kind, part("Transfer").kind), (Kind::Aggregate, Kind::Aggregate));
    assert_eq!(
        (part("OwnerHoldings").kind, &part("OwnerHoldings").reads[..]),
        (Kind::View, &["AccountSummary".into()][..])
    );
    assert_eq!(part("Statement").version, 1);
    assert_eq!((part("FraudReview").kind, &part("FraudReview").reads[..]), (Kind::Policy, &["Account".into()][..]));
    assert_eq!(part("ExecuteTransfer").kind, Kind::Saga);
    assert_eq!(catalog.parts.iter().filter(|p| p.name == "Account").count(), 1);

    let status = telemetry.status().unwrap();
    assert_eq!(status.streams, BTreeMap::from([("Account/kim-main".into(), 2), ("Account/sam-main".into(), 3)]));
}

#[test]
fn a_replay_reads_what_an_earlier_run_kept_on_disk() {
    let path = std::env::temp_dir().join(format!("bank-telemetry-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let open = || Memory::with_clock(ticking()).persist_to(Sqlite::open(&path).unwrap()).with::<Bank>();
    let shown = {
        let bank = open();
        let live = bank.telemetry().trace(Since::Now).unwrap();
        busy_day(&bank);
        without_commands(&live.drain())
    };

    // No account has been used in this run, so only the file has their events.
    let bank = open();
    let replay = bank.telemetry().trace(Since::Start).unwrap().drain();
    assert_eq!(without_commands(&replay), shown);
    let then = bank.telemetry().aggregate("Account/sam-main", Some(3)).unwrap().unwrap();
    assert!(then.state.contains("balance: 70,"), "{}", then.state);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn a_server_serves_telemetry_only_to_whom_it_allows_and_a_trace_carries_on_after_a_reconnect() {
    let bank = Arc::new(Memory::with_clock(ticking()).with::<Bank>());
    let serve = |addr: &str| {
        Server::<BankApi>::new(bank.clone())
            .sessions(|token| ["sam", "ops"].contains(&token).then(|| User(token.into())))
            .telemetry(|user: Option<&User>| user.is_some_and(|u| u.0 == "ops"))
            .listen(addr)
            .unwrap()
    };
    let server = serve("127.0.0.1:0");
    let addr = server.addr().to_string();
    bank.send(&sam(), OpenAccount { owner: "sam".into() }).unwrap();

    let stranger = Client::<BankApi>::connect(&server.url(), Some("sam"));
    let refused = wait(stranger.telemetry().catalog()).unwrap_err();
    assert!(refused.contains("may not read telemetry"), "{refused}");
    let ended = stranger.telemetry().trace(Since::Start);
    let first = next(&ended, |t| matches!(t, Trace::Failed(_)));
    assert!(matches!(&first[..], [Trace::Failed(_)]), "{first:#?}");

    let ops = Client::<BankApi>::connect(&server.url(), Some("ops"));
    let catalog = wait(ops.telemetry().catalog()).unwrap();
    assert!(catalog.parts.iter().any(|p| p.name == "OwnerHoldings"));
    let account = wait(ops.telemetry().aggregate("Account/sam-main", None)).unwrap().unwrap();
    assert_eq!(account.version, 1);
    let trace = ops.telemetry().trace(Since::Start);
    let mut seen = next(&trace, |t| matches!(t, Trace::CaughtUp { .. }));
    bank.send(&sam(), DepositMoney { amount: 10 }).unwrap();
    seen.extend(next(&trace, |t| matches!(t, Trace::View(v) if v.view == "Statement")));

    // The server goes away and the bank moves on; the trace picks up where it was.
    server.stop();
    bank.send(&sam(), DepositMoney { amount: 5 }).unwrap();
    let _server = serve(&addr);
    seen.extend(next(&trace, |t| matches!(t, Trace::CaughtUp { .. })));
    bank.send(&sam(), DepositMoney { amount: 1 }).unwrap();
    seen.extend(next(&trace, |t| matches!(t, Trace::View(v) if v.view == "Statement" && v.emitted[0].cursor == 3)));

    let seqs: Vec<u64> = seen
        .iter()
        .filter_map(|t| match t {
            Trace::Event(e) => Some(e.seq),
            _ => None,
        })
        .collect();
    assert_eq!(seqs, [1, 2, 3, 4], "each event once: {seen:#?}");
    let live: Vec<&str> = seen
        .iter()
        .filter_map(|t| match t {
            Trace::Command(c) => Some(c.command.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(live, ["DepositMoney", "DepositMoney"], "commands show live; the one while away doesn't");
}

/// Reads `feed` until a trace `done` accepts arrives, and returns everything up to it.
#[track_caller]
fn next(feed: &TraceFeed, done: impl Fn(&Trace) -> bool) -> Vec<Trace> {
    let start = Instant::now();
    let mut got = Vec::new();
    loop {
        while let Some(trace) = feed.try_next() {
            let last = done(&trace);
            got.push(trace);
            if last {
                return got;
            }
        }
        assert!(start.elapsed() < Duration::from_secs(5), "not within 5s: {got:#?}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[track_caller]
fn wait<T>(pending: Pending<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(value) = pending.try_get() {
            return value;
        }
        assert!(start.elapsed() < Duration::from_secs(5), "no answer within 5s");
        std::thread::sleep(Duration::from_millis(5));
    }
}
