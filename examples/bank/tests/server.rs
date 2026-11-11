//! The bank served over WebSocket: the same app code as in process, with sessions, many users,
//! and a connection that drops.

use bank::*;
use domain::{Cursor, Frame, SendError};
use domain_client::{Client, Connection, Pending, ReadError};
use domain_memory::Memory;
use domain_server::{Listening, Server};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn serve(memory: &Arc<Memory>, addr: &str) -> Listening {
    // Only sam and kim may sign in; the token is their name.
    Server::<BankApi>::new(memory.clone())
        .sessions(|token| ["sam", "kim"].contains(&token).then(|| User(token.into())))
        .listen(addr)
        .unwrap()
}

fn bank() -> Arc<Memory> {
    Arc::new(Memory::new().with::<Bank>())
}

fn sam() -> AccountId {
    AccountId::new("sam-main")
}

#[test]
fn the_app_code_works_the_same_in_process_and_over_a_connection() {
    fn deposit_and_overdraw(client: &Client<BankApi>, someone_else: &Client<BankApi>) {
        let summary = client.view::<AccountSummary>().live(&sam());
        // Before it's anyone's, anyone may follow the statement.
        let snooping = someone_else.view::<Statement>().events(&sam(), Cursor::START);
        let watching = someone_else.view::<AccountSummary>().live(&sam());
        wait(client.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
        let receipt = wait(client.send(&sam(), DepositMoney { amount: 100 })).unwrap();
        assert_eq!(receipt.reply, Balance { balance: 100 });
        let Err(SendError::Rejected(r)) = wait(client.send(&sam(), WithdrawMoney { amount: 500 })) else {
            panic!("an overdraft is refused")
        };
        assert_eq!(r, InsufficientFunds { balance: 100, requested: 500 }.into());

        let read = wait(client.view::<AccountSummary>().get_at_least(&sam(), &receipt));
        assert_eq!(read, Ok(Some(Summary { owner: Some("sam".into()), balance: 100 })));
        eventually(|| *summary.latest().0 == Ok(Some(Summary { owner: Some("sam".into()), balance: 100 })));
        let lines = wait(client.view::<Statement>().list(&sam(), Cursor::START, 10)).unwrap();
        assert_eq!(lines.len(), 1);
        let feed = client.view::<Statement>().events(&sam(), Cursor::START);
        let mut frames = Vec::new();
        eventually(|| {
            frames.extend(feed.drain());
            frames.len() == 2
        });
        assert!(matches!(&frames[..], [Frame::Entry(e), Frame::CaughtUp { .. }] if e.cursor == Cursor::new(1)));

        // Someone else may neither read it nor take from it, and their feed ended at sam's first line.
        let mut snooped = Vec::new();
        eventually(|| {
            snooped.extend(snooping.drain());
            matches!(snooped.last(), Some(Frame::Rejected(_)))
        });
        assert!(snooped.iter().all(|f| f.entry().is_none()), "no lines reach them: {snooped:?}");
        eventually(|| *watching.latest().0 == Err(ReadError::Rejected(NotOwner.into())));
        let theirs = wait(someone_else.view::<AccountSummary>().get(&sam()));
        assert_eq!(theirs, Err(ReadError::Rejected(NotOwner.into())));
        let Err(SendError::Rejected(r)) = wait(someone_else.send(&sam(), WithdrawMoney { amount: 1 })) else {
            panic!("only the owner withdraws")
        };
        assert_eq!(r, NotOwner.into());

        // A retry under the same id takes effect once.
        let first = wait(client.send_with_id(&sam(), DepositMoney { amount: 5 }, "tip-1")).unwrap();
        let retry = wait(client.send_with_id(&sam(), DepositMoney { amount: 5 }, "tip-1")).unwrap();
        assert_eq!(first, retry);
    }

    let local = Client::in_process(bank());
    deposit_and_overdraw(&local.acting_as(User("sam".into())), &local.acting_as(User("kim".into())));
    let server = serve(&bank(), "127.0.0.1:0");
    deposit_and_overdraw(&Client::connect(&server.url(), Some("sam")), &Client::connect(&server.url(), Some("kim")));
}

#[test]
fn each_user_sees_only_their_own_accounts() {
    let server = serve(&bank(), "127.0.0.1:0");
    let (sam_app, kim_app) = (
        Client::<BankApi>::connect(&server.url(), Some("sam")),
        Client::<BankApi>::connect(&server.url(), Some("kim")),
    );
    // Before sam opens it, the account has no owner, so anyone may watch for it.
    let kim_watching = kim_app.view::<AccountSummary>().live(&sam());
    eventually(|| kim_watching.latest().1 == 1);

    wait(sam_app.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
    wait(sam_app.send(&sam(), DepositMoney { amount: 100 })).unwrap();

    // Once it's sam's, kim's watch ends, and her reads and withdrawals are refused.
    eventually(|| *kim_watching.latest().0 == Err(ReadError::Rejected(NotOwner.into())));
    assert_eq!(wait(kim_app.view::<AccountSummary>().get(&sam())), Err(ReadError::Rejected(NotOwner.into())));
    let Err(SendError::Rejected(r)) = wait(kim_app.send(&sam(), WithdrawMoney { amount: 1 })) else {
        panic!("kim can't withdraw from sam's account")
    };
    assert_eq!(r, NotOwner.into());
    let Err(SendError::Rejected(r)) = wait(kim_app.send(&sam(), OpenAccount { owner: "sam".into() })) else {
        panic!("kim can't open an account for sam")
    };
    assert_eq!(r, NotOwner.into());
    let feed = kim_app.view::<Statement>().events(&sam(), Cursor::START);
    eventually(|| matches!(feed.try_next(), Some(Frame::Rejected(_))));
}

#[test]
fn a_change_from_one_user_reaches_everyone_watching() {
    let server = serve(&bank(), "127.0.0.1:0");
    let (phone, laptop) = (
        Client::<BankApi>::connect(&server.url(), Some("sam")),
        Client::<BankApi>::connect(&server.url(), Some("sam")),
    );
    wait(phone.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
    let on_laptop = laptop.view::<AccountSummary>().live(&sam());
    let lines = laptop.view::<Statement>().events(&sam(), Cursor::START);
    eventually(|| matches!(lines.try_next(), Some(Frame::CaughtUp { .. })));

    wait(phone.send(&sam(), DepositMoney { amount: 40 })).unwrap();
    eventually(|| on_laptop.latest().0.as_ref().clone().is_ok_and(|s| s.is_some_and(|s| s.balance == 40)));
    eventually(|| matches!(lines.try_next(), Some(Frame::Entry(e)) if e.cursor == Cursor::new(1)));
}

#[test]
fn a_token_the_server_doesnt_accept_is_refused_for_good() {
    let server = serve(&bank(), "127.0.0.1:0");
    let stranger = Client::<BankApi>::connect(&server.url(), Some("eve"));
    let connection = stranger.connection();
    eventually(|| *connection.latest().0 == Connection::Refused { status: 401 });
    let Err(SendError::Unavailable { reason, .. }) = wait(stranger.send(&sam(), DepositMoney { amount: 1 })) else {
        panic!("nothing reaches the model")
    };
    assert!(reason.contains("401"), "{reason}");
}

#[test]
fn after_the_connection_drops_live_reads_carry_on_from_where_they_were() {
    let memory = bank();
    let server = serve(&memory, "127.0.0.1:0");
    let addr = server.addr().to_string();
    let app = Client::<BankApi>::connect(&server.url(), Some("sam"));
    wait(app.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
    wait(app.send(&sam(), DepositMoney { amount: 10 })).unwrap();
    let summary = app.view::<AccountSummary>().live(&sam());
    let lines = app.view::<Statement>().events(&sam(), Cursor::START);
    let mut seen = Vec::new();
    eventually(|| {
        seen.extend(lines.drain());
        seen.iter().any(Frame::is_caught_up)
    });
    let connection = app.connection();

    // The server goes away, and the model changes while the app is cut off.
    server.stop();
    eventually(|| matches!(*connection.latest().0, Connection::Reconnecting { .. }));
    assert!(matches!(wait(app.send(&sam(), DepositMoney { amount: 1 })), Err(SendError::Unavailable { .. })));
    memory.send(&sam(), DepositMoney { amount: 5 }).unwrap();

    let _server = serve(&memory, &addr);
    eventually(|| *connection.latest().0 == Connection::Connected);
    eventually(|| matches!(&*summary.latest().0, Ok(Some(s)) if s.balance == 15));
    eventually(|| {
        seen.extend(lines.drain());
        seen.iter().filter(|f| f.is_caught_up()).count() == 2
    });
    // Each line once: the feed picked up after the last one it had.
    let cursors: Vec<u64> = seen.iter().filter_map(|f| f.entry()).map(|e| e.cursor.seq()).collect();
    assert_eq!(cursors, [1, 2]);
}

/// The frame loop: check each frame until it's there.
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

#[track_caller]
fn eventually(mut check: impl FnMut() -> bool) {
    let start = Instant::now();
    while !check() {
        assert!(start.elapsed() < Duration::from_secs(5), "not within 5s");
        std::thread::sleep(Duration::from_millis(5));
    }
}
