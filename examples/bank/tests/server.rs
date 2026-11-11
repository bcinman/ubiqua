//! The bank served over WebSocket: the same app code as in process, with sessions, many users,
//! and a connection that drops.

use bank::*;
use domain::{Cursor, Frame, SendError};
use domain_client::{Client, ClientTls, Connection, Pending, ReadError};
use domain_memory::{Memory, Runtime};
use domain_server::{Listening, Server};
use domain_websocket::ServerTls;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn serve(memory: &Runtime, addr: &str) -> Listening {
    // Only sam and kim may sign in; the token is their name.
    Server::<BankApi>::new(memory.clone())
        .sessions(|token| ["sam", "kim"].contains(&token).then(|| User(token.into())))
        .listen(addr)
        .unwrap()
}

fn bank() -> Runtime {
    Runtime::spawn(Memory::new().with::<Bank>())
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
        let Err(SendError::Rejected(r)) = wait(someone_else.send(&sam(), OpenAccount { owner: "sam".into() })) else {
            panic!("nobody opens an account in someone else's name")
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

    // Over TLS, with a certificate the test's own authority signed.
    let certs = concat!(env!("CARGO_MANIFEST_DIR"), "/../../crates/domain-websocket/tests/certs/");
    let read = |name: &str| std::fs::read(format!("{certs}{name}")).unwrap();
    let tls = ServerTls::from_pem(&read("server.pem"), &read("server-key.pem")).unwrap();
    let server = Server::<BankApi>::new(bank())
        .sessions(|token| ["sam", "kim"].contains(&token).then(|| User(token.into())))
        .tls(tls)
        .listen("127.0.0.1:0")
        .unwrap();
    assert!(server.url().starts_with("wss://"));
    let trusted = ClientTls::trusting(&read("ca.pem")).unwrap();
    let connect = |token| Client::connect_with(&server.url(), Some(token), trusted.clone());
    deposit_and_overdraw(&connect("sam"), &connect("kim"));
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
fn after_the_connection_drops_live_reads_and_sends_carry_on() {
    let memory = bank();
    let server = serve(&memory, "127.0.0.1:0");
    let addr = server.addr().to_string();
    let app = Client::<BankApi>::connect(&server.url(), Some("sam"));
    wait(app.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
    wait(app.send(&sam(), DepositMoney { amount: 10 })).unwrap();
    let summary = app.view::<AccountSummary>().live(&sam());
    let shown = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = shown.clone();
    let _redraw = summary.on_change(move |read, _| {
        if let Ok(Some(s)) = &**read {
            record.lock().unwrap().push(s.balance);
        }
    });
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
    memory.call(move |m| m.send(&sam(), DepositMoney { amount: 5 }).map(drop)).unwrap().unwrap();
    // The app sends while offline; the command waits, and the summary shows it at once.
    let offline = app.send(&sam(), DepositMoney { amount: 1 });
    assert_eq!(*app.unsent().latest().0, 1);
    assert_eq!(*summary.latest().0, Ok(Some(Summary { owner: Some("sam".into()), balance: 11 })));
    std::thread::sleep(Duration::from_millis(300));
    assert!(offline.try_get().is_none(), "it waits for the connection, through failed attempts");

    let _server = serve(&memory, &addr);
    assert_eq!(wait(offline).unwrap().reply, Balance { balance: 16 });
    assert_eq!(*app.unsent().latest().0, 0);
    assert_eq!(*connection.latest().0, Connection::Connected);
    eventually(|| matches!(&*summary.latest().0, Ok(Some(s)) if s.balance == 16));
    // The server's 15 and the app's 1 arrive together: the deposit never shows twice.
    assert_eq!(shown.lock().unwrap().last(), Some(&16));
    assert!(!shown.lock().unwrap().contains(&17), "{:?}", shown.lock().unwrap());
    eventually(|| {
        seen.extend(lines.drain());
        seen.iter().filter(|f| f.is_caught_up()).count() == 2
    });
    // Each line once: the feed picked up after the last one it had.
    let cursors: Vec<u64> = seen.iter().filter_map(|f| f.entry()).map(|e| e.cursor.seq()).collect();
    assert_eq!(cursors, [1, 2, 3]);
}

#[test]
fn a_refused_prediction_is_taken_back() {
    let memory = bank();
    let server = serve(&memory, "127.0.0.1:0");
    let addr = server.addr().to_string();
    let app = Client::<BankApi>::connect(&server.url(), Some("sam"));
    wait(app.send(&sam(), OpenAccount { owner: "sam".into() })).unwrap();
    wait(app.send(&sam(), DepositMoney { amount: 100 })).unwrap();
    let summary = app.view::<AccountSummary>().live(&sam());
    let balance = || match &*summary.latest().0 {
        Ok(Some(s)) => s.balance,
        other => panic!("{other:?}"),
    };
    eventually(|| matches!(&*summary.latest().0, Ok(Some(s)) if s.balance == 100));

    // Frozen while the app is offline; the app doesn't know.
    server.stop();
    eventually(|| matches!(*app.connection().latest().0, Connection::Reconnecting { .. }));
    memory.call(move |m| m.send(&sam(), FreezeAccount).map(drop)).unwrap().unwrap();
    let withdraw = app.send(&sam(), WithdrawMoney { amount: 30 });
    assert_eq!(balance(), 70, "shown at once");
    let overdraft = app.send(&sam(), WithdrawMoney { amount: 500 });
    assert_eq!(balance(), 70, "an overdraft isn't predicted");

    let _server = serve(&memory, &addr);
    assert!(matches!(wait(withdraw), Err(SendError::Rejected(r)) if r == Frozen.into()));
    assert!(matches!(wait(overdraft), Err(SendError::Rejected(_))));
    eventually(|| balance() == 100);
}

#[test]
fn a_command_cut_off_before_its_answer_is_sent_again_and_takes_effect_once() {
    let memory = bank();
    memory.call(move |m| m.send(&sam(), OpenAccount { owner: "sam".into() }).map(drop)).unwrap().unwrap();
    // The first server commits the deposit, then the connection drops before the answer.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let committing = memory.clone();
    let cut_off = std::thread::spawn(move || {
        use domain::wire::{Op, Request};
        use domain_websocket::Message;
        let (stream, _) = listener.accept().unwrap();
        let ((), mut reader, writer) = domain_websocket::accept(stream, |_| Ok(())).unwrap();
        loop {
            let Ok(Message::Text(text)) = reader.read() else { continue };
            if let Op::Send { input, command_id, .. } = serde_json::from_str::<Request>(&text).unwrap().op {
                let deposit: DepositMoney = serde_json::from_value(input).unwrap();
                committing.call(move |m| m.send_with_id(&sam(), deposit, command_id).map(drop)).unwrap().unwrap();
                return writer.shutdown();
            }
        }
    });

    let app = Client::<BankApi>::connect(&format!("ws://{addr}/"), Some("sam"));
    let deposit = app.send(&sam(), DepositMoney { amount: 10 });
    cut_off.join().unwrap();
    let _server = serve(&memory, &addr);
    assert_eq!(wait(deposit).unwrap().reply, Balance { balance: 10 }, "the first receipt");
    assert_eq!(memory.call(|m| m.events(&sam()).len()).unwrap(), 2, "deposited once");
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
