//! An app's view of a bank. It shows a live summary, sends commands the way button handlers would,
//! and checks results once per frame, as a UI loop does. The same app code runs three ways:
//!
//! - `cargo run -p bank` runs the bank in this process. Add a path, `cargo run -p bank -- bank.db`,
//!   to keep it in that file; run it again and it carries on.
//! - `cargo run -p bank -- serve 127.0.0.1:7878 [bank.db]` serves the bank to apps elsewhere.
//! - `cargo run -p bank -- join ws://127.0.0.1:7878/ sam` runs the app against that server,
//!   signed in as sam. It stays to show changes; join as sam again elsewhere and watch both.

use bank::*;
use domain::{Answer, Answered, Cursor};
use domain_client::{Client, Listener, LiveView, Pending, Read};
use domain_memory::Memory;
use domain_server::Server;
use domain_sqlite::Sqlite;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        ["serve", addr, ref db @ ..] => {
            // Demo only: the token is the user's name. Check a real session here.
            let server = Server::<BankApi>::new(Arc::new(bank(db.first().copied())))
                .sessions(|token| Some(User(token.to_string())))
                .listen(addr)
                .expect("listen");
            println!("serving {}", server.url());
            server.wait();
        }
        ["join", url, name] => {
            let client = Client::<BankApi>::connect(url, Some(name));
            let _summary = app(&client, name);
            println!("watching; press Ctrl-C to stop");
            loop {
                std::thread::park();
            }
        }
        [] | [_] => {
            // The model runs on its own thread; the app talks to it through a client.
            let client = Client::<BankApi>::in_process(Arc::new(bank(args.first().map(String::as_str))));
            app(&client.acting_as(User("sam".into())), "sam");
        }
        _ => eprintln!("usage: bank [bank.db] | bank serve ADDR [bank.db] | bank join URL NAME"),
    }
}

/// The bank, with its fraud review asking a stand-in scorer.
fn bank(db: Option<&str>) -> Memory {
    let memory = match db {
        Some(path) => Memory::new().persist_to(Sqlite::open(path).expect("open the database")),
        None => Memory::new(),
    };
    memory.with::<Bank>().external(FraudCheck, RuleOfThumb)
}

/// What a UI would do. It doesn't know whether the bank is in this process or on a server.
/// Returns the summary it keeps redrawing, for as long as it's held.
fn app(client: &Client<BankApi>, me: &str) -> (LiveView<Read<AccountSummary>>, Listener<Read<AccountSummary>>) {
    let account = AccountId::new(format!("{me}-main"));

    // Redraw whenever the summary changes.
    let summary = client.view::<AccountSummary>().live(&account);
    let redraw = summary.on_change(|summary, version| println!("redraw #{version}: {summary:?}"));
    let statement = client.view::<Statement>();
    let seen = next_frame(statement.snapshot(&account)).map_or(Cursor::START, |s| s.cursor);

    // Button handlers: send and move on. The account is already open on a second run.
    client.send(&account, OpenAccount { owner: me.into() });
    client.send(&account, DepositMoney { amount: 1000 });
    client.send(&account, WithdrawMoney { amount: 30 });

    // Move some to savings. The transfer finishes on its own; the summary redraws when it does.
    let savings = AccountId::new(format!("{me}-savings"));
    client.send(&savings, OpenAccount { owner: me.into() });
    let transfer = RequestTransfer { from: account.clone(), to: savings, amount: 50 };
    if let Err(refusal) = next_frame(client.send(&TransferId::new(format!("{me}-to-savings")), transfer)) {
        println!("transfer: {refusal}");
    }
    // Scored as risky, so the account freezes for a day and the next withdrawal is refused.
    for amount in [900, 10] {
        match next_frame(client.send(&account, WithdrawMoney { amount })) {
            Ok(receipt) => println!("withdrew {amount}, balance {}", receipt.reply.balance),
            Err(refusal) => println!("withdraw {amount}: {refusal}"),
        }
    }

    // The statement lines this run added.
    for line in next_frame(statement.list(&account, seen, 10)).unwrap_or_default() {
        println!("statement #{}: {:?}", line.cursor.seq(), line.event);
    }
    (summary, redraw)
}

/// The frame loop checks what it's waiting on, once per frame.
fn next_frame<T>(pending: Pending<T>) -> T {
    loop {
        if let Some(value) = pending.try_get() {
            return value;
        }
        std::thread::sleep(Duration::from_millis(16));
    }
}

/// Stands in for the fraud vendor: the bigger the withdrawal, the riskier.
struct RuleOfThumb;

impl Answer<ScoreWithdrawal> for RuleOfThumb {
    async fn answer(&self, call: ScoreWithdrawal) -> Answered<ScoreWithdrawal> {
        Ok(Risk((call.amount / 10).min(100) as u8))
    }
}
