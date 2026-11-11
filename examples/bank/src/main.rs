//! `cargo run -p bank`: an app's view of a bank. It shows a live summary, sends commands the way
//! button handlers would, and checks results once per frame, as a UI loop does.
//!
//! `cargo run -p bank -- bank.db` keeps the bank in that file. Run it again and it carries on.

use bank::*;
use domain_client::Client;
use domain_memory::Memory;
use domain_sqlite::Sqlite;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    let memory = match std::env::args().nth(1) {
        Some(path) => Memory::new().persist_to(Sqlite::open(&path).expect("open the database")),
        None => Memory::new(),
    };
    // The model runs on its own thread; the app talks to it through a client.
    let client = Client::<BankApi>::in_process(Arc::new(memory.with::<Bank>()));
    let sam = AccountId::new("sam-main");

    // Redraw whenever the summary changes.
    let summary = client.view::<AccountSummary>().live(&sam);
    println!("showing: {:?}", summary.latest().0);
    let _redraw = summary.on_change(|summary, version| println!("redraw #{version}: {summary:?}"));
    let statement = client.view::<Statement>();
    let seen = statement.snapshot(&sam).try_get().unwrap().unwrap().cursor;

    // Button handlers: send and move on. The account is already open on a second run.
    client.send(&sam, OpenAccount { owner: "sam".into() });
    client.send(&sam, DepositMoney { amount: 100 });
    client.send(&sam, WithdrawMoney { amount: 30 });
    let big = client.send(&sam, WithdrawMoney { amount: 500 });

    // The frame loop checks what it's waiting on.
    let sent = loop {
        if let Some(sent) = big.try_get() {
            break sent;
        }
        std::thread::sleep(Duration::from_millis(16));
    };
    match sent {
        Ok(receipt) => println!("withdrew 500, balance {}", receipt.reply.balance),
        Err(refusal) => println!("withdraw 500: {refusal}"),
    }

    // The statement lines this run added.
    for line in statement.list(&sam, seen, 10).try_get().unwrap().unwrap() {
        println!("statement #{}: {:?}", line.cursor.seq(), line.event);
    }
}
