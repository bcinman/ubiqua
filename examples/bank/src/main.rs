//! `cargo run -p bank`: an app's view of a bank. It shows a live summary, sends commands the way
//! button handlers would, and checks results once per frame, as a UI loop does.

use bank::*;
use domain::Cursor;
use domain_client::Client;
use domain_memory::Memory;
use std::sync::Arc;
use std::time::Duration;

fn main() {
    // The model runs on its own thread; the app talks to it through a client.
    let client = Client::<BankApi>::in_process(Arc::new(Memory::new().with::<Bank>()));
    let sam = AccountId::new("sam-main");

    // Redraw whenever the summary changes.
    let summary = client.view::<AccountSummary>().live(&sam);
    let _redraw = summary.on_change(|summary, version| println!("redraw #{version}: {summary:?}"));

    // Button handlers: send and move on.
    client.send(&sam, OpenAccount { owner: "sam".into() });
    client.send(&sam, DepositMoney { amount: 100 });
    client.send(&sam, WithdrawMoney { amount: 30 });
    let too_much = client.send(&sam, WithdrawMoney { amount: 500 });

    // The frame loop checks what it's waiting on.
    let refusal = loop {
        if let Some(sent) = too_much.try_get() {
            break sent.expect_err("the account only holds 70");
        }
        std::thread::sleep(Duration::from_millis(16));
    };
    println!("withdraw 500: {refusal}");

    let (latest, version) = summary.latest();
    println!("showing #{version}: {latest:?}");

    let statement = client.view::<Statement>().list(&sam, Cursor::START, 10).try_get().unwrap().unwrap();
    for line in statement {
        println!("statement #{}: {:?}", line.cursor.seq(), line.event);
    }
}
