//! `cargo run -p bank`: opens an account, moves money, shows a refusal, and reads the views.

use bank::*;
use domain::{Cursor, Frame};
use domain_memory::Memory;

fn main() {
    let bank = Memory::new().with::<Bank>();
    let sam = AccountId::new("sam-main");
    let savings = AccountId::new("sam-savings");

    // Follow the statement live, from the start.
    let statement = bank.view::<Statement>().events(&sam, Cursor::START);

    bank.send(&sam, OpenAccount { owner: "sam".into() }).unwrap();

    let receipt = bank.send(&sam, DepositMoney { amount: 100 }).unwrap();
    println!("deposited 100, balance {}", receipt.reply.balance);

    let receipt = bank.send(&sam, WithdrawMoney { amount: 30 }).unwrap();
    println!("withdrew 30, balance {}", receipt.reply.balance);

    match bank.send(&sam, WithdrawMoney { amount: 500 }) {
        Err(error) => println!("withdraw 500: {error}"),
        Ok(_) => unreachable!("the account only holds 70"),
    }

    // Read your own write: the summary has applied the withdrawal.
    let summary = bank.view::<AccountSummary>().at_least(&receipt).unwrap().get(&sam).unwrap();
    println!("summary: {summary:?}");

    bank.send(&savings, OpenAccount { owner: "sam".into() }).unwrap();
    bank.send(&savings, DepositMoney { amount: 500 }).unwrap();
    let holdings = bank.view::<OwnerHoldings>().get(&OwnerId::new("sam")).unwrap();
    println!("sam holds {} across {:?}", holdings.total(), holdings.balances);

    for frame in statement.drain() {
        match frame {
            Frame::CaughtUp { cursor } => println!("statement caught up at {}", cursor.seq()),
            Frame::Entry(line) => println!("statement #{}: {:?}", line.cursor.seq(), line.event),
        }
    }
}
