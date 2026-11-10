//! `cargo run -p bank`: opens an account, moves money, and shows a refusal.

use bank::*;
use domain_memory::Memory;

fn main() {
    let bank = Memory::new();
    let sam = AccountId::new("sam-main");

    bank.send(&sam, OpenAccount { owner: "sam".into() }).unwrap();

    let receipt = bank.send(&sam, DepositMoney { amount: 100 }).unwrap();
    println!("deposited 100, balance {}", receipt.reply.balance);

    let receipt = bank.send(&sam, WithdrawMoney { amount: 30 }).unwrap();
    println!("withdrew 30, balance {}", receipt.reply.balance);

    match bank.send(&sam, WithdrawMoney { amount: 500 }) {
        Err(error) => println!("withdraw 500: {error}"),
        Ok(_) => unreachable!("the account only holds 70"),
    }

    for envelope in bank.events(&sam) {
        println!("{} #{}: {:?}", envelope.stream, envelope.seq, envelope.event);
    }
}
