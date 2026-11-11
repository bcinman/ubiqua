//! What the benchmarks share: the bank on a fixed clock, ways to fill it, and a timer that starts
//! from a fresh bank every few thousand commands, so memory stays bounded however many iterations
//! criterion asks for and every sample measures the same history sizes.

#![allow(dead_code)]

use bank::*;
use criterion::Criterion;
use domain::{Answer, Answered, Timestamp};
use domain_memory::Memory;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

/// Commands per fresh bank in [`chunked`]: the most history one account reaches in a sample.
pub const CHUNK: u64 = 10_000;

/// Every command is decided at this time, so runs don't depend on the clock.
pub const NOW: Timestamp = Timestamp::from_millis(1_762_819_200_000);

pub fn config() -> Criterion {
    Criterion::default().warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3))
}

pub fn memory() -> Memory {
    Memory::with_clock(|| NOW)
}

/// The bank's views without its policies and saga, so a benchmark "with views" measures only them.
#[domain::model(aggregates(Account, Transfer), views(AccountSummary, OwnerHoldings, Statement))]
pub struct BankViews;

/// The whole bank as the app runs it: views, policies and the transfer saga, with a fraud
/// scorer that finds every withdrawal safe, so no account freezes.
pub fn app_bank() -> Memory {
    memory().with::<Bank>().external(FraudCheck, Safe)
}

pub struct Safe;

impl Answer<ScoreWithdrawal> for Safe {
    async fn answer(&self, _: ScoreWithdrawal) -> Answered<ScoreWithdrawal> {
        Ok(Risk(0))
    }
}

pub fn account(n: u64) -> AccountId {
    AccountId::new(format!("acct-{n}"))
}

/// Accounts are held in pairs, like a main account and savings.
pub fn owner(n: u64) -> String {
    format!("owner-{}", n / 2)
}

/// Opens accounts `0..n` and deposits `balance` into each.
pub fn open(bank: &Memory, n: u64, balance: u64) {
    for i in 0..n {
        bank.send(&account(i), OpenAccount { owner: owner(i) }).unwrap();
        if balance > 0 {
            bank.send(&account(i), DepositMoney { amount: balance }).unwrap();
        }
    }
}

/// Imports `events` events spread over `accounts` accounts, as history: an opening, then deposits.
pub fn import(bank: &Memory, accounts: u64, events: u64) {
    let per_account = events / accounts;
    for i in 0..accounts {
        let opened = AccountEvent::from(AccountOpened { owner: owner(i) });
        let deposits = (1..per_account).map(|n| AccountEvent::from(MoneyDeposited { amount: 1, balance_after: n }));
        bank.append(&account(i), std::iter::once(opened).chain(deposits).collect::<Vec<_>>());
    }
}

/// Times `run` over `iters` operations, in chunks of at most `chunk`, each on a fresh `setup()`.
/// Setting up and dropping are not timed.
pub fn chunked<S>(
    iters: u64,
    chunk: u64,
    mut setup: impl FnMut() -> S,
    mut run: impl FnMut(&S, std::ops::Range<u64>),
) -> Duration {
    let mut total = Duration::ZERO;
    let mut done = 0;
    while done < iters {
        let n = chunk.min(iters - done);
        let state = setup();
        let start = Instant::now();
        run(&state, done..done + n);
        total += start.elapsed();
        drop(state);
        done += n;
    }
    total
}

/// Waits for a `Pending` or other future on this thread, parking until it's woken.
pub fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        std::thread::park();
    }
}

/// A directory for database files, deleted when dropped.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("bank-bench-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
