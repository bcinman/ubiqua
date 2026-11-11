//! Sending commands in memory: what every command costs before storage or the network, alone and
//! with the views that read it, and how it holds up across many accounts and threads.

mod common;

use bank::*;
use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;

const THREADS: u64 = 4;

fn send(c: &mut Criterion) {
    let mut group = c.benchmark_group("send");
    group.throughput(Throughput::Elements(1));

    // Decide, evolve and commit, with nothing reading the account.
    group.bench_function("deposit", |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || {
                    let bank = memory();
                    open(&bank, 1, 0);
                    bank
                },
                |bank, range| {
                    let id = account(0);
                    for _ in range {
                        black_box(bank.send(&id, DepositMoney { amount: 1 }).unwrap());
                    }
                },
            )
        })
    });

    // The same, delivered to the bank's three views (one of them reads another) before it returns.
    group.bench_function("deposit_with_views", |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || {
                    let bank = memory().with::<BankViews>();
                    open(&bank, 1, 0);
                    bank
                },
                |bank, range| {
                    let id = account(0);
                    for _ in range {
                        black_box(bank.send(&id, DepositMoney { amount: 1 }).unwrap());
                    }
                },
            )
        })
    });

    // Finding one account among many, and its key in each view.
    group.bench_function("deposit_with_views_across_10k_accounts", |b| {
        let accounts = 10_000;
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || {
                    let bank = memory().with::<BankViews>();
                    open(&bank, accounts, 0);
                    let ids: Vec<_> = (0..accounts).map(account).collect();
                    (bank, ids)
                },
                |(bank, ids), range| {
                    for i in range {
                        let id = &ids[(i % accounts) as usize];
                        black_box(bank.send(id, DepositMoney { amount: 1 }).unwrap());
                    }
                },
            )
        })
    });

    // A refusal records nothing, so it should cost little more than deciding.
    group.bench_function("rejected", |b| {
        let bank = memory().with::<BankViews>();
        open(&bank, 1, 100);
        let id = account(0);
        b.iter(|| black_box(bank.send(&id, WithdrawMoney { amount: 500 }).unwrap_err()))
    });

    // A retry under a command id already used returns the first receipt without deciding again.
    group.bench_function("retry", |b| {
        let bank = memory().with::<BankViews>();
        open(&bank, 1, 0);
        let id = account(0);
        bank.send_with_id(&id, DepositMoney { amount: 1 }, "req-1").unwrap();
        b.iter(|| black_box(bank.send_with_id(&id, DepositMoney { amount: 1 }, "req-1").unwrap()))
    });

    group.finish();
}

/// Commands from several threads at once, with and without the bank's views. Different accounts
/// should run in parallel; one account takes its commands one at a time. Throughput is commands
/// per second across all threads; compare it with `send/deposit` and `send/deposit_with_views`.
fn send_parallel(c: &mut Criterion) {
    let mut group = c.benchmark_group("send_parallel");
    group.throughput(Throughput::Elements(1));

    for views in [false, true] {
        for (name, same_account) in [("own_account_per_thread", false), ("same_account", true)] {
            let name = if views { format!("{name}_with_views") } else { name.to_string() };
            group.bench_function(format!("{THREADS}_threads/{name}"), |b| {
                b.iter_custom(|iters| {
                    chunked(
                        iters,
                        CHUNK,
                        || {
                            let bank = if views { memory().with::<BankViews>() } else { memory() };
                            open(&bank, THREADS, 0);
                            bank
                        },
                        |bank, range| {
                            let each = range.end.saturating_sub(range.start).div_ceil(THREADS);
                            std::thread::scope(|s| {
                                for t in 0..THREADS {
                                    let id = account(if same_account { 0 } else { t });
                                    s.spawn(move || {
                                        for _ in 0..each {
                                            black_box(bank.send(&id, DepositMoney { amount: 1 }).unwrap());
                                        }
                                    });
                                }
                            });
                        },
                    )
                })
            });
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = send, send_parallel
}
criterion_main!(benches);
