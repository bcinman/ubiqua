//! Reading views, and rebuilding them from the events: what a read costs, and how long a view
//! added to existing history (or changed, so it rebuilds) takes to catch up.

mod common;

use bank::*;
use common::*;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use domain::Cursor;
use std::hint::black_box;
use std::time::Duration;

fn read(c: &mut Criterion) {
    let mut group = c.benchmark_group("read");
    let accounts = 10_000;

    let bank = memory().with::<BankViews>();
    open(&bank, accounts, 100);
    let ids: Vec<_> = (0..accounts).map(account).collect();
    let summaries = bank.view::<AccountSummary>();
    let mut i = 0;
    group.bench_function("summary_among_10k_accounts", |b| {
        b.iter(|| {
            i = (i + 1) % accounts as usize;
            black_box(summaries.get(&ids[i]).unwrap())
        })
    });

    // The latest page of a long statement.
    let id = account(0);
    for _ in 0..1000 {
        bank.send(&id, DepositMoney { amount: 1 }).unwrap();
    }
    let statement = bank.view::<Statement>();
    group.bench_function("statement_page_of_10_after_1000_lines", |b| {
        b.iter(|| black_box(statement.list(&id, Cursor::new(990), 10)))
    });
    group.finish();
}

/// The bank's views built from history already recorded, as when a view is added or its version
/// changes. Throughput is events replayed per second.
fn replay(c: &mut Criterion) {
    let mut group = c.benchmark_group("replay");
    group.sample_size(10).measurement_time(Duration::from_secs(10));
    for events in [1_000, 10_000] {
        group.throughput(Throughput::Elements(events));
        group.bench_function(format!("bank_views/{events}_events_100_accounts"), |b| {
            b.iter_batched(
                || {
                    let bank = memory();
                    import(&bank, 100, events);
                    bank
                },
                |bank| bank.with::<BankViews>(),
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = read, replay
}
criterion_main!(benches);
