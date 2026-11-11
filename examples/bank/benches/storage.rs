//! Keeping the bank in SQLite: what a durable command costs, how long the first command to an
//! account waits to read its history, and how long a restart takes with views stored or rebuilt.
//!
//! These write real files in the system's temporary directory, so they measure that disk too.
//! Compare them on the same machine only.

mod common;

use bank::*;
use common::*;
use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use domain_memory::Memory;
use domain_sqlite::Sqlite;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Commands per fresh database.
const DB_CHUNK: u64 = 1_000;

fn persisted(path: &Path) -> Memory {
    memory().persist_to(Sqlite::open(path).unwrap())
}

/// A path no other benchmark iteration has used.
fn fresh(dir: &TempDir) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let path = dir.path().join(format!("{}.db", N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_file(&path);
    path
}

/// Removes a database and the files SQLite keeps beside it.
fn remove(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

fn send(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite/send");
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);
    let dir = TempDir::new("send");

    // The events and receipt in one transaction; with views, each view saves what it applied too.
    for (name, views) in [("deposit", false), ("deposit_with_views", true)] {
        group.bench_function(name, |b| {
            b.iter_custom(|iters| {
                chunked(
                    iters,
                    DB_CHUNK,
                    || {
                        let path = fresh(&dir);
                        let bank = persisted(&path);
                        let bank = if views { bank.with::<BankViews>() } else { bank };
                        open(&bank, 1, 0);
                        Cleanup(path, Some(bank))
                    },
                    |Cleanup(_, bank), range| {
                        let bank = bank.as_ref().unwrap();
                        let id = account(0);
                        for _ in range {
                            black_box(bank.send(&id, DepositMoney { amount: 1 }).unwrap());
                        }
                    },
                )
            })
        });
    }
    group.finish();
}

/// The first read of an account after a restart reads its events and evolves its state.
/// Throughput is events read per second.
fn first_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite/first_read");
    group.sample_size(10).measurement_time(Duration::from_secs(5));
    let dir = TempDir::new("first-read");
    for events in [100, 10_000] {
        let path = dir.path().join(format!("account-{events}.db"));
        import(&persisted(&path), 1, events);
        group.throughput(Throughput::Elements(events));
        group.bench_function(format!("account_with_{events}_events"), |b| {
            let id = account(0);
            b.iter_batched_ref(|| persisted(&path), |bank| black_box(bank.state(&id)), BatchSize::PerIteration)
        });
    }
    group.finish();
}

/// Opening a bank of 10,000 events over 100 accounts with its views: loading what they stored, or
/// rebuilding them from every event when none are stored, as after a version change.
fn restart(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite/restart");
    group.sample_size(10).measurement_time(Duration::from_secs(20));
    let dir = TempDir::new("restart");
    let events = 10_000;
    for (name, stored_views) in [("load_views", true), ("rebuild_views", false)] {
        let template = dir.path().join(format!("{name}.db"));
        {
            let bank = persisted(&template);
            let bank = if stored_views { bank.with::<BankViews>() } else { bank };
            import(&bank, 100, events);
        }
        group.bench_function(format!("{name}/{events}_events_100_accounts"), |b| {
            b.iter_batched(
                || {
                    let path = fresh(&dir);
                    std::fs::copy(&template, &path).unwrap();
                    path
                },
                |path| {
                    let bank = persisted(&path).with::<BankViews>();
                    Cleanup(path, Some(bank))
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}

/// A bank and its database file, deleted once the bank is dropped.
struct Cleanup(std::path::PathBuf, Option<Memory>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        drop(self.1.take());
        remove(&self.0);
    }
}

criterion_group! {
    name = benches;
    config = config();
    targets = send, first_read, restart
}
criterion_main!(benches);
