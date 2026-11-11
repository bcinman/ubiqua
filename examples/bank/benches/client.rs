//! What an app waits for through a `Client`: a command's receipt and a read, from a domain thread
//! in this process or from a server over a WebSocket on loopback, and how long a change takes to
//! reach someone else watching it. Also how many commands go through when several are in flight
//! at once, from one thread or several, in memory and kept in SQLite.

mod common;

use bank::*;
use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use domain_client::{Client, Pending, Sent};
use domain_memory::{Memory, Runtime};
use domain_server::{Listening, Server};
use domain_sqlite::Sqlite;
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::Duration;

/// Commands an app keeps in flight when it sends without waiting for each answer.
const IN_FLIGHT: usize = 64;

/// Commands per fresh database.
const DB_CHUNK: u64 = 1_000;

/// The bank running on a domain thread of its own, as an app runs it.
fn local(memory: Memory) -> Client<BankApi> {
    Client::in_process(Runtime::spawn(memory))
}

fn serve(memory: Memory) -> Listening {
    Server::<BankApi>::new(Runtime::spawn(memory)).listen("127.0.0.1:0").unwrap()
}

/// The whole bank, opened with accounts `0..accounts`.
fn opened(accounts: u64) -> Memory {
    let bank = app_bank();
    open(&bank, accounts, 0);
    bank
}

/// The same, kept in a fresh SQLite file in `dir`.
fn stored(dir: &TempDir, n: u64, accounts: u64) -> Memory {
    let path = dir.path().join(format!("{n}.db"));
    let bank = memory().persist_to(Sqlite::open(path).unwrap()).with::<Bank>().external(FraudCheck, Safe);
    open(&bank, accounts, 0);
    bank
}

fn deposit(client: &Client<BankApi>, id: &AccountId) -> Pending<Sent<DepositMoney>> {
    client.send(id, DepositMoney { amount: 1 })
}

/// Sends `n` deposits, keeping up to `in_flight` of them unanswered, then waits for the rest.
fn pipelined(client: &Client<BankApi>, id: &AccountId, n: u64, in_flight: usize) {
    let mut waiting: VecDeque<Pending<Sent<DepositMoney>>> = VecDeque::with_capacity(in_flight);
    for _ in 0..n {
        if waiting.len() == in_flight {
            black_box(block_on(waiting.pop_front().unwrap()).unwrap());
        }
        waiting.push_back(deposit(client, id));
    }
    waiting.into_iter().for_each(|sent| drop(black_box(block_on(sent).unwrap())));
}

/// `threads` threads, each with its own client and account, each sending its share pipelined.
fn from_threads(clients: &[Client<BankApi>], range: std::ops::Range<u64>, in_flight: usize) {
    let each = (range.end - range.start).div_ceil(clients.len() as u64);
    std::thread::scope(|s| {
        for (t, client) in clients.iter().enumerate() {
            s.spawn(move || pipelined(client, &account(t as u64), each, in_flight));
        }
    });
}

fn in_process(c: &mut Criterion) {
    let mut group = c.benchmark_group("client/in_process");
    group.throughput(Throughput::Elements(1));
    let id = account(0);

    group.bench_function("send", |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || local(opened(1)),
                |client, range| range.for_each(|_| drop(black_box(block_on(deposit(client, &id)).unwrap()))),
            )
        })
    });

    // An app that sends without waiting for each answer, as a UI's button handlers do.
    group.bench_function(format!("send_{IN_FLIGHT}_in_flight"), |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || local(opened(1)),
                |client, range| pipelined(client, &id, range.end - range.start, IN_FLIGHT),
            )
        })
    });

    // Four threads sharing the bank, each with its own account and commands in flight.
    group.bench_function(format!("send_from_4_threads_{IN_FLIGHT}_in_flight"), |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                CHUNK,
                || {
                    let client = local(opened(4));
                    vec![client.clone(), client.clone(), client.clone(), client]
                },
                |clients, range| from_threads(clients, range, IN_FLIGHT),
            )
        })
    });

    let client = local(opened(1));
    group.bench_function("read", |b| b.iter(|| black_box(block_on(client.view::<AccountSummary>().get(&id)).unwrap())));
    group.finish();
}

/// The bank kept in SQLite, with its views, policies and saga: one command at a time, then with
/// commands in flight, which commit together where the host batches them.
fn sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("client/sqlite");
    group.throughput(Throughput::Elements(1));
    group.sample_size(10).measurement_time(Duration::from_secs(10));
    let dir = TempDir::new("client");
    let id = account(0);
    let mut n = 0;
    let mut fresh = |accounts| {
        n += 1;
        stored(&dir, n, accounts)
    };

    for in_flight in [1, IN_FLIGHT] {
        group.bench_function(format!("send_{in_flight}_in_flight"), |b| {
            b.iter_custom(|iters| {
                chunked(
                    iters,
                    DB_CHUNK,
                    || local(fresh(1)),
                    |client, range| pipelined(client, &id, range.end - range.start, in_flight),
                )
            })
        });
    }

    // Four apps connected to a server, each with commands in flight.
    group.bench_function("websocket_4_clients_16_in_flight", |b| {
        b.iter_custom(|iters| {
            chunked(
                iters,
                DB_CHUNK,
                || {
                    let server = serve(fresh(4));
                    let clients: Vec<_> = (0..4).map(|_| Client::<BankApi>::connect(&server.url(), None)).collect();
                    // Wait for each connection, so the first sample doesn't include it.
                    for (t, client) in clients.iter().enumerate() {
                        block_on(client.view::<AccountSummary>().get(&account(t as u64))).unwrap();
                    }
                    (server, clients)
                },
                |(_, clients), range| from_threads(clients, range, 16),
            )
        })
    });
    group.finish();
}

fn websocket(c: &mut Criterion) {
    let mut group = c.benchmark_group("client/websocket");
    group.throughput(Throughput::Elements(1));
    let id = account(0);

    let server = serve(opened(1));
    let client = Client::<BankApi>::connect(&server.url(), None);
    // Wait for the connection, so the first sample doesn't include it.
    block_on(deposit(&client, &id)).unwrap();

    group.bench_function("send", |b| b.iter(|| black_box(block_on(deposit(&client, &id)).unwrap())));
    group.bench_function("read", |b| b.iter(|| black_box(block_on(client.view::<AccountSummary>().get(&id)).unwrap())));

    // From one client's send until another client's live view shows the new balance.
    let watcher = Client::<BankApi>::connect(&server.url(), None);
    let summary = watcher.view::<AccountSummary>().live(&id);
    let shown = || summary.latest().0.as_ref().as_ref().ok().and_then(|s| s.as_ref().map(|s| s.balance));
    let balance = block_on(client.view::<AccountSummary>().get(&id)).unwrap().map(|s| s.balance);
    let mut seen = summary.latest().1;
    while shown() != balance {
        seen = block_on(summary.changed(seen)).1;
    }
    group.bench_function("change_reaches_another_client", |b| {
        b.iter(|| {
            let seen = summary.latest().1;
            drop(deposit(&client, &id));
            black_box(block_on(summary.changed(seen)))
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = in_process, sqlite, websocket
}
criterion_main!(benches);
