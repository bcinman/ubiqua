//! What an app waits for through a `Client`: a command's receipt and a read, from a domain thread
//! in this process or from a server over a WebSocket on loopback, and how long a change takes to
//! reach someone else watching it.

mod common;

use bank::*;
use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use domain_client::Client;
use domain_server::Server;
use std::hint::black_box;
use std::sync::Arc;

fn in_process(c: &mut Criterion) {
    let mut group = c.benchmark_group("client/in_process");
    group.throughput(Throughput::Elements(1));
    let id = account(0);

    let fresh = || {
        let bank = Arc::new(app_bank());
        open(&bank, 1, 0);
        Client::<BankApi>::in_process(bank)
    };
    group.bench_function("send", |b| {
        b.iter_custom(|iters| {
            chunked(iters, CHUNK, fresh, |client, range| {
                for _ in range {
                    black_box(block_on(client.send(&id, DepositMoney { amount: 1 })).unwrap());
                }
            })
        })
    });

    let client = fresh();
    group.bench_function("read", |b| b.iter(|| black_box(block_on(client.view::<AccountSummary>().get(&id)).unwrap())));
    group.finish();
}

fn websocket(c: &mut Criterion) {
    let mut group = c.benchmark_group("client/websocket");
    group.throughput(Throughput::Elements(1));
    let id = account(0);

    let bank = Arc::new(app_bank());
    open(&bank, 1, 0);
    let server = Server::<BankApi>::new(bank).listen("127.0.0.1:0").unwrap();
    let client = Client::<BankApi>::connect(&server.url(), None);
    // Wait for the connection, so the first sample doesn't include it.
    block_on(client.send(&id, DepositMoney { amount: 1 })).unwrap();

    group.bench_function("send", |b| {
        b.iter(|| black_box(block_on(client.send(&id, DepositMoney { amount: 1 })).unwrap()))
    });
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
            drop(client.send(&id, DepositMoney { amount: 1 }));
            black_box(block_on(summary.changed(seen)))
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = in_process, websocket
}
criterion_main!(benches);
