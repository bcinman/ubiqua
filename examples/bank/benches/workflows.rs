//! Work that spans more than one command: a policy reacting to an event through an external call,
//! and a transfer's saga, debiting one account and crediting another.

mod common;

use bank::*;
use common::*;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use domain_memory::{Memory, SagaStatus};
use std::hint::black_box;
use std::rc::Rc;

/// Commands per fresh bank: each transfer adds a saga run and five events.
const WORKFLOW_CHUNK: u64 = 1_000;

/// The whole bank, with two accounts holding plenty: its views, both policies and the saga.
fn bank() -> Rc<Memory> {
    let bank = Rc::new(app_bank());
    open(&bank, 2, 1_000_000_000);
    bank
}

fn workflows(c: &mut Criterion) {
    let mut group = c.benchmark_group("workflows");
    group.throughput(Throughput::Elements(1));

    let request = |n: u64| RequestTransfer { from: account(0), to: account(1), amount: 1 + n % 7 };

    // Check once that one `run_due` finishes the transfer, so the benchmark times all of it.
    let check = bank();
    let t = TransferId::new("check");
    check.send(&t, request(0)).unwrap();
    check.run_due();
    assert_eq!(check.saga_status::<ExecuteTransfer>(&t), SagaStatus::Completed);

    // Request, then the policy starts the saga, which debits, credits and completes.
    group.bench_function("transfer", |b| {
        b.iter_custom(|iters| {
            chunked(iters, WORKFLOW_CHUNK, bank, |bank, range| {
                for n in range {
                    bank.send(&TransferId::new(format!("t-{n}")), request(n)).unwrap();
                    black_box(bank.run_due());
                }
            })
        })
    });

    // A large withdrawal, then the fraud review scores it through its external system.
    let id = account(0);
    let withdraw = WithdrawMoney { amount: SCORED_FROM };
    let check = bank();
    check.send(&id, withdraw.clone()).unwrap();
    check.run_due();
    assert!(check.failures().is_empty(), "{:?}", check.failures());
    group.bench_function("scored_withdrawal", |b| {
        b.iter_custom(|iters| {
            chunked(iters, CHUNK, bank, |bank, range| {
                for _ in range {
                    bank.send(&id, withdraw.clone()).unwrap();
                    black_box(bank.run_due());
                }
            })
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config();
    targets = workflows
}
criterion_main!(benches);
