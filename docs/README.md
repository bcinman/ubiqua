# domain

Business logic as commands, aggregates and events, in Rust. A port of [Alchemy Domain](https://alchemy.run/domain/).

```rust
let bank = Memory::new();
let sam = AccountId::new("sam-main");

bank.send(&sam, OpenAccount { owner: "sam".into() })?;
let receipt = bank.send(&sam, DepositMoney { amount: 100 })?;
assert_eq!(receipt.reply.balance, 100);

let refused = bank.send(&sam, WithdrawMoney { amount: 500 });
// Err(Rejected(InsufficientFunds { balance: 100, requested: 500 }))
```

```diagram
command ──▶ aggregate ──▶ events ──▶ views
   ▲         decide        recorded,   answer reads:
   │         evolve        never       get, list,
   │                       changed     live feeds
   │                         │
   └────── policies ◀────────┘
           call outside systems, schedule for later
```

- A **command** asks for a change: `WithdrawMoney { amount: 30 }`.
- An **aggregate** decides it against its state and returns **events** or a **rejection**.
- **Events** are the record. The state is rebuilt from them.
- A **view** folds events into what people read: a balance, a statement.
- A **policy** reacts to events with more commands, now or later.

## Guides

1. [Getting started](getting-started.md): build the bank in `examples/bank`.
2. [Commands and events](commands.md): declare what a command asks for and how it can end.
3. [Aggregates](aggregates.md): state, `evolve`, `decide`, `reply`.
4. [Views](views.md): read models, live feeds, reading your own write.
5. [Running a model](running.md): `Memory`, retries, threads, the web.
6. [Keep it on disk](storage.md): SQLite, restarts, changing a view.
7. [The client](client.md): what an app uses: send, read, follow live, with any UI.
8. [Policies](policies.md): react to events, call outside systems, schedule for later.
9. [Stories](stories.md): test rules as given, when, then.
10. [Compile-time checks](compile-time-checks.md): the mistakes that don't build.

## Crates

| Crate | What it holds |
| --- | --- |
| `domain` | traits, derives, `Receipt`, `Envelope`, the read protocol. No runtime. |
| `domain-memory` | `Memory`: runs a model, its views and policies in memory. |
| `domain-sqlite` | `Sqlite`: keeps a `Memory` in a file. |
| `domain-client` | `Client`, `Pending`, `LiveView`: what an app uses. |
| `domain-story` | `Story`: given, when, then. |

No UI framework and no async runtime. Everything except `domain-sqlite` builds for `wasm32-unknown-unknown`.

## Not yet

A server and a client that talks to it, projections, sagas. See the design sketch's build order.
