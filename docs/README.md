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
command ──▶ aggregate ──▶ events
             decide        recorded, never changed
             evolve        state is a fold of them
```

- A **command** asks for a change: `WithdrawMoney { amount: 30 }`.
- An **aggregate** decides it against its state and returns **events** or a **rejection**.
- **Events** are the record. The state is rebuilt from them.

## Guides

1. [Getting started](getting-started.md): build the bank in `examples/bank`.
2. [Commands and events](commands.md): declare what a command asks for and how it can end.
3. [Aggregates](aggregates.md): state, `evolve`, `decide`, `reply`.
4. [Running a model](running.md): `Memory`, retries, threads, the web.
5. [Stories](stories.md): test rules as given, when, then.
6. [Compile-time checks](compile-time-checks.md): the mistakes that don't build.

## Crates

| Crate | What it holds |
| --- | --- |
| `domain` | traits, derives, `Receipt`, `Envelope`. No runtime. |
| `domain-memory` | `Memory`: runs a model in memory. |
| `domain-story` | `Story`: given, when, then. |

No UI framework and no async runtime. Everything builds for `wasm32-unknown-unknown`.

## Not yet

Views, policies, projections, sagas, persistence, the client and server. See the design sketch's build order.
