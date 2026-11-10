# domain

Business logic as commands, aggregates and events, in Rust. A port of [Alchemy Domain](https://alchemy.run/domain/).

```rust
let bank = Memory::new();
bank.send(&sam, OpenAccount { owner: "sam".into() })?;
let receipt = bank.send(&sam, DepositMoney { amount: 100 })?;
assert_eq!(receipt.reply.balance, 100);
```

```sh
cargo run -p bank     # the example in examples/bank
cargo test            # stories, host tests, compile-time checks
```

Read the [docs](docs/README.md).
