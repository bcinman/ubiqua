# Compile-time checks

These mistakes don't build. Each one is a `compile_fail` doctest in [`compile_checks.rs`](../crates/domain/src/compile_checks.rs), so `cargo test` fails if one starts compiling.

## An event the command doesn't declare

```rust
impl Decide<DepositMoney> for Account {
    fn decide(..) -> Decision<DepositMoney> {
        Ok(vec![MoneyWithdrawn { .. }.into()])
    }
}
```

```text
error[E0277]: the trait bound `DepositMoneyEvent: From<MoneyWithdrawn>` is not satisfied
```

Fix: add it to `#[command(events(..))]`, or record the right event.

## A rejection the command doesn't declare

```rust
Err(InsufficientFunds { .. }.into())   // in Decide<DepositMoney>
```

```text
error[E0277]: the trait bound `DepositMoneyRejection: From<InsufficientFunds>` is not satisfied
```

## A command listed but not decided

```rust
#[aggregate(.., commands(Close))]
pub struct Door;
// no impl Decide<Close> for Door
```

```text
error[E0277]: `Door` doesn't handle `Close`
  needs `impl Decide<Close> for Door`, with every event of `Close` in its `events(..)`
```

## A command's event missing from the aggregate

```rust
#[command(target = DoorId, events(Closed))]
pub struct Close;

#[aggregate(id = DoorId, events(Opened), commands(Close))]   // no Closed
pub struct Door;
```

```text
error[E0277]: the trait bound `DoorEvent: From<Closed>` is not satisfied
```

## No `Evolve`

```text
error[E0277]: `Door` has no `impl Evolve`
```

## A command sent to the wrong kind of id

```rust
memory.send(&CustomerId::new("c-1"), DepositMoney { amount: 1 });
```

```text
error[E0271]: type mismatch resolving `<DepositMoney as Command>::Target == CustomerId`
```

## A view reads an event its source never records

```rust
#[view(key = DoorId, from(Door(Opened)))]   // Door records Closed only
pub struct Log;
```

```text
error[E0277]: `Opened` isn't one of the events in `DoorEvent`
  this source doesn't record `Opened`
```

## A view reads an event its source view doesn't emit

```rust
#[view(key = DoorId, from(Door(Closed)))]   // emits Closed only
pub struct Closings;

#[view(key = DoorId, from(Closings(Opened)))]
pub struct Log;
```

```text
error[E0277]: `Opened` isn't one of the events in `ClosingsInput`
```

## A view keyed by something else, without `key_of`

```rust
#[view(key = HouseId, from(Door(Closed)))]
pub struct Log;
```

```text
error[E0277]: a `DoorId` source can't key a view keyed by `HouseId`
  list this source in `key_of(..)` and implement `KeyOf` for it
```

## A view without `Apply`

```text
error[E0277]: `Log` has no `impl Apply`
```

## A missing attribute

```rust
#[derive(Command, Debug)]
pub struct Close;
```

```text
error: missing `#[command(..)]`
```

## Not checked yet

Whether `evolve` handles every event is up to your `match`. A `_ => {}` arm also hides events you forgot.
