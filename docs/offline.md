# Offline and instant

Over a connection, a command takes a round trip. Two things hide it: the app shows what a command will do as soon as it's sent, and a command sent while offline waits and goes out on reconnect.

```text
tap Deposit 100      balance 70 → 170     shown at once, predicted
                     … offline …
reconnected          balance 170          confirmed, the same 100, counted once
```

## Predict a view

List the commands a view predicts, and say how each would change it. From [`examples/bank/src/views.rs`](../examples/bank/src/views.rs):

```rust
#[domain::view(
    key = AccountId,
    from(Account(AccountOpened, MoneyDeposited, MoneyWithdrawn)),
    state = Summary,
    rejects(NotOwner),
    predicts(DepositMoney, WithdrawMoney)
)]
pub struct AccountSummary;

impl Predict<DepositMoney> for AccountSummary {
    fn predict(account: &AccountId, summary: &mut Option<Summary>, to: &AccountId, cmd: &DepositMoney) {
        if let Some(summary) = summary.as_mut().filter(|_| account == to) {
            summary.balance += cmd.amount;
        }
    }
}

impl Predict<WithdrawMoney> for AccountSummary {
    fn predict(account: &AccountId, summary: &mut Option<Summary>, from: &AccountId, cmd: &WithdrawMoney) {
        // An overdraft will be refused, so it changes nothing.
        if let Some(summary) = summary.as_mut().filter(|s| account == from && s.balance >= cmd.amount) {
            summary.balance -= cmd.amount;
        }
    }
}
```

| `predict(..)` gets | |
| --- | --- |
| `account` | the key of the live view being drawn |
| `summary` | its state; `None` before its first event |
| `to`, `cmd` | where the command goes, and the command |

A predicted command derives `Clone`. Nothing else changes: the app sends as before, and every `live(..)` of the view shows the prediction.

```rust
let summary = client.view::<AccountSummary>().live(&sam);   // balance 70
client.send(&sam, DepositMoney { amount: 100 });            // balance 170, before the answer
```

## When the answer comes

| The answer | The live view |
| --- | --- |
| it committed | shows the server's state, which has the command in it |
| it was refused | drops the prediction: back to the server's state |

```text
redraw #4: Ok(Some(Summary { .., balance: 20 }))
redraw #5: Ok(Some(Summary { .., balance: 10 }))      withdraw 10, predicted
redraw #6: Ok(Some(Summary { .., balance: 20 }))      rejected: Frozen
```

A change never shows twice. While a command waits, the view holds back newer states from the server, and when the answer arrives it shows them in one step.

Predictions are only for drawing. `get` and `list` return the server's state, and so does `decide`.

## Send while offline

A connected client keeps every command until it's answered. If the connection drops, the command goes out again on reconnect, under the same command id, so it happens once.

```rust
let deposit = client.send(&sam, DepositMoney { amount: 1 });   // offline: it waits
client.unsent().latest().0                                     // 1
// … reconnected …
deposit.try_get()                                              // Some(Ok(Receipt { reply: Balance { balance: 16 }, .. }))
client.unsent().latest().0                                     // 0
```

```rust
let _badge = client.unsent().on_change(|n, _| show_sync_badge(**n));    // a LiveView<usize>
```

| | While offline |
| --- | --- |
| `send` | waits, then resolves with the server's answer |
| `get`, `list`, `snapshot` | fail with `Unavailable` |
| `live`, `events` | keep their last value, then catch up |

The server still decides. A command sent offline can be refused once it arrives, say because the account was frozen meanwhile, so the app must show refusals that come late.

Unsent commands live in memory: quitting the app drops them.

## Test it

[`examples/bank/tests/server.rs`](../examples/bank/tests/server.rs) stops the server, sends, and starts it again:

```rust
server.stop();
memory.send(&sam(), FreezeAccount).unwrap();                    // the app doesn't know
let withdraw = app.send(&sam(), WithdrawMoney { amount: 30 });
assert_eq!(balance(), 70, "shown at once");

let _server = serve(&memory, &addr);
assert!(matches!(wait(withdraw), Err(SendError::Rejected(r)) if r == Frozen.into()));
eventually(|| balance() == 100);
```

Next: [Stories](stories.md).
