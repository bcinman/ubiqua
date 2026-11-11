# The client

An app talks to its model through a `Client`. It sends commands, reads views, and follows them live. It doesn't depend on any UI framework or async runtime.

```rust
let client = Client::<BankApi>::in_process(Arc::new(Memory::new().with::<Bank>()));

let summary = client.view::<AccountSummary>().live(&sam);
let _redraw = summary.on_change(|summary, _| redraw(summary));

client.send(&sam, DepositMoney { amount: 100 });
```

```text
redraw: Some(Summary { owner: Some("sam"), balance: 100 })
```

`cargo run -p bank` runs the full example.

## Say what the app may use

```rust
#[derive(Api)]
#[api(commands(OpenAccount, DepositMoney, WithdrawMoney), views(AccountSummary, OwnerHoldings, Statement))]
pub struct BankApi;
```

If something isn't listed, the code doesn't build:

```rust
client.send(&sam, CloseAccount);
```

```text
error[E0277]: `BankApi` doesn't let clients send `CloseAccount`
  add `CloseAccount` to `#[api(commands(..))]`
```

```rust
client.view::<AuditLog>();
```

```text
error[E0277]: `BankApi` doesn't let clients read `AuditLog`
  add `AuditLog` to `#[api(views(..))]`
```

## Start one

| | Commands run on | Use it for |
| --- | --- | --- |
| `Client::in_process(memory)` | a thread of their own, in order | apps: the UI thread never waits |
| `Client::inline(memory)` | the caller's thread | tests, scripts, the web |

On the web there are no threads, so `in_process` behaves like `inline`.

## Send

`send` returns right away. Use the result whichever way suits your UI:

```rust
// Fire and forget.
client.send(&sam, DepositMoney { amount: 100 });

// Check once per frame.
let pending = client.send(&sam, WithdrawMoney { amount: 30 });
if let Some(sent) = pending.try_get() {
    show(sent);
}

// Await it, on any executor.
let receipt = client.send(&sam, WithdrawMoney { amount: 30 }).await?;

// Get a callback. It runs on the domain thread.
client.send_then(&sam, WithdrawMoney { amount: 30 }, move |sent| ui.update(sent));
```

`try_get` returns the result once. After that it returns `None`.

The result is a `Sent<C>`, which is the same `Result<Receipt, SendError>` that [`Memory::send`](running.md#what-send-returns) returns.

## When the outcome is unknown

```rust
Err(SendError::Unavailable { command_id: "memory-7", reason: "the command panicked: .." })
```

The command may or may not have happened. To retry safely, reuse its id:

```rust
let sent = match client.send_with_id(&sam, WithdrawMoney { amount: 30 }, "withdraw-1").await {
    Err(SendError::Unavailable { .. }) => client.send_with_id(&sam, WithdrawMoney { amount: 30 }, "withdraw-1").await,
    other => other,
};
```

Even if both attempts reach the model, the money leaves once. A panic during one command doesn't stop the commands after it.

## Read

```rust
let summaries = client.view::<AccountSummary>();

summaries.get(&sam);                          // Pending<Result<Option<Summary>, ReadError>>
summaries.get_at_least(&sam, &receipt);       // after your own write
summaries.snapshot(&sam);                     // state + cursor
client.view::<Statement>().list(&sam, Cursor::START, 20);
client.view::<Statement>().events(&sam, cursor);   // a Feed, as in Views
```

In the same process, reads are ready at once. A client that talks to a server will have the same methods, so treat every read as `Pending`.

## Follow it live

`live` returns a `LiveView`, which always holds the latest state. It offers three ways to see changes:

```rust
let summary = client.view::<AccountSummary>().live(&sam);

// Read it each frame.
let (latest, version) = summary.latest();

// Get called when it changes. Dropping the listener stops the calls.
let listener = summary.on_change(|latest, version| schedule_redraw());

// Await the next change.
let (latest, version) = summary.changed(version).await;
```

`version` counts changes. It lets you skip work when nothing changed, or wait for the change after one you've seen.

Dropping every clone of a `LiveView` stops its updates.

## With a UI framework

| Your UI | Use |
| --- | --- |
| Redraws every frame (egui, imgui) | `latest()` and `try_get()` in the frame; `on_change` to request a repaint |
| Keeps state and redraws on change (Rune, iced, gpui, Slint) | `on_change` / `send_then`, then hand the value to the UI thread |
| Async (Dioxus, Leptos) | `.await` sends, `changed(v).await` in a task |

For example, in Rune:

```rust
let ui: AppHandle<State> = app.handle();
let _listener = summary.on_change(move |latest, _| {
    let latest = latest.clone();
    ui.update(move |state, _| state.summary = (*latest).clone());
});
```

Callbacks run on the domain thread, so they should only hand work to the UI thread. They may read views and send commands. No lock is held while they run.

Next: [Stories](stories.md).
