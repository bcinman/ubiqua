# The client

An app talks to its model through a `Client`. It sends commands, reads views, and follows them live. It doesn't depend on any UI framework or async runtime.

```rust
let client = Client::<BankApi>::in_process(Runtime::spawn(|| Memory::new().with::<Bank>()));

let summary = client.view::<AccountSummary>().live(&sam);
let _redraw = summary.on_change(|summary, _| redraw(summary));

client.send(&sam, DepositMoney { amount: 100 });
```

```text
redraw: Ok(Some(Summary { owner: Some("sam"), balance: 100 }))
```

`cargo run -p bank` runs the full example. The same code runs against a [server](server.md).

## Say what the app may use

```rust
#[domain::api(commands(OpenAccount, DepositMoney, WithdrawMoney), views(AccountSummary, OwnerHoldings, Statement))]
pub struct BankApi;
```

If something isn't listed, the code doesn't build:

```rust
client.send(&sam, CloseAccount);
```

```text
error[E0277]: `BankApi` doesn't let clients send `CloseAccount`
  add `CloseAccount` to `#[domain::api(commands(..))]`
```

```rust
client.view::<AuditLog>();
```

```text
error[E0277]: `BankApi` doesn't let clients read `AuditLog`
  add `AuditLog` to `#[domain::api(views(..))]`
```

## Start one

| | Commands run on | Use it for |
| --- | --- | --- |
| `Client::in_process(Runtime::spawn(build))` | the runtime's thread, in order | apps, tests: the UI thread never waits |
| `Client::connect(url, token)` | the [server](server.md) | client/server, many users |

```rust
let me = client.acting_as(User("sam".into()));   // in process: who sends and reads, see Who is asking
client.connection();                              // LiveView<Connection>: Connected, Reconnecting, ..
```

Reads run there too, so a `get` resolves once the runtime reaches it, and a live view starts at `Ok(None)`, version 0, until the runtime has read the state now. [Policies](policies.md) and scheduled commands also run there, between commands.

On the web there are no threads, so jobs run at once, on the page's thread. See [Running a model](running.md#one-thread-changes-it).

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

Over a connection, a client does this for you when the connection drops: see [Offline and instant](offline.md).

## Read

```rust
let summaries = client.view::<AccountSummary>();

summaries.get(&sam);                          // Pending<Read<AccountSummary>>
summaries.get_at_least(&sam, &receipt);       // after your own write
summaries.snapshot(&sam);                     // state + cursor
client.view::<Statement>().list(&sam, Cursor::START, 20);
client.view::<Statement>().events(&sam, cursor);   // a Feed, as in Views
```

`Read<V>` is `Result<Option<V::State>, ReadError<V::Rejection>>`:

```rust
Ok(Some(Summary { owner: Some("sam"), balance: 70 }))
Ok(None)                                        // no events for this key yet
Err(ReadError::Rejected(NotOwner))              // the view's validate refused you
Err(ReadError::NotCaughtUp(..))                 // get_at_least, before the view applied it
Err(ReadError::Unavailable(reason))             // the server couldn't be reached
```

In the same process, reads are ready at once. Over a connection they take a round trip, so treat every read as `Pending`.

## Follow it live

`live` returns a `LiveView`, which always holds the latest `Read<V>`: what `get` would return now. It offers three ways to see changes:

```rust
let summary = client.view::<AccountSummary>().live(&sam);

// Read it each frame.
let (latest, version) = summary.latest();

// Get called when it changes. Dropping the listener stops the calls.
let listener = summary.on_change(|latest, version| schedule_redraw());

// Await the next change.
let (latest, version) = summary.changed(version).await;
```

`version` counts changes. It lets you skip work when nothing changed, or wait for the change after one you've seen. Over a connection, version 0 is `Ok(None)` until the server's first answer.

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

Callbacks run on the domain thread, or the connection's, so they should only hand work to the UI thread. They may read views and send commands, which queue behind the callback, so don't wait on them there.

Next: [Policies](policies.md).
