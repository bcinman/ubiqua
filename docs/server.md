# Serve it

Run the model on a server, and apps elsewhere use it through the same `Client`.

```rust
let memory = Arc::new(Memory::new().with::<Bank>());
let server = Server::<BankApi>::new(memory)
    .sessions(|token| users.check(token))       // Option<User>
    .listen("0.0.0.0:7878")?;
server.wait();
```

```rust
let client = Client::<BankApi>::connect("ws://bank.example.com:7878/", Some(&token));
client.send(&sam, DepositMoney { amount: 100 });    // the same code as in process
```

```diagram
In process      UI ─▶ Client ─▶ Memory
Client/server   UI ─▶ Client ──WebSocket──▶ Server ─▶ Memory
Many users      UI ─▶ Client ──┐
                UI ─▶ Client ──┼──▶ Server ─▶ Memory     each sees the others' changes
                UI ─▶ Client ──┘
Host mode       UI ─▶ Client ─▶ Memory ◀─ Server ◀── other apps
```

## Try it

```sh
cargo run -p bank -- serve 127.0.0.1:7878 bank.db
cargo run -p bank -- join ws://127.0.0.1:7878/ sam      # in two terminals
```

```text
first sam                                          second sam
redraw #5: Ok(Some(Summary { .., balance: 70 }))
watching; press Ctrl-C to stop
                                                   withdraw 900: rejected: Frozen
redraw #6: Ok(Some(Summary { .., balance: 1070 })) ◀── deposited 1000
```

## Sign connections in

The client sends its token as `Authorization: Bearer <token>`. The session check turns it into the [actor](sessions.md), or refuses the connection:

```rust
.sessions(|token| sessions.lookup(token).map(|s| User(s.user)))
```

```rust
let stranger = Client::<BankApi>::connect(&url, Some("bad-token"));
stranger.connection().latest().0                 // Connection::Refused { status: 401 }
stranger.send(&sam, DepositMoney { amount: 1 })  // Err(Unavailable { reason: "the server refused the connection (401)" })
```

A refused client doesn't try again. Without `.sessions(..)`, every connection acts as nobody.

## When the connection drops

The client reconnects by itself, waiting 0.1s, then 0.2s, up to 5s between tries.

| What | While it's down |
| --- | --- |
| A send or read it was carrying | `Unavailable`. Retry a send with the [same id](client.md#when-the-outcome-is-unknown) |
| A send or read made now | waits for the connection, or fails with the next failed try |
| `live` | keeps its last value, then catches up |
| `events` | picks up after the last entry it had, then sends `CaughtUp` again |

```rust
let connection = client.connection();          // LiveView<Connection>
let _banner = connection.on_change(|c, _| show_offline_banner(!matches!(**c, Connection::Connected)));
```

```text
Connecting → Connected → Reconnecting { reason: "unexpected end of file" } → Connected
```

## Host mode

One app runs the model and serves it to others:

```rust
let memory = Arc::new(Memory::new().persist_to(Sqlite::open("bank.db")?).with::<Bank>());
let me = Client::<BankApi>::in_process(memory.clone()).acting_as(User("sam".into()));
let _server = Server::<BankApi>::new(memory).sessions(check).listen("0.0.0.0:7878")?;
```

## What goes over the wire

JSON text messages, one per request and answer. Values are in the same JSON as storage. See [`domain::wire`](../crates/domain/src/wire.rs).

```text
→ {"id":1,"op":"send","command":"DepositMoney","target":"sam-main","command_id":"9f3c..-1","input":{"amount":100}}
← {"id":1,"ok":{"stream":"Account/sam-main","version":2,"command_id":"9f3c..-1","events":[..],"reply":{"balance":100}}}
→ {"id":2,"op":"watch","view":"AccountSummary","key":"sam-main"}
← {"id":2,"state":{"owner":"sam","balance":100}}
← {"id":2,"refused":{"NotOwner":null}}
```

Only what the Api lists can be reached:

```text
← {"id":3,"err":{"invalid":"this Api has no command FreezeAccount"}}
```

## Stop it

```rust
server.stop();     // or drop it: stops listening and closes every connection
```

## Limits

- `ws://` only. Put a TLS proxy in front for `wss://`.
- `Client::connect` is native only; a browser transport comes later.
- One server process. A server also runs the model's policies and schedules.

Next: [Stories](stories.md).
