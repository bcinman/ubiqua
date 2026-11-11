# Serve it

Run the model on a server, and apps elsewhere use it through the same `Client`.

```rust
let bank = Runtime::spawn(|| Memory::new().with::<Bank>());
let server = Server::<BankApi>::new(bank)
    .sessions(|token| users.check(token))       // Option<User>
    .listen("0.0.0.0:7878")?;
server.wait();
```

```rust
let client = Client::<BankApi>::connect("wss://bank.example.com/", Some(&token));
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
| A send | waits, and goes out again on reconnect under the same id; see [Offline and instant](offline.md) |
| A read it was carrying | `Unavailable` |
| A read made now | waits for the connection, or fails with the next failed try |
| `live` | keeps its last value, then catches up |
| `events` | picks up after the last entry it had, then sends `CaughtUp` again |

```rust
let connection = client.connection();          // LiveView<Connection>
let _banner = connection.on_change(|c, _| show_offline_banner(!matches!(**c, Connection::Connected)));
```

```text
Connecting → Connected → Reconnecting { reason: "unexpected end of file" } → Connected
```

## Serve it over TLS

Tokens travel on the connection, so encrypt it whenever clients connect over the internet:

```rust
let tls = ServerTls::from_pem(&std::fs::read("fullchain.pem")?, &std::fs::read("privkey.pem")?)?;
let server = Server::<BankApi>::new(bank).sessions(check).tls(tls).listen("0.0.0.0:443")?;
server.url()                                    // "wss://0.0.0.0:443/"
```

A client connects to `wss://` the same way, trusting what the system trusts:

```rust
let client = Client::<BankApi>::connect("wss://bank.example.com/", Some(&token));
```

For a certificate your own authority signed, as in tests:

```rust
let client = Client::<BankApi>::connect_with(&url, Some(&token), ClientTls::trusting(&std::fs::read("ca.pem")?)?);
```

```text
Reconnecting { reason: "invalid peer certificate: UnknownIssuer" }     a server it doesn't trust
```

A TLS proxy in front of a plain server works too.

## Host mode

One app runs the model and serves it to others:

```rust
let bank = Runtime::spawn(|| Memory::new().persist_to(Sqlite::open("bank.db").unwrap()).with::<Bank>());
let me = Client::<BankApi>::in_process(bank.clone()).acting_as(User("sam".into()));
let _server = Server::<BankApi>::new(bank).sessions(check).listen("0.0.0.0:7878")?;
```

The app's commands and reads, and every connection's, queue for the same runtime thread.

## Serve telemetry

```rust
.telemetry(|user: Option<&User>| user.is_some_and(|u| u.is_staff()))   // who may watch it run
```

Without it, nobody gets telemetry. See [Watch it run](telemetry.md).

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

- `Client::connect` is native only; a browser transport comes later.
- One server process. A server also runs the model's policies and schedules.

Next: [Offline and instant](offline.md).
