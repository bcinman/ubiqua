//! Serves a model's [`Api`] over WebSocket, so apps elsewhere use the model through
//! `domain_client::Client::connect` exactly as they would in process. Many clients can connect at
//! once: each sees the others' changes as they commit.
//!
//! ```ignore
//! let memory = Arc::new(Memory::new().with::<Bank>());
//! let server = Server::<BankApi>::new(memory).sessions(|token| users.check(token)).listen("0.0.0.0:7878")?;
//! server.wait();
//! ```
//!
//! Plain threads, no async runtime: one pair per connection. [`Server::tls`] serves `wss://`. The protocol is [`domain::wire`].

use domain::host::{ApiRegistry, CurrentActor};
use domain::wire::{self, At, Body, Failure, Op, Request, Response};
use domain::{
    AggregateId, Api, Apply, Command, Cursor, Frame, Handles, Position, ReadCx, SendError, StreamId, Validate, View,
};
use domain_memory::{Feed, Memory, Watch};
use domain_websocket::{Message, ServerTls, Writer};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::io;
use std::marker::PhantomData;
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

/// The most entries one `list` returns.
pub const MAX_LIST: usize = 1000;

type Sessions = Arc<dyn Fn(&str) -> Option<CurrentActor> + Send + Sync>;

/// A model's Api, ready to serve. Start it with [`listen`](Self::listen).
pub struct Server<A: Api> {
    memory: Arc<Memory>,
    routes: Arc<Routes>,
    sessions: Option<Sessions>,
    tls: Option<ServerTls>,
    _api: PhantomData<fn() -> A>,
}

impl<A: Api> Server<A> {
    /// Serves the commands and views `A` lists, on `memory`.
    ///
    /// # Panics
    /// When `memory` doesn't host one of the Api's views.
    pub fn new(memory: Arc<Memory>) -> Self {
        let mut registrar = Registrar { memory: &memory, routes: Routes::default() };
        A::register(&mut registrar);
        let routes = Arc::new(registrar.routes);
        Self { memory, routes, sessions: None, tls: None, _api: PhantomData }
    }

    /// Serves `wss://` with this certificate, so connections are encrypted and clients can check
    /// they reached this server. Do this, or serve behind a proxy that does, whenever clients
    /// connect over the internet: their tokens travel on the connection.
    pub fn tls(mut self, tls: ServerTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Signs each connection in. `check` gets the token the client connected with and returns who
    /// it is, or `None` to refuse the connection. `decide` reads who with `cx.actor::<T>()`, and
    /// views' `validate` with `cx.actor::<T>()`.
    ///
    /// Without it, every connection acts as nobody, like code in the app.
    pub fn sessions<T>(mut self, check: impl Fn(&str) -> Option<T> + Send + Sync + 'static) -> Self
    where
        T: Any + std::fmt::Debug + Send + Sync,
    {
        self.sessions = Some(Arc::new(move |token| check(token).map(CurrentActor::new)));
        self
    }

    /// Listens on `addr`, such as `"0.0.0.0:7878"`, or `"127.0.0.1:0"` for any free port. Also runs
    /// the model's policies and schedules. Serves until [`Listening::stop`] or until it's dropped.
    pub fn listen(self, addr: impl ToSocketAddrs) -> io::Result<Listening> {
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        let shared = Arc::new(Shared {
            stopped: AtomicBool::new(false),
            connections: Mutex::default(),
            next: AtomicU64::new(0),
        });
        let wake = run_due(self.memory.clone());
        let scheme = if self.tls.is_some() { "wss" } else { "ws" };
        let accepting = shared.clone();
        let accept = std::thread::Builder::new().name("domain-server".into()).spawn(move || {
            for stream in listener.incoming() {
                if accepting.stopped.load(Ordering::SeqCst) {
                    return;
                }
                let Ok(stream) = stream else { continue };
                let connection = Connection {
                    memory: self.memory.clone(),
                    routes: self.routes.clone(),
                    sessions: self.sessions.clone(),
                    tls: self.tls.clone(),
                    shared: accepting.clone(),
                };
                let _ = std::thread::Builder::new()
                    .name("domain-connection".into())
                    .spawn(move || connection.serve(stream));
            }
        })?;
        Ok(Listening { addr, scheme, shared, accept: Some(accept), _wake: wake })
    }
}

/// A running server. Stops when dropped.
pub struct Listening {
    addr: SocketAddr,
    scheme: &'static str,
    shared: Arc<Shared>,
    accept: Option<std::thread::JoinHandle<()>>,
    /// Keeps the policy thread running; dropping it stops that thread.
    _wake: Arc<Sender<()>>,
}

impl Listening {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `ws://addr/`, or `wss://addr/` with [`Server::tls`], for `Client::connect`.
    pub fn url(&self) -> String {
        format!("{}://{}/", self.scheme, self.addr)
    }

    /// Serves until the process ends.
    pub fn wait(mut self) {
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }

    /// Stops accepting connections and closes the open ones. Their clients see the connection
    /// drop, as if the network failed.
    pub fn stop(self) {}
}

impl Drop for Listening {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::SeqCst);
        // Wake the accept loop so it sees `stopped`.
        let _ = TcpStream::connect(self.addr);
        for (_, writer) in lock(&self.shared.connections).drain() {
            writer.shutdown();
        }
        // Once it returns, the port is free again.
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
    }
}

struct Shared {
    stopped: AtomicBool,
    /// A writer per open connection, to close it on stop.
    connections: Mutex<HashMap<u64, Writer>>,
    next: AtomicU64,
}

/// Runs the model's policies and schedules on a thread of their own, until the returned sender
/// is dropped.
fn run_due(memory: Arc<Memory>) -> Arc<Sender<()>> {
    let (wake, woken) = mpsc::channel::<()>();
    let wake = Arc::new(wake);
    let weak = Arc::downgrade(&wake);
    memory.on_wake(move || {
        if let Some(wake) = weak.upgrade() {
            let _ = wake.send(());
        }
    });
    let _ = std::thread::Builder::new().name("domain-policies".into()).spawn(move || {
        loop {
            let due = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| memory.run_due())).ok().flatten();
            let next = match due {
                Some(at) => woken.recv_timeout(memory.now().until(at)),
                None => woken.recv().map_err(|_| RecvTimeoutError::Disconnected),
            };
            if next == Err(RecvTimeoutError::Disconnected) {
                return;
            }
        }
    });
    wake
}

/// How to run each command and read each view the Api lists, by name.
#[derive(Default)]
struct Routes {
    commands: HashMap<&'static str, CommandFn>,
    views: HashMap<&'static str, ViewRoute>,
}

/// Collects the routes, checking each view is hosted.
struct Registrar<'a> {
    memory: &'a Memory,
    routes: Routes,
}

type CommandFn = fn(&Memory, Option<&CurrentActor>, Value, String, Value) -> Body;

struct ViewRoute {
    get: fn(&Memory, Option<&CurrentActor>, Value, Option<At>) -> Body,
    snapshot: fn(&Memory, Option<&CurrentActor>, Value) -> Body,
    list: fn(&Memory, Option<&CurrentActor>, Value, u64, usize) -> Body,
    /// Starts a watch or feed. The returned handle keeps it running.
    watch: WatchFn,
    events: EventsFn,
}

type WatchFn = fn(&Memory, Option<CurrentActor>, Value, Outbox) -> Result<Handle, Body>;
type EventsFn = fn(&Memory, Option<CurrentActor>, Value, u64, Outbox) -> Result<Handle, Body>;

type Handle = Box<dyn Any + Send>;

impl ApiRegistry for Registrar<'_> {
    fn command<C: Command>(&mut self)
    where
        <C::Target as AggregateId>::Aggregate: Handles<C>,
    {
        self.routes.commands.insert(C::NAME, run_command::<C>);
    }

    fn view<V: Apply + Validate>(&mut self) {
        // Fails now, with the view's name, rather than on the first read.
        let _ = self.memory.view::<V>();
        let route = ViewRoute {
            get: get::<V>,
            snapshot: snapshot::<V>,
            list: list::<V>,
            watch: watch::<V>,
            events: events::<V>,
        };
        self.routes.views.insert(V::NAME, route);
    }
}

fn run_command<C: Command>(
    memory: &Memory,
    actor: Option<&CurrentActor>,
    target: Value,
    command_id: String,
    input: Value,
) -> Body
where
    <C::Target as AggregateId>::Aggregate: Handles<C>,
{
    let (target, command) = match (decode::<C::Target>(target), decode::<C>(input)) {
        (Ok(target), Ok(command)) => (target, command),
        (Err(e), _) | (_, Err(e)) => return Body::Err(Failure::Invalid(format!("{}: {e}", C::NAME))),
    };
    let sent =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| memory.send_as(actor, &target, command, command_id)));
    match sent {
        Ok(Ok(receipt)) => match wire::encode_receipt(&receipt) {
            Ok(receipt) => Body::Ok(receipt),
            Err(e) => Body::Err(Failure::Unavailable(format!("the receipt doesn't encode: {e}"))),
        },
        Ok(Err(SendError::Rejected(r))) => encode(&r).map_or_else(unavailable, |r| Body::Err(Failure::Rejected(r))),
        Ok(Err(SendError::CommandIdReused { stream, command_id })) => {
            Body::Err(Failure::CommandIdReused { stream: stream.to_string(), command_id: command_id.to_string() })
        }
        Ok(Err(other)) => Body::Err(Failure::Unavailable(other.to_string())),
        Err(_) => Body::Err(Failure::Unavailable("the command panicked".into())),
    }
}

/// `Ok(())` when `actor` may read `key` in this state, else the `refused` answer.
fn validate<V: Validate>(actor: Option<&CurrentActor>, key: &V::Key, state: Option<&V::State>) -> Result<(), Body> {
    V::validate(&ReadCx::new(actor), key, state).map_err(|r| encode(&r).map_or_else(unavailable, Body::Refused))
}

fn get<V: Apply + Validate>(memory: &Memory, actor: Option<&CurrentActor>, key: Value, at_least: Option<At>) -> Body {
    let read = || -> Result<Body, Body> {
        let key = decode::<V::Key>(key).map_err(invalid::<V>)?;
        let reader = memory.view::<V>();
        if let Some(At { stream, version }) = at_least {
            reader.at_least(Position { stream: StreamId::from(stream), version }).map_err(|e| {
                Body::Err(Failure::NotCaughtUp {
                    view: e.view.into(),
                    stream: e.stream.to_string(),
                    wanted: e.wanted,
                    applied: e.applied,
                })
            })?;
        }
        let state = reader.get(&key);
        validate::<V>(actor, &key, state.as_ref())?;
        Ok(encode(&state).map_or_else(unavailable, Body::Ok))
    };
    read().unwrap_or_else(|body| body)
}

fn snapshot<V: Apply + Validate>(memory: &Memory, actor: Option<&CurrentActor>, key: Value) -> Body {
    let read = || -> Result<Body, Body> {
        let key = decode::<V::Key>(key).map_err(invalid::<V>)?;
        let snapshot = memory.view::<V>().snapshot(&key);
        validate::<V>(actor, &key, snapshot.state.as_ref())?;
        Ok(wire::encode_snapshot(&snapshot).map_or_else(unavailable, Body::Ok))
    };
    read().unwrap_or_else(|body| body)
}

fn list<V: Apply + Validate>(
    memory: &Memory,
    actor: Option<&CurrentActor>,
    key: Value,
    after: u64,
    take: usize,
) -> Body {
    let read = || -> Result<Body, Body> {
        let key = decode::<V::Key>(key).map_err(invalid::<V>)?;
        let reader = memory.view::<V>();
        validate::<V>(actor, &key, reader.get(&key).as_ref())?;
        let entries: Result<Vec<_>, _> =
            reader.list(&key, Cursor::new(after), take.min(MAX_LIST)).iter().map(wire::encode_entry).collect();
        Ok(entries.and_then(|e| encode(&e)).map_or_else(unavailable, Body::Ok))
    };
    read().unwrap_or_else(|body| body)
}

/// Sends a watched key's state to its connection, validating each change.
struct Relay<V: View, S> {
    subscription: S,
    /// Held while pumping, so changes go out in the order they were applied.
    order: Mutex<()>,
    key: V::Key,
    actor: Option<CurrentActor>,
    out: Outbox,
    ended: AtomicBool,
}

fn watch<V: Apply + Validate>(
    memory: &Memory,
    actor: Option<CurrentActor>,
    key: Value,
    out: Outbox,
) -> Result<Handle, Body> {
    let key = decode::<V::Key>(key).map_err(invalid::<V>)?;
    let watch = memory.view::<V>().watch(&key);
    let relay = Arc::new(Relay::<V, Watch<V>> {
        subscription: watch,
        order: Mutex::new(()),
        key,
        actor,
        out,
        ended: AtomicBool::new(false),
    });
    start(&relay, |relay| {
        let _order = lock(&relay.order);
        // Only the latest state matters.
        let Some(latest) = relay.subscription.drain().pop() else { return };
        if relay.ended.load(Ordering::SeqCst) {
            return;
        }
        let body = match validate::<V>(relay.actor.as_ref(), &relay.key, latest.state.as_ref()) {
            Ok(()) => encode(&latest.state).map_or_else(unavailable, Body::State),
            Err(refused) => {
                relay.ended.store(true, Ordering::SeqCst);
                refused
            }
        };
        relay.out.send(body);
    });
    Ok(Box::new(relay))
}

fn events<V: Apply + Validate>(
    memory: &Memory,
    actor: Option<CurrentActor>,
    key: Value,
    after: u64,
    out: Outbox,
) -> Result<Handle, Body> {
    let key = decode::<V::Key>(key).map_err(invalid::<V>)?;
    let reader = memory.view::<V>();
    validate::<V>(actor.as_ref(), &key, reader.get(&key).as_ref())?;
    let feed = reader.events(&key, Cursor::new(after));
    let relay = Arc::new(Relay::<V, Feed<V>> {
        subscription: feed,
        order: Mutex::new(()),
        key,
        actor,
        out,
        ended: AtomicBool::new(false),
    });
    start(&relay, |relay| {
        let _order = lock(&relay.order);
        for frame in relay.subscription.drain() {
            if relay.ended.load(Ordering::SeqCst) {
                return;
            }
            let body = match frame {
                Frame::Entry(entry) => match validate::<V>(relay.actor.as_ref(), &relay.key, Some(&entry.state)) {
                    Ok(()) => wire::encode_entry(&entry).map_or_else(unavailable, Body::Entry),
                    Err(refused) => {
                        relay.ended.store(true, Ordering::SeqCst);
                        refused
                    }
                },
                Frame::CaughtUp { cursor } => Body::CaughtUp(cursor.seq()),
                Frame::Rejected(r) => encode(&r).map_or_else(unavailable, Body::Refused),
            };
            relay.out.send(body);
        }
    });
    Ok(Box::new(relay))
}

/// Calls `pump` now and whenever the relay's subscription has news. Weak, so dropping the
/// handle stops it.
fn start<V: View, S: Notifies + Send + Sync + 'static>(relay: &Arc<Relay<V, S>>, pump: fn(&Relay<V, S>)) {
    let weak: Weak<Relay<V, S>> = Arc::downgrade(relay);
    relay.subscription.notify(Box::new(move || {
        if let Some(relay) = weak.upgrade() {
            pump(&relay);
        }
    }));
}

trait Notifies {
    fn notify(&self, f: Box<dyn Fn() + Send + Sync>);
}

impl<T> Notifies for domain_memory::Subscription<T> {
    fn notify(&self, f: Box<dyn Fn() + Send + Sync>) {
        domain_memory::Subscription::notify(self, f);
    }
}

/// Queues messages for one connection's writer thread.
#[derive(Clone)]
struct Outbox {
    id: u64,
    out: Sender<Outgoing>,
}

impl Outbox {
    fn send(&self, body: Body) {
        let _ = self.out.send(Outgoing::Response(Response { id: self.id, body }));
    }
}

enum Outgoing {
    Response(Response),
    Pong(Vec<u8>),
    Close,
}

struct Connection {
    memory: Arc<Memory>,
    routes: Arc<Routes>,
    sessions: Option<Sessions>,
    tls: Option<ServerTls>,
    shared: Arc<Shared>,
}

impl Connection {
    fn serve(self, stream: TcpStream) {
        let sessions = self.sessions.clone();
        let check = |request: &domain_websocket::Request| match &sessions {
            None => Ok(None),
            Some(check) => request.bearer().and_then(|token| check(token)).map(Some).ok_or(401),
        };
        let accepted = match &self.tls {
            Some(tls) => domain_websocket::accept_tls(stream, tls, check),
            None => domain_websocket::accept(stream, check),
        };
        let Ok((actor, mut reader, mut writer)) = accepted else { return };
        let number = self.shared.next.fetch_add(1, Ordering::Relaxed);
        if let Ok(closer) = writer.try_clone() {
            lock(&self.shared.connections).insert(number, closer);
        }
        if self.shared.stopped.load(Ordering::SeqCst) {
            writer.shutdown();
        }

        let (out, outgoing) = mpsc::channel::<Outgoing>();
        let writing = std::thread::Builder::new().name("domain-connection-writer".into()).spawn(move || {
            for message in outgoing {
                let written = match message {
                    Outgoing::Response(response) => match serde_json::to_string(&response) {
                        Ok(text) => writer.text(&text),
                        Err(_) => Ok(()),
                    },
                    Outgoing::Pong(payload) => writer.pong(&payload),
                    Outgoing::Close => break,
                };
                if written.is_err() {
                    break;
                }
            }
            writer.close();
        });

        // Watches and feeds, by request id. Dropping one stops it.
        let mut live: HashMap<u64, Handle> = HashMap::new();
        loop {
            match reader.read() {
                Ok(Message::Text(text)) => self.handle(&text, actor.as_ref(), &out, &mut live),
                Ok(Message::Ping(payload)) => {
                    let _ = out.send(Outgoing::Pong(payload));
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close) | Err(_) => break,
            }
        }
        drop(live);
        let _ = out.send(Outgoing::Close);
        drop(out);
        if let Ok(writing) = writing {
            let _ = writing.join();
        }
        lock(&self.shared.connections).remove(&number);
    }

    fn handle(
        &self,
        text: &str,
        actor: Option<&CurrentActor>,
        out: &Sender<Outgoing>,
        live: &mut HashMap<u64, Handle>,
    ) {
        let request: Request = match serde_json::from_str(text) {
            Ok(request) => request,
            Err(e) => {
                // Answer under the request's id if it has one, so the client isn't left waiting.
                let id = serde_json::from_str::<Value>(text).ok().and_then(|v| v.get("id")?.as_u64()).unwrap_or(0);
                let body = Body::Err(Failure::Invalid(format!("the request doesn't decode: {e}")));
                let _ = out.send(Outgoing::Response(Response { id, body }));
                return;
            }
        };
        let outbox = Outbox { id: request.id, out: out.clone() };
        let memory = &*self.memory;
        let route = |view: &str| self.routes.views.get(view).ok_or_else(|| unknown("view", view));
        let answer = match request.op {
            Op::Send { command, target, command_id, input } => match self.routes.commands.get(command.as_str()) {
                Some(run) => run(memory, actor, target, command_id, input),
                None => unknown("command", &command),
            },
            Op::Get { view, key, at_least } => {
                route(&view).map_or_else(|e| e, |r| (r.get)(memory, actor, key, at_least))
            }
            Op::Snapshot { view, key } => route(&view).map_or_else(|e| e, |r| (r.snapshot)(memory, actor, key)),
            Op::List { view, key, after, take } => {
                route(&view).map_or_else(|e| e, |r| (r.list)(memory, actor, key, after, take))
            }
            Op::Watch { view, key } => {
                match route(&view).and_then(|r| (r.watch)(memory, actor.cloned(), key, outbox.clone())) {
                    Ok(handle) => {
                        live.insert(request.id, handle);
                        return;
                    }
                    Err(body) => body,
                }
            }
            Op::Events { view, key, after } => {
                match route(&view).and_then(|r| (r.events)(memory, actor.cloned(), key, after, outbox.clone())) {
                    Ok(handle) => {
                        live.insert(request.id, handle);
                        return;
                    }
                    Err(body) => body,
                }
            }
            Op::Stop => {
                live.remove(&request.id);
                return;
            }
        };
        outbox.send(answer);
    }
}

fn unknown(kind: &str, name: &str) -> Body {
    Body::Err(Failure::Invalid(format!("this Api has no {kind} {name}")))
}

fn invalid<V: View>(e: String) -> Body {
    Body::Err(Failure::Invalid(format!("{} key: {e}", V::NAME)))
}

fn unavailable(e: String) -> Body {
    Body::Err(Failure::Unavailable(e))
}

fn encode<T: Serialize>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}

fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
