//! A client for a model on a server: requests and live reads over one WebSocket that reconnects
//! by itself. The protocol is `domain::wire`.

use crate::live::{LiveView, Shared};
use crate::pending::{Pending, lock, pending};
use crate::{AggregateOf, Connection, Read, ReadError, ReadResult, Sent};
use domain::wire::{self, At, Body, Failure, Op, Request, Response};
use domain::{Apply, Command, CommandId, Cursor, Entry, Frame, Handles, Position, SendError, Snapshot, Validate, View};
use domain_memory::Feed;
use domain_websocket::{ConnectError, Message, Writer};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

/// Waits between connection attempts, doubling from the first to the last.
const BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(5));

/// Gets the answer to a one-off request, or why there won't be one.
type Waiter = Box<dyn FnOnce(Result<Body, String>) + Send>;

/// A watch or feed: how to ask for it again after a reconnect, and what to do with each message.
/// `on` returns `false` once it's done.
struct Live {
    request: Box<dyn Fn() -> Op + Send>,
    on: Arc<dyn Fn(Body) -> bool + Send + Sync>,
}

pub(crate) struct Remote {
    /// Starts each command id this client makes.
    prefix: String,
    sent: AtomicU64,
    state: Mutex<State>,
    status: Arc<Shared<Connection>>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    /// The writer thread's queue, while connected.
    out: Option<Sender<Outgoing>>,
    /// Closes the socket, so the connection thread stops reading.
    closer: Option<Writer>,
    /// Requests not sent yet, waiting for a connection.
    queued: Vec<String>,
    waiting: HashMap<u64, Waiter>,
    live: HashMap<u64, Live>,
    /// The server refused the connection; nothing more will be sent.
    refused: Option<u16>,
}

enum Outgoing {
    Text(String),
    Pong(Vec<u8>),
}

impl Remote {
    pub(crate) fn start(url: &str, token: Option<&str>) -> Arc<Self> {
        let remote = Arc::new(Remote {
            prefix: format!("{:016x}", domain_websocket::random_u64()),
            sent: AtomicU64::new(0),
            state: Mutex::default(),
            status: Shared::new(Connection::Connecting),
        });
        let (url, token, weak) = (url.to_string(), token.map(|t| format!("Bearer {t}")), Arc::downgrade(&remote));
        std::thread::Builder::new()
            .name("domain-client".into())
            .spawn(move || run(weak, url, token))
            .expect("spawn the connection thread");
        remote
    }

    pub(crate) fn connection(&self) -> LiveView<Connection> {
        LiveView::new(self.status.clone(), Arc::new(()))
    }

    pub(crate) fn new_command_id(&self) -> CommandId {
        let n = self.sent.fetch_add(1, Ordering::Relaxed) + 1;
        CommandId::new(format!("{}-{n}", self.prefix))
    }

    pub(crate) fn send<C>(
        &self,
        id: &C::Target,
        command: C,
        command_id: CommandId,
        then: impl FnOnce(Sent<C>) + Send + 'static,
    ) where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let unavailable = {
            let command_id = command_id.clone();
            move |reason: String| SendError::Unavailable { command_id, reason }
        };
        let op = match (encode(id), encode(&command)) {
            (Ok(target), Ok(input)) => {
                Op::Send { command: C::NAME.into(), target, command_id: command_id.to_string(), input }
            }
            (Err(e), _) | (_, Err(e)) => return then(Err(unavailable(e))),
        };
        self.request(
            op,
            Box::new(move |answer| {
                then(match answer {
                    Ok(Body::Ok(receipt)) => wire::decode_receipt::<C>(receipt).map_err(unavailable),
                    Ok(Body::Err(Failure::Rejected(r))) => {
                        Err(decode::<C::Rejection>(r).map_or_else(unavailable, SendError::Rejected))
                    }
                    Ok(Body::Err(Failure::CommandIdReused { stream, command_id })) => {
                        Err(SendError::CommandIdReused { stream: stream.into(), command_id: command_id.into() })
                    }
                    Ok(other) => Err(unavailable(failure(other))),
                    Err(reason) => Err(unavailable(reason)),
                })
            }),
        );
    }

    pub(crate) fn get<V: Apply + Validate>(&self, key: &V::Key, at_least: Option<Position>) -> Pending<Read<V>> {
        let at_least = at_least.map(|p| At { stream: p.stream.to_string(), version: p.version });
        self.read::<V, _>(key, |key| Op::Get { view: V::NAME.into(), key, at_least }, decode::<Option<V::State>>)
    }

    pub(crate) fn snapshot<V: Apply + Validate>(&self, key: &V::Key) -> Pending<ReadResult<V, Snapshot<V>>> {
        self.read::<V, _>(key, |key| Op::Snapshot { view: V::NAME.into(), key }, wire::decode_snapshot::<V>)
    }

    pub(crate) fn list<V: Apply + Validate>(
        &self,
        key: &V::Key,
        after: Cursor,
        take: usize,
    ) -> Pending<ReadResult<V, Vec<Entry<V>>>> {
        let op = |key| Op::List { view: V::NAME.into(), key, after: after.seq(), take };
        self.read::<V, _>(key, op, |entries| {
            decode::<Vec<wire::WireEntry>>(entries)?.into_iter().map(wire::decode_entry::<V>).collect()
        })
    }

    /// A one-off read whose `ok` answer `decode` turns into a `T`.
    fn read<V: View, T: Send + 'static>(
        &self,
        key: &V::Key,
        op: impl FnOnce(Value) -> Op,
        decode_ok: fn(Value) -> Result<T, String>,
    ) -> Pending<Result<T, ReadError<V::Rejection>>> {
        let key = match encode(key) {
            Ok(key) => key,
            Err(e) => return Pending::ready(Err(ReadError::Unavailable(e))),
        };
        let (pending, resolver) = pending();
        self.request(
            op(key),
            Box::new(move |answer| {
                resolver.resolve(match answer {
                    Ok(Body::Ok(value)) => decode_ok(value).map_err(ReadError::Unavailable),
                    Ok(Body::Refused(r)) => {
                        Err(decode::<V::Rejection>(r).map_or_else(ReadError::Unavailable, ReadError::Rejected))
                    }
                    Ok(Body::Err(Failure::NotCaughtUp { view: _, stream, wanted, applied })) => {
                        Err(ReadError::NotCaughtUp(domain::NotCaughtUp {
                            view: V::NAME,
                            stream: stream.into(),
                            wanted,
                            applied,
                        }))
                    }
                    Ok(other) => Err(ReadError::Unavailable(failure(other))),
                    Err(reason) => Err(ReadError::Unavailable(reason)),
                })
            }),
        );
        pending
    }

    pub(crate) fn watch<V: Apply + Validate>(self: &Arc<Self>, key: &V::Key) -> LiveView<Read<V>> {
        let target = Shared::<Read<V>>::new(Ok(None));
        let key = match encode(key) {
            Ok(key) => key,
            Err(e) => {
                target.publish(Err(ReadError::Unavailable(e)));
                return LiveView::new(target, Arc::new(()));
            }
        };
        let id = self.next_id();
        let publish = target.clone();
        let on = move |body: Body| match body {
            Body::State(state) => {
                publish.publish(decode::<Option<V::State>>(state).map_err(ReadError::Unavailable));
                true
            }
            Body::Refused(r) => {
                publish
                    .publish(Err(decode::<V::Rejection>(r).map_or_else(ReadError::Unavailable, ReadError::Rejected)));
                false
            }
            other => {
                publish.publish(Err(ReadError::Unavailable(failure(other))));
                false
            }
        };
        self.subscribe(id, Box::new(move || Op::Watch { view: V::NAME.into(), key: key.clone() }), Arc::new(on));
        LiveView::new(target, Arc::new(Unsubscribe { remote: Arc::downgrade(self), id }))
    }

    pub(crate) fn events<V: Apply + Validate>(self: &Arc<Self>, key: &V::Key, after: Cursor) -> Feed<V> {
        let id = self.next_id();
        let (publisher, feed) = domain_memory::channel::<Frame<V>>(Unsubscribe { remote: Arc::downgrade(self), id });
        let key = match encode(key) {
            Ok(key) => key,
            Err(_) => return feed,
        };
        // Where to pick up after a reconnect: the last entry seen.
        let seen = Arc::new(AtomicU64::new(after.seq()));
        let last = seen.clone();
        let on = move |body: Body| match body {
            Body::Entry(entry) => match wire::decode_entry::<V>(entry) {
                Ok(entry) => {
                    seen.fetch_max(entry.cursor.seq(), Ordering::SeqCst);
                    publisher.push(Frame::Entry(entry))
                }
                Err(_) => false,
            },
            Body::CaughtUp(cursor) => publisher.push(Frame::CaughtUp { cursor: Cursor::new(cursor) }),
            Body::Refused(r) => {
                if let Ok(r) = decode::<V::Rejection>(r) {
                    publisher.push(Frame::Rejected(r));
                }
                false
            }
            _ => false,
        };
        let request = move || Op::Events { view: V::NAME.into(), key: key.clone(), after: last.load(Ordering::SeqCst) };
        self.subscribe(id, Box::new(request), Arc::new(on));
        feed
    }

    fn next_id(&self) -> u64 {
        let mut state = lock(&self.state);
        state.next_id += 1;
        state.next_id
    }

    /// Sends a one-off request now, or once connected. `waiter` gets the answer.
    fn request(&self, op: Op, waiter: Waiter) {
        let id = self.next_id();
        let Ok(text) = serde_json::to_string(&Request { id, op }) else {
            return waiter(Err("the request doesn't encode".into()));
        };
        let mut state = lock(&self.state);
        if let Some(status) = state.refused {
            drop(state);
            return waiter(Err(format!("the server refused the connection ({status})")));
        }
        state.waiting.insert(id, waiter);
        match &state.out {
            Some(out) => {
                let _ = out.send(Outgoing::Text(text));
            }
            None => state.queued.push(text),
        }
    }

    /// Starts a watch or feed under `id`: now if connected, and again after every reconnect.
    fn subscribe(&self, id: u64, request: Box<dyn Fn() -> Op + Send>, on: Arc<dyn Fn(Body) -> bool + Send + Sync>) {
        let mut state = lock(&self.state);
        if let Some(out) = &state.out
            && let Ok(text) = serde_json::to_string(&Request { id, op: request() })
        {
            let _ = out.send(Outgoing::Text(text));
        }
        state.live.insert(id, Live { request, on });
    }

    fn unsubscribe(&self, id: u64) {
        let mut state = lock(&self.state);
        if state.live.remove(&id).is_some()
            && let Some(out) = &state.out
            && let Ok(text) = serde_json::to_string(&Request { id, op: Op::Stop })
        {
            let _ = out.send(Outgoing::Text(text));
        }
    }

    /// A connection is up: send what's waiting and restart the live reads.
    fn connected(&self, out: Sender<Outgoing>, closer: Writer) {
        let mut state = lock(&self.state);
        for text in state.queued.drain(..) {
            let _ = out.send(Outgoing::Text(text));
        }
        for (&id, live) in &state.live {
            if let Ok(text) = serde_json::to_string(&Request { id, op: (live.request)() }) {
                let _ = out.send(Outgoing::Text(text));
            }
        }
        state.out = Some(out);
        state.closer = Some(closer);
        drop(state);
        self.status.publish(Connection::Connected);
    }

    /// The connection is down, or couldn't be made: whatever was waiting on it fails.
    fn disconnected(&self, status: Connection) {
        let failed: Vec<Waiter> = {
            let mut state = lock(&self.state);
            state.out = None;
            state.closer = None;
            state.queued.clear();
            if let Connection::Refused { status } = status {
                state.refused = Some(status);
            }
            state.waiting.drain().map(|(_, waiter)| waiter).collect()
        };
        let reason = match &status {
            Connection::Reconnecting { reason } => reason.clone(),
            Connection::Refused { status } => format!("the server refused the connection ({status})"),
            _ => "disconnected".into(),
        };
        for waiter in failed {
            waiter(Err(reason.clone()));
        }
        self.status.publish(status);
    }

    /// Hands one message to whatever is waiting for it, with no lock held.
    fn dispatch(&self, response: Response) {
        let mut state = lock(&self.state);
        if let Some(waiter) = state.waiting.remove(&response.id) {
            drop(state);
            return waiter(Ok(response.body));
        }
        let Some(on) = state.live.get(&response.id).map(|live| live.on.clone()) else { return };
        drop(state);
        if !on(response.body) {
            lock(&self.state).live.remove(&response.id);
        }
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        if let Some(closer) = lock(&self.state).closer.take() {
            closer.shutdown();
        }
    }
}

/// Stops a watch or feed when the reader drops it.
struct Unsubscribe {
    remote: Weak<Remote>,
    id: u64,
}

impl Drop for Unsubscribe {
    fn drop(&mut self) {
        if let Some(remote) = self.remote.upgrade() {
            remote.unsubscribe(self.id);
        }
    }
}

/// The connection thread: connects, reads until the connection drops, and tries again, until
/// every client is gone or the server refuses it. Holds the client only weakly while it waits.
fn run(remote: Weak<Remote>, url: String, authorization: Option<String>) {
    let mut backoff = BACKOFF.0;
    loop {
        let headers: Vec<(&str, &str)> = authorization.iter().map(|value| ("Authorization", value.as_str())).collect();
        let connected = domain_websocket::connect(&url, &headers);
        let Some(client) = remote.upgrade() else { return };
        let (mut reader, writer) = match connected {
            Ok(connection) => connection,
            Err(ConnectError::Refused(status @ (401 | 403))) => {
                return client.disconnected(Connection::Refused { status });
            }
            Err(e) => {
                client.disconnected(Connection::Reconnecting { reason: e.to_string() });
                drop(client);
                std::thread::sleep(backoff);
                backoff = (backoff * 2).min(BACKOFF.1);
                continue;
            }
        };
        backoff = BACKOFF.0;
        let Ok(closer) = writer.try_clone() else { continue };
        let (out, outgoing) = mpsc::channel::<Outgoing>();
        let mut writer = writer;
        let writing = std::thread::Builder::new().name("domain-client-writer".into()).spawn(move || {
            for message in outgoing {
                let written = match message {
                    Outgoing::Text(text) => writer.text(&text),
                    Outgoing::Pong(payload) => writer.pong(&payload),
                };
                if written.is_err() {
                    break;
                }
            }
            writer.shutdown();
        });
        client.connected(out.clone(), closer);
        drop(client);

        let reason = loop {
            match reader.read() {
                Ok(Message::Text(text)) => {
                    let Some(client) = remote.upgrade() else { return };
                    if let Ok(response) = serde_json::from_str::<Response>(&text) {
                        client.dispatch(response);
                    }
                }
                Ok(Message::Ping(payload)) => {
                    let _ = out.send(Outgoing::Pong(payload));
                }
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close) => break "the server closed the connection".to_string(),
                Err(e) => break e.to_string(),
            }
        };
        // Fail what's waiting first: that also drops the client's handle on the writer's queue,
        // so the writer thread can finish.
        if let Some(client) = remote.upgrade() {
            client.disconnected(Connection::Reconnecting { reason });
        }
        drop(out);
        if let Ok(writing) = writing {
            let _ = writing.join();
        }
        if remote.strong_count() == 0 {
            return;
        }
        std::thread::sleep(backoff);
    }
}

fn failure(body: Body) -> String {
    match body {
        Body::Err(Failure::Unavailable(reason)) | Body::Err(Failure::Invalid(reason)) => reason,
        other => format!("unexpected answer {other:?}"),
    }
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Value, String> {
    serde_json::to_value(value).map_err(|e| e.to_string())
}

fn decode<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|e| e.to_string())
}
