//! The protocol between a client and a server: JSON text messages over a WebSocket. Values such as
//! ids, commands, states, events and rejections are in their serde JSON form, as in storage.
//!
//! The client numbers each request. The server answers with that number: once for `send`, `get`,
//! `snapshot` and `list`; for `watch` and `events`, until the client sends `stop` or the read is
//! refused.
//!
//! ```text
//! → {"id":1,"op":"send","command":"DepositMoney","target":"sam-main","command_id":"c-1","input":{"amount":100}}
//! ← {"id":1,"ok":{"stream":"Account/sam-main","version":2,"command_id":"c-1","events":[..],"reply":{"balance":100}}}
//! → {"id":2,"op":"watch","view":"AccountSummary","key":"sam-main"}
//! ← {"id":2,"state":{"owner":"sam","balance":100}}
//! ← {"id":2,"refused":{"NotOwner":null}}
//! ```
//!
//! A server that allows it also serves [telemetry](crate::telemetry): the model's parts, its
//! streams, and a trace of everything that happens, outside the Api and its views' `validate`.
//!
//! ```text
//! → {"id":3,"op":"trace","since":"start"}
//! ← {"id":3,"trace":{"event":{"aggregate":"Account","stream":"Account/sam-main","seq":1,..}}}
//! ← {"id":3,"trace":{"caught_up":{"streams":{"Account/sam-main":1}}}}
//! ```

use crate::telemetry::{Since, Trace};
use crate::{Command, CommandId, Cursor, Entry, Receipt, Snapshot, StreamId, Timestamp, View};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

/// A client's message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    #[serde(flatten)]
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    /// Send a command: answered with a receipt, or `err`.
    Send { command: String, target: Value, command_id: String, input: Value },
    /// A key's state, or `null`. With `at_least`, once the view has applied that position.
    Get {
        view: String,
        key: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at_least: Option<At>,
    },
    /// `{"state":..,"cursor":n}`.
    Snapshot { view: String, key: Value },
    /// Up to `take` entries after `after`.
    List { view: String, key: Value, after: u64, take: usize },
    /// The key's state now, then each time it changes, as `state` messages.
    Watch { view: String, key: Value },
    /// The key's entries after `after`, then `caught_up`, then new ones as they're recorded.
    Events { view: String, key: Value, after: u64 },
    /// Ends the `watch`, `events` or `trace` with this id.
    Stop,
    /// Telemetry: the model's parts, as a `Catalog`.
    Catalog,
    /// Telemetry: every stream's version and what's failing, as a `Status`.
    Status,
    /// Telemetry: one aggregate instance at `at`, or now, as an `AggregateTrace` or `null`.
    Aggregate {
        stream: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        at: Option<u64>,
    },
    /// Telemetry: every key of a view, as `ViewKey`s.
    ViewKeys { view: String },
    /// Telemetry: what happened since `since` as `trace` messages, then what happens, until `stop`.
    Trace { since: Since },
}

/// A stream at a version, such as a receipt's.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct At {
    pub stream: String,
    pub version: u64,
}

/// A server's message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(flatten)]
    pub body: Body,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    /// The answer to `send`, `get`, `snapshot` or `list`.
    Ok(Value),
    Err(Failure),
    /// A watched key's state.
    State(Value),
    /// One of a key's emitted events.
    Entry(WireEntry),
    /// Everything recorded so far has been sent; the number is the cursor.
    CaughtUp(u64),
    /// The view's `validate` refused the read. Nothing more follows for this id.
    Refused(Value),
    /// One step of a `trace`.
    Trace(Box<Trace>),
}

/// Why a request failed.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Failure {
    /// The aggregate refused the command.
    Rejected(Value),
    CommandIdReused {
        stream: String,
        command_id: String,
    },
    /// The outcome is unknown.
    Unavailable(String),
    NotCaughtUp {
        view: String,
        stream: String,
        wanted: u64,
        applied: u64,
    },
    /// A command or view the Api doesn't list, or a request that doesn't decode.
    Invalid(String),
}

/// One of a view's emitted events, with the key's state after it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WireEntry {
    pub cursor: u64,
    pub event: Value,
    pub state: Value,
    pub stream: String,
    pub seq: u64,
    pub at: u64,
}

pub fn encode_receipt<C: Command>(receipt: &Receipt<C>) -> Result<Value, String> {
    let Receipt { stream, version, command_id, events, reply } = receipt;
    let events = serde_json::to_value(events).map_err(|e| e.to_string())?;
    let reply = serde_json::to_value(reply).map_err(|e| e.to_string())?;
    Ok(
        json!({ "stream": stream.as_str(), "version": version, "command_id": command_id.as_str(), "events": events, "reply": reply }),
    )
}

pub fn decode_receipt<C: Command>(value: Value) -> Result<Receipt<C>, String> {
    #[derive(Deserialize)]
    #[serde(bound = "")]
    struct Wire<C: Command> {
        stream: String,
        version: u64,
        command_id: String,
        events: Vec<C::Event>,
        reply: C::Reply,
    }
    let Wire::<C> { stream, version, command_id, events, reply } =
        serde_json::from_value(value).map_err(|e| e.to_string())?;
    Ok(Receipt { stream: StreamId::from(stream), version, command_id: CommandId::new(command_id), events, reply })
}

pub fn encode_snapshot<V: View>(snapshot: &Snapshot<V>) -> Result<Value, String> {
    let state = serde_json::to_value(&snapshot.state).map_err(|e| e.to_string())?;
    Ok(json!({ "state": state, "cursor": snapshot.cursor.seq() }))
}

pub fn decode_snapshot<V: View>(value: Value) -> Result<Snapshot<V>, String> {
    #[derive(Deserialize)]
    #[serde(bound = "")]
    struct Wire<V: View> {
        state: Option<V::State>,
        cursor: u64,
    }
    let Wire::<V> { state, cursor } = serde_json::from_value(value).map_err(|e| e.to_string())?;
    Ok(Snapshot { state, cursor: Cursor::new(cursor) })
}

pub fn encode_entry<V: View>(entry: &Entry<V>) -> Result<WireEntry, String> {
    Ok(WireEntry {
        cursor: entry.cursor.seq(),
        event: serde_json::to_value(&entry.event).map_err(|e| e.to_string())?,
        state: serde_json::to_value(&*entry.state).map_err(|e| e.to_string())?,
        stream: entry.stream.to_string(),
        seq: entry.seq,
        at: entry.at.as_millis(),
    })
}

pub fn decode_entry<V: View>(entry: WireEntry) -> Result<Entry<V>, String> {
    Ok(Entry {
        cursor: Cursor::new(entry.cursor),
        event: serde_json::from_value(entry.event).map_err(|e| e.to_string())?,
        state: Arc::new(serde_json::from_value(entry.state).map_err(|e| e.to_string())?),
        stream: StreamId::from(entry.stream),
        seq: entry.seq,
        at: Timestamp::from_millis(entry.at),
    })
}
