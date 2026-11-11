use crate::{Aggregate, Command};
use std::fmt;

/// Milliseconds since the Unix epoch. Serializes as that number.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// The system clock. Not available on wasm, which has none; hosts there take a clock.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn now() -> Self {
        let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        Self(since.as_millis() as u64)
    }
}

/// Identifies one command. Sending the same id twice decides it once.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommandId(String);

impl CommandId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CommandId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for CommandId {
    fn from(id: &str) -> Self {
        Self(id.into())
    }
}

impl From<String> for CommandId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

/// One aggregate instance's event stream, written `Account/a-1`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamId(String);

impl StreamId {
    pub fn of<A: Aggregate>(id: &A::Id) -> Self {
        Self::new(A::NAME, id)
    }

    /// `name/key`, such as a view key's stream of emitted events: `Statement/a-1`.
    pub fn new(name: &str, key: impl fmt::Display) -> Self {
        Self(format!("{name}/{key}"))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for StreamId {
    fn from(stream: String) -> Self {
        Self(stream)
    }
}

impl std::borrow::Borrow<str> for StreamId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A recorded event with where and when it was recorded.
#[derive(Clone, Debug, PartialEq)]
pub struct Envelope<E> {
    pub stream: StreamId,
    /// Position in the stream, from 1.
    pub seq: u64,
    pub at: Timestamp,
    /// The command that recorded it; `None` for events written directly, as a story's `given` does.
    pub command_id: Option<CommandId>,
    pub event: E,
}

/// What a command returns once its events commit.
#[derive(Debug)]
pub struct Receipt<C: Command> {
    pub stream: StreamId,
    /// The stream's version after the commit: the `seq` of its last event.
    pub version: u64,
    pub command_id: CommandId,
    pub events: Vec<C::Event>,
    pub reply: C::Reply,
}

impl<C: Command> Clone for Receipt<C> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            version: self.version,
            command_id: self.command_id.clone(),
            events: self.events.clone(),
            reply: self.reply.clone(),
        }
    }
}

impl<C: Command> PartialEq for Receipt<C> {
    fn eq(&self, other: &Self) -> bool {
        self.stream == other.stream
            && self.version == other.version
            && self.command_id == other.command_id
            && self.events == other.events
            && self.reply == other.reply
    }
}

/// Why sending a command didn't commit.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum SendError<R> {
    /// The aggregate refused it. Nothing was recorded.
    Rejected(R),
    /// This command id was already used for a different kind of command on this stream.
    CommandIdReused { stream: StreamId, command_id: CommandId },
    /// The outcome is unknown: the command may have committed. Send it again with the same
    /// `command_id` to find out; it takes effect at most once.
    Unavailable { command_id: CommandId, reason: String },
}

impl<R: fmt::Display> fmt::Display for SendError<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(r) => write!(f, "rejected: {r}"),
            Self::CommandIdReused { stream, command_id } => {
                write!(f, "command id {command_id} was already used for another command on {stream}")
            }
            Self::Unavailable { command_id, reason } => {
                write!(f, "unavailable ({reason}); it may have committed, so retry with command id {command_id}")
            }
        }
    }
}

impl<R: fmt::Debug + fmt::Display> std::error::Error for SendError<R> {}

impl<R> SendError<R> {
    /// The rejection, if the aggregate refused the command.
    pub fn rejection(&self) -> Option<&R> {
        match self {
            Self::Rejected(r) => Some(r),
            _ => None,
        }
    }
}
