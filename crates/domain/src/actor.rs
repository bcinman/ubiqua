//! Who is asking: the person or system behind a command or a read.

use crate::{MaybeSend, MaybeSync};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Who sent a command or asked for a read: the user a server's session check signed in, or
/// whoever an in-process client acts as. Holds a value of your own type, such as a `User`.
/// Commands read it with [`Cx::actor`](crate::Cx::actor), views with [`ReadCx::actor`].
#[derive(Clone)]
pub struct CurrentActor(Arc<dyn ActorValue>);

#[cfg(not(target_arch = "wasm32"))]
trait ActorValue: Any + fmt::Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}
#[cfg(target_arch = "wasm32")]
trait ActorValue: Any + fmt::Debug {
    fn as_any(&self) -> &dyn Any;
}

impl<T: Any + fmt::Debug + MaybeSend + MaybeSync> ActorValue for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl CurrentActor {
    pub fn new<T: Any + fmt::Debug + MaybeSend + MaybeSync>(actor: T) -> Self {
        Self(Arc::new(actor))
    }

    /// The actor, if it's a `T`.
    pub fn get<T: Any>(&self) -> Option<&T> {
        // On the value, not the `Arc`, which is an `ActorValue` too.
        (*self.0).as_any().downcast_ref()
    }
}

impl fmt::Debug for CurrentActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What a view's [`Validate`](crate::Validate) may read besides the key and its state.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReadCx<'a> {
    actor: Option<&'a CurrentActor>,
}

impl<'a> ReadCx<'a> {
    pub fn new(actor: Option<&'a CurrentActor>) -> Self {
        Self { actor }
    }

    /// Who is reading, if they're a `T`. `None` when nobody signed in, as for code in the app.
    pub fn actor<T: Any>(&self) -> Option<&'a T> {
        self.actor?.get()
    }
}
