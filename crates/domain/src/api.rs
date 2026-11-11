//! What a model publishes to its clients: the commands they may send and the views they may read.

use crate::{AggregateId, Apply, Command, Handles, Validate, View};

/// The commands and views a client may use. Declare it with `#[domain::api(..)]`.
pub trait Api: 'static {
    const NAME: &'static str;

    /// Visits each command and view it lists, so a server can serve them.
    fn register<R: ApiRegistry>(registry: &mut R);
}

/// Receives an Api's commands and views. Servers implement it.
pub trait ApiRegistry {
    fn command<C: Command>(&mut self)
    where
        <C::Target as AggregateId>::Aggregate: Handles<C>;
    fn view<V: Apply + Validate>(&mut self);
}

/// The Api lets clients send `C`. `#[domain::api(..)]` implements it for each of `commands(..)`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't let clients send `{C}`",
    label = "add `{C}` to `#[domain::api(commands(..))]`"
)]
pub trait Sends<C: Command>: Api {}

/// The Api lets clients read `V`. `#[domain::api(..)]` implements it for each of `views(..)`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't let clients read `{V}`",
    label = "add `{V}` to `#[domain::api(views(..))]`"
)]
pub trait Reads<V: View>: Api {}

/// `C` may be sent through `A`; holds whenever `A: Sends<C>`. Client methods bound on this instead
/// of `Sends`, so the compiler learns the command from the argument before it checks the Api and
/// names the command in the error.
#[doc(hidden)]
pub trait SentVia<A> {}
impl<C: Command, A: Sends<C>> SentVia<A> for C {}
