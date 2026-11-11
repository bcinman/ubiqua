//! What a model publishes to its clients: the commands they may send and the views they may read.

use crate::{Command, View};

/// The commands and views a client may use. Derive it with `#[derive(Api)]`.
pub trait Api: 'static {
    const NAME: &'static str;
}

/// The Api lets clients send `C`. `#[derive(Api)]` implements it for each of `commands(..)`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't let clients send `{C}`",
    label = "add `{C}` to `#[api(commands(..))]`"
)]
pub trait Sends<C: Command>: Api {}

/// The Api lets clients read `V`. `#[derive(Api)]` implements it for each of `views(..)`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't let clients read `{V}`",
    label = "add `{V}` to `#[api(views(..))]`"
)]
pub trait Reads<V: View>: Api {}

/// `C` may be sent through `A`; holds whenever `A: Sends<C>`. Client methods bound on this instead
/// of `Sends`, so the compiler learns the command from the argument before it checks the Api and
/// names the command in the error.
#[doc(hidden)]
pub trait SentVia<A> {}
impl<C: Command, A: Sends<C>> SentVia<A> for C {}
