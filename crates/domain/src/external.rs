//! External systems: services outside the model, such as a payment processor, named by the calls
//! the model makes to them in its own words. An adapter answers the calls; a story answers them
//! itself.

use crate::Rejections;
use std::fmt::{self, Debug};
use std::future::Future;
use std::rc::Rc;

/// One call the model makes to an outside system; the struct holds its arguments.
/// Derive it with `#[derive(Call)]`.
pub trait Call: Debug + PartialEq + 'static {
    const NAME: &'static str;
    /// The system's name, such as `FraudCheck`.
    const SYSTEM: &'static str;
    /// What a successful call returns.
    type Returns: Debug + PartialEq + 'static;
    /// The refusals the system can answer with, which the model has an answer for.
    type Error: Rejections;
}

/// Why a call didn't return.
#[derive(Clone, Debug, PartialEq)]
pub enum CallError<E> {
    /// The system refused, with one of the call's declared errors.
    Refused(E),
    /// It failed some other way: a timeout, a 500. The policy that made the call runs again.
    Failed(String),
}

impl<E: fmt::Display> fmt::Display for CallError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(e) => write!(f, "refused: {e}"),
            Self::Failed(reason) => write!(f, "failed: {reason}"),
        }
    }
}

impl<E: Debug + fmt::Display> std::error::Error for CallError<E> {}

/// What a call's future resolves to.
pub type Answered<C> = Result<<C as Call>::Returns, CallError<<C as Call>::Error>>;

/// An adapter's answer to one call: the only code that talks to the vendor.
#[diagnostic::on_unimplemented(
    message = "`{Self}` doesn't answer `{C}`",
    label = "implement `Answer<{C}>` for `{Self}`"
)]
pub trait Answer<C: Call>: 'static {
    fn answer(&self, call: C) -> impl Future<Output = Answered<C>>;
}

/// An outside system and the calls the model makes to it. Derive it with
/// `#[derive(ExternalSystem)]`.
pub trait ExternalSystem: 'static {
    const NAME: &'static str;
}

/// `I` answers every call of this system. `#[derive(ExternalSystem)]` implements it.
#[diagnostic::on_unimplemented(
    message = "`{I}` doesn't answer every call of `{Self}`",
    label = "needs `impl Answer<..> for {I}` for each call in `#[external_system(calls(..))]`"
)]
pub trait Adapter<I>: ExternalSystem {
    fn register<R: Calls>(adapter: Rc<I>, calls: &mut R);
}

/// Receives an adapter's answers. Hosts implement it.
pub trait Calls {
    fn answer<C: Call, I: Answer<C>>(&mut self, adapter: Rc<I>);
}
