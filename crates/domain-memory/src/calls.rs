//! External system calls: answered by an adapter registered with `Memory::external`, or, when
//! calls are stubbed, by whoever takes them with `Memory::next_call`, as a story does.

use crate::views::AnyBox;
use crate::{Memory, lock};
use domain::host::{Adapter, Calls};
use domain::{Answer, Answered, Call};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// A boxed future that can cross threads where there are threads.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T>>>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type AnswerFn = Arc<dyn Fn(AnyBox) -> BoxFuture<AnyBox> + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub(crate) type AnswerFn = Arc<dyn Fn(AnyBox) -> BoxFuture<AnyBox>>;

/// Each call's adapter, by the call's `TypeId`.
pub(crate) struct Answers<'a>(pub &'a mut HashMap<TypeId, AnswerFn>);

impl Calls for Answers<'_> {
    fn answer<C: Call, I: Answer<C>>(&mut self, adapter: Arc<I>) {
        let answer: AnswerFn = Arc::new(move |call: AnyBox| {
            let call = *call.downcast::<C>().expect("a call matches its TypeId");
            let adapter = adapter.clone();
            Box::pin(async move { Box::new(adapter.answer(call).await) as AnyBox })
        });
        self.0.insert(TypeId::of::<C>(), answer);
    }
}

impl Memory {
    /// Answers `system`'s calls with `adapter`, the code that talks to the vendor.
    ///
    /// ```ignore
    /// let bank = Memory::new().with::<Bank>().external(FraudCheck, VendorFraud::new(key));
    /// ```
    pub fn external<S: Adapter<I>, I>(mut self, _system: S, adapter: I) -> Self {
        S::register(Arc::new(adapter), &mut Answers(&mut self.answers));
        self
    }

    /// Holds calls no adapter answers until they're taken with [`next_call`](Self::next_call)
    /// and answered. Stories do this.
    pub fn stub_calls(mut self) -> Self {
        self.stub_calls = true;
        self
    }

    /// The oldest call waiting for an answer, if calls are stubbed.
    pub fn next_call(&self) -> Option<StubbedCall> {
        lock(&self.stubbed).pop_front()
    }

    /// Every call waiting for an answer, described.
    pub fn waiting_calls(&self) -> Vec<String> {
        lock(&self.stubbed).iter().map(|call| call.to_string()).collect()
    }

    /// Starts `call`: through its adapter, held for an answer, or failed when neither applies.
    pub(crate) fn start_call<C: Call>(&self, call: C) -> BoxFuture<AnyBox> {
        if let Some(answer) = self.answers.get(&TypeId::of::<C>()) {
            return answer(Box::new(call));
        }
        if !self.stub_calls {
            let reason = format!("nothing answers {}.{}: pass an adapter to `Memory::external`", C::SYSTEM, C::NAME);
            let failed: Answered<C> = Err(domain::CallError::Failed(reason));
            return Box::pin(std::future::ready(Box::new(failed) as AnyBox));
        }
        let slot = Arc::new(Mutex::new(Slot { answer: None, waker: None }));
        let describe = format!("{}.{} {call:?}", C::SYSTEM, C::NAME);
        lock(&self.stubbed).push_back(StubbedCall { describe, call: Box::new(call), slot: slot.clone() });
        Box::pin(Waiting { slot })
    }
}

/// A call held for an answer. Check it with [`args`](Self::args), then [`answer`](Self::answer) it.
pub struct StubbedCall {
    describe: String,
    call: AnyBox,
    slot: Arc<Mutex<Slot>>,
}

impl StubbedCall {
    /// The call's arguments, if it's a `C`.
    pub fn args<C: Call>(&self) -> Option<&C> {
        (*self.call).downcast_ref::<C>()
    }

    /// Answers the call, waking the policy that made it. Run it with `Memory::run_due`.
    ///
    /// # Panics
    /// When the call isn't a `C`.
    pub fn answer<C: Call>(self, answer: Answered<C>) {
        assert!(self.args::<C>().is_some(), "{self} isn't a {}.{}", C::SYSTEM, C::NAME);
        let waker = {
            let mut slot = lock(&self.slot);
            slot.answer = Some(Box::new(answer));
            slot.waker.take()
        };
        waker.into_iter().for_each(Waker::wake);
    }
}

impl fmt::Display for StubbedCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.describe)
    }
}

impl fmt::Debug for StubbedCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.describe)
    }
}

struct Slot {
    answer: Option<AnyBox>,
    waker: Option<Waker>,
}

struct Waiting {
    slot: Arc<Mutex<Slot>>,
}

impl Future for Waiting {
    type Output = AnyBox;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<AnyBox> {
        let mut slot = lock(&self.slot);
        match slot.answer.take() {
            Some(answer) => Poll::Ready(answer),
            None => {
                slot.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

/// Unboxes an answer started by `start_call::<C>`.
pub(crate) fn unbox<C: Call>(answer: AnyBox) -> Answered<C> {
    let answer: Box<dyn Any> = answer;
    *answer.downcast::<Answered<C>>().expect("an answer matches its call")
}
