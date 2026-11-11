use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};

/// A result that arrives later. Check it each frame with [`try_get`](Self::try_get), or `.await`
/// it on any executor: it only uses the waker it's polled with.
pub struct Pending<T> {
    slot: Arc<Mutex<Slot<T>>>,
}

struct Slot<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

/// Fills a [`Pending`].
pub(crate) struct Resolver<T> {
    slot: Arc<Mutex<Slot<T>>>,
}

pub(crate) fn pending<T>() -> (Pending<T>, Resolver<T>) {
    let slot = Arc::new(Mutex::new(Slot { value: None, waker: None }));
    (Pending { slot: slot.clone() }, Resolver { slot })
}

impl<T> Pending<T> {
    /// Already resolved.
    pub fn ready(value: T) -> Self {
        Self { slot: Arc::new(Mutex::new(Slot { value: Some(value), waker: None })) }
    }

    /// The result, once it has arrived. Returns it once; later calls return `None`.
    pub fn try_get(&self) -> Option<T> {
        lock(&self.slot).value.take()
    }
}

impl<T> Resolver<T> {
    pub fn resolve(self, value: T) {
        let waker = {
            let mut slot = lock(&self.slot);
            slot.value = Some(value);
            slot.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Future for Pending<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut slot = lock(&self.slot);
        match slot.value.take() {
            Some(value) => Poll::Ready(value),
            None => {
                slot.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub(crate) fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}
