use crate::pending::lock;
use domain::{MaybeSend, MaybeSync};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[cfg(not(target_arch = "wasm32"))]
type Callback<T> = Arc<dyn Fn(&Arc<T>, u64) + Send + Sync>;
#[cfg(target_arch = "wasm32")]
type Callback<T> = Arc<dyn Fn(&Arc<T>, u64)>;

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type KeepAlive = Arc<dyn Any + Send + Sync>;
#[cfg(target_arch = "wasm32")]
pub(crate) type KeepAlive = Arc<dyn Any>;

/// The latest value of something that changes, such as one view key's state. Read it three ways,
/// so it fits any UI framework:
///
/// - pull: [`latest`](Self::latest) each frame, which never waits;
/// - push: [`on_change`](Self::on_change) callbacks;
/// - async: [`changed`](Self::changed)`.await` on any executor.
///
/// Fast changes coalesce: it holds the latest value only. Dropping every clone stops the updates.
pub struct LiveView<T> {
    shared: Arc<Shared<T>>,
    /// Whatever feeds it, kept running while the view is held.
    _source: KeepAlive,
}

pub(crate) struct Shared<T> {
    inner: Mutex<Inner<T>>,
}

struct Inner<T> {
    value: Arc<T>,
    version: u64,
    wakers: Vec<Waker>,
    callbacks: Vec<(u64, Callback<T>)>,
    next_listener: u64,
}

impl<T> Clone for LiveView<T> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), _source: self._source.clone() }
    }
}

impl<T: MaybeSend + MaybeSync + 'static> LiveView<T> {
    /// A view of `shared`, which `source` keeps current for as long as the view is held.
    pub(crate) fn new(shared: Arc<Shared<T>>, source: KeepAlive) -> Self {
        Self { shared, _source: source }
    }

    /// The latest value and its version, which counts the changes so far.
    pub fn latest(&self) -> (Arc<T>, u64) {
        let inner = lock(&self.shared.inner);
        (inner.value.clone(), inner.version)
    }

    /// Calls `f` with each new value. It runs on the thread that made the change, usually the
    /// domain's, so it should only schedule work on the UI thread. Stops when the `Listener` drops.
    pub fn on_change(&self, f: impl Fn(&Arc<T>, u64) + MaybeSend + MaybeSync + 'static) -> Listener<T> {
        let mut inner = lock(&self.shared.inner);
        inner.next_listener += 1;
        let id = inner.next_listener;
        inner.callbacks.push((id, Arc::new(f)));
        Listener { shared: self.shared.clone(), id }
    }

    /// Resolves with the value once its version is past `seen`.
    pub fn changed(&self, seen: u64) -> Changed<T> {
        Changed { shared: self.shared.clone(), seen }
    }
}

impl<T> Shared<T> {
    pub(crate) fn new(initial: T) -> Arc<Self> {
        let inner =
            Inner { value: Arc::new(initial), version: 0, wakers: Vec::new(), callbacks: Vec::new(), next_listener: 0 };
        Arc::new(Shared { inner: Mutex::new(inner) })
    }

    /// Sets the value, wakes waiters, then runs callbacks, with no lock held.
    pub(crate) fn publish(&self, value: T) {
        let (value, version, wakers, callbacks) = {
            let mut inner = lock(&self.inner);
            inner.value = Arc::new(value);
            inner.version += 1;
            let callbacks: Vec<Callback<T>> = inner.callbacks.iter().map(|(_, f)| f.clone()).collect();
            (inner.value.clone(), inner.version, std::mem::take(&mut inner.wakers), callbacks)
        };
        wakers.into_iter().for_each(Waker::wake);
        for f in callbacks {
            f(&value, version);
        }
    }
}

/// An [`on_change`](LiveView::on_change) callback. Dropping it unsubscribes.
pub struct Listener<T> {
    shared: Arc<Shared<T>>,
    id: u64,
}

impl<T> Drop for Listener<T> {
    fn drop(&mut self) {
        lock(&self.shared.inner).callbacks.retain(|(id, _)| *id != self.id);
    }
}

/// The future [`LiveView::changed`] returns: the new value and its version.
pub struct Changed<T> {
    shared: Arc<Shared<T>>,
    seen: u64,
}

impl<T> Future for Changed<T> {
    type Output = (Arc<T>, u64);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = lock(&self.shared.inner);
        if inner.version > self.seen {
            Poll::Ready((inner.value.clone(), inner.version))
        } else {
            inner.wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}
