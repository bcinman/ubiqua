//! `Send` and `Sync` on native targets, and no requirement on wasm, where there is one thread and
//! browser objects can't leave it. Bound on these instead of `Send` and `Sync` so a model and its
//! hosts also build for the browser.

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    pub trait MaybeSync: Sync {}
    impl<T: Sync + ?Sized> MaybeSync for T {}
}

#[cfg(target_arch = "wasm32")]
mod imp {
    pub trait MaybeSend {}
    impl<T: ?Sized> MaybeSend for T {}

    pub trait MaybeSync {}
    impl<T: ?Sized> MaybeSync for T {}
}

pub use imp::{MaybeSend, MaybeSync};
