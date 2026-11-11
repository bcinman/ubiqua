//! Predictions: a live view shows the commands sent but not answered yet as if they had taken
//! effect, using the view's `Predict` impls.
//!
//! A view's state arrives before the answer to a command that changed it, in process and over a
//! connection. So while commands wait, newer states are held back until all their answers arrive:
//! then the state that includes them replaces the predictions in one step, never showing one
//! twice. Commands that run in one batch are answered after it, so the state that includes one
//! may include those sent after it, still waiting; until they're answered too, one that
//! committed stays shown as predicted, and one that was refused is taken back.

use crate::Read;
use crate::live::Shared;
use crate::pending::lock;
use domain::host::{Keyed, Predictions};
use domain::{Command, MaybeSend, MaybeSync, Predict, View};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError, Weak};

/// How one waiting command changes one key's state.
#[cfg(not(target_arch = "wasm32"))]
type Change<V> = Box<dyn Fn(&<V as Keyed>::Key, &mut Option<<V as View>::State>) + Send>;
#[cfg(target_arch = "wasm32")]
type Change<V> = Box<dyn Fn(&<V as Keyed>::Key, &mut Option<<V as View>::State>)>;

/// Makes a [`Change`] from a command's target and the command, both as `&dyn Any`.
type Make<V> = fn(&dyn Any, &dyn Any) -> Change<V>;

/// The live views of a client and its clones, by the commands they predict.
#[derive(Default)]
pub(crate) struct Predictor {
    overlays: Mutex<HashMap<TypeId, Vec<Weak<dyn Predicting>>>>,
    next: AtomicU64,
}

impl Predictor {
    /// Shows `command` on every live view that predicts it, until the returned prediction drops.
    pub(crate) fn predict<C: Command>(&self, target: &C::Target, command: &C) -> Option<Prediction> {
        let overlays: Vec<Arc<dyn Predicting>> = {
            let mut overlays = lock(&self.overlays);
            let list = overlays.get_mut(&TypeId::of::<C>())?;
            list.retain(|overlay| overlay.strong_count() > 0);
            list.iter().filter_map(Weak::upgrade).collect()
        };
        if overlays.is_empty() {
            return None;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        for overlay in &overlays {
            overlay.add(id, target, command);
        }
        Some(Prediction { id, overlays: overlays.iter().map(Arc::downgrade).collect(), committed: false })
    }

    pub(crate) fn register<V: View>(&self, overlay: &Arc<Overlay<V>>)
    where
        Read<V>: MaybeSync,
    {
        let weak: Weak<dyn Predicting> = Arc::downgrade(overlay) as Weak<Overlay<V>>;
        let mut overlays = lock(&self.overlays);
        for command in overlay.makers.keys() {
            overlays.entry(*command).or_default().push(weak.clone());
        }
    }
}

/// A command shown on live views before its answer. [`answered`](Self::answered) tells them the
/// answer; dropping it unanswered takes it off them as if it had been refused.
pub(crate) struct Prediction {
    id: u64,
    overlays: Vec<Weak<dyn Predicting>>,
    committed: bool,
}

impl Prediction {
    pub(crate) fn answered(mut self, committed: bool) {
        self.committed = committed;
    }
}

impl Drop for Prediction {
    fn drop(&mut self) {
        for overlay in self.overlays.iter().filter_map(Weak::upgrade) {
            overlay.settle(self.id, self.committed);
        }
    }
}

trait Predicting: MaybeSend + MaybeSync {
    fn add(&self, id: u64, target: &dyn Any, command: &dyn Any);
    fn settle(&self, id: u64, committed: bool);
}

/// One live view's state from the model, with the waiting commands on top.
pub(crate) struct Overlay<V: View> {
    makers: HashMap<TypeId, Make<V>>,
    layers: Mutex<Layers<V>>,
    target: Arc<Shared<Read<V>>>,
    /// Held while publishing, so values go out in order.
    publishing: Mutex<()>,
    /// Set by a publish that found another running; that one publishes again for it.
    requested: AtomicBool,
    /// A state from the model with nothing waiting: publish it even if it looks the same, as a
    /// live view without predictions does.
    fresh: AtomicBool,
}

struct Layers<V: View> {
    key: V::Key,
    /// The newest state from the model.
    latest: Read<V>,
    /// What the predictions sit on: `latest` as of the last time nothing was waiting.
    base: Read<V>,
    /// Commands sent since `base`, oldest first, and whether each one's answer is in. All of
    /// them change what's shown: a refused one is removed.
    waiting: Vec<Waiting<V>>,
}

struct Waiting<V: View> {
    id: u64,
    change: Change<V>,
    answered: bool,
}

impl<V: View> Overlay<V> {
    pub(crate) fn new(key: V::Key, initial: Read<V>) -> Arc<Self> {
        let mut makers = Makers::<V>(HashMap::new());
        V::predictions(&mut makers);
        Arc::new(Self {
            makers: makers.0,
            layers: Mutex::new(Layers { key, latest: initial.clone(), base: initial.clone(), waiting: Vec::new() }),
            target: Shared::new(initial),
            publishing: Mutex::new(()),
            requested: AtomicBool::new(false),
            fresh: AtomicBool::new(false),
        })
    }

    pub(crate) fn target(&self) -> Arc<Shared<Read<V>>> {
        self.target.clone()
    }

    /// A new state from the model.
    pub(crate) fn confirm(&self, read: Read<V>) {
        {
            let mut layers = lock(&self.layers);
            if layers.waiting.is_empty() {
                layers.base = read.clone();
                self.fresh.store(true, Ordering::SeqCst);
            }
            layers.latest = read;
        }
        self.publish();
    }

    /// Publishes the base with the waiting commands on top, unless that's what it shows already.
    /// One publish at a time, without a lock held while callbacks run, so they may send.
    fn publish(&self) {
        self.requested.store(true, Ordering::SeqCst);
        loop {
            let publishing = match self.publishing.try_lock() {
                Ok(guard) => guard,
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(TryLockError::WouldBlock) => return,
            };
            self.requested.store(false, Ordering::SeqCst);
            let fresh = self.fresh.swap(false, Ordering::SeqCst);
            let shown = {
                let layers = lock(&self.layers);
                let mut shown = layers.base.clone();
                if let Ok(state) = &mut shown {
                    layers.waiting.iter().for_each(|waiting| (waiting.change)(&layers.key, state));
                }
                shown
            };
            if fresh {
                self.target.publish(shown);
            } else {
                self.target.publish_if_changed(shown);
            }
            drop(publishing);
            if !self.requested.load(Ordering::SeqCst) {
                return;
            }
        }
    }
}

impl<V: View> Predicting for Overlay<V>
where
    Read<V>: MaybeSync,
{
    fn add(&self, id: u64, target: &dyn Any, command: &dyn Any) {
        let Some(make) = self.makers.get(&command.type_id()) else { return };
        lock(&self.layers).waiting.push(Waiting { id, change: make(target, command), answered: false });
        self.publish();
    }

    fn settle(&self, id: u64, committed: bool) {
        {
            let mut layers = lock(&self.layers);
            let Some(at) = layers.waiting.iter().position(|waiting| waiting.id == id) else { return };
            if committed {
                layers.waiting[at].answered = true;
            } else {
                layers.waiting.remove(at);
            }
            // `latest` includes every command answered so far, and perhaps some still waiting,
            // so it replaces the predictions only once none is.
            if layers.waiting.iter().all(|waiting| waiting.answered) {
                layers.waiting.clear();
                layers.base = layers.latest.clone();
            }
        }
        self.publish();
    }
}

struct Makers<V: View>(HashMap<TypeId, Make<V>>);

impl<V: View> Predictions<V> for Makers<V> {
    fn predicts<C: Command + Clone>(&mut self)
    where
        V: Predict<C>,
    {
        self.0.insert(TypeId::of::<C>(), make::<V, C>);
    }
}

fn make<V: Predict<C>, C: Command + Clone>(target: &dyn Any, command: &dyn Any) -> Change<V> {
    let target = target.downcast_ref::<C::Target>().expect("a command's target").clone();
    let command = command.downcast_ref::<C>().expect("the command it was made for").clone();
    Box::new(move |key, state| V::predict(key, state, &target, &command))
}
