//! One thread owns a [`Memory`] and makes every change to it: commands, views, policies,
//! schedules, and the store's writes. Clients, servers and other threads hand it jobs.
//!
//! It runs the jobs that have queued up together, in one store batch, so a durable store such as
//! `domain-sqlite` syncs to disk once per batch rather than once per command. Each job's answer
//! goes out once its batch is durable.
//!
//! Reads don't queue: view readers and telemetry read the views directly, from any thread.

use crate::{Memory, reason};
use domain::{Apply, CommandId, MaybeSend, Timestamp};
use std::sync::Arc;

/// The most jobs one batch runs.
#[cfg(not(target_arch = "wasm32"))]
const MAX_BATCH: usize = 256;

/// Runs a job, given the memory, or `None` when the runtime can't run it. Returns how to answer
/// once the batch it ran in is durable, or isn't.
#[cfg(not(target_arch = "wasm32"))]
type Job = Box<dyn FnOnce(Option<&Memory>) -> Answer + Send>;
#[cfg(target_arch = "wasm32")]
type Job = Box<dyn FnOnce(Option<&Memory>) -> Answer>;

#[cfg(not(target_arch = "wasm32"))]
type Answer = Box<dyn FnOnce(Result<(), String>) + Send>;
#[cfg(target_arch = "wasm32")]
type Answer = Box<dyn FnOnce(Result<(), String>)>;

/// A [`Memory`] and the thread that changes it. Clone it to share; the thread stops once every
/// clone is dropped.
#[derive(Clone)]
pub struct Runtime {
    inner: Arc<Inner>,
}

struct Inner {
    memory: Arc<Memory>,
    /// Jobs for the runtime's thread; `None` runs them on the caller's thread.
    #[cfg(not(target_arch = "wasm32"))]
    jobs: Option<std::sync::mpsc::Sender<Job>>,
}

impl Runtime {
    /// Moves `memory` onto a thread of its own, which runs jobs as they come, and the model's
    /// policies and scheduled commands between them. On wasm, which has no threads, this is
    /// [`inline`](Self::inline).
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spawn(memory: Memory) -> Self {
        use std::sync::mpsc::RecvTimeoutError;
        let (jobs, queue) = std::sync::mpsc::channel::<Job>();
        let memory = Arc::new(memory);
        let inner = Arc::new(Inner { memory: memory.clone(), jobs: Some(jobs) });
        // An empty job wakes the thread when policies have work. Weak, so the thread still stops
        // once every handle is gone.
        let wake = Arc::downgrade(&inner);
        memory.on_wake(move || {
            if let Some(inner) = wake.upgrade() {
                inner.post(Box::new(|_| Box::new(|_| {})));
            }
        });
        std::thread::Builder::new()
            .name("domain".into())
            .spawn(move || {
                // Why a batch didn't commit, once one hasn't.
                let mut failed = None;
                let mut due = None;
                loop {
                    let first = match due {
                        Some(at) => queue.recv_timeout(memory.now().until(at)),
                        None => queue.recv().map_err(|_| RecvTimeoutError::Disconnected),
                    };
                    let mut batch = match first {
                        Ok(job) => vec![job],
                        Err(RecvTimeoutError::Timeout) => Vec::new(),
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    batch.extend(std::iter::from_fn(|| queue.try_recv().ok()).take(MAX_BATCH - 1));
                    run_batch(&memory, batch, &mut failed);
                    due = None;
                    if failed.is_none() {
                        let run_due = || {
                            let run = std::panic::AssertUnwindSafe(|| memory.run_due());
                            std::panic::catch_unwind(run).ok().flatten()
                        };
                        let (next, outcome) = memory.batch(run_due);
                        due = next;
                        note_failure(outcome, &mut failed);
                    }
                }
            })
            .expect("spawn the domain thread");
        Self { inner }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn spawn(memory: Memory) -> Self {
        Self::inline(memory)
    }

    /// Runs each job on the thread that hands it in, at once: for tests, and on the web. Policies
    /// run on whichever thread wakes them. Scheduled commands fire when something calls
    /// `Memory::run_due`; on the web, call it from a timer.
    pub fn inline(memory: Memory) -> Self {
        let memory = Arc::new(memory);
        let weak = Arc::downgrade(&memory);
        memory.on_wake(move || {
            if let Some(memory) = weak.upgrade() {
                memory.run_due();
            }
        });
        #[cfg(not(target_arch = "wasm32"))]
        let inner = Inner { memory, jobs: None };
        #[cfg(target_arch = "wasm32")]
        let inner = Inner { memory };
        Self { inner: Arc::new(inner) }
    }

    /// Runs `job` on the runtime's thread, then calls `then` with what it returned, once what it
    /// wrote is durable. `then` gets `Err` when the job panicked, or its batch didn't commit; a
    /// command may then have committed or not, as with `SendError::Unavailable`.
    pub fn exec<T: MaybeSend + 'static>(
        &self,
        job: impl FnOnce(&Memory) -> T + MaybeSend + 'static,
        then: impl FnOnce(Result<T, String>) + MaybeSend + 'static,
    ) {
        self.inner.post(Box::new(move |memory: Option<&Memory>| -> Answer {
            let Some(memory) = memory else {
                return Box::new(move |outcome: Result<(), String>| then(Err(outcome.err().unwrap_or_default())));
            };
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job(memory))) {
                Ok(value) => Box::new(move |outcome: Result<(), String>| then(outcome.map(|()| value))),
                Err(panic) => {
                    let reason = reason(&*panic);
                    Box::new(move |_| then(Err(reason)))
                }
            }
        }));
    }

    /// Runs `job` on the runtime's thread and waits for what it returns, once what it wrote is
    /// durable: for tests and scripts.
    ///
    /// # Panics
    /// On the runtime's own thread, such as in a `then`, where it would wait forever.
    pub fn call<T: MaybeSend + 'static>(
        &self,
        job: impl FnOnce(&Memory) -> T + MaybeSend + 'static,
    ) -> Result<T, String> {
        #[cfg(not(target_arch = "wasm32"))]
        assert!(
            self.inner.jobs.is_none() || std::thread::current().name() != Some("domain"),
            "`Runtime::call` on the runtime's own thread would wait forever; use `exec`"
        );
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        self.exec(job, move |result| {
            let _ = answer.send(result);
        });
        answered.recv().unwrap_or_else(|_| Err("didn't run: the domain thread stopped".into()))
    }

    /// Reads a view, on the caller's thread.
    pub fn view<V: Apply>(&self) -> crate::ViewReader<V> {
        self.inner.memory.view::<V>()
    }

    /// The model's telemetry, read on the caller's thread.
    pub fn telemetry(&self) -> crate::Telemetry<'_> {
        self.inner.memory.telemetry()
    }

    /// A command id no other command sent here has used.
    pub fn new_command_id(&self) -> CommandId {
        self.inner.memory.new_command_id()
    }

    /// The time now, by the memory's clock.
    pub fn now(&self) -> Timestamp {
        self.inner.memory.now()
    }
}

impl Inner {
    fn post(&self, job: Job) {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(jobs) = &self.jobs {
            // Only fails once the thread has stopped, after a panic outside any job.
            if let Err(std::sync::mpsc::SendError(job)) = jobs.send(job) {
                job(None)(Err("didn't run: the domain thread stopped".into()));
            }
            return;
        }
        let answer = job(Some(&self.memory));
        answer(Ok(()));
    }
}

/// Runs `jobs` in one store batch, then answers each.
#[cfg(not(target_arch = "wasm32"))]
fn run_batch(memory: &Memory, jobs: Vec<Job>, failed: &mut Option<String>) {
    if jobs.is_empty() {
        return;
    }
    let (answers, outcome) = match failed {
        Some(e) => {
            let refused = format!("didn't run: the store failed earlier ({e}); restart to carry on from what it holds");
            (jobs.into_iter().map(|job| job(None)).collect::<Vec<_>>(), Err(refused))
        }
        None => {
            let (answers, outcome) = memory.batch(|| jobs.into_iter().map(|job| job(Some(memory))).collect());
            note_failure(outcome.clone(), failed);
            (answers, outcome.map_err(|e| format!("wasn't stored: the store didn't commit its batch ({e})")))
        }
    };
    for answer in answers {
        // A panicking `then` mustn't stop the answers after it.
        let outcome = outcome.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || answer(outcome)));
    }
}

/// Once a batch didn't commit, memory is ahead of the store, so nothing more runs.
#[cfg(not(target_arch = "wasm32"))]
fn note_failure(outcome: Result<(), String>, failed: &mut Option<String>) {
    if let Err(e) = outcome {
        failed.get_or_insert(e);
    }
}
