//! One thread owns a [`Memory`] and makes every change to it: commands, views, policies,
//! schedules, and the store's writes. Clients, servers and other threads hand it jobs.
//!
//! It runs the jobs that have queued up together, in one store batch, so a durable store such as
//! `domain-sqlite` syncs to disk once per batch rather than once per command. Each job's answer
//! goes out once its batch is durable.
//!
//! Reads queue too: a view, a subscription or telemetry is read in a job, so nothing the thread
//! owns is ever shared or locked. What a job returns, such as a value or a live feed, crosses
//! back.

#[cfg(not(target_arch = "wasm32"))]
use crate::CommandIds;
use crate::{Memory, reason};
use domain::{CommandId, MaybeSend};
#[cfg(target_arch = "wasm32")]
use std::rc::Rc;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

/// The most jobs one batch runs.
#[cfg(not(target_arch = "wasm32"))]
const MAX_BATCH: usize = 256;

/// Runs a job, given the memory, or `None` when the runtime can't run it. Returns how to answer
/// once the batch it ran in is durable, or isn't.
#[cfg(not(target_arch = "wasm32"))]
type Job = Box<dyn FnOnce(Option<&Memory>) -> Answer + Send>;
#[cfg(not(target_arch = "wasm32"))]
type Answer = Box<dyn FnOnce(Result<(), String>) + Send>;

/// The thread that owns a [`Memory`] and changes it, and how to hand it jobs. Clone it to share;
/// the thread stops once every clone is dropped.
#[derive(Clone)]
pub struct Runtime {
    #[cfg(not(target_arch = "wasm32"))]
    jobs: Arc<Jobs>,
    #[cfg(not(target_arch = "wasm32"))]
    ids: Arc<CommandIds>,
    /// On the web, which has no threads, jobs run at once on the page's thread.
    #[cfg(target_arch = "wasm32")]
    memory: Rc<Memory>,
}

#[cfg(not(target_arch = "wasm32"))]
struct Jobs(std::sync::mpsc::Sender<Job>);

impl Runtime {
    /// Starts a thread that builds a [`Memory`] with `build`, then owns it: it runs jobs as they
    /// come, and the model's policies and scheduled commands between them. The memory never
    /// leaves the thread, so nothing in it is locked.
    ///
    /// On the web there are no threads: jobs run at once, on the thread that hands them in, and
    /// so do policies. Scheduled commands fire when a job calls `Memory::run_due`; call one from
    /// a timer.
    ///
    /// # Panics
    /// When `build` panics, such as when the store it opens fails.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spawn(build: impl FnOnce() -> Memory + Send + 'static) -> Self {
        use std::sync::mpsc::RecvTimeoutError;
        let (sender, queue) = std::sync::mpsc::channel::<Job>();
        let jobs = Arc::new(Jobs(sender));
        // An empty job wakes the thread when policies have work. Weak, so the thread still stops
        // once every handle is gone.
        let wake = Arc::downgrade(&jobs);
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("domain".into())
            .spawn(move || {
                let memory = std::rc::Rc::new(build());
                memory.on_wake(move || {
                    if let Some(jobs) = wake.upgrade() {
                        jobs.post(Box::new(|_| Box::new(|_| {})));
                    }
                });
                let _ = ready.send(memory.command_ids());
                // Why a batch didn't commit, once one hasn't.
                let mut failed = None;
                // Run what's due right away: schedules and policy work the store held.
                let mut due = Some(memory.now());
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
        match started.recv() {
            Ok(ids) => Self { jobs, ids },
            Err(_) => std::panic::resume_unwind(thread.join().expect_err("the thread stopped before it started")),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn spawn(build: impl FnOnce() -> Memory + 'static) -> Self {
        let memory = Rc::new(build());
        let weak = Rc::downgrade(&memory);
        memory.on_wake(move || {
            if let Some(memory) = weak.upgrade() {
                memory.run_due();
            }
        });
        Self { memory }
    }

    /// Runs `job` on the runtime's thread, then calls `then` with what it returned, once what it
    /// wrote is durable. `then` gets `Err` when the job panicked, or its batch didn't commit; a
    /// command may then have committed or not, as with `SendError::Unavailable`.
    pub fn exec<T: MaybeSend + 'static>(
        &self,
        job: impl FnOnce(&Memory) -> T + MaybeSend + 'static,
        then: impl FnOnce(Result<T, String>) + MaybeSend + 'static,
    ) {
        // `None` when the runtime refused the job.
        let run = move |memory: Option<&Memory>| {
            let memory = memory?;
            let job = std::panic::AssertUnwindSafe(|| job(memory));
            Some(std::panic::catch_unwind(job).map_err(|panic| reason(&*panic)))
        };
        #[cfg(target_arch = "wasm32")]
        then(run(Some(&self.memory)).expect("a job given the memory runs"));
        #[cfg(not(target_arch = "wasm32"))]
        self.jobs.post(Box::new(move |memory: Option<&Memory>| -> Answer {
            let ran = run(memory);
            Box::new(move |outcome: Result<(), String>| {
                then(match ran {
                    Some(Ok(value)) => outcome.map(|()| value),
                    Some(Err(panic)) => Err(panic),
                    None => Err(outcome.err().unwrap_or_default()),
                })
            })
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
            std::thread::current().name() != Some("domain"),
            "`Runtime::call` on the runtime's own thread would wait forever; use `exec`"
        );
        let (answer, answered) = std::sync::mpsc::sync_channel(1);
        self.exec(job, move |result| {
            let _ = answer.send(result);
        });
        answered.recv().unwrap_or_else(|_| Err("didn't run: the domain thread stopped".into()))
    }

    /// A command id no other command sent here has used, made without a job.
    pub fn new_command_id(&self) -> CommandId {
        #[cfg(not(target_arch = "wasm32"))]
        return self.ids.next();
        #[cfg(target_arch = "wasm32")]
        self.memory.new_command_id()
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Jobs {
    fn post(&self, job: Job) {
        // Only fails once the thread has stopped, after a panic outside any job.
        if let Err(std::sync::mpsc::SendError(job)) = self.0.send(job) {
            job(None)(Err("didn't run: the domain thread stopped".into()));
        }
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
