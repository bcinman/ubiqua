//! Running sagas. Each run keeps a journal: every step's result, and the signals it got. A run
//! that's retried, or resumed after a restart, runs its code again from the top, and each step it
//! already took returns its journaled result instead of sending again. So a saga's code must take
//! the same steps, in the same order, given the same input and results.
//!
//! When a run is refused, the compensations of the steps that succeeded run, last first, until
//! each one has been sent.

use crate::calls::BoxFuture;
use crate::policies::{Next, PolicyRun};
use crate::schedules::{PENDING, Unfired, command_key};
use crate::store::{KeyRow, ScheduleRow, StoreError, ViewRows};
use crate::{AggregateOf, Memory, decode_json, json, lock, reason};
use domain::{
    Command, CurrentActor, Handles, MaybeSend, MaybeSync, NeverRejects, Saga, SendError, StreamId, Timestamp,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// What a saga does. Write it as an `async fn` that takes its steps through `cx`:
///
/// ```ignore
/// impl Run for ExecuteTransfer {
///     async fn run(self, cx: &SagaCx<Self>) -> Result<(), ExecuteTransferRejection> {
///         cx.send("debit", &self.from, WithdrawMoney { amount: self.amount })
///             .compensate("refund", &self.from, RefundMoney { amount: self.amount })
///             .await?;
///         ..
///     }
/// }
/// ```
pub trait Run: Saga {
    /// Carries out one run. `Err` refuses it: the compensations of the steps that succeeded run.
    fn run(self, cx: &SagaCx<Self>) -> impl Future<Output = Result<(), Self::Rejection>> + MaybeSend;
}

/// Where a run stands. `R` is the saga's rejection.
#[derive(Clone, Debug, PartialEq)]
pub enum SagaStatus<R> {
    NotStarted,
    /// Taking steps, waiting for a signal, or undoing its steps after a refusal.
    Running,
    Completed,
    /// Refused, and every compensation has run.
    Rejected(R),
    /// It panicked, or its journal no longer matches its code. It won't run again.
    Died(String),
}

/// Why a signal couldn't be sent.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignalError {
    /// No run with this key was started.
    NotStarted,
    /// The store failed; the run didn't get the signal.
    Unavailable(String),
}

impl fmt::Display for SignalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotStarted => f.write_str("no run with this key was started"),
            Self::Unavailable(reason) => write!(f, "unavailable: {reason}"),
        }
    }
}

impl std::error::Error for SignalError {}

/// What one run keeps, in memory and in the store.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Record {
    input: Value,
    status: Stored,
    steps: BTreeMap<String, StepRow>,
    /// Steps in the order they finished, so compensations run last first.
    order: Vec<String>,
    signals: BTreeMap<String, Value>,
    /// Compensations already sent.
    compensated: BTreeSet<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Stored {
    Running,
    /// Refused with this rejection; compensating.
    Compensating(Value),
    Completed,
    Rejected(Value),
    Died(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StepRow {
    /// The reply, or the rejection.
    result: Result<Value, Value>,
    compensation: Option<Compensation>,
}

/// A command that undoes a step, ready to send.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Compensation {
    name: String,
    /// `Aggregate.Command`.
    command: String,
    stream: String,
    id: String,
    payload: String,
}

/// A run's journal, shared between its code and its host.
struct RunCell {
    record: Record,
    /// Step names taken in this execution, to catch a name used twice.
    used: HashSet<String>,
    /// Set by a step that can't finish now; the host acts on it.
    suspend: Option<Suspend>,
}

enum Suspend {
    /// Run it again later: something was unavailable.
    Retry(String),
    Die(String),
}

/// What a run may read and do. Every step goes through it, so it can be journaled.
pub struct SagaCx<S: Saga> {
    memory: Weak<Memory>,
    key: S::Key,
    key_json: String,
    cell: Arc<Mutex<RunCell>>,
}

impl<S: Saga> SagaCx<S> {
    fn memory(&self) -> Arc<Memory> {
        self.memory.upgrade().expect("a saga runs while its memory is alive")
    }

    /// The run's key.
    pub fn key(&self) -> &S::Key {
        &self.key
    }

    /// The time now. Don't decide on it: a replay sees another time. Decide in an aggregate.
    pub fn now(&self) -> Timestamp {
        self.memory().now()
    }

    /// A step: sends `command` to `id`, once, under an id made from the run and `name`. Await it
    /// for the reply or the rejection. Give every step of a saga its own name.
    pub fn send<C>(&self, name: impl Into<String>, id: &C::Target, command: C) -> Step<'_, S, C>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        Step { cx: self, name: name.into(), id: id.clone(), command, actor: None, compensation: None }
    }

    /// Waits until the run gets the signal `name`, such as a decision, and returns its value.
    /// A signal sent before the run waits for it is kept.
    pub fn signal<T: DeserializeOwned>(&self, name: impl Into<String>) -> Signal<'_, S, T> {
        Signal { cx: self, name: name.into(), _value: PhantomData }
    }

    fn command_id(&self, name: &str) -> String {
        format!("{}:{}:{name}", S::NAME, self.key)
    }

    fn suspend<T>(&self, suspend: Suspend) -> Option<T> {
        lock(&self.cell).suspend.get_or_insert(suspend);
        None
    }

    /// Takes a step, or returns its journaled result. `None` suspends the run.
    fn step<C>(&self, step: Step<'_, S, C>) -> Option<Result<C::Reply, C::Rejection>>
    where
        C: Command,
        AggregateOf<C::Target>: Handles<C>,
    {
        let Step { name, id, command, actor, compensation, .. } = step;
        {
            let mut cell = lock(&self.cell);
            if cell.suspend.is_some() {
                return None;
            }
            if !cell.used.insert(name.clone()) {
                drop(cell);
                return self.suspend(Suspend::Die(format!("step {name} ran twice; give each step its own name")));
            }
            if let Some(row) = cell.record.steps.get(&name) {
                let decoded = match row.result.clone() {
                    Ok(reply) => serde_json::from_value(reply).map(Ok),
                    Err(rejection) => serde_json::from_value(rejection).map(Err),
                };
                drop(cell);
                return match decoded {
                    Ok(result) => Some(result),
                    Err(e) => self.suspend(Suspend::Die(format!("step {name}'s journaled result doesn't decode: {e}"))),
                };
            }
        }
        let memory = self.memory();
        let (result, outcome) = match memory.send_as(actor.as_ref(), &id, command, self.command_id(&name)) {
            Ok(receipt) => (serde_json::to_value(&receipt.reply).map(Ok), Ok(receipt.reply)),
            Err(SendError::Rejected(r)) => (serde_json::to_value(&r).map(Err), Err(r)),
            Err(SendError::Unavailable { reason, .. }) => {
                return self.suspend(Suspend::Retry(format!("step {name}: {reason}")));
            }
            Err(other) => return self.suspend(Suspend::Die(format!("step {name}: {other}"))),
        };
        let result = match result {
            Ok(result) => result,
            Err(e) => return self.suspend(Suspend::Die(format!("step {name}'s result doesn't encode: {e}"))),
        };
        let compensation = if outcome.is_ok() { compensation } else { None };
        let mut cell = lock(&self.cell);
        cell.record.steps.insert(name.clone(), StepRow { result, compensation });
        cell.record.order.push(name);
        if let Err(e) = memory.save_run(S::NAME, &self.key_json, &cell.record) {
            // The step's command ran under its id, so taking it again is safe.
            cell.suspend = Some(Suspend::Retry(e));
            return None;
        }
        Some(outcome)
    }
}

/// One step of a saga, from [`SagaCx::send`]. Await it.
pub struct Step<'a, S: Saga, C: Command> {
    cx: &'a SagaCx<S>,
    name: String,
    id: C::Target,
    command: C,
    actor: Option<CurrentActor>,
    compensation: Option<Compensation>,
}

impl<S: Saga, C: Command> Step<'_, S, C> {
    /// Sends the step's command on behalf of `actor`, for a `decide` that checks who asks.
    pub fn acting_as<T: Any + fmt::Debug + MaybeSend + MaybeSync>(mut self, actor: T) -> Self {
        self.actor = Some(CurrentActor::new(actor));
        self
    }

    /// What undoes this step if the run is refused later: `command` to `id`, sent under an id
    /// made from the run and `name`. It can't be rejected, so the undoing can't get stuck.
    ///
    /// # Panics
    /// When the command doesn't encode.
    pub fn compensate<D>(mut self, name: impl Into<String>, id: &D::Target, command: D) -> Self
    where
        D: NeverRejects,
        AggregateOf<D::Target>: Handles<D>,
    {
        self.cx.memory().register_firer::<D>();
        let encode = |what: &str, value: Result<String, String>| value.unwrap_or_else(|e| panic!("{what}: {e}"));
        self.compensation = Some(Compensation {
            name: name.into(),
            command: command_key::<D>(),
            stream: StreamId::of::<AggregateOf<D::Target>>(id).to_string(),
            id: encode("the compensation's id", json(id)),
            payload: encode("the compensation", json(&command)),
        });
        self
    }
}

impl<'a, S: Saga, C> IntoFuture for Step<'a, S, C>
where
    C: Command,
    AggregateOf<C::Target>: Handles<C>,
{
    type Output = Result<C::Reply, C::Rejection>;
    type IntoFuture = Settle<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Settle(self.cx.step(self))
    }
}

/// A step's outcome. It never resolves when the run is suspended; the host runs it again.
pub struct Settle<T>(Option<T>);

impl<T> Unpin for Settle<T> {}

impl<T> Future for Settle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        match self.get_mut().0.take() {
            Some(outcome) => Poll::Ready(outcome),
            None => Poll::Pending,
        }
    }
}

/// Waits for a signal, from [`SagaCx::signal`].
pub struct Signal<'a, S: Saga, T> {
    cx: &'a SagaCx<S>,
    name: String,
    _value: PhantomData<fn() -> T>,
}

impl<S: Saga, T: DeserializeOwned> Future for Signal<'_, S, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        let mut cell = lock(&self.cx.cell);
        let Some(value) = cell.record.signals.get(&self.name).cloned() else { return Poll::Pending };
        match serde_json::from_value(value) {
            Ok(value) => Poll::Ready(value),
            Err(e) => {
                let why = format!("signal {} doesn't decode as {}: {e}", self.name, std::any::type_name::<T>());
                cell.suspend.get_or_insert(Suspend::Die(why));
                Poll::Pending
            }
        }
    }
}

/// The longest a run waits before it's tried again.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

pub(crate) struct SagaHost<S: Run> {
    runs: Mutex<HashMap<String, Arc<Mutex<RunHost<S>>>>>,
}

struct RunHost<S: Run> {
    key: S::Key,
    key_json: String,
    cell: Arc<Mutex<RunCell>>,
    future: Option<BoxFuture<Result<(), S::Rejection>>>,
    attempts: u32,
    retry_at: Option<Timestamp>,
    failure: Option<String>,
}

impl<S: Run> RunHost<S> {
    fn status(&self) -> Stored {
        lock(&self.cell).record.status.clone()
    }

    /// Sets the status and keeps it. If the store fails, a restart takes the run up again, which
    /// is safe.
    fn finish(&mut self, memory: &Memory, status: Stored) -> Next {
        let mut cell = lock(&self.cell);
        cell.record.status = status;
        let _ = memory.save_run(S::NAME, &self.key_json, &cell.record);
        self.retry_at = None;
        self.attempts = 0;
        Next::Idle
    }

    fn die(&mut self, memory: &Memory, why: String) -> Next {
        self.future = None;
        self.failure = Some(format!("{} {} died: {why}", S::NAME, self.key));
        self.finish(memory, Stored::Died(why))
    }

    fn retry(&mut self, memory: &Memory, why: String) -> Next {
        self.future = None;
        self.attempts += 1;
        let backoff = Duration::from_secs(1 << (self.attempts - 1).min(6)).min(MAX_BACKOFF);
        let at = memory.now().after(backoff);
        self.retry_at = Some(at);
        self.failure = Some(format!("{} {} failed: {why}", S::NAME, self.key));
        Next::RetryAt(at)
    }

    fn advance(&mut self, memory: &Arc<Memory>, waker: &Waker) -> Next {
        if let Some(at) = self.retry_at
            && memory.now() < at
        {
            return Next::RetryAt(at);
        }
        match self.status() {
            Stored::Running => self.step(memory, waker),
            Stored::Compensating(rejection) => self.compensate(memory, rejection),
            _ => Next::Idle,
        }
    }

    fn step(&mut self, memory: &Arc<Memory>, waker: &Waker) -> Next {
        if self.future.is_none() {
            let input = {
                let mut cell = lock(&self.cell);
                cell.used.clear();
                cell.suspend = None;
                cell.record.input.clone()
            };
            let saga: S = match serde_json::from_value(input) {
                Ok(saga) => saga,
                Err(e) => return self.die(memory, format!("its input doesn't decode: {e}")),
            };
            let cx = SagaCx::<S> {
                memory: Arc::downgrade(memory),
                key: self.key.clone(),
                key_json: self.key_json.clone(),
                cell: self.cell.clone(),
            };
            self.future = Some(Box::pin(async move { saga.run(&cx).await }));
        }
        let future = self.future.as_mut().expect("set above");
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().poll(&mut Context::from_waker(waker))
        }));
        match polled {
            Err(panic) => self.die(memory, reason(&*panic)),
            Ok(Poll::Pending) => match { lock(&self.cell).suspend.take() } {
                None => Next::Waiting,
                Some(Suspend::Die(why)) => self.die(memory, why),
                Some(Suspend::Retry(why)) => self.retry(memory, why),
            },
            Ok(Poll::Ready(Ok(()))) => {
                self.future = None;
                self.failure = None;
                self.finish(memory, Stored::Completed)
            }
            Ok(Poll::Ready(Err(rejection))) => {
                self.future = None;
                self.failure = None;
                let rejection = match serde_json::to_value(&rejection) {
                    Ok(rejection) => rejection,
                    Err(e) => return self.die(memory, format!("its rejection doesn't encode: {e}")),
                };
                self.finish(memory, Stored::Compensating(rejection.clone()));
                self.compensate(memory, rejection)
            }
        }
    }

    /// Sends each compensation not sent yet, last step first, then records the refusal.
    fn compensate(&mut self, memory: &Memory, rejection: Value) -> Next {
        let pending: Vec<Compensation> = {
            let cell = lock(&self.cell);
            let record = &cell.record;
            record
                .order
                .iter()
                .rev()
                .filter_map(|name| record.steps.get(name)?.compensation.clone())
                .filter(|compensation| !record.compensated.contains(&compensation.name))
                .collect()
        };
        for compensation in pending {
            let fire = lock(&memory.firers).get(&compensation.command).copied();
            let Some(fire) = fire else {
                return self.die(memory, format!("no {} is hosted to compensate with", compensation.command));
            };
            let row = ScheduleRow {
                stream: compensation.stream.clone(),
                command_id: format!("{}:{}:{}", S::NAME, self.key, compensation.name),
                command: compensation.command.clone(),
                id: compensation.id.clone(),
                payload: compensation.payload.clone(),
                at: 0,
                state: PENDING.into(),
            };
            match fire(memory, &row) {
                Ok(()) => {
                    let mut cell = lock(&self.cell);
                    cell.record.compensated.insert(compensation.name);
                    let _ = memory.save_run(S::NAME, &self.key_json, &cell.record);
                }
                Err(Unfired::Retry) => return self.retry(memory, format!("compensation {}", compensation.name)),
                Err(Unfired::Undecodable) => {
                    return self.die(memory, format!("compensation {} doesn't decode", compensation.name));
                }
            }
        }
        self.failure = None;
        self.finish(memory, Stored::Rejected(rejection))
    }
}

impl<S: Run> PolicyRun for SagaHost<S> {
    fn run(&self, memory: &Arc<Memory>, waker: &Waker) -> Next {
        // Each run on its own lock, so a step may start or signal another run.
        let runs: Vec<_> = lock(&self.runs).values().cloned().collect();
        let mut next = Next::Idle;
        for run in runs {
            next = match (lock(&run).advance(memory, waker), next) {
                (Next::RetryAt(at), Next::RetryAt(earlier)) => Next::RetryAt(at.min(earlier)),
                (Next::RetryAt(at), _) | (_, Next::RetryAt(at)) => Next::RetryAt(at),
                (Next::Waiting, _) | (_, Next::Waiting) => Next::Waiting,
                _ => Next::Idle,
            };
        }
        next
    }

    fn failures(&self) -> Vec<String> {
        let runs: Vec<_> = lock(&self.runs).values().cloned().collect();
        runs.iter().filter_map(|run| lock(run).failure.clone()).collect()
    }
}

impl Memory {
    /// Runs `S`'s runs, started with [`start_saga`](Self::start_saga). With a store, runs left
    /// unfinished by an earlier run of the app carry on. Runs advance in [`run_due`](Self::run_due).
    ///
    /// # Panics
    /// When the store fails, or a stored run doesn't decode.
    pub fn saga<S: Run>(mut self) -> Self {
        let name = Self::saga_name::<S>();
        let mut runs = HashMap::new();
        if let Some(store) = &self.store {
            let rows = store.load_view(&name).unwrap_or_else(|e| panic!("reading {name} from the store failed: {e}"));
            for KeyRow { key: key_json, state } in rows.keys {
                let key: S::Key = decode_json(&key_json).unwrap_or_else(|e| panic!("a stored run of {name}: {e}"));
                let record: Record = decode_json(&state).unwrap_or_else(|e| panic!("a stored run of {name}: {e}"));
                runs.insert(key_json.clone(), Arc::new(Mutex::new(RunHost::<S>::new(key, key_json, record))));
            }
        }
        let host = Arc::new(SagaHost::<S> { runs: Mutex::new(runs) });
        let hosted = self.hosts.insert(TypeId::of::<S>(), Box::new(host.clone()));
        assert!(
            hosted.is_none(),
            "{} is hosted twice: list it in the model's `sagas(..)` or add it with `saga`, not both",
            S::NAME
        );
        self.runners.push(host);
        self
    }

    /// Starts a run of `S` under `key`, with `input`. Nobody waits for it: it runs in
    /// [`run_due`](Self::run_due). A run already started under `key` carries on as it was, so
    /// starting twice, as a policy that runs twice does, starts one run.
    ///
    /// # Panics
    /// When `S` isn't hosted here, or the input doesn't encode.
    pub fn start_saga<S: Run>(&self, key: &S::Key, input: S) -> Result<(), StoreError> {
        let host = self.saga_host::<S>();
        let key_json = json(key).unwrap_or_else(|e| panic!("{e}"));
        let mut runs = lock(&host.runs);
        if runs.contains_key(&key_json) {
            return Ok(());
        }
        let record = Record {
            input: serde_json::to_value(&input).unwrap_or_else(|e| panic!("{}'s input doesn't encode: {e}", S::NAME)),
            status: Stored::Running,
            steps: BTreeMap::new(),
            order: Vec::new(),
            signals: BTreeMap::new(),
            compensated: BTreeSet::new(),
        };
        self.save_run(S::NAME, &key_json, &record).map_err(StoreError)?;
        runs.insert(key_json.clone(), Arc::new(Mutex::new(RunHost::new(key.clone(), key_json, record))));
        drop(runs);
        self.wake();
        Ok(())
    }

    /// Where the run of `S` under `key` stands.
    ///
    /// # Panics
    /// When `S` isn't hosted here.
    pub fn saga_status<S: Run>(&self, key: &S::Key) -> SagaStatus<S::Rejection> {
        let host = self.saga_host::<S>();
        let Some(run) = json(key).ok().and_then(|key| lock(&host.runs).get(&key).cloned()) else {
            return SagaStatus::NotStarted;
        };
        let status = lock(&run).status();
        match status {
            Stored::Running | Stored::Compensating(_) => SagaStatus::Running,
            Stored::Completed => SagaStatus::Completed,
            Stored::Died(why) => SagaStatus::Died(why),
            Stored::Rejected(rejection) => match serde_json::from_value(rejection) {
                Ok(rejection) => SagaStatus::Rejected(rejection),
                Err(e) => SagaStatus::Died(format!("its rejection doesn't decode: {e}")),
            },
        }
    }

    /// Sends the run of `S` under `key` the signal `name`, for a `cx.signal(name)` it waits on,
    /// or will. The first value sent under a name is the one it gets, so sending again is safe.
    ///
    /// # Panics
    /// When `S` isn't hosted here, or the value doesn't encode.
    pub fn signal<S: Run>(&self, key: &S::Key, name: &str, value: impl Serialize) -> Result<(), SignalError> {
        let host = self.saga_host::<S>();
        let key_json = json(key).unwrap_or_else(|e| panic!("{e}"));
        let Some(run) = lock(&host.runs).get(&key_json).cloned() else { return Err(SignalError::NotStarted) };
        let value = serde_json::to_value(value).unwrap_or_else(|e| panic!("signal {name} doesn't encode: {e}"));
        {
            let run = lock(&run);
            let mut cell = lock(&run.cell);
            if cell.record.signals.contains_key(name) {
                return Ok(());
            }
            cell.record.signals.insert(name.to_string(), value);
            if let Err(e) = self.save_run(S::NAME, &key_json, &cell.record) {
                cell.record.signals.remove(name);
                return Err(SignalError::Unavailable(e));
            }
        }
        self.wake();
        Ok(())
    }

    fn saga_host<S: Run>(&self) -> &Arc<SagaHost<S>> {
        let host = self.hosts.get(&TypeId::of::<S>()).and_then(|host| (**host).downcast_ref::<Arc<SagaHost<S>>>());
        host.unwrap_or_else(|| panic!("{} isn't hosted here: add it with `Memory::saga`", S::NAME))
    }

    fn saga_name<S: Saga>() -> String {
        format!("saga:{}", S::NAME)
    }

    fn save_run(&self, saga: &str, key_json: &str, record: &Record) -> Result<(), String> {
        let Some(store) = &self.store else { return Ok(()) };
        let rows =
            ViewRows { keys: vec![KeyRow { key: key_json.into(), state: json(record)? }], ..ViewRows::default() };
        store.save_view(&format!("saga:{saga}"), &rows).map_err(|e| format!("the store failed: {e}"))
    }
}

impl<S: Run> RunHost<S> {
    fn new(key: S::Key, key_json: String, record: Record) -> Self {
        let cell = Arc::new(Mutex::new(RunCell { record, used: HashSet::new(), suspend: None }));
        Self { key, key_json, cell, future: None, attempts: 0, retry_at: None, failure: None }
    }
}

/// A saga in a model's `sagas(..)`.
impl<S: Run> domain::HostSaga<S> for Memory {
    fn host_saga(self) -> Self {
        self.saga::<S>()
    }
}
