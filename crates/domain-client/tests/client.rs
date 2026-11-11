use domain::{Aggregate, Api, Apply, Cx, Decide, Decision, Evolve, Model, Reply, SendError, View, ViewCx};
use domain_client::{Client, ReadError};
use domain_memory::{Memory, Runtime};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

domain::id!(
    struct CounterId;
);
domain::id!(
    struct OtherId;
);

#[domain::command(target = CounterId, events(Added), rejects(TooBig), reply = i64)]
struct Add {
    n: i64,
}

#[domain::event]
struct Added {
    n: i64,
    total: i64,
}

#[domain::rejection]
struct TooBig;

#[derive(Aggregate)]
#[aggregate(id = CounterId, state = i64, events(Added), commands(Add))]
struct Counter;

impl Evolve for Counter {
    fn evolve(total: &mut i64, event: &CounterEvent) {
        let CounterEvent::Added(e) = event;
        *total = e.total;
    }
}

impl Decide<Add> for Counter {
    fn decide(total: &i64, cmd: &Add, _: &Cx) -> Decision<Add> {
        if cmd.n > 1000 {
            return Err(TooBig.into());
        }
        // Panics on 666, to show a command failing before it commits.
        assert!(cmd.n != 666, "Counter can't add 666");
        Ok(vec![Added { n: cmd.n, total: total + cmd.n }.into()])
    }
}

impl Reply<Counter> for Add {
    fn reply(total: &i64) -> i64 {
        *total
    }
}

#[domain::command(target = OtherId, events(Pinged))]
struct Ping;

#[domain::event]
struct Pinged;

#[derive(Aggregate)]
#[aggregate(id = OtherId, state = (), events(Pinged), commands(Ping))]
struct Other;

impl Evolve for Other {
    fn evolve(_: &mut (), _: &OtherEvent) {}
}

impl Decide<Ping> for Other {
    fn decide(_: &(), _: &Ping, _: &Cx) -> Decision<Ping> {
        Ok(vec![Pinged.into()])
    }
}

#[derive(View)]
#[view(key = CounterId, from(Counter(Added)), state = i64)]
struct Total;

impl Apply for Total {
    fn apply(total: &mut i64, input: TotalInput, _: &mut ViewCx<Self>) {
        let TotalInput::Added(e) = input;
        // Panics on -13, to show a view failing after its command committed.
        assert!(e.n != -13, "Total can't count -13");
        *total = e.total;
    }
}

#[derive(Model)]
#[model(aggregates(Counter, Other), views(Total))]
struct Counting;

#[derive(Api)]
#[api(commands(Add, Ping), views(Total))]
struct CountingApi;

fn c(id: &str) -> CounterId {
    CounterId::new(id)
}

fn memory() -> Memory {
    Memory::new().with::<Counting>()
}

#[test]
fn an_inline_client_resolves_sends_at_once() {
    let client = Client::<CountingApi>::in_process(Runtime::inline(memory()));
    let receipt = client.send(&c("a"), Add { n: 2 }).try_get().unwrap().unwrap();
    assert_eq!((receipt.reply, receipt.version), (2, 1));

    let refused = client.send(&c("a"), Add { n: 5000 }).try_get().unwrap();
    assert_eq!(refused.unwrap_err().rejection(), Some(&AddRejection::TooBig(TooBig)));
}

#[test]
fn an_in_process_client_runs_commands_on_its_domain_thread_in_order() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let (tx, rx) = mpsc::channel();
    for n in 1..=50 {
        let tx = tx.clone();
        client.send_then(&c("a"), Add { n }, move |sent| {
            tx.send((std::thread::current().name().map(String::from), sent.unwrap().reply)).unwrap()
        });
    }
    let results: Vec<_> = (0..50).map(|_| rx.recv_timeout(Duration::from_secs(2)).unwrap()).collect();
    assert!(results.iter().all(|(thread, _)| thread.as_deref() == Some("domain")));
    let totals: Vec<i64> = results.into_iter().map(|(_, total)| total).collect();
    assert_eq!(totals, (1..=50).map(|n| n * (n + 1) / 2).collect::<Vec<_>>());
}

#[test]
fn a_pending_send_can_be_awaited_or_polled_each_frame() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let awaited = block_on(client.send(&c("a"), Add { n: 4 })).unwrap();
    assert_eq!(awaited.reply, 4);

    let pending = client.send(&c("a"), Add { n: 1 });
    let deadline = Instant::now() + Duration::from_secs(2);
    let polled = loop {
        if let Some(sent) = pending.try_get() {
            break sent.unwrap();
        }
        assert!(Instant::now() < deadline, "the send never resolved");
        std::thread::sleep(Duration::from_millis(1));
    };
    assert_eq!(polled.reply, 5);
    assert!(pending.try_get().is_none(), "a result is returned once");
}

#[test]
fn a_command_that_panics_is_unavailable_and_the_domain_thread_keeps_going() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let error = block_on(client.send(&c("a"), Add { n: 666 })).unwrap_err();
    let SendError::Unavailable { command_id, reason } = error else { panic!("{error:?}") };
    assert!(reason.contains("Counter can't add 666"), "{reason}");
    assert!(command_id.as_str().starts_with("memory-"), "retry with {command_id}");

    // Nothing committed, and later commands still run.
    assert_eq!(block_on(client.send(&c("a"), Add { n: 1 })).unwrap().reply, 1);
}

#[test]
fn a_command_whose_view_panics_still_commits_and_the_view_reports_it_is_behind() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let receipt = block_on(client.send(&c("a"), Add { n: -13 })).expect("committed");
    assert_eq!(receipt.reply, -13);
    let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| client.view::<Total>().get(&c("a")).try_get()));
    assert!(read.is_err(), "reading a view that can't apply its events fails loudly");
}

#[test]
fn reads_see_a_receipt_once_the_view_applied_it() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let receipt = block_on(client.send(&c("a"), Add { n: 3 })).unwrap();
    let totals = client.view::<Total>();
    assert_eq!(block_on(totals.get_at_least(&c("a"), &receipt)), Ok(Some(3)));
    assert_eq!(block_on(totals.get(&c("b"))), Ok(None));

    let pinged = block_on(client.send(&OtherId::new("x"), Ping)).unwrap();
    let Err(ReadError::NotCaughtUp(e)) = block_on(totals.get_at_least(&c("a"), &pinged)) else {
        panic!("Other/x doesn't reach Total")
    };
    assert_eq!(e.stream.as_str(), "Other/x");
}

#[test]
fn a_live_view_starts_at_the_state_now_and_follows_changes() {
    let client = Client::<CountingApi>::in_process(Runtime::inline(memory()));
    client.send(&c("a"), Add { n: 2 });
    let live = client.view::<Total>().live(&c("a"));
    assert_eq!(*live.latest().0, Ok(Some(2)));
    assert_eq!(live.latest().1, 0);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let listener = live.on_change(move |total, version| log.lock().unwrap().push(((**total).clone(), version)));
    client.send(&c("a"), Add { n: 3 });
    client.send(&c("b"), Add { n: 100 }); // another key
    client.send(&c("a"), Add { n: 0 }); // no change
    client.send(&c("a"), Add { n: 1 });
    assert_eq!(*seen.lock().unwrap(), [(Ok(Some(5)), 1), (Ok(Some(6)), 2)]);

    drop(listener);
    client.send(&c("a"), Add { n: 1 });
    assert_eq!(seen.lock().unwrap().len(), 2, "no callbacks after the listener drops");
    assert_eq!(live.latest(), (Arc::new(Ok(Some(7))), 3));
}

#[test]
fn a_live_view_can_be_awaited_from_another_thread() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    let live = client.view::<Total>().live(&c("a"));
    let (_, seen) = live.latest();
    let waiting = {
        let live = live.clone();
        std::thread::spawn(move || block_on(live.changed(seen)))
    };
    std::thread::sleep(Duration::from_millis(20));
    client.send(&c("a"), Add { n: 9 });
    let (total, version) = waiting.join().unwrap();
    assert_eq!(((*total).clone(), version), (Ok(Some(9)), 1));
}

#[test]
fn a_panicking_callback_doesnt_stop_the_domain_thread() {
    let client = Client::<CountingApi>::in_process(Runtime::spawn(memory()));
    client.send_then(&c("a"), Add { n: 1 }, |_| panic!("the UI's callback fails"));
    assert_eq!(block_on(client.send(&c("a"), Add { n: 1 })).unwrap().reply, 2);
}

#[test]
fn callbacks_may_read_and_send_without_deadlocking() {
    let client = Client::<CountingApi>::in_process(Runtime::inline(memory()));
    let live = client.view::<Total>().live(&c("a"));
    let reads = Arc::new(Mutex::new(Vec::new()));
    let (inner, me, log) = (client.clone(), live.clone(), reads.clone());
    let _listener = live.on_change(move |total, _| {
        // Read the view, and this live view itself, then send to it again until it reaches 3.
        let read = inner.view::<Total>().get(&c("a")).try_get().unwrap().unwrap();
        let latest = me.latest().0.as_ref().clone().unwrap();
        let total = total.as_ref().clone().unwrap();
        log.lock().unwrap().push((total, read, latest));
        if total.unwrap() < 3 {
            inner.send(&c("a"), Add { n: 1 });
        }
    });
    client.send(&c("a"), Add { n: 1 });
    assert_eq!(*live.latest().0, Ok(Some(3)));
    let reads = reads.lock().unwrap();
    assert_eq!(reads.last(), Some(&(Some(3), Some(3), Some(3))));
    assert!(reads.iter().all(|(seen, read, latest)| seen <= read && seen <= latest), "{reads:?}");
}

#[test]
fn a_live_view_ends_at_the_last_state_under_concurrent_writers() {
    let runtime = Runtime::inline(memory());
    let client = Client::<CountingApi>::in_process(runtime.clone());
    let live = client.view::<Total>().live(&c("a"));
    let versions = Arc::new(Mutex::new(Vec::new()));
    let log = versions.clone();
    let _listener = live.on_change(move |_, version| log.lock().unwrap().push(version));

    let writers: Vec<_> = (0..8)
        .map(|_| {
            let client = Client::<CountingApi>::in_process(runtime.clone());
            std::thread::spawn(move || {
                for _ in 0..100 {
                    client.send(&c("a"), Add { n: 1 });
                }
            })
        })
        .collect();
    writers.into_iter().for_each(|w| w.join().unwrap());

    assert_eq!(*live.latest().0, Ok(Some(800)));
    let versions = versions.lock().unwrap();
    assert!(versions.windows(2).all(|w| w[0] < w[1]), "versions only go up");
}

#[test]
fn dropping_a_live_view_stops_its_updates() {
    let client = Client::<CountingApi>::in_process(Runtime::inline(memory()));
    let live = client.view::<Total>().live(&c("a"));
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let listener = live.on_change(move |_, _| flag.store(true, Ordering::SeqCst));
    drop(live);
    client.send(&c("a"), Add { n: 1 });
    assert!(!called.load(Ordering::SeqCst), "the live view was dropped, so nothing feeds it");
    drop(listener);
}

/// Polls again only when woken, so a missing wake fails instead of being re-polled.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct Flag {
        thread: std::thread::Thread,
        woken: AtomicBool,
    }
    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.woken.store(true, Ordering::SeqCst);
            self.thread.unpark();
        }
    }
    let flag = Arc::new(Flag { thread: std::thread::current(), woken: AtomicBool::new(false) });
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        while !flag.woken.swap(false, Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "nothing woke the future");
            std::thread::park_timeout(Duration::from_millis(50));
        }
    }
}

/// Pings `<counter>` on `Other` for each addition, and again a little later.
#[derive(domain::Policy)]
#[policy(from(Counter(Added)))]
struct PingBack;

impl domain_memory::React for PingBack {
    async fn react(
        &self,
        cx: &domain_memory::PolicyCx<Self>,
        _: PingBackInput,
    ) -> Result<(), domain_memory::PolicyError> {
        let other = OtherId::new(cx.source::<Counter>().expect("from counters").as_str());
        cx.send(&other, Ping)?;
        cx.schedule(&other, Ping, cx.now().after(Duration::from_millis(50)))?;
        Ok(())
    }
}

#[test]
fn the_domain_thread_runs_policies_and_schedules_on_its_own() {
    let runtime = Runtime::spawn(Memory::new().with::<Counting>().policy(PingBack));
    let client = Client::<CountingApi>::in_process(runtime.clone());
    client.send(&c("a"), Add { n: 1 });

    let pings = || runtime.call(|m| m.events(&OtherId::new("a")).len()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while pings() < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(pings(), 2, "the policy's ping, then the scheduled one");
}

#[test]
fn an_inline_client_runs_policies_when_woken() {
    let runtime = Runtime::inline(Memory::new().with::<Counting>().policy(PingBack));
    let client = Client::<CountingApi>::in_process(runtime.clone());
    client.send(&c("a"), Add { n: 1 }).try_get().unwrap().unwrap();
    assert_eq!(
        runtime.call(|m| m.events(&OtherId::new("a")).len()).unwrap(),
        1,
        "the policy ran; the schedule isn't due"
    );
}

// An adapter that answers from another thread, as an HTTP client would.

#[derive(domain::Call, Debug, PartialEq)]
#[call(system = Slow, returns = bool)]
struct Check;

#[derive(domain::ExternalSystem)]
#[external_system(calls(Check))]
struct Slow;

struct SlowAdapter;

impl domain::Answer<Check> for SlowAdapter {
    async fn answer(&self, _: Check) -> domain::Answered<Check> {
        let slot: Arc<Mutex<(Option<bool>, Option<Waker>)>> = Arc::default();
        let answering = slot.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            let waker = {
                let mut slot = answering.lock().unwrap();
                slot.0 = Some(true);
                slot.1.take()
            };
            waker.into_iter().for_each(Waker::wake);
        });
        Ok(std::future::poll_fn(|cx| {
            let mut slot = slot.lock().unwrap();
            match slot.0 {
                Some(answer) => Poll::Ready(answer),
                None => {
                    slot.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
        .await)
    }
}

#[derive(domain::Policy)]
#[policy(from(Counter(Added)))]
struct CheckThenPing;

impl domain_memory::React for CheckThenPing {
    async fn react(
        &self,
        cx: &domain_memory::PolicyCx<Self>,
        _: CheckThenPingInput,
    ) -> Result<(), domain_memory::PolicyError> {
        if cx.call(Check).await? {
            cx.send(&OtherId::new("checked"), Ping)?;
        }
        Ok(())
    }
}

#[test]
fn a_call_answered_on_another_thread_wakes_the_domain_thread() {
    let runtime = Runtime::spawn(Memory::new().with::<Counting>().policy(CheckThenPing).external(Slow, SlowAdapter));
    let client = Client::<CountingApi>::in_process(runtime.clone());
    client.send(&c("a"), Add { n: 1 });

    let deadline = Instant::now() + Duration::from_secs(5);
    while runtime.call(|m| m.events(&OtherId::new("checked")).is_empty()).unwrap() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(runtime.call(|m| m.events(&OtherId::new("checked")).len()).unwrap(), 1);
}
