use super::*;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[test]
fn take_stops_at_the_limit_and_charges_nothing_past_it() {
    static POOL: DecodePool = DecodePool::new(1000);
    let budget = DecodeBudget::with_pool(100, &POOL);
    assert_eq!(budget.take(60), Ok(()));
    assert_eq!(budget.take(41), Err(Exhausted::Limit));
    assert_eq!(budget.exhausted(), Some(Exhausted::Limit));
    assert_eq!(budget.charged(), 60);
    assert_eq!(POOL.remaining(), 940);
    drop(budget);
    assert_eq!(POOL.remaining(), 1000, "dropped budget gives back");
}

#[test]
fn work_counts_what_was_given_back_and_stays_exhausted() {
    static POOL: DecodePool = DecodePool::new(1000);
    let budget = DecodeBudget::with_pool(100, &POOL).limit_work(150);
    budget.take(80).unwrap();
    budget.rewind(0);
    assert_eq!((budget.charged(), budget.work()), (0, 80));
    assert_eq!(budget.room(), 70, "the work left bounds the room");
    assert_eq!(budget.take(71), Err(Exhausted::Work));
    assert_eq!(budget.exhausted(), Some(Exhausted::Work));
    assert!(budget.work_exhausted());
    budget.rewind(0);
    assert!(
        budget.work_exhausted(),
        "a rewind gives back memory, not work"
    );
    assert_eq!(budget.exhausted(), None);
    assert_eq!(POOL.remaining(), 1000);
    // Unlimited by default.
    let unlimited = DecodeBudget::with_pool(100, &POOL);
    for _ in 0..10 {
        unlimited.take(100).unwrap();
        unlimited.rewind(0);
    }
    assert!(!unlimited.work_exhausted());
}

#[test]
fn take_stops_at_the_global_budget_and_says_what_it_wanted() {
    static POOL: DecodePool = DecodePool::new(50);
    let budget = DecodeBudget::with_pool(100, &POOL);
    assert_eq!(budget.take(40), Ok(()));
    assert_eq!(budget.take(20), Err(Exhausted::Global));
    assert_eq!(budget.exhausted(), Some(Exhausted::Global));
    assert_eq!(
        budget.wanted(),
        60,
        "a retry needs its 40 plus the 20 refused"
    );
    assert_eq!(POOL.remaining(), 10);
}

#[test]
fn the_charge_is_held_until_the_decoded_request_is_dropped() {
    static POOL: DecodePool = DecodePool::new(1000);
    let budget = DecodeBudget::with_pool(1000, &POOL);
    budget.take(300).unwrap();
    let charge = budget.into_charge();
    assert_eq!(charge.bytes(), 300);
    assert_eq!(POOL.remaining(), 700);
    drop(charge);
    assert_eq!(POOL.remaining(), 1000);
    drop(DecodedCharge::default());
}

#[test]
fn the_wait_is_half_the_produce_timeout_at_most_ten_seconds() {
    assert_eq!(
        decode_wait(30_000),
        Duration::from_secs(10),
        "the clients' default"
    );
    assert_eq!(decode_wait(5_000), Duration::from_millis(2_500));
    assert_eq!(decode_wait(1), Duration::ZERO);
    assert_eq!(decode_wait(0), MAX_DECODE_WAIT, "no deadline asked for");
    assert_eq!(decode_wait(-1), MAX_DECODE_WAIT);
    assert_eq!(decode_wait(i32::MAX), MAX_DECODE_WAIT);
}

#[tokio::test]
async fn a_waiter_gets_the_budget_a_decoded_request_gives_back() {
    static POOL: DecodePool = DecodePool::new(100);
    let budget = DecodeBudget::with_pool(100, &POOL);
    budget.take(80).unwrap();
    let held = budget.into_charge();
    let deadline = Instant::now() + Duration::from_secs(30);
    assert!(POOL.wait_for(20, deadline).await, "20 are free at once");
    let waiter = tokio::spawn(POOL.wait_for(50, deadline));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiter.is_finished(), "only 20 of the 50 wanted are free");
    // Given back on another thread, as the DB thread drops a request it ran.
    std::thread::spawn(move || drop(held)).join().unwrap();
    let woken = tokio::time::timeout(Duration::from_secs(5), waiter).await;
    assert!(
        matches!(woken, Ok(Ok(true))),
        "woken when the request is dropped"
    );
}

#[tokio::test]
async fn a_budget_wait_ends_at_its_deadline() {
    static POOL: DecodePool = DecodePool::new(100);
    let budget = DecodeBudget::with_pool(100, &POOL);
    budget.take(100).unwrap();
    let start = Instant::now();
    assert!(!POOL.wait_for(1, start + Duration::from_millis(300)).await);
    assert!(start.elapsed() >= Duration::from_millis(300));
    // Bytes given back that are still too few do not end the wait early.
    let wait = tokio::spawn(POOL.wait_for(50, Instant::now() + Duration::from_millis(300)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    budget.release(10);
    assert!(!wait.await.unwrap());
}

#[test]
fn waiting_frames_are_charged_like_incomplete_ones() {
    static BUDGET: AtomicUsize = AtomicUsize::new(5 * 1024 * 1024);
    let small = HeldFrame::charge_to(UNMETERED_FRAME_BYTES, &BUDGET).unwrap();
    assert_eq!(
        BUDGET.load(Ordering::Relaxed),
        5 * 1024 * 1024,
        "not metered"
    );
    let large = HeldFrame::charge_to(3 * 1024 * 1024, &BUDGET).unwrap();
    assert_eq!(BUDGET.load(Ordering::Relaxed), 2 * 1024 * 1024);
    assert!(
        HeldFrame::charge_to(3 * 1024 * 1024, &BUDGET).is_none(),
        "the budget lacks a second one"
    );
    drop((small, large));
    assert_eq!(BUDGET.load(Ordering::Relaxed), 5 * 1024 * 1024);
}

/// Spawn `n` tasks that each run a decode of `ms` milliseconds under `slots`, recording the
/// peak number running at once.
async fn run_decodes(
    slots: &'static LargeDecodes,
    n: usize,
    ms: u64,
    wait: Duration,
) -> (usize, Vec<Result<(), NotRun>>) {
    let running = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let deadline = Instant::now() + wait;
    let tasks: Vec<_> = (0..n)
        .map(|_| {
            let (running, peak) = (Arc::clone(&running), Arc::clone(&peak));
            tokio::spawn(slots.run(deadline, move || {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(ms));
                running.fetch_sub(1, Ordering::SeqCst);
            }))
        })
        .collect();
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.unwrap());
    }
    (peak.load(Ordering::SeqCst), results)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_decodes_run_at_most_slots_at_a_time_and_all_waiters_get_one() {
    static SLOTS: LargeDecodes = LargeDecodes::new(LARGE_DECODE_SLOTS);
    // Many more waiters than slots: none is turned away while the deadline allows.
    let (peak, results) = run_decodes(&SLOTS, 24, 20, Duration::from_secs(30)).await;
    assert!(peak <= LARGE_DECODE_SLOTS, "{peak} ran at once");
    assert!(results.iter().all(Result::is_ok), "{results:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn waiting_for_a_slot_holds_no_thread() {
    static SLOTS: LargeDecodes = LargeDecodes::new(LARGE_DECODE_SLOTS);
    // Two slow decodes and a hundred waiters: the two I/O threads still run a timer task.
    let decodes = tokio::spawn(run_decodes(&SLOTS, 102, 200, Duration::from_secs(1)));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let start = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let waited = start.elapsed();
    assert!(
        waited < Duration::from_millis(300),
        "timer waited {waited:?}"
    );
    let (peak, results) = decodes.await.unwrap();
    assert!(peak <= LARGE_DECODE_SLOTS);
    let timed_out = results
        .iter()
        .filter(|r| **r == Err(NotRun::TimedOut))
        .count();
    assert!(
        (90..=100).contains(&timed_out),
        "a second fits about ten 200 ms decodes in two slots; {timed_out} timed out"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slots_are_granted_in_arrival_order() {
    static SLOTS: LargeDecodes = LargeDecodes::new(1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let first = tokio::spawn(SLOTS.run(deadline, move || {
        let _ = hold.recv();
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut queued = Vec::new();
    for i in 0..5 {
        let order = Arc::clone(&order);
        queued.push(tokio::spawn(SLOTS.run(deadline, move || {
            order.lock().unwrap().push(i);
        })));
        // Each joins the queue before the next.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    for q in queued {
        q.await.unwrap().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_that_stops_waiting_does_not_free_the_running_decodes_slot() {
    static SLOTS: LargeDecodes = LargeDecodes::new(1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let caller = tokio::spawn(SLOTS.run(deadline, move || {
        let _ = hold.recv();
    }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    caller.abort();
    let _ = caller.await;
    let short = Instant::now() + Duration::from_millis(100);
    assert_eq!(
        SLOTS.run(short, || ()).await,
        Err(NotRun::TimedOut),
        "the abandoned decode still runs and holds the slot"
    );
    release.send(()).unwrap();
    assert_eq!(SLOTS.run(deadline, || 7).await, Ok(7));
}

#[tokio::test]
async fn a_panicking_decode_is_reported_and_frees_its_slot() {
    static SLOTS: LargeDecodes = LargeDecodes::new(1);
    let deadline = Instant::now() + Duration::from_secs(30);
    assert_eq!(
        SLOTS
            .run(deadline, || -> () { panic!("decoder bug") })
            .await,
        Err(NotRun::Panicked)
    );
    assert_eq!(SLOTS.run(deadline, || 1).await, Ok(1));
}
