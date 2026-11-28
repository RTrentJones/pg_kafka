//! `pg_cancel_backend()` on the listener worker.

use super::{
    boxed, into_test_result, listener_pid, quick_producer, send, session, unique,
    wait_for_serving_listener, SendResult,
};
use crate::common::TestResult;
use crate::error_codes;
use crate::wire::requests::{conn, join_now, join_req};
use std::time::Duration;

/// Cancels sent; each is followed at once by a JoinGroup and a Produce.
const ROUNDS: usize = 8;

/// A cancel interrupts the statement it arrives in; one that arrives while the worker is idle
/// has nothing to cancel. Either way the worker keeps serving: Postgres's `errfinish` runs
/// CHECK_FOR_INTERRUPTS after every log line, so without interrupts held around the worker's
/// log macros the pending cancel raised its ERROR from inside the first LOG line after it (a
/// JoinGroup's), past the Rust frames, and the worker exited and restarted.
///
/// Asserts, for each of several rounds: `pg_cancel_backend()` reaches the listener; a JoinGroup
/// of a new group sent right after it is answered NONE; a Produce right after that succeeds;
/// and the listener's pid never changes (no restart).
pub async fn test_cancel_does_not_restart_the_worker() -> TestResult {
    into_test_result(cancel_rounds().await)
}

async fn cancel_rounds() -> SendResult<()> {
    println!("=== Test: pg_cancel_backend() on the listener ===\n");
    let db = session().await?;
    let pid = wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("cancel");
    let producer = quick_producer("15000")?;
    send(&producer, &topic, "before").await?;
    println!("Listener pid {pid}; {ROUNDS} rounds of cancel, JoinGroup, Produce...");
    for round in 0..ROUNDS {
        let signalled: bool = db
            .query_one("SELECT pg_cancel_backend($1)", &[&pid])
            .await
            .map_err(boxed)?
            .get(0);
        assert!(
            signalled,
            "round {round}: pg_cancel_backend() reached the listener"
        );
        let group = unique(&format!("cancel-{round}"));
        let mut c = conn().await.map_err(|e| boxed(e.to_string()))?;
        let joined = join_now(&mut c, &join_req(&group, "", "consumer", &["range"]))
            .await
            .map_err(|e| boxed(e.to_string()));
        let now = listener_pid(&db).await?;
        assert_eq!(
            now,
            Some(pid),
            "round {round}: the listener did not restart after the cancel"
        );
        let (code, ..) = joined?;
        assert_eq!(
            code,
            error_codes::NONE,
            "round {round}: JoinGroup after the cancel"
        );
        send(&producer, &topic, &format!("round-{round}")).await?;
        assert_eq!(
            listener_pid(&db).await?,
            Some(pid),
            "round {round}: the listener did not restart"
        );
    }
    println!("✅ The worker answered every request and kept pid {pid}");
    println!("✅ Test PASSED\n");
    Ok(())
}
