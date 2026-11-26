//! `pg_debug!` messages reach the server log when DEBUG1 is logged.

use super::{boxed, into_test_result, session, set_guc, unique, SendResult, ServerLog};
use crate::common::TestResult;
use crate::error_codes;
use crate::wire::requests::{conn, s};
use kafka_protocol::messages::heartbeat_request::HeartbeatRequest;
use kafka_protocol::messages::GroupId;
use std::time::{Duration, Instant};

/// `pg_debug!` formats its message only when `message_level_is_interesting(DEBUG1)`; a wrong
/// check would silence every DEBUG1 line of the worker.
///
/// Asserts: with `log_min_messages = debug1`, a Heartbeat for an unknown group writes the
/// handler's `pg_debug!` line naming the group to the server log; with the setting reset, a
/// second one does not (when the server log is a regular file the test can read).
pub async fn test_pg_debug_logs_when_debug1_is_logged() -> TestResult {
    into_test_result(debug_lines().await)
}

async fn heartbeat(group: &str) -> SendResult<()> {
    let mut c = conn().await.map_err(|e| boxed(e.to_string()))?;
    let mut r = HeartbeatRequest::default();
    r.group_id = GroupId(s(group));
    r.generation_id = 1;
    r.member_id = s("nobody");
    let resp = crate::wire::roundtrip(&mut c, 1, 1, &r)
        .await
        .map_err(boxed)?;
    assert_eq!(resp.error_code, error_codes::UNKNOWN_MEMBER_ID);
    Ok(())
}

/// Wait until the log since `from` contains `needle`, for up to `within`.
async fn logged(log: &ServerLog, from: u64, needle: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if log.since(from).contains(needle) {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn debug_lines() -> SendResult<()> {
    println!("=== Test: pg_debug! logs when DEBUG1 is logged ===\n");
    let db = session().await?;
    let Some(log) = ServerLog::open(&db).await? else {
        println!("  (the server log is not a regular file; nothing to check)");
        return Ok(());
    };
    let loud = unique("debug-on");
    let quiet = unique("debug-off");
    println!("Step 1: log_min_messages = debug1, a Heartbeat for {loud}...");
    let from = log.end();
    set_guc(&db, "log_min_messages", Some("debug1")).await?;
    let sent = heartbeat(&loud).await;
    let found = logged(
        &log,
        from,
        &format!("group_id={loud}"),
        Duration::from_secs(5),
    )
    .await;
    set_guc(&db, "log_min_messages", None).await?;
    sent?;
    assert!(found, "the handler's pg_debug! line is in the server log");

    println!("Step 2: log_min_messages reset, a Heartbeat for {quiet}...");
    let from = log.end();
    heartbeat(&quiet).await?;
    // The answer is sent after the handler ran, so its line would be written by now.
    let found = logged(&log, from, &quiet, Duration::from_millis(500)).await;
    assert!(!found, "no DEBUG1 line with the default log_min_messages");
    println!("✅ Test PASSED\n");
    Ok(())
}
