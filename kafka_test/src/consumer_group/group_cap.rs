//! `pg_kafka.max_consumer_groups` lowered below the number of groups in memory.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::requests::{conn, join_now, join_req, leave, list_groups};
use tokio_postgres::Client;

const GUC: &str = "pg_kafka.max_consumer_groups";

/// ALTER SYSTEM SET (or RESET for `None`) the cap, then reload. The JoinGroup handler
/// reads the cap before every join.
async fn set_cap(db: &Client, value: Option<usize>) -> DynResult<()> {
    let sql = match value {
        Some(v) => format!("ALTER SYSTEM SET {GUC} = {v}"),
        None => format!("ALTER SYSTEM RESET {GUC}"),
    };
    db.batch_execute(&sql).await?;
    db.batch_execute("SELECT pg_reload_conf()").await?;
    // The worker reloads on SIGHUP at its next loop iteration.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    Ok(())
}

/// A group whose only member joins and leaves, so it is Empty in memory.
async fn empty_group(group: &str) -> DynResult<()> {
    let mut c = conn().await?;
    let (e, _, member, _) = join_now(&mut c, &join_req(group, "", "consumer", &["range"])).await?;
    assert_eq!(e, error_codes::NONE);
    assert_eq!(leave(&mut c, group, &member).await?, error_codes::NONE);
    Ok(())
}

/// With the cap lowered to one more than the groups that have members (so at least
/// three Empty groups are over it), a JoinGroup for a new group is admitted: the
/// coordinator forgets as many Empty groups as it takes, not only one.
pub async fn test_lowered_group_cap_still_admits_new_groups() -> TestResult {
    println!("=== Test: a lowered max_consumer_groups still admits new groups ===\n");
    let ctx = TestContext::new().await?;
    let fresh = ctx.unique_group("cap-new").await;

    println!("Step 1: three Empty groups in memory...");
    for i in 0..3 {
        empty_group(&ctx.unique_group(&format!("cap-empty-{i}")).await).await?;
    }
    let live = list_groups()
        .await?
        .into_iter()
        .filter(|(_, _, state)| state != "Empty")
        .count();
    let cap = live + 1;
    println!("✅ {live} group(s) with members; lowering the cap to {cap}\n");

    println!("Step 2: a JoinGroup for a new group...");
    set_cap(ctx.db(), Some(cap)).await?;
    let outcome = async {
        let mut c = conn().await?;
        let (e, _, member, _) =
            join_now(&mut c, &join_req(&fresh, "", "consumer", &["range"])).await?;
        if e == error_codes::NONE {
            leave(&mut c, &fresh, &member).await?;
        }
        DynResult::Ok(e)
    }
    .await
    // Held across the reset below; a boxed error is not Send.
    .map_err(|e| e.to_string());
    set_cap(ctx.db(), None).await?;
    assert_eq!(outcome?, error_codes::NONE, "the new group is admitted");
    println!("✅ admitted\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
