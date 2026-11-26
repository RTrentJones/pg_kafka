//! What ListGroups reads of the groups known by their offsets, and in which order it lists
//! them: an id-order (byte order) prefix of the groups it selects, read by walking the
//! distinct group ids of the offset tables' indexes up to the response bound.

use crate::common::{DynResult, TestResult};
use crate::error_codes;
use crate::setup::{SystemGucGuard, TestContext};
use crate::wire::requests::{conn, join_now, join_req, leave, s};
use crate::wire::Wire;
use crate::worker::{boxed, listener_pid, wait_for_serving_listener, ServerLog};
use kafka_protocol::messages::list_groups_request::ListGroupsRequest;
use kafka_protocol::messages::list_groups_response::ListGroupsResponse;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_postgres::Client;

/// Groups of the read-count test, each with [`PARTITIONS_PER_GROUP`] committed offsets.
const GROUPS: i32 = 50;
/// Committed offsets of each such group: 200 000 rows in all.
const PARTITIONS_PER_GROUP: i32 = 4_000;
/// What one ListGroups may read of the offset tables (rows and index entries): far below
/// one read of the test's rows, whatever other tests left behind (a few entries per group).
const MAX_READ: i64 = 20_000;

/// `pg_kafka.fetch_max_bytes` for the ordering tests, in bytes.
const CAP: usize = 128 * 1024;

/// The ListGroups offset-group read was a UNION of every row of kafka.consumer_offsets and
/// kafka.txn_pending_offsets, sorted (504 ms for a million rows on the DB thread, on every
/// ListGroups), whatever it then listed. Now it walks the distinct ids through the indexes,
/// a few index entries per listed group.
///
/// Asserts: with 50 groups of 4 000 committed offsets each, one ListGroups lists every one
/// of them and reads fewer than 20 000 rows and index entries of the two offset tables
/// (their `seq_tup_read + idx_tup_fetch` and their indexes' `idx_tup_read`, flushed by
/// ending the worker before and after).
pub async fn test_list_groups_reads_only_the_listed_groups() -> TestResult {
    println!("=== Test: ListGroups reads a few index entries per listed group ===\n");
    let ctx = TestContext::new().await?;
    let base = ctx.unique_group("list-reads").await;
    let topic = crate::fixtures::topic_with_partitions(&ctx, "list-reads", 1).await?;
    let result = reads(ctx.db(), &base, &topic)
        .await
        .map_err(|e| e.to_string());
    ctx.db()
        .execute(
            "DELETE FROM kafka.consumer_offsets WHERE starts_with(group_id, $1)",
            &[&base],
        )
        .await?;
    result?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn reads(db: &Client, base: &str, topic: &str) -> DynResult<()> {
    println!(
        "Step 1: {} committed offsets in {GROUPS} groups...",
        GROUPS * PARTITIONS_PER_GROUP
    );
    db.execute(
        "INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset)
         SELECT $1 || '-' || lpad(g::text, 3, '0'), t.id, p, 0
         FROM kafka.topics t, generate_series(1, $3) g, generate_series(0, $4 - 1) p
         WHERE t.name = $2",
        &[&base, &topic, &GROUPS, &PARTITIONS_PER_GROUP],
    )
    .await?;
    db.batch_execute("VACUUM (ANALYZE) kafka.consumer_offsets")
        .await?;

    println!("Step 2: ListGroups, between two flushes of the worker's statistics...");
    let before = offset_table_reads(db).await.map_err(|e| e.to_string())?;
    let mut w = Wire::connect().await?;
    let started = Instant::now();
    let resp: ListGroupsResponse = w.call(4, &ListGroupsRequest::default()).await?;
    let took = started.elapsed();
    drop(w);
    let after = offset_table_reads(db).await.map_err(|e| e.to_string())?;
    assert_eq!(resp.error_code, error_codes::NONE);
    let ours = resp
        .groups
        .iter()
        .filter(|g| g.group_id.as_str().starts_with(base))
        .count();
    let read = after - before;
    println!(
        "  {} groups listed ({ours} of the test's {GROUPS}) in {took:?}, {read} rows and index entries read",
        resp.groups.len()
    );
    assert_eq!(ours, usize::try_from(GROUPS)?, "every test group is listed");
    assert!(
        read < MAX_READ,
        "ListGroups read {read} rows and index entries of the offset tables"
    );
    Ok(())
}

/// Rows and index entries of kafka.consumer_offsets and kafka.txn_pending_offsets read
/// so far. The worker flushes its statistics only when it exits, so it is ended first (and
/// waited for to serve again); the counters are then read until two readings a second
/// apart agree.
async fn offset_table_reads(db: &Client) -> crate::worker::SendResult<i64> {
    let old = listener_pid(db).await?;
    if let Some(pid) = old {
        db.execute("SELECT pg_terminate_backend($1)", &[&pid])
            .await
            .map_err(boxed)?;
    }
    wait_for_serving_listener(db, old, Duration::from_secs(30)).await?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last = read_counters(db).await?;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = read_counters(db).await?;
        if now == last {
            return Ok(now);
        }
        if Instant::now() > deadline {
            return Err("the statistics of the offset tables never settled".into());
        }
        last = now;
    }
}

async fn read_counters(db: &Client) -> crate::worker::SendResult<i64> {
    db.batch_execute("SELECT pg_stat_clear_snapshot()")
        .await
        .map_err(boxed)?;
    let row = db
        .query_one(
            "SELECT (SELECT COALESCE(sum(COALESCE(seq_tup_read, 0) + COALESCE(idx_tup_fetch, 0)), 0)
                     FROM pg_stat_all_tables
                     WHERE relid IN ('kafka.consumer_offsets'::regclass,
                                     'kafka.txn_pending_offsets'::regclass))::bigint
                  + (SELECT COALESCE(sum(idx_tup_read), 0)
                     FROM pg_stat_all_indexes
                     WHERE relid IN ('kafka.consumer_offsets'::regclass,
                                     'kafka.txn_pending_offsets'::regclass))::bigint",
            &[],
        )
        .await
        .map_err(boxed)?;
    Ok(row.get(0))
}

/// A ListGroups v4 of the groups in `states` (all if empty): the listed ids that start with
/// `prefix`, in the order listed, and whether all listed ids are in byte order.
async fn listed(prefix: &str, states: &[&str]) -> DynResult<(Vec<String>, bool)> {
    let mut req = ListGroupsRequest::default();
    req.states_filter = states.iter().map(|state| s(state)).collect();
    let mut w = Wire::connect().await?;
    let resp: ListGroupsResponse = w.call(4, &req).await?;
    assert_eq!(resp.error_code, error_codes::NONE);
    let ids: Vec<&str> = resp.groups.iter().map(|g| g.group_id.as_str()).collect();
    let in_order = ids.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes());
    Ok((
        ids.iter()
            .filter(|id| id.starts_with(prefix))
            .map(ToString::to_string)
            .collect(),
        in_order,
    ))
}

/// Commit offset 0 of partition 0 of `topic` for each of `groups`, by SQL.
async fn commit_by_sql(db: &Client, topic: &str, groups: &[String]) -> DynResult<()> {
    db.execute(
        "INSERT INTO kafka.consumer_offsets (group_id, topic_id, partition_id, committed_offset)
         SELECT g, t.id, 0, 0 FROM kafka.topics t, unnest($2::text[]) g WHERE t.name = $1",
        &[&topic, &groups],
    )
    .await?;
    Ok(())
}

/// The collation of `table`'s group_id column.
async fn group_id_collation(db: &Client, table: &str) -> DynResult<String> {
    Ok(db
        .query_one(
            "SELECT c.collname::text FROM pg_attribute a JOIN pg_collation c ON c.oid = a.attcollation
             WHERE a.attrelid = $1::text::regclass AND a.attname = 'group_id'",
            &[&table],
        )
        .await?
        .get(0))
}

/// The SQL that cut the offset-only groups ordered them by the column's collation, the
/// database's default, and the handler sorted the ids it got by their bytes. Under a
/// linguistic collation ('a' < 'B' < 'c') the cut kept the first groups in that order and the
/// listing then went on in byte order ('B' < 'a'), so groups between them were left out
/// without a cut ever being logged.
///
/// Asserts: with 8 offset-only groups of 30 kB ids whose first letters alternate in case (a,
/// B, c, D, ...) and `pg_kafka.fetch_max_bytes` at 128 kB, ListGroups lists a non-empty
/// byte-order prefix of them, in byte order (this fails before only on a database whose
/// collation is linguistic); and the group_id columns of kafka.consumer_offsets and
/// kafka.txn_pending_offsets sort by bytes (collation "C"), whatever the database's collation.
pub async fn test_list_groups_is_a_byte_order_prefix() -> TestResult {
    println!("=== Test: ListGroups lists a byte-order prefix ===\n");
    let ctx = TestContext::new().await?;
    let cap = SystemGucGuard::set(ctx.db(), "pg_kafka.fetch_max_bytes", &CAP.to_string())
        .await
        .map_err(widen)?
        .ok_or("the server refused pg_kafka.fetch_max_bytes")?;
    // "!" sorts before the letters and digits other tests' groups start with.
    let prefix = format!("!{}", ctx.unique_group("list-order").await);
    let result = byte_order(&ctx, &prefix).await.map_err(|e| e.to_string());
    cap.restore().await.map_err(widen)?;
    ctx.db()
        .execute(
            "DELETE FROM kafka.consumer_offsets WHERE starts_with(group_id, $1)",
            &[&prefix],
        )
        .await?;
    result?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn byte_order(ctx: &TestContext, prefix: &str) -> DynResult<()> {
    let database: String = ctx
        .db()
        .query_one(
            "SELECT datcollate FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await?
        .get(0);
    println!("  the database's collation: {database}");

    let topic = crate::fixtures::topic_with_partitions(ctx, "list-order", 1).await?;
    let pad = "x".repeat(30 * 1024);
    let mut groups: Vec<String> = ["a", "B", "c", "D", "e", "F", "g", "H"]
        .iter()
        .map(|letter| format!("{prefix}-{letter}-{pad}"))
        .collect();
    println!("Step 1: offsets for 8 groups of 30 kB ids, a B c D e F g H...");
    commit_by_sql(ctx.db(), &topic, &groups).await?;
    groups.sort_unstable();

    println!("Step 2: ListGroups...");
    let (ours, in_order) = listed(prefix, &[]).await?;
    let letters: Vec<&str> = ours
        .iter()
        .map(|id| &id[prefix.len() + 1..prefix.len() + 2])
        .collect();
    println!("  listed {letters:?}");
    assert!(in_order, "listed in byte order");
    assert!(!ours.is_empty(), "the first groups are listed");
    assert!(ours.len() < groups.len(), "the list is cut");
    assert_eq!(
        ours,
        groups[..ours.len()],
        "the listed groups are the first in byte order"
    );
    // Checked last, so that on a database with a linguistic collation the listing itself is
    // what fails before the schema sorts these columns by bytes.
    for table in ["kafka.consumer_offsets", "kafka.txn_pending_offsets"] {
        let collation = group_id_collation(ctx.db(), table).await?;
        println!("  {table}.group_id collation: {collation}");
        assert_eq!(collation, "C", "{table}.group_id must sort by bytes");
    }
    Ok(())
}

/// With a state filter the offset-only groups the SQL read were cut by a budget that also
/// counted groups the filter then removed (held groups in another state), so the list could
/// end before the bound with offset-only groups left out and a later held group listed, and
/// no cut logged.
///
/// Asserts: with three held CompletingRebalance groups (with offsets), seven offset-only
/// groups and one held Empty group after them, all of 30 kB ids, ListGroups filtered to Empty
/// lists a non-empty prefix (in byte order) of the eight Empty groups, and the server logs the
/// cut when its log is readable.
pub async fn test_list_groups_state_filter_lists_a_prefix() -> TestResult {
    println!("=== Test: ListGroups with a state filter lists a prefix ===\n");
    let ctx = TestContext::new().await?;
    let cap = SystemGucGuard::set(ctx.db(), "pg_kafka.fetch_max_bytes", &CAP.to_string())
        .await
        .map_err(widen)?
        .ok_or("the server refused pg_kafka.fetch_max_bytes")?;
    let prefix = format!("!{}", ctx.unique_group("list-filter").await);
    let mut held = Vec::new();
    let result = filtered(&ctx, &prefix, &mut held)
        .await
        .map_err(|e| e.to_string());
    cap.restore().await.map_err(widen)?;
    for (mut c, group, member) in held {
        let _ = leave(&mut c, &group, &member).await;
    }
    ctx.db()
        .execute(
            "DELETE FROM kafka.consumer_offsets WHERE starts_with(group_id, $1)",
            &[&prefix],
        )
        .await?;
    result?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

async fn filtered(
    ctx: &TestContext,
    prefix: &str,
    held: &mut Vec<(TcpStream, String, String)>,
) -> DynResult<()> {
    let topic = crate::fixtures::topic_with_partitions(ctx, "list-filter", 1).await?;
    let pad = "x".repeat(30 * 1024);
    let id = |name: &str| format!("{prefix}-{name}-{pad}");

    println!("Step 1: three held groups in their sync phase, with offsets...");
    let busy: Vec<String> = ["A0", "A1", "A2"].iter().map(|n| id(n)).collect();
    for group in &busy {
        let mut c = conn().await?;
        let mut join = join_req(group, "", "consumer", &["range"]);
        join.session_timeout_ms = 60_000;
        join.rebalance_timeout_ms = 60_000;
        let (e, _, member, _) = join_now(&mut c, &join).await?;
        assert_eq!(e, error_codes::NONE);
        held.push((c, group.clone(), member));
    }
    commit_by_sql(ctx.db(), &topic, &busy).await?;

    println!("Step 2: seven offset-only groups and a held Empty group after them...");
    let offset_only: Vec<String> = ["B", "C", "D", "E", "F", "G", "H"]
        .iter()
        .map(|n| id(n))
        .collect();
    commit_by_sql(ctx.db(), &topic, &offset_only).await?;
    let empty = id("Z");
    let mut c = conn().await?;
    let (e, _, member, _) = join_now(&mut c, &join_req(&empty, "", "consumer", &["range"])).await?;
    assert_eq!(e, error_codes::NONE);
    assert_eq!(leave(&mut c, &empty, &member).await?, error_codes::NONE);

    println!("Step 3: ListGroups of the Empty groups...");
    let log = ServerLog::open(ctx.db()).await.map_err(|e| e.to_string())?;
    let log_start = log.as_ref().map_or(0, ServerLog::end);
    let (ours, in_order) = listed(prefix, &["Empty"]).await?;
    let names: Vec<&str> = ours
        .iter()
        .map(|id| &id[prefix.len() + 1..prefix.len() + 2])
        .collect();
    println!("  listed {names:?}");
    let mut expected = offset_only.clone();
    expected.push(empty);
    assert!(in_order, "listed in byte order");
    assert!(!ours.is_empty(), "the first groups are listed");
    assert!(ours.len() < expected.len(), "the list is cut");
    assert_eq!(
        ours,
        expected[..ours.len()],
        "the listed groups are the first Empty groups in byte order"
    );
    match log {
        Some(log) => {
            // The WARNING is written once the response is built, before it is sent.
            let logged = log.since(log_start);
            assert!(
                logged.contains("ListGroups:") && logged.contains("left out"),
                "the cut is logged"
            );
        }
        None => {
            println!("  (the server log is not a regular file; the cut's WARNING is not checked)");
        }
    }
    Ok(())
}

/// The guard's errors are `Send + Sync`; this file's results are not.
fn widen(e: crate::setup::GucError) -> Box<dyn std::error::Error> {
    e
}
