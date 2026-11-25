//! The SQL maintenance functions against a concurrent DROP EXTENSION: they lock the
//! extension object before any member table, as every worker transaction does.

use super::lock_order::{backend_pid, drop_furthest_leaf, wait_until, ATTEMPTS};
use crate::common::TestResult;
use crate::worker::{boxed, into_test_result, session, wait_for_serving_listener, SendResult};
use std::time::Duration;
use tokio_postgres::Client;

/// What a backend waits for: the relation, the catalog of a database object
/// (`pg_extension`), or the lock type.
const WAITS_FOR: &str = "SELECT CASE locktype WHEN 'relation' THEN relation::regclass::text
                                              WHEN 'object' THEN classid::regclass::text
                                              ELSE locktype END
                         FROM pg_locks WHERE pid = $1 AND NOT granted";

const WAITING: &str = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)";

async fn waits_for(db: &Client, pid: i32) -> SendResult<Option<String>> {
    Ok(db
        .query_opt(WAITS_FOR, &[&pid])
        .await
        .map_err(boxed)?
        .map(|r| r.get(0)))
}

/// `pg_kafka_run_partition_maintenance()` must take the extension lock before any
/// member table. DROP EXTENSION locks the extension and then its members (kafka.topics
/// before kafka.messages); the pass locked ONLY kafka.messages ACCESS SHARE first and
/// then waited (in `lock_message_tables`) for kafka.topics, so a DROP that arrived in
/// between took kafka.topics and waited for kafka.messages: a lock cycle, which
/// Postgres broke with "deadlock detected" in DROP (or, had the pass got to its
/// `ALTER EXTENSION ... ADD TABLE`, which locks the extension, in either).
///
/// The race, made deterministic: the worker's furthest leaf ahead is dropped so the
/// pass has one to create, and a session holds kafka.topics ROW EXCLUSIVE, so the pass
/// stops in `lock_message_tables`. Then `BEGIN; DROP EXTENSION pg_kafka CASCADE;
/// ROLLBACK` runs in another session, and kafka.topics is released.
///
/// Asserts: DROP waits for the extension object (the pass holds it), not for a member
/// table; then the pass recreates the leaf and DROP succeeds (and is rolled back). An
/// attempt the worker's own maintenance overlaps (it recreated the leaf first, or held
/// the maintenance lock so the pass gave up before waiting for kafka.topics) proves
/// nothing and is run again, up to [`ATTEMPTS`] times.
pub async fn test_partition_maintenance_vs_drop_extension() -> TestResult {
    into_test_result(maintenance_vs_drop().await)
}

async fn maintenance_vs_drop() -> SendResult<()> {
    println!("=== Test: partition maintenance against a concurrent DROP EXTENSION ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;

    // The worker's own pass (every 60 s) can overlap an attempt: it may recreate the
    // dropped leaf before the test's pass (which then has nothing to create,
    // `created == Ok(0)`), or hold the maintenance lock, itself stopped behind the
    // blocker, so the test's pass gives up at `try_maintenance_lock` and never waits
    // for kafka.topics. Neither says anything about the lock order: run the whole race
    // again. Every assertion applies, unchanged, to the first conclusive attempt.
    let mut inconclusive = String::new();
    for attempt in 1..=ATTEMPTS {
        println!("--- attempt {attempt} of {ATTEMPTS} ---");
        let leaf = drop_furthest_leaf(&db).await?;

        let result = maintenance_race(&db).await;

        // Whatever happened, leave the worker's layout as it was.
        db.query("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map_err(boxed)?;
        let MaintenanceRace {
            stopped,
            drop_waited_for,
            drop_result,
            created,
        } = result?;
        if !stopped || created == Ok(0) {
            inconclusive = if stopped {
                format!("the pass found {leaf} already recreated (by the worker's own pass)")
            } else {
                "the pass never waited for kafka.topics".to_string()
            };
            println!(
                "  inconclusive: {inconclusive}; the worker's maintenance overlapped, retrying"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        assert_eq!(
            drop_waited_for.as_deref(),
            Some("pg_extension"),
            "DROP EXTENSION must wait for the pass's extension lock, not hold a member table \
             the pass needs and wait for another"
        );
        drop_result.map_err(|e| format!("DROP EXTENSION failed: {e}"))?;
        assert_eq!(created, Ok(1), "the pass must recreate {leaf}");
        println!("✅ DROP EXTENSION waited for the pass, and both succeeded");
        return Ok(());
    }
    Err(format!("no conclusive attempt in {ATTEMPTS}; the last: {inconclusive}").into())
}

/// What [`maintenance_race`] observed.
struct MaintenanceRace {
    /// Whether the pass was seen waiting for kafka.topics.
    stopped: bool,
    /// The lock DROP EXTENSION waited for.
    drop_waited_for: Option<String>,
    /// DROP EXTENSION's outcome.
    drop_result: Result<(), String>,
    /// The pass's outcome: the leaves it created.
    created: Result<i64, String>,
}

async fn maintenance_race(db: &Client) -> SendResult<MaintenanceRace> {
    let blocker = session().await?;
    blocker
        .batch_execute("BEGIN; LOCK TABLE kafka.topics IN ROW EXCLUSIVE MODE")
        .await
        .map_err(boxed)?;
    println!("Step 1: a session holds kafka.topics ROW EXCLUSIVE");

    let maintainer = session().await?;
    maintainer
        .batch_execute("SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let maintainer_pid = backend_pid(&maintainer).await?;
    let pass = tokio::spawn(async move {
        maintainer
            .query_one("SELECT pg_kafka_run_partition_maintenance()", &[])
            .await
            .map(|r| r.get::<_, i64>(0))
            .map_err(|e| format!("{e:?}"))
    });
    let stopped = wait_until(
        db,
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted
                        AND locktype = 'relation' AND relation = 'kafka.topics'::regclass)",
        maintainer_pid,
        Duration::from_secs(5),
    )
    .await?;
    println!("Step 2: the pass waits for kafka.topics: {stopped}");

    let dropper = session().await?;
    dropper
        .batch_execute("SET lock_timeout = '20s'; SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let dropper_pid = backend_pid(&dropper).await?;
    let drop = tokio::spawn(async move {
        dropper
            .batch_execute("BEGIN; DROP EXTENSION pg_kafka CASCADE; ROLLBACK")
            .await
            .map_err(|e| format!("{e:?}"))
    });
    let drop_waited_for = if wait_until(db, WAITING, dropper_pid, Duration::from_secs(5)).await? {
        waits_for(db, dropper_pid).await?
    } else {
        None
    };
    println!("Step 3: DROP EXTENSION waits for {drop_waited_for:?}");

    blocker.batch_execute("COMMIT").await.map_err(boxed)?;
    println!("Step 4: kafka.topics released");
    let created = pass.await.map_err(boxed)?;
    let drop_result = drop.await.map_err(boxed)?;
    println!("Step 5: the pass returned {created:?}, DROP EXTENSION {drop_result:?}");
    Ok(MaintenanceRace {
        stopped,
        drop_waited_for,
        drop_result,
        created,
    })
}

/// `pg_kafka_run_retention_sweep()` must take the extension lock before any member
/// table too: a session holding an uncommitted DROP EXTENSION makes the sweep wait for
/// the extension object, not for kafka.messages (which, with the extension, it would
/// otherwise reach after taking the maintenance lock, the order that deadlocks with a
/// DROP still walking the members). The DROP is rolled back and the sweep then runs.
pub async fn test_retention_sweep_waits_for_drop_extension_first() -> TestResult {
    into_test_result(sweep_vs_drop().await)
}

async fn sweep_vs_drop() -> SendResult<()> {
    println!("=== Test: the retention sweep waits for DROP EXTENSION's extension lock ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;

    let dropper = session().await?;
    dropper
        .batch_execute("SET lock_timeout = '10s'; BEGIN; DROP EXTENSION pg_kafka CASCADE")
        .await
        .map_err(boxed)?;
    println!("Step 1: a session ran DROP EXTENSION pg_kafka CASCADE, uncommitted");

    let sweeper = session().await?;
    sweeper
        .batch_execute("SET lock_timeout = '20s'; SET statement_timeout = '30s'")
        .await
        .map_err(boxed)?;
    let sweeper_pid = backend_pid(&sweeper).await?;
    let sweep = tokio::spawn(async move {
        sweeper
            .query("SELECT * FROM pg_kafka_run_retention_sweep()", &[])
            .await
            .map(|rows| rows.len())
            .map_err(|e| format!("{e:?}"))
    });
    let waited = if wait_until(&db, WAITING, sweeper_pid, Duration::from_secs(5)).await? {
        waits_for(&db, sweeper_pid).await?
    } else {
        None
    };
    println!("Step 2: the sweep waits for {waited:?}");

    dropper.batch_execute("ROLLBACK").await.map_err(boxed)?;
    println!("Step 3: DROP EXTENSION rolled back");
    let swept = sweep.await.map_err(boxed)?;
    println!("Step 4: the sweep returned {swept:?}");

    assert_eq!(
        waited.as_deref(),
        Some("pg_extension"),
        "the sweep must wait for the extension lock before locking any member table"
    );
    swept.map_err(|e| format!("the sweep failed: {e}"))?;
    println!("✅ the sweep waited for the extension first, then ran");
    Ok(())
}

/// The SQL maintenance functions are for superusers: EXECUTE is revoked from PUBLIC.
/// Both take the extension lock first and then wait for the maintenance advisory lock,
/// which needs no privilege at all: a role that could call the sweep would, holding
/// `pg_advisory_lock(MAINTENANCE_LOCK_KEY)` in one session and calling it with
/// `lock_timeout = 0` in another, hold the extension lock for as long as it liked, so
/// DROP EXTENSION would wait forever and every worker transaction queue behind DROP.
///
/// Asserts: PUBLIC has no EXECUTE on either function; a role with no grant, holding the
/// advisory lock, is refused both at once ("permission denied for function"), not left
/// waiting while holding the extension lock; a superuser still runs both.
pub async fn test_maintenance_functions_refuse_public() -> TestResult {
    into_test_result(maintenance_functions_refuse_public().await)
}

/// The partition-maintenance advisory lock (`retention::MAINTENANCE_LOCK_KEY`).
pub(crate) const MAINTENANCE_LOCK_KEY: i64 = 8_099_547_369_649_367_905;

const MAINTENANCE_FUNCTIONS: [(&str, &str); 2] = [
    (
        "pg_kafka_run_retention_sweep(int, bigint)",
        "SELECT * FROM pg_kafka_run_retention_sweep()",
    ),
    (
        "pg_kafka_run_partition_maintenance(int)",
        "SELECT pg_kafka_run_partition_maintenance()",
    ),
];

async fn maintenance_functions_refuse_public() -> SendResult<()> {
    println!("=== Test: PUBLIC cannot execute the SQL maintenance functions ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;

    for (signature, _) in MAINTENANCE_FUNCTIONS {
        let public: bool = db
            .query_one(
                &format!("SELECT has_function_privilege('public', '{signature}', 'EXECUTE')"),
                &[],
            )
            .await
            .map_err(boxed)?
            .get(0);
        println!("Step 1: PUBLIC may execute {signature}: {public}");
        if public {
            return Err(format!("PUBLIC may execute {signature}").into());
        }
    }

    let role = format!("kt_unprivileged_{}", uuid::Uuid::new_v4().simple());
    db.batch_execute(&format!("CREATE ROLE {role} NOLOGIN"))
        .await
        .map_err(boxed)?;
    let result = calls_as(&db, &role).await;
    if let Err(e) = db.batch_execute(&format!("DROP ROLE {role}")).await {
        eprintln!("⚠️  cleanup: failed to drop role {role}: {e}");
    }
    result?;

    println!("Step 3: a superuser still runs both...");
    for (_, call) in MAINTENANCE_FUNCTIONS {
        db.query(call, &[]).await.map_err(boxed)?;
    }
    println!("✅ PUBLIC is refused both functions; a superuser runs them");
    Ok(())
}

/// Call both functions as `role` while another session of `role` holds the maintenance
/// advisory lock; each must be refused at once.
async fn calls_as(db: &Client, role: &str) -> SendResult<()> {
    let holder = session().await?;
    holder
        .batch_execute(&format!(
            "SET ROLE {role}; SELECT pg_advisory_lock({MAINTENANCE_LOCK_KEY})"
        ))
        .await
        .map_err(boxed)?;
    println!("Step 2: {role} holds the maintenance advisory lock in one session");
    let result = async {
        for (signature, call) in MAINTENANCE_FUNCTIONS {
            let caller = session().await?;
            caller
                .batch_execute(&format!(
                    "SET ROLE {role}; SET lock_timeout = 0; SET statement_timeout = '20s'"
                ))
                .await
                .map_err(boxed)?;
            let pid = backend_pid(&caller).await?;
            let call = call.to_string();
            let mut task = tokio::spawn(async move {
                caller
                    .query(&call, &[])
                    .await
                    .map(|rows| rows.len())
                    .map_err(|e| {
                        e.as_db_error()
                            .map_or(e.to_string(), |d| d.message().to_string())
                    })
            });
            let outcome = tokio::time::timeout(Duration::from_secs(3), &mut task).await;
            let outcome = if let Ok(joined) = outcome {
                joined.map_err(boxed)?
            } else {
                // Still running: say what it holds, then cancel it.
                let holds_extension: bool = db
                    .query_one(
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND granted
                                         AND locktype = 'object'
                                         AND classid = 'pg_extension'::regclass)",
                        &[&pid],
                    )
                    .await
                    .map_err(boxed)?
                    .get(0);
                db.query("SELECT pg_cancel_backend($1)", &[&pid])
                    .await
                    .map_err(boxed)?;
                let _ = task.await;
                return Err(format!(
                    "{signature} called by {role} was still running after 3 s (holding the \
                     extension lock: {holds_extension}): PUBLIC may execute it"
                )
                .into());
            };
            println!("  {signature} as {role}: {outcome:?}");
            match outcome {
                Err(msg) if msg.contains("permission denied for function") => {}
                other => {
                    return Err(format!(
                        "{signature} called by {role} must be refused with \"permission \
                         denied for function\", got {other:?}"
                    )
                    .into())
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    if let Err(e) = holder
        .batch_execute(&format!(
            "SELECT pg_advisory_unlock({MAINTENANCE_LOCK_KEY}); RESET ROLE"
        ))
        .await
    {
        eprintln!("⚠️  cleanup: failed to release the maintenance advisory lock: {e}");
    }
    result
}
