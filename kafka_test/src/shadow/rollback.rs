//! Sync-mode forwarding happens only after the local commit.
//!
//! A sync-mode forward runs after the commit (`forward_sync_after_commit`), and only
//! for rows whose writing subtransaction committed: forwarded from inside the
//! request's transaction, a request that then rolled back would leave the external
//! broker a copy of records pg_kafka never stored.

use super::external_client::{consume_from_external, verify_external_kafka_ready};
use super::helpers::{enable_shadow_mode, ShadowMode, ShadowTopicConfig, SyncMode, WriteMode};
use crate::common::broker_addr;
use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::records::{encode, plain_record};
use kafka_protocol::messages::produce_request::ProduceRequest;
use kafka_protocol::records::Compression;
use std::time::Duration;
use tokio::net::TcpStream;

/// One acks=1 produce carrying one record per listed partition, in order.
fn produce_request(topic: &str, partitions: &[(i32, &str)]) -> ProduceRequest {
    let parts = partitions
        .iter()
        .map(|&(index, value)| {
            let record = plain_record(0, Some(value), value.as_bytes());
            (index, encode(&[record], Compression::None))
        })
        .collect();
    crate::wire::requests::produce_request(topic, parts, 1)
}

async fn local_count(ctx: &TestContext, topic: &str) -> Result<i64, Box<dyn std::error::Error>> {
    Ok(ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0))
}

/// A sync-mode produce whose request rolls back must leave no copy on the
/// external broker.
///
/// One raw Produce writes partition 0 (fine) and then partition 1, where a test
/// trigger on `kafka.messages` raises an ERROR. The ERROR rolls the whole
/// request back, including partition 0, and nothing reaches the external broker. A second, clean produce to partition 0
/// checks that sync forwarding still delivers after commit.
pub async fn test_sync_forward_skips_rolled_back_request() -> TestResult {
    println!("=== Test: Sync Forward Skips A Rolled-Back Request ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("shadow-sync-rollback").await;

    verify_external_kafka_ready().await?;

    // Two partitions; created before enable_shadow_mode, which reuses it.
    let topic_id: i32 = ctx
        .db()
        .query_one(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2) RETURNING id",
            &[&topic],
        )
        .await?
        .get(0);
    enable_shadow_mode(
        ctx.db(),
        &topic,
        &ShadowTopicConfig {
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: SyncMode::Sync,
            write_mode: WriteMode::DualWrite,
        },
    )
    .await?;

    // Inject a Postgres ERROR on any insert into partition 1 of this topic.
    let function = format!("kafka.test_fail_partition_one_{topic_id}");
    ctx.db()
        .batch_execute(&format!(
            "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RAISE EXCEPTION 'injected failure for the rollback test'; END $$; \
             CREATE TRIGGER test_fail_partition_one_{topic_id} BEFORE INSERT ON kafka.messages \
             FOR EACH ROW WHEN (NEW.topic_id = {topic_id} AND NEW.partition_id = 1) \
             EXECUTE FUNCTION {function}();"
        ))
        .await?;

    let outcome = async {
        let mut stream = TcpStream::connect(broker_addr()).await?;
        let failed = crate::wire::roundtrip(
            &mut stream,
            7,
            701,
            &produce_request(&topic, &[(0, "rolled-back"), (1, "poison")]),
        )
        .await?;
        // A Postgres ERROR in the handler is answered UNKNOWN_SERVER_ERROR, today
        // as a response with no partitions; either way no partition may succeed.
        let codes: Vec<i16> = failed
            .responses
            .iter()
            .flat_map(|t| t.partition_responses.iter().map(|p| p.error_code))
            .collect();
        let stored_after_failure = local_count(&ctx, &topic).await?;

        let ok = crate::wire::roundtrip(
            &mut stream,
            7,
            702,
            &produce_request(&topic, &[(0, "committed")]),
        )
        .await?;
        let ok_code = ok.responses[0].partition_responses[0].error_code;
        Ok::<_, Box<dyn std::error::Error>>((codes, stored_after_failure, ok_code))
    }
    .await
    .map_err(|e| e.to_string());

    // Drop the trigger before asserting so a failure cannot leave it behind.
    //
    // kafka.messages is locked first: DROP TRIGGER on a partitioned table takes only
    // ACCESS SHARE on the parent, then ACCESS EXCLUSIVE on each leaf's clone of the
    // trigger, newest leaf first, while every worker statement reading through the
    // parent locks the leaves oldest first. Racing such a statement (the outbox
    // claim, Fetch, an offset probe) it deadlocks: the DROP holds a newer leaf and
    // waits for an older one the worker holds, and the worker waits for the newer
    // one. Holding the parent ACCESS EXCLUSIVE first, the DROP waits for the worker's
    // transactions to end and then takes the leaves unopposed.
    ctx.db()
        .batch_execute(&format!(
            "BEGIN; \
             LOCK TABLE kafka.messages IN ACCESS EXCLUSIVE MODE; \
             DROP TRIGGER IF EXISTS test_fail_partition_one_{topic_id} ON kafka.messages; \
             DROP FUNCTION IF EXISTS {function}(); \
             COMMIT;"
        ))
        .await?;
    let (codes, stored_after_failure, ok_code) = outcome?;

    println!("   failed produce: partition codes {codes:?}, {stored_after_failure} local row(s)");
    assert!(
        !codes.contains(&0),
        "no partition of the poisoned request may report success: {codes:?}"
    );
    assert_eq!(
        stored_after_failure, 0,
        "the rolled-back request must store nothing locally"
    );
    assert_eq!(ok_code, 0, "the clean produce must succeed");

    // Give forwarding (sync, plus the async poll as a fallback) time to deliver.
    let external = consume_from_external(&topic, 10, Duration::from_secs(10)).await?;
    let values: Vec<String> = external
        .iter()
        .map(|m| String::from_utf8_lossy(m.value.as_deref().unwrap_or_default()).to_string())
        .collect();
    println!("   external values: {values:?}");
    assert!(
        !values.iter().any(|v| v == "rolled-back" || v == "poison"),
        "a rolled-back record reached the external broker: {values:?}"
    );
    assert_eq!(
        values,
        vec!["committed".to_string()],
        "exactly the committed record must be forwarded"
    );

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
