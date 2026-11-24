//! E2E tests for the config/log-management admin APIs:
//! DescribeConfigs (32), IncrementalAlterConfigs (44), DeleteRecords (21),
//! and the per-topic `retention.ms` enforcement by the retention sweep.
//!
//! rdkafka 0.36 wraps DescribeConfigs but not IncrementalAlterConfigs or
//! DeleteRecords, so those two are exercised via raw protocol encoding
//! (established pattern: idempotent/protocol_encoding.rs) with database
//! verification through ctx.db().

use crate::common::create_admin_client;
use crate::common::{create_base_consumer, create_producer, get_bootstrap_servers, TestResult};
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::roundtrip;
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::Request;
use kafka_protocol::protocol::StrBytes;
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;

async fn db_topic_retention(
    db: &tokio_postgres::Client,
    topic: &str,
) -> Result<Option<i64>, Box<dyn std::error::Error>> {
    let row = db
        .query_one(
            "SELECT retention_ms FROM kafka.topics WHERE name = $1",
            &[&topic],
        )
        .await?;
    Ok(row.get(0))
}

async fn db_message_count(
    db: &tokio_postgres::Client,
    topic: &str,
) -> Result<i64, Box<dyn std::error::Error>> {
    let count: i64 = db
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    Ok(count)
}

const OP_SET: i8 = 0;
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// One request on a fresh connection.
async fn call<R: Request>(
    version: i16,
    correlation_id: i32,
    request: &R,
) -> Result<R::Response, String> {
    let mut stream = TcpStream::connect(get_bootstrap_servers())
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    roundtrip(&mut stream, version, correlation_id, request)
        .await
        .map_err(|e| format!("{} v{version} failed: {e}", std::any::type_name::<R>()))
}

/// Send an IncrementalAlterConfigs v0 SET/DELETE for one topic config and
/// return the per-resource (error_code, error_message).
async fn alter_topic_config(
    topic: &str,
    config_name: &str,
    op: i8,
    value: Option<&str>,
) -> Result<(i16, Option<String>), String> {
    let request = IncrementalAlterConfigsRequest::default()
        .with_validate_only(false)
        .with_resources(vec![AlterConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.to_string()))
            .with_configs(vec![AlterableConfig::default()
                .with_name(StrBytes::from_string(config_name.to_string()))
                .with_config_operation(op)
                .with_value(value.map(|v| StrBytes::from_string(v.to_string())))])]);

    // v0 = non-flexible.
    let response = call(0, 7001, &request).await?;
    let resource = response
        .responses
        .first()
        .ok_or("empty alter response".to_string())?;
    Ok((
        resource.error_code,
        resource.error_message.as_ref().map(ToString::to_string),
    ))
}

/// Send a DeleteRecords v1 request for one partition and return
/// (error_code, low_watermark).
pub(crate) async fn delete_records(
    topic: &str,
    partition: i32,
    offset: i64,
) -> Result<(i16, i64), String> {
    let request = DeleteRecordsRequest::default()
        .with_timeout_ms(10_000)
        .with_topics(vec![DeleteRecordsTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![DeleteRecordsPartition::default()
                .with_partition_index(partition)
                .with_offset(offset)])]);

    // v1 = non-flexible.
    let response = call(1, 7002, &request).await?;
    let partition_result = response
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .ok_or("empty delete-records response".to_string())?;
    Ok((partition_result.error_code, partition_result.low_watermark))
}

/// DescribeConfigs via rdkafka AdminClient: a fresh topic reports the honest
/// config set (retention.ms from the global GUC, cleanup.policy=delete), and
/// a retention.ms override set via IncrementalAlterConfigs is reflected.
pub async fn test_describe_configs_reports_topic_configs() -> TestResult {
    println!("=== Test: DescribeConfigs Reports Topic Configs (API 32) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("describe-configs").await;

    println!("Step 1: Creating topic via produce (auto-create)...");
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).key("k").payload("v"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;
    println!("✅ Topic exists\n");

    println!("Step 2: DescribeConfigs on the fresh topic...");
    let admin = create_admin_client()?;
    let opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));
    let results = admin
        .describe_configs(&[ResourceSpecifier::Topic(&topic)], &opts)
        .await?;
    assert_eq!(results.len(), 1);
    let resource = results[0].as_ref().map_err(|e| format!("{e:?}"))?;
    let retention = resource
        .entries
        .iter()
        .find(|e| e.name == "retention.ms")
        .expect("retention.ms reported");
    // Global sweep is off in the test cluster → infinite (-1), default source.
    assert_eq!(retention.value.as_deref(), Some("-1"));
    let cleanup = resource
        .entries
        .iter()
        .find(|e| e.name == "cleanup.policy")
        .expect("cleanup.policy reported");
    assert_eq!(cleanup.value.as_deref(), Some("delete"));
    println!("✅ Fresh topic: retention.ms=-1 (default), cleanup.policy=delete\n");

    println!("Step 3: Setting retention.ms=86400000 and re-describing...");
    let (error_code, error_message) =
        alter_topic_config(&topic, "retention.ms", OP_SET, Some("86400000")).await?;
    assert_eq!(
        error_code,
        error_codes::NONE,
        "alter failed: {error_message:?}"
    );
    let results = admin
        .describe_configs(&[ResourceSpecifier::Topic(&topic)], &opts)
        .await?;
    let resource = results[0].as_ref().map_err(|e| format!("{e:?}"))?;
    let retention = resource
        .entries
        .iter()
        .find(|e| e.name == "retention.ms")
        .expect("retention.ms reported");
    assert_eq!(retention.value.as_deref(), Some("86400000"));
    println!("✅ Override reflected in DescribeConfigs\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// IncrementalAlterConfigs: SET persists to kafka.topics.retention_ms,
/// DELETE clears it, unsupported keys are rejected with INVALID_CONFIG and
/// nothing is written.
pub async fn test_incremental_alter_configs_retention_roundtrip() -> TestResult {
    println!("=== Test: IncrementalAlterConfigs retention.ms Roundtrip (API 44) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("alter-configs").await;
    let producer = create_producer()?;
    producer
        .send(
            FutureRecord::to(&topic).key("k").payload("v"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    println!("Step 1: SET retention.ms=60000...");
    let (code, msg) = alter_topic_config(&topic, "retention.ms", OP_SET, Some("60000")).await?;
    assert_eq!(code, error_codes::NONE, "SET failed: {msg:?}");
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, Some(60000));
    println!("✅ Persisted to kafka.topics.retention_ms\n");

    println!("Step 2: DELETE the override...");
    let (code, msg) = alter_topic_config(&topic, "retention.ms", 1 /* DELETE */, None).await?;
    assert_eq!(code, error_codes::NONE, "DELETE failed: {msg:?}");
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, None);
    println!("✅ Override cleared\n");

    println!("Step 3: Unsupported key and bad value are rejected...");
    let (code, _) = alter_topic_config(&topic, "max.message.bytes", OP_SET, Some("1")).await?;
    assert_eq!(code, error_codes::INVALID_CONFIG);
    let (code, _) = alter_topic_config(&topic, "retention.ms", OP_SET, Some("abc")).await?;
    assert_eq!(code, error_codes::INVALID_CONFIG);
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, None);
    println!("✅ Rejected with INVALID_CONFIG, nothing written\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// DeleteRecords: rows below the offset are deleted, the low watermark
/// advances, later records survive, and out-of-range offsets error.
pub async fn test_delete_records_truncates_partition() -> TestResult {
    println!("=== Test: DeleteRecords Truncates Partition (API 21) ===\n");

    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("delete-records").await;
    let producer = create_producer()?;

    println!("Step 1: Producing 5 records...");
    for i in 0..5 {
        producer
            .send(
                FutureRecord::to(&topic)
                    .key("k")
                    .payload(format!("v{i}").as_str())
                    .partition(0),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    println!("✅ 5 records at offsets 0..4\n");

    println!("Step 2: DeleteRecords(offset=3)...");
    let (code, low_watermark) = delete_records(&topic, 0, 3).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!(low_watermark, 3, "low watermark must advance to 3");

    let row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*)::bigint AS cnt, MIN(partition_offset) AS min_off
             FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let count: i64 = row.get("cnt");
    let min_off: Option<i64> = row.get("min_off");
    assert_eq!(count, 2, "offsets 3 and 4 must survive");
    assert_eq!(min_off, Some(3));
    println!("✅ Rows below offset 3 deleted, 3..4 survive\n");

    println!("Step 3: Out-of-range offset errors, unknown topic errors...");
    let (code, _) = delete_records(&topic, 0, 99).await?;
    assert_eq!(code, error_codes::OFFSET_OUT_OF_RANGE);
    let (code, _) = delete_records("no-such-topic-xyz", 0, 1).await?;
    assert_eq!(code, 3 /* UNKNOWN_TOPIC_OR_PARTITION */);
    println!("✅ Error paths correct\n");

    println!("Step 4: DeleteRecords(offset=-1) truncates to the high watermark...");
    let (code, low_watermark) = delete_records(&topic, 0, -1).await?;
    assert_eq!(code, error_codes::NONE);
    assert_eq!(low_watermark, 5);
    let count: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON m.topic_id = t.id WHERE t.name = $1",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(count, 0, "partition fully truncated");
    println!("✅ Full truncation\n");

    // The persisted log start must NOT regress to 0 on the now-empty partition.
    // Primary proof: the durable column. Guards the Codex-flagged
    // log-start-regression on the DeleteRecords path.
    println!("Step 5: log start must persist at 5 (not regress to 0)...");
    let stored_log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT po.log_start_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON po.topic_id = t.id
             WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic],
        )
        .await?
        .get(0);
    assert_eq!(stored_log_start, 5, "durable log_start_offset must be 5");

    // Client-visible proof: ListOffsets(EARLIEST) — via fetch_watermarks' low
    // watermark — must report 5, not 0. rdkafka's BaseConsumer is !Send, so run
    // it on a blocking thread to keep the test future Send.
    let topic_for_wm = topic.clone();
    let low_wm: i64 = tokio::task::spawn_blocking(move || -> Result<i64, String> {
        let consumer =
            create_base_consumer("delete-records-lso").map_err(|e| format!("consumer: {e}"))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok((low, _high)) =
                consumer.fetch_watermarks(&topic_for_wm, 0, Duration::from_secs(5))
            {
                if low == 5 || Instant::now() > deadline {
                    return Ok(low);
                }
            } else if Instant::now() > deadline {
                return Err("fetch_watermarks never succeeded".to_string());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    })
    .await
    .map_err(|e| format!("join error: {e}"))??;
    assert_eq!(
        low_wm, 5,
        "EARLIEST offset regressed after full truncation (expected 5, got {low_wm})"
    );
    println!("✅ Log start persisted at 5; EARLIEST reports 5, no regression\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// QA-1 for the sweep change: a per-topic retention.ms override must be
/// enforced by the retention sweep even when the global GUC is off (0), and
/// a sibling topic without the override must keep its rows.
pub async fn test_per_topic_retention_override_enforced_by_sweep() -> TestResult {
    println!("=== Test: Per-Topic retention.ms Enforced by Sweep ===\n");

    let ctx = TestContext::new().await?;
    let topic_with = ctx.unique_topic("retention-override").await;
    let topic_without = ctx.unique_topic("retention-inherit").await;
    let producer = create_producer()?;

    println!("Step 1: Producing one aged record to each topic...");
    for topic in [&topic_with, &topic_without] {
        producer
            .send(
                FutureRecord::to(topic).key("k").payload("old"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _)| e)?;
    }
    // Age both rows one hour into the past.
    ctx.db()
        .execute(
            "UPDATE kafka.messages SET created_at = NOW() - interval '1 hour'
             WHERE topic_id IN (SELECT id FROM kafka.topics WHERE name = ANY($1))",
            &[&vec![topic_with.clone(), topic_without.clone()]],
        )
        .await?;
    println!("✅ Rows aged 1 hour\n");

    println!("Step 2: Setting retention.ms=60000 on ONE topic (via API 44)...");
    let (code, msg) =
        alter_topic_config(&topic_with, "retention.ms", OP_SET, Some("60000")).await?;
    assert_eq!(code, error_codes::NONE, "alter failed: {msg:?}");
    println!("✅ Override set\n");

    println!("Step 3: Running the sweep with the GLOBAL retention disabled (0)...");
    ctx.db()
        .query("SELECT * FROM pg_kafka_run_retention_sweep(0, 3600)", &[])
        .await?;

    assert_eq!(
        db_message_count(ctx.db(), &topic_with).await?,
        0,
        "override topic's aged row must be swept despite global retention being off"
    );
    assert_eq!(
        db_message_count(ctx.db(), &topic_without).await?,
        1,
        "topic without override must keep its row (global retention off)"
    );
    println!("✅ Override enforced; inheriting topic untouched\n");

    // The sweep must advance the durable log start of the emptied partition to
    // the HWM (next_offset = 1 after one produce), so EARLIEST does not regress
    // to 0 (Codex review, PR #95). The untouched sibling keeps log_start = 0.
    println!("Step 4: Emptied partition's log start must advance to the HWM (1)...");
    let swept_log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT po.log_start_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON po.topic_id = t.id
             WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic_with],
        )
        .await?
        .get(0);
    assert_eq!(
        swept_log_start, 1,
        "retention-emptied partition must advance log_start to the HWM, not leave it at 0"
    );
    let untouched_log_start: i64 = ctx
        .db()
        .query_one(
            "SELECT po.log_start_offset FROM kafka.partition_offsets po
             JOIN kafka.topics t ON po.topic_id = t.id
             WHERE t.name = $1 AND po.partition_id = 0",
            &[&topic_without],
        )
        .await?
        .get(0);
    assert_eq!(
        untouched_log_start, 0,
        "partition the sweep did not touch must keep log_start = 0"
    );
    println!("✅ Log start advanced on the swept partition; sibling unchanged\n");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// Kafka 3.7 answers an IncrementalAlterConfigs SET without a value INVALID_REQUEST (a
/// malformed request), whatever the key, where pg_kafka answered INVALID_CONFIG; and its
/// DescribeConfigs reports cleanup.policy as a LIST config (type 7), not STRING (2).
pub async fn test_config_api_answers_match_kafka() -> TestResult {
    use kafka_protocol::messages::describe_configs_request::{
        DescribeConfigsRequest, DescribeConfigsResource,
    };
    println!("=== Test: IncrementalAlterConfigs null SET, DescribeConfigs config types ===\n");
    let ctx = TestContext::new().await?;
    let topic = crate::fixtures::topic_with_partitions(&ctx, "configs-parity", 1).await?;
    for (key, op) in [("retention.ms", OP_SET), ("max.message.bytes", OP_SET)] {
        let (code, _) = alter_topic_config(&topic, key, op, None).await?;
        assert_eq!(code, error_codes::INVALID_REQUEST, "SET {key} = null");
    }
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, None);
    let request =
        DescribeConfigsRequest::default().with_resources(vec![DescribeConfigsResource::default()
            .with_resource_type(RESOURCE_TYPE_TOPIC)
            .with_resource_name(StrBytes::from_string(topic.clone()))
            .with_configuration_keys(None)]);
    let response = call(4, 7003, &request).await?;
    let types: Vec<(String, i8)> = response.results[0]
        .configs
        .iter()
        .map(|c| (c.name.to_string(), c.config_type))
        .collect();
    assert!(
        types.contains(&("cleanup.policy".to_string(), 7)),
        "{types:?}"
    );
    assert!(
        types.contains(&("retention.ms".to_string(), 5)),
        "{types:?}"
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
