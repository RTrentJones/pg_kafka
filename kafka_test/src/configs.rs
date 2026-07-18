//! E2E tests for the config/log-management admin APIs:
//! DescribeConfigs (32), IncrementalAlterConfigs (44), DeleteRecords (21),
//! and the per-topic `retention.ms` enforcement by the retention sweep.
//!
//! rdkafka 0.36 wraps DescribeConfigs but not IncrementalAlterConfigs or
//! DeleteRecords, so those two are exercised via raw protocol encoding
//! (established pattern: idempotent/protocol_encoding.rs) with database
//! verification through ctx.db().

use crate::common::{create_producer, get_bootstrap_servers, TestResult};
use crate::setup::TestContext;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};
use kafka_protocol::messages::delete_records_response::DeleteRecordsResponse;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use kafka_protocol::messages::incremental_alter_configs_response::IncrementalAlterConfigsResponse;
use kafka_protocol::messages::{RequestHeader, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureRecord;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

const ERROR_NONE: i16 = 0;
const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERROR_INVALID_CONFIG: i16 = 40;
const OP_SET: i8 = 0;
const RESOURCE_TYPE_TOPIC: i8 = 2;

fn create_admin_client() -> Result<AdminClient<DefaultClientContext>, Box<dyn std::error::Error>> {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", get_bootstrap_servers())
        .set("broker.address.family", "v4")
        .create()?;
    Ok(admin)
}

/// Frame a header+body into a length-prefixed request.
fn frame(
    header: &RequestHeader,
    header_version: i16,
    encode_body: impl FnOnce(&mut BytesMut),
) -> Bytes {
    let mut body = BytesMut::new();
    header.encode(&mut body, header_version).unwrap();
    encode_body(&mut body);
    let mut framed = BytesMut::with_capacity(body.len() + 4);
    framed.put_i32(body.len() as i32);
    framed.put(body);
    framed.freeze()
}

async fn roundtrip(request: Bytes) -> Result<Bytes, String> {
    let mut stream = TcpStream::connect(get_bootstrap_servers())
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    stream
        .write_all(&request)
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .await
        .map_err(|e| format!("read size failed: {e}"))?;
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut payload = vec![0u8; size];
    stream
        .read_exact(&mut payload)
        .await
        .map_err(|e| format!("read frame failed: {e}"))?;
    Ok(Bytes::from(payload))
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

    let header = RequestHeader::default()
        .with_request_api_key(44)
        .with_request_api_version(0) // v0 = non-flexible (header v1)
        .with_correlation_id(7001)
        .with_client_id(Some(StrBytes::from_static_str("configs-test")));
    let framed = frame(&header, 1, |buf| request.encode(buf, 0).unwrap());

    let mut payload = roundtrip(framed).await?;
    let _correlation_id = payload.get_i32(); // response header v0
    let response = IncrementalAlterConfigsResponse::decode(&mut payload, 0)
        .map_err(|e| format!("decode alter response failed: {e}"))?;
    let resource = response
        .responses
        .first()
        .ok_or("empty alter response".to_string())?;
    Ok((
        resource.error_code,
        resource.error_message.as_ref().map(|m| m.to_string()),
    ))
}

/// Send a DeleteRecords v1 request for one partition and return
/// (error_code, low_watermark).
async fn delete_records(topic: &str, partition: i32, offset: i64) -> Result<(i16, i64), String> {
    let request = DeleteRecordsRequest::default()
        .with_timeout_ms(10_000)
        .with_topics(vec![DeleteRecordsTopic::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partitions(vec![DeleteRecordsPartition::default()
                .with_partition_index(partition)
                .with_offset(offset)])]);

    let header = RequestHeader::default()
        .with_request_api_key(21)
        .with_request_api_version(1) // v1 = non-flexible (header v1)
        .with_correlation_id(7002)
        .with_client_id(Some(StrBytes::from_static_str("configs-test")));
    let framed = frame(&header, 1, |buf| request.encode(buf, 1).unwrap());

    let mut payload = roundtrip(framed).await?;
    let _correlation_id = payload.get_i32(); // response header v0
    let response = DeleteRecordsResponse::decode(&mut payload, 1)
        .map_err(|e| format!("decode delete-records response failed: {e}"))?;
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
    assert_eq!(error_code, ERROR_NONE, "alter failed: {error_message:?}");
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
    assert_eq!(code, ERROR_NONE, "SET failed: {msg:?}");
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, Some(60000));
    println!("✅ Persisted to kafka.topics.retention_ms\n");

    println!("Step 2: DELETE the override...");
    let (code, msg) = alter_topic_config(&topic, "retention.ms", 1 /* DELETE */, None).await?;
    assert_eq!(code, ERROR_NONE, "DELETE failed: {msg:?}");
    assert_eq!(db_topic_retention(ctx.db(), &topic).await?, None);
    println!("✅ Override cleared\n");

    println!("Step 3: Unsupported key and bad value are rejected...");
    let (code, _) = alter_topic_config(&topic, "max.message.bytes", OP_SET, Some("1")).await?;
    assert_eq!(code, ERROR_INVALID_CONFIG);
    let (code, _) = alter_topic_config(&topic, "retention.ms", OP_SET, Some("abc")).await?;
    assert_eq!(code, ERROR_INVALID_CONFIG);
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
    assert_eq!(code, ERROR_NONE);
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
    assert_eq!(code, ERROR_OFFSET_OUT_OF_RANGE);
    let (code, _) = delete_records("no-such-topic-xyz", 0, 1).await?;
    assert_eq!(code, 3 /* UNKNOWN_TOPIC_OR_PARTITION */);
    println!("✅ Error paths correct\n");

    println!("Step 4: DeleteRecords(offset=-1) truncates to the high watermark...");
    let (code, low_watermark) = delete_records(&topic, 0, -1).await?;
    assert_eq!(code, ERROR_NONE);
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
    assert_eq!(code, ERROR_NONE, "alter failed: {msg:?}");
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

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
