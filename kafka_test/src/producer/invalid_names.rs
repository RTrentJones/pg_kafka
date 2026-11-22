//! Produce to a topic name Kafka would not accept.

use crate::common::TestResult;
use crate::error_codes;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::{conn, metadata_raw};
use crate::wire::Wire;
use bytes::Bytes;
use kafka_protocol::records::Compression;

/// Kafka 3.7 writes only to partitions that exist and answers a Produce to any other
/// UNKNOWN_TOPIC_OR_PARTITION, whatever the name: a valid batch to a name longer than
/// 249 characters, with a character Kafka refuses, or "..", is answered that way and
/// creates nothing. Metadata reports such a name INVALID_TOPIC_EXCEPTION, with or without
/// auto-creation.
pub async fn test_produce_to_an_invalid_topic_name_is_unknown_topic() -> TestResult {
    println!("=== Test: Produce to an invalid topic name ===\n");
    let ctx = TestContext::new().await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let long = format!("{suffix}{}", "l".repeat(250 - suffix.len()));
    let names = [long, format!("bad name!{suffix}"), "..".to_string()];
    let mut w = Wire::connect().await?;
    for name in &names {
        let value = plain_batch(&[Bytes::from_static(b"x")], Compression::None);
        let (code, base_offset) = w.produce(name, 0, value, None).await?;
        println!("  Produce to a {}-character name: {code}", name.len());
        assert_eq!(code, error_codes::UNKNOWN_TOPIC_OR_PARTITION, "{name}");
        assert_eq!(base_offset, -1);
    }
    let mut c = conn().await?;
    let long_name: &str = &names[0];
    let md = metadata_raw(&mut c, &[long_name]).await?;
    println!("  Metadata (no auto-create) of it: {}", md[0].error_code);
    assert_eq!(md[0].error_code, error_codes::INVALID_TOPIC_EXCEPTION);
    let mut req = kafka_protocol::messages::MetadataRequest::default();
    let mut t = kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
    t.name = Some(kafka_protocol::messages::TopicName(
        kafka_protocol::protocol::StrBytes::from_string(names[0].clone()),
    ));
    req.topics = Some(vec![t]);
    req.allow_auto_topic_creation = true;
    let resp = crate::wire::roundtrip(&mut c, 4, 3, &req).await?;
    println!(
        "  Metadata (auto-create) of it: {}",
        resp.topics[0].error_code
    );
    assert_eq!(
        resp.topics[0].error_code,
        error_codes::INVALID_TOPIC_EXCEPTION
    );
    for name in &names {
        let rows = ctx
            .db()
            .query("SELECT 1 FROM kafka.topics WHERE name = $1", &[name])
            .await?;
        assert!(rows.is_empty(), "{name} must not be created");
    }
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
