//! Expected failure tests
//!
//! Tests that verify proper error handling in failure scenarios.

use crate::common::TestResult;
use crate::fixtures::TestTopicBuilder;
use crate::setup::TestContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::FutureProducer;
use std::time::Duration;

/// SEC-8: the external-Kafka SASL password GUC must be readable only by superusers, so an
/// ordinary DB role cannot read the cleartext credential via `SHOW`. The fix locks it down with
/// `SUPERUSER_ONLY | NO_SHOW_ALL` (mirroring the license-key GUC).
pub async fn test_sasl_password_guc_is_superuser_only() -> TestResult {
    println!("=== Test: shadow_sasl_password GUC is superuser-only (SEC-8) ===\n");
    let ctx = TestContext::new().await?;
    let db = ctx.db();

    // Create a throwaway non-superuser role (idempotent across re-runs). The name must NOT start
    // with `pg_` — PostgreSQL reserves that prefix and rejects CREATE ROLE for it.
    db.batch_execute(
        "DO $$ BEGIN
           IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sec8_unpriv_probe') THEN
             CREATE ROLE sec8_unpriv_probe NOSUPERUSER;
           END IF;
         END $$;",
    )
    .await?;

    // Sanity: as the (superuser) test role, reading the GUC succeeds. Use simple_query — SHOW/SET
    // are utility statements that the extended (prepared) protocol can reject.
    db.simple_query("SHOW pg_kafka.shadow_sasl_password").await?;

    // As the non-superuser role, reading it must be rejected (SUPERUSER_ONLY).
    db.simple_query("SET ROLE sec8_unpriv_probe").await?;
    let unpriv_read = db.simple_query("SHOW pg_kafka.shadow_sasl_password").await;
    db.simple_query("RESET ROLE").await?;

    if unpriv_read.is_ok() {
        return Err(
            "SEC-8 regression: a non-superuser read pg_kafka.shadow_sasl_password"
                .to_string()
                .into(),
        );
    }
    println!("✅ a non-superuser cannot read the SASL password GUC");
    Ok(())
}

/// Test connection to wrong port fails gracefully
pub async fn test_connection_refused() -> TestResult {
    println!("=== Test: Connection Refused ===\n");

    // Try to connect to a port that's not running kafka
    let producer_result: Result<FutureProducer, _> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:19999") // Non-existent port
        .set("message.timeout.ms", "1000")
        .set("socket.timeout.ms", "1000")
        .create();

    match producer_result {
        Ok(producer) => {
            // Producer created, but should fail on actual operation
            let result = producer
                .send(
                    rdkafka::producer::FutureRecord::to("test-topic")
                        .payload("test")
                        .key("key"),
                    Duration::from_secs(2),
                )
                .await;

            match result {
                Err((kafka_error, _)) => {
                    println!("   Expected failure occurred: {}", kafka_error);
                }
                Ok(_) => {
                    // QA-3: producing to a broker that isn't listening (localhost:19999) must NOT
                    // succeed.
                    return Err("produce to a non-existent broker unexpectedly succeeded".into());
                }
            }
        }
        Err(e) => {
            // Failing to even build the client is also an acceptable "refused" outcome.
            println!("   Producer creation failed (expected): {}", e);
        }
    }

    println!("   Connection refused handled gracefully");
    Ok(())
}

/// Test produce with very short timeout
pub async fn test_produce_timeout() -> TestResult {
    println!("=== Test: Produce Timeout ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "timeout-test").build().await?;

    // Create producer with very short timeout
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("message.timeout.ms", "1") // 1ms timeout - extremely short
        .create()?;

    let mut timeout_count = 0;
    let mut success_count = 0;

    for i in 0..5 {
        let result = producer
            .send(
                rdkafka::producer::FutureRecord::to(&topic.name)
                    .payload(&format!("message-{}", i))
                    .key(&format!("key-{}", i)),
                Duration::from_millis(1),
            )
            .await;

        match result {
            Err((e, _)) => {
                timeout_count += 1;
                if i == 0 {
                    println!("   Timeout error (expected): {}", e);
                }
            }
            Ok(_) => {
                success_count += 1;
            }
        }
    }

    println!(
        "   Timeouts: {}, Successes: {}",
        timeout_count, success_count
    );
    // QA-3: a 1ms message.timeout.ms is far below the time a real produce (network + SPI insert)
    // takes, so at least one of five sends must trip the local delivery timeout. (Returning Err
    // rather than asserting keeps a failure isolated to this test instead of unwinding the suite.)
    ctx.cleanup().await?;
    if timeout_count == 0 {
        return Err("a 1ms produce timeout should have fired at least once".into());
    }
    Ok(())
}

/// Test invalid (empty) group ID
pub async fn test_invalid_group_id() -> TestResult {
    println!("=== Test: Invalid Group ID ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "invalid-group").build().await?;

    // Seed one record so the recv() branch below is meaningful: a *working* consumer would deliver
    // it, so "no delivery" genuinely reflects the empty group.id being rejected rather than an
    // empty topic.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("message.timeout.ms", "5000")
        .create()?;
    producer
        .send(
            rdkafka::producer::FutureRecord::to(&topic.name)
                .payload("seed")
                .key("seed"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _)| e)?;

    // Try to create consumer with empty group ID
    let consumer_result: Result<StreamConsumer, _> = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", "") // Empty group ID - invalid
        .set("auto.offset.reset", "earliest")
        .create();

    // QA-3: an empty group.id is invalid — it must never end up consuming a message. We don't pin
    // *where* it's rejected (librdkafka may reject at create, at subscribe, or just never deliver),
    // only that the invalid configuration cannot silently behave like a valid consumer.
    let consumed_with_empty_group = match consumer_result {
        Err(e) => {
            println!("   Consumer creation rejected empty group.id (expected): {}", e);
            false
        }
        Ok(consumer) => match consumer.subscribe(&[&topic.name]) {
            Err(e) => {
                println!("   Subscribe rejected empty group.id (expected): {}", e);
                false
            }
            Ok(_) => match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                Ok(Ok(_)) => true, // a real delivery — that's the regression we guard against
                Ok(Err(e)) => {
                    println!("   Recv error (expected): {}", e);
                    false
                }
                Err(_) => {
                    println!("   Timeout, no delivery (acceptable for empty group)");
                    false
                }
            },
        },
    };

    ctx.cleanup().await?;
    if consumed_with_empty_group {
        return Err("a consumer with an empty group.id must not consume".into());
    }
    Ok(())
}

/// Test duplicate consumer join (same member ID)
pub async fn test_duplicate_consumer_join() -> TestResult {
    println!("=== Test: Duplicate Consumer Join ===\n");

    let ctx = TestContext::new().await?;
    let topic = TestTopicBuilder::new(&ctx, "duplicate-join")
        .build()
        .await?;

    let group_id = ctx.unique_group("duplicate-group").await;

    // Create first consumer
    let consumer1: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;

    consumer1.subscribe(&[&topic.name])?;
    println!("   Consumer 1 joined group: {}", group_id);

    // Let first consumer establish itself
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Create second consumer in same group (should cause rebalance, not error)
    let consumer2: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", crate::common::get_bootstrap_servers())
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "6000")
        .create()?;

    consumer2.subscribe(&[&topic.name])?;
    println!("   Consumer 2 joined same group");

    // Both consumers should be able to operate (triggering rebalance)
    // With a single-partition topic, only one will actually receive messages
    tokio::time::sleep(Duration::from_millis(500)).await;

    // QA-3: a second join into a live group must be handled as a rebalance, not an error — both
    // members stay subscribed to the topic. (subscribe() above already `?`-propagates failure; this
    // asserts the subscription actually took on both members rather than silently being dropped.)
    let sub1 = consumer1.subscription()?;
    let sub2 = consumer2.subscription()?;
    let topic_name = topic.name.as_str();
    let has_topic = |sub: &rdkafka::TopicPartitionList| {
        sub.elements().iter().any(|e| e.topic() == topic_name)
    };
    let (ok1, ok2) = (has_topic(&sub1), has_topic(&sub2));
    println!("   subscriptions after rejoin: consumer1={ok1}, consumer2={ok2}");

    ctx.cleanup().await?;
    if !ok1 || !ok2 {
        return Err("both members must stay subscribed after the rejoin".into());
    }
    Ok(())
}
