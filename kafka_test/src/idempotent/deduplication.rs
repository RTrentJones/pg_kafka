//! TRUE Deduplication Testing with Manual Protocol Bytes
//!
//! This module tests idempotent producer deduplication by manually crafting
//! identical Kafka protocol requests and sending them multiple times.
//!
//! Unlike smart clients (rdkafka) that hide retry logic, this "dumb client"
//! approach gives us deterministic control over duplicate transmissions.

use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::requests::{init_producer_id_request, sequenced_produce_request};
use crate::wire::{read_response, request_frame, roundtrip};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const PRODUCE_VERSION: i16 = 7;

/// The error code of the first partition of a Produce response (0 when it has none).
fn first_partition_error(response: &ProduceResponse) -> i16 {
    response
        .responses
        .first()
        .and_then(|t| t.partition_responses.first())
        .map_or(0, |p| p.error_code)
}

/// Test TRUE deduplication with manual protocol replay
///
/// This test:
/// 1. Connects via raw TCP to the broker
/// 2. Sends InitProducerId to get a producer_id
/// 3. Manually crafts a ProduceRequest with producer metadata
/// 4. Sends the EXACT SAME request twice (duplicate)
/// 5. Verifies both return success (error_code=0)
/// 6. Verifies database has exactly 1 message (not 2)
///
/// This proves the broker correctly deduplicates retry attempts.
pub async fn test_true_deduplication_manual_replay() -> TestResult {
    println!("=== Test: TRUE Deduplication with Manual Protocol Replay ===\n");

    // Setup test context for isolation
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("dedup-manual").await;

    // Step 1: Connect to broker via raw TCP
    println!("Step 1: Connecting to broker at 127.0.0.1:9092...");
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("✅ Connected to broker\n");

    // Step 2: Send InitProducerId request to get a producer_id
    println!("Step 2: Sending InitProducerId request...");
    let init = roundtrip(&mut stream, 4, 1, &init_producer_id_request(None, 60_000)).await?;
    if init.error_code != 0 {
        return Err(format!("InitProducerId failed: error_code={}", init.error_code).into());
    }
    let producer_id = init.producer_id.0;
    println!("✅ InitProducerId returned producer_id={producer_id}\n");

    // Step 3: Craft ProduceRequest with RecordBatch containing producer metadata
    println!("Step 3: Encoding ProduceRequest with producer metadata...");
    let partition = 0;
    let producer_epoch = 0i16;
    let base_sequence = 0i32; // First message for this producer

    let produce_request = request_frame(
        PRODUCE_VERSION,
        2,
        &sequenced_produce_request(
            &topic,
            partition,
            producer_id,
            producer_epoch,
            base_sequence,
            &["test-value-1"], // One record
        ),
    );

    println!(
        "✅ ProduceRequest encoded ({} bytes)\n",
        produce_request.len()
    );

    // Step 4: Send EXACT SAME request TWICE
    println!("=== CRITICAL TEST: Sending IDENTICAL request twice ===\n");

    println!("Step 4a: Sending request #1...");
    stream.write_all(&produce_request).await?;
    let (_, response1) = read_response::<ProduceRequest>(&mut stream, PRODUCE_VERSION).await?;
    let error_code1 = first_partition_error(&response1);
    println!("✅ Response #1: error_code={error_code1}\n");

    println!("Step 4b: Sending request #2 (IDENTICAL BYTES)...");
    stream.write_all(&produce_request).await?; // SAME BYTES!
    let (_, response2) = read_response::<ProduceRequest>(&mut stream, PRODUCE_VERSION).await?;
    let error_code2 = first_partition_error(&response2);
    println!("✅ Response #2: error_code={error_code2}\n");

    // Step 5: Verify both responses are SUCCESS (error_code=0)
    println!("=== Verifying Protocol Responses ===\n");
    assert_eq!(
        error_code1, 0,
        "First request should succeed, got error_code={error_code1}"
    );
    println!("✅ First request succeeded (error_code=0)");

    assert_eq!(
        error_code2, 0,
        "Duplicate request should also return SUCCESS (error_code=0), got error_code={error_code2}. \
         This is correct Kafka behavior - duplicates are ACKed, not rejected!"
    );
    println!("✅ Duplicate request succeeded (error_code=0) - Correct behavior!\n");

    // Step 6: THE TRUTH - Query database to verify deduplication
    println!("=== Verifying Database State ===\n");
    let count_row = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m
             JOIN kafka.topics t ON m.topic_id = t.id
             WHERE t.name = $1",
            &[&topic],
        )
        .await?;
    let message_count: i64 = count_row.get(0);

    println!("Messages in database: {message_count}");

    // This is the CRITICAL assertion
    assert_eq!(
        message_count, 1,
        "DEDUPLICATION FAILED! Expected 1 message, found {message_count}. \
         The broker should have detected the duplicate and skipped the second insert."
    );

    println!("✅ Database has exactly 1 message (not 2)\n");

    // Step 7: Verify producer metadata was stored correctly
    println!("=== Verifying Producer Metadata ===\n");
    let producer_row = ctx
        .db()
        .query_one(
            "SELECT producer_id, epoch FROM kafka.producer_ids WHERE producer_id = $1",
            &[&producer_id],
        )
        .await?;
    let stored_producer_id: i64 = producer_row.get(0);
    let stored_epoch: i16 = producer_row.get(1);
    println!("✅ Producer metadata: producer_id={stored_producer_id}, epoch={stored_epoch}");

    let seq_row = ctx
        .db()
        .query_one(
            "SELECT last_sequence FROM kafka.producer_sequences
             WHERE producer_id = $1 AND partition_id = $2",
            &[&producer_id, &partition],
        )
        .await?;
    let last_sequence: i32 = seq_row.get(0);
    println!("✅ Last sequence: {last_sequence}\n");

    assert_eq!(
        last_sequence, 0,
        "Last sequence should be 0 (base_sequence + record_count - 1 = 0 + 1 - 1 = 0)"
    );

    // Cleanup
    ctx.cleanup().await?;

    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║          TRUE DEDUPLICATION TEST PASSED ✅                 ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Summary:");
    println!("  ✅ InitProducerId allocated producer_id={producer_id}");
    println!("  ✅ First ProduceRequest succeeded (error_code=0)");
    println!("  ✅ Duplicate ProduceRequest succeeded (error_code=0) - Correct!");
    println!("  ✅ Database has 1 message (not 2) - Deduplication works!");
    println!("  ✅ Producer metadata stored correctly");
    println!();
    println!("This test proves that pg_kafka correctly implements");
    println!("Kafka idempotent producer semantics:");
    println!("  - Duplicates return SUCCESS (not error)");
    println!("  - Duplicates are silently discarded (not re-inserted)");
    println!("  - Sequence tracking prevents message duplication");
    println!();

    Ok(())
}
