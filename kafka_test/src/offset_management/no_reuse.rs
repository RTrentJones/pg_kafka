//! Offset-monotonicity regression test (audit BUG-3)
//!
//! After cleanup_aborted_messages deletes the highest (aborted) rows of a partition, the next
//! produce must NOT reuse those offsets. This proves the per-partition monotonic offset counter
//! (kafka.partition_offsets) keeps offsets strictly increasing for the life of the partition.

use crate::common::{create_db_client, create_producer, TestResult};
use rdkafka::producer::FutureRecord;
use std::time::Duration;

/// Produce three messages, delete the highest row directly (simulating cleanup of an aborted
/// tail, which lowers MAX(partition_offset)), then produce again and assert the new offset is
/// `last + 1` — not the reused `last` that a plain MAX+1 assignment would yield.
pub async fn test_offset_no_reuse_after_cleanup() -> TestResult {
    println!("=== Test: No offset reuse after aborted-tail cleanup (BUG-3) ===\n");

    let topic = format!("offset-no-reuse-{}", uuid::Uuid::new_v4());
    let producer = create_producer()?;

    // Produce three messages to a fresh topic -> consecutive offsets ending at `last`.
    let mut last_offset: Option<(i32, i64)> = None;
    for i in 0..3 {
        let payload = format!("m{}", i);
        let (p, off) = producer
            .send(
                FutureRecord::to(&topic).payload(&payload).key("k"),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _msg)| e)?;
        last_offset = Some((p, off));
    }
    let (partition, last) = last_offset.expect("produced at least one message");
    println!("Produced 3 messages, last offset = {}", last);

    // Simulate cleanup_aborted_messages deleting the highest row, which lowers
    // MAX(partition_offset). Only the persisted monotonic counter prevents reuse.
    let db = create_db_client().await?;
    let topic_id: i32 = db
        .query_one("SELECT id FROM kafka.topics WHERE name = $1", &[&topic])
        .await?
        .get(0);
    let deleted = db
        .execute(
            "DELETE FROM kafka.messages
             WHERE topic_id = $1 AND partition_id = $2 AND partition_offset = $3",
            &[&topic_id, &partition, &last],
        )
        .await?;
    assert_eq!(deleted, 1, "expected to delete exactly the tail message");

    // The next produce must assign last + 1, not reuse `last`.
    let (_p, new_off) = producer
        .send(
            FutureRecord::to(&topic).payload("m3").key("k"),
            Duration::from_secs(5),
        )
        .await
        .map_err(|(e, _msg)| e)?;
    println!("Next offset after cleanup = {} (expected {})", new_off, last + 1);

    if new_off != last + 1 {
        return Err(format!(
            "BUG-3 regression: offset reuse after cleanup — expected {}, got {}",
            last + 1,
            new_off
        )
        .into());
    }

    println!("✅ offsets stayed monotonic across cleanup");
    Ok(())
}
