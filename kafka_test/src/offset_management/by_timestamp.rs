//! ListOffsets-by-timestamp (audit CONF-7)
//!
//! A timestamp lookup resolves to the earliest offset whose record timestamp is at or after the
//! target, instead of returning UNSUPPORTED_VERSION.

use crate::common::{create_base_consumer, create_producer, TestResult};
use rdkafka::consumer::Consumer;
use rdkafka::producer::FutureRecord;
use rdkafka::{Offset, TopicPartitionList};
use std::time::Duration;

/// Produce three records at base, base+1000, base+2000 ms (offsets 0,1,2), then resolve timestamps
/// via `offsets_for_times`: a target between two records returns the earliest offset at/after it,
/// and a target before all records returns the first offset (CONF-7).
pub async fn test_list_offsets_by_timestamp() -> TestResult {
    println!("=== Test: ListOffsets by timestamp (CONF-7) ===\n");

    let topic = format!("listoffsets-ts-{}", uuid::Uuid::new_v4());
    let producer = create_producer()?;

    let base: i64 = 1_600_000_000_000; // 2020-09-13
    let mut partition = 0i32;
    for i in 0..3i64 {
        let payload = format!("m{}", i);
        let (p, _off) = producer
            .send(
                FutureRecord::to(&topic)
                    .payload(&payload)
                    .key("k")
                    .timestamp(base + i * 1000),
                Duration::from_secs(5),
            )
            .await
            .map_err(|(e, _msg)| e)?;
        partition = p;
    }
    println!("Produced 3 records at {}, +1000, +2000 ms", base);

    let consumer = create_base_consumer("listoffsets-ts-probe")?;

    // Resolve a target timestamp to an offset and assert it equals `expected`.
    let resolve = |target: i64| -> Result<i64, Box<dyn std::error::Error>> {
        let mut query = TopicPartitionList::new();
        query.add_partition_offset(&topic, partition, Offset::Offset(target))?;
        let resolved = consumer.offsets_for_times(query, Duration::from_secs(5))?;
        let elem = resolved
            .find_partition(&topic, partition)
            .ok_or("partition missing from offsets_for_times result")?;
        match elem.offset() {
            Offset::Offset(o) => Ok(o),
            other => Err(format!("expected a concrete offset, got {:?}", other).into()),
        }
    };

    // base+1500 lies between record 1 (base+1000) and record 2 (base+2000): earliest at/after is 2.
    let mid = resolve(base + 1500)?;
    println!("offset for base+1500 = {} (expected 2)", mid);
    if mid != 2 {
        return Err(format!("CONF-7: expected offset 2 for base+1500, got {}", mid).into());
    }

    // base-1 is before all records: the earliest offset is 0.
    let early = resolve(base - 1)?;
    println!("offset for base-1 = {} (expected 0)", early);
    if early != 0 {
        return Err(format!("CONF-7: expected offset 0 for base-1, got {}", early).into());
    }

    println!("✅ ListOffsets resolves timestamps to offsets");
    Ok(())
}
