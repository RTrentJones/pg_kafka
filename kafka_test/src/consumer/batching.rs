//! RecordBatch grouping of fetched records.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::{batch, batch_headers, fetched_values};
use crate::wire::Wire;

/// Fetch groups a producer's consecutive records into one RecordBatch, not one batch per
/// record.
pub async fn test_fetch_groups_records_into_batches() -> TestResult {
    println!("=== Test: fetch returns records grouped into batches ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-batch", 1).await?;
    let mut w = Wire::connect().await?;
    for i in 0..10 {
        let values: Vec<String> = (0..10).map(|j| format!("m{}", i * 10 + j)).collect();
        let refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            w.produce(&t, 0, batch(&refs, -1, -1, -1, false, None), None)
                .await?
                .0,
            error_codes::NONE
        );
    }
    let pd = w.fetch(&t, 0, 0, 0).await?;
    let records = pd.records.unwrap_or_default();
    assert_eq!(
        batch_headers(&records),
        vec![(0, 99, 100)],
        "one batch of 100"
    );
    let values = fetched_values(&records)?;
    assert_eq!(values.len(), 100);
    assert!(values
        .iter()
        .enumerate()
        .all(|(i, (o, v))| *o == i as i64 && *v == format!("m{}", i)));
    println!("  100 records in {} bytes (one batch)", records.len());
    // From the middle of the run.
    let pd = w.fetch(&t, 0, 37, 0).await?;
    assert_eq!(
        batch_headers(&pd.records.unwrap_or_default()),
        vec![(37, 62, 63)]
    );
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
