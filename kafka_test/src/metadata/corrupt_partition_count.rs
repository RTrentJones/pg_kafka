//! Metadata over a corrupt stored partition count.

use crate::common::{create_db_client, TestResult};
use crate::error_codes;
use crate::fixtures::produce_one;
use crate::setup::TestContext;
use crate::wire::requests::*;
use crate::wire::roundtrip;
use kafka_protocol::messages::metadata_request::MetadataRequest;

/// A corrupt stored partition count (only possible when the CHECK constraint is
/// bypassed) is reported as an error for that topic instead of being allocated; the worker and
/// the server stay up and other topics keep working. Sequential: drops and restores the
/// constraint for the duration of the test.
pub async fn test_metadata_survives_corrupt_partition_count() -> TestResult {
    println!("=== Test: Metadata survives a corrupt stored partition count ===\n");
    let ctx = TestContext::new().await?;
    let bad = ctx.unique_topic("corrupt").await;
    let good = ctx.unique_topic("good").await;
    produce_one(&good).await?;

    let db = create_db_client().await?;
    db.batch_execute("ALTER TABLE kafka.topics DROP CONSTRAINT topics_partitions_range")
        .await?;
    let outcome = async {
        db.execute(
            "INSERT INTO kafka.topics (name, partitions) VALUES ($1, 2000000000)",
            &[&bad],
        )
        .await?;
        let mut c = conn().await?;
        let md = metadata_raw(&mut c, &[&bad, &good]).await?;
        println!("Metadata: {:?}", md);
        assert_eq!(
            md[0].error_code,
            error_codes::UNKNOWN_SERVER_ERROR,
            "corrupt topic is an error"
        );
        assert_eq!(
            md[0].partitions, 0,
            "no partition entries for a corrupt count"
        );
        assert_eq!(md[1].error_code, error_codes::NONE);
        assert_eq!(md[1].partitions, 1);
        // An all-topics Metadata (what every client sends on startup) too.
        let mut req = MetadataRequest::default();
        req.topics = None;
        let all = roundtrip(&mut c, 9, 3, &req).await?;
        assert!(all
            .topics
            .iter()
            .any(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some(good.as_str())));
        // The same connection (and so the same worker) still answers.
        produce_one(&good).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await
    .map_err(|e| e.to_string());
    db.execute("DELETE FROM kafka.topics WHERE name = $1", &[&bad])
        .await?;
    db.batch_execute(
        "ALTER TABLE kafka.topics ADD CONSTRAINT topics_partitions_range
         CHECK (partitions >= 1 AND partitions <= 100000)",
    )
    .await?;
    outcome.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
