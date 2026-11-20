//! InitProducerId for a transactional id with the producer id and epoch the client
//! presents (v3+, KIP-360): Kafka 3.7 bumps only the current owner, answers a retry of a
//! bump it already made with the same epoch, and fences everyone else.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::{ProducerId, TransactionalId};
use kafka_protocol::protocol::StrBytes;
use uuid::Uuid;

/// PRODUCER_FENCED (Kafka 3.7's answer to a fenced InitProducerId).
const PRODUCER_FENCED: i16 = 90;

/// InitProducerId v4 presenting `(producer_id, epoch)`: `(error, producer_id, epoch)`.
async fn init(
    w: &mut Wire,
    txn: Option<&str>,
    producer_id: i64,
    epoch: i16,
) -> Result<(i16, i64, i16), Box<dyn std::error::Error>> {
    let mut req = InitProducerIdRequest::default();
    req.transactional_id = txn.map(|t| TransactionalId(StrBytes::from_string(t.to_string())));
    req.transaction_timeout_ms = 60_000;
    req.producer_id = ProducerId(producer_id);
    req.producer_epoch = epoch;
    let resp: InitProducerIdResponse = w.call(4, &req).await?;
    Ok((resp.error_code, resp.producer_id.0, resp.producer_epoch))
}

/// Kafka 3.7's answers, in order: a bump by the owner, a retry of that bump, a new
/// producer taking over, then zombies and malformed presented ids.
pub async fn test_init_producer_id_checks_the_presented_epoch() -> TestResult {
    println!("=== Test: InitProducerId Checks the Presented Producer Epoch ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-presented", 1).await?;
    let txn = format!("txn-presented-{}", Uuid::new_v4());
    let mut w = Wire::connect().await?;

    let (err, pid, e0) = init(&mut w, Some(&txn), -1, -1).await?;
    assert_eq!(err, error_codes::NONE);
    println!("Step 1: The owner {pid}/{e0} re-initialises (KIP-360 recovery)...");
    assert_eq!(
        init(&mut w, Some(&txn), pid, e0).await?,
        (error_codes::NONE, pid, e0 + 1),
        "the current owner is bumped"
    );
    assert_eq!(
        init(&mut w, Some(&txn), pid, e0).await?,
        (error_codes::NONE, pid, e0 + 1),
        "a retry of that bump gets the same epoch back"
    );
    println!(
        "✅ Bumped to {}, the retry answered without a second bump\n",
        e0 + 1
    );

    println!("Step 2: A new producer (no id presented) takes over...");
    let (err, pid2, e2) = init(&mut w, Some(&txn), -1, -1).await?;
    assert_eq!((err, pid2, e2), (error_codes::NONE, pid, e0 + 2));
    let added = w.add_partitions(&txn, pid, e2, &[(&t, &[0])]).await?;
    assert_eq!(added[0].2, error_codes::NONE);
    let (err, _) = w
        .produce(&t, 0, batch(&["live"], pid, e2, 0, true, None), Some(&txn))
        .await?;
    assert_eq!(err, error_codes::NONE);
    println!("✅ The new owner {pid}/{e2} has a transaction open\n");

    println!("Step 3: Zombies re-initialising are fenced, the owner is untouched...");
    for (zpid, zep, what) in [
        (pid, e0 + 1, "the fenced previous owner"),
        (pid, e0, "an older epoch"),
        (pid, e2 + 1, "a future epoch"),
        (pid + 999, 0, "another producer id"),
    ] {
        assert_eq!(
            init(&mut w, Some(&txn), zpid, zep).await?,
            (PRODUCER_FENCED, -1, -1),
            "{what} ({zpid}/{zep}) must be PRODUCER_FENCED"
        );
    }
    assert_eq!(
        init(&mut w, Some(&txn), -1, 3).await?.0,
        error_codes::INVALID_REQUEST,
        "an epoch without a producer id"
    );
    assert_eq!(
        init(&mut w, None, 7, -1).await?.0,
        error_codes::INVALID_REQUEST,
        "a producer id without an epoch"
    );
    assert_eq!(
        w.end_txn(&txn, pid, e2, true).await?,
        error_codes::NONE,
        "the live owner commits"
    );
    let visible: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.txn_state IS NULL",
            &[&t],
        )
        .await?
        .get(0);
    assert_eq!(visible, 1);
    println!("✅ Zombies fenced; the owner's transaction committed\n");

    println!("Step 4: Presented ids on a new transactional id and an idempotent producer...");
    let (err, _, epoch) = init(&mut w, Some(&format!("{txn}-new")), pid, 4).await?;
    assert_eq!((err, epoch), (error_codes::NONE, 0), "a new id starts at 0");
    let (err, fresh, epoch) = init(&mut w, None, pid, e2).await?;
    assert_eq!((err, epoch), (error_codes::NONE, 0));
    assert_ne!(fresh, pid, "an idempotent producer always gets a new id");
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
