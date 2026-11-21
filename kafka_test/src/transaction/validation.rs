//! Validation of transactional requests.

use crate::common::TestResult;
use crate::error_codes;
use crate::fixtures::topic_with_partitions as topic;
use crate::setup::TestContext;
use crate::wire::records::batch;
use crate::wire::Wire;

/// Transaction validation: InitProducerId timeouts, AddPartitionsToTxn on an unknown
/// topic (not created any more), and a transactional Produce to a partition the
/// transaction did not add.
pub async fn test_transactional_requests_are_validated() -> TestResult {
    println!("=== Test: transaction validation ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-val", 2).await?;
    let tx = ctx.unique_topic("txn-val-tx").await;
    let mut w = Wire::connect().await?;
    for timeout in [-5, 0, 900_001, i32::MAX] {
        assert_eq!(
            w.init_producer_id(Some(&tx), timeout).await?,
            (error_codes::INVALID_TRANSACTION_TIMEOUT, -1, -1),
            "timeout {timeout}"
        );
    }
    // Idempotent producers ignore the timeout.
    assert_eq!(w.init_producer_id(None, -5).await?.0, error_codes::NONE);
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 900_000).await?;
    assert_eq!(err, error_codes::NONE);

    let missing = ctx.unique_topic("txn-val-missing").await;
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&missing, &[0]), (&t, &[0])])
            .await?,
        vec![
            (missing.clone(), 0, error_codes::UNKNOWN_TOPIC_OR_PARTITION),
            (t.clone(), 0, error_codes::OPERATION_NOT_ATTEMPTED)
        ]
    );
    let created: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.topics WHERE name = $1",
            &[&missing],
        )
        .await?
        .get(0);
    assert_eq!(created, 0, "AddPartitionsToTxn must not create topics");
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0, 9])]).await?,
        vec![
            (t.clone(), 0, error_codes::OPERATION_NOT_ATTEMPTED),
            (t.clone(), 9, error_codes::UNKNOWN_TOPIC_OR_PARTITION)
        ]
    );
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0])]).await?[0].2,
        error_codes::NONE
    );

    let tb = |seq: i32| batch(&["t"], pid, ep, seq, true, None);
    assert_eq!(
        w.produce(&t, 0, tb(0), Some(&tx)).await?,
        (error_codes::NONE, 0)
    );
    assert_eq!(
        w.produce(&t, 1, tb(0), Some(&tx)).await?.0,
        error_codes::INVALID_TXN_STATE,
        "partition 1 not added"
    );
    assert_eq!(
        w.produce(&t, 0, batch(&["t"], pid, ep - 1, 1, true, None), Some(&tx))
            .await?
            .0,
        error_codes::INVALID_PRODUCER_EPOCH
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, error_codes::NONE);
    // The next transaction starts with an empty registry.
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[1])]).await?[0].2,
        error_codes::NONE
    );
    assert_eq!(
        w.produce(&t, 0, tb(1), Some(&tx)).await?.0,
        error_codes::INVALID_TXN_STATE,
        "added in the previous transaction only"
    );
    assert_eq!(
        w.produce(&t, 1, tb(0), Some(&tx)).await?.0,
        error_codes::NONE
    );
    assert_eq!(w.end_txn(&tx, pid, ep, false).await?, error_codes::NONE);
    let p1_rows: i64 = ctx
        .db()
        .query_one(
            "SELECT COUNT(*) FROM kafka.messages m JOIN kafka.topics t ON t.id = m.topic_id
             WHERE t.name = $1 AND m.partition_id = 1 AND m.txn_state IS DISTINCT FROM 'aborted'",
            &[&t],
        )
        .await?
        .get(0);
    assert_eq!(p1_rows, 0);
    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}

/// INVALID_PRODUCER_ID_MAPPING.
const INVALID_PRODUCER_ID_MAPPING: i16 = 49;

/// Each partition's `(partition, error_code)` of a Produce v7 of `parts` to `topic`.
async fn codes(
    w: &mut Wire,
    topic: &str,
    parts: Vec<(i32, bytes::Bytes)>,
    transactional_id: Option<&str>,
) -> crate::common::DynResult<Vec<(i32, i16)>> {
    Ok(w.produce_many(&[(topic, parts)], transactional_id)
        .await?
        .into_iter()
        .map(|(_, p, code, _)| (p, code))
        .collect())
}

/// The rows of `topic` by partition and transaction state.
async fn rows(
    ctx: &TestContext,
    topic: &str,
) -> crate::common::DynResult<Vec<(i32, Option<String>, i64)>> {
    Ok(ctx
        .db()
        .query(
            "SELECT m.partition_id, m.txn_state, count(*) FROM kafka.messages m
             JOIN kafka.topics t ON t.id = m.topic_id WHERE t.name = $1
             GROUP BY 1, 2 ORDER BY 1, 2 NULLS FIRST",
            &[&topic],
        )
        .await?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect())
}

/// Whether the broker closes `stream` within 3 s.
async fn closes(stream: &mut tokio::net::TcpStream) -> bool {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 64];
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(3), stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) => return true,
            Ok(Ok(_)) => {}
            Err(_) => return false,
        }
    }
}

/// Send an acks=0 Produce of `parts` with `transactional_id` on a fresh connection; whether
/// the broker closed it.
async fn acks0_closes(
    topic: &str,
    parts: Vec<(i32, bytes::Bytes)>,
    transactional_id: &str,
) -> crate::common::DynResult<bool> {
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::protocol::StrBytes;
    use tokio::io::AsyncWriteExt;
    let mut request = crate::wire::requests::produce_request(topic, parts, 0);
    request.transactional_id = Some(TransactionalId(StrBytes::from_string(
        transactional_id.to_string(),
    )));
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", 9092)).await?;
    s.write_all(&crate::wire::request_frame(7, 1, &request))
        .await?;
    Ok(closes(&mut s).await)
}

/// Kafka 3.7 checks a transactional Produce per partition, by whether the partition's batch
/// is transactional (`ReplicaManager.appendRecords` verifies the transactional batches with
/// the transaction coordinator): a transactional batch of an unknown transactional id, or of
/// a producer id that does not own it, is INVALID_PRODUCER_ID_MAPPING, of a stale epoch
/// INVALID_PRODUCER_EPOCH, to a partition not added INVALID_TXN_STATE; a plain or idempotent
/// batch in the same request is written as it would be without the transactional id. With
/// acks=0 the same checks apply, and an error closes the connection.
pub async fn test_transactional_produce_is_validated_per_partition() -> TestResult {
    println!("=== Test: transactional Produce validated per partition ===\n");
    let ctx = TestContext::new().await?;
    let t = topic(&ctx, "txn-part", 2).await?;
    let unknown = ctx.unique_topic("txn-part-unknown").await;
    let mut w = Wire::connect().await?;
    let plain = || batch(&["p"], -1, -1, -1, false, None);
    let committed = |p, n| (p, None, n);

    println!("Step 1: an unknown transactional id");
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![(0, batch(&["t"], 7, 0, 0, true, None)), (1, plain())],
            Some(&unknown)
        )
        .await?,
        vec![(0, INVALID_PRODUCER_ID_MAPPING), (1, error_codes::NONE)]
    );
    let (err, idem_pid, idem_ep) = w.init_producer_id(None, 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![
                (0, batch(&["i"], idem_pid, idem_ep, 0, false, None)),
                (1, plain())
            ],
            Some(&unknown)
        )
        .await?,
        vec![(0, error_codes::NONE), (1, error_codes::NONE)]
    );
    assert_eq!(
        rows(&ctx, &t).await?,
        vec![committed(0, 1), committed(1, 2)]
    );
    println!("✅ 49 on the transactional partition; the others written");

    println!("Step 2: a known transactional id");
    let tx = ctx.unique_topic("txn-part-tx").await;
    let (err, pid, ep) = w.init_producer_id(Some(&tx), 60_000).await?;
    assert_eq!(err, error_codes::NONE);
    let tb = |pid, ep, seq| batch(&["t"], pid, ep, seq, true, None);
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![(0, tb(pid, ep, 0)), (1, plain())],
            Some(&tx)
        )
        .await?,
        vec![(0, error_codes::INVALID_TXN_STATE), (1, error_codes::NONE)],
        "not added"
    );
    assert_eq!(
        w.add_partitions(&tx, pid, ep, &[(&t, &[0])]).await?[0].2,
        error_codes::NONE
    );
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![(0, tb(pid + 1000, ep, 0)), (1, plain())],
            Some(&tx)
        )
        .await?,
        vec![(0, INVALID_PRODUCER_ID_MAPPING), (1, error_codes::NONE)],
        "another producer id"
    );
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![(0, tb(pid, ep + 3, 0)), (1, plain())],
            Some(&tx)
        )
        .await?,
        vec![
            (0, error_codes::INVALID_PRODUCER_EPOCH),
            (1, error_codes::NONE)
        ],
        "another epoch"
    );
    assert_eq!(
        codes(
            &mut w,
            &t,
            vec![(0, tb(pid, ep, 0)), (1, plain())],
            Some(&tx)
        )
        .await?,
        vec![(0, error_codes::NONE), (1, error_codes::NONE)]
    );
    assert_eq!(
        rows(&ctx, &t).await?,
        vec![
            committed(0, 1),
            (0, Some("pending".to_string()), 1),
            committed(1, 6)
        ]
    );
    println!("✅ per partition; plain batches written committed");

    println!("Step 3: acks=0");
    assert!(
        !acks0_closes(&t, vec![(1, plain())], &unknown).await?,
        "a plain batch with an unknown transactional id is written"
    );
    assert!(
        acks0_closes(&t, vec![(0, tb(7, 0, 0)), (1, plain())], &unknown).await?,
        "a transactional batch of an unknown transactional id closes"
    );
    assert!(
        !acks0_closes(&t, vec![(0, tb(pid, ep, 1))], &tx).await?,
        "a valid transactional batch is written"
    );
    assert!(
        acks0_closes(&t, vec![(1, tb(pid, ep, 0))], &tx).await?,
        "a partition not added closes"
    );
    assert_eq!(
        rows(&ctx, &t).await?,
        vec![
            committed(0, 1),
            (0, Some("pending".to_string()), 2),
            committed(1, 8)
        ],
        "acks=0 transactional batches are written in the transaction"
    );
    assert_eq!(w.end_txn(&tx, pid, ep, true).await?, error_codes::NONE);
    assert_eq!(
        rows(&ctx, &t).await?,
        vec![committed(0, 3), committed(1, 8)]
    );
    println!("✅ the same checks; an error closes the connection");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
