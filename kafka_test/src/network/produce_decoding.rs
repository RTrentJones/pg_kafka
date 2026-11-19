//! Produce requests the broker must refuse while decoding them: an acks=0 Produce at a
//! version the broker does not serve.

use super::{answers, time_until_closed, PORT};
use crate::common::TestResult;
use crate::setup::TestContext;
use crate::wire::records::plain_batch;
use crate::wire::requests::produce_request;
use crate::wire::{decode_response, read_frame_within, request_frame};
use bytes::Bytes;
use kafka_protocol::messages::ProduceRequest;
use kafka_protocol::records::Compression;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// An acks=0 Produce at a version the broker does not serve (v10; it serves 3-9) gets no
/// response, so it cannot be told UNSUPPORTED_VERSION: the broker closes the connection,
/// as Kafka does, instead of dropping the records unnoticed.
pub async fn test_unsupported_version_acks0_produce_closes_connection() -> TestResult {
    println!("=== Test: acks=0 Produce at an unsupported version closes ===\n");
    let ctx = TestContext::new().await?;
    let topic = ctx.unique_topic("net-acks0-v10").await;
    let batch = plain_batch(&[Bytes::from_static(b"lost?")], Compression::None);

    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let request = produce_request(&topic, vec![(0, batch.clone())], 0);
    s.write_all(&request_frame(10, 1, &request)).await?;
    let closed = time_until_closed(&mut s, Duration::from_secs(5)).await;
    assert!(
        closed.is_some(),
        "acks=0 Produce v10 must close the connection"
    );
    println!("✅ closed after {:?}", closed.unwrap_or_default());

    // acks=1 at the same version is answered UNSUPPORTED_VERSION; the connection stays.
    let mut s = TcpStream::connect(("127.0.0.1", PORT)).await?;
    let request = produce_request(&topic, vec![(0, batch)], 1);
    s.write_all(&request_frame(10, 2, &request)).await?;
    let response = read_frame_within(&mut s, Duration::from_secs(5))
        .await?
        .ok_or("acks=1 v10 closed instead of answering")?;
    let (_, resp) = decode_response::<ProduceRequest>(10, &response)?;
    assert_eq!(
        resp.responses[0].partition_responses[0].error_code,
        crate::error_codes::UNSUPPORTED_VERSION
    );
    assert!(answers(&mut s, 3).await, "the acks=1 connection stays open");
    println!("✅ acks=1 v10: UNSUPPORTED_VERSION, connection open");

    ctx.cleanup().await?;
    println!("✅ Test PASSED\n");
    Ok(())
}
