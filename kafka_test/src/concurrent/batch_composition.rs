//! Heartbeats and group-commit batches.

use crate::common::TestResult;
use crate::worker::*;
use rdkafka::producer::FutureProducer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// -------------------------------------------------------------------------------------
// Item 7: heartbeats no longer split group-commit batches.
// -------------------------------------------------------------------------------------

/// Before: a batch stopped taking requests as soon as a Heartbeat waited on the
/// liveness lane, so with many consumers heartbeating, produce batches collapsed to one
/// request each (one commit, one fsync each). Now heartbeats waiting during a batch are
/// answered between its requests and the batch keeps taking queued work.
///
/// Construction: four raw connections keep the liveness lane busy with pipelined
/// Heartbeats (written continuously, read by a separate task), and every produce batch is delayed 200 ms before
/// its commit (`pg_kafka.test_pre_commit_delay_ms`), so the number of batches shows in
/// the elapsed time. 12 producers send 5 records each, one at a time. Before, most of
/// the 60 produces committed in batches of their own (measured 5-6 s); now concurrent
/// produces share batches (a few rounds of ~200 ms).
///
/// Asserts: the 60 produces complete within 5 s while the heartbeat flood is served
/// (every flood connection keeps getting responses).
pub async fn test_heartbeats_do_not_split_group_commit_batches() -> TestResult {
    into_test_result(heartbeats_batches().await)
}

/// One connection that keeps 32 pipelined Heartbeat (v0) requests in flight until
/// `stop`; returns how many responses it read.
async fn heartbeat_flood(stop: Arc<AtomicBool>) -> DynResult<u64> {
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::messages::{GroupId, HeartbeatRequest, RequestHeader};
    use kafka_protocol::protocol::{Encodable, HeaderVersion, Request, StrBytes};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut header = RequestHeader::default();
    header.request_api_key = HeartbeatRequest::KEY;
    header.request_api_version = 0;
    header.correlation_id = 7;
    header.client_id = Some(StrBytes::from_static_str("w3-heartbeat-flood"));
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("w3-worker-flood-group"));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("w3-flood-member");
    let mut body = BytesMut::new();
    header
        .encode(&mut body, HeartbeatRequest::header_version(0))
        .map_err(boxed)?;
    request.encode(&mut body, 0).map_err(boxed)?;
    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    let burst: Vec<u8> = frame.repeat(32);

    let stream = tokio::net::TcpStream::connect("127.0.0.1:9092")
        .await
        .map_err(boxed)?;
    let (mut reader, mut writer) = stream.into_split();
    // Writer and reader are decoupled: the socket always holds unread Heartbeats, so
    // the broker re-fills the liveness lane the moment it has room.
    let writer_stop = stop.clone();
    let writing = tokio::spawn(async move {
        while !writer_stop.load(Ordering::Relaxed) {
            if writer.write_all(&burst).await.is_err() {
                break;
            }
        }
    });
    let mut responses = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let mut size = [0u8; 4];
        match tokio::time::timeout(Duration::from_millis(500), reader.read_exact(&mut size)).await {
            Ok(r) => {
                r.map_err(boxed)?;
            }
            Err(_) => continue,
        }
        let mut buf = vec![0u8; i32::from_be_bytes(size) as usize];
        reader.read_exact(&mut buf).await.map_err(boxed)?;
        responses += 1;
    }
    writing.abort();
    Ok(responses)
}

async fn heartbeats_batches() -> DynResult<()> {
    println!("=== Test: heartbeats do not split group-commit batches ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("hb-batch");
    let producers: Vec<FutureProducer> = (0..12)
        .map(|_| quick_producer("30000"))
        .collect::<DynResult<_>>()?;
    for p in &producers {
        send(p, &topic, "warm").await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let floods: Vec<_> = (0..4)
        .map(|_| tokio::spawn(heartbeat_flood(stop.clone())))
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    set_guc(&db, "pg_kafka.test_pre_commit_delay_ms", Some("200")).await?;
    let result: DynResult<Duration> = async {
        let start = Instant::now();
        let mut tasks = Vec::new();
        for p in &producers {
            let p = p.clone();
            let t = topic.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..5 {
                    send(&p, &t, &format!("r{}", i)).await?;
                }
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }));
        }
        for t in tasks {
            t.await.map_err(boxed)??;
        }
        Ok(start.elapsed())
    }
    .await;
    set_guc(&db, "pg_kafka.test_pre_commit_delay_ms", None).await?;
    stop.store(true, Ordering::Relaxed);
    let mut served = Vec::new();
    for f in floods {
        served.push(f.await.map_err(boxed)??);
    }
    let took = result?;
    println!(
        "Step 1: 60 produces in {:?} while the flood got {:?} heartbeat responses",
        took, served
    );
    assert!(
        served.iter().all(|n| *n > 0),
        "every heartbeat connection must be served"
    );
    assert!(
        took < Duration::from_millis(5000),
        "produces were committed one batch each under heartbeat traffic ({:?})",
        took
    );
    println!("✅ produce batches stay large under heartbeat traffic");
    Ok(())
}
