//! Heartbeats and group-commit batches.

use crate::common::TestResult;
use crate::wire::request_frame;
use crate::worker::*;
use kafka_protocol::messages::{GroupId, HeartbeatRequest};
use kafka_protocol::protocol::StrBytes;
use rdkafka::producer::FutureProducer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A Heartbeat waiting on the liveness lane is answered between a batch's requests; it
/// does not end the batch. Otherwise, with many consumers heartbeating, produce batches
/// collapse to one request each (one commit, one fsync each).
///
/// Construction: four raw connections keep the liveness lane busy with pipelined
/// Heartbeats (written continuously, read by a separate task), and every produce batch
/// is delayed 200 ms before its commit (`pg_kafka.test_pre_commit_delay_ms`), so the
/// number of batches shows in the elapsed time. 12 producers send 5 records each, one at
/// a time: in batches of their own the 60 produces take 5-6 s; sharing batches, a few
/// rounds of ~200 ms.
///
/// Asserts: the 60 produces complete within 5 s while the heartbeat flood is served
/// (every flood connection keeps getting responses).
pub async fn test_heartbeats_do_not_split_group_commit_batches() -> TestResult {
    into_test_result(heartbeats_batches().await)
}

/// One connection that keeps 32 pipelined Heartbeat (v0) requests in flight until
/// `stop`; returns how many responses it read.
async fn heartbeat_flood(stop: Arc<AtomicBool>) -> SendResult<u64> {
    let mut request = HeartbeatRequest::default();
    request.group_id = GroupId(StrBytes::from_static_str("heartbeat-flood-group"));
    request.generation_id = 1;
    request.member_id = StrBytes::from_static_str("heartbeat-flood-member");
    let burst: Vec<u8> = request_frame(0, 7, &request).repeat(32);

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

async fn heartbeats_batches() -> SendResult<()> {
    println!("=== Test: heartbeats do not split group-commit batches ===\n");
    let db = session().await?;
    wait_for_serving_listener(&db, None, Duration::from_secs(30)).await?;
    let topic = unique("hb-batch");
    let producers: Vec<FutureProducer> = (0..12)
        .map(|_| quick_producer("30000"))
        .collect::<SendResult<_>>()?;
    for p in &producers {
        send(p, &topic, "warm").await?;
    }

    let stop = Arc::new(AtomicBool::new(false));
    let floods: Vec<_> = (0..4)
        .map(|_| tokio::spawn(heartbeat_flood(stop.clone())))
        .collect();
    tokio::time::sleep(Duration::from_millis(500)).await;
    set_guc(&db, "pg_kafka.test_pre_commit_delay_ms", Some("200")).await?;
    let result: SendResult<Duration> = async {
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
