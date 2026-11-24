//! Requests whose decoded form is many times their frame: a Metadata v9 request of 49
//! million empty topic names (98 MB on the wire, 2 bytes per topic) and a DeleteGroups v2
//! request of 98 million empty group names (1 byte per group). Before the decode meter each
//! decoded to some 6 GB on the network thread (every entry a struct of 60 to 90 bytes, and
//! the vectors holding them growing to twice that), and an allocation failure aborted the
//! listener worker, sending the postmaster through crash recovery. Each must now close its
//! connection (Kafka closes it for an oversized request) with the listener worker's memory
//! bounded, the worker still the same process, and other connections served meanwhile.

use super::{answers, connect_from, port, time_until_closed};
use crate::common::{create_db_client, TestResult};
use crate::worker::listener_pid;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

/// The most the listener worker's resident memory may grow while it refuses one such
/// request: the frame itself (98 MB, in the codec's buffer), the decode stopped at the
/// 64 MiB request limit (holding at most about three times that at once) and slack. Before
/// the meter it grew by some 6 GB.
const GROWTH_BOUND_BYTES: u64 = 1024 * 1024 * 1024;

/// The longest an ApiVersions on another connection may take while the attack runs (the
/// stopped decodes run off the I/O threads). Two 24 MB such requests delayed it by 4.8 s.
const PING_BOUND: Duration = Duration::from_secs(2);

fn uvarint(out: &mut Vec<u8>, mut n: u64) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

/// A flexible request frame (size prefix, header v2 with a null client id) of `body`.
fn flexible_frame(api_key: i16, version: i16, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(body.len() + 16);
    frame.extend_from_slice(&[0; 4]);
    frame.extend_from_slice(&api_key.to_be_bytes());
    frame.extend_from_slice(&version.to_be_bytes());
    frame.extend_from_slice(&1i32.to_be_bytes());
    frame.extend_from_slice(&(-1i16).to_be_bytes());
    frame.push(0);
    frame.extend_from_slice(body);
    let len = i32::try_from(frame.len() - 4).expect("under 2 GB");
    frame[..4].copy_from_slice(&len.to_be_bytes());
    frame
}

/// Metadata v9 of `n` topics of an empty name, auto-creation off.
pub fn metadata_of_empty_names(n: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(2 * n + 16);
    uvarint(&mut body, n as u64 + 1);
    for _ in 0..n {
        body.extend_from_slice(&[0x01, 0x00]);
    }
    body.extend_from_slice(&[0, 0, 0, 0]);
    flexible_frame(3, 9, &body)
}

/// DeleteGroups v2 of `n` empty group names.
pub fn delete_groups_of_empty_names(n: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(n + 16);
    uvarint(&mut body, n as u64 + 1);
    body.resize(body.len() + n, 0x01);
    body.push(0);
    flexible_frame(42, 2, &body)
}

/// A field of `/proc/<pid>/status` in bytes (`VmHWM`, `VmRSS`), if it can be read.
fn proc_status_bytes(pid: i32, field: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|l| l.starts_with(field))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Reset the process's peak resident size to its current one (`clear_refs` 5); false where
/// that is not allowed.
fn reset_peak(pid: i32) -> bool {
    std::fs::write(format!("/proc/{pid}/clear_refs"), "5").is_ok()
}

/// Send `frame` from a fresh connection and check that the broker closes it without an
/// answer, the listener's memory grows by at most [`GROWTH_BOUND_BYTES`], the listener is
/// the same process, and it answers afterwards.
async fn refused_within_bounded_memory(
    name: &str,
    frame: &[u8],
    src: Ipv4Addr,
) -> Result<(), String> {
    let db = create_db_client().await.map_err(|e| e.to_string())?;
    let pid = listener_pid(&db)
        .await
        .map_err(|e| e.to_string())?
        .ok_or("no pg_kafka_listener")?;
    let reset = reset_peak(pid);
    let rss_before = proc_status_bytes(pid, "VmRSS");
    let hwm_before = proc_status_bytes(pid, "VmHWM");
    println!(
        "{name}: {} byte frame; listener pid {pid}, VmRSS {rss_before:?}, VmHWM {hwm_before:?} (peak reset: {reset})",
        frame.len()
    );

    let mut attack = connect_from(src, port()).await.map_err(|e| e.to_string())?;
    let started = Instant::now();
    // The broker may close the connection before it read the whole frame.
    let _ = attack.write_all(frame).await;
    let closed = time_until_closed(&mut attack, Duration::from_secs(120)).await;
    println!(
        "{name}: connection closed after {closed:?} ({:?} from the first byte)",
        started.elapsed()
    );
    if closed.is_none() {
        return Err(format!(
            "{name}: the broker neither answered nor closed the connection"
        ));
    }

    let after = listener_pid(&db).await.map_err(|e| e.to_string())?;
    if after != Some(pid) {
        return Err(format!(
            "{name}: the listener worker was pid {pid}, now {after:?}: it died (an allocation \
             failure aborts it and the postmaster goes through crash recovery)"
        ));
    }
    match (rss_before, hwm_before, proc_status_bytes(pid, "VmHWM")) {
        (Some(rss), Some(hwm), Some(hwm_after)) => {
            let bound = hwm.max(rss + GROWTH_BOUND_BYTES);
            println!(
                "{name}: VmHWM {} MiB after (grew {} MiB over VmRSS before; bound {} MiB)",
                hwm_after >> 20,
                hwm_after.saturating_sub(rss) >> 20,
                GROWTH_BOUND_BYTES >> 20
            );
            if hwm_after > bound {
                return Err(format!(
                    "{name}: the listener's peak resident memory grew to {hwm_after} bytes from \
                     {rss} (bound {bound})"
                ));
            }
        }
        _ => println!("{name}: /proc/{pid}/status is not readable here; memory not checked"),
    }

    let mut probe = connect_from(Ipv4Addr::LOCALHOST, port())
        .await
        .map_err(|e| e.to_string())?;
    if !answers(&mut probe, 77).await {
        return Err(format!(
            "{name}: the broker does not answer after the attack"
        ));
    }
    Ok(())
}

/// The reviewer's 98 MB Metadata v9 frame of empty names and a DeleteGroups v2 frame of 98
/// million empty names close their connections with the listener's memory bounded, while
/// ApiVersions on another connection keeps answering.
pub async fn test_requests_that_decode_to_many_times_their_frame_are_refused() -> TestResult {
    println!("=== Test: requests that decode to many times their frame are refused ===\n");

    let stop = Arc::new(AtomicBool::new(false));
    let worst_ping_ms = Arc::new(AtomicU64::new(0));
    let pinger = {
        let (stop, worst) = (Arc::clone(&stop), Arc::clone(&worst_ping_ms));
        tokio::spawn(async move {
            let mut s = connect_from(Ipv4Addr::LOCALHOST, port()).await.ok()?;
            let mut id = 1000;
            while !stop.load(Ordering::Relaxed) {
                let t = Instant::now();
                if !answers(&mut s, id).await {
                    return Some(false);
                }
                let ms = u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX);
                worst.fetch_max(ms, Ordering::Relaxed);
                id += 1;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Some(true)
        })
    };

    println!("Step 1: Metadata v9 of 49,000,000 empty topic names (98 MB)...");
    let result = refused_within_bounded_memory(
        "Metadata v9",
        &metadata_of_empty_names(49_000_000),
        Ipv4Addr::new(127, 0, 0, 21),
    )
    .await;
    println!("Step 2: DeleteGroups v2 of 98,000,000 empty group names (98 MB)...");
    let result = match result {
        Ok(()) => {
            refused_within_bounded_memory(
                "DeleteGroups v2",
                &delete_groups_of_empty_names(98_000_000),
                Ipv4Addr::new(127, 0, 0, 22),
            )
            .await
        }
        Err(e) => Err(e),
    };
    stop.store(true, Ordering::Relaxed);
    let pinged = pinger.await?;
    result?;
    let worst = Duration::from_millis(worst_ping_ms.load(Ordering::Relaxed));
    println!("ApiVersions on another connection: worst {worst:?}");
    if pinged != Some(true) {
        return Err("ApiVersions on another connection stopped answering".into());
    }
    if worst > PING_BOUND {
        return Err(format!("ApiVersions took {worst:?} while the attack ran").into());
    }
    println!("✅ Test PASSED\n");
    Ok(())
}
