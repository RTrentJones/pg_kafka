//! Handing parsed requests to the DB thread.

use crossbeam_channel::Sender;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::long_poll::handle_fetch_long_poll;
use crate::kafka::api::{FetchRequestData, RequestBody, RequestHeader};
use crate::kafka::constants::MAX_LONG_POLLS_PER_CONNECTION;
use crate::kafka::messages::KafkaRequest;
use crate::kafka::pending_fetches::PendingFetchRegistry;

/// Hand one parsed request to the DB thread: a Fetch with `max_wait_ms > 0` goes to a
/// long-poll task (while the per-connection cap allows), everything else straight to
/// its lane. Returns false when the worker channel is gone.
pub(super) async fn dispatch_request(
    request: KafkaRequest,
    request_lanes: &crate::kafka::RequestLanes,
    registry: &Arc<PendingFetchRegistry>,
    long_poll_permits: &Arc<tokio::sync::Semaphore>,
    enable_long_polling: bool,
    poll_interval_ms: i32,
) -> bool {
    debug!("Request parsed successfully, dispatching to worker...");

    // Check if this is a Fetch request that should use long polling
    let should_long_poll = enable_long_polling
        && matches!(
            &request.body,
            RequestBody::Fetch(fetch) if fetch.max_wait_ms > 0
        );

    // Acquire a long-poll slot; at the cap the fetch falls through
    // to the immediate path below instead of spawning a task.
    let long_poll_permit = if should_long_poll {
        match long_poll_permits.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(_) => {
                debug!(
                    "Per-connection long-poll cap ({}) reached; serving fetch immediately",
                    MAX_LONG_POLLS_PER_CONNECTION
                );
                None
            }
        }
    } else {
        None
    };

    let request = match (long_poll_permit, request) {
        (
            Some(permit),
            KafkaRequest {
                header:
                    RequestHeader {
                        correlation_id,
                        client_id,
                        api_version,
                        ..
                    },
                body:
                    RequestBody::Fetch(FetchRequestData {
                        max_wait_ms,
                        min_bytes,
                        max_bytes,
                        isolation_level,
                        topic_data,
                    }),
                response_tx: original_response_tx,
            },
        ) => {
            debug!(
                "Fetch request with long polling: max_wait_ms={}, min_bytes={}, isolation_level={}",
                max_wait_ms, min_bytes, isolation_level
            );

            let long_poll_registry = registry.clone();
            let long_poll_request_tx = request_lanes.main.clone();

            tokio::spawn(async move {
                // Hold the per-connection slot for the task's lifetime.
                let _long_poll_slot = permit;
                if let Err(e) = handle_fetch_long_poll(
                    correlation_id,
                    client_id,
                    api_version,
                    max_wait_ms,
                    min_bytes,
                    max_bytes,
                    isolation_level,
                    topic_data,
                    long_poll_request_tx,
                    original_response_tx,
                    long_poll_registry,
                    poll_interval_ms,
                )
                .await
                {
                    warn!("Long poll handler error: {}", e);
                }
            });

            // Don't send to worker directly - the long poll handler will do it
            return true;
        }
        (_, request) => request,
    };

    // Normal path - send to worker directly (SEC-4: non-blocking handoff).
    // DR-12: Heartbeat routes to the liveness lane, everything else to
    // the main lane, so group liveness never queues behind heavy work.
    let lane = request_lanes.route(&request).clone();
    send_with_backpressure(&lane, request).await
}

/// SEC-4: hand a request to the DB thread without blocking the async runtime.
///
/// The request channel is a bounded crossbeam channel; a plain `Sender::send` *blocks the calling
/// thread* when the channel is full (DB thread behind), which on the tokio runtime parks a whole
/// worker thread and starves every other connection scheduled on it. Instead we `try_send` and, on a
/// full channel, `await` a short sleep and retry — yielding the worker to other tasks. This preserves
/// the bounded channel's backpressure (we never drop or reorder the request, we wait for room) while
/// keeping the runtime responsive. The normal, not-full case takes the first `try_send` with no sleep.
///
/// Returns `true` once the item is enqueued, or `false` if the channel is disconnected (DB thread
/// gone) — the caller tears down the connection in that case.
pub(super) async fn send_with_backpressure<T>(tx: &Sender<T>, mut item: T) -> bool {
    loop {
        match tx.try_send(item) {
            Ok(()) => return true,
            Err(crossbeam_channel::TrySendError::Full(returned)) => {
                item = returned;
                // Yield the worker thread instead of blocking it, then retry.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_send_with_backpressure_succeeds_once_room_frees() {
        // SEC-4: a full bounded channel must not make the send hang forever or drop the item — it
        // waits (yielding the runtime) until a slot frees, then enqueues the item in order.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        tx.try_send(1).unwrap(); // fill to capacity

        let tx2 = tx.clone();
        let send = tokio::spawn(async move { send_with_backpressure(&tx2, 2).await });

        // Let the sender spin a few backoff iterations, then free a slot.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(rx.recv().unwrap(), 1, "first item drains");

        let join = tokio::time::timeout(Duration::from_millis(500), send).await;
        assert!(join.is_ok(), "send task should finish, not hang");
        let sent = join.unwrap().expect("send task should not panic");
        assert!(sent, "send should report success once room frees");
        assert_eq!(
            rx.recv().unwrap(),
            2,
            "the backpressured item is enqueued in order"
        );
    }

    #[tokio::test]
    async fn test_send_with_backpressure_reports_disconnect() {
        // SEC-4: if the DB thread is gone (receiver dropped), the send reports failure so the caller
        // tears down the connection instead of looping forever.
        let (tx, rx) = crossbeam_channel::bounded::<i32>(1);
        drop(rx);
        assert!(
            !send_with_backpressure(&tx, 1).await,
            "a disconnected channel must report failure"
        );
    }
}
