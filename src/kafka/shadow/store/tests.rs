// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Unit tests of `ShadowStore`'s transaction handling, its delegation to the
//! wrapped store and its in-memory counters.

use super::*;
use crate::kafka::shadow::outbox::fake::InMemoryShadowDb;
use crate::testing::mocks::MockKafkaStore;

fn buffered_record(topic_id: i32) -> PendingForwardRecord {
    PendingForwardRecord {
        row: RowKey::new(topic_id, 0, -1),
        key: None,
    }
}

#[test]
fn test_abort_finalizes_inner_txn_whatever_was_buffered() {
    // The inner abort always runs, or the transaction stays Ongoing.
    let mut mock = MockKafkaStore::new();
    mock.expect_abort_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    {
        let mut pending = store.pending_txn_messages.write().unwrap();
        pending
            .entry((100, 0))
            .or_default()
            .records
            .push(buffered_record(1));
    }

    store.abort_transaction("txn-1", 100, 0).unwrap();
}

#[test]
fn test_commit_finalizes_inner_txn() {
    // The inner commit runs unconditionally (state row → committed, pending
    // offsets applied); forwarding happens later, through the outbox.
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    {
        let mut pending = store.pending_txn_messages.write().unwrap();
        pending
            .entry((100, 0))
            .or_default()
            .records
            .push(buffered_record(1));
    }

    store.commit_transaction("txn-1", 100, 0).unwrap();
}

#[test]
fn test_unchanged_roles_are_delegated_to_the_inner_store() {
    // TopicStore, OffsetStore and ProducerStore are forwarded wholesale
    // (delegate_*_store!): each call reaches the wrapped store with its arguments
    // and returns the inner result unchanged.
    let mut mock = MockKafkaStore::new();
    mock.expect_get_topic_id()
        .withf(|name| name == "orders")
        .times(1)
        .returning(|_| Ok(Some(7)));
    mock.expect_set_topic_retention_ms()
        .withf(|id, ms| *id == 7 && *ms == Some(60_000))
        .times(1)
        .returning(|_, _| Ok(()));
    mock.expect_commit_offset()
        .withf(|g, t, p, o, m| g == "g" && *t == 7 && *p == 1 && *o == 42 && m.is_none())
        .times(1)
        .returning(|_, _, _, _, _| Ok(()));
    mock.expect_delete_consumer_group_offsets()
        .times(1)
        .returning(|_| Ok(()));
    mock.expect_allocate_producer_id()
        .times(1)
        .returning(|_, _| Ok((5, 0)));
    mock.expect_record_producer_sequence()
        .withf(|pid, t, p, seq| (*pid, *t, *p, *seq) == (5, 7, 1, 9))
        .times(1)
        .returning(|_, _, _, _| Ok(()));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    assert_eq!(store.get_topic_id("orders").unwrap(), Some(7));
    store.set_topic_retention_ms(7, Some(60_000)).unwrap();
    store.commit_offset("g", 7, 1, 42, None).unwrap();
    store.delete_consumer_group_offsets("g").unwrap();
    assert_eq!(store.allocate_producer_id(None, None).unwrap(), (5, 0));
    store.record_producer_sequence(5, 7, 1, 9).unwrap();
}

#[test]
fn test_abort_timed_out_evicts_stale_pending_buffers() {
    // The storage sweeper force-aborts timed-out transactions without
    // routing through abort_transaction(), so a buffer for an abandoned
    // producer must be evicted on the same timeout or it leaks forever.
    let mut mock = MockKafkaStore::new();
    mock.expect_abort_timed_out_transactions()
        .times(1)
        .returning(|_| Ok(vec![]));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    {
        let mut pending = store.pending_txn_messages.write().unwrap();
        // Stale: first_buffered older than the timeout we will sweep with.
        let mut stale = PendingTxnBuffer::from(vec![buffered_record(1)]);
        stale.first_buffered = Instant::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap_or_else(Instant::now);
        pending.insert((1, 0), stale);
        // Fresh: just buffered, well within the timeout.
        pending.insert((2, 0), PendingTxnBuffer::from(vec![buffered_record(1)]));
    }

    store
        .abort_timed_out_transactions(Duration::from_secs(60))
        .unwrap();

    let pending = store.pending_txn_messages.read().unwrap();
    assert!(
        !pending.contains_key(&(1, 0)),
        "stale buffer should be evicted by the timeout sweep"
    );
    assert!(
        pending.contains_key(&(2, 0)),
        "fresh buffer must be retained"
    );
}

#[test]
fn test_reinit_drops_stale_epoch_pending_buffer() {
    // A producer re-init bumps the epoch; the old-epoch in-RAM
    // forward buffer must be dropped on re-init (commit/abort look it up by the
    // NEW epoch and would otherwise never free it). A different producer's
    // buffer must be left untouched.
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_transactional_producer()
        .times(1)
        .returning(|_, _, _| Ok((100, 1)));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    {
        let mut pending = store.pending_txn_messages.write().unwrap();
        // Stale: producer 100 at the OLD epoch 0.
        pending.insert((100, 0), PendingTxnBuffer::from(vec![buffered_record(1)]));
        // Unrelated producer 200 — must be kept.
        pending.insert((200, 0), PendingTxnBuffer::from(vec![buffered_record(1)]));
    }

    let (pid, epoch) = store
        .get_or_create_transactional_producer("txn-1", 60_000, None)
        .unwrap();
    assert_eq!((pid, epoch), (100, 1));

    let pending = store.pending_txn_messages.read().unwrap();
    assert!(
        !pending.contains_key(&(100, 0)),
        "stale old-epoch buffer should be dropped on re-init"
    );
    assert!(
        pending.contains_key(&(200, 0)),
        "a different producer's buffer must be retained"
    );
}

/// Producer keys at the edges of both halves of the `(producer_id, epoch)`
/// buffer key: distinct ids, distinct epochs of one id, and the extremes.
const BOUNDARY_TXN_KEYS: [TxnKey; 8] = [
    (1000, 0),
    (1000, 1),
    (1001, 0),
    (1000, i16::MAX),
    (1000, i16::MIN),
    (i64::MAX, 0),
    (i64::MIN, 0),
    (0, 0),
];

fn store_with_buffers(mock: MockKafkaStore) -> ShadowStore<MockKafkaStore, InMemoryShadowDb> {
    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    {
        let mut pending = store.pending_txn_messages.write().unwrap();
        for key in BOUNDARY_TXN_KEYS {
            pending.insert(key, PendingTxnBuffer::from(vec![buffered_record(1)]));
        }
    }
    store
}

#[test]
fn test_commit_removes_only_its_own_pending_buffer() {
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(BOUNDARY_TXN_KEYS.len())
        .returning(|_, _, _| Ok(()));
    let store = store_with_buffers(mock);

    for (done, &(producer_id, epoch)) in BOUNDARY_TXN_KEYS.iter().enumerate() {
        store.commit_transaction("txn", producer_id, epoch).unwrap();
        let pending = store.pending_txn_messages.read().unwrap();
        assert!(
            !pending.contains_key(&(producer_id, epoch)),
            "{producer_id}/{epoch}"
        );
        for later in &BOUNDARY_TXN_KEYS[done + 1..] {
            assert!(
                pending.contains_key(later),
                "{later:?} dropped by {producer_id}/{epoch}"
            );
        }
    }
    assert!(store.pending_txn_messages.read().unwrap().is_empty());
}

#[test]
fn test_abort_discards_only_its_own_pending_buffer() {
    let mut mock = MockKafkaStore::new();
    mock.expect_abort_transaction()
        .times(BOUNDARY_TXN_KEYS.len())
        .returning(|_, _, _| Ok(()));
    let store = store_with_buffers(mock);

    for (done, &(producer_id, epoch)) in BOUNDARY_TXN_KEYS.iter().enumerate() {
        store.abort_transaction("txn", producer_id, epoch).unwrap();
        let pending = store.pending_txn_messages.read().unwrap();
        assert!(
            !pending.contains_key(&(producer_id, epoch)),
            "{producer_id}/{epoch}"
        );
        assert_eq!(pending.len(), BOUNDARY_TXN_KEYS.len() - done - 1);
    }
}

#[test]
fn test_metrics_counters_are_safe_to_share_between_threads() {
    let metrics = std::sync::Arc::new(ShadowMetrics::default());
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let m = std::sync::Arc::clone(&metrics);
            std::thread::spawn(move || {
                for _ in 0..100 {
                    m.forwarded.fetch_add(1, Ordering::Relaxed);
                }
                if i % 2 == 0 {
                    m.skipped.fetch_add(3, Ordering::Relaxed);
                } else {
                    m.failed.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }
    assert_eq!(metrics.forwarded.load(Ordering::Relaxed), 1000);
    assert_eq!(metrics.skipped.load(Ordering::Relaxed), 15);
    assert_eq!(metrics.failed.load(Ordering::Relaxed), 5);
}

#[test]
fn test_metrics_flush_resets_the_counters() {
    let store = ShadowStore::with_db(MockKafkaStore::new(), InMemoryShadowDb::default());
    assert_eq!(store.metrics.forwarded.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.skipped.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.failed.load(Ordering::Relaxed), 0);
    store.metrics.forwarded.fetch_add(10, Ordering::Relaxed);
    store.metrics.skipped.fetch_add(5, Ordering::Relaxed);
    store.metrics.failed.fetch_add(2, Ordering::Relaxed);
    assert_eq!(store.metrics.forwarded.load(Ordering::Relaxed), 10);
    assert_eq!(store.metrics.skipped.load(Ordering::Relaxed), 5);
    assert_eq!(store.metrics.failed.load(Ordering::Relaxed), 2);
    store.flush_metrics_to_db().unwrap();
    assert_eq!(store.metrics.forwarded.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.skipped.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.failed.load(Ordering::Relaxed), 0);
}
