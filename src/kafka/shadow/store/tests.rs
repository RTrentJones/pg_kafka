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

fn external_only_record(topic_id: i32) -> PendingForwardRecord {
    PendingForwardRecord {
        topic_id,
        partition_id: 0,
        offset: -1,
        key: None,
    }
}

#[test]
fn test_abort_finalizes_inner_txn_with_only_external_only_records() {
    // Regression: aborting a transaction with no locally-written records
    // used to skip the inner abort, leaving the transaction Ongoing
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
            .push(external_only_record(1));
    }

    store.abort_transaction("txn-1", 100, 0).unwrap();
}

#[test]
fn test_commit_finalizes_inner_txn() {
    // The inner commit must run unconditionally (state row → committed,
    // pending offsets applied) even though forwarding is now deferred to the
    // durable outbox (RA-1/RA-2) rather than done inline. In test builds the
    // outbox enqueue is a no-op stub, so this asserts the inner commit fires.
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
            .push(external_only_record(1));
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
    // SH-4: the storage sweeper force-aborts timed-out transactions without
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
        let mut stale = PendingTxnBuffer::from(vec![external_only_record(1)]);
        stale.first_buffered = Instant::now()
            .checked_sub(Duration::from_secs(120))
            .unwrap_or_else(Instant::now);
        pending.insert((1, 0), stale);
        // Fresh: just buffered, well within the timeout.
        pending.insert(
            (2, 0),
            PendingTxnBuffer::from(vec![external_only_record(1)]),
        );
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
    // RV-2 residual: a producer re-init bumps the epoch; the old-epoch in-RAM
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
        pending.insert(
            (100, 0),
            PendingTxnBuffer::from(vec![external_only_record(1)]),
        );
        // Unrelated producer 200 — must be kept.
        pending.insert(
            (200, 0),
            PendingTxnBuffer::from(vec![external_only_record(1)]),
        );
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

#[test]
fn test_metrics_flush_resets_the_counters() {
    let store = ShadowStore::with_db(MockKafkaStore::new(), InMemoryShadowDb::default());
    store.metrics.forwarded.fetch_add(10, Ordering::Relaxed);
    store.metrics.skipped.fetch_add(5, Ordering::Relaxed);
    store.metrics.failed.fetch_add(2, Ordering::Relaxed);
    store.flush_metrics_to_db().unwrap();
    assert_eq!(store.metrics.forwarded.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.skipped.load(Ordering::Relaxed), 0);
    assert_eq!(store.metrics.failed.load(Ordering::Relaxed), 0);
}
