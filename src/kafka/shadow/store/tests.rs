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

#[test]
fn test_abort_finalizes_inner_txn_and_enqueues_nothing() {
    // The inner abort always runs, or the transaction stays Ongoing; nothing was
    // enqueued for the open transaction, so there is nothing to undo.
    let mut mock = MockKafkaStore::new();
    mock.expect_abort_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::enabled());
    store.abort_transaction("txn-1", 100, 0).unwrap();
    assert!(store.db().state().tracking.is_empty());
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
    store.commit_transaction("txn-1", 100, 0).unwrap();
}

#[test]
fn test_transactional_writes_and_reinit_are_delegated_without_state() {
    // A transactional produce and a re-init reach the wrapped store unchanged and
    // keep nothing in the shadow store: the commit finds the records in
    // kafka.messages.
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_transactional_producer()
        .times(1)
        .returning(|_, _, _| Ok((100, 1)));
    mock.expect_insert_transactional_records()
        .withf(|t, p, records, pid, epoch| {
            (*t, *p, records.len(), *pid, *epoch) == (1, 0, 1, 100, 1)
        })
        .times(1)
        .returning(|_, _, _, _, _| Ok(7));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::enabled());
    assert_eq!(
        store
            .get_or_create_transactional_producer("txn-1", 60_000, None)
            .unwrap(),
        (100, 1)
    );
    let record = Record {
        key: None,
        value: Some(b"v".to_vec()),
        headers: vec![],
        timestamp: None,
    };
    assert_eq!(
        store
            .insert_transactional_records(1, 0, &[record], 100, 1)
            .unwrap(),
        7
    );
    assert!(store.db().state().tracking.is_empty());
}

#[test]
fn test_maintenance_is_delegated_to_the_inner_store() {
    let mut mock = MockKafkaStore::new();
    mock.expect_abort_timed_out_transactions()
        .withf(|t| *t == Duration::from_secs(60))
        .times(1)
        .returning(|_| Ok(vec!["abandoned".to_string()]));
    mock.expect_cleanup_aborted_messages()
        .times(1)
        .returning(|_| Ok(3));

    let store = ShadowStore::with_db(mock, InMemoryShadowDb::default());
    assert_eq!(
        store
            .abort_timed_out_transactions(Duration::from_secs(60))
            .unwrap(),
        vec!["abandoned".to_string()]
    );
    assert_eq!(
        store
            .cleanup_aborted_messages(Duration::from_secs(1))
            .unwrap(),
        3
    );
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
