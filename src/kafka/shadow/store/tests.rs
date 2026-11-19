// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Unit tests of `ShadowStore`: the delegation, the transaction buffers and the metrics.

use super::*;
use crate::kafka::shadow::config::{ShadowMode, WriteMode};

#[test]
fn test_shadow_store_topic_cache() {
    // Test that topic cache is accessible
    let cache = TopicConfigCache::new();
    let config = super::super::config::TopicShadowConfig {
        topic_id: 1,
        topic_name: "test".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: None,
        sync_mode: super::super::config::SyncMode::Async,
        write_mode: WriteMode::DualWrite,
    };
    cache.update(config);

    let retrieved = cache.get(1);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().topic_name, "test");
}

#[test]
fn test_forward_decision_percentage() {
    // Test that forward decisions work correctly
    let cache = TopicConfigCache::new();
    let config = super::super::config::TopicShadowConfig {
        topic_id: 1,
        topic_name: "test".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 50,
        external_topic_name: None,
        sync_mode: super::super::config::SyncMode::Async,
        write_mode: WriteMode::DualWrite,
    };
    cache.update(config);

    // The decision logic is tested in forwarder module
    // Here we just verify cache integration works
    let retrieved = cache.get(1).unwrap();
    assert!(retrieved.should_forward());
}

#[test]
fn test_txn_key_type() {
    // TxnKey is (producer_id, producer_epoch)
    let key1: TxnKey = (1000, 0);
    let key2: TxnKey = (1000, 1);
    let key3: TxnKey = (1001, 0);

    // Keys with same producer_id but different epoch should differ
    assert_ne!(key1, key2);
    // Keys with different producer_id should differ
    assert_ne!(key1, key3);

    // Test HashMap key behavior
    let mut map: HashMap<TxnKey, Vec<PendingForwardRecord>> = HashMap::new();
    map.insert(key1, vec![]);
    map.insert(key2, vec![]);

    assert_eq!(map.len(), 2);
    assert!(map.contains_key(&(1000, 0)));
    assert!(map.contains_key(&(1000, 1)));
    assert!(!map.contains_key(&(1000, 2)));
}

#[test]
fn test_pending_txn_clear_on_commit() {
    let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));
    let key: TxnKey = (1000, 0);

    // Add records
    {
        let mut guard = pending.write().unwrap();
        guard.insert(
            key,
            vec![PendingForwardRecord {
                topic_id: 1,
                partition_id: 0,
                offset: 0,
                key: None,
            }]
            .into(),
        );
    }

    // Simulate commit by removing
    {
        let mut guard = pending.write().unwrap();
        let removed = guard.remove(&key);
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().records.len(), 1);
    }

    // Verify empty
    let guard = pending.read().unwrap();
    assert!(guard.get(&key).is_none());
}

#[test]
fn test_pending_txn_abort_clears_records() {
    let pending: PendingTxnMessages = Arc::new(RwLock::new(HashMap::new()));
    let key: TxnKey = (1000, 0);

    // Add records
    {
        let mut guard = pending.write().unwrap();
        guard.insert(
            key,
            vec![PendingForwardRecord {
                topic_id: 1,
                partition_id: 0,
                offset: 0,
                key: None,
            }]
            .into(),
        );
    }

    // Simulate abort by removing without forwarding
    {
        let mut guard = pending.write().unwrap();
        guard.remove(&key); // Discard records on abort
    }

    // Verify empty
    assert!(pending.read().unwrap().is_empty());
}

#[test]
fn test_shadow_metrics_initialization() {
    let metrics = ShadowMetrics::default();

    assert_eq!(
        metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        metrics
            .fallback_local
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn test_shadow_metrics_atomic_increment() {
    let metrics = ShadowMetrics::default();

    // Increment counters
    metrics
        .forwarded
        .fetch_add(10, std::sync::atomic::Ordering::Relaxed);
    metrics
        .skipped
        .fetch_add(5, std::sync::atomic::Ordering::Relaxed);
    metrics
        .failed
        .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
    metrics
        .fallback_local
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    assert_eq!(
        metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
        10
    );
    assert_eq!(
        metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
        5
    );
    assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 2);
    assert_eq!(
        metrics
            .fallback_local
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn test_shadow_metrics_thread_safe() {
    use std::sync::atomic::Ordering;
    use std::thread;

    let metrics = Arc::new(ShadowMetrics::default());

    // Spawn multiple threads incrementing counters
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let m = Arc::clone(&metrics);
            thread::spawn(move || {
                for _ in 0..100 {
                    m.forwarded.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(metrics.forwarded.load(Ordering::Relaxed), 1000);
}

// ===== Transaction finalization and ExternalOnly fallback tests =====

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
fn test_shadow_metrics_new() {
    let metrics = ShadowMetrics::new();
    assert_eq!(
        metrics.forwarded.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics.skipped.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(metrics.failed.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(
        metrics
            .fallback_local
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn test_shadow_metrics_snapshot() {
    let metrics = ShadowMetrics::default();

    // Set some values
    metrics
        .forwarded
        .store(100, std::sync::atomic::Ordering::Relaxed);
    metrics
        .skipped
        .store(50, std::sync::atomic::Ordering::Relaxed);
    metrics
        .failed
        .store(5, std::sync::atomic::Ordering::Relaxed);
    metrics
        .fallback_local
        .store(3, std::sync::atomic::Ordering::Relaxed);

    // Get snapshot
    let (forwarded, skipped, failed, fallback) = metrics.snapshot();

    assert_eq!(forwarded, 100);
    assert_eq!(skipped, 50);
    assert_eq!(failed, 5);
    assert_eq!(fallback, 3);
}

#[test]
fn test_shadow_metrics_concurrent_updates() {
    use std::sync::atomic::Ordering;
    use std::thread;

    let metrics = Arc::new(ShadowMetrics::default());

    // Spawn threads for each counter
    let handles: Vec<_> = vec![
        {
            let m = Arc::clone(&metrics);
            thread::spawn(move || {
                for _ in 0..50 {
                    m.forwarded.fetch_add(1, Ordering::Relaxed);
                }
            })
        },
        {
            let m = Arc::clone(&metrics);
            thread::spawn(move || {
                for _ in 0..30 {
                    m.skipped.fetch_add(1, Ordering::Relaxed);
                }
            })
        },
        {
            let m = Arc::clone(&metrics);
            thread::spawn(move || {
                for _ in 0..10 {
                    m.failed.fetch_add(1, Ordering::Relaxed);
                }
            })
        },
        {
            let m = Arc::clone(&metrics);
            thread::spawn(move || {
                for _ in 0..5 {
                    m.fallback_local.fetch_add(1, Ordering::Relaxed);
                }
            })
        },
    ];

    for handle in handles {
        handle.join().unwrap();
    }

    let (forwarded, skipped, failed, fallback) = metrics.snapshot();
    assert_eq!(forwarded, 50);
    assert_eq!(skipped, 30);
    assert_eq!(failed, 10);
    assert_eq!(fallback, 5);
}

#[test]
fn test_topic_config_cache_get_by_name() {
    let cache = TopicConfigCache::new();

    // Insert config
    let config = super::super::config::TopicShadowConfig {
        topic_id: 42,
        topic_name: "my-special-topic".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: Some("external-topic".to_string()),
        sync_mode: super::super::config::SyncMode::Sync,
        write_mode: WriteMode::DualWrite,
    };
    cache.update(config);

    // Get by name
    let retrieved = cache.get_by_name("my-special-topic");
    assert!(retrieved.is_some());
    let cfg = retrieved.unwrap();
    assert_eq!(cfg.topic_id, 42);
    assert_eq!(cfg.external_topic_name, Some("external-topic".to_string()));

    // Non-existent name
    assert!(cache.get_by_name("nonexistent").is_none());
}

#[test]
fn test_topic_config_cache_all() {
    let cache = TopicConfigCache::new();

    // Insert multiple configs
    for i in 1..=5 {
        let config = super::super::config::TopicShadowConfig {
            topic_id: i,
            topic_name: format!("topic-{}", i),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: super::super::config::SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);
    }

    let all = cache.all();
    assert_eq!(all.len(), 5);

    // Verify all topics present
    let names: Vec<String> = all.iter().map(|c| c.topic_name.clone()).collect();
    for i in 1..=5 {
        assert!(names.contains(&format!("topic-{}", i)));
    }
}

#[test]
fn test_topic_config_cache_clear() {
    let cache = TopicConfigCache::new();

    // Add some configs
    for i in 1..=3 {
        let config = super::super::config::TopicShadowConfig {
            topic_id: i,
            topic_name: format!("topic-{}", i),
            mode: ShadowMode::Shadow,
            forward_percentage: 100,
            external_topic_name: None,
            sync_mode: super::super::config::SyncMode::Async,
            write_mode: WriteMode::DualWrite,
        };
        cache.update(config);
    }

    assert_eq!(cache.all().len(), 3);

    // Clear cache
    cache.clear();

    assert_eq!(cache.all().len(), 0);
    assert!(cache.get(1).is_none());
}

#[test]
fn test_topic_config_cache_update_existing() {
    let cache = TopicConfigCache::new();

    // Initial config
    let config1 = super::super::config::TopicShadowConfig {
        topic_id: 1,
        topic_name: "topic".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 50,
        external_topic_name: None,
        sync_mode: super::super::config::SyncMode::Async,
        write_mode: WriteMode::DualWrite,
    };
    cache.update(config1);

    // Verify initial
    assert_eq!(cache.get(1).unwrap().forward_percentage, 50);

    // Update same topic
    let config2 = super::super::config::TopicShadowConfig {
        topic_id: 1,
        topic_name: "topic".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: Some("external".to_string()),
        sync_mode: super::super::config::SyncMode::Sync,
        write_mode: WriteMode::ExternalOnly,
    };
    cache.update(config2);

    // Verify updated
    let updated = cache.get(1).unwrap();
    assert_eq!(updated.forward_percentage, 100);
    assert_eq!(updated.external_topic_name, Some("external".to_string()));
    assert!(matches!(
        updated.sync_mode,
        super::super::config::SyncMode::Sync
    ));
    assert!(matches!(updated.write_mode, WriteMode::ExternalOnly));
}

#[test]
fn test_txn_key_epoch_rollover() {
    // Test epoch handling at boundaries
    let key_max_epoch: TxnKey = (1000, i16::MAX);
    let key_min_epoch: TxnKey = (1000, i16::MIN);
    let key_zero_epoch: TxnKey = (1000, 0);

    assert_ne!(key_max_epoch, key_min_epoch);
    assert_ne!(key_max_epoch, key_zero_epoch);
    assert_ne!(key_min_epoch, key_zero_epoch);

    // Verify HashMap handles edge cases
    let mut map: HashMap<TxnKey, String> = HashMap::new();
    map.insert(key_max_epoch, "max".to_string());
    map.insert(key_min_epoch, "min".to_string());
    map.insert(key_zero_epoch, "zero".to_string());

    assert_eq!(map.len(), 3);
    assert_eq!(map.get(&key_max_epoch), Some(&"max".to_string()));
    assert_eq!(map.get(&key_min_epoch), Some(&"min".to_string()));
    assert_eq!(map.get(&key_zero_epoch), Some(&"zero".to_string()));
}

#[test]
fn test_txn_key_producer_id_boundaries() {
    // Test producer_id at boundaries
    let key_max_pid: TxnKey = (i64::MAX, 0);
    let key_min_pid: TxnKey = (i64::MIN, 0);
    let key_zero_pid: TxnKey = (0, 0);

    assert_ne!(key_max_pid, key_min_pid);
    assert_ne!(key_max_pid, key_zero_pid);

    let mut map: HashMap<TxnKey, String> = HashMap::new();
    map.insert(key_max_pid, "max".to_string());
    map.insert(key_min_pid, "min".to_string());

    assert_eq!(map.len(), 2);
}
