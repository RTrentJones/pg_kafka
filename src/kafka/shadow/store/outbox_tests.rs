// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! Unit tests of the outbox logic against the in-memory `ShadowDb`.

use super::*;
use crate::kafka::shadow::config::ShadowMode;
use crate::kafka::shadow::outbox::fake::InMemoryShadowDb;
use crate::testing::mocks::MockKafkaStore;

const T: i32 = 7;

type Store = ShadowStore<MockKafkaStore, InMemoryShadowDb>;

fn topic_config(sync_mode: SyncMode, write_mode: WriteMode) -> TopicShadowConfig {
    TopicShadowConfig {
        topic_id: T,
        topic_name: "local".to_string(),
        mode: ShadowMode::Shadow,
        forward_percentage: 100,
        external_topic_name: Some("ext".to_string()),
        sync_mode,
        write_mode,
    }
}

struct Harness {
    store: Store,
    forwards: crossbeam_channel::Receiver<ForwardRequest>,
    acks: crossbeam_channel::Sender<ForwardAck>,
}

/// A store with shadow mode enabled, `configs` loaded into its cache, and
/// forward/ack channels (forward capacity `cap`).
fn harness(mock: MockKafkaStore, configs: Vec<TopicShadowConfig>, cap: usize) -> Harness {
    let db = InMemoryShadowDb::enabled();
    db.add_topic(T, "local", Some("ext"));
    db.state().configs = Some(configs);
    let store = ShadowStore::with_db(mock, db);
    store.load_topic_config_from_db().unwrap();
    let (forward_tx, forwards) = crossbeam_channel::bounded(cap);
    let (acks, ack_rx) = crossbeam_channel::unbounded();
    store.set_forward_channel(forward_tx);
    store.set_ack_channel(ack_rx);
    Harness {
        store,
        forwards,
        acks,
    }
}

fn record(key: &[u8], value: &[u8]) -> Record {
    Record {
        key: Some(key.to_vec()),
        value: Some(value.to_vec()),
        headers: vec![],
        timestamp: None,
    }
}

fn ok_ack(key: RowKey, external_offset: i64) -> ForwardAck {
    ForwardAck::new(key, ForwardOutcome::Delivered(external_offset))
}

fn err_ack(key: RowKey) -> ForwardAck {
    ForwardAck::new(key, ForwardOutcome::Failed("broker down".to_string()))
}

fn skipped_ack(key: RowKey) -> ForwardAck {
    ForwardAck::new(key, ForwardOutcome::NotAttempted("fenced".to_string()))
}

/// Seed messages and pending outbox rows for `keys`.
fn seed_rows(store: &Store, keys: &[RowKey]) {
    for &k in keys {
        store
            .db()
            .add_message(k, b"k", format!("v{}", k.local_offset).as_bytes());
    }
    store.db().insert_outbox_rows(keys).unwrap();
}

fn async_harness(cap: usize) -> Harness {
    harness(
        MockKafkaStore::new(),
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        cap,
    )
}

/// The records the poll sent, in order.
fn sent_records(h: &Harness) -> Vec<ForwardRecord> {
    h.forwards
        .try_iter()
        .filter_map(|m| match m {
            ForwardRequest::Record(r) => Some(r),
            ForwardRequest::Hold(_) => None,
        })
        .collect()
}

fn offsets(records: &[ForwardRecord]) -> Vec<i64> {
    records.iter().map(|r| r.row.local_offset).collect()
}

#[test]
fn produce_writes_one_pending_outbox_row_per_record() {
    let mut mock = MockKafkaStore::new();
    mock.expect_insert_records().returning(|_, _, _| Ok(10));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    h.store.db().add_message(RowKey::new(T, 0, 10), b"a", b"1");
    h.store.db().add_message(RowKey::new(T, 0, 11), b"b", b"2");

    let base = h
        .store
        .insert_records(T, 0, &[record(b"a", b"1"), record(b"b", b"2")])
        .unwrap();

    assert_eq!(base, 10);
    for off in [10, 11] {
        let row = h
            .store
            .db()
            .row(RowKey::new(T, 0, off))
            .expect("outbox row");
        assert_eq!(row.retry_count, 0);
        assert_eq!(row.external_offset, None);
        assert_eq!(row.forwarded_at, None);
        assert!(!row.attempt_open);
    }
    // Async: nothing is sent inline; the poll forwards.
    assert!(h.forwards.try_recv().is_err());
}

#[test]
fn produce_with_shadow_disabled_writes_no_outbox_row() {
    let mut mock = MockKafkaStore::new();
    mock.expect_insert_records().returning(|_, _, _| Ok(0));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    h.store.db().state().enabled = false;
    h.store.db().add_message(RowKey::new(T, 0, 0), b"a", b"1");

    h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();

    assert!(h.store.db().state().tracking.is_empty());
}

#[test]
fn outbox_row_is_only_written_for_an_existing_message() {
    let h = async_harness(16);
    h.store.db().add_message(RowKey::new(T, 0, 1), b"a", b"1");

    h.store
        .db()
        .insert_outbox_rows(&[RowKey::new(T, 0, 1), RowKey::new(T, 0, 2)])
        .unwrap();

    assert!(h.store.db().row(RowKey::new(T, 0, 1)).is_some());
    assert!(h.store.db().row(RowKey::new(T, 0, 2)).is_none());
}

#[test]
fn duplicate_success_ack_counts_once() {
    let h = async_harness(16);
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    h.store.poll_and_forward_outbox().unwrap();

    sent_records(&h);
    h.acks.send(ok_ack(RowKey::new(T, 0, 1), 9)).unwrap();
    h.acks.send(ok_ack(RowKey::new(T, 0, 1), 10)).unwrap();
    assert_eq!(h.store.drain_forward_acks(), 2);

    assert_eq!(
        h.store
            .db()
            .row(RowKey::new(T, 0, 1))
            .unwrap()
            .external_offset,
        Some(9)
    );
    let m = h.store.db().state().metrics[&(T, 0)].clone();
    assert_eq!(m.messages_forwarded, 1);
    assert_eq!(m.last_forwarded_offset, Some(1));
    assert_eq!(h.store.metrics().forwarded.load(Ordering::Relaxed), 1);
}

#[test]
fn poll_claim_failure_is_a_database_error() {
    let h = async_harness(16);
    h.store.db().state().fail_claim = true;
    assert!(matches!(
        h.store.poll_and_forward_outbox(),
        Err(super::super::error::ShadowError::DatabaseError(_))
    ));
}

#[test]
fn commit_enqueues_the_transactions_records_read_from_messages() {
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    for off in [5, 6] {
        let mut s = h.store.db().state();
        s.messages.insert(
            RowKey::new(T, 0, off),
            crate::kafka::shadow::outbox::fake::FakeMessage {
                txn: Some((100, 0)),
                txn_pending: true,
                ..Default::default()
            },
        );
    }
    // Another producer's pending record is not part of this commit.
    h.store.db().state().messages.insert(
        RowKey::new(T, 0, 7),
        crate::kafka::shadow::outbox::fake::FakeMessage {
            txn: Some((200, 0)),
            txn_pending: true,
            ..Default::default()
        },
    );

    // No RAM buffer (e.g. after a worker restart): the DB read still finds them.
    h.store.commit_transaction("txn", 100, 0).unwrap();

    assert!(h.store.db().row(RowKey::new(T, 0, 5)).is_some());
    assert!(h.store.db().row(RowKey::new(T, 0, 6)).is_some());
    assert!(h.store.db().row(RowKey::new(T, 0, 7)).is_none());
}

#[test]
fn commit_falls_back_to_the_ram_buffer_when_the_lookup_fails() {
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    h.store.db().add_message(RowKey::new(T, 0, 5), b"k", b"v");
    h.store.db().state().fail_pending_txn_lookup = true;
    h.store.pending_txn_messages.write().unwrap().insert(
        (100, 0),
        PendingTxnBuffer::from(vec![PendingForwardRecord {
            row: RowKey::new(T, 0, 5),
            key: None,
        }]),
    );

    h.store.commit_transaction("txn", 100, 0).unwrap();

    assert!(h.store.db().row(RowKey::new(T, 0, 5)).is_some());
}

#[test]
fn commit_with_shadow_disabled_enqueues_nothing() {
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    h.store.db().state().messages.insert(
        RowKey::new(T, 0, 5),
        crate::kafka::shadow::outbox::fake::FakeMessage {
            txn: Some((100, 0)),
            txn_pending: true,
            ..Default::default()
        },
    );
    h.store.db().state().enabled = false;

    h.store.commit_transaction("txn", 100, 0).unwrap();

    assert!(h.store.db().state().tracking.is_empty());
}

#[test]
fn config_load_replaces_cache_and_forces_full_forwarding_for_external_primary() {
    let mut external = topic_config(SyncMode::Async, WriteMode::ExternalOnly);
    external.forward_percentage = 30;
    let mut other = topic_config(SyncMode::Async, WriteMode::DualWrite);
    other.topic_id = T + 1;
    other.forward_percentage = 30;
    let h = harness(MockKafkaStore::new(), vec![external, other], 16);

    assert_eq!(
        h.store.topic_cache().get(T).unwrap().forward_percentage,
        100
    );
    assert_eq!(
        h.store.topic_cache().get(T + 1).unwrap().forward_percentage,
        30
    );

    // A deleted row disappears from the cache on the next load.
    h.store.db().state().configs = Some(vec![topic_config(SyncMode::Async, WriteMode::DualWrite)]);
    assert_eq!(h.store.load_topic_config_from_db().unwrap(), 1);
    assert!(h.store.topic_cache().get(T + 1).is_none());

    // A failed query keeps the previous cache.
    h.store.db().state().configs = None;
    assert!(h.store.load_topic_config_from_db().is_err());
    assert!(h.store.topic_cache().get(T).is_some());
}

#[test]
fn poll_claims_due_rows_in_order_with_payload_headers_and_timestamp() {
    let h = async_harness(16);
    seed_rows(&h.store, &[RowKey::new(T, 0, 2), RowKey::new(T, 0, 1)]);
    {
        let mut s = h.store.db().state();
        let m = s.messages.get_mut(&RowKey::new(T, 0, 1)).unwrap();
        m.headers = vec![("h".to_string(), b"x".to_vec())];
        m.timestamp_ms = Some(1234);
    }

    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);

    let sent = sent_records(&h);
    assert_eq!(offsets(&sent), vec![1, 2]);
    assert_eq!(sent[0].topic_name, "ext");
    assert_eq!(sent[0].value.as_deref(), Some(&b"v1"[..]));
    assert_eq!(sent[0].headers, vec![("h".to_string(), b"x".to_vec())]);
    assert_eq!(sent[0].timestamp_ms, Some(1234));
    assert!(sent[0].restart, "lowest pending row of the partition");
    assert!(!sent[1].restart);
    for off in [1, 2] {
        let row = h.store.db().row(RowKey::new(T, 0, off)).unwrap();
        assert_eq!(row.retry_count, 0, "a claim is not a failure");
        assert!(row.attempt_open);
        assert_eq!(row.forwarded_at, Some(0));
    }
}

#[test]
fn in_flight_rows_are_never_reclaimed_and_only_real_failures_count() {
    let h = async_harness(16);
    seed_rows(&h.store, &[RowKey::new(T, 0, 1), RowKey::new(T, 1, 2)]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    sent_records(&h);

    // Still in flight, however long: excluded from the claim.
    h.store.db().advance_ms(60_000);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);

    // Row (T,0,1) fails, (T,1,2) is delivered.
    h.acks.send(err_ack(RowKey::new(T, 0, 1))).unwrap();
    h.acks.send(ok_ack(RowKey::new(T, 1, 2), 500)).unwrap();
    assert_eq!(h.store.drain_forward_acks(), 2);
    let failed = h.store.db().row(RowKey::new(T, 0, 1)).unwrap();
    assert_eq!(failed.error_message.as_deref(), Some("broker down"));
    assert_eq!(failed.retry_count, 1);
    assert!(!failed.attempt_open);
    assert_eq!(
        h.store
            .db()
            .row(RowKey::new(T, 1, 2))
            .unwrap()
            .external_offset,
        Some(500)
    );

    // Backing off (100 ms base): not due yet, then due.
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    h.store.db().advance_ms(101);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert_eq!(offsets(&sent_records(&h)), vec![1]);
    assert_eq!(
        h.store.db().row(RowKey::new(T, 0, 1)).unwrap().retry_count,
        1
    );
}

#[test]
fn a_backing_off_row_blocks_later_rows_of_its_partition_only() {
    let h = async_harness(16);
    seed_rows(
        &h.store,
        &[
            RowKey::new(T, 0, 1),
            RowKey::new(T, 0, 2),
            RowKey::new(T, 0, 3),
            RowKey::new(T, 1, 1),
        ],
    );
    h.store.poll_and_forward_outbox().unwrap();
    sent_records(&h);
    // 1 fails, 2 and 3 are fenced by the forwarder, partition 1 succeeds.
    for ack in [
        err_ack(RowKey::new(T, 0, 1)),
        skipped_ack(RowKey::new(T, 0, 2)),
        skipped_ack(RowKey::new(T, 0, 3)),
        ok_ack(RowKey::new(T, 1, 1), 7),
    ] {
        h.acks.send(ack).unwrap();
    }
    h.store.drain_forward_acks();
    // The fenced rows were not attempted: no retry counted.
    assert_eq!(
        h.store.db().row(RowKey::new(T, 0, 2)).unwrap().retry_count,
        0
    );

    // While 1 backs off, 2 and 3 must not overtake it.
    seed_rows(&h.store, &[RowKey::new(T, 1, 2)]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert_eq!(sent_records(&h)[0].row, RowKey::new(T, 1, 2));

    // After the backoff the partition resumes from 1, in order.
    h.store.db().advance_ms(101);
    h.store.poll_and_forward_outbox().unwrap();
    let sent = sent_records(&h);
    assert_eq!(offsets(&sent), vec![1, 2, 3]);
    assert!(sent[0].restart);
}

#[test]
fn a_lost_attempt_counts_as_a_failure() {
    // The attempt stays open in the table but this worker has no in-flight
    // entry for it (worker restart): claimed again, counting one failure.
    let h = async_harness(16);
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    h.store
        .db()
        .state()
        .tracking
        .get_mut(&RowKey::new(T, 0, 1))
        .unwrap()
        .attempt_open = true;
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert_eq!(
        h.store.db().row(RowKey::new(T, 0, 1)).unwrap().retry_count,
        1
    );
}

#[test]
fn failing_forward_is_dead_lettered_and_external_primary_reads_fall_back() {
    let mut mock = MockKafkaStore::new();
    // Called exactly once: only after the dead letter lifts suppression.
    mock.expect_fetch_records()
        .times(1)
        .returning(|_, _, _, _| Ok(vec![]));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::ExternalOnly)],
        16,
    );
    let cap = 4;
    h.store.db().state().settings.max_retries = cap;
    seed_rows(&h.store, &[RowKey::new(T, 0, 1), RowKey::new(T, 0, 2)]);

    // Healthy external-primary topic: local reads suppressed.
    assert!(h.store.fetch_records(T, 0, 0, 1024).unwrap().is_empty());

    for attempt in 1..=cap {
        h.store.poll_and_forward_outbox().unwrap();
        let sent = sent_records(&h);
        assert_eq!(sent[0].row.local_offset, 1, "attempt {attempt}");
        h.acks.send(err_ack(RowKey::new(T, 0, 1))).unwrap();
        for r in &sent[1..] {
            h.acks.send(skipped_ack(r.row)).unwrap();
        }
        h.store.drain_forward_acks();
        h.store.db().advance_ms(OUTBOX_MAX_BACKOFF_MS + 1);
    }

    // At the cap the row is a dead letter: never claimed again, kept pending;
    // the next row is now the head of the partition (restart).
    h.store.poll_and_forward_outbox().unwrap();
    let sent = sent_records(&h);
    assert_eq!(offsets(&sent), vec![2]);
    assert!(sent[0].restart);
    let row = h.store.db().row(RowKey::new(T, 0, 1)).unwrap();
    assert_eq!(row.retry_count, cap);
    assert_eq!(row.external_offset, None);
    assert_eq!(
        h.store.db().row(RowKey::new(T, 0, 2)).unwrap().retry_count,
        0
    );
    assert_eq!(h.store.db().state().metrics[&(T, 0)].messages_failed, cap);
    assert!(h.store.topic_has_dead_letters(T));

    // The dead letter lifts read suppression (inner is now called) ...
    h.store.fetch_records(T, 0, 0, 1024).unwrap();
    // ... unless strict mode keeps it.
    h.store.db().state().strict = true;
    assert!(h.store.fetch_records(T, 0, 0, 1024).unwrap().is_empty());
}

#[test]
fn raising_the_retry_cap_revives_dead_letters() {
    let h = async_harness(16);
    h.store.db().state().settings.max_retries = 1;
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    h.store.poll_and_forward_outbox().unwrap();
    h.acks.send(err_ack(RowKey::new(T, 0, 1))).unwrap();
    h.store.drain_forward_acks();
    h.store.db().advance_ms(OUTBOX_MAX_BACKOFF_MS + 1);
    sent_records(&h);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);

    h.store.db().state().settings.max_retries = 2;
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
}

#[test]
fn batch_size_bounds_the_claim_and_requests_another_poll() {
    let h = async_harness(64);
    h.store.db().state().settings.batch_size = 2;
    seed_rows(
        &h.store,
        &[
            RowKey::new(T, 0, 1),
            RowKey::new(T, 0, 2),
            RowKey::new(T, 0, 3),
        ],
    );

    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    assert!(
        h.store.outbox_tick_due(Duration::ZERO),
        "full claim: poll again"
    );
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert!(!h.store.outbox_tick_due(Duration::ZERO));
    assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
}

#[test]
fn full_forward_channel_closes_the_attempt_without_counting_it() {
    let h = async_harness(1);
    seed_rows(
        &h.store,
        &[
            RowKey::new(T, 0, 1),
            RowKey::new(T, 0, 2),
            RowKey::new(T, 0, 3),
        ],
    );

    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    let live = h.store.inflight.live_keys();
    assert_eq!(
        live.topic_ids.len(),
        1,
        "undelivered rows must not stay in flight"
    );
    assert_eq!(live.local_offsets, vec![1]);
    for off in [2, 3] {
        let row = h.store.db().row(RowKey::new(T, 0, off)).unwrap();
        assert!(!row.attempt_open);
        assert_eq!(row.retry_count, 0);
    }

    // They are due again at once.
    sent_records(&h);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert_eq!(
        h.store.db().row(RowKey::new(T, 0, 2)).unwrap().retry_count,
        0
    );
}

#[test]
fn poll_without_forward_channel_is_a_no_op() {
    let db = InMemoryShadowDb::enabled();
    db.add_topic(T, "local", None);
    let store = ShadowStore::with_db(MockKafkaStore::new(), db);
    seed_rows(&store, &[RowKey::new(T, 0, 1)]);

    assert_eq!(store.poll_and_forward_outbox().unwrap(), 0);
    assert!(!store.db().row(RowKey::new(T, 0, 1)).unwrap().attempt_open);
}

#[test]
fn skipped_records_are_counted_in_shadow_metrics() {
    let mut mock = MockKafkaStore::new();
    mock.expect_insert_records().returning(|_, _, _| Ok(0));
    let mut cfg = topic_config(SyncMode::Async, WriteMode::DualWrite);
    cfg.forward_percentage = 50;
    let h = harness(mock, vec![cfg.clone()], 16);
    let records: Vec<Record> = (0..20u8).map(|i| record(&[i], b"v")).collect();
    let expected = (0..20u8)
        .filter(|&i| make_forward_decision(Some(&[i]), i64::from(i), 50) == ForwardDecision::Skip)
        .count();
    let expected = i64::try_from(expected).unwrap();
    assert!(expected > 0);
    h.store.insert_records(T, 0, &records).unwrap();
    assert_eq!(
        h.store.db().state().metrics[&(T, 0)].messages_skipped,
        expected
    );

    // shadow_metrics_enabled = off: the table is not written.
    h.store.db().state().settings.metrics_enabled = false;
    h.store.insert_records(T, 0, &records).unwrap();
    assert_eq!(
        h.store.db().state().metrics[&(T, 0)].messages_skipped,
        expected
    );
}

#[test]
fn metrics_disabled_still_finalizes_rows() {
    let h = async_harness(16);
    h.store.db().state().settings.metrics_enabled = false;
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    h.store.poll_and_forward_outbox().unwrap();
    h.acks.send(ok_ack(RowKey::new(T, 0, 1), 3)).unwrap();
    h.store.drain_forward_acks();
    assert_eq!(
        h.store
            .db()
            .row(RowKey::new(T, 0, 1))
            .unwrap()
            .external_offset,
        Some(3)
    );
    assert!(!h.store.db().state().metrics.contains_key(&(T, 0)));
}

#[test]
fn outbox_failures_back_off_and_log_sparsely() {
    let h = async_harness(16);
    assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
    assert!(h.store.note_outbox_tick(false), "first failure is logged");
    assert!(!h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL), "paused");
    assert!(h.store.note_outbox_tick(false), "2nd");
    assert!(!h.store.note_outbox_tick(false), "3rd is not logged");
    assert!(h.store.note_outbox_tick(false), "4th");
    // Success resets.
    assert!(!h.store.note_outbox_tick(true));
    assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
}

fn sync_harness() -> Harness {
    let mut mock = MockKafkaStore::new();
    mock.expect_insert_records().returning(|_, _, _| Ok(10));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Sync, WriteMode::DualWrite)],
        16,
    );
    h.store.db().add_message(RowKey::new(T, 0, 10), b"a", b"1");
    h.store.db().state().current_xid = 77;
    h
}

#[test]
fn sync_produce_forwards_nothing_before_commit() {
    let h = sync_harness();

    h.store.begin_request();
    h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();

    // The outbox row is written in the request's transaction, but nothing is
    // sent while that transaction can still roll back.
    assert!(h.store.db().row(RowKey::new(T, 0, 10)).is_some());
    assert!(h.forwards.try_recv().is_err());
    assert_eq!(h.store.sync_pending.lock().unwrap().len(), 1);
}

#[test]
fn committed_sync_rows_are_reported_per_request_and_request_a_poll() {
    let h = sync_harness();
    let slot = h.store.begin_request();
    h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
    let other = h.store.begin_request();
    assert_ne!(slot, other);
    h.store.db().state().committed_xids.insert(77);

    let holds = h.store.take_sync_holds();
    assert_eq!(holds.get(&slot), Some(&vec![RowKey::new(T, 0, 10)]));
    assert!(!holds.contains_key(&other));
    assert!(h.store.sync_pending.lock().unwrap().is_empty());
    assert!(h.store.outbox_tick_due(Duration::ZERO));
    // The DB thread did not wait for anything; the poll sends the row.
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
}

#[test]
fn rolled_back_sync_rows_are_not_held() {
    let h = sync_harness();
    h.store.begin_request();
    h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
    // xid 77 never committed (the request's subtransaction rolled back).
    assert!(h.store.take_sync_holds().is_empty());
    assert!(!h.store.outbox_tick_due(Duration::ZERO));
}

#[test]
fn sync_timeout_zero_holds_nothing() {
    let h = sync_harness();
    h.store.db().state().settings.sync_timeout_ms = 0;
    h.store.begin_request();
    h.store.insert_records(T, 0, &[record(b"a", b"1")]).unwrap();
    h.store.db().state().committed_xids.insert(77);
    assert!(h.store.take_sync_holds().is_empty());
}

#[test]
fn hold_goes_to_the_forwarder_or_is_released_at_once() {
    use std::sync::atomic::AtomicUsize;
    let h = sync_harness();
    let released = std::sync::Arc::new(AtomicUsize::new(0));
    let r = released.clone();
    h.store.hold_response(
        vec![RowKey::new(T, 0, 10)],
        HeldRelease::new(move || {
            r.fetch_add(1, Ordering::SeqCst);
        }),
    );
    match h.forwards.try_recv().unwrap() {
        ForwardRequest::Hold(hold) => {
            assert_eq!(hold.keys, vec![RowKey::new(T, 0, 10)]);
            assert_eq!(hold.timeout, Duration::from_millis(5_000));
            assert_eq!(released.load(Ordering::SeqCst), 0);
            hold.release.release();
        }
        ForwardRequest::Record(record) => panic!("expected a hold, got {record:?}"),
    }
    assert_eq!(released.load(Ordering::SeqCst), 1);

    // No forwarder: released immediately (the client is never left hanging).
    let db = InMemoryShadowDb::enabled();
    let store = ShadowStore::with_db(MockKafkaStore::new(), db);
    let r = released.clone();
    store.hold_response(
        vec![RowKey::new(T, 0, 10)],
        HeldRelease::new(move || {
            r.fetch_add(1, Ordering::SeqCst);
        }),
    );
    assert_eq!(released.load(Ordering::SeqCst), 2);
}
