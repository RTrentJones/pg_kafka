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
use crate::kafka::shadow::outbox::per_partition_claim_cap;
use crate::kafka::shadow::ForwardRecord;
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

/// One ack application whose transaction committed (the worker's tick).
fn drain(h: &Harness) -> usize {
    let applied = h.store.drain_forward_acks();
    h.store.commit_applied_acks();
    applied
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
            ForwardRequest::Hold(_) | ForwardRequest::Reset(_) => None,
        })
        .collect()
}

fn offsets(records: &[ForwardRecord]) -> Vec<i64> {
    records.iter().map(|r| r.row.local_offset).collect()
}

/// Whether the next outbox tick is requested at once (the worker cuts its
/// receive wait short).
fn requested(h: &Harness) -> bool {
    h.store.outbox_requested_wait(Duration::ZERO).is_some()
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
fn acks_from_a_dropped_schema_do_not_touch_its_successors_rows() {
    let h = async_harness(16);
    let delivered = RowKey::new(T, 0, 0);
    let failed = RowKey::new(T, 0, 1);
    seed_rows(&h.store, &[delivered, failed]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    sent_records(&h);

    // DROP + CREATE EXTENSION: the new schema's first rows reuse the keys.
    {
        let mut state = h.store.db().state();
        state.tracking.clear();
        state.messages.clear();
    }
    h.store.forget_dropped_schema();
    seed_rows(&h.store, &[delivered, failed]);

    // The forwarder is told first, then the new rows go out: their keys are
    // not in flight any more.
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    let sent: Vec<ForwardRequest> = h.forwards.try_iter().collect();
    assert!(
        matches!(sent.first(), Some(ForwardRequest::Reset(1))),
        "{sent:?}"
    );
    assert_eq!(sent.len(), 3);

    // Late acks of the old forwards (epoch 0) are ignored.
    h.acks.send(ok_ack(delivered, 9)).unwrap();
    h.acks.send(err_ack(failed)).unwrap();
    assert_eq!(drain(&h), 0);
    let row = h.store.db().row(delivered).unwrap();
    assert_eq!(row.external_offset, None);
    assert_eq!(h.store.db().row(failed).unwrap().retry_count, 0);

    // The new forwards' own acks are applied.
    let mut ack = ok_ack(delivered, 3);
    ack.epoch = 1;
    h.acks.send(ack).unwrap();
    assert_eq!(drain(&h), 1);
    assert_eq!(
        h.store.db().row(delivered).unwrap().external_offset,
        Some(3)
    );
}

#[test]
fn a_reset_the_forward_channel_refused_goes_out_before_any_record() {
    let h = async_harness(1);
    seed_rows(&h.store, &[RowKey::new(T, 0, 0)]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    // The channel is full: the reset waits, and so does every record.
    h.store.forget_dropped_schema();
    {
        let mut state = h.store.db().state();
        state.tracking.clear();
        state.messages.clear();
    }
    seed_rows(&h.store, &[RowKey::new(T, 0, 0)]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    assert!(matches!(
        h.forwards.try_recv(),
        Ok(ForwardRequest::Record(_))
    ));
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    assert!(matches!(
        h.forwards.try_recv(),
        Ok(ForwardRequest::Reset(1))
    ));
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert!(matches!(
        h.forwards.try_recv(),
        Ok(ForwardRequest::Record(_))
    ));
}

#[test]
fn duplicate_success_ack_counts_once() {
    let h = async_harness(16);
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    h.store.poll_and_forward_outbox().unwrap();

    sent_records(&h);
    h.acks.send(ok_ack(RowKey::new(T, 0, 1), 9)).unwrap();
    h.acks.send(ok_ack(RowKey::new(T, 0, 1), 10)).unwrap();
    assert_eq!(drain(&h), 2);

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

/// A pending record of producer 100's transaction (epoch 0) on `topic_id`.
fn pending_txn_message(h: &Harness, row: RowKey, key: Option<&[u8]>) {
    h.store.db().state().messages.insert(
        row,
        crate::kafka::shadow::outbox::fake::FakeMessage {
            key: key.map(<[u8]>::to_vec),
            txn: Some((100, 0)),
            txn_pending: true,
            ..Default::default()
        },
    );
}

#[test]
fn commit_fails_whole_when_the_enqueue_fails() {
    // The EndTxn fails (and its request rolls back) rather than committing records
    // that would never be forwarded; the inner commit does not run.
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction().never();
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    pending_txn_message(&h, RowKey::new(T, 0, 5), None);
    h.store.db().state().fail_txn_enqueue = true;

    assert!(h.store.commit_transaction("txn", 100, 0).is_err());
    assert!(h.store.db().state().tracking.is_empty());
}

#[test]
fn commit_routes_a_sampled_topic_like_a_plain_produce() {
    // A topic below 100% forwards exactly the records make_forward_decision selects
    // (keyed by murmur2, keyless by offset) and counts the others skipped.
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    let mut sampled = topic_config(SyncMode::Async, WriteMode::DualWrite);
    sampled.forward_percentage = 50;
    let h = harness(mock, vec![sampled], 16);
    let mut expected = Vec::new();
    for off in 0..200i64 {
        let key = (off % 2 == 0).then(|| format!("k{off}").into_bytes());
        pending_txn_message(&h, RowKey::new(T, 0, off), key.as_deref());
        if make_forward_decision(key.as_deref(), off, 50) == ForwardDecision::Forward {
            expected.push(RowKey::new(T, 0, off));
        }
    }

    h.store.commit_transaction("txn", 100, 0).unwrap();

    let s = h.store.db().state();
    let enqueued: Vec<RowKey> = s.tracking.keys().copied().collect();
    assert_eq!(enqueued, expected);
    assert!(!expected.is_empty() && expected.len() < 200);
    assert_eq!(
        s.metrics[&(T, 0)].messages_skipped,
        200 - i64::try_from(expected.len()).unwrap()
    );
}

#[test]
fn commit_enqueues_only_the_forwarding_topics() {
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(1)
        .returning(|_, _, _| Ok(()));
    let mut local = topic_config(SyncMode::Async, WriteMode::DualWrite);
    local.topic_id = T + 1;
    local.mode = ShadowMode::LocalOnly;
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite), local],
        16,
    );
    pending_txn_message(&h, RowKey::new(T, 0, 1), None);
    pending_txn_message(&h, RowKey::new(T + 1, 0, 1), None);
    pending_txn_message(&h, RowKey::new(T + 2, 0, 1), None);

    h.store.commit_transaction("txn", 100, 0).unwrap();

    let enqueued: Vec<RowKey> = h.store.db().state().tracking.keys().copied().collect();
    assert_eq!(enqueued, vec![RowKey::new(T, 0, 1)]);
}

#[test]
fn a_repeated_enqueue_leaves_one_row_per_record() {
    // ON CONFLICT DO NOTHING: an enqueue that runs again (a retried EndTxn whose
    // first attempt rolled back after enqueueing) adds no second row.
    let mut mock = MockKafkaStore::new();
    mock.expect_commit_transaction()
        .times(2)
        .returning(|_, _, _| Ok(()));
    let h = harness(
        mock,
        vec![topic_config(SyncMode::Async, WriteMode::DualWrite)],
        16,
    );
    pending_txn_message(&h, RowKey::new(T, 0, 3), None);
    h.store.commit_transaction("txn", 100, 0).unwrap();
    h.store
        .db()
        .state()
        .tracking
        .get_mut(&RowKey::new(T, 0, 3))
        .unwrap()
        .retry_count = 2;
    h.store.commit_transaction("txn", 100, 0).unwrap();

    let s = h.store.db().state();
    assert_eq!(s.tracking.len(), 1);
    assert_eq!(
        s.tracking[&RowKey::new(T, 0, 3)].retry_count,
        2,
        "left alone"
    );
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
    assert_eq!(drain(&h), 2);
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
    drain(&h);
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
        drain(&h);
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
    drain(&h);
    h.store.db().advance_ms(OUTBOX_MAX_BACKOFF_MS + 1);
    sent_records(&h);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);

    h.store.db().state().settings.max_retries = 2;
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
}

/// The caps the claims synced the dead flags to, emptied.
fn dead_flag_syncs(h: &Harness) -> Vec<i64> {
    std::mem::take(&mut h.store.db().state().dead_flag_syncs)
}

/// One poll whose tick committed (the worker's tick: acks, then the poll).
fn committed_poll(h: &Harness) {
    h.store.drain_forward_acks();
    h.store.poll_and_forward_outbox().unwrap();
    h.store.commit_applied_acks();
}

/// The full dead-flag sync runs on the worker's first claim and then only when the
/// flags may disagree with the cap: steady-state claims skip it.
#[test]
fn the_dead_flag_sync_runs_once_then_only_when_the_cap_changes() {
    let h = async_harness(16);
    h.store.db().state().settings.max_retries = 5;
    for _ in 0..3 {
        committed_poll(&h);
    }
    assert_eq!(dead_flag_syncs(&h), vec![5], "first claim only");

    // A reload changes the cap: one sync with the new cap.
    h.store.db().state().settings.max_retries = 3;
    committed_poll(&h);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), vec![3]);

    // A failure ack at the synced cap keeps `dead` itself: no sync.
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    committed_poll(&h);
    sent_records(&h);
    h.acks.send(err_ack(RowKey::new(T, 0, 1))).unwrap();
    committed_poll(&h);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), Vec::<i64>::new());

    // The schema was found current again (dropped and recreated, upgraded).
    h.store.forget_dead_flags_sync();
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), vec![3]);
}

/// A failure ack applied with another cap than the last sync's set `dead` by that
/// cap, so the next claim syncs even when the cap is back to the synced one by then.
#[test]
fn a_failure_ack_at_another_cap_makes_the_next_claim_sync() {
    let h = async_harness(16);
    h.store.db().state().settings.max_retries = 5;
    seed_rows(&h.store, &[RowKey::new(T, 0, 1)]);
    committed_poll(&h);
    sent_records(&h);
    assert_eq!(dead_flag_syncs(&h), vec![5]);

    h.store.db().state().settings.max_retries = 2;
    h.acks.send(err_ack(RowKey::new(T, 0, 1))).unwrap();
    drain(&h);
    h.store.db().state().settings.max_retries = 5;
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), vec![5]);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), Vec::<i64>::new());
}

/// A sync counts only once its tick committed: a tick that rolled back (its commit
/// never reported), a poll that failed after the sync, and a failed claim all
/// leave the next claim to sync again. No transaction's status is looked up, so a
/// sync followed by a long pause in forwarding cannot outlive its clog.
#[test]
fn a_dead_flag_sync_counts_only_once_its_tick_committed() {
    let h = async_harness(16);
    // The tick's transaction rolled back: commit_applied_acks never ran.
    h.store.drain_forward_acks();
    h.store.poll_and_forward_outbox().unwrap();
    assert_eq!(dead_flag_syncs(&h).len(), 1);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h).len(), 1, "rolled-back sync runs again");
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), Vec::<i64>::new(), "committed: no sync");

    // The poll failed after its sync (the worker discards it), then committed.
    h.store.forget_dead_flags_sync();
    h.store.drain_forward_acks();
    h.store.poll_and_forward_outbox().unwrap();
    h.store.discard_staged_poll();
    h.store.commit_applied_acks();
    assert_eq!(dead_flag_syncs(&h).len(), 1);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h).len(), 1, "discarded sync runs again");

    // A failed claim, after a committed sync.
    h.store.db().state().fail_claim = true;
    assert!(h.store.poll_and_forward_outbox().is_err());
    h.store.db().state().fail_claim = false;
    dead_flag_syncs(&h);
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h).len(), 1);
}

/// A sync staged by a tick whose commit was never reported is dropped by the next
/// tick, even when that tick claims nothing (no broker configured) and commits:
/// otherwise its commit would make the rolled-back sync count.
#[test]
fn a_rolled_back_sync_is_not_made_final_by_a_later_tick_without_a_claim() {
    let h = async_harness(16);
    // Tick N: the poll syncs and stages; the transaction rolls back.
    h.store.drain_forward_acks();
    h.store.poll_and_forward_outbox().unwrap();
    assert_eq!(dead_flag_syncs(&h).len(), 1);
    // Tick N+1: no broker, so the poll returns before any claim; it commits.
    h.store.db().state().bootstrap_unset = true;
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h), Vec::<i64>::new());
    // The broker is back: the sync of tick N never counted, so the claim syncs.
    h.store.db().state().bootstrap_unset = false;
    committed_poll(&h);
    assert_eq!(dead_flag_syncs(&h).len(), 1);
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
    assert!(requested(&h), "full claim: poll again");
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert!(!requested(&h));
    assert!(h.store.outbox_tick_due(OUTBOX_POLL_INTERVAL));
}

#[test]
fn a_backlog_does_not_starve_other_partitions() {
    // Topic T has a 100-row backlog (a replay, an outage); topic T + 1 then gets
    // one record. The claim shares the batch between the partitions, so the
    // second topic's record goes out with the first claim instead of waiting
    // behind the whole backlog (and a sync-mode response behind its timeout).
    let h = async_harness(256);
    h.store.db().add_topic(T + 1, "other", None);
    h.store.db().state().settings.batch_size = 10;
    let backlog: Vec<RowKey> = (0..100).map(|o| RowKey::new(T, 0, o)).collect();
    seed_rows(&h.store, &backlog);
    seed_rows(&h.store, &[RowKey::new(T + 1, 0, 0)]);

    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 6);
    let sent = sent_records(&h);
    let keys: Vec<RowKey> = sent.iter().map(|r| r.row).collect();
    let mut expected: Vec<RowKey> = (0..5).map(|o| RowKey::new(T, 0, o)).collect();
    expected.push(RowKey::new(T + 1, 0, 0));
    assert_eq!(
        keys, expected,
        "the backlog's lowest offsets, then the other topic"
    );
    assert!(
        requested(&h),
        "the backlog used its whole share: poll again at once"
    );

    // Once the other topic's record is delivered, the next claim continues the
    // backlog in order, now alone: the whole batch.
    h.acks.send(ok_ack(RowKey::new(T + 1, 0, 0), 0)).unwrap();
    drain(&h);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 10);
    assert_eq!(offsets(&sent_records(&h)), (5..15).collect::<Vec<_>>());
}

#[test]
fn partitions_with_nothing_due_take_no_share_of_the_claim() {
    // Partitions whose pending rows cannot be claimed now (dead letters, which
    // stay pending until a replay; an orphan pointer whose message retention
    // deleted; a row in flight; a row backing off after a failure) must not
    // shrink the share of the partitions that have rows due: a backlog next to
    // 100 dead-letter partitions still gets the whole batch.
    let h = async_harness(512);
    h.store.db().add_topic(T + 1, "stuck", None);
    h.store.db().state().settings.batch_size = 100;
    let in_flight = RowKey::new(T + 1, 102, 0);
    seed_rows(&h.store, &[in_flight]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    drop(sent_records(&h));

    let cap = h.store.db().state().settings.max_retries;
    for p in 0..100 {
        let dead = RowKey::new(T + 1, p, 0);
        seed_rows(&h.store, &[dead]);
        h.store
            .db()
            .state()
            .tracking
            .get_mut(&dead)
            .unwrap()
            .retry_count = cap;
    }
    let orphan = RowKey::new(T + 1, 100, 0);
    seed_rows(&h.store, &[orphan]);
    h.store.db().state().messages.remove(&orphan);
    let backing_off = RowKey::new(T + 1, 101, 0);
    seed_rows(&h.store, &[backing_off]);
    {
        let mut s = h.store.db().state();
        let now = s.now_ms;
        let row = s.tracking.get_mut(&backing_off).unwrap();
        row.retry_count = 1;
        row.forwarded_at = Some(now);
    }
    let backlog: Vec<RowKey> = (0..300).map(|o| RowKey::new(T, 0, o)).collect();
    seed_rows(&h.store, &backlog);

    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 100);
    assert_eq!(offsets(&sent_records(&h)), (0..100).collect::<Vec<_>>());
    assert!(requested(&h), "a full claim asks for another one at once");
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 100);
    assert_eq!(offsets(&sent_records(&h)), (100..200).collect::<Vec<_>>());
}

#[test]
fn nothing_due_asks_for_no_immediate_poll() {
    // The worker skips its receive wait while a poll is requested, so a claim
    // that leaves nothing due, or rows the full forward channel did not take,
    // must not request one: the worker would spin on the claim.
    let h = async_harness(1);
    h.store.db().state().settings.batch_size = 2;
    seed_rows(
        &h.store,
        &[
            RowKey::new(T, 0, 1),
            RowKey::new(T, 0, 2),
            RowKey::new(T, 0, 3),
        ],
    );
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert!(
        !requested(&h),
        "a row the full channel refused waits for the next interval"
    );
    drop(sent_records(&h));
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    drop(sent_records(&h));
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    drop(sent_records(&h));
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    assert!(!requested(&h), "nothing due");
}

#[test]
fn the_partition_share_is_an_equal_split_of_at_least_one() {
    assert_eq!(per_partition_claim_cap(1000, 1), 1000);
    assert_eq!(per_partition_claim_cap(1000, 0), 1000);
    assert_eq!(per_partition_claim_cap(10, 2), 5);
    assert_eq!(per_partition_claim_cap(10, 3), 4);
    assert_eq!(per_partition_claim_cap(10, 40), 1);
    assert_eq!(per_partition_claim_cap(0, 3), 1);
}

/// Claim the one seeded row and answer it Delivered on the ack channel.
fn claimed_and_acked(h: &Harness, key: RowKey) {
    seed_rows(&h.store, &[key]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    drop(sent_records(h));
    h.acks.send(ok_ack(key, 42)).unwrap();
}

/// After its ack is finally applied the row was delivered exactly once: no lost
/// attempt counted, not re-sent, the forward metric counted once.
fn assert_delivered_once(h: &Harness, key: RowKey) {
    let row = h.store.db().row(key).unwrap();
    assert_eq!(row.external_offset, Some(42));
    assert_eq!(row.retry_count, 0, "no attempt counted as lost");
    assert!(!row.attempt_open);
    assert_eq!(
        h.store.db().state().metrics[&key.partition()].messages_forwarded,
        1
    );
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0, "not re-sent");
}

#[test]
fn a_failed_ack_statement_keeps_its_acks_for_the_next_tick() {
    let h = async_harness(16);
    let key = RowKey::new(T, 0, 3);
    claimed_and_acked(&h, key);

    h.store.db().state().fail_apply_delivered = true;
    assert_eq!(drain(&h), 0);
    // The row is still in flight: the poll does not re-send it meanwhile.
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    assert_eq!(sent_records(&h).len(), 0);

    h.store.db().state().fail_apply_delivered = false;
    assert_eq!(drain(&h), 1);
    assert_delivered_once(&h, key);
}

#[test]
fn an_error_raised_while_applying_acks_does_not_lose_them() {
    let h = async_harness(16);
    let key = RowKey::new(T, 0, 3);
    claimed_and_acked(&h, key);

    // A Postgres ERROR unwinds out of the ack application (the worker's
    // subtransaction rolls it back and the tick goes on to poll).
    h.store.db().state().raise_in_apply_delivered = true;
    let raised = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        h.store.drain_forward_acks()
    }));
    assert!(raised.is_err());
    assert_eq!(
        h.store.poll_and_forward_outbox().unwrap(),
        0,
        "still in flight"
    );

    h.store.db().state().raise_in_apply_delivered = false;
    assert_eq!(drain(&h), 1);
    assert_delivered_once(&h, key);
}

#[test]
fn acks_of_a_tick_whose_commit_failed_are_applied_again() {
    // The ack application ran, then the tick's COMMIT failed: the database
    // forgot it. Until a commit is reported the acks stay queued and their rows
    // in flight (no re-send, no lost attempt counted), the counters do not
    // move, and the next tick applies them again.
    let h = async_harness(16);
    let key = RowKey::new(T, 0, 3);
    claimed_and_acked(&h, key);
    let before = h.store.db().row(key).unwrap();
    let metrics_before = h.store.db().state().metrics.clone();

    assert_eq!(h.store.drain_forward_acks(), 1);
    // The rollback of the failed COMMIT.
    *h.store.db().state().tracking.get_mut(&key).unwrap() = before;
    h.store.db().state().metrics = metrics_before;
    assert_eq!(
        h.store.metrics().forwarded.load(Ordering::Relaxed),
        0,
        "nothing counted before the commit"
    );
    assert_eq!(
        h.store.poll_and_forward_outbox().unwrap(),
        0,
        "still in flight: not re-sent"
    );
    assert_eq!(sent_records(&h).len(), 0);

    assert_eq!(drain(&h), 1, "applied again");
    assert_eq!(h.store.metrics().forwarded.load(Ordering::Relaxed), 1);
    assert_delivered_once(&h, key);
}

#[test]
fn stale_in_flight_entries_do_not_use_up_the_in_flight_room() {
    // MAX_INFLIGHT forwards whose acks were lost (a forwarder crash) are stale
    // once evict_after passes: their rows are claimable again, and they must not
    // keep the in-flight room at zero, which would stop every claim for good.
    // The horizon is wide enough that filling the tracker cannot outlast it on a
    // slow (instrumented) build; the first poll must still see every entry live.
    const EVICT: Duration = Duration::from_secs(1);
    let mut h = async_harness(16);
    h.store.inflight = crate::kafka::shadow::inflight::InflightTracker::new(EVICT);
    let filled = Instant::now();
    for o in 0..MAX_INFLIGHT {
        assert!(h
            .store
            .inflight
            .try_begin(RowKey::new(T + 1, 0, i64::try_from(o).unwrap())));
    }
    seed_rows(&h.store, &[RowKey::new(T, 0, 0)]);
    assert_eq!(
        h.store.poll_and_forward_outbox().unwrap(),
        0,
        "no room while the entries are live"
    );
    assert!(
        filled.elapsed() < EVICT,
        "the first poll ran past the horizon, so it did not test live entries"
    );
    std::thread::sleep(EVICT.saturating_sub(filled.elapsed()) + Duration::from_millis(50));
    assert_eq!(
        h.store.poll_and_forward_outbox().unwrap(),
        1,
        "the stale entries no longer count against the room"
    );
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
    drain(&h);
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
    assert!(requested(&h));
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
    assert!(!requested(&h));
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
        other => panic!("expected a hold, got {other:?}"),
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

#[test]
fn a_requested_tick_waits_for_the_minimum_spacing() {
    let h = async_harness(64);
    h.store.db().state().settings.batch_size = 1;
    seed_rows(&h.store, &[RowKey::new(T, 0, 1), RowKey::new(T, 0, 2)]);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 1);
    assert_eq!(
        h.store.outbox_requested_wait(Duration::ZERO),
        Some(OUTBOX_MIN_TICK_SPACING)
    );
    assert!(
        !h.store.outbox_tick_due(Duration::ZERO),
        "right after a tick"
    );
    assert!(h.store.outbox_tick_due(OUTBOX_MIN_TICK_SPACING));
    assert_eq!(
        h.store.outbox_requested_wait(OUTBOX_MIN_TICK_SPACING),
        Some(Duration::ZERO)
    );
}

#[test]
fn no_broker_configured_claims_nothing() {
    // The forwarder answers every record NotAttempted without a broker, which
    // leaves it due again: a claim would only churn the outbox.
    let h = async_harness(64);
    h.store.db().state().bootstrap_unset = true;
    h.store.db().state().settings.batch_size = 2;
    let keys = [
        RowKey::new(T, 0, 1),
        RowKey::new(T, 0, 2),
        RowKey::new(T, 0, 3),
    ];
    seed_rows(&h.store, &keys);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 0);
    assert_eq!(sent_records(&h).len(), 0, "nothing sent");
    assert!(!requested(&h));
    for key in keys {
        let row = h.store.db().row(key).unwrap();
        assert!(!row.attempt_open, "the row was not claimed");
        assert_eq!(row.retry_count, 0);
    }
    // Once a broker is configured the rows go out.
    h.store.db().state().bootstrap_unset = false;
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
}

#[test]
fn acks_all_not_attempted_ask_for_no_immediate_poll() {
    // The forwarder answering without sending gives the rows back due at
    // once: a claim right after would take them again, and again.
    let h = async_harness(64);
    h.store.db().state().settings.batch_size = 2;
    let keys = [
        RowKey::new(T, 0, 1),
        RowKey::new(T, 0, 2),
        RowKey::new(T, 0, 3),
        RowKey::new(T, 0, 4),
    ];
    seed_rows(&h.store, &keys);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    assert!(requested(&h), "full claim: poll again");
    for record in sent_records(&h) {
        h.acks.send(skipped_ack(record.row)).unwrap();
    }
    assert_eq!(drain(&h), 2);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    assert!(!requested(&h), "every drained ack was NotAttempted");

    // A delivery among the acks: the forwarder is sending, a full claim asks again.
    let sent = sent_records(&h);
    h.acks.send(ok_ack(sent[0].row, 1)).unwrap();
    h.acks.send(skipped_ack(sent[1].row)).unwrap();
    assert_eq!(drain(&h), 2);
    assert_eq!(h.store.poll_and_forward_outbox().unwrap(), 2);
    assert!(requested(&h));
}
