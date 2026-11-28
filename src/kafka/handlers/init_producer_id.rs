//! InitProducerId: producer ids and epochs for idempotent and transactional producers.

use crate::kafka::constants::{ERROR_CONCURRENT_TRANSACTIONS, ERROR_NONE, ERROR_PRODUCER_FENCED};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::storage::{TransactionState, TransactionalProducer, TxnPartitions};
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::ProducerId;
use pgrx::GucSetting;

/// `pg_kafka.transaction_max_timeout_ms`, Kafka's `transaction.max.timeout.ms`:
/// the largest transaction timeout InitProducerId accepts. Registered in
/// `config::init`; reloadable.
pub static TRANSACTION_MAX_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS);

/// Whether a transactional producer may use `timeout_ms`: Kafka refuses a timeout
/// <= 0 or above `transaction.max.timeout.ms` with INVALID_TRANSACTION_TIMEOUT.
pub fn transaction_timeout_is_valid(timeout_ms: i32, max_timeout_ms: i32) -> bool {
    timeout_ms > 0 && timeout_ms <= max_timeout_ms
}

/// What InitProducerId does for a transactional id, given the producer id and epoch
/// the request presents (v3+, KIP-360) and the id's current producer: Kafka 3.7's
/// `TransactionCoordinator.handleInitProducerId` / `prepareIncrementProducerEpoch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxnInit {
    /// Move to the next epoch (or a new producer id), fencing the previous holder.
    /// `requested_by` is the epoch to remember when the current producer asked for the
    /// bump itself, so a retry of the request is recognised.
    Bump { requested_by: Option<i16> },
    /// A retry of a bump the producer already got: answer the current id and epoch.
    Current { producer_id: i64, epoch: i16 },
    /// A presented id or epoch that is not the current producer's: PRODUCER_FENCED.
    Fenced,
}

/// Decide [`TxnInit`] from the presented `(producer_id, epoch)` (None for -1/-1) and the
/// transactional id's current producer (None for a new transactional id).
pub(crate) fn decide_txn_init(
    presented: Option<(i64, i16)>,
    current: Option<&TransactionalProducer>,
) -> TxnInit {
    let (Some((producer_id, epoch)), Some(current)) = (presented, current) else {
        // A new producer, or a new transactional id: Kafka accepts any presented id.
        return TxnInit::Bump { requested_by: None };
    };
    if producer_id != current.producer_id {
        TxnInit::Fenced
    } else if epoch == current.epoch {
        TxnInit::Bump {
            requested_by: Some(epoch),
        }
    } else if Some(epoch) == current.previous_epoch {
        TxnInit::Current {
            producer_id: current.producer_id,
            epoch: current.epoch,
        }
    } else {
        TxnInit::Fenced
    }
}

/// Handle InitProducerId request
///
/// Allocates a new producer ID or bumps the epoch for an existing producer.
/// Used by idempotent and transactional producers to get their identity.
///
/// For transactional producers (transactional_id is Some), see `decide_txn_init`:
/// a new transactional id gets a new producer id; an existing one gets its next epoch
/// (fencing the previous producer) unless the request presents a producer id and epoch
/// that are not the current producer's (PRODUCER_FENCED) or that a bump already moved
/// past (the current epoch, without another bump). While the transactional id has a
/// transaction Ongoing, a request that is not fenced aborts it, fences its epoch and is
/// answered CONCURRENT_TRANSACTIONS (the client retries), as in Kafka 3.7.
///
/// For idempotent-only producers (transactional_id is None) a new producer ID is
/// allocated; the producer id and epoch the request presents are ignored, as in Kafka 3.7.
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - Optional transactional ID (for transactional producers)
/// * `transaction_timeout_ms` - Transaction timeout in ms (for transactional producers)
/// * `presented` - The producer id and epoch the request presents (None for -1/-1)
/// * `client_id` - Optional client identifier for debugging
///
/// # Returns
/// InitProducerIdResponse with allocated (producer_id, epoch) or error
pub fn handle_init_producer_id(
    ctx: &HandlerContext,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    presented: Option<(i64, i16)>,
    client_id: Option<&str>,
) -> Result<InitProducerIdResponse> {
    init_producer(
        ctx,
        transactional_id,
        transaction_timeout_ms,
        presented,
        client_id,
    )
    .map(|(response, _)| response)
}

/// [`handle_init_producer_id`], also returning the partitions of the open transaction it
/// aborted (none unless it aborted one), the partitions whose read_committed long polls
/// to wake.
pub fn init_producer(
    ctx: &HandlerContext,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    presented: Option<(i64, i16)>,
    client_id: Option<&str>,
) -> Result<(InitProducerIdResponse, TxnPartitions)> {
    // The GUC is read only for a transactional producer (the bound does not apply to
    // idempotent ones, as in Kafka).
    let max_timeout_ms = if transactional_id.is_some() {
        TRANSACTION_MAX_TIMEOUT_MS.get()
    } else {
        i32::MAX
    };
    init_producer_id_bounded(
        ctx,
        transactional_id,
        transaction_timeout_ms,
        presented,
        client_id,
        max_timeout_ms,
    )
}

/// An InitProducerId error response.
fn init_error(error_code: i16) -> InitProducerIdResponse {
    let mut response = InitProducerIdResponse::default();
    response.error_code = error_code;
    response.producer_id = ProducerId(-1);
    response.producer_epoch = -1;
    response
}

/// [`init_producer`] with an explicit `transaction.max.timeout.ms`.
pub(crate) fn init_producer_id_bounded(
    ctx: &HandlerContext,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    presented: Option<(i64, i16)>,
    client_id: Option<&str>,
    max_timeout_ms: i32,
) -> Result<(InitProducerIdResponse, TxnPartitions)> {
    let store = ctx.store;
    let mut response = InitProducerIdResponse::default();

    // Handle transactional producers
    if let Some(txn_id) = transactional_id {
        // Validate the timeout first, as Kafka does (the producer is not
        // touched, so an existing transaction is not fenced by a bad request).
        if !transaction_timeout_is_valid(transaction_timeout_ms, max_timeout_ms) {
            return Ok((
                init_error(crate::kafka::constants::ERROR_INVALID_TRANSACTION_TIMEOUT),
                TxnPartitions::new(),
            ));
        }
        let current = if presented.is_some() {
            store.transactional_producer(txn_id)?
        } else {
            None
        };
        let decision = decide_txn_init(presented, current.as_ref());
        if decision != TxnInit::Fenced
            && store.get_transaction_state(txn_id)? == Some(TransactionState::Ongoing)
        {
            // Kafka 3.7 (TransactionCoordinator.prepareInitProducerIdTransit, state Ongoing)
            // aborts the open transaction under a fenced epoch and answers
            // CONCURRENT_TRANSACTIONS, never handing out that epoch: a retry presenting the
            // same epoch is then fenced, and one presenting none gets the next epoch. The
            // abort and the fence are the bump below; the bump is not remembered as asked
            // for by the producer, so no presented epoch counts as a retry of it. The
            // aborted transaction's partitions are read first: the bump clears them.
            let aborted = store.transaction_partitions(txn_id)?;
            store.get_or_create_transactional_producer(
                txn_id,
                transaction_timeout_ms,
                client_id,
            )?;
            return Ok((init_error(ERROR_CONCURRENT_TRANSACTIONS), aborted));
        }
        // No transaction is open (or the request is fenced): nothing ends, nothing to wake.
        let (producer_id, epoch) = match decision {
            TxnInit::Fenced => {
                return Ok((init_error(ERROR_PRODUCER_FENCED), TxnPartitions::new()));
            }
            TxnInit::Current { producer_id, epoch } => (producer_id, epoch),
            TxnInit::Bump { requested_by } => {
                // Either creates a new producer id and transaction record (a new
                // transactional id) or bumps the epoch, fencing the previous producer.
                let (producer_id, epoch) = store.get_or_create_transactional_producer(
                    txn_id,
                    transaction_timeout_ms,
                    client_id,
                )?;
                if let Some(previous) = requested_by {
                    store.remember_previous_epoch(producer_id, previous)?;
                }
                (producer_id, epoch)
            }
        };

        response.error_code = ERROR_NONE;
        response.producer_id = ProducerId(producer_id);
        response.producer_epoch = epoch;

        return Ok((response, TxnPartitions::new()));
    }

    // Idempotent-only producer: as in Kafka, always a fresh producer id at epoch 0,
    // whatever id and epoch the request presents. A client resuming after its state
    // expired (KIP-360) presents its old id; refusing it (UNKNOWN_PRODUCER_ID, or
    // PRODUCER_FENCED for a retried request whose epoch was already bumped) is fatal
    // to the Java producer, while a new id lets it reset its sequences and go on.
    let (producer_id, epoch) = store.allocate_producer_id(client_id, None)?;
    response.error_code = ERROR_NONE;
    response.producer_id = ProducerId(producer_id);
    response.producer_epoch = epoch;

    Ok((response, TxnPartitions::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;
    use crate::testing::TestEnv;

    fn run_txn(store: &MockKafkaStore, timeout_ms: i32) -> InitProducerIdResponse {
        let env = TestEnv::new();
        let ctx = env.ctx(store);
        init_producer_id_bounded(
            &ctx,
            Some("tx"),
            timeout_ms,
            None,
            None,
            crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
        )
        .unwrap()
        .0
    }

    #[test]
    fn transaction_timeout_bounds() {
        assert!(!transaction_timeout_is_valid(-5, 900_000));
        assert!(!transaction_timeout_is_valid(0, 900_000));
        assert!(transaction_timeout_is_valid(1, 900_000));
        assert!(transaction_timeout_is_valid(900_000, 900_000));
        assert!(!transaction_timeout_is_valid(900_001, 900_000));
    }

    #[test]
    fn invalid_transaction_timeout_is_refused_without_touching_the_producer() {
        // InitProducerId accepted any timeout (-5, 0, i32::MAX). Kafka answers
        // INVALID_TRANSACTION_TIMEOUT (50) and leaves the transactional id alone; the
        // mock has no expectation, so any store call would fail the test.
        let store = MockKafkaStore::new();
        for timeout in [-5, 0, 900_001, i32::MAX] {
            let r = run_txn(&store, timeout);
            assert_eq!(
                r.error_code,
                crate::kafka::constants::ERROR_INVALID_TRANSACTION_TIMEOUT
            );
            assert_eq!(r.producer_id.0, -1);
            assert_eq!(r.producer_epoch, -1);
        }
        // The default bound (15 minutes) itself is accepted.
        let mut store = MockKafkaStore::new();
        store.expect_get_transaction_state().returning(|_| Ok(None));
        store
            .expect_get_or_create_transactional_producer()
            .times(1)
            .returning(|_, _, _| Ok((7, 0)));
        assert_eq!(run_txn(&store, 900_000).error_code, ERROR_NONE);
    }

    const OWNER: TransactionalProducer = TransactionalProducer {
        producer_id: 7,
        epoch: 3,
        previous_epoch: Some(2),
    };

    #[test]
    fn presented_epochs_follow_kafka() {
        let bump = TxnInit::Bump { requested_by: None };
        assert_eq!(
            decide_txn_init(None, Some(&OWNER)),
            bump,
            "a new producer fences"
        );
        assert_eq!(
            decide_txn_init(Some((7, 3)), None),
            bump,
            "a new transactional id"
        );
        assert_eq!(decide_txn_init(None, None), bump);
        assert_eq!(
            decide_txn_init(Some((7, 3)), Some(&OWNER)),
            TxnInit::Bump {
                requested_by: Some(3)
            },
            "the owner at its epoch is bumped"
        );
        assert_eq!(
            decide_txn_init(Some((7, 2)), Some(&OWNER)),
            TxnInit::Current {
                producer_id: 7,
                epoch: 3
            },
            "a retry of that bump"
        );
        for zombie in [(7, 1), (7, 4), (8, 3), (8, 2)] {
            assert_eq!(
                decide_txn_init(Some(zombie), Some(&OWNER)),
                TxnInit::Fenced,
                "{zombie:?}"
            );
        }
        let fenced = TransactionalProducer {
            previous_epoch: None,
            ..OWNER
        };
        assert_eq!(
            decide_txn_init(Some((7, 2)), Some(&fenced)),
            TxnInit::Fenced
        );
    }

    fn run_presented(store: &MockKafkaStore, presented: (i64, i16)) -> InitProducerIdResponse {
        let env = TestEnv::new();
        let ctx = env.ctx(store);
        init_producer_id_bounded(
            &ctx,
            Some("tx"),
            60_000,
            Some(presented),
            None,
            crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
        )
        .unwrap()
        .0
    }

    #[test]
    fn a_zombie_is_fenced_without_touching_the_owner() {
        // No bump expectation: get_or_create_transactional_producer must not run.
        let mut store = MockKafkaStore::new();
        store
            .expect_transactional_producer()
            .returning(|_| Ok(Some(OWNER)));
        let r = run_presented(&store, (7, 1));
        assert_eq!(r.error_code, ERROR_PRODUCER_FENCED);
        assert_eq!((r.producer_id.0, r.producer_epoch), (-1, -1));
    }

    #[test]
    fn a_retry_gets_the_current_epoch_and_the_owner_is_bumped_and_remembered() {
        let mut store = MockKafkaStore::new();
        store
            .expect_transactional_producer()
            .returning(|_| Ok(Some(OWNER)));
        store
            .expect_get_transaction_state()
            .returning(|_| Ok(Some(TransactionState::CompleteCommit)));
        let r = run_presented(&store, (7, 2));
        assert_eq!(
            (r.error_code, r.producer_id.0, r.producer_epoch),
            (ERROR_NONE, 7, 3)
        );

        store
            .expect_get_or_create_transactional_producer()
            .times(1)
            .returning(|_, _, _| Ok((7, 4)));
        store
            .expect_remember_previous_epoch()
            .withf(|&pid, &epoch| pid == 7 && epoch == 3)
            .times(1)
            .returning(|_, _| Ok(()));
        let r = run_presented(&store, (7, 3));
        assert_eq!(
            (r.error_code, r.producer_id.0, r.producer_epoch),
            (ERROR_NONE, 7, 4)
        );
    }

    #[test]
    fn an_ongoing_transaction_is_aborted_and_answered_concurrent_transactions() {
        // Kafka 3.7 aborts the open transaction under a fenced epoch and answers
        // CONCURRENT_TRANSACTIONS, whether the request presents the owner's epoch, the one
        // before its last bump or none; the fenced epoch is not handed out nor remembered
        // as asked for, so a retry presenting the same epoch is fenced.
        for presented in [Some((7, 3)), Some((7, 2)), None] {
            let mut store = MockKafkaStore::new();
            store
                .expect_transactional_producer()
                .returning(|_| Ok(Some(OWNER)));
            store
                .expect_get_transaction_state()
                .withf(|id| id == "tx")
                .returning(|_| Ok(Some(TransactionState::Ongoing)));
            store
                .expect_transaction_partitions()
                .withf(|id| id == "tx")
                .times(1)
                .returning(|_| Ok(vec![("t".to_string(), vec![2])]));
            store
                .expect_get_or_create_transactional_producer()
                .times(1)
                .returning(|_, _, _| Ok((7, 4)));
            store.expect_remember_previous_epoch().never();
            let env = TestEnv::new();
            let ctx = env.ctx(&store);
            let (r, aborted) = init_producer_id_bounded(
                &ctx,
                Some("tx"),
                60_000,
                presented,
                None,
                crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
            )
            .unwrap();
            assert_eq!(
                (r.error_code, r.producer_id.0, r.producer_epoch),
                (ERROR_CONCURRENT_TRANSACTIONS, -1, -1),
                "{presented:?}"
            );
            assert_eq!(aborted, vec![("t".to_string(), vec![2])], "{presented:?}");
        }
        // A fenced request does not touch the open transaction.
        let mut store = MockKafkaStore::new();
        store
            .expect_transactional_producer()
            .returning(|_| Ok(Some(OWNER)));
        store.expect_get_transaction_state().never();
        store.expect_get_or_create_transactional_producer().never();
        assert_eq!(
            run_presented(&store, (7, 1)).error_code,
            ERROR_PRODUCER_FENCED
        );
    }

    /// Only an InitProducerId that aborts an open transaction names partitions to wake:
    /// an idempotent producer's, a new transactional id's, a bump of a finished
    /// transaction's and a refused one's end no transaction, and read no registry.
    #[test]
    fn only_an_aborting_init_names_partitions_to_wake() {
        let env = TestEnv::new();
        // transaction_partitions has no expectation: any call fails the test.
        let mut store = MockKafkaStore::new();
        store
            .expect_allocate_producer_id()
            .returning(|_, _| Ok((9, 0)));
        let ctx = env.ctx(&store);
        for _ in 0..3 {
            let (r, woken) = init_producer(&ctx, None, 60_000, None, None).unwrap();
            assert_eq!((r.error_code, r.producer_id.0), (ERROR_NONE, 9));
            assert_eq!(woken, TxnPartitions::new());
        }
        for state in [None, Some(TransactionState::CompleteCommit)] {
            let mut store = MockKafkaStore::new();
            let current = state.clone();
            store
                .expect_get_transaction_state()
                .returning(move |_| Ok(current.clone()));
            store
                .expect_get_or_create_transactional_producer()
                .returning(|_, _, _| Ok((7, 1)));
            let ctx = env.ctx(&store);
            let (r, woken) = init_producer_id_bounded(
                &ctx,
                Some("tx"),
                60_000,
                None,
                None,
                crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
            )
            .unwrap();
            assert_eq!(r.error_code, ERROR_NONE, "{state:?}");
            assert_eq!(woken, TxnPartitions::new(), "{state:?}");
        }
        let store = MockKafkaStore::new();
        let ctx = env.ctx(&store);
        let (r, woken) =
            init_producer_id_bounded(&ctx, Some("tx"), -1, None, None, 60_000).unwrap();
        assert_ne!(r.error_code, ERROR_NONE);
        assert_eq!(woken, TxnPartitions::new());
    }
}
