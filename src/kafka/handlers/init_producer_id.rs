// InitProducerId handler
//
// Handles InitProducerIdRequest - allocates producer IDs for idempotent producers.
// Phase 9: Idempotent Producer Support
// Phase 10: Extended for Transactional Producer Support

use crate::kafka::constants::ERROR_NONE;
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;
use kafka_protocol::messages::init_producer_id_response::InitProducerIdResponse;
use kafka_protocol::messages::ProducerId;
use pgrx::GucSetting;

/// `pg_kafka.transaction_max_timeout_ms` (wave 3), Kafka's `transaction.max.timeout.ms`:
/// the largest transaction timeout InitProducerId accepts. Registered in
/// `config::init`; reloadable.
pub static TRANSACTION_MAX_TIMEOUT_MS: GucSetting<i32> =
    GucSetting::<i32>::new(crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS);

/// Whether a transactional producer may use `timeout_ms`: Kafka refuses a timeout
/// <= 0 or above `transaction.max.timeout.ms` with INVALID_TRANSACTION_TIMEOUT.
pub fn transaction_timeout_is_valid(timeout_ms: i32, max_timeout_ms: i32) -> bool {
    timeout_ms > 0 && timeout_ms <= max_timeout_ms
}

/// Handle InitProducerId request
///
/// Allocates a new producer ID or bumps the epoch for an existing producer.
/// Used by idempotent and transactional producers to get their identity.
///
/// For transactional producers (transactional_id is Some):
/// - Looks up existing producer by transactional_id
/// - If found, bumps epoch (fences old producer instances)
/// - If not found, allocates new producer ID and creates transaction record
///
/// For idempotent-only producers (transactional_id is None) a new producer ID is
/// allocated, whatever `existing_producer_id` / `existing_epoch` say (Kafka 3.7).
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - Optional transactional ID (for transactional producers)
/// * `transaction_timeout_ms` - Transaction timeout in ms (for transactional producers)
/// * `existing_producer_id` - Producer ID the client holds (-1 for none; ignored)
/// * `existing_epoch` - Epoch the client holds (-1 for none; ignored)
/// * `client_id` - Optional client identifier for debugging
///
/// # Returns
/// InitProducerIdResponse with allocated (producer_id, epoch) or error
pub fn handle_init_producer_id(
    ctx: &HandlerContext,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    _existing_producer_id: i64,
    _existing_epoch: i16,
    client_id: Option<&str>,
) -> Result<InitProducerIdResponse> {
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
        client_id,
        max_timeout_ms,
    )
}

/// [`handle_init_producer_id`] with an explicit `transaction.max.timeout.ms`.
pub(crate) fn init_producer_id_bounded(
    ctx: &HandlerContext,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    client_id: Option<&str>,
    max_timeout_ms: i32,
) -> Result<InitProducerIdResponse> {
    let store = ctx.store;
    let mut response = InitProducerIdResponse::default();

    // Phase 10: Handle transactional producers
    if let Some(ref txn_id) = transactional_id {
        // Wave 3: validate the timeout first, as Kafka does (the producer is not
        // touched, so an existing transaction is not fenced by a bad request).
        if !transaction_timeout_is_valid(transaction_timeout_ms, max_timeout_ms) {
            response.error_code = crate::kafka::constants::ERROR_INVALID_TRANSACTION_TIMEOUT;
            response.producer_id = ProducerId(-1);
            response.producer_epoch = -1;
            return Ok(response);
        }
        // Transactional producer: use transactional_id as the key
        // This will either:
        // - Create a new producer ID and transaction record if txn_id is new
        // - Bump epoch and return existing producer ID if txn_id exists (fencing)
        let (producer_id, epoch) = store.get_or_create_transactional_producer(
            txn_id,
            transaction_timeout_ms,
            client_id,
        )?;

        response.error_code = ERROR_NONE;
        response.producer_id = ProducerId(producer_id);
        response.producer_epoch = epoch;

        return Ok(response);
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

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mocks::MockKafkaStore;

    fn run(store: &MockKafkaStore, existing_epoch: i16) -> Result<InitProducerIdResponse> {
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            store,
            &coordinator,
            &broker,
            1,
            kafka_protocol::records::Compression::None,
        );
        handle_init_producer_id(&ctx, None, 60000, 1000, existing_epoch, None)
    }

    fn run_txn(store: &MockKafkaStore, timeout_ms: i32) -> InitProducerIdResponse {
        let coordinator = crate::kafka::GroupCoordinator::new();
        let broker = crate::kafka::BrokerMetadata::new("localhost".to_string(), 9092);
        let ctx = HandlerContext::new(
            store,
            &coordinator,
            &broker,
            1,
            kafka_protocol::records::Compression::None,
        );
        init_producer_id_bounded(
            &ctx,
            Some("tx".to_string()),
            timeout_ms,
            None,
            crate::kafka::constants::DEFAULT_TRANSACTION_MAX_TIMEOUT_MS,
        )
        .unwrap()
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
        // Wave 3: InitProducerId accepted any timeout (-5, 0, i32::MAX). Kafka answers
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
        store
            .expect_get_or_create_transactional_producer()
            .times(1)
            .returning(|_, _, _| Ok((7, 0)));
        assert_eq!(run_txn(&store, 900_000).error_code, ERROR_NONE);
    }

    /// Kafka 3.7 answers an idempotent InitProducerId that presents a producer id
    /// (KIP-360: a client resuming after its state expired, or retrying a request
    /// whose epoch bump it never saw) with a fresh producer id at epoch 0, whether
    /// the id is current, stale or unknown. pg_kafka answered PRODUCER_FENCED or
    /// UNKNOWN_PRODUCER_ID, which the Java producer treats as fatal.
    #[test]
    fn idempotent_reinit_with_an_existing_id_gets_a_new_producer_id() {
        for epoch in [-1i16, 0, 2, 3, 4, i16::MAX] {
            let mut store = MockKafkaStore::new();
            store
                .expect_allocate_producer_id()
                .times(1)
                .returning(|_, _| Ok((2000, 0)));
            let response = run(&store, epoch).unwrap();
            assert_eq!(
                (
                    response.error_code,
                    response.producer_id.0,
                    response.producer_epoch
                ),
                (ERROR_NONE, 2000, 0),
                "presented epoch {}",
                epoch
            );
        }
    }
}
