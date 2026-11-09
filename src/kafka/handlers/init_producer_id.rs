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
/// For idempotent-only producers (transactional_id is None):
/// - If existing_producer_id is -1, allocates new producer ID
/// - Otherwise, validates and bumps epoch for reconnecting producer
///
/// # Arguments
/// * `ctx` - Handler context (store, coordinator, broker metadata, etc.)
/// * `transactional_id` - Optional transactional ID (for transactional producers)
/// * `transaction_timeout_ms` - Transaction timeout in ms (for transactional producers)
/// * `existing_producer_id` - Existing producer ID for reconnection (-1 for new)
/// * `existing_epoch` - Existing epoch for reconnection (-1 for new)
/// * `client_id` - Optional client identifier for debugging
///
/// # Returns
/// InitProducerIdResponse with allocated (producer_id, epoch) or error
pub fn handle_init_producer_id(
    ctx: &HandlerContext,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    existing_producer_id: i64,
    existing_epoch: i16,
    client_id: Option<&str>,
) -> Result<InitProducerIdResponse> {
    let store = ctx.store;
    let mut response = InitProducerIdResponse::default();

    // Phase 10: Handle transactional producers
    if let Some(ref txn_id) = transactional_id {
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

    // Idempotent-only producer (no transactional_id)
    if existing_producer_id == -1 {
        // New producer: allocate a fresh producer ID with epoch 0
        let (producer_id, epoch) = store.allocate_producer_id(client_id, None)?;

        response.error_code = ERROR_NONE;
        response.producer_id = ProducerId(producer_id);
        response.producer_epoch = epoch;
    } else {
        // Existing producer reconnecting: validate and bump epoch
        // First, check if the producer exists and get current epoch
        match store.get_producer_epoch(existing_producer_id)? {
            Some(current_epoch) => {
                // KIP-360: the client must present the epoch it currently holds. A
                // lower epoch means a newer instance took over (fenced); a higher one
                // was never issued by this broker.
                if existing_epoch != current_epoch {
                    return Err(crate::kafka::error::KafkaError::producer_fenced(
                        existing_producer_id,
                        existing_epoch,
                        current_epoch,
                    ));
                }

                // Bump the epoch (or, once it is exhausted, get a fresh producer id).
                let (producer_id, new_epoch) =
                    store.increment_producer_epoch(existing_producer_id)?;

                response.error_code = ERROR_NONE;
                response.producer_id = ProducerId(producer_id);
                response.producer_epoch = new_epoch;
            }
            None => {
                // Producer ID not found - client has an invalid ID
                return Err(crate::kafka::error::KafkaError::unknown_producer_id(
                    existing_producer_id,
                ));
            }
        }
    }

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

    #[test]
    fn reinit_requires_the_current_epoch() {
        // KIP-360: both a stale and a never-issued (higher) epoch are fenced; the
        // epoch must not be bumped for either.
        for epoch in [2i16, 4] {
            let mut store = MockKafkaStore::new();
            store.expect_get_producer_epoch().returning(|_| Ok(Some(3)));
            let err = run(&store, epoch).unwrap_err();
            assert!(
                matches!(err, crate::kafka::error::KafkaError::ProducerFenced { .. }),
                "epoch {} should be fenced, got {:?}",
                epoch,
                err
            );
        }
    }

    #[test]
    fn reinit_returns_the_id_the_store_hands_out() {
        // Once the epoch is exhausted the store allocates a fresh producer id; the
        // response must carry that id, not the old one.
        let mut store = MockKafkaStore::new();
        store
            .expect_get_producer_epoch()
            .returning(|_| Ok(Some(i16::MAX)));
        store
            .expect_increment_producer_epoch()
            .returning(|_| Ok((2000, 0)));
        let response = run(&store, i16::MAX).unwrap();
        assert_eq!(response.producer_id.0, 2000);
        assert_eq!(response.producer_epoch, 0);
    }
}
