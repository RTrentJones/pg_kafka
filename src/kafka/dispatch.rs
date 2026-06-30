// Request dispatch helpers for the Kafka protocol handler
//
// This module provides utilities to reduce boilerplate in process_request().
// Each handler follows the same pattern:
// 1. Call handler function
// 2. On success: wrap result and send response
// 3. On error: log, create error response, send
//
// The dispatch_response() function encapsulates this pattern.

use crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR;
use crate::kafka::error::KafkaError;
use crate::kafka::messages::KafkaResponse;
use crate::{pg_log, pg_warning};
use kafka_protocol::messages::produce_response::ProduceResponse;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use tokio::sync::mpsc::UnboundedSender;

/// Dispatch a handler result to the response channel.
///
/// This eliminates repetitive error handling boilerplate across all request handlers.
/// It handles both success and error cases uniformly:
/// - On success: wraps the result using the provided wrapper function and sends
/// - On error: wraps the error using the error wrapper function and sends an API-specific error response
///
/// # Type Parameters
/// * `R` - The handler's successful response type
/// * `F` - Handler function type
/// * `W` - Response wrapper function type
/// * `E` - Error wrapper function type
///
/// # Arguments
/// * `handler_name` - Name of the handler for logging
/// * `response_tx` - Channel to send the response
/// * `handler` - The handler function to call
/// * `wrap_response` - Function to wrap successful result into KafkaResponse
/// * `wrap_error` - Function to wrap error code into API-specific KafkaResponse
pub fn dispatch_response<R, F, W, E>(
    handler_name: &str,
    response_tx: UnboundedSender<KafkaResponse>,
    handler: F,
    wrap_response: W,
    wrap_error: E,
) where
    F: FnOnce() -> Result<R, KafkaError>,
    W: FnOnce(R) -> KafkaResponse,
    E: FnOnce(i16) -> KafkaResponse,
{
    // BUG-5: a handler panic (including a Postgres ERROR, which pgrx surfaces as a Rust panic) must
    // not leave the client hanging until its request timeout. Catch the panic, reply
    // UNKNOWN_SERVER_ERROR, then re-raise the original payload so the surrounding subtransaction
    // still rolls back the request's partial writes (see worker::run_request_in_subtransaction).
    // The panic branch does only Rust work (channel send + response construction, Rust allocator) —
    // no Postgres calls — so it is safe to run before FlushErrorState at the subtransaction boundary.
    match catch_unwind(AssertUnwindSafe(handler)) {
        Ok(Ok(result)) => {
            let response = wrap_response(result);
            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send {} response: {}", handler_name, e);
            } else {
                pg_log!("{} response sent successfully", handler_name);
            }
        }
        Ok(Err(e)) => {
            pg_warning!("Failed to handle {} request: {}", handler_name, e);
            let error_response = wrap_error(e.to_kafka_error_code());
            if let Err(send_err) = response_tx.send(error_response) {
                pg_warning!("Failed to send error response: {}", send_err);
            }
        }
        Err(panic) => {
            pg_warning!(
                "{} handler panicked; replying UNKNOWN_SERVER_ERROR and rolling back",
                handler_name
            );
            let error_response = wrap_error(ERROR_UNKNOWN_SERVER_ERROR);
            let _ = response_tx.send(error_response);
            resume_unwind(panic);
        }
    }
}

/// Dispatch a handler that cannot fail (always returns Ok).
///
/// For handlers like ApiVersions that don't use storage and can't fail.
pub fn dispatch_infallible<R, F, W>(
    handler_name: &str,
    response_tx: UnboundedSender<KafkaResponse>,
    handler: F,
    wrap_response: W,
) where
    F: FnOnce() -> R,
    W: FnOnce(R) -> KafkaResponse,
{
    let result = handler();
    let response = wrap_response(result);
    if let Err(e) = response_tx.send(response) {
        pg_warning!("Failed to send {} response: {}", handler_name, e);
    } else {
        pg_log!("{} response sent successfully", handler_name);
    }
}

/// Produce-specific wrapper around [`dispatch_response`] (RA-3).
///
/// The Produce `acks >= 1` path can't go through the generic enum dispatch
/// because it needs a success side-effect (waking long-poll consumers) and the
/// `acks = 0` no-response special case lives next to it. It used to hand-roll the
/// `Ok`/`Err` match and send, which skipped panic safety: a handler panic (a
/// Postgres ERROR surfaces through pgrx as a Rust panic) sent no response and the
/// client hung to its timeout. This helper restores the same `catch_unwind`
/// behaviour as `dispatch_response` — panic → reply UNKNOWN_SERVER_ERROR → resume
/// so the surrounding subtransaction still rolls back — while wiring the Produce
/// response shape and an `on_success` hook.
pub fn dispatch_produce<H, N>(
    response_tx: UnboundedSender<KafkaResponse>,
    correlation_id: i32,
    api_version: i16,
    handler: H,
    on_success: N,
) where
    H: FnOnce() -> Result<ProduceResponse, KafkaError>,
    N: FnOnce(&ProduceResponse),
{
    dispatch_response(
        "Produce",
        response_tx,
        handler,
        move |response| {
            on_success(&response);
            KafkaResponse::Produce {
                correlation_id,
                api_version,
                response,
            }
        },
        move |error_code| KafkaResponse::Produce {
            correlation_id,
            api_version,
            response: crate::kafka::response_builders::build_produce_error_response(error_code),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::error::KafkaError;
    use tokio::sync::mpsc;

    #[test]
    fn test_dispatch_response_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        dispatch_response(
            "Test",
            tx,
            || Ok::<i32, KafkaError>(100),
            |result| KafkaResponse::Error {
                correlation_id: result,
                error_code: 0,
                error_message: None,
            },
            |error_code| KafkaResponse::Error {
                correlation_id: 42,
                error_code,
                error_message: Some("error".to_string()),
            },
        );

        let response = rx.try_recv().expect("Should receive response");
        match response {
            KafkaResponse::Error {
                correlation_id,
                error_code,
                ..
            } => {
                assert_eq!(correlation_id, 100); // We used result as correlation_id in wrapper
                assert_eq!(error_code, 0);
            }
            _ => panic!("Unexpected response type"),
        }
    }

    #[test]
    fn test_dispatch_response_error() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        dispatch_response(
            "Test",
            tx,
            || Err::<i32, KafkaError>(KafkaError::Internal("test error".into())),
            |_result| KafkaResponse::Error {
                correlation_id: 0,
                error_code: 0,
                error_message: None,
            },
            |error_code| KafkaResponse::Error {
                correlation_id: 42,
                error_code,
                error_message: Some("API-specific error".to_string()),
            },
        );

        let response = rx.try_recv().expect("Should receive error response");
        match response {
            KafkaResponse::Error {
                correlation_id,
                error_code,
                error_message,
            } => {
                assert_eq!(correlation_id, 42);
                // Error code for Internal is -1 (see error.rs)
                assert_eq!(error_code, -1);
                assert_eq!(error_message.unwrap(), "API-specific error");
            }
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_dispatch_response_handler_panic_replies_and_resumes() {
        // BUG-5: a panicking handler must still produce a response (so the client is not left
        // hanging) AND re-raise the panic (so the surrounding subtransaction rolls back).
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_response(
                "Test",
                tx,
                || -> Result<i32, KafkaError> { panic!("handler boom") },
                |result| KafkaResponse::Error {
                    correlation_id: result,
                    error_code: 0,
                    error_message: None,
                },
                |error_code| KafkaResponse::Error {
                    correlation_id: 42,
                    error_code,
                    error_message: None,
                },
            );
        }));

        // The panic is re-raised for the caller (subtransaction) to roll back.
        assert!(outcome.is_err());

        // ...but an error response was sent first, so the client is not left hanging.
        match rx.try_recv().expect("error response should have been sent") {
            KafkaResponse::Error {
                correlation_id,
                error_code,
                ..
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(error_code, ERROR_UNKNOWN_SERVER_ERROR);
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_dispatch_infallible() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        dispatch_infallible(
            "Test",
            tx,
            || 42,
            |result| KafkaResponse::Error {
                correlation_id: result,
                error_code: 0,
                error_message: None,
            },
        );

        let response = rx.try_recv().expect("Should receive response");
        match response {
            KafkaResponse::Error { correlation_id, .. } => {
                assert_eq!(correlation_id, 42);
            }
            _ => panic!("Unexpected response type"),
        }
    }

    #[test]
    fn test_dispatch_produce_success_runs_on_success_and_sends() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let ran = std::cell::Cell::new(false);
        dispatch_produce(
            tx,
            3,
            2,
            || Ok(crate::kafka::response_builders::build_produce_error_response(0)),
            |_resp| ran.set(true),
        );
        assert!(ran.get(), "on_success should run on Ok");
        match rx.try_recv().expect("a Produce response should be sent") {
            KafkaResponse::Produce {
                correlation_id,
                api_version,
                ..
            } => {
                assert_eq!(correlation_id, 3);
                assert_eq!(api_version, 2);
            }
            _ => panic!("expected a Produce response"),
        }
    }

    #[test]
    fn test_dispatch_produce_panic_replies_produce_error_and_resumes() {
        // RA-3: a panicking Produce handler must still send a Produce response
        // (client not left hanging) AND re-raise so the subtransaction rolls back.
        let (tx, mut rx) = mpsc::unbounded_channel();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_produce(
                tx,
                7,
                9,
                || -> Result<ProduceResponse, KafkaError> { panic!("produce boom") },
                |_resp| {},
            );
        }));
        assert!(
            outcome.is_err(),
            "panic must be re-raised so the subtransaction rolls back"
        );
        match rx
            .try_recv()
            .expect("a Produce response must have been sent before resume")
        {
            KafkaResponse::Produce {
                correlation_id,
                api_version,
                ..
            } => {
                assert_eq!(correlation_id, 7);
                assert_eq!(api_version, 9);
            }
            _ => panic!("expected a Produce response"),
        }
    }
}
