// Request dispatch helpers for the Kafka protocol handler
//
// This module provides utilities to reduce boilerplate in process_request().
// Each handler follows the same pattern:
// 1. Call handler function
// 2. On success: wrap result and send response
// 3. On error: log, create error response, send
//
// The dispatch_response() function encapsulates this pattern.

use crate::kafka::error::KafkaError;
use crate::kafka::messages::KafkaResponse;
use crate::{pg_log, pg_warning};
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
    match handler() {
        Ok(result) => {
            let response = wrap_response(result);
            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send {} response: {}", handler_name, e);
            } else {
                pg_log!("{} response sent successfully", handler_name);
            }
        }
        Err(e) => {
            pg_warning!("Failed to handle {} request: {}", handler_name, e);
            let error_response = wrap_error(e.to_kafka_error_code());
            if let Err(send_err) = response_tx.send(error_response) {
                pg_warning!("Failed to send error response: {}", send_err);
            }
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
}
