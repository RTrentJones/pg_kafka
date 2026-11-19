//! Running one handler on the DB thread and answering it exactly once.
//!
//! [`dispatch_response`] sends the handler's response on success; on a handler `Err` it
//! sends the API's error response and reports [`RequestOutcome::Rollback`]; on a panic it
//! sends UNKNOWN_SERVER_ERROR and re-raises, so the request's subtransaction rolls back.
//! Every API reaches it through `crate::kafka::api::Call::run`.

use crate::kafka::constants::ERROR_UNKNOWN_SERVER_ERROR;
use crate::kafka::error::KafkaError;
use crate::kafka::messages::KafkaResponse;
use crate::{pg_debug, pg_warning};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use tokio::sync::mpsc::UnboundedSender;

/// What the worker must do with a request's subtransaction once its handler has
/// sent a response.
///
/// A handler that fails part-way through may already have written to the database
/// (e.g. IncrementalAlterConfigs applied resource 1, then failed on resource 2). The
/// client is told the request failed, so those writes must not commit. The request
/// runs inside `worker::run_request_in_subtransaction`, which releases the
/// subtransaction on `Commit` and rolls it back on `Rollback`. Panics (including
/// Postgres ERRORs, which pgrx surfaces as panics) are not an outcome: they still
/// unwind, and the subtransaction wrapper's `catch_unwind` rolls them back.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestOutcome {
    /// Keep the request's writes (they commit with the surrounding transaction).
    Commit,
    /// Discard the request's writes; its error response has already been sent.
    Rollback,
}

/// A closure that returns nothing (maintenance work) keeps its writes.
impl From<()> for RequestOutcome {
    fn from((): ()) -> Self {
        RequestOutcome::Commit
    }
}

/// Run `handler`, wrap its result with `wrap_response` and send it on `response_tx`.
///
/// A handler `Err` is answered with `wrap_error(code)` and returns
/// [`RequestOutcome::Rollback`] (the handler may have written before failing, and the client
/// is told the request failed). A panic in `handler` or `wrap_response` is answered with
/// `wrap_error(UNKNOWN_SERVER_ERROR)` and then re-raised. `handler_name` names the API in
/// the logs.
pub fn dispatch_response<R, F, W, E>(
    handler_name: &str,
    response_tx: &UnboundedSender<KafkaResponse>,
    handler: F,
    wrap_response: W,
    wrap_error: E,
) -> RequestOutcome
where
    F: FnOnce() -> Result<R, KafkaError>,
    W: FnOnce(R) -> KafkaResponse,
    E: FnOnce(i16) -> KafkaResponse,
{
    // A handler panic (a Postgres ERROR surfaces as one) must not leave the client waiting
    // until its request timeout: reply UNKNOWN_SERVER_ERROR, then re-raise the payload so the
    // surrounding subtransaction still rolls back the request's partial writes (see
    // worker::run_request_in_subtransaction). The guard covers the success response builder
    // too, since a builder may read SPI. The panic branch does only Rust work (channel send,
    // enum construction, no Postgres calls), so it is safe before FlushErrorState at the
    // subtransaction boundary.
    let guarded = catch_unwind(AssertUnwindSafe(|| handler().map(wrap_response)));
    match guarded {
        Ok(Ok(response)) => {
            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send {} response: {}", handler_name, e);
            } else {
                pg_debug!("{} response sent successfully", handler_name);
            }
            RequestOutcome::Commit
        }
        Ok(Err(e)) => {
            pg_warning!("Failed to handle {} request: {}", handler_name, e);
            let error_response = wrap_error(e.to_kafka_error_code());
            if let Err(send_err) = response_tx.send(error_response) {
                pg_warning!("Failed to send error response: {}", send_err);
            }
            // Discard any writes the handler made before failing.
            RequestOutcome::Rollback
        }
        Err(panic) => {
            pg_warning!(
                "{} handler panicked; replying UNKNOWN_SERVER_ERROR and rolling back",
                handler_name
            );
            // A closed channel only means the client is gone; the panic is re-raised anyway.
            let _ = response_tx.send(wrap_error(ERROR_UNKNOWN_SERVER_ERROR));
            resume_unwind(panic);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::api::ResponseBody;
    use crate::kafka::error::KafkaError;
    use tokio::sync::mpsc;

    #[test]
    fn test_dispatch_response_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = dispatch_response(
            "Test",
            &tx,
            || Ok::<i32, KafkaError>(100),
            |result| KafkaResponse::new(result, 0, ResponseBody::UnknownApi { error_code: 0 }),
            |error_code| KafkaResponse::new(42, 0, ResponseBody::UnknownApi { error_code }),
        );
        assert_eq!(outcome, RequestOutcome::Commit);

        let response = rx.try_recv().expect("Should receive response");
        match response {
            KafkaResponse {
                correlation_id,
                body: ResponseBody::UnknownApi { error_code, .. },
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

        // A handler error sends the error response, then reports Rollback (an
        // ordinary return, not an unwind) so the request's subtransaction discards
        // any partial writes.
        let outcome = dispatch_response(
            "Test",
            &tx,
            || Err::<i32, KafkaError>(KafkaError::Internal("test error".into())),
            |_result| KafkaResponse::new(0, 0, ResponseBody::UnknownApi { error_code: 0 }),
            |error_code| KafkaResponse::new(42, 0, ResponseBody::UnknownApi { error_code }),
        );
        assert_eq!(outcome, RequestOutcome::Rollback);

        let response = rx.try_recv().expect("Should receive error response");
        match response {
            KafkaResponse {
                correlation_id,
                body: ResponseBody::UnknownApi { error_code },
                ..
            } => {
                assert_eq!(correlation_id, 42);
                // Error code for Internal is -1 (see error.rs)
                assert_eq!(error_code, -1);
            }
            _ => panic!("Expected error response"),
        }
    }

    #[test]
    fn test_unit_converts_to_commit() {
        // Maintenance closures return (); the subtransaction wrapper treats that
        // as Commit.
        assert_eq!(RequestOutcome::from(()), RequestOutcome::Commit);
    }

    #[test]
    fn test_dispatch_response_handler_panic_replies_and_resumes() {
        // A panicking handler must still produce a response (so the client is not left
        // hanging) AND re-raise the panic (so the surrounding subtransaction rolls back).
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = dispatch_response(
                "Test",
                &tx,
                || -> Result<i32, KafkaError> { panic!("handler boom") },
                |result| KafkaResponse::new(result, 0, ResponseBody::UnknownApi { error_code: 0 }),
                |error_code| KafkaResponse::new(42, 0, ResponseBody::UnknownApi { error_code }),
            );
        }));

        // The panic is re-raised for the caller (subtransaction) to roll back.
        assert!(outcome.is_err());

        // ...but an error response was sent first, so the client is not left hanging.
        match rx.try_recv().expect("error response should have been sent") {
            KafkaResponse {
                correlation_id,
                body: ResponseBody::UnknownApi { error_code, .. },
                ..
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(error_code, ERROR_UNKNOWN_SERVER_ERROR);
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_dispatch_response_builder_panic_replies_and_resumes() {
        // A panic in the success response builder (`wrap_response`), not just in the
        // handler, must still reply UNKNOWN_SERVER_ERROR and re-raise, so the client is not
        // left waiting.
        let (tx, mut rx) = mpsc::unbounded_channel();

        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = dispatch_response(
                "Test",
                &tx,
                || Ok::<i32, KafkaError>(7),
                |_result| -> KafkaResponse { panic!("builder boom") },
                |error_code| KafkaResponse::new(42, 0, ResponseBody::UnknownApi { error_code }),
            );
        }));

        // The panic is re-raised for the subtransaction to roll back.
        assert!(outcome.is_err());

        // ...but an error response was sent first, so the client is not left hanging.
        match rx.try_recv().expect("error response should have been sent") {
            KafkaResponse {
                correlation_id,
                body: ResponseBody::UnknownApi { error_code, .. },
                ..
            } => {
                assert_eq!(correlation_id, 42);
                assert_eq!(error_code, ERROR_UNKNOWN_SERVER_ERROR);
            }
            _ => panic!("Expected Error response"),
        }
    }
}
