//! The PostgreSQL storage implementation, over pgrx SPI.
//!
//! [`PostgresStore`] implements every storage role (see `roles`); the SQL for each
//! concern lives in its own submodule as inherent methods. Every method assumes it runs
//! inside a transaction the caller (the background worker, or a SQL function)
//! manages.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use crate::kafka::error::KafkaError;
use pgrx::prelude::*;
use pgrx::spi::{OwnedPreparedStatement, SpiClient, SpiError, SpiTupleTable};

mod log;
mod maintenance;
mod offsets;
mod producers;
mod roles;
mod topics;
mod transactions;
mod worker_role;

pub use maintenance::{RetentionSweepStats, ABORTED_MESSAGE_GRACE};
pub use worker_role::apply_worker_role;

// ADR-001: kafka.messages is time-partitioned, and the hot read statements (produce's
// base-offset probe, Fetch, the partition-bounds/HWM/earliest probes) do not name the
// partition key, so planning them expands every leaf: with ~11 leaves planning costs
// several times the execution. These statements therefore run through prepared plans
// kept for the life of the backend (SPI_keepplan). The plan cache re-plans them
// automatically when a leaf is created or dropped. One cache per backend: the worker's
// single DB thread, or a backend running a pg_extern. Keyed by the SQL text.
thread_local! {
    static PLAN_CACHE: RefCell<HashMap<&'static str, OwnedPreparedStatement>> =
        RefCell::new(HashMap::new());
}

/// Run a read-only `sql` with `args` (of `types`) through a kept prepared plan.
fn select_cached<'conn>(
    client: &SpiClient<'conn>,
    sql: &'static str,
    types: &[PgBuiltInOids],
    args: &[pgrx::datum::DatumWithOid],
) -> Result<SpiTupleTable<'conn>, SpiError> {
    PLAN_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let plan = match cache.entry(sql) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let oids: Vec<PgOid> = types.iter().map(|t| PgOid::BuiltIn(*t)).collect();
                // prepare_mut: executed with read_only = false, so every execution takes
                // a fresh READ COMMITTED snapshot, like a one-shot statement. A read-only
                // execution reuses the active snapshot, which inside a group-commit
                // transaction can predate commits the client already saw (a Fetch would
                // report a stale high watermark).
                entry.insert(client.prepare_mut(sql, &oids)?.keep())
            }
        };
        client.select(&*plan, None, args)
    })
}

const INT4OID: PgBuiltInOids = PgBuiltInOids::INT4OID;
const INT8OID: PgBuiltInOids = PgBuiltInOids::INT8OID;

/// Map any error of storage operation `op` to `KafkaError::Internal("<op> failed: ...")`.
fn internal(op: &'static str) -> impl FnOnce(KafkaError) -> KafkaError {
    move |e| KafkaError::Internal(format!("{op} failed: {e}"))
}

/// Like [`internal`], but pass through the errors `keep` selects: the typed errors an
/// operation reports to its caller (fencing, unknown ids, ...).
fn internal_unless(
    op: &'static str,
    keep: fn(&KafkaError) -> bool,
) -> impl FnOnce(KafkaError) -> KafkaError {
    move |e| {
        if keep(&e) {
            e
        } else {
            KafkaError::Internal(format!("{op} failed: {e}"))
        }
    }
}

/// The errors a transaction operation reports as such.
fn is_txn_error(e: &KafkaError) -> bool {
    matches!(
        e,
        KafkaError::TransactionalIdNotFound { .. }
            | KafkaError::ProducerFenced { .. }
            | KafkaError::InvalidProducerIdMapping { .. }
            | KafkaError::InvalidTxnState { .. }
    )
}

/// The PostgreSQL-backed store. Stateless: all state is in the `kafka` schema (and the
/// backend's plan cache).
#[derive(Debug, Default, Clone, Copy)]
pub struct PostgresStore;

impl PostgresStore {
    /// A store handle.
    pub fn new() -> Self {
        PostgresStore
    }
}
