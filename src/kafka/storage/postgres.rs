// PostgreSQL implementation of the KafkaStore trait
//
// This module implements the storage layer using PostgreSQL and pgrx SPI.
// It assumes it runs within a transaction context managed by the caller.

use crate::kafka::error::KafkaError;
use pgrx::prelude::*;

mod log;
mod maintenance;
mod offsets;
mod producers;
mod roles;
mod topics;
mod transactions;
mod worker_role;

pub use maintenance::{
    RetentionSweepStats, ABORTED_MESSAGE_GRACE, PRODUCER_ID_RETENTION,
    RETENTION_DELETE_BATCH, RETENTION_PASS_BUDGET, SHADOW_DELIVERED_RETENTION,
    TERMINAL_TXN_RETENTION,
};
pub use worker_role::apply_worker_role;
// ADR-001: kafka.messages is time-partitioned, and the hot read statements (produce's
// base-offset probe, Fetch, the partition-bounds/HWM/earliest probes) do not name the
// partition key, so planning them expands every leaf: with ~11 leaves planning cost
// several times the execution. These statements therefore run through prepared plans
// kept for the life of the backend (SPI_keepplan). The plan cache re-plans them
// automatically when a leaf is created or dropped. One cache per backend: the worker's
// single DB thread, or a backend running a pg_extern. Keyed by the SQL text.
thread_local! {
    static PLAN_CACHE: std::cell::RefCell<
        std::collections::HashMap<&'static str, pgrx::spi::OwnedPreparedStatement>,
    > = std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Run a read-only `sql` with `args` (of `types`) through a kept prepared plan.
fn select_cached<'conn>(
    client: &pgrx::spi::SpiClient<'conn>,
    sql: &'static str,
    types: &[pg_sys::PgBuiltInOids],
    args: &[pgrx::datum::DatumWithOid],
) -> std::result::Result<pgrx::spi::SpiTupleTable<'conn>, pgrx::spi::SpiError> {
    PLAN_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if !cache.contains_key(sql) {
            let oids: Vec<PgOid> = types.iter().map(|t| PgOid::BuiltIn(*t)).collect();
            // prepare_mut: executed with read_only = false, so every execution takes a
            // fresh READ COMMITTED snapshot exactly like the one-shot statements did.
            // A read-only execution reuses the active snapshot, which inside a
            // group-commit transaction can predate commits the client already saw
            // (e.g. a Fetch would report a stale high watermark).
            let plan = client.prepare_mut(sql, &oids)?.keep();
            cache.insert(sql, plan);
        }
        let plan = cache.get(sql).expect("plan cached above");
        client.select(plan, None, args)
    })
}

const INT4OID: pg_sys::PgBuiltInOids = pg_sys::PgBuiltInOids::INT4OID;
const INT8OID: pg_sys::PgBuiltInOids = pg_sys::PgBuiltInOids::INT8OID;

/// PostgreSQL-backed implementation of KafkaStore
///
/// This implementation uses pgrx's SPI (Server Programming Interface) to execute
/// SQL queries. All operations assume they run within an active transaction
/// started by BackgroundWorker::transaction() in worker.rs.
pub struct PostgresStore;

impl PostgresStore {
    /// Create a new PostgresStore instance
    pub fn new() -> Self {
        PostgresStore
    }
}

impl Default for PostgresStore {
    fn default() -> Self {
        Self::new()
    }
}
