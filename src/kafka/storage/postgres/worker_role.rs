//! The worker's startup switch to its least-privilege role.

use crate::kafka::storage::privileges::{self, RoleDecision};
use pgrx::bgworkers::BackgroundWorker;
use pgrx::prelude::*;
use pgrx::spi::SpiError;

/// Whether `role` exists, and the privileges (`object:PRIVILEGE`) it lacks.
fn probe_role(role: &str) -> Result<(bool, Vec<String>), SpiError> {
    let checks = privileges::required_worker_privileges();
    let kinds: Vec<&str> = checks.iter().map(|c| c.kind.as_str()).collect();
    let objects: Vec<&str> = checks.iter().map(|c| c.object).collect();
    let privs: Vec<&str> = checks.iter().map(|c| c.privilege).collect();
    BackgroundWorker::transaction(move || {
        Spi::connect(move |client| {
            let exists: bool = client
                .select(
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = $1) AS e",
                    None,
                    &[role.into()],
                )?
                .first()
                .get_by_name("e")?
                .unwrap_or(false);
            if !exists {
                return Ok((false, Vec::new()));
            }
            // One row per missing privilege. to_regclass guards a relation the schema
            // lacks (reported as missing instead of raising an error).
            let table = client.select(
                "SELECT r.obj || ':' || r.priv AS missing
                 FROM unnest($2::text[], $3::text[], $4::text[]) AS r(kind, obj, priv)
                 WHERE NOT CASE r.kind
                     WHEN 'schema' THEN pg_catalog.has_schema_privilege($1::name, r.obj, r.priv)
                     WHEN 'sequence' THEN CASE WHEN pg_catalog.to_regclass(r.obj) IS NULL THEN false
                          ELSE pg_catalog.has_sequence_privilege($1::name, r.obj, r.priv) END
                     WHEN 'function' THEN CASE WHEN pg_catalog.to_regprocedure(r.obj) IS NULL THEN false
                          ELSE pg_catalog.has_function_privilege($1::name, r.obj, r.priv) END
                     ELSE CASE WHEN pg_catalog.to_regclass(r.obj) IS NULL THEN false
                          ELSE pg_catalog.has_table_privilege($1::name, r.obj, r.priv) END
                 END",
                None,
                &[
                    role.into(),
                    kinds.into(),
                    objects.into(),
                    privs.into(),
                ],
            )?;
            let mut missing = Vec::new();
            for row in table {
                missing.push(row.get_by_name::<String, _>("missing")?.unwrap_or_default());
            }
            Ok((true, missing))
        })
    })
}

/// 0.1.0 least privilege: switch the background worker from the bootstrap superuser
/// to `pg_kafka.run_as_role` (default `pgkafka_worker`). Call once, after the schema
/// exists and before serving requests.
///
/// `set_config('role', ..., false)` is `SET ROLE` at session level: it survives the
/// commit of this transaction and every later transaction or subtransaction abort
/// (those only undo GUC changes made inside themselves). The worker keeps the
/// superuser and logs a WARNING when the GUC is blank, the role does not exist (an
/// upgraded binary whose `ALTER EXTENSION pg_kafka UPDATE` has not run yet), or the
/// role lacks any privilege in [`privileges::required_worker_privileges`], so a
/// partially upgraded install keeps working instead of failing requests at runtime.
pub fn apply_worker_role() {
    let Some(role) = privileges::configured_run_as_role() else {
        log!("pg_kafka: pg_kafka.run_as_role is empty; the worker keeps the bootstrap superuser");
        return;
    };

    let decision = match probe_role(&role) {
        Ok((exists, missing)) => privileges::decide_role_switch(Some(role), exists, missing),
        Err(e) => {
            warning!(
                "pg_kafka: could not check pg_kafka.run_as_role '{role}' ({e}); the worker keeps the bootstrap superuser"
            );
            return;
        }
    };

    match decision {
        RoleDecision::Switch(role) => {
            let switched: Result<(), spi::Error> =
                BackgroundWorker::transaction(|| {
                    Spi::run_with_args(
                        "SELECT pg_catalog.set_config('role', $1, false)",
                        &[role.as_str().into()],
                    )
                });
            match switched {
                Ok(()) => log!("pg_kafka: background worker now runs as role '{role}'"),
                Err(e) => warning!(
                    "pg_kafka: could not switch to role '{role}' ({e}); the worker keeps the bootstrap superuser"
                ),
            }
        }
        RoleDecision::MissingRole(role) => warning!(
            "pg_kafka: pg_kafka.run_as_role '{role}' does not exist (run ALTER EXTENSION pg_kafka UPDATE, \
             or set pg_kafka.run_as_role = '' to silence this); the worker keeps the bootstrap superuser"
        ),
        RoleDecision::MissingPrivileges(role, missing) => warning!(
            "pg_kafka: pg_kafka.run_as_role '{role}' lacks {}; the worker keeps the bootstrap superuser",
            missing.join(", ")
        ),
        RoleDecision::Disabled => {}
    }
}
