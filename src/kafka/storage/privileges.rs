//! Least-privilege roles (0.1.0).
//!
//! The extension script (`sql/bootstrap.sql`) creates two NOLOGIN roles:
//!
//! * [`READER_ROLE`] — read-only access to the kafka schema, replacing the former
//!   `GRANT SELECT ... TO PUBLIC` (which exposed every stored message to every role
//!   in the database). Grant it to the roles that should read pg_kafka data.
//! * [`WORKER_ROLE`] — exactly the privileges the background worker's SQL needs. Once
//!   the schema exists the worker switches to the role named by `pg_kafka.run_as_role`
//!   (default [`WORKER_ROLE`]) instead of running everything as the bootstrap
//!   superuser.
//!
//! The worker's grants are derived from the statements it issues
//! (`storage/postgres.rs`, `shadow/store.rs`, the retention sweep): [`WORKER_TABLE_GRANTS`]
//! lists them, `bootstrap.sql` and the upgrade script grant exactly that, and the unit
//! tests check both the scripts and the statement text against this list, so a new
//! statement on a new table (or with a new verb) fails `cargo test` until it is granted.
//!
//! Role names cannot start with `pg_` (a reserved prefix), hence `pgkafka_*`.

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

/// Read-only role (replaces the PUBLIC SELECT grants).
pub const READER_ROLE: &str = "pgkafka_reader";
/// Least-privilege role for the background worker's SQL.
pub const WORKER_ROLE: &str = "pgkafka_worker";

/// Table privileges the worker needs, per table in the `kafka` schema.
///
/// * DELETE needs SELECT for its WHERE clause; `INSERT ... ON CONFLICT DO UPDATE`
///   needs UPDATE.
/// * `partition_offsets` and `shadow_metrics` are only inserted/upserted, never
///   deleted directly (rows go with their topic through ON DELETE CASCADE, which runs
///   with the table owner's rights).
/// * `shadow_config` is only read (operators manage it).
pub const WORKER_TABLE_GRANTS: &[(&str, &[&str])] = &[
    // Runtime metrics, flushed by one upsert per tick (kafka/metrics.rs FLUSH_SQL).
    ("kafka.broker_api_stats", &["SELECT", "INSERT", "UPDATE"]),
    ("kafka.broker_gauges", &["SELECT", "INSERT", "UPDATE"]),
    ("kafka.topics", &["SELECT", "INSERT", "UPDATE", "DELETE"]),
    ("kafka.messages", &["SELECT", "INSERT", "UPDATE", "DELETE"]),
    // ADR-001: the retention sweep's row delete in the DEFAULT time partition (every
    // other leaf is reached through kafka.messages).
    ("kafka.messages_default", &["SELECT", "DELETE"]),
    ("kafka.partition_offsets", &["SELECT", "INSERT", "UPDATE"]),
    (
        "kafka.consumer_offsets",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    (
        "kafka.producer_ids",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    (
        "kafka.producer_sequences",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    (
        "kafka.transactions",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    (
        "kafka.txn_pending_offsets",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    ("kafka.shadow_config", &["SELECT"]),
    (
        "kafka.shadow_tracking",
        &["SELECT", "INSERT", "UPDATE", "DELETE"],
    ),
    ("kafka.shadow_metrics", &["SELECT", "INSERT", "UPDATE"]),
];

/// Sequences behind the SERIAL/BIGSERIAL columns the worker inserts into, plus the
/// `nextval(pg_get_serial_sequence('kafka.producer_ids', ...))` reallocation (RV-13).
pub const WORKER_SEQUENCE_GRANTS: &[&str] = &[
    "kafka.topics_id_seq",
    "kafka.messages_global_offset_seq",
    "kafka.producer_ids_producer_id_seq",
];

/// SECURITY DEFINER functions the worker calls (ADR-001 time partitions): all
/// partition DDL runs inside them, as the extension owner, so the worker role itself
/// needs no DDL rights. Each validates that it only touches leaves of kafka.messages.
/// Signatures as written in the GRANT statements (lower case).
pub const WORKER_FUNCTION_GRANTS: &[&str] = &[
    "kafka.message_partition_contents(regclass)",
    "kafka.ensure_message_partitions(int, int)",
    "kafka.drop_message_partition(regclass, int[])",
];

/// [`WORKER_ROLE`] as the GUC's compiled-in default.
const WORKER_ROLE_C: &std::ffi::CStr = c"pgkafka_worker";

static RUN_AS_ROLE: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(WORKER_ROLE_C));

/// Register `pg_kafka.run_as_role`. Called from `config::init()`.
pub fn register_gucs() {
    GucRegistry::define_string_guc(
        c"pg_kafka.run_as_role",
        c"Role the background worker switches to once the schema exists",
        c"Default 'pgkafka_worker', created by the extension with only the privileges the worker's SQL needs. Empty = keep running as the bootstrap superuser. If the role does not exist or lacks a required privilege (e.g. the binary was upgraded but ALTER EXTENSION pg_kafka UPDATE has not run yet) the worker logs a WARNING and keeps the superuser. Requires restart.",
        &RUN_AS_ROLE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

/// The configured role, `None` when the switch is disabled (unset or blank).
pub fn configured_run_as_role() -> Option<String> {
    normalize_role(RUN_AS_ROLE.get().and_then(|c| c.into_string().ok()))
}

/// Trimmed role name; unset or blank disables the switch.
pub fn normalize_role(raw: Option<String>) -> Option<String> {
    raw.map(|r| r.trim().to_string()).filter(|r| !r.is_empty())
}

/// Every privilege the worker role must hold, as `(kind, object, privilege)` rows for
/// the startup check. `kind` is `schema`, `table`, `sequence` or `function`. One row per privilege:
/// `has_table_privilege` with a comma list is true if ANY of them is held.
pub fn required_worker_privileges() -> Vec<(&'static str, &'static str, &'static str)> {
    let mut out = vec![("schema", "kafka", "USAGE")];
    for (table, privs) in WORKER_TABLE_GRANTS {
        for p in *privs {
            out.push(("table", table, p));
        }
    }
    for seq in WORKER_SEQUENCE_GRANTS {
        out.push(("sequence", seq, "USAGE"));
    }
    for func in WORKER_FUNCTION_GRANTS {
        out.push(("function", func, "EXECUTE"));
    }
    out
}

/// What the worker does with `pg_kafka.run_as_role` at startup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleDecision {
    /// The GUC is blank: keep the bootstrap superuser (operator's explicit choice).
    Disabled,
    /// Switch to this role.
    Switch(String),
    /// The role does not exist (e.g. `ALTER EXTENSION pg_kafka UPDATE` not run yet):
    /// keep the superuser and warn, so an upgraded binary on an old schema still works.
    MissingRole(String),
    /// The role exists but lacks these privileges: keep the superuser and warn rather
    /// than fail requests at runtime.
    MissingPrivileges(String, Vec<String>),
}

/// Decide the startup role switch from the configured role, whether it exists, and
/// the privileges it is missing (`object:PRIVILEGE` strings).
pub fn decide_role_switch(
    configured: Option<String>,
    role_exists: bool,
    missing: Vec<String>,
) -> RoleDecision {
    match configured {
        None => RoleDecision::Disabled,
        Some(role) if !role_exists => RoleDecision::MissingRole(role),
        Some(role) if !missing.is_empty() => RoleDecision::MissingPrivileges(role, missing),
        Some(role) => RoleDecision::Switch(role),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOOTSTRAP_SQL: &str = include_str!("../../../sql/bootstrap.sql");
    const UPGRADE_0_1_0_SQL: &str = include_str!("../../../sql/pg_kafka--0.0.0--0.1.0.sql");
    /// The SQL-issuing files on the worker's path. Add a file here when it starts
    /// issuing SQL from the background worker.
    const WORKER_SQL_SOURCES: &[(&str, &str)] = &[
        ("storage/postgres.rs", include_str!("postgres.rs")),
        ("shadow/store.rs", include_str!("../shadow/store.rs")),
        ("shadow/outbox.rs", include_str!("../shadow/outbox.rs")),
        ("metrics.rs", include_str!("../metrics.rs")),
        ("worker.rs", include_str!("../../worker.rs")),
    ];

    fn granted(table: &str, privilege: &str) -> bool {
        WORKER_TABLE_GRANTS
            .iter()
            .any(|(t, privs)| *t == table && privs.contains(&privilege))
    }

    /// `(privileges, object)` of every `GRANT ... TO <role>;` line in `sql`.
    pub(crate) fn grants_to(sql: &str, role: &str) -> Vec<(Vec<String>, String)> {
        let suffix = format!(" to {};", role);
        sql.lines()
            .map(|l| l.trim().to_lowercase())
            .filter(|l| l.starts_with("grant ") && l.ends_with(&suffix))
            .filter_map(|l| {
                let body = &l["grant ".len()..l.len() - suffix.len()];
                let (privs, object) = body.split_once(" on ")?;
                let object = object
                    .trim_start_matches("sequence ")
                    .trim_start_matches("schema ")
                    .trim_start_matches("function ")
                    .trim()
                    .to_string();
                // Drop column lists: "select (a, b)" is one SELECT privilege.
                let mut flat = String::new();
                let mut depth = 0;
                for c in privs.chars() {
                    match c {
                        '(' => depth += 1,
                        ')' => depth -= 1,
                        _ if depth == 0 => flat.push(c),
                        _ => {}
                    }
                }
                let privs = flat.split(',').map(|p| p.trim().to_uppercase()).collect();
                Some((privs, object))
            })
            .collect()
    }

    #[test]
    fn bootstrap_grants_exactly_the_worker_privileges() {
        let grants = grants_to(BOOTSTRAP_SQL, WORKER_ROLE);
        let mut flat: Vec<(String, String)> = grants
            .iter()
            .flat_map(|(privs, obj)| privs.iter().map(move |p| (obj.clone(), p.clone())))
            .collect();
        flat.sort();
        let mut expected: Vec<(String, String)> = required_worker_privileges()
            .into_iter()
            .map(|(_, obj, p)| (obj.to_string(), p.to_string()))
            .collect();
        expected.sort();
        assert_eq!(flat, expected);
    }

    /// A 0.0.0 install upgraded to 0.1.0 grants exactly what a fresh 0.1.0 install does.
    #[test]
    fn upgrade_script_grants_match_bootstrap() {
        for role in [READER_ROLE, WORKER_ROLE] {
            let mut fresh = grants_to(BOOTSTRAP_SQL, role);
            let mut upgraded = grants_to(UPGRADE_0_1_0_SQL, role);
            fresh.sort();
            upgraded.sort();
            assert_eq!(fresh, upgraded, "grants to {} differ", role);
        }
        assert!(
            UPGRADE_0_1_0_SQL.contains("REVOKE EXECUTE ON FUNCTION kafka.replay_shadow_messages")
        );
    }

    #[test]
    fn bootstrap_grants_nothing_to_public() {
        for line in BOOTSTRAP_SQL.lines() {
            let l = line.trim().to_lowercase();
            if l.starts_with("--") {
                continue;
            }
            assert!(
                !(l.starts_with("grant ") && l.contains(" to public")),
                "PUBLIC grant left in bootstrap.sql: {}",
                line
            );
        }
        assert!(BOOTSTRAP_SQL.contains("REVOKE EXECUTE ON FUNCTION kafka.replay_shadow_messages"));
    }

    #[test]
    fn reader_role_is_read_only() {
        let grants = grants_to(BOOTSTRAP_SQL, READER_ROLE);
        assert!(!grants.is_empty());
        for (privs, obj) in grants {
            for p in privs {
                assert!(
                    p == "USAGE" || p.starts_with("SELECT"),
                    "reader role granted {} on {}",
                    p,
                    obj
                );
            }
        }
    }

    /// Every `kafka.<table>` the worker's SQL touches is granted the matching verb:
    /// INSERT INTO -> INSERT, UPDATE -> UPDATE, DELETE FROM -> DELETE, ON CONFLICT ...
    /// DO UPDATE -> UPDATE on the insert target, and any reference -> SELECT.
    #[test]
    fn worker_grants_cover_every_statement_it_issues() {
        let mut checked = 0;
        for (file, src) in WORKER_SQL_SOURCES {
            let lower = src.to_lowercase();
            let mut i = 0;
            while let Some(pos) = lower[i..].find("kafka.") {
                let start = i + pos;
                i = start + "kafka.".len();
                let name: String = lower[i..]
                    .chars()
                    .take_while(|c| c.is_ascii_lowercase() || *c == '_')
                    .collect();
                if name.is_empty() {
                    continue;
                }
                // Only SQL text: `kafka.` preceded by a SQL keyword or punctuation,
                // not a Rust path (`crate::kafka::...` has no dot) or a doc comment.
                let before = lower[..start].trim_end();
                let sql_context = [
                    "from", "into", "update", "join", "using", "exists", "on", "(", ",", "'",
                ]
                .iter()
                .any(|k| before.ends_with(k));
                if !sql_context {
                    continue;
                }
                let table = format!("kafka.{}", name);
                // Views and functions are not tables the worker writes.
                if [
                    "kafka.shadow_status",
                    "kafka.metrics",
                    "kafka.replay_shadow_messages",
                    "kafka.message_partitions",
                    "kafka.message_partition_contents",
                    "kafka.ensure_message_partitions",
                    "kafka.drop_message_partition",
                ]
                .contains(&table.as_str())
                {
                    continue;
                }
                // Skip mentions inside line comments.
                let line_start = lower[..start].rfind('\n').map_or(0, |p| p + 1);
                if lower[line_start..start].contains("//") {
                    continue;
                }
                let verb = if before.ends_with("into") {
                    Some("INSERT")
                } else if before.ends_with("update") {
                    Some("UPDATE")
                } else if before.ends_with("delete from") {
                    Some("DELETE")
                } else {
                    None
                };
                if let Some(v) = verb {
                    assert!(
                        granted(&table, v),
                        "{}: {} on {} is not granted to {}",
                        file,
                        v,
                        table,
                        WORKER_ROLE
                    );
                }
                checked += 1;
                assert!(
                    granted(&table, "SELECT"),
                    "{}: {} is used but SELECT is not granted to {}",
                    file,
                    table,
                    WORKER_ROLE
                );
                if verb == Some("INSERT") {
                    let stmt_end = lower[i..].find("\",").map_or(lower.len(), |p| i + p);
                    if lower[i..stmt_end].contains("do update") {
                        assert!(
                            granted(&table, "UPDATE"),
                            "{}: upsert on {} needs UPDATE",
                            file,
                            table
                        );
                    }
                }
            }
        }
        // Guard against the scan silently matching nothing.
        assert!(checked > 50, "only {} SQL table references found", checked);
    }

    #[test]
    fn required_privileges_list_one_row_per_privilege() {
        let rows = required_worker_privileges();
        assert_eq!(rows[0], ("schema", "kafka", "USAGE"));
        assert!(rows.contains(&("table", "kafka.messages", "DELETE")));
        assert!(rows.contains(&("sequence", "kafka.producer_ids_producer_id_seq", "USAGE")));
        assert!(rows.contains(&(
            "function",
            "kafka.drop_message_partition(regclass, int[])",
            "EXECUTE"
        )));
        assert!(rows.iter().all(|(_, _, p)| !p.contains(',')));
    }

    /// Every `kafka.<function>(` the worker's SQL calls is either granted to the worker
    /// role (SECURITY DEFINER partition functions) or executable by PUBLIC
    /// (`kafka.message_partitions()`, a catalog read). The partition DDL functions must
    /// never be left executable by PUBLIC.
    #[test]
    fn worker_partition_functions_are_granted_and_not_public() {
        let granted: Vec<String> = WORKER_FUNCTION_GRANTS
            .iter()
            .map(|f| f.split('(').next().unwrap().to_string())
            .collect();
        for (_, src) in WORKER_SQL_SOURCES {
            let lower = src.to_lowercase();
            for f in [
                "kafka.message_partition_contents(",
                "kafka.ensure_message_partitions(",
                "kafka.drop_message_partition(",
            ] {
                if lower.contains(f) {
                    assert!(
                        granted.contains(&f.trim_end_matches('(').to_string()),
                        "{} not granted",
                        f
                    );
                }
            }
        }
        let bootstrap = BOOTSTRAP_SQL.to_lowercase();
        for f in [
            "kafka.message_partition_contents(regclass)",
            "kafka.create_message_partition(timestamptz, timestamptz)",
            "kafka.ensure_message_partitions(int, int)",
            "kafka.drop_message_partition(regclass, int[])",
        ] {
            assert!(
                bootstrap.contains(&format!("revoke execute on function {} from public;", f)),
                "{} must be revoked from PUBLIC in bootstrap.sql",
                f
            );
        }
        // Every partition DDL function pins its search_path.
        for def in bootstrap.split("create function kafka.").skip(1) {
            let header = def.split("$$").next().unwrap_or("");
            if header.contains("security definer") {
                assert!(
                    header.contains("set search_path = pg_catalog, pg_temp"),
                    "SECURITY DEFINER function without a fixed search_path: {}",
                    def.lines().next().unwrap_or("")
                );
            }
        }
    }

    #[test]
    fn role_name_normalization() {
        assert_eq!(normalize_role(None), None);
        assert_eq!(normalize_role(Some("  ".into())), None);
        assert_eq!(normalize_role(Some(" r1 ".into())), Some("r1".to_string()));
    }

    #[test]
    fn role_switch_decisions() {
        assert_eq!(
            decide_role_switch(None, true, vec![]),
            RoleDecision::Disabled
        );
        assert_eq!(
            decide_role_switch(Some("r".into()), false, vec![]),
            RoleDecision::MissingRole("r".into())
        );
        assert_eq!(
            decide_role_switch(Some("r".into()), true, vec!["kafka.topics:INSERT".into()]),
            RoleDecision::MissingPrivileges("r".into(), vec!["kafka.topics:INSERT".into()])
        );
        assert_eq!(
            decide_role_switch(Some("r".into()), true, vec![]),
            RoleDecision::Switch("r".into())
        );
    }

    #[test]
    fn worker_role_is_the_guc_default_and_not_reserved() {
        assert!(!WORKER_ROLE.starts_with("pg_"));
        assert!(!READER_ROLE.starts_with("pg_"));
        // The GUC's compiled-in default (RUN_AS_ROLE boot value).
        assert_eq!(WORKER_ROLE_C.to_str().unwrap(), WORKER_ROLE);
    }
}
