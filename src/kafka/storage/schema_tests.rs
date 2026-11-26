//! Static checks over the extension's SQL scripts (`sql/bootstrap.sql` and the
//! version-upgrade scripts). The scripts only run inside Postgres, so these tests
//! guard their shape from plain `cargo test`.

const BOOTSTRAP_SQL: &str = include_str!("../../../sql/bootstrap.sql");
const UPGRADE_0_1_0_SQL: &str = include_str!("../../../sql/pg_kafka--0.0.0--0.1.0.sql");
const CARGO_TOML: &str = include_str!("../../../Cargo.toml");

/// Lower-cased script with `--` comments stripped, so assertions match statements only.
fn statements(sql: &str) -> String {
    sql.lines()
        .map(|line| match line.find("--") {
            Some(i) => &line[..i],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
        .to_lowercase()
}

#[test]
fn bootstrap_has_no_index_duplicating_the_messages_primary_key() {
    // The primary key (topic_id, partition_id, partition_offset) already provides
    // this btree; a second identical index only doubles write amplification.
    let sql = statements(BOOTSTRAP_SQL);
    assert!(
        !sql.contains("idx_messages_topic_partition_offset"),
        "the duplicate of the kafka.messages primary key must not be created"
    );
    // Time-partitioned (ADR-001): no primary key on the parent; every leaf carries the
    // unique (topic_id, partition_id, partition_offset) btree instead.
    assert!(sql.contains(") partition by range (created_at);"));
    assert!(sql.contains(
        "create unique index messages_default_offset_key\non kafka.messages_default (topic_id, partition_id, partition_offset);"
    ));
    assert!(sql.contains("on kafka.%i (topic_id, partition_id, partition_offset)"));
    assert!(
        sql.contains("create table kafka.messages_default partition of kafka.messages default;")
    );
}

/// The SQL bodies of the partition functions, keyed by name, in script order.
fn partition_functions(sql: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for part in sql.split("CREATE FUNCTION kafka.").skip(1) {
        let name: String = part.chars().take_while(|c| *c != '(').collect();
        if !name.contains("message_partition") {
            continue;
        }
        // Header + body up to the closing `$$;`.
        let mut seen = 0;
        let mut end = part.len();
        for (i, _) in part.match_indices("$$") {
            seen += 1;
            if seen == 2 {
                end = i;
                break;
            }
        }
        out.push((name, part[..end].to_string()));
    }
    out
}

#[test]
fn upgrade_script_defines_the_partition_functions_exactly_as_bootstrap() {
    let fresh = partition_functions(BOOTSTRAP_SQL);
    let upgraded = partition_functions(UPGRADE_0_1_0_SQL);
    let names: Vec<&str> = fresh.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(
        names,
        vec![
            "message_partitions",
            "message_partition_contents",
            "create_message_partition",
            "ensure_message_partitions",
            "drop_message_partition"
        ]
    );
    assert_eq!(fresh, upgraded);
}

#[test]
fn upgrade_script_converts_the_populated_table_into_a_leaf() {
    let sql = statements(UPGRADE_0_1_0_SQL);
    assert!(sql.contains("alter table kafka.messages rename to messages_legacy;"));
    assert!(sql.contains(
        "alter table kafka.messages attach partition kafka.messages_legacy for values from (minvalue) to (%l)"
    ));
    // The global_offset sequence moves to the new parent (dropping the legacy leaf
    // must not drop it), and the 0.0.0 PUBLIC grant does not survive on the leaf.
    assert!(sql.contains(
        "alter sequence kafka.messages_global_offset_seq owned by kafka.messages.global_offset;"
    ));
    assert!(sql.contains("revoke all on kafka.messages_legacy from public;"));
    // The views over the renamed table are rebuilt on the new parent.
    assert!(sql.contains("drop view kafka.shadow_status;"));
    assert!(sql.contains("drop view kafka.metrics;"));
    assert_eq!(sql.matches("create view kafka.shadow_status as").count(), 1);
    assert_eq!(sql.matches("create view kafka.metrics as").count(), 1);
}

#[test]
fn security_definer_partition_functions_pin_search_path_and_bound_lock_waits() {
    for (name, body) in partition_functions(BOOTSTRAP_SQL) {
        let b = body.to_lowercase();
        if b.contains("security definer") {
            assert!(
                b.contains("set search_path = pg_catalog, pg_temp"),
                "{name} lacks a fixed search_path"
            );
        }
        // DDL waits at most 2 s per lock, or the caller's lower lock_timeout: set in the
        // body, and reverted on return by the function's SET clause (search_path).
        if b.contains("create table") || b.contains("drop table") {
            assert!(
                b.contains("perform set_config('lock_timeout',")
                    && b.contains("< interval '2s'")
                    && b.contains("else '2s' end, true);"),
                "{name} DDL without its bounded lock_timeout"
            );
            assert!(
                b.contains("\nset search_path"),
                "{name} sets lock_timeout without a SET clause to revert it"
            );
        }
    }
}

#[test]
fn bootstrap_indexes_created_at_for_the_retention_sweep() {
    let sql = statements(BOOTSTRAP_SQL);
    assert!(
        sql.contains("create index idx_messages_created_at_brin\non kafka.messages using brin (created_at) with (autosummarize = on)"),
        "the retention sweep's created_at cutoff needs an index"
    );
}

#[test]
fn crate_version_has_an_upgrade_script_from_0_0_0() {
    // The extension version is the crate version (pg_kafka.control: @CARGO_VERSION@).
    assert!(CARGO_TOML.contains("\nversion = \"0.1.0\""));
    assert_eq!(env!("CARGO_PKG_VERSION"), "0.1.0");
    assert!(
        UPGRADE_0_1_0_SQL.contains("\\quit"),
        "the script must refuse to run under psql"
    );
}

#[test]
fn upgrade_script_applies_the_bootstrap_index_changes() {
    let sql = statements(UPGRADE_0_1_0_SQL);
    assert!(sql.contains("drop index if exists kafka.idx_messages_topic_partition_offset"));
    assert!(sql.contains(
        "create index idx_messages_created_at_brin\non kafka.messages using brin (created_at) with (autosummarize = on)"
    ));
}
