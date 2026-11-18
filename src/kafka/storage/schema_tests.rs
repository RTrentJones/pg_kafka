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
    assert!(sql.contains("primary key (topic_id, partition_id, partition_offset)"));
}

#[test]
fn bootstrap_indexes_created_at_for_the_retention_sweep() {
    let sql = statements(BOOTSTRAP_SQL);
    assert!(
        sql.contains("create index idx_messages_created_at_brin\non kafka.messages using brin (created_at) with (autosummarize = on)"),
        "the retention sweep's created_at cutoff needs an index"
    );
}

mod partition_bounds {
    use crate::kafka::storage::{order_topic_metadata, IsolationLevel, PartitionBounds};

    #[test]
    fn empty_partition_without_counter_row() {
        let b =
            PartitionBounds::from_raw(None, None, None, None, None, IsolationLevel::ReadCommitted);
        assert_eq!(b, PartitionBounds::default());
    }

    #[test]
    fn hwm_takes_the_larger_of_counter_and_max_row() {
        // Counter ahead of rows (highest aborted rows cleaned up): counter wins (BUG-4).
        let b = PartitionBounds::from_raw(
            Some(10),
            Some(0),
            Some(0),
            Some(5),
            None,
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(b.high_watermark, 10);
        assert_eq!(b.last_stable_offset, 10);
        // Rows that predate the counter: MAX + 1 wins.
        let b = PartitionBounds::from_raw(
            Some(2),
            Some(0),
            Some(0),
            Some(7),
            None,
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(b.high_watermark, 8);
    }

    #[test]
    fn lso_is_first_pending_offset_for_read_committed_only() {
        let rc = PartitionBounds::from_raw(
            Some(10),
            Some(0),
            Some(0),
            Some(9),
            Some(4),
            IsolationLevel::ReadCommitted,
        );
        assert_eq!(rc.last_stable_offset, 4);
        let ru = PartitionBounds::from_raw(
            Some(10),
            Some(0),
            Some(0),
            Some(9),
            Some(4),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(ru.last_stable_offset, 10);
    }

    #[test]
    fn earliest_respects_durable_log_start() {
        // Truncated by DeleteRecords to 6 and now empty: earliest is the log start.
        let b = PartitionBounds::from_raw(
            Some(6),
            Some(6),
            None,
            None,
            None,
            IsolationLevel::ReadCommitted,
        );
        assert_eq!(b.earliest_offset, 6);
        assert_eq!(b.log_start_offset, 6);
        assert_eq!(b.high_watermark, 6);
        // Aborted leading rows cleaned up: first row above the log start.
        let b = PartitionBounds::from_raw(
            Some(5),
            Some(0),
            Some(2),
            Some(4),
            None,
            IsolationLevel::ReadCommitted,
        );
        assert_eq!(b.earliest_offset, 2);
        assert_eq!(b.log_start_offset, 0);
    }

    #[test]
    fn topic_metadata_follows_request_order() {
        let names: Vec<String> = ["b", "missing", "a", "b"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rows = vec![("a".to_string(), 1, 3), ("b".to_string(), 2, 1)];
        let out = order_topic_metadata(&names, rows);
        let got: Vec<(&str, i32, i32)> = out
            .iter()
            .map(|t| (t.name.as_str(), t.id, t.partition_count))
            .collect();
        assert_eq!(got, vec![("b", 2, 1), ("a", 1, 3), ("b", 2, 1)]);
        assert!(order_topic_metadata(&[], vec![]).is_empty());
    }
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
        "create index if not exists idx_messages_created_at_brin\non kafka.messages using brin (created_at) with (autosummarize = on)"
    ));
}
