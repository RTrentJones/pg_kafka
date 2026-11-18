//! Static checks over the extension's SQL scripts (`sql/bootstrap.sql` and the
//! version-upgrade scripts). The scripts only run inside Postgres, so these tests
//! guard their shape from plain `cargo test`.

const BOOTSTRAP_SQL: &str = include_str!("../../../sql/bootstrap.sql");

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
