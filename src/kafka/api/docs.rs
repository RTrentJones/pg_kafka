//! Documentation generated from the API registry, with a drift check.
//!
//! The docs carry generated regions between marker comments; this test renders each
//! region from [`APIS`] and fails when the committed text differs. Run
//!
//! ```text
//! PG_KAFKA_UPDATE_DOCS=1 cargo test --lib generated_api_docs
//! ```
//!
//! to rewrite the regions in place (`scripts/gen-docs.py --write` does that too, along
//! with the GUC and test-count regions it owns).
//!
//! Marker syntax, inline or spanning lines:
//!
//! ```text
//! <!--gen:KIND-->...<!--/gen-->          e.g. <!--gen:api-count-->28<!--/gen-->
//! <!--gen:KIND:ARG-->...<!--/gen-->      e.g. <!--gen:api-versions:Fetch-->4-11<!--/gen-->
//! ```
//!
//! Kinds starting with `api-` belong to this test; every other kind (`guc-*`, `tests-*`)
//! belongs to `scripts/gen-docs.py`, which in turn ignores the `api-*` kinds.
//!
//! | kind | renders |
//! |------|---------|
//! | `api-count` | number of registered APIs |
//! | `api-coverage` | `N of ~50 standard Kafka APIs (P%)` |
//! | `api-versions:Name` | `min-max` of API `Name` |
//! | `api-vversions:Name` | `vmin-vmax` of API `Name` |
//! | `api-table` | the README table (key order) |

use super::*;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

/// The "~50 standard Kafka APIs" the coverage figures are quoted against (the client-facing
/// APIs of a single-node broker; KAFKA_PROTOCOL_COVERAGE.md lists them).
const STANDARD_API_COUNT: usize = 50;

/// One-line description per API for the README table. A registered API without an entry
/// here fails the test, so a new API cannot land with an undocumented row.
const DESCRIPTIONS: &[(&str, &str)] = &[
    ("Produce", "Write messages to the database"),
    ("Fetch", "Read messages (RecordBatch v2), with long polling"),
    ("ListOffsets", "Earliest/latest/by-timestamp offsets"),
    ("Metadata", "Broker and topic metadata"),
    ("OffsetCommit", "Commit consumer offsets"),
    ("OffsetFetch", "Retrieve committed offsets"),
    (
        "FindCoordinator",
        "Locate the group / transaction coordinator",
    ),
    ("JoinGroup", "Join a consumer group"),
    ("Heartbeat", "Keep group membership alive"),
    ("LeaveGroup", "Leave a consumer group"),
    ("SyncGroup", "Distribute partition assignments"),
    ("DescribeGroups", "Consumer group state and members"),
    ("ListGroups", "List consumer groups"),
    (
        "SaslHandshake",
        "Negotiate the SASL mechanism (SASL listeners; v0 answered UNSUPPORTED_VERSION)",
    ),
    ("ApiVersions", "Protocol version negotiation"),
    ("CreateTopics", "Create topics"),
    ("DeleteTopics", "Delete topics and their messages"),
    ("DeleteRecords", "Truncate partitions below an offset"),
    (
        "InitProducerId",
        "Allocate a producer id (idempotence, transactions)",
    ),
    ("AddPartitionsToTxn", "Register partitions in a transaction"),
    ("AddOffsetsToTxn", "Add a group's offsets to a transaction"),
    ("EndTxn", "Commit or abort a transaction"),
    ("TxnOffsetCommit", "Commit offsets inside a transaction"),
    (
        "DescribeConfigs",
        "Report the topic configs pg_kafka honors",
    ),
    (
        "SaslAuthenticate",
        "PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 exchange",
    ),
    ("CreatePartitions", "Add partitions to existing topics"),
    ("DeleteGroups", "Delete consumer groups"),
    (
        "IncrementalAlterConfigs",
        "Set per-topic `retention.ms` (other keys rejected)",
    ),
];

/// The files that may carry generated regions (relative to the crate root).
fn doc_files(root: &Path) -> Vec<PathBuf> {
    let mut files = vec![root.join("README.md"), root.join("CLAUDE.md")];
    let mut docs: Vec<PathBuf> = std::fs::read_dir(root.join("docs"))
        .expect("docs/ exists")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "md"))
        .collect();
    docs.sort();
    files.extend(docs);
    files
}

fn api_by_name(name: &str) -> Option<&'static ApiInfo> {
    APIS.iter().find(|a| a.name == name)
}

fn render_table() -> String {
    let mut apis: Vec<&ApiInfo> = APIS.iter().collect();
    apis.sort_by_key(|a| a.key);
    let mut out = String::from(
        "| API | Key | Versions | Served by | Description |\n\
         |-----|-----|----------|-----------|-------------|\n",
    );
    for a in apis {
        let desc = DESCRIPTIONS.iter().find(|(n, _)| *n == a.name).map_or_else(
            || panic!("no README description for API {}", a.name),
            |(_, d)| *d,
        );
        let served = match a.served_by {
            ServedBy::Worker(Lane::Main) => "DB thread",
            ServedBy::Worker(Lane::Liveness) => "DB thread (liveness lane)",
            ServedBy::NetworkThread => "network thread",
        };
        writeln!(
            out,
            "| {} | {} | {}-{} | {served} | {desc} |",
            a.name, a.key, a.min_version, a.max_version
        )
        .expect("writing to a String cannot fail");
    }
    out
}

/// Render one region, `None` for a kind this test does not own.
fn render(kind: &str, arg: Option<&str>) -> Option<Result<String, String>> {
    if !kind.starts_with("api-") {
        return None;
    }
    let api = |arg: Option<&str>| {
        let name = arg.ok_or_else(|| format!("{kind} needs an API name"))?;
        api_by_name(name).ok_or_else(|| format!("{kind}: no registered API named {name}"))
    };
    Some(match kind {
        "api-count" => Ok(APIS.len().to_string()),
        "api-coverage" => Ok(format!(
            "{} of ~{} standard Kafka APIs ({}%)",
            APIS.len(),
            STANDARD_API_COUNT,
            APIS.len() * 100 / STANDARD_API_COUNT
        )),
        "api-versions" => api(arg).map(|a| format!("{}-{}", a.min_version, a.max_version)),
        "api-vversions" => api(arg).map(|a| format!("v{}-v{}", a.min_version, a.max_version)),
        // A block region: keep it on its own lines.
        "api-table" => Ok(format!("\n{}", render_table())),
        other => Err(format!("unknown generated-doc kind {other}")),
    })
}

/// Rewrite every region this test owns; returns the new text and the errors found.
fn regenerate(text: &str) -> (String, Vec<String>) {
    const OPEN: &str = "<!--gen:";
    const CLOSE: &str = "<!--/gen-->";
    let mut out = String::with_capacity(text.len());
    let mut errors = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find(OPEN) {
        let after_open = &rest[start + OPEN.len()..];
        let Some(tag_end) = after_open.find("-->") else {
            errors.push("unterminated <!--gen: marker".into());
            break;
        };
        let tag = &after_open[..tag_end];
        let body_start = start + OPEN.len() + tag_end + 3;
        let Some(body_len) = rest[body_start..].find(CLOSE) else {
            errors.push(format!("<!--gen:{tag}--> has no <!--/gen-->"));
            break;
        };
        let (kind, arg) = match tag.split_once(':') {
            Some((k, a)) => (k, Some(a)),
            None => (tag, None),
        };
        out.push_str(&rest[..body_start]);
        match render(kind, arg) {
            Some(Ok(body)) => out.push_str(&body),
            Some(Err(e)) => {
                errors.push(e);
                out.push_str(&rest[body_start..body_start + body_len]);
            }
            None => out.push_str(&rest[body_start..body_start + body_len]),
        }
        out.push_str(CLOSE);
        rest = &rest[body_start + body_len + CLOSE.len()..];
    }
    out.push_str(rest);
    (out, errors)
}

#[test]
fn generated_api_docs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let update = std::env::var_os("PG_KAFKA_UPDATE_DOCS").is_some();
    let mut stale = Vec::new();
    let mut errors = Vec::new();
    for path in doc_files(root) {
        let text = std::fs::read_to_string(&path).expect("readable doc");
        let (new, errs) = regenerate(&text);
        let rel = path.strip_prefix(root).unwrap().display().to_string();
        errors.extend(errs.into_iter().map(|e| format!("{rel}: {e}")));
        if new != text {
            if update {
                std::fs::write(&path, new).expect("writable doc");
            } else {
                stale.push(rel);
            }
        }
    }
    assert!(errors.is_empty(), "generated-doc errors: {errors:#?}");
    assert!(
        stale.is_empty(),
        "docs generated from the API registry are stale: {stale:?}; \
         run `PG_KAFKA_UPDATE_DOCS=1 cargo test --lib generated_api_docs` \
         (or scripts/gen-docs.py --write) and commit the result"
    );
}

#[test]
fn every_api_has_a_description() {
    for a in APIS {
        assert!(
            DESCRIPTIONS.iter().any(|(n, _)| *n == a.name),
            "API {} has no README description in src/kafka/api/docs.rs",
            a.name
        );
    }
    for (n, _) in DESCRIPTIONS {
        assert!(
            api_by_name(n).is_some(),
            "description for unregistered API {n}"
        );
    }
}

#[test]
fn regenerate_rewrites_only_owned_regions() {
    let text = "a <!--gen:api-versions:Fetch-->0-99<!--/gen--> b \
                <!--gen:guc-table-->keep<!--/gen--> c";
    let (out, errs) = regenerate(text);
    assert!(errs.is_empty(), "{errs:?}");
    let fetch = api_by_name("Fetch").unwrap();
    assert!(out.contains(&format!(
        "<!--gen:api-versions:Fetch-->{}-{}<!--/gen-->",
        fetch.min_version, fetch.max_version
    )));
    assert!(out.contains("<!--gen:guc-table-->keep<!--/gen-->"));
    assert!(out.ends_with(" c"));
    // Idempotent.
    assert_eq!(regenerate(&out).0, out);
}

#[test]
fn regenerate_reports_bad_markers() {
    assert_ne!(
        regenerate("<!--gen:api-versions:NoSuchApi-->x<!--/gen-->").1,
        [] as [String; 0]
    );
    assert_ne!(
        regenerate("<!--gen:api-bogus-->x<!--/gen-->").1,
        [] as [String; 0]
    );
    assert_ne!(regenerate("<!--gen:api-count-->x").1, [] as [String; 0]);
}
