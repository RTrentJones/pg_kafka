//! Configuration management handlers (DescribeConfigs 32, IncrementalAlterConfigs 44)
//! and log management (DeleteRecords 21).
//!
//! DescribeConfigs reports the configs pg_kafka
//! actually honors (with real values and correct sources); IncrementalAlterConfigs
//! accepts only the keys it can enforce (`retention.ms`) and rejects everything
//! else with INVALID_CONFIG rather than silently accepting; DeleteRecords
//! truncates partitions below an offset and reports the new low watermark.
//!
//! None of the three handlers returns `Err`: their responses have no top-level
//! error code, and the request-level error builders used on `Err`
//! (`build_*_error_response`) can only produce an empty, error-free response. Every
//! failure, storage failures included, is reported on the resource or partition it
//! affects, and the other items in the request are still processed.

use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResponse, DescribeConfigsResult,
};
use kafka_protocol::messages::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use super::helpers::{merge_by_partition, resolve_topics};
use crate::kafka::constants::{
    ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_OFFSET_OUT_OF_RANGE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::{KafkaError, Result};
use crate::kafka::handler_context::HandlerContext;
use std::collections::{HashMap, HashSet};

/// Kafka resource type for topics (DescribeConfigs/IncrementalAlterConfigs).
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigSource`: dynamic per-topic override.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
/// Kafka `ConfigSource`: static default.
const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// Kafka `ConfigType` ordinals (subset used here).
const CONFIG_TYPE_LONG: i8 = 5;
const CONFIG_TYPE_LIST: i8 = 7;

/// `AlterConfigOpType` wire values.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

/// One reportable topic config: (name, value, source, type).
#[derive(Clone)]
struct TopicConfigEntry {
    name: &'static str,
    value: String,
    source: i8,
    config_type: i8,
}

/// The topic configs pg_kafka genuinely honors, with their effective values.
///
/// `retention.ms` reflects the per-topic override when set (DYNAMIC source),
/// else the global `pg_kafka.message_retention_hours` GUC (DEFAULT source;
/// -1 = infinite, matching Kafka's convention). `cleanup.policy` is always
/// "delete" — pg_kafka has no compaction.
fn effective_topic_configs(
    override_ms: Option<i64>,
    global_retention_hours: i32,
) -> Vec<TopicConfigEntry> {
    let (retention_value, retention_source) = match override_ms {
        Some(ms) if ms >= 0 => (ms.to_string(), CONFIG_SOURCE_DYNAMIC_TOPIC),
        Some(_) => ("-1".to_string(), CONFIG_SOURCE_DYNAMIC_TOPIC),
        None => {
            let value = if global_retention_hours > 0 {
                (i64::from(global_retention_hours) * 60 * 60 * 1000).to_string()
            } else {
                "-1".to_string()
            };
            (value, CONFIG_SOURCE_DEFAULT)
        }
    };

    vec![
        TopicConfigEntry {
            name: "retention.ms",
            value: retention_value,
            source: retention_source,
            config_type: CONFIG_TYPE_LONG,
        },
        TopicConfigEntry {
            name: "cleanup.policy",
            value: "delete".to_string(),
            source: CONFIG_SOURCE_DEFAULT,
            // A list-valued config (Kafka 3.7 reports LIST).
            config_type: CONFIG_TYPE_LIST,
        },
    ]
}

/// Handle DescribeConfigs (API 32).
///
/// TOPIC resources report the configs above (filtered to `configuration_keys`
/// when the request names specific keys); unknown topics get a per-resource
/// UNKNOWN_TOPIC_OR_PARTITION; non-topic resource types get INVALID_REQUEST
/// (pg_kafka has no broker/broker-logger config store).
pub fn handle_describe_configs(
    ctx: &HandlerContext,
    resources: Vec<(i8, String, Option<Vec<String>>)>,
    global_retention_hours: i32,
) -> Result<DescribeConfigsResponse> {
    let mut results = Vec::with_capacity(resources.len());
    // The topics in one lookup, and each topic's configs read once however often the
    // request names it (Kafka answers every entry).
    let topics = resolve_topics(
        ctx.store,
        resources
            .iter()
            .filter(|(resource_type, _, _)| *resource_type == RESOURCE_TYPE_TOPIC)
            .map(|(_, name, _)| name.as_str()),
    );
    // Every named topic's retention override in one statement, so the cost is one lookup
    // however many topics the request names (each named topic costs O(1) after it).
    let retention = match &topics {
        Ok(found) if !found.is_empty() => {
            let ids: Vec<i32> = found.values().map(|&(id, _)| id).collect();
            ctx.store.get_topics_retention_ms(&ids).map_err(|e| {
                warn_if_server_error("describe configs for", "the requested topics", &e);
                (e.to_kafka_error_code(), e.to_string())
            })
        }
        _ => Ok(HashMap::new()),
    };

    for (resource_type, resource_name, requested_keys) in resources {
        let mut result = DescribeConfigsResult::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_string(resource_name.clone()));

        if resource_type != RESOURCE_TYPE_TOPIC {
            result.error_code = ERROR_INVALID_REQUEST;
            result.error_message = Some(StrBytes::from_static_str(
                "pg_kafka only supports TOPIC resources",
            ));
            results.push(result);
            continue;
        }

        // A storage failure is reported on this resource only (error_code + message);
        // a handler `Err` would reach the client as an empty, error-free response. A topic
        // deleted between the two lookups is unknown.
        let lookup = match &topics {
            Ok(found) => match (found.get(&resource_name), &retention) {
                (Some((topic_id, _)), Ok(retention)) => {
                    Ok(retention.get(topic_id).map(|&override_ms| {
                        effective_topic_configs(override_ms, global_retention_hours)
                    }))
                }
                (Some(_), Err(e)) => Err(e.clone()),
                (None, _) => Ok(None),
            },
            Err(code) => Err((*code, format!("Failed to look up topic '{resource_name}'"))),
        };
        match lookup {
            Ok(Some(configs)) => {
                let wanted = |name: &str| match &requested_keys {
                    Some(keys) if !keys.is_empty() => keys.iter().any(|k| k == name),
                    _ => true,
                };
                result.configs = configs
                    .into_iter()
                    .filter(|c| wanted(c.name))
                    .map(|c| {
                        DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str(c.name))
                            .with_value(Some(StrBytes::from_string(c.value)))
                            .with_read_only(false)
                            .with_config_source(c.source)
                            .with_is_sensitive(false)
                            .with_config_type(c.config_type)
                    })
                    .collect();
                result.error_code = ERROR_NONE;
            }
            Ok(None) => {
                result.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                result.error_message = Some(StrBytes::from_string(format!(
                    "Unknown topic '{resource_name}'"
                )));
            }
            Err((error_code, message)) => {
                result.error_code = error_code;
                result.error_message = Some(StrBytes::from_string(message));
            }
        }
        results.push(result);
    }

    Ok(DescribeConfigsResponse::default().with_results(results))
}

/// Parse and validate a `retention.ms` SET value.
fn parse_retention_ms(value: Option<&str>) -> std::result::Result<i64, &'static str> {
    let raw = value.ok_or("retention.ms requires a value")?;
    let ms: i64 = raw
        .trim()
        .parse()
        .map_err(|_| "retention.ms must be an integer")?;
    if ms < -1 {
        return Err("retention.ms must be >= -1");
    }
    Ok(ms)
}

/// Handle IncrementalAlterConfigs (API 44).
///
/// Supports SET/DELETE of `retention.ms` on TOPIC resources (persisted to
/// `kafka.topics.retention_ms`, enforced by the retention sweep). Every other
/// key or operation is rejected per-resource with INVALID_CONFIG — accepting a
/// config we do not enforce would be a silent lie to admin tooling. All of a
/// resource's alterations are validated before any is applied, so a resource
/// is applied all-or-nothing; `validate_only` runs the same checks without
/// applying.
pub fn handle_incremental_alter_configs(
    ctx: &HandlerContext,
    resources: crate::kafka::messages::AlterConfigsResources,
    validate_only: bool,
) -> Result<IncrementalAlterConfigsResponse> {
    // Kafka 3.7 refuses every entry of a resource named more than once, without acting
    // on any of them.
    let mut named: HashMap<(i8, &str), usize> = HashMap::new();
    for (resource_type, resource_name, _) in &resources {
        *named.entry((*resource_type, resource_name)).or_default() += 1;
    }
    let repeated: HashSet<(i8, String)> = named
        .into_iter()
        .filter(|&(_, n)| n > 1)
        .map(|((resource_type, name), _)| (resource_type, name.to_string()))
        .collect();
    let topics = resolve_topics(
        ctx.store,
        resources
            .iter()
            .filter(|(resource_type, _, _)| *resource_type == RESOURCE_TYPE_TOPIC)
            .map(|(_, name, _)| name.as_str()),
    );
    let responses = resources
        .into_iter()
        .map(|(resource_type, resource_name, configs)| {
            let mut resp = AlterConfigsResourceResponse::default()
                .with_resource_type(resource_type)
                .with_resource_name(StrBytes::from_string(resource_name.clone()));
            let resource = Resource {
                kind: resource_type,
                name: &resource_name,
                topic: topics
                    .as_ref()
                    .map(|found| found.get(&resource_name).map(|&(id, _)| id))
                    .map_err(|&code| code),
            };
            let outcome = if repeated.contains(&(resource_type, resource_name.clone())) {
                Err((
                    ERROR_INVALID_REQUEST,
                    "Each resource must appear at most once.".to_string(),
                ))
            } else {
                alter_topic_configs(ctx, resource, &configs, validate_only)
            };
            match outcome {
                Ok(()) => resp.error_code = ERROR_NONE,
                Err((error_code, message)) => {
                    resp.error_code = error_code;
                    resp.error_message = Some(StrBytes::from_string(message));
                }
            }
            resp
        })
        .collect();

    Ok(IncrementalAlterConfigsResponse::default().with_responses(responses))
}

/// Validate one resource's alterations and, unless `validate_only`, apply them: the
/// resource's error code and message if it is refused.
fn alter_topic_configs(
    ctx: &HandlerContext,
    resource: Resource<'_>,
    configs: &[(String, i8, Option<String>)],
    validate_only: bool,
) -> std::result::Result<(), (i16, String)> {
    let Resource {
        kind: resource_type,
        name: resource_name,
        topic,
    } = resource;
    malformed_alterations(configs).map_or(Ok(()), |msg| Err((ERROR_INVALID_REQUEST, msg)))?;
    if resource_type != RESOURCE_TYPE_TOPIC {
        return Err((
            ERROR_INVALID_REQUEST,
            "pg_kafka only supports TOPIC resources".to_string(),
        ));
    }
    let storage_error = |e: KafkaError| {
        warn_if_server_error("alter configs for", resource_name, &e);
        (e.to_kafka_error_code(), e.to_string())
    };
    let topic_id = topic
        .map_err(|code| (code, format!("Failed to look up topic '{resource_name}'")))?
        .ok_or_else(|| {
            (
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                format!("Unknown topic '{resource_name}'"),
            )
        })?;
    let planned = plan_retention_change(configs).map_err(|msg| (ERROR_INVALID_CONFIG, msg))?;
    if let (false, Some(change)) = (validate_only, planned) {
        let retention_ms = match change {
            RetentionChange::Set(ms) => Some(ms),
            RetentionChange::Delete => None,
        };
        ctx.store
            .set_topic_retention_ms(topic_id, retention_ms)
            .map_err(storage_error)?;
    }
    Ok(())
}

/// Why a resource's alterations are a malformed request, if they are. Kafka 3.7
/// (`ConfigAdminManager.preprocess`) refuses, before it looks at the resource type or the
/// topic and before it validates any config, a resource that names one key twice and then
/// a SET, APPEND or SUBTRACT without a value, whatever the key, each INVALID_REQUEST.
fn malformed_alterations(configs: &[(String, i8, Option<String>)]) -> Option<String> {
    let mut names = HashSet::with_capacity(configs.len());
    if !configs
        .iter()
        .all(|(name, _, _)| names.insert(name.as_str()))
    {
        return Some("Error due to duplicate config keys".to_string());
    }
    let null_updates: Vec<&str> = configs
        .iter()
        .filter(|(_, op, value)| *op != OP_DELETE && value.is_none())
        .map(|(name, _, _)| name.as_str())
        .collect();
    (!null_updates.is_empty())
        .then(|| format!("Null value not supported for : {}", null_updates.join(", ")))
}

/// An IncrementalAlterConfigs resource: its type, its name and, for a topic, the topic's
/// id (`None` for a topic that does not exist) or the error code of a failed lookup.
#[derive(Clone, Copy)]
struct Resource<'a> {
    kind: i8,
    name: &'a str,
    topic: std::result::Result<Option<i32>, i16>,
}

/// A topic's `retention.ms` alteration.
#[derive(Clone, Copy)]
enum RetentionChange {
    Set(i64),
    Delete,
}

/// Validate a resource's alterations before any is applied (all-or-nothing per resource):
/// the `retention.ms` change (`None` for no change), or why they are refused. A resource
/// names each key at most once ([`malformed_alterations`]), so it has at most one change.
fn plan_retention_change(
    configs: &[(String, i8, Option<String>)],
) -> std::result::Result<Option<RetentionChange>, String> {
    let mut planned = None;
    for (name, op, value) in configs {
        if name != "retention.ms" {
            return Err(format!(
                "Unsupported config '{name}' (pg_kafka honors: retention.ms)"
            ));
        }
        planned = match *op {
            OP_SET => Some(RetentionChange::Set(
                parse_retention_ms(value.as_deref()).map_err(str::to_string)?,
            )),
            OP_DELETE => Some(RetentionChange::Delete),
            _ => return Err("Unsupported operation (only SET and DELETE)".to_string()),
        };
    }
    Ok(planned)
}

/// Handle DeleteRecords (API 21).
///
/// Per partition: `offset == -1` truncates to the high watermark; an offset
/// past the high watermark is OFFSET_OUT_OF_RANGE; otherwise rows below the
/// offset are deleted and the new low watermark reported. Unknown topics get
/// per-partition UNKNOWN_TOPIC_OR_PARTITION.
pub fn handle_delete_records(
    ctx: &HandlerContext,
    topics: Vec<(String, Vec<(i32, i64)>)>,
) -> Result<DeleteRecordsResponse> {
    // Kafka 3.7 collects the partitions into a map: a repeated partition is truncated
    // once, to its last entry's offset, and answered once.
    let topics = merge_by_partition(topics, |&(partition, _)| partition);
    // The topics in one lookup. Unknown topic and a storage failure are both reported on
    // every requested partition of the topic (`Err` holds the error code).
    let found = resolve_topics(ctx.store, topics.iter().map(|(name, _)| name.as_str()));
    let mut topic_results = Vec::with_capacity(topics.len());

    for (topic_name, partitions) in topics {
        let resolved: std::result::Result<(i32, i32), i16> = match &found {
            Ok(found) => found
                .get(&topic_name)
                .copied()
                .ok_or(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            Err(code) => Err(*code),
        };
        let mut partition_results = Vec::with_capacity(partitions.len());

        for (partition_id, requested_offset) in partitions {
            let mut pr = DeleteRecordsPartitionResult::default()
                .with_partition_index(partition_id)
                .with_low_watermark(-1);
            pr.error_code = match resolved {
                Ok((topic_id, partition_count)) => {
                    match delete_partition_records(
                        ctx,
                        topic_id,
                        partition_count,
                        partition_id,
                        requested_offset,
                    ) {
                        Ok(low_watermark) => {
                            pr.low_watermark = low_watermark;
                            ERROR_NONE
                        }
                        Err(code) => code,
                    }
                }
                Err(code) => code,
            };
            partition_results.push(pr);
        }

        topic_results.push(
            DeleteRecordsTopicResult::default()
                .with_name(TopicName(StrBytes::from_string(topic_name)))
                .with_partitions(partition_results),
        );
    }

    Ok(DeleteRecordsResponse::default().with_topics(topic_results))
}

/// DeleteRecords for one partition of a resolved topic: the new low watermark, or
/// the partition's error code. Storage failures are per-partition, like the rest.
fn delete_partition_records(
    ctx: &HandlerContext,
    topic_id: i32,
    partition_count: i32,
    partition_id: i32,
    requested_offset: i64,
) -> std::result::Result<i64, i16> {
    if partition_id < 0 || partition_id >= partition_count {
        return Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION);
    }
    let on_err = |e: KafkaError| {
        warn_if_server_error(
            "delete records from",
            &format!("topic_id={topic_id} partition={partition_id}"),
            &e,
        );
        e.to_kafka_error_code()
    };
    let high_watermark = ctx
        .store
        .get_high_watermark(topic_id, partition_id)
        .map_err(on_err)?;
    // -1 = "truncate everything" (to the HWM), per Kafka semantics.
    let before_offset = if requested_offset == -1 {
        high_watermark
    } else {
        requested_offset
    };
    if before_offset < 0 || before_offset > high_watermark {
        return Err(ERROR_OFFSET_OUT_OF_RANGE);
    }
    ctx.store
        .delete_records_before(topic_id, partition_id, before_offset)
        .map_err(on_err)
}

/// Log a storage failure that is being reported to the client per item.
fn warn_if_server_error(action: &str, target: &str, e: &KafkaError) {
    if e.is_server_error() {
        crate::pg_warning!("Failed to {} '{}': {}", action, target, e);
    }
}
