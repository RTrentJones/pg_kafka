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

use crate::kafka::constants::{
    ERROR_INVALID_CONFIG, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_OFFSET_OUT_OF_RANGE,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use crate::kafka::error::Result;
use crate::kafka::handler_context::HandlerContext;

/// Kafka resource type for topics (DescribeConfigs/IncrementalAlterConfigs).
const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigSource`: dynamic per-topic override.
const CONFIG_SOURCE_DYNAMIC_TOPIC: i8 = 1;
/// Kafka `ConfigSource`: static default.
const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// Kafka `ConfigType` ordinals (subset used here).
const CONFIG_TYPE_STRING: i8 = 2;
const CONFIG_TYPE_LONG: i8 = 5;

/// `AlterConfigOpType` wire values.
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

/// One reportable topic config: (name, value, source, type).
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
    ctx: &HandlerContext,
    topic_id: i32,
    global_retention_hours: i32,
) -> Result<Vec<TopicConfigEntry>> {
    let override_ms = ctx.store.get_topic_retention_ms(topic_id)?;
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

    Ok(vec![
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
            config_type: CONFIG_TYPE_STRING,
        },
    ])
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
        // a handler `Err` would reach the client as an empty, error-free response.
        let lookup = ctx
            .store
            .get_topic_id(&resource_name)
            .and_then(|id| match id {
                Some(topic_id) => {
                    effective_topic_configs(ctx, topic_id, global_retention_hours).map(Some)
                }
                None => Ok(None),
            });
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
            Err(e) => {
                warn_if_server_error("describe configs for", &resource_name, &e);
                result.error_code = e.to_kafka_error_code();
                result.error_message = Some(StrBytes::from_string(e.to_string()));
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
    let mut responses = Vec::with_capacity(resources.len());

    for (resource_type, resource_name, configs) in resources {
        let mut resp = AlterConfigsResourceResponse::default()
            .with_resource_type(resource_type)
            .with_resource_name(StrBytes::from_string(resource_name.clone()));

        if resource_type != RESOURCE_TYPE_TOPIC {
            resp.error_code = ERROR_INVALID_REQUEST;
            resp.error_message = Some(StrBytes::from_static_str(
                "pg_kafka only supports TOPIC resources",
            ));
            responses.push(resp);
            continue;
        }

        let topic_id = match ctx.store.get_topic_id(&resource_name) {
            Ok(Some(id)) => id,
            Ok(None) => {
                resp.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                resp.error_message = Some(StrBytes::from_string(format!(
                    "Unknown topic '{resource_name}'"
                )));
                responses.push(resp);
                continue;
            }
            Err(e) => {
                warn_if_server_error("alter configs for", &resource_name, &e);
                resp.error_code = e.to_kafka_error_code();
                resp.error_message = Some(StrBytes::from_string(e.to_string()));
                responses.push(resp);
                continue;
            }
        };

        // Validate everything first (all-or-nothing per resource). Every supported
        // alteration targets the same column, so applying them in order is the same
        // as applying the last one: `planned` keeps only the final value, and the
        // resource is applied with ONE write that either fully succeeds or fails.
        let mut planned: Option<Option<i64>> = None;
        let mut validation_error: Option<String> = None;
        for (name, op, value) in &configs {
            if name != "retention.ms" {
                validation_error = Some(format!(
                    "Unsupported config '{name}' (pg_kafka honors: retention.ms)"
                ));
                break;
            }
            match *op {
                OP_SET => match parse_retention_ms(value.as_deref()) {
                    Ok(ms) => planned = Some(Some(ms)),
                    Err(msg) => {
                        validation_error = Some(msg.to_string());
                        break;
                    }
                },
                OP_DELETE => planned = Some(None),
                _ => {
                    validation_error =
                        Some("Unsupported operation (only SET and DELETE)".to_string());
                    break;
                }
            }
        }

        if let Some(msg) = validation_error {
            resp.error_code = ERROR_INVALID_CONFIG;
            resp.error_message = Some(StrBytes::from_string(msg));
            responses.push(resp);
            continue;
        }

        if let (false, Some(retention_ms)) = (validate_only, planned) {
            if let Err(e) = ctx.store.set_topic_retention_ms(topic_id, retention_ms) {
                warn_if_server_error("alter configs for", &resource_name, &e);
                resp.error_code = e.to_kafka_error_code();
                resp.error_message = Some(StrBytes::from_string(e.to_string()));
                responses.push(resp);
                continue;
            }
        }
        resp.error_code = ERROR_NONE;
        responses.push(resp);
    }

    Ok(IncrementalAlterConfigsResponse::default().with_responses(responses))
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
    let mut topic_results = Vec::with_capacity(topics.len());

    for (topic_name, partitions) in topics {
        // Resolve the topic once. Unknown topic and a storage failure are both reported
        // on every requested partition of this topic (`Err` holds the error code).
        let resolved: std::result::Result<(i32, i32), i16> = match (
            ctx.store.get_topic_id(&topic_name),
            ctx.store.get_topic_partition_count(&topic_name),
        ) {
            (Ok(Some(id)), Ok(Some(count))) => Ok((id, count)),
            (Ok(_), Ok(_)) => Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            (Err(e), _) | (_, Err(e)) => {
                warn_if_server_error("delete records from", &topic_name, &e);
                Err(e.to_kafka_error_code())
            }
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
    let on_err = |e: crate::kafka::error::KafkaError| {
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
fn warn_if_server_error(action: &str, target: &str, e: &crate::kafka::error::KafkaError) {
    if e.is_server_error() {
        crate::pg_warning!("Failed to {} '{}': {}", action, target, e);
    }
}
