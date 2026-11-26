//! Config and log management: DeleteRecords (21), DescribeConfigs (32),
//! IncrementalAlterConfigs (44).

use kafka_protocol::messages::delete_records_request::DeleteRecordsRequest;
use kafka_protocol::messages::delete_records_response::{
    DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsRequest;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResponse, DescribeConfigsResult,
};
use kafka_protocol::messages::incremental_alter_configs_request::IncrementalAlterConfigsRequest;
use kafka_protocol::messages::incremental_alter_configs_response::{
    AlterConfigsResourceResponse, IncrementalAlterConfigsResponse,
};
use kafka_protocol::messages::TopicName;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader, TopicPartitions};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::AlterConfigsResources;
use crate::kafka::notifications::{send_best_effort, InternalNotification};

/// Requested `(resource_type, resource_name)` pairs, the echo of the config APIs.
pub type ConfigResources = Vec<(i8, String)>;

/// DeleteRecords: truncate partitions below an offset.
pub struct DeleteRecordsApi;

/// DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsRequestData {
    /// `(topic, [(partition, before_offset)])`; -1 truncates to the high watermark.
    pub topics: Vec<(String, Vec<(i32, i64)>)>,
    /// Request timeout (milliseconds).
    pub timeout_ms: i32,
}

impl ApiSchema for DeleteRecordsApi {
    type Wire = DeleteRecordsRequest;
    type Echo = TopicPartitions;
    const NAME: &'static str = "DeleteRecords";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 2;

    /// Errors are per partition: report the code on every requested partition.
    fn error_response(
        echo: Option<&TopicPartitions>,
        _version: i16,
        error_code: i16,
    ) -> DeleteRecordsResponse {
        let mut response = DeleteRecordsResponse::default();
        response.topics = echo
            .into_iter()
            .flatten()
            .map(|(name, partitions)| {
                let mut t = DeleteRecordsTopicResult::default();
                t.name = TopicName(name.clone().into());
                t.partitions = partitions
                    .iter()
                    .map(|&index| {
                        let mut p = DeleteRecordsPartitionResult::default();
                        p.partition_index = index;
                        p.low_watermark = -1;
                        p.error_code = error_code;
                        p
                    })
                    .collect();
                t
            })
            .collect();
        response
    }

    fn response_has_error(response: &DeleteRecordsResponse) -> bool {
        response
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .any(|p| p.error_code != 0)
    }
}

impl KafkaApi for DeleteRecordsApi {
    type Request = DeleteRecordsRequestData;

    fn decode(
        wire: DeleteRecordsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(DeleteRecordsRequestData {
            topics: wire
                .topics
                .into_iter()
                .map(|t| {
                    (
                        t.name.to_string(),
                        t.partitions
                            .into_iter()
                            .map(|p| (p.partition_index, p.offset))
                            .collect(),
                    )
                })
                .collect(),
            timeout_ms: wire.timeout_ms,
        })
    }

    fn echo(request: &Self::Request) -> TopicPartitions {
        request
            .topics
            .iter()
            .map(|(name, partitions)| (name.clone(), partitions.iter().map(|(p, _)| *p).collect()))
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_delete_records(&env.ctx, r.topics)
                .inspect(|response| emit_log_start_notifications(response, env.notify_tx))
        })
    }
}

/// Wake the long polls of every partition DeleteRecords moved the log start of: one fetching
/// from below the new start is answered OFFSET_OUT_OF_RANGE now rather than at its deadline.
/// One notification per partition answered without error, O(the response).
fn emit_log_start_notifications(
    response: &DeleteRecordsResponse,
    notify_tx: &crossbeam_channel::Sender<InternalNotification>,
) {
    for topic in &response.topics {
        for partition in topic.partitions.iter().filter(|p| p.error_code == 0) {
            send_best_effort(
                notify_tx,
                InternalNotification::LogStartMoved {
                    topic_name: topic.name.0.to_string(),
                    partition_id: partition.partition_index,
                },
            );
        }
    }
}

/// DescribeConfigs: read topic configs.
pub struct DescribeConfigsApi;

/// DescribeConfigs request.
#[derive(Debug, Clone)]
pub struct DescribeConfigsRequestData {
    /// `(resource_type, resource_name, requested keys)`; `None` or empty = all keys.
    pub resources: Vec<(i8, String, Option<Vec<String>>)>,
}

impl ApiSchema for DescribeConfigsApi {
    type Wire = DescribeConfigsRequest;
    type Echo = ConfigResources;
    const NAME: &'static str = "DescribeConfigs";

    // The crate implements v1-4 only (v0 lacks per-config source/synonym metadata); every
    // mainstream admin client negotiates >= 1.
    const MIN_VERSION: i16 = 1;
    const MAX_VERSION: i16 = 4;

    /// Errors are per resource: report the code on every requested resource.
    fn error_response(
        echo: Option<&ConfigResources>,
        _version: i16,
        error_code: i16,
    ) -> DescribeConfigsResponse {
        let mut response = DescribeConfigsResponse::default();
        response.results = echo
            .into_iter()
            .flatten()
            .map(|(resource_type, name)| {
                let mut r = DescribeConfigsResult::default();
                r.resource_type = *resource_type;
                r.resource_name = name.clone().into();
                r.error_code = error_code;
                r
            })
            .collect();
        response
    }

    fn response_has_error(response: &DescribeConfigsResponse) -> bool {
        response.results.iter().any(|r| r.error_code != 0)
    }
}

impl KafkaApi for DescribeConfigsApi {
    type Request = DescribeConfigsRequestData;

    fn decode(
        wire: DescribeConfigsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(DescribeConfigsRequestData {
            resources: wire
                .resources
                .into_iter()
                .map(|r| {
                    let keys = r
                        .configuration_keys
                        .map(|keys| keys.into_iter().map(|k| k.to_string()).collect());
                    (r.resource_type, r.resource_name.to_string(), keys)
                })
                .collect(),
        })
    }

    fn echo(request: &Self::Request) -> ConfigResources {
        request
            .resources
            .iter()
            .map(|(t, name, _)| (*t, name.clone()))
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        let global_retention_hours = crate::config::MESSAGE_RETENTION_HOURS.get();
        call.run(request, |r, _| {
            handlers::handle_describe_configs(&env.ctx, r.resources, global_retention_hours)
        })
    }
}

/// IncrementalAlterConfigs: SET/DELETE individual topic configs.
pub struct IncrementalAlterConfigsApi;

/// IncrementalAlterConfigs request.
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequestData {
    /// `(resource_type, resource_name, [(config_name, operation, value)])`; operation is the
    /// wire enum (0 = SET, 1 = DELETE, 2 = APPEND, 3 = SUBTRACT).
    pub resources: AlterConfigsResources,
    /// Validate without applying.
    pub validate_only: bool,
}

impl ApiSchema for IncrementalAlterConfigsApi {
    type Wire = IncrementalAlterConfigsRequest;
    type Echo = ConfigResources;
    const NAME: &'static str = "IncrementalAlterConfigs";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 1;

    /// Errors are per resource: report the code on every requested resource.
    fn error_response(
        echo: Option<&ConfigResources>,
        _version: i16,
        error_code: i16,
    ) -> IncrementalAlterConfigsResponse {
        let mut response = IncrementalAlterConfigsResponse::default();
        response.responses = echo
            .into_iter()
            .flatten()
            .map(|(resource_type, name)| {
                let mut r = AlterConfigsResourceResponse::default();
                r.resource_type = *resource_type;
                r.resource_name = name.clone().into();
                r.error_code = error_code;
                r
            })
            .collect();
        response
    }

    fn response_has_error(response: &IncrementalAlterConfigsResponse) -> bool {
        response.responses.iter().any(|r| r.error_code != 0)
    }
}

impl KafkaApi for IncrementalAlterConfigsApi {
    type Request = IncrementalAlterConfigsRequestData;

    fn decode(
        wire: IncrementalAlterConfigsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(IncrementalAlterConfigsRequestData {
            resources: wire
                .resources
                .into_iter()
                .map(|r| {
                    let configs = r
                        .configs
                        .into_iter()
                        .map(|c| {
                            (
                                c.name.to_string(),
                                c.config_operation,
                                c.value.map(|v| v.to_string()),
                            )
                        })
                        .collect();
                    (r.resource_type, r.resource_name.to_string(), configs)
                })
                .collect(),
            validate_only: wire.validate_only,
        })
    }

    fn echo(request: &Self::Request) -> ConfigResources {
        request
            .resources
            .iter()
            .map(|(t, name, _)| (*t, name.clone()))
            .collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_incremental_alter_configs(&env.ctx, r.resources, r.validate_only)
        })
    }
}
