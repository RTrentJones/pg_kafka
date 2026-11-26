//! Topic admin APIs: CreateTopics (19), DeleteTopics (20), CreatePartitions (37).

use kafka_protocol::messages::create_partitions_request::CreatePartitionsRequest;
use kafka_protocol::messages::create_partitions_response::{
    CreatePartitionsResponse, CreatePartitionsTopicResult,
};
use kafka_protocol::messages::create_topics_request::CreateTopicsRequest;
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicResult, CreateTopicsResponse,
};
use kafka_protocol::messages::delete_topics_request::DeleteTopicsRequest;
use kafka_protocol::messages::delete_topics_response::{
    DeletableTopicResult, DeleteTopicsResponse,
};
use kafka_protocol::messages::TopicName;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader};
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;
use crate::kafka::messages::{CreatePartitionsTopicRequest, CreateTopicRequest};
use crate::kafka::notifications::{send_best_effort, InternalNotification};

fn topic_name(name: &str) -> TopicName {
    TopicName(name.to_string().into())
}

/// CreateTopics: create topics.
pub struct CreateTopicsApi;

/// CreateTopics request.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequestData {
    /// Topics to create.
    pub topics: Vec<CreateTopicRequest>,
    /// Request timeout (milliseconds).
    pub timeout_ms: i32,
    /// Validate without creating.
    pub validate_only: bool,
}

impl ApiSchema for CreateTopicsApi {
    type Wire = CreateTopicsRequest;

    /// The requested topic names.
    type Echo = Vec<String>;
    const NAME: &'static str = "CreateTopics";

    // The crate cannot decode CreateTopics v0-1.
    const MIN_VERSION: i16 = 2;
    const MAX_VERSION: i16 = 5;

    /// Errors are per topic: report the code on every requested topic.
    fn error_response(
        echo: Option<&Vec<String>>,
        _version: i16,
        error_code: i16,
    ) -> CreateTopicsResponse {
        let mut response = CreateTopicsResponse::default();
        response.throttle_time_ms = 0;
        response.topics = echo
            .into_iter()
            .flatten()
            .map(|name| {
                let mut t = CreatableTopicResult::default();
                t.name = topic_name(name);
                t.error_code = error_code;
                t.num_partitions = -1;
                t.replication_factor = -1;
                t
            })
            .collect();
        response
    }

    fn response_has_error(response: &CreateTopicsResponse) -> bool {
        response.topics.iter().any(|t| t.error_code != 0)
    }
}

impl KafkaApi for CreateTopicsApi {
    type Request = CreateTopicsRequestData;

    fn decode(
        wire: CreateTopicsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(CreateTopicsRequestData {
            topics: wire
                .topics
                .into_iter()
                .map(|t| CreateTopicRequest {
                    name: t.name.to_string(),
                    num_partitions: t.num_partitions,
                    replication_factor: t.replication_factor,
                })
                .collect(),
            timeout_ms: wire.timeout_ms,
            validate_only: wire.validate_only,
        })
    }

    fn echo(request: &Self::Request) -> Vec<String> {
        request.topics.iter().map(|t| t.name.clone()).collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_create_topics(&env.ctx, r.topics, r.validate_only)
        })
    }
}

/// DeleteTopics: delete topics and their messages.
pub struct DeleteTopicsApi;

/// DeleteTopics request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequestData {
    /// Topic names to delete.
    pub topic_names: Vec<String>,
    /// Request timeout (milliseconds).
    pub timeout_ms: i32,
}

impl ApiSchema for DeleteTopicsApi {
    type Wire = DeleteTopicsRequest;

    /// The requested topic names.
    type Echo = Vec<String>;
    const NAME: &'static str = "DeleteTopics";

    // The crate cannot decode DeleteTopics v0.
    const MIN_VERSION: i16 = 1;
    const MAX_VERSION: i16 = 4;

    /// Errors are per topic: report the code on every requested topic.
    fn error_response(
        echo: Option<&Vec<String>>,
        _version: i16,
        error_code: i16,
    ) -> DeleteTopicsResponse {
        let mut response = DeleteTopicsResponse::default();
        response.throttle_time_ms = 0;
        response.responses = echo
            .into_iter()
            .flatten()
            .map(|name| {
                let mut t = DeletableTopicResult::default();
                t.name = Some(topic_name(name));
                t.error_code = error_code;
                t
            })
            .collect();
        response
    }

    fn response_has_error(response: &DeleteTopicsResponse) -> bool {
        response.responses.iter().any(|t| t.error_code != 0)
    }
}

impl KafkaApi for DeleteTopicsApi {
    type Request = DeleteTopicsRequestData;

    fn decode(
        wire: DeleteTopicsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(DeleteTopicsRequestData {
            topic_names: wire
                .topic_names
                .into_iter()
                .map(|t| t.to_string())
                .collect(),
            timeout_ms: wire.timeout_ms,
        })
    }

    fn echo(request: &Self::Request) -> Vec<String> {
        request.topic_names.clone()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_delete_topics(&env.ctx, r.topic_names).inspect(|response| {
                // Wake the long polls of each deleted topic: they are answered
                // UNKNOWN_TOPIC_OR_PARTITION now rather than at their deadline.
                for topic in response.responses.iter().filter(|t| t.error_code == 0) {
                    if let Some(name) = &topic.name {
                        send_best_effort(
                            env.notify_tx,
                            InternalNotification::TopicDeleted {
                                topic_name: name.0.to_string(),
                            },
                        );
                    }
                }
            })
        })
    }
}

/// CreatePartitions: grow topics' partition counts.
pub struct CreatePartitionsApi;

/// CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsRequestData {
    /// Topics with their new total partition counts.
    pub topics: Vec<CreatePartitionsTopicRequest>,
    /// Request timeout (milliseconds).
    pub timeout_ms: i32,
    /// Validate without changing anything.
    pub validate_only: bool,
}

impl ApiSchema for CreatePartitionsApi {
    type Wire = CreatePartitionsRequest;

    /// The requested topic names.
    type Echo = Vec<String>;
    const NAME: &'static str = "CreatePartitions";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 2;

    /// Errors are per topic: report the code on every requested topic.
    fn error_response(
        echo: Option<&Vec<String>>,
        _version: i16,
        error_code: i16,
    ) -> CreatePartitionsResponse {
        let mut response = CreatePartitionsResponse::default();
        response.throttle_time_ms = 0;
        response.results = echo
            .into_iter()
            .flatten()
            .map(|name| {
                let mut t = CreatePartitionsTopicResult::default();
                t.name = topic_name(name);
                t.error_code = error_code;
                t
            })
            .collect();
        response
    }

    fn response_has_error(response: &CreatePartitionsResponse) -> bool {
        response.results.iter().any(|r| r.error_code != 0)
    }
}

impl KafkaApi for CreatePartitionsApi {
    type Request = CreatePartitionsRequestData;

    fn decode(
        wire: CreatePartitionsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        Ok(CreatePartitionsRequestData {
            topics: wire
                .topics
                .into_iter()
                .map(|t| CreatePartitionsTopicRequest {
                    name: t.name.to_string(),
                    count: t.count,
                })
                .collect(),
            timeout_ms: wire.timeout_ms,
            validate_only: wire.validate_only,
        })
    }

    fn echo(request: &Self::Request) -> Vec<String> {
        request.topics.iter().map(|t| t.name.clone()).collect()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_create_partitions(&env.ctx, r.topics, r.validate_only)
        })
    }
}
