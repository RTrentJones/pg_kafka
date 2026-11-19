//! ApiVersions (18) and Metadata (3).

use kafka_protocol::messages::api_versions_request::ApiVersionsRequest;
use kafka_protocol::messages::api_versions_response::ApiVersionsResponse;
use kafka_protocol::messages::metadata_request::MetadataRequest;
use kafka_protocol::messages::metadata_response::MetadataResponse;
use kafka_protocol::messages::BrokerId;

use super::{ApiEnv, ApiSchema, Call, KafkaApi, Rejection, RequestHeader};
use crate::kafka::constants::ERROR_UNSUPPORTED_VERSION;
use crate::kafka::dispatch::RequestOutcome;
use crate::kafka::handlers;

/// ApiVersions: which APIs and versions the broker serves.
pub struct ApiVersionsApi;

/// ApiVersions request (the body carries only client software name/version, which pg_kafka
/// does not use).
#[derive(Debug, Clone, Default)]
pub struct ApiVersionsRequestData;

impl ApiSchema for ApiVersionsApi {
    type Wire = ApiVersionsRequest;
    type Echo = ();
    const NAME: &'static str = "ApiVersions";
    const MIN_VERSION: i16 = 0;
    const MAX_VERSION: i16 = 3;

    fn error_response(_echo: Option<&()>, _version: i16, error_code: i16) -> ApiVersionsResponse {
        let mut response = crate::kafka::response_builders::build_api_versions_response();
        response.error_code = error_code;
        response
    }

    fn response_has_error(response: &ApiVersionsResponse) -> bool {
        response.error_code != 0
    }
}

impl KafkaApi for ApiVersionsApi {
    type Request = ApiVersionsRequestData;

    fn decode(
        _wire: ApiVersionsRequest,
        _header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<()>> {
        Ok(ApiVersionsRequestData)
    }

    fn echo(_request: &Self::Request) {}

    /// KIP-511: a version we do not support must still be answered, in the v0 format, with
    /// UNSUPPORTED_VERSION and the full key list, so the client can retry with a version
    /// from that list.
    fn unsupported_version_reply(
        _echo: Option<&()>,
        _version: i16,
    ) -> Option<(i16, ApiVersionsResponse)> {
        Some((0, Self::error_response(None, 0, ERROR_UNSUPPORTED_VERSION)))
    }

    fn handle(request: Self::Request, call: Call<Self>, _env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |_, _| Ok(handlers::handle_api_versions()))
    }
}

/// Metadata: brokers and topic/partition layout.
pub struct MetadataApi;

/// Metadata request.
#[derive(Debug, Clone)]
pub struct MetadataRequestData {
    /// Topics to describe; `None` = all topics, `Some(vec![])` = none (v1+ empty array).
    pub topics: Option<Vec<String>>,
    /// v4+ `allow_auto_topic_creation` (true for older versions). When false, a
    /// missing requested topic is reported as UNKNOWN_TOPIC_OR_PARTITION, not created.
    pub allow_auto_topic_creation: bool,
}

impl ApiSchema for MetadataApi {
    type Wire = MetadataRequest;

    /// The requested topic names (`None` = all topics).
    type Echo = Option<Vec<String>>;
    const NAME: &'static str = "Metadata";
    const MIN_VERSION: i16 = 0;

    // v10 (KIP-516) adds a per-topic `topic_id` to the response, left as the zero UUID
    // (by-name clients ignore it). It is served because clients with a pinned broker
    // version that do not down-negotiate (Sarama `V2_8_0_0`) send v10 unconditionally.
    const MAX_VERSION: i16 = 10;

    /// MetadataResponse has no top-level error code: report it on every requested topic.
    fn error_response(
        echo: Option<&Self::Echo>,
        _version: i16,
        error_code: i16,
    ) -> MetadataResponse {
        let mut response = crate::kafka::response_builders::build_metadata_error_response();
        response.controller_id = BrokerId(-1);
        if let Some(Some(topics)) = echo {
            response.topics = topics
                .iter()
                .map(|name| {
                    crate::kafka::response_builders::build_topic_metadata(
                        name.clone(),
                        error_code,
                        Vec::new(),
                    )
                })
                .collect();
        }
        response
    }

    fn response_has_error(response: &MetadataResponse) -> bool {
        response.error_code != 0
            || response
                .topics
                .iter()
                .any(|t| t.error_code != 0 || t.partitions.iter().any(|p| p.error_code != 0))
    }
}

impl KafkaApi for MetadataApi {
    type Request = MetadataRequestData;

    fn decode(
        wire: MetadataRequest,
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>> {
        // Empty-vs-null is version dependent. v0 has no nullable array and an empty
        // array means "all topics"; v1+ null means all topics and an empty array none.
        let topics = match wire.topics {
            Some(t) if !t.is_empty() => Some(
                t.into_iter()
                    .filter_map(|t| t.name.map(|n| n.to_string()))
                    .collect(),
            ),
            Some(_) if header.api_version == 0 => None,
            Some(_) => Some(Vec::new()),
            None => None,
        };
        Ok(MetadataRequestData {
            topics,
            allow_auto_topic_creation: wire.allow_auto_topic_creation,
        })
    }

    fn echo(request: &Self::Request) -> Self::Echo {
        request.topics.clone()
    }

    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome {
        call.run(request, |r, _| {
            handlers::handle_metadata(&env.ctx, r.topics, r.allow_auto_topic_creation)
        })
    }
}
