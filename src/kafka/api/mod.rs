//! One description per Kafka API.
//!
//! Every Kafka API pg_kafka serves is a type implementing [`KafkaApi`]. That one impl owns
//! everything the request pipeline needs to know about the API:
//!
//! - its wire types (`Wire`, a kafka-protocol request whose `KEY` is the api key and whose
//!   `Response` is the response body),
//! - its display name (logs, `kafka.api_metrics`) and supported version range (advertised in
//!   ApiVersions and enforced on decode),
//! - which request lane it travels on ([`Lane`]) and whether its reply may be held past the
//!   request's transaction ([`KafkaApi::DEFERRED_REPLY`]),
//! - how the decoded wire request becomes the handler's input ([`KafkaApi::decode`]),
//! - how to answer it with an error code ([`ApiSchema::error_response`]), echoing the requested
//!   topics/partitions/groups ([`ApiSchema::Echo`]) where the response has no top-level code,
//! - whether a response carries an error (metrics),
//! - and how the DB thread runs it ([`KafkaApi::handle`]).
//!
//! The registry at the bottom of this file (`kafka_apis!`) lists each API once and
//! generates the request/response enums and the per-API dispatch from it. Adding an API is
//! one `impl KafkaApi` plus one registry line; see "Adding a New Kafka API Handler" in
//! CLAUDE.md.
//!
//! ## Thread safety
//!
//! Decoding (`decode_body`) and encoding run on the network thread and must not touch
//! pgrx. [`dispatch`] runs on the DB thread.

use bytes::BytesMut;
use crossbeam_channel::Sender;
use kafka_protocol::messages::ResponseHeader;
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, Message, Request as WireRequest,
};
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

use crate::kafka::constants::{ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION};
use crate::kafka::dispatch::{dispatch_response, RequestOutcome};
use crate::kafka::error::KafkaError;
use crate::kafka::handler_context::HandlerContext;
use crate::kafka::messages::{KafkaRequest, KafkaResponse};
use crate::kafka::notifications::InternalNotification;

mod admin;
mod configs;
mod consumer;
mod fetch;
mod group;
mod metadata;
mod produce;
mod sasl;
mod transaction;

pub use admin::*;
pub use configs::*;
pub use consumer::*;
pub use fetch::*;
pub use group::*;
pub use metadata::*;
pub use produce::*;
pub use sasl::*;
pub use transaction::*;

/// Requested `(topic, [partition])` pairs, the echo of the partition-addressed APIs.
pub type TopicPartitions = Vec<(String, Vec<i32>)>;

/// The response body type of API `A`.
pub type ResponseOf<A> = <<A as ApiSchema>::Wire as WireRequest>::Response;

/// The request header fields every API carries.
#[derive(Debug, Clone)]
pub struct RequestHeader {
    /// Wire API key.
    pub api_key: i16,
    /// Request version; the response is encoded at the same version.
    pub api_version: i16,
    /// Client-assigned id, echoed in the response.
    pub correlation_id: i32,
    /// Optional client identifier.
    pub client_id: Option<String>,
}

/// The two request lanes from the network thread to the DB thread (see
/// [`crate::kafka::messages::RequestLanes`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Everything that is not liveness traffic.
    Main,
    /// Liveness traffic (Heartbeat), drained by the DB thread before the main lane.
    Liveness,
}

/// Why [`KafkaApi::decode`] refused a request whose body did decode.
#[derive(Debug)]
pub enum Rejection<E> {
    /// Answer with [`ApiSchema::error_response`] for `echo` and `error_code`.
    Reply {
        /// Kafka error code to report.
        error_code: i16,
        /// What the error response should echo, if anything.
        echo: Option<E>,
    },
    /// Send nothing (the client expects no response, e.g. an acks=0 Produce).
    Silent,
    /// Send nothing and close the connection (an acks=0 Produce Kafka would fail, e.g. a
    /// corrupt batch: Kafka closes the connection so the client notices).
    Close(&'static str),
}

/// What a DB-thread handler may use besides its request.
pub struct ApiEnv<'a> {
    /// Store, coordinator, broker metadata, defaults.
    pub ctx: HandlerContext<'a>,
    /// Best-effort wake-ups for long-poll fetches (Produce, EndTxn).
    pub notify_tx: &'a Sender<InternalNotification>,
}

/// What every registered API has, whoever serves it: wire types, name, supported
/// versions and how to answer it with an error. The registry derives ApiVersions,
/// version enforcement, encoding (header version from the response type), metrics names
/// and error detection from this alone.
pub trait ApiSchema: Sized + 'static {
    /// The kafka-protocol request type; its `KEY` is the API key and its `Response` the
    /// response body.
    type Wire: WireRequest;
    /// What an error response echoes from the request (requested topics, partitions,
    /// groups, ...). `()` for an API whose response has a top-level error code only.
    type Echo: Clone + std::fmt::Debug + Send + 'static;

    /// Display name (logs and `kafka.api_metrics`).
    const NAME: &'static str;
    /// Lowest supported (advertised) request version.
    const MIN_VERSION: i16;
    /// Highest supported (advertised) request version.
    const MAX_VERSION: i16;

    /// A well-formed response at `version` reporting `error_code`: in the top-level error
    /// field where the response has one, and on every echoed entry.
    fn error_response(echo: Option<&Self::Echo>, version: i16, error_code: i16)
        -> ResponseOf<Self>;

    /// Whether `response` carries any non-zero error code (metrics).
    fn response_has_error(response: &ResponseOf<Self>) -> bool;
}

/// An API the DB thread serves: requests are decoded on the network thread into
/// [`KafkaApi::Request`], travel on a [`Lane`] and run in [`KafkaApi::handle`]. See the
/// module documentation.
///
/// APIs answered entirely on the network thread (SaslHandshake/SaslAuthenticate, from
/// the connection's SASL state in `listener::sasl` / `auth::session`) implement only
/// [`ApiSchema`] and are listed in the registry's `network_thread` section: they are
/// advertised, version-checked, encoded and counted like the others, but never decoded
/// into a [`KafkaRequest`] or dispatched to the worker.
pub trait KafkaApi: ApiSchema {
    /// The handler's input, built from `Wire` on the network thread.
    type Request: std::fmt::Debug + Send + 'static;

    /// The lane the request travels on.
    const LANE: Lane = Lane::Main;
    /// The handler may answer after the request's transaction (JoinGroup/SyncGroup are held
    /// by the coordinator). Such an API writes nothing to the database, so its response goes
    /// straight to the client instead of through the worker's commit barrier (RB-1).
    const DEFERRED_REPLY: bool = false;

    /// Turn the decoded wire request into the handler's input.
    fn decode(
        wire: Self::Wire,
        header: &RequestHeader,
    ) -> Result<Self::Request, Rejection<Self::Echo>>;

    /// The part of `request` an error response echoes.
    fn echo(request: &Self::Request) -> Self::Echo;

    /// Whether the client expects a response to `request` at all.
    fn expects_response(_request: &Self::Request) -> bool {
        true
    }

    /// The reply to a request whose version is outside `MIN_VERSION..=MAX_VERSION`, as
    /// `(encode_version, response)`, or `None` to close the connection (there is no
    /// response format to answer in). The default answers UNSUPPORTED_VERSION in the
    /// request's own version when kafka-protocol can encode that version.
    fn unsupported_version_reply(
        echo: Option<&Self::Echo>,
        version: i16,
    ) -> Option<(i16, ResponseOf<Self>)> {
        if !response_encodable::<Self>(version) {
            return None;
        }
        // An error response built for a version the API does not serve may set a field
        // that version lacks (e.g.
        // FindCoordinator v4+ has no top-level error code); it would fail to encode on
        // the network thread. Answer only with a response that encodes: the full one,
        // else one echoing nothing (e.g. AddPartitionsToTxn v4: a top-level error code
        // but no v0-3 per-partition results), else close the connection.
        let encodes = |r: &ResponseOf<Self>| r.encode(&mut BytesMut::new(), version).is_ok();
        let response = Self::error_response(echo, version, ERROR_UNSUPPORTED_VERSION);
        if encodes(&response) {
            return Some((version, response));
        }
        let bare = Self::error_response(None, version, ERROR_UNSUPPORTED_VERSION);
        encodes(&bare).then_some((version, bare))
    }

    /// Run the request on the DB thread. Most APIs call [`Call::run`] with their handler;
    /// it sends the response, answers a handler `Err` or panic with an error response, and
    /// reports whether the request's writes may commit.
    fn handle(request: Self::Request, call: Call<Self>, env: &ApiEnv<'_>) -> RequestOutcome;
}

/// The API key of `A`.
pub const fn api_key_of<A: ApiSchema>() -> i16 {
    <A::Wire as WireRequest>::KEY
}

/// Whether kafka-protocol can encode `A`'s response at `version`.
pub fn response_encodable<A: ApiSchema>(version: i16) -> bool {
    let range = <ResponseOf<A> as Message>::VERSIONS;
    (range.min..=range.max).contains(&version)
}

/// The response side of one request on the DB thread: where its response goes and how to
/// wrap it.
pub struct Call<A: KafkaApi> {
    header: RequestHeader,
    response_tx: UnboundedSender<KafkaResponse>,
    wrap: fn(ResponseOf<A>) -> ResponseBody,
}

impl<A: KafkaApi> Call<A> {
    fn new(
        header: RequestHeader,
        response_tx: UnboundedSender<KafkaResponse>,
        wrap: fn(ResponseOf<A>) -> ResponseBody,
    ) -> Self {
        Self {
            header,
            response_tx,
            wrap,
        }
    }

    /// The request's header and the client's response sender, for an API that answers
    /// later ([`KafkaApi::DEFERRED_REPLY`]).
    pub fn into_parts(self) -> (RequestHeader, UnboundedSender<KafkaResponse>) {
        (self.header, self.response_tx)
    }

    /// Run `handler` for `request` and send its response. A handler `Err` is answered with
    /// [`ApiSchema::error_response`] (echoing the request) and rolls the request's writes
    /// back; a panic is answered with UNKNOWN_SERVER_ERROR and re-raised, so the
    /// surrounding subtransaction rolls back (see [`dispatch_response`]).
    pub fn run<F>(self, request: A::Request, handler: F) -> RequestOutcome
    where
        F: FnOnce(A::Request, &RequestHeader) -> Result<ResponseOf<A>, KafkaError>,
    {
        let echo = A::echo(&request);
        let Call {
            header,
            response_tx,
            wrap,
        } = self;
        let (correlation_id, version) = (header.correlation_id, header.api_version);
        dispatch_response(
            A::NAME,
            &response_tx,
            || handler(request, &header),
            move |response| KafkaResponse::new(correlation_id, version, wrap(response)),
            move |error_code| {
                KafkaResponse::new(
                    correlation_id,
                    version,
                    wrap(A::error_response(Some(&echo), version, error_code)),
                )
            },
        )
    }
}

/// Outcome of decoding one request frame on the network thread.
pub enum Decoded {
    /// A request for the DB thread.
    Request(KafkaRequest),
    /// The request was answered (or deliberately not answered) on the network thread.
    Answered,
    /// No response can be shaped for this request: close the connection.
    Close(String),
}

/// Decode the body of a request for API `A` whose header has been parsed.
///
/// Every failure after a valid header is answered with a well-formed response of `A`
/// carrying an error code, with the request's correlation id:
///
/// - a version outside `MIN_VERSION..=MAX_VERSION` gets UNSUPPORTED_VERSION in the
///   requested version (ApiVersions: in v0 with the key list), echoing whatever the body
///   told us when kafka-protocol can decode it at that version; a version kafka-protocol
///   cannot encode a response in closes the connection (there is no format to answer in);
/// - a body that does not decode (an error, or a decoder panic on a truncated body) gets
///   INVALID_REQUEST;
/// - [`KafkaApi::decode`] may reject a decoded body with its own code and echo.
fn decode_api<A: KafkaApi>(
    buf: &mut BytesMut,
    header: RequestHeader,
    response_tx: UnboundedSender<KafkaResponse>,
    wrap_request: fn(A::Request) -> RequestBody,
    wrap_response: fn(ResponseOf<A>) -> ResponseBody,
) -> Decoded {
    let version = header.api_version;
    let reply = |encode_version: i16, response: ResponseOf<A>| {
        // A closed channel means the connection is gone; there is nobody to answer.
        let _ = response_tx.send(KafkaResponse::new(
            header.correlation_id,
            encode_version,
            wrap_response(response),
        ));
    };

    let parsed = decode_wire::<A>(buf, &header);

    if !(A::MIN_VERSION..=A::MAX_VERSION).contains(&version) {
        return reject_unsupported_version::<A>(parsed.as_ref(), version, reply);
    }

    match parsed {
        Some(Ok(request)) => Decoded::Request(KafkaRequest {
            body: wrap_request(request),
            header,
            response_tx,
        }),
        Some(Err(Rejection::Reply { error_code, echo })) => {
            reply(
                version,
                A::error_response(echo.as_ref(), version, error_code),
            );
            Decoded::Answered
        }
        Some(Err(Rejection::Silent)) => Decoded::Answered,
        Some(Err(Rejection::Close(reason))) => Decoded::Close(reason.to_string()),
        None => {
            reply(
                version,
                A::error_response(None, version, ERROR_INVALID_REQUEST),
            );
            Decoded::Answered
        }
    }
}

/// The body of a request for `A`, decoded and turned into the handler's input; `None` when
/// it does not decode (or kafka-protocol has no decoder for `version`).
///
/// The kafka-protocol body decoders index without bounds checks and panic on a truncated
/// body, so the decode runs under `catch_unwind`: a malformed body after a valid header is
/// answered with an error response instead of tearing the connection down.
fn decode_wire<A: KafkaApi>(
    buf: &mut BytesMut,
    header: &RequestHeader,
) -> Option<Result<A::Request, Rejection<A::Echo>>> {
    let version = header.api_version;
    let wire_range = <A::Wire as Message>::VERSIONS;
    if !(wire_range.min..=wire_range.max).contains(&version) {
        return None;
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        A::Wire::decode(buf, version)
    })) {
        Ok(Ok(wire)) => Some(A::decode(wire, header)),
        Ok(Err(e)) => {
            warn!("Failed to decode {} v{version} body: {e}", A::NAME);
            None
        }
        Err(_) => {
            warn!("Malformed {} v{version} body (decoder panicked)", A::NAME);
            None
        }
    }
}

/// Answer a request for `A` at a `version` outside the advertised range with the API's
/// own error response (when it can be encoded), echoing what the body told us.
fn reject_unsupported_version<A: KafkaApi>(
    parsed: Option<&Result<A::Request, Rejection<A::Echo>>>,
    version: i16,
    reply: impl FnOnce(i16, ResponseOf<A>),
) -> Decoded {
    warn!(
        "Rejecting {} version {version} outside supported range {}..={}",
        A::NAME,
        A::MIN_VERSION,
        A::MAX_VERSION
    );
    // A client that expects no response (an acks=0 Produce) cannot be told
    // UNSUPPORTED_VERSION, and dropping the request would lose it unnoticed: close the
    // connection, as Kafka does, so the client notices.
    let no_response = || {
        Decoded::Close(format!(
            "{} version {version} is unsupported and the client expects no response",
            A::NAME
        ))
    };
    let echo = match parsed {
        Some(Ok(request)) if !A::expects_response(request) => return no_response(),
        Some(Ok(request)) => Some(A::echo(request)),
        Some(Err(Rejection::Silent)) => return no_response(),
        Some(Err(Rejection::Close(reason))) => return Decoded::Close((*reason).to_string()),
        Some(Err(Rejection::Reply { echo, .. })) => echo.clone(),
        None => None,
    };
    match A::unsupported_version_reply(echo.as_ref(), version) {
        Some((encode_version, response)) => {
            reply(encode_version, response);
            Decoded::Answered
        }
        None => Decoded::Close(format!(
            "{} version {version} has no response format to answer in",
            A::NAME
        )),
    }
}

/// Encode `response` (header + body) at `version`.
fn encode_frame<A: ApiSchema>(
    response: &ResponseOf<A>,
    correlation_id: i32,
    version: i16,
    buf: &mut BytesMut,
) -> crate::kafka::error::Result<()> {
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header.encode(
        buf,
        <ResponseOf<A> as HeaderVersion>::header_version(version),
    )?;
    response.encode(buf, version)?;
    Ok(())
}

/// Static facts about one registered API.
#[derive(Debug, Clone, Copy)]
pub struct ApiInfo {
    /// Wire API key.
    pub key: i16,
    /// Display name.
    pub name: &'static str,
    /// Lowest supported version.
    pub min_version: i16,
    /// Highest supported version.
    pub max_version: i16,
    /// Who answers the API.
    pub served_by: ServedBy,
}

/// Where an API's requests are answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServedBy {
    /// Decoded on the network thread and run by the DB thread ([`KafkaApi`]), on this lane.
    Worker(Lane),
    /// Answered on the network thread from per-connection state (SASL), never sent to the
    /// worker; the API implements only [`ApiSchema`].
    NetworkThread,
}

impl ApiInfo {
    const fn of<A: ApiSchema>(served_by: ServedBy) -> Self {
        Self {
            key: api_key_of::<A>(),
            name: A::NAME,
            min_version: A::MIN_VERSION,
            max_version: A::MAX_VERSION,
            served_by,
        }
    }
}

/// The registered API with `api_key`.
pub fn api_info(api_key: i16) -> Option<&'static ApiInfo> {
    APIS.iter().find(|info| info.key == api_key)
}

/// Display name of `api_key` (`"Unknown"` for an API pg_kafka does not serve).
pub fn api_name(api_key: i16) -> &'static str {
    api_info(api_key).map_or("Unknown", |info| info.name)
}

/// The registry: one line per API, `Variant => ApiType`. It generates [`RequestBody`],
/// [`ResponseBody`], [`Echo`], [`APIS`] and the per-API matches below; nothing else in the
/// pipeline lists the APIs.
macro_rules! kafka_apis {
    (
        worker { $($variant:ident => $api:ty),* $(,)? }
        network_thread { $($net_variant:ident => $net_api:ty),* $(,)? }
    ) => {
        /// Every registered API, in registry order.
        pub static APIS: &[ApiInfo] = &[
            $(ApiInfo::of::<$api>(ServedBy::Worker(<$api as KafkaApi>::LANE)),)*
            $(ApiInfo::of::<$net_api>(ServedBy::NetworkThread),)*
        ];

        /// A decoded request body, one variant per registered API.
        #[derive(Debug)]
        pub enum RequestBody {
            $(
                #[doc = concat!("A decoded ", stringify!($variant), " request.")]
                $variant(<$api as KafkaApi>::Request),
            )*
        }

        /// A response body, one variant per registered API.
        #[derive(Debug, Clone)]
        pub enum ResponseBody {
            $(
                #[doc = concat!("A ", stringify!($variant), " response.")]
                $variant(ResponseOf<$api>),
            )*
            $(
                #[doc = concat!("A ", stringify!($net_variant), " response.")]
                $net_variant(ResponseOf<$net_api>),
            )*
            /// The generic error frame for an API key pg_kafka does not serve (there is no
            /// response schema to answer in): correlation id, error code, null message.
            UnknownApi {
                /// Kafka error code.
                error_code: i16,
            },
            /// Not a frame: tells the connection's writer to close the
            /// connection once every earlier response is written (an acks=0 Produce that
            /// hit an error: Kafka closes the connection so the client refreshes its
            /// metadata). Sent through the request's response channel, so it passes the
            /// worker's commit barrier like a response (RB-1).
            CloseConnection,
        }

        /// What an error response for a request echoes, one variant per registered API.
        #[derive(Debug, Clone)]
        pub enum Echo {
            $(
                #[doc = concat!("What a ", stringify!($variant), " error response echoes.")]
                $variant(<$api as ApiSchema>::Echo),
            )*
        }

        impl RequestBody {
            /// Wire API key.
            pub fn api_key(&self) -> i16 {
                match self { $(Self::$variant(_) => api_key_of::<$api>(),)* }
            }
            /// Display name.
            pub fn api_name(&self) -> &'static str {
                match self { $(Self::$variant(_) => <$api as ApiSchema>::NAME,)* }
            }
            /// The lane this request travels on.
            pub fn lane(&self) -> Lane {
                match self { $(Self::$variant(_) => <$api as KafkaApi>::LANE,)* }
            }
            /// See [`KafkaApi::DEFERRED_REPLY`].
            pub fn deferred_reply(&self) -> bool {
                match self { $(Self::$variant(_) => <$api as KafkaApi>::DEFERRED_REPLY,)* }
            }
            /// Whether the client expects a response.
            pub fn expects_response(&self) -> bool {
                match self {
                    $(Self::$variant(r) => <$api as KafkaApi>::expects_response(r),)*
                }
            }
            /// What an error response for this request echoes.
            pub fn echo(&self) -> Echo {
                match self { $(Self::$variant(r) => Echo::$variant(<$api as KafkaApi>::echo(r)),)* }
            }
        }

        impl Echo {
            /// The API's error response for this echo, at `version`.
            pub fn error_response(&self, version: i16, error_code: i16) -> ResponseBody {
                match self {
                    $(Self::$variant(e) => ResponseBody::$variant(
                        <$api as ApiSchema>::error_response(Some(e), version, error_code),
                    ),)*
                }
            }
        }

        impl ResponseBody {
            /// Wire API key (`None` for [`ResponseBody::UnknownApi`]).
            pub fn api_key(&self) -> Option<i16> {
                match self {
                    $(Self::$variant(_) => Some(api_key_of::<$api>()),)*
                    $(Self::$net_variant(_) => Some(api_key_of::<$net_api>()),)*
                    Self::UnknownApi { .. } | Self::CloseConnection => None,
                }
            }
            /// Whether the response carries any non-zero error code.
            pub fn has_error(&self) -> bool {
                match self {
                    $(Self::$variant(r) => <$api as ApiSchema>::response_has_error(r),)*
                    $(Self::$net_variant(r) => <$net_api as ApiSchema>::response_has_error(r),)*
                    Self::UnknownApi { error_code } => *error_code != 0,
                    Self::CloseConnection => true,
                }
            }
            /// Encode header and body.
            pub(crate) fn encode_frame(
                &self,
                correlation_id: i32,
                version: i16,
                buf: &mut BytesMut,
            ) -> crate::kafka::error::Result<()> {
                match self {
                    $(Self::$variant(r) => encode_frame::<$api>(r, correlation_id, version, buf),)*
                    $(Self::$net_variant(r) => encode_frame::<$net_api>(r, correlation_id, version, buf),)*
                    Self::UnknownApi { error_code } => {
                        encode_unknown_api_frame(correlation_id, *error_code, buf)
                    }
                    Self::CloseConnection => Err(KafkaError::encoding(
                        "CloseConnection is a connection instruction, not a frame",
                    )),
                }
            }
        }

        /// The error response of `api_key` at `version` with nothing echoed, or `None` for
        /// an API pg_kafka does not serve.
        pub fn error_response_for(api_key: i16, version: i16, error_code: i16) -> Option<ResponseBody> {
            $(
                if api_key == api_key_of::<$api>() {
                    return Some(ResponseBody::$variant(
                        <$api as ApiSchema>::error_response(None, version, error_code),
                    ));
                }
            )*
            $(
                if api_key == api_key_of::<$net_api>() {
                    return Some(ResponseBody::$net_variant(
                        <$net_api as ApiSchema>::error_response(None, version, error_code),
                    ));
                }
            )*
            None
        }

        /// Decode the body of a request whose header has been parsed; `None` for an API
        /// pg_kafka does not serve.
        pub(crate) fn decode_body(
            buf: &mut BytesMut,
            header: RequestHeader,
            response_tx: UnboundedSender<KafkaResponse>,
        ) -> Result<Decoded, (RequestHeader, UnboundedSender<KafkaResponse>)> {
            $(
                if header.api_key == api_key_of::<$api>() {
                    return Ok(decode_api::<$api>(
                        buf,
                        header,
                        response_tx,
                        RequestBody::$variant,
                        ResponseBody::$variant,
                    ));
                }
            )*
            $(
                // Answered by the connection (`listener::sasl`) before parse_request; a frame
                // that reaches the worker decoder anyway has no handler to run.
                if header.api_key == api_key_of::<$net_api>() {
                    return Ok(Decoded::Close(format!(
                        "{} is answered on the network thread",
                        <$net_api as ApiSchema>::NAME
                    )));
                }
            )*
            Err((header, response_tx))
        }

        /// Run `request` on the DB thread.
        pub fn dispatch(request: KafkaRequest, env: &ApiEnv<'_>) -> RequestOutcome {
            let KafkaRequest { header, body, response_tx } = request;
            match body {
                $(RequestBody::$variant(r) => <$api as KafkaApi>::handle(
                    r,
                    Call::new(header, response_tx, ResponseBody::$variant),
                    env,
                ),)*
            }
        }
    };
}

/// The generic error frame for an unknown API key: response header v0, error code, null
/// error message. Strict clients cannot parse it, but there is no schema to answer an
/// unknown API in; a client that sends one is not negotiating through ApiVersions.
fn encode_unknown_api_frame(
    correlation_id: i32,
    error_code: i16,
    buf: &mut BytesMut,
) -> crate::kafka::error::Result<()> {
    use bytes::BufMut;
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    header.encode(buf, 0)?;
    buf.put_i16(error_code);
    buf.put_i16(-1);
    Ok(())
}

kafka_apis! {
    worker {
        ApiVersions => ApiVersionsApi,
        Metadata => MetadataApi,
        Produce => ProduceApi,
        Fetch => FetchApi,
        ListOffsets => ListOffsetsApi,
        OffsetCommit => OffsetCommitApi,
        OffsetFetch => OffsetFetchApi,
        FindCoordinator => FindCoordinatorApi,
        JoinGroup => JoinGroupApi,
        Heartbeat => HeartbeatApi,
        LeaveGroup => LeaveGroupApi,
        SyncGroup => SyncGroupApi,
        DescribeGroups => DescribeGroupsApi,
        ListGroups => ListGroupsApi,
        CreateTopics => CreateTopicsApi,
        DeleteTopics => DeleteTopicsApi,
        CreatePartitions => CreatePartitionsApi,
        DeleteGroups => DeleteGroupsApi,
        InitProducerId => InitProducerIdApi,
        AddPartitionsToTxn => AddPartitionsToTxnApi,
        AddOffsetsToTxn => AddOffsetsToTxnApi,
        EndTxn => EndTxnApi,
        TxnOffsetCommit => TxnOffsetCommitApi,
        DeleteRecords => DeleteRecordsApi,
        DescribeConfigs => DescribeConfigsApi,
        IncrementalAlterConfigs => IncrementalAlterConfigsApi,
    }
    network_thread {
        SaslHandshake => SaslHandshakeApi,
        SaslAuthenticate => SaslAuthenticateApi,
    }
}

#[cfg(test)]
mod docs;
#[cfg(test)]
pub(crate) mod tests;
