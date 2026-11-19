//! Raw Kafka protocol client for the wave-3 txn tests: one TCP connection, typed
//! requests and responses from the kafka-protocol crate. rdkafka hides the details
//! these tests assert on (base offsets of duplicate batches, per-partition error codes,
//! the batches inside a fetch response), so they speak the protocol directly.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

/// One connection to the broker under test.
pub struct Wire {
    stream: TcpStream,
    correlation_id: i32,
}

impl Wire {
    pub async fn connect() -> Result<Self> {
        let addr = crate::common::get_bootstrap_servers();
        let stream = TcpStream::connect(addr.as_str()).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            correlation_id: 0,
        })
    }

    /// Send `request` as `api_key` v`version` and decode the response.
    pub async fn call<Req, Resp>(
        &mut self,
        api_key: i16,
        version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable + HeaderVersion,
        Resp: Decodable + HeaderVersion,
    {
        self.correlation_id += 1;
        let header = RequestHeader::default()
            .with_request_api_key(api_key)
            .with_request_api_version(version)
            .with_correlation_id(self.correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("w3-txn")));
        let mut body = BytesMut::new();
        header.encode(&mut body, Req::header_version(version))?;
        request.encode(&mut body, version)?;
        let mut frame = BytesMut::with_capacity(body.len() + 4);
        frame.put_i32(body.len() as i32);
        frame.put(body);
        self.stream.write_all(&frame).await?;

        let mut len = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(30), self.stream.read_exact(&mut len)).await??;
        let mut buf = vec![0u8; i32::from_be_bytes(len) as usize];
        self.stream.read_exact(&mut buf).await?;
        let mut buf = Bytes::from(buf);
        let header = ResponseHeader::decode(&mut buf, Resp::header_version(version))?;
        if header.correlation_id != self.correlation_id {
            return Err(format!(
                "correlation id {} != {}",
                header.correlation_id, self.correlation_id
            )
            .into());
        }
        Ok(Resp::decode(&mut buf, version)?)
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A RecordBatch v2 of `values` (timestamps `timestamps[i]`, or now), written by
/// `producer_id`/`epoch` from `base_sequence` (all -1 for a plain producer).
pub fn batch(
    values: &[&str],
    producer_id: i64,
    epoch: i16,
    base_sequence: i32,
    transactional: bool,
    timestamps: Option<&[i64]>,
) -> Bytes {
    let now = now_ms();
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, v)| Record {
            transactional,
            control: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch: epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            // The encoder keeps records in one batch while offset - sequence is
            // constant; the batch's base sequence is the first record's (-1 = none).
            sequence: base_sequence.wrapping_add(i as i32),
            timestamp: timestamps.map_or(now, |t| t[i]),
            key: None,
            value: Some(Bytes::copy_from_slice(v.as_bytes())),
            headers: Default::default(),
        })
        .collect();
    let mut out = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut out,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode batch");
    out.freeze()
}

/// The batch headers inside a fetched `records` blob:
/// `(base_offset, last_offset_delta, record_count)`.
pub fn batch_headers(records: &Bytes) -> Vec<(i64, i32, i32)> {
    let mut out = Vec::new();
    let mut b = records.clone();
    while b.remaining() >= 61 {
        let base = b.get_i64();
        let len = b.get_i32() as usize;
        let body = b.copy_to_bytes(len.min(b.remaining()));
        // body: leader epoch(4) magic(1) crc(4) attributes(2) last delta(4) ...
        let last_delta = i32::from_be_bytes(body[11..15].try_into().unwrap());
        let count = i32::from_be_bytes(body[45..49].try_into().unwrap());
        out.push((base, last_delta, count));
    }
    out
}

/// Every record (offset, value) in a fetched `records` blob.
pub fn fetched_values(records: &Bytes) -> Result<Vec<(i64, String)>> {
    let mut b = records.clone();
    let mut out = Vec::new();
    for set in RecordBatchDecoder::decode_all(&mut b)? {
        for r in set.records {
            let v = r
                .value
                .map(|v| String::from_utf8_lossy(&v).into_owned())
                .unwrap_or_default();
            out.push((r.offset, v));
        }
    }
    Ok(out)
}

use kafka_protocol::messages::{
    add_offsets_to_txn_request::AddOffsetsToTxnRequest,
    add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTopic},
    add_partitions_to_txn_response::AddPartitionsToTxnResponse,
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic},
    delete_records_response::DeleteRecordsResponse,
    end_txn_request::EndTxnRequest,
    end_txn_response::EndTxnResponse,
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    fetch_response::{FetchResponse, PartitionData},
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse,
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsResponse,
    offset_fetch_request::{OffsetFetchRequest, OffsetFetchRequestTopic},
    offset_fetch_response::OffsetFetchResponse,
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    produce_response::ProduceResponse,
    txn_offset_commit_request::{
        TxnOffsetCommitRequest, TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    },
    txn_offset_commit_response::TxnOffsetCommitResponse,
    BrokerId, GroupId, ProducerId, TopicName, TransactionalId,
};

fn topic_name(name: &str) -> TopicName {
    TopicName(StrBytes::from_string(name.to_string()))
}

fn txn_id(id: &str) -> TransactionalId {
    TransactionalId(StrBytes::from_string(id.to_string()))
}

impl Wire {
    /// Produce v7 (acks=-1): `(error_code, base_offset)` of the one partition.
    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        records: Bytes,
        transactional_id: Option<&str>,
    ) -> Result<(i16, i64)> {
        let mut p = PartitionProduceData::default();
        p.index = partition;
        p.records = Some(records);
        let mut t = TopicProduceData::default();
        t.name = topic_name(topic);
        t.partition_data = vec![p];
        let mut req = ProduceRequest::default();
        req.acks = -1;
        req.timeout_ms = 10_000;
        req.transactional_id = transactional_id.map(txn_id);
        req.topic_data = vec![t];
        let resp: ProduceResponse = self.call(0, 7, &req).await?;
        let p = &resp.responses[0].partition_responses[0];
        Ok((p.error_code, p.base_offset))
    }

    /// InitProducerId v1: `(error_code, producer_id, epoch)`.
    pub async fn init_producer_id(
        &mut self,
        transactional_id: Option<&str>,
        timeout_ms: i32,
    ) -> Result<(i16, i64, i16)> {
        let mut req = InitProducerIdRequest::default();
        req.transactional_id = transactional_id.map(txn_id);
        req.transaction_timeout_ms = timeout_ms;
        req.producer_id = ProducerId(-1);
        req.producer_epoch = -1;
        let resp: InitProducerIdResponse = self.call(22, 1, &req).await?;
        Ok((resp.error_code, resp.producer_id.0, resp.producer_epoch))
    }

    /// AddPartitionsToTxn v1: the per-partition codes in request order.
    pub async fn add_partitions(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        topics: &[(&str, &[i32])],
    ) -> Result<Vec<(String, i32, i16)>> {
        let mut req = AddPartitionsToTxnRequest::default();
        req.v3_and_below_transactional_id = txn_id(transactional_id);
        req.v3_and_below_producer_id = ProducerId(producer_id);
        req.v3_and_below_producer_epoch = epoch;
        req.v3_and_below_topics = topics
            .iter()
            .map(|(name, parts)| {
                let mut t = AddPartitionsToTxnTopic::default();
                t.name = topic_name(name);
                t.partitions = parts.to_vec();
                t
            })
            .collect();
        let resp: AddPartitionsToTxnResponse = self.call(24, 1, &req).await?;
        let mut out = Vec::new();
        for t in resp.results_by_topic_v3_and_below {
            for p in t.results_by_partition {
                out.push((
                    t.name.to_string(),
                    p.partition_index,
                    p.partition_error_code,
                ));
            }
        }
        Ok(out)
    }

    pub async fn add_offsets(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        group: &str,
    ) -> Result<i16> {
        let mut req = AddOffsetsToTxnRequest::default();
        req.transactional_id = txn_id(transactional_id);
        req.producer_id = ProducerId(producer_id);
        req.producer_epoch = epoch;
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        let resp: AddOffsetsToTxnResponse = self.call(25, 1, &req).await?;
        Ok(resp.error_code)
    }

    /// TxnOffsetCommit v1 (generation -1, simple-consumer style).
    #[allow(clippy::too_many_arguments)]
    pub async fn txn_offset_commit(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<i16> {
        let mut p = TxnOffsetCommitRequestPartition::default();
        p.partition_index = partition;
        p.committed_offset = offset;
        let mut t = TxnOffsetCommitRequestTopic::default();
        t.name = topic_name(topic);
        t.partitions = vec![p];
        let mut req = TxnOffsetCommitRequest::default();
        req.transactional_id = txn_id(transactional_id);
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.producer_id = ProducerId(producer_id);
        req.producer_epoch = epoch;
        req.generation_id = -1;
        req.topics = vec![t];
        let resp: TxnOffsetCommitResponse = self.call(28, 1, &req).await?;
        Ok(resp.topics[0].partitions[0].error_code)
    }

    pub async fn end_txn(
        &mut self,
        transactional_id: &str,
        producer_id: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<i16> {
        let mut req = EndTxnRequest::default();
        req.transactional_id = txn_id(transactional_id);
        req.producer_id = ProducerId(producer_id);
        req.producer_epoch = epoch;
        req.committed = commit;
        let resp: EndTxnResponse = self.call(26, 1, &req).await?;
        Ok(resp.error_code)
    }

    /// Fetch v11 of one partition (no wait).
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        isolation_level: i8,
    ) -> Result<PartitionData> {
        let mut p = FetchPartition::default();
        p.partition = partition;
        p.fetch_offset = offset;
        p.partition_max_bytes = 1 << 20;
        p.log_start_offset = -1;
        p.current_leader_epoch = -1;
        p.last_fetched_epoch = -1;
        let mut t = FetchTopic::default();
        t.topic = topic_name(topic);
        t.partitions = vec![p];
        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        req.max_wait_ms = 0;
        req.min_bytes = 0;
        req.max_bytes = 1 << 20;
        req.isolation_level = isolation_level;
        req.session_epoch = -1;
        req.topics = vec![t];
        let resp: FetchResponse = self.call(1, 11, &req).await?;
        Ok(resp.responses[0].partitions[0].clone())
    }

    /// ListOffsets v7: `(error_code, offset, timestamp)`.
    pub async fn list_offsets(
        &mut self,
        topic: &str,
        partition: i32,
        timestamp: i64,
        isolation_level: i8,
    ) -> Result<(i16, i64, i64)> {
        let mut p = ListOffsetsPartition::default();
        p.partition_index = partition;
        p.timestamp = timestamp;
        p.current_leader_epoch = -1;
        let mut t = ListOffsetsTopic::default();
        t.name = topic_name(topic);
        t.partitions = vec![p];
        let mut req = ListOffsetsRequest::default();
        req.replica_id = BrokerId(-1);
        req.isolation_level = isolation_level;
        req.topics = vec![t];
        let resp: ListOffsetsResponse = self.call(2, 7, &req).await?;
        let p = &resp.topics[0].partitions[0];
        Ok((p.error_code, p.offset, p.timestamp))
    }

    /// OffsetFetch v7: `(top-level error, [(partition, offset, error)])`.
    pub async fn offset_fetch(
        &mut self,
        group: &str,
        topic: &str,
        partitions: &[i32],
        require_stable: bool,
    ) -> Result<(i16, Vec<(i32, i64, i16)>)> {
        let mut t = OffsetFetchRequestTopic::default();
        t.name = topic_name(topic);
        t.partition_indexes = partitions.to_vec();
        let mut req = OffsetFetchRequest::default();
        req.group_id = GroupId(StrBytes::from_string(group.to_string()));
        req.topics = Some(vec![t]);
        req.require_stable = require_stable;
        let resp: OffsetFetchResponse = self.call(9, 7, &req).await?;
        let parts = resp
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .map(|p| (p.partition_index, p.committed_offset, p.error_code))
            .collect();
        Ok((resp.error_code, parts))
    }

    /// DeleteRecords v1 of one partition: `(error_code, low_watermark)`.
    pub async fn delete_records(
        &mut self,
        topic: &str,
        partition: i32,
        before: i64,
    ) -> Result<(i16, i64)> {
        let mut p = DeleteRecordsPartition::default();
        p.partition_index = partition;
        p.offset = before;
        let mut t = DeleteRecordsTopic::default();
        t.name = topic_name(topic);
        t.partitions = vec![p];
        let mut req = DeleteRecordsRequest::default();
        req.topics = vec![t];
        req.timeout_ms = 10_000;
        let resp: DeleteRecordsResponse = self.call(21, 1, &req).await?;
        let p = &resp.topics[0].partitions[0];
        Ok((p.error_code, p.low_watermark))
    }
}
