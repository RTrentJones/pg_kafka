//! [`Wire`]: one raw connection with typed calls for the produce, fetch, offset and
//! transaction APIs. rdkafka hides what these tests assert on (base offsets of duplicate
//! batches, per-partition error codes, the batches inside a fetch response), so they
//! speak the protocol directly.

use super::{roundtrip, Result};
use bytes::Bytes;
use kafka_protocol::protocol::{Request, StrBytes};
use std::time::Duration;
use tokio::net::TcpStream;

/// One connection to the broker under test.
pub struct Wire {
    stream: TcpStream,
    correlation_id: i32,
}

impl Wire {
    pub async fn connect() -> Result<Self> {
        Self::connect_to(&crate::common::get_bootstrap_servers()).await
    }

    /// A connection to the broker at `addr` (`host:port`), for example the external
    /// Kafka of the shadow tests.
    pub async fn connect_to(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            correlation_id: 0,
        })
    }

    /// Send `request` at `version` and decode its response (within 30 s).
    pub async fn call<R: Request>(&mut self, version: i16, request: &R) -> Result<R::Response> {
        self.correlation_id += 1;
        let call = roundtrip(&mut self.stream, version, self.correlation_id, request);
        tokio::time::timeout(Duration::from_secs(30), call).await?
    }
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
        let resp: ProduceResponse = self.call(7, &req).await?;
        let p = &resp.responses[0].partition_responses[0];
        Ok((p.error_code, p.base_offset))
    }

    /// Produce v7 (acks=-1) of several topics' partitions in one request: every
    /// partition's `(topic, partition, error_code, base_offset)`, sorted.
    pub async fn produce_many(
        &mut self,
        topics: &[(&str, Vec<(i32, Bytes)>)],
        transactional_id: Option<&str>,
    ) -> Result<Vec<(String, i32, i16, i64)>> {
        let mut req = ProduceRequest::default();
        req.acks = -1;
        req.timeout_ms = 10_000;
        req.transactional_id = transactional_id.map(txn_id);
        for (name, parts) in topics {
            let mut t = TopicProduceData::default();
            t.name = topic_name(name);
            for (index, records) in parts {
                let mut p = PartitionProduceData::default();
                p.index = *index;
                p.records = Some(records.clone());
                t.partition_data.push(p);
            }
            req.topic_data.push(t);
        }
        let resp: ProduceResponse = self.call(7, &req).await?;
        let mut out: Vec<_> = resp
            .responses
            .iter()
            .flat_map(|t| {
                t.partition_responses
                    .iter()
                    .map(|p| (t.name.to_string(), p.index, p.error_code, p.base_offset))
            })
            .collect();
        out.sort();
        Ok(out)
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
        let resp: InitProducerIdResponse = self.call(1, &req).await?;
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
        let resp: AddPartitionsToTxnResponse = self.call(1, &req).await?;
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
        let resp: AddOffsetsToTxnResponse = self.call(1, &req).await?;
        Ok(resp.error_code)
    }

    /// TxnOffsetCommit v1 (generation -1, simple-consumer style).
    #[expect(
        clippy::too_many_arguments,
        reason = "one argument per TxnOffsetCommit field the tests vary"
    )]
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
        let resp: TxnOffsetCommitResponse = self.call(1, &req).await?;
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
        let resp: EndTxnResponse = self.call(1, &req).await?;
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
        let parts = self
            .fetch_partitions(topic, &[(partition, offset)], 1 << 20, isolation_level)
            .await?;
        Ok(parts[0].clone())
    }

    /// Fetch v11 (no wait) of `(partition, offset)`s of one topic, in that order, within
    /// `max_bytes` (each partition's own limit 1 MiB): the partitions' entries.
    pub async fn fetch_partitions(
        &mut self,
        topic: &str,
        partitions: &[(i32, i64)],
        max_bytes: i32,
        isolation_level: i8,
    ) -> Result<Vec<PartitionData>> {
        let mut t = FetchTopic::default();
        t.topic = topic_name(topic);
        t.partitions = partitions
            .iter()
            .map(|&(partition, offset)| {
                let mut p = FetchPartition::default();
                p.partition = partition;
                p.fetch_offset = offset;
                p.partition_max_bytes = 1 << 20;
                p.log_start_offset = -1;
                p.current_leader_epoch = -1;
                p.last_fetched_epoch = -1;
                p
            })
            .collect();
        let mut req = FetchRequest::default();
        req.replica_id = BrokerId(-1);
        req.max_wait_ms = 0;
        req.min_bytes = 0;
        req.max_bytes = max_bytes;
        req.isolation_level = isolation_level;
        req.session_epoch = -1;
        req.topics = vec![t];
        let resp: FetchResponse = self.call(11, &req).await?;
        Ok(resp.responses[0].partitions.clone())
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
        let resp: ListOffsetsResponse = self.call(7, &req).await?;
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
        let resp: OffsetFetchResponse = self.call(7, &req).await?;
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
        let resp: DeleteRecordsResponse = self.call(1, &req).await?;
        let p = &resp.topics[0].partitions[0];
        Ok((p.error_code, p.low_watermark))
    }
}
