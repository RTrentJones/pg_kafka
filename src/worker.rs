// Background worker module for pg_kafka
//
// This module implements the persistent background worker that runs the Kafka
// protocol listener. The worker is started by Postgres when the extension loads
// (via shared_preload_libraries in postgresql.conf).

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;

// Import conditional logging macros from lib.rs
use crate::kafka::constants::*;
use crate::{pg_log, pg_warning};

/// Main entry point for the pg_kafka background worker.
///
/// IMPORTANT: This function MUST follow pgrx background worker conventions:
/// 1. Must be marked with #[pg_guard] to handle Postgres errors safely
/// 2. Must attach signal handlers BEFORE doing any work
/// 3. Must check for shutdown signals regularly and exit gracefully
/// 4. Must be marked with #[no_mangle] to ensure Postgres can find it
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_kafka_listener_main(_arg: pg_sys::Datum) {
    // Step 1: Attach to Postgres signal handling system
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Step 2: Connect to the database (required for SPI access in future)
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    // Step 3: Load configuration
    let config = crate::config::Config::load();
    log!(
        "pg_kafka background worker started (port: {}, host: {})",
        config.port,
        config.host
    );

    // Step 4: Create shutdown channel for communicating between sync and async worlds
    // The watch channel allows the sync signal handler to notify async tasks
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Step 5: Create tokio runtime for async TCP listener
    // CRITICAL: Must use current_thread runtime, NOT multi-threaded!
    // Postgres FFI cannot be called from multiple threads, and pgrx enforces this.
    // The current_thread runtime runs all tasks on a single thread.
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            pgrx::error!("Failed to create tokio runtime: {}", e);
        }
    };

    // Step 6: Start the TCP listener as a local task
    // We use LocalSet for single-threaded async execution
    let local_set = tokio::task::LocalSet::new();

    // Clone config for async context
    let port = config.port;
    let host = config.host.clone();
    let log_connections = config.log_connections;
    let shutdown_timeout = core::time::Duration::from_millis(config.shutdown_timeout_ms as u64);

    // Spawn the listener task on the LocalSet
    let listener_handle = local_set.spawn_local(async move {
        if let Err(e) = crate::kafka::run_listener(shutdown_rx, &host, port, log_connections).await
        {
            pgrx::error!("TCP listener error: {}", e);
        }
    });

    // Step 7: Get the request queue receiver
    // This is how we receive Kafka requests from the async tokio tasks
    let request_rx = crate::kafka::request_receiver();

    // Step 8: Main event loop
    // ═══════════════════════════════════════════════════════════════════════════════
    // MAIN EVENT LOOP: The Heart of the Async/Sync Architecture
    // ═══════════════════════════════════════════════════════════════════════════════
    //
    // This loop serves THREE critical functions:
    //
    // 1. PROCESS DATABASE REQUESTS (Sync World)
    //    - Receive Kafka requests from the queue
    //    - Execute blocking database operations via Postgres SPI
    //    - Send responses back to async tasks
    //    - WHY HERE: SPI can ONLY be called from the main thread in sync context
    //
    // 2. DRIVE ASYNC NETWORK I/O (Async World)
    //    - Run tokio tasks that handle TCP connections
    //    - Parse Kafka binary protocol from sockets
    //    - Accept new client connections
    //    - WHY SEPARATE: Async I/O is non-blocking and handles thousands of connections
    //
    // 3. CHECK FOR SHUTDOWN SIGNALS (Postgres Integration)
    //    - Poll for SIGTERM from Postgres
    //    - Ensure graceful shutdown on server stop
    //    - WHY POLLING: pgrx requires regular latch checks for signal handling
    //
    // The TIMING is carefully balanced:
    // - 100ms for async I/O: Long enough to batch network operations efficiently
    // - 1ms for signal check: Short enough for responsive shutdown
    // - Non-blocking queue check: No delay for processing database requests
    //
    // This architecture solves the fundamental incompatibility:
    // - Tokio wants async functions (network I/O benefits from non-blocking)
    // - Postgres SPI wants sync functions on main thread (database safety)
    // - The queue bridges these worlds cleanly
    // ═══════════════════════════════════════════════════════════════════════════════

    loop {
        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 1: Process Database Requests (SYNC, Blocking)         │
        // └─────────────────────────────────────────────────────────────┘
        // Process all pending requests without blocking.
        // Each request is processed in its own transaction using BackgroundWorker::transaction().
        // This is required for SPI calls (INSERT/SELECT) to work correctly.
        while let Ok(request) = request_rx.try_recv() {
            // CRITICAL: Wrap each request in a transaction
            // Background workers don't have implicit transactions like client sessions do.
            // BackgroundWorker::transaction() starts a transaction, calls our closure,
            // and commits/rolls back based on the result.
            BackgroundWorker::transaction(|| {
                process_request(request);
            });
        }

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 2: Drive Async Network I/O (ASYNC, Non-blocking)      │
        // └─────────────────────────────────────────────────────────────┘
        // block_on() is NOT a mistake here. We're running a sync loop
        // that periodically executes async code for 100ms.
        // This drives the LocalSet forward, processing:
        // - TCP accept() calls
        // - Socket read()/write() calls
        // - Kafka protocol parsing
        // Then we return to sync context to process database requests.
        runtime.block_on(async {
            tokio::select! {
                // Timeout after ASYNC_IO_INTERVAL_MS to return to sync processing
                _ = tokio::time::sleep(core::time::Duration::from_millis(crate::kafka::ASYNC_IO_INTERVAL_MS)) => {
                    // Normal path: timeout reached, go check for database requests
                }
                // Fallback: If listener exits (shouldn't happen during normal operation)
                _ = local_set.run_until(async {
                    std::future::pending::<()>().await
                }) => {
                    // LocalSet completed - listener task finished unexpectedly
                }
            }
        });

        // ┌─────────────────────────────────────────────────────────────┐
        // │ PART 3: Check for Shutdown Signal (POSTGRES Integration)   │
        // └─────────────────────────────────────────────────────────────┘
        // wait_latch(SIGNAL_CHECK_INTERVAL_MS) returns false when Postgres sends SIGTERM.
        // This is the SYNC boundary - we're fully outside async context.
        // CRITICAL: This must be polled regularly for graceful shutdown.
        if !BackgroundWorker::wait_latch(Some(core::time::Duration::from_millis(
            crate::kafka::SIGNAL_CHECK_INTERVAL_MS,
        ))) {
            log!("pg_kafka background worker shutting down");

            // Signal the async listener to stop
            let _ = shutdown_tx.send(true);

            // Step 10: Graceful shutdown - wait for listener to finish
            runtime.block_on(async {
                // Give the listener configured timeout to shut down cleanly
                // We need to drive the LocalSet to completion for the listener to actually finish
                let shutdown_result =
                    tokio::time::timeout(shutdown_timeout, local_set.run_until(listener_handle))
                        .await;

                match shutdown_result {
                    Ok(Ok(_)) => {
                        log!("pg_kafka TCP listener shut down cleanly");
                    }
                    Ok(Err(e)) => {
                        pgrx::warning!("pg_kafka TCP listener error during shutdown: {:?}", e);
                    }
                    Err(_) => {
                        pgrx::warning!("pg_kafka TCP listener shutdown timed out");
                    }
                }
            });

            break;
        }
    }
}

/// Get metadata for all topics in the database
fn get_all_topics_metadata(
) -> Vec<kafka_protocol::messages::metadata_response::MetadataResponseTopic> {
    use crate::kafka::constants::*;

    // Query all topics from the database
    // We need to extract data inside the SPI connection closure due to lifetimes
    Spi::connect(|client| {
        let table = match client.select("SELECT name FROM kafka.topics ORDER BY name", None, &[]) {
            Ok(t) => t,
            Err(e) => {
                pg_warning!("Failed to query topics from database: {}", e);
                return Vec::new(); // Return empty list on error
            }
        };

        let mut topics_metadata = Vec::new();
        for row in table {
            if let Ok(Some(name)) = row.get::<&str>(1) {
                let partition = crate::kafka::build_partition_metadata(
                    0,
                    DEFAULT_BROKER_ID,
                    vec![DEFAULT_BROKER_ID],
                    vec![DEFAULT_BROKER_ID],
                );
                let topic = crate::kafka::build_topic_metadata(
                    name.to_string(),
                    ERROR_NONE,
                    vec![partition],
                );
                topics_metadata.push(topic);
            }
        }
        topics_metadata
    })
}

/// Get or create a topic by name, returning its ID
///
/// This function uses PostgreSQL's SPI (Server Programming Interface) to:
/// 1. Try to INSERT a new topic (will fail if it already exists due to UNIQUE constraint)
/// 2. Use ON CONFLICT to return the existing ID if the topic already exists
///
/// This is more efficient than SELECT-then-INSERT because it's a single round-trip.
///
/// # Rust Explanation
/// - `Result<i32, Box<dyn std::error::Error>>`: Returns topic ID on success, error on failure
/// - `Spi::connect()`: Opens a connection to the Postgres database
/// - `|client| { ... }`: This is a "closure" (anonymous function) that receives the SPI client
/// - `?` operator: Propagates errors up the call stack (like try/catch in other languages)
fn get_or_create_topic(topic_name: &str) -> Result<i32, Box<dyn std::error::Error>> {
    pg_log!("get_or_create_topic: '{}'", topic_name);

    // Wrap SPI calls in a connection context
    // Background workers don't have an implicit transaction, so we need to explicitly
    // start one via Spi::connect() before making any SPI calls
    Spi::connect(|_client| {
        // SQL: Try to INSERT, but if name already exists (UNIQUE constraint),
        // return the existing row's ID instead. This is an "upsert" pattern.
        //
        // Convert &str to String for type compatibility with IntoDatum
        let topic_name_string = topic_name.to_string();

        let topic_id: i32 = Spi::get_one_with_args::<i32>(
            "INSERT INTO kafka.topics (name, partitions)
             VALUES ($1, $2)
             ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
             RETURNING id",
            &[
                topic_name_string.into(),
                crate::kafka::DEFAULT_TOPIC_PARTITIONS.into(),
            ],
        )?
        .ok_or("Failed to get topic ID")?;

        pg_log!("Topic '{}' has id={}", topic_name, topic_id);
        Ok(topic_id)
    })
}

/// Insert records into kafka.messages table with dual-offset design
///
/// This function implements the core dual-offset INSERT pattern:
/// 1. Get the current MAX(partition_offset) with FOR UPDATE lock (prevents race conditions)
/// 2. Insert all records with sequential partition_offsets
/// 3. global_offset is auto-generated by PostgreSQL's BIGSERIAL
///
/// # Dual-Offset Design
/// - `partition_offset`: Kafka-compatible offset (0, 1, 2, ... per partition)
/// - `global_offset`: Temporal ordering across ALL partitions (auto-incremented)
///
/// # Rust Explanation
/// - `&[Record]`: A "slice" (view into an array) - doesn't own the data
/// - `-> Result<i64, ...>`: Returns the first partition_offset on success
fn insert_records(
    topic_id: i32,
    partition_id: i32,
    records: &[crate::kafka::messages::Record],
) -> Result<i64, Box<dyn std::error::Error>> {
    // Early return if no records to insert
    if records.is_empty() {
        return Ok(0);
    }

    pg_log!(
        "Inserting {} records for topic_id={}, partition_id={}",
        records.len(),
        topic_id,
        partition_id
    );

    // Execute within the current SPI transaction
    // IMPORTANT: The SELECT with FOR UPDATE and all INSERTs must be in the same transaction
    // for atomicity and to prevent race conditions on partition_offset calculation
    // NOTE: We're already in a transaction (started by BackgroundWorker::transaction()),
    // so we don't call BEGIN here (it would fail with "already in transaction" error).
    // The transaction will be committed by BackgroundWorker::transaction() when process_request() returns.
    Spi::connect_mut(|client| {
        let result = (|| -> Result<i64, Box<dyn std::error::Error>> {
            // ============================================
            // STEP 1: Get current max offset with lock
            // ============================================
            // Lock THIS SPECIFIC PARTITION for exclusive access to prevent race conditions.
            // CRITICAL: We use PostgreSQL advisory locks to lock (topic_id, partition_id) pair.
            // This allows concurrent writes to DIFFERENT partitions of the same topic,
            // which is the whole point of partitioning!
            //
            // pg_advisory_xact_lock(key1, key2) automatically releases at transaction end.
            // The lock is held until the transaction commits/rolls back.
            client.select(
                "SELECT pg_advisory_xact_lock($1, $2)",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            // Now get the max offset (no FOR UPDATE needed since we have topic lock)
            let table = client.select(
                "SELECT COALESCE(MAX(partition_offset), -1) as max_offset
                 FROM kafka.messages
                 WHERE topic_id = $1 AND partition_id = $2",
                None,
                &[topic_id.into(), partition_id.into()],
            )?;

            let max_offset: i64 = table
                .first()
                .get_by_name::<i64, _>("max_offset")?
                .unwrap_or(-1);

            // Calculate the first offset for this batch
            let base_offset = max_offset + 1;

            pg_log!(
                "Current max_offset={}, new base_offset={}",
                max_offset,
                base_offset
            );

            // ============================================
            // STEP 2: Insert all records (one INSERT per record)
            // ============================================
            // Note: We use individual INSERTs instead of multi-row INSERT because:
            // 1. Safety: Parameterized queries prevent SQL injection
            // 2. Simplicity: pgrx's SPI doesn't easily support dynamic multi-row syntax
            // 3. Atomicity: All INSERTs in same transaction, so batch is atomic
            // 4. Performance: Acceptable for Phase 2 (Kafka batches ~100-1000 msgs)
            //
            // Future optimization: Use PostgreSQL COPY protocol for 10x speedup
            //
            // Each record gets:
            // - Sequential partition_offset (base_offset + i)
            // - Auto-incremented global_offset (BIGSERIAL)

            for (i, record) in records.iter().enumerate() {
                let offset = base_offset + i as i64;

                // Serialize headers to JSONB format
                // Note: We propagate errors instead of silently converting to {}
                let headers_json = if record.headers.is_empty() {
                    "{}".to_string()
                } else {
                    let headers_map: std::collections::HashMap<String, String> = record
                        .headers
                        .iter()
                        .map(|h| (h.key.clone(), hex_encode(&h.value)))
                        .collect();
                    serde_json::to_string(&headers_map).map_err(|e| {
                        format!("Failed to serialize headers for record {}: {}", i, e)
                    })?
                };

                // Convert key and value for IntoDatum compatibility
                // Note: Cloning is required because pgrx IntoDatum takes ownership
                let key_vec: Option<Vec<u8>> = record.key.clone();
                let value_vec: Option<Vec<u8>> = record.value.clone();

                // Insert this record with parameterized query
                client.update(
                    "INSERT INTO kafka.messages (topic_id, partition_id, partition_offset, key, value, headers)
                     VALUES ($1, $2, $3, $4, $5, $6::jsonb)",
                    None,
                    &[
                        topic_id.into(),
                        partition_id.into(),
                        offset.into(),
                        key_vec.into(),
                        value_vec.into(),
                        headers_json.into(),
                    ],
                ).map_err(|e| format!("Failed to insert record {} (offset {}): {}", i, offset, e))?;
            }

            pg_log!(
                "Successfully inserted {} records (offsets {} to {})",
                records.len(),
                base_offset,
                base_offset + records.len() as i64 - 1
            );

            Ok(base_offset)
        })();

        // Return the result directly
        // The outer BackgroundWorker::transaction() will handle commit/rollback
        result
    })
}

/// Hex-encode binary data for JSONB storage
///
/// We store binary header values as hex strings in JSONB because JSONB doesn't
/// support raw binary data. Each byte becomes two hex characters (e.g., 0xFF -> "ff")
///
/// # Rust Explanation
/// - `&[u8]`: A byte slice (array of bytes)
/// - `.iter()`: Create an iterator over the bytes
/// - `.map(|b| ...)`: Transform each byte
/// - `.collect()`: Collect into a String
fn hex_encode(data: &[u8]) -> String {
    data.iter()
        .map(|b| format!("{:02x}", b)) // Format each byte as 2-digit hex
        .collect() // Collect into String
}

/// Process a Kafka request and send the response
///
/// In Step 3, we implemented ApiVersions.
/// In Step 4, we're adding Metadata support with hardcoded topics.
/// In Phase 2, we'll add SPI calls to query actual database tables.
///
/// # Testing
/// This function is public to allow unit testing, but is not part of the
/// public API and should not be called directly by external code.
#[doc(hidden)]
pub fn process_request(request: crate::kafka::KafkaRequest) {
    use crate::kafka::messages::KafkaResponse;

    pg_log!("process_request() called!");

    match request {
        crate::kafka::KafkaRequest::ApiVersions {
            correlation_id,
            client_id,
            api_version,
            response_tx,
        } => {
            pg_log!(
                "Processing ApiVersions request, correlation_id: {}, api_version: {}",
                correlation_id,
                api_version
            );
            if let Some(id) = client_id {
                pg_log!("ApiVersions request from client: {}", id);
            }

            // Build ApiVersionsResponse using helper
            let kafka_response = crate::kafka::build_api_versions_response();

            pg_log!(
                "Building ApiVersions response with {} API versions",
                kafka_response.api_keys.len()
            );

            let response = KafkaResponse::ApiVersions {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send ApiVersions response: {}", e);
            } else {
                pg_log!("ApiVersions response sent successfully to async task");
            }
        }
        crate::kafka::KafkaRequest::Metadata {
            correlation_id,
            client_id,
            api_version,
            topics: requested_topics,
            response_tx,
        } => {
            pg_log!(
                "Processing Metadata request, correlation_id: {}",
                correlation_id
            );
            if let Some(id) = client_id {
                pg_log!("Metadata request from client: {}", id);
            }

            // Get configuration for our broker info
            let config = crate::config::Config::load();

            // CRITICAL: Convert bind address to advertised address
            // We bind to 0.0.0.0 (all interfaces) but must advertise a routable address
            // that clients can actually connect to. If host is 0.0.0.0, advertise localhost.
            let advertised_host = if config.host == "0.0.0.0" || config.host.is_empty() {
                pg_log!(
                    "Converting bind address '{}' to advertised address 'localhost'",
                    config.host
                );
                "localhost".to_string()
            } else {
                pg_log!(
                    "Using configured host '{}' as advertised address",
                    config.host
                );
                config.host.clone()
            };

            // Build MetadataResponse using kafka-protocol types
            let mut kafka_response =
                kafka_protocol::messages::metadata_response::MetadataResponse::default();

            // Add broker
            kafka_response
                .brokers
                .push(crate::kafka::build_broker_metadata(
                    DEFAULT_BROKER_ID,
                    advertised_host,
                    config.port,
                ));

            // Build topic metadata based on what was requested
            let topics_to_add = match requested_topics {
                None => {
                    // Client wants all topics - query from database
                    pg_log!("Building metadata for ALL topics");
                    get_all_topics_metadata()
                }
                Some(topic_names) => {
                    // Client wants specific topics - create them if needed and return metadata
                    pg_log!("Building metadata for requested topics: {:?}", topic_names);
                    let mut topics_metadata = Vec::new();
                    for topic_name in topic_names {
                        // Auto-create topic if it doesn't exist
                        match get_or_create_topic(&topic_name) {
                            Ok(_topic_id) => {
                                // Return metadata for this topic
                                let partition = crate::kafka::build_partition_metadata(
                                    0,
                                    DEFAULT_BROKER_ID,
                                    vec![DEFAULT_BROKER_ID],
                                    vec![DEFAULT_BROKER_ID],
                                );
                                let topic = crate::kafka::build_topic_metadata(
                                    topic_name,
                                    ERROR_NONE,
                                    vec![partition],
                                );
                                topics_metadata.push(topic);
                            }
                            Err(e) => {
                                pg_warning!("Failed to get/create topic '{}': {}", topic_name, e);
                                // Return error for this topic
                                let topic = crate::kafka::build_topic_metadata(
                                    topic_name,
                                    ERROR_UNKNOWN_SERVER_ERROR,
                                    vec![],
                                );
                                topics_metadata.push(topic);
                            }
                        }
                    }
                    topics_metadata
                }
            };

            kafka_response.topics = topics_to_add;

            pg_log!(
                "Building Metadata response with {} brokers and {} topics",
                kafka_response.brokers.len(),
                kafka_response.topics.len()
            );

            let response = KafkaResponse::Metadata {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Metadata response: {}", e);
            } else {
                pg_log!("Metadata response sent successfully to async task");
            }
        }
        crate::kafka::KafkaRequest::Produce {
            correlation_id,
            client_id,
            api_version,
            acks,
            timeout_ms,
            topic_data,
            response_tx,
        } => {
            pg_log!(
                "Processing Produce request, correlation_id: {}",
                correlation_id
            );
            if let Some(id) = &client_id {
                pg_log!("Produce request from client: {}", id);
            }
            pg_log!(
                "Produce parameters: acks={}, timeout_ms={}",
                acks,
                timeout_ms
            );

            // Handle acks=0 (fire-and-forget) - not yet supported
            if acks == 0 {
                pg_warning!("acks=0 (fire-and-forget) not yet supported");
                let error_response = KafkaResponse::Error {
                    correlation_id,
                    error_code: ERROR_UNSUPPORTED_VERSION,
                    error_message: Some("acks=0 not yet supported".to_string()),
                };
                if let Err(e) = response_tx.send(error_response) {
                    pg_warning!("Failed to send error response: {}", e);
                }
                return;
            }

            // Build ProduceResponse using kafka-protocol types
            let mut kafka_response = crate::kafka::build_produce_response();

            // Process each topic
            for topic in topic_data {
                let topic_name = topic.name.clone();
                pg_log!("Processing topic: {}", topic_name);

                // Get or create topic
                let topic_id = match get_or_create_topic(&topic_name) {
                    Ok(id) => {
                        pg_log!("Topic '{}' has ID: {}", topic_name, id);
                        id
                    }
                    Err(e) => {
                        pg_warning!("Failed to get/create topic '{}': {:?}", topic_name, e);
                        let error_response = KafkaResponse::Error {
                            correlation_id,
                            error_code: ERROR_UNKNOWN_SERVER_ERROR,
                            error_message: Some(format!("Failed to create topic: {}", e)),
                        };
                        if let Err(e) = response_tx.send(error_response) {
                            pg_warning!("Failed to send error response: {}", e);
                        }
                        return;
                    }
                };

                // Build topic response
                let mut topic_response =
                    kafka_protocol::messages::produce_response::TopicProduceResponse::default();
                topic_response.name =
                    kafka_protocol::messages::TopicName(topic_name.clone().into());

                // Process each partition
                for partition in topic.partitions {
                    let partition_index = partition.partition_index;
                    pg_log!(
                        "Processing partition {} with {} records",
                        partition_index,
                        partition.records.len()
                    );

                    let mut partition_response = kafka_protocol::messages::produce_response::PartitionProduceResponse::default();
                    partition_response.index = partition_index;

                    // Validate partition index (we only support partition 0 for now)
                    if partition_index != 0 {
                        pg_warning!(
                            "Invalid partition index {} for topic '{}' (only partition 0 supported)",
                            partition_index,
                            topic_name
                        );
                        partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                        partition_response.base_offset = -1;
                        partition_response.log_append_time_ms = -1;
                        partition_response.log_start_offset = -1;
                        topic_response.partition_responses.push(partition_response);
                        continue;
                    }

                    // Insert records
                    match insert_records(topic_id, partition_index, &partition.records) {
                        Ok(base_offset) => {
                            pg_log!(
                                "Successfully inserted {} records starting at partition_offset {}",
                                partition.records.len(),
                                base_offset
                            );
                            partition_response.error_code = ERROR_NONE;
                            partition_response.base_offset = base_offset;
                            partition_response.log_append_time_ms = -1; // Not tracking log append time yet
                            partition_response.log_start_offset = -1; // Not tracking log start offset yet
                        }
                        Err(e) => {
                            pg_warning!(
                                "Failed to insert records for topic '{}', partition {}: {:?}",
                                topic_name,
                                partition_index,
                                e
                            );
                            partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            partition_response.base_offset = -1;
                            partition_response.log_append_time_ms = -1;
                            partition_response.log_start_offset = -1;
                        }
                    }

                    topic_response.partition_responses.push(partition_response);
                }

                kafka_response.responses.push(topic_response);
            }

            // Send successful response
            let response = KafkaResponse::Produce {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Produce response: {}", e);
            } else {
                pg_log!("Produce response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::Fetch {
            correlation_id,
            client_id: _,
            api_version,
            max_wait_ms: _,
            min_bytes: _,
            max_bytes: _,
            topic_data,
            response_tx,
        } => {
            pg_log!(
                "Processing Fetch request, correlation_id: {}",
                correlation_id
            );

            // Import types needed for FetchResponse
            use kafka_protocol::messages::fetch_response::{
                FetchResponse, FetchableTopicResponse, PartitionData,
            };
            use kafka_protocol::messages::TopicName;
            use kafka_protocol::protocol::StrBytes;
            use kafka_protocol::records::{
                Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
            };

            let mut responses = Vec::new();

            // Process each topic requested
            for topic_fetch in topic_data {
                let topic_name = topic_fetch.name.clone();

                pg_log!("Fetching from topic: {}", topic_name);

                // Look up topic_id
                // TODO: Use prepared statements when pgrx supports them to prevent SQL injection
                let topic_name_escaped = topic_name.replace("'", "''");
                let topic_id_result = BackgroundWorker::transaction(|| {
                    pgrx::Spi::get_one::<i32>(&format!(
                        "SELECT id FROM kafka.topics WHERE name = '{}'",
                        topic_name_escaped
                    ))
                });

                let topic_id = match topic_id_result {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        pg_warning!("Topic not found: {}", topic_name);
                        // Return error response for this topic
                        let mut error_partitions = Vec::new();
                        for p in topic_fetch.partitions {
                            let mut partition_data = PartitionData::default();
                            partition_data.partition_index = p.partition_index;
                            partition_data.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                            partition_data.high_watermark = -1;
                            error_partitions.push(partition_data);
                        }
                        let mut topic_response = FetchableTopicResponse::default();
                        topic_response.topic = TopicName(StrBytes::from_string(topic_name));
                        topic_response.partitions = error_partitions;
                        responses.push(topic_response);
                        continue;
                    }
                    Err(e) => {
                        pg_warning!("Failed to query topic: {}", e);
                        continue;
                    }
                };

                let mut partition_responses = Vec::new();

                // Process each partition
                for partition_fetch in topic_fetch.partitions {
                    let partition_id = partition_fetch.partition_index;
                    let fetch_offset = partition_fetch.fetch_offset;

                    pg_log!(
                        "Fetching topic_id={}, partition={}, offset>={}",
                        topic_id,
                        partition_id,
                        fetch_offset
                    );

                    // Query messages from database
                    let records_result = BackgroundWorker::transaction(|| {
                        let query = format!(
                            "SELECT partition_offset, key, value
                             FROM kafka.messages
                             WHERE topic_id = {} AND partition_id = {}
                               AND partition_offset >= {}
                             ORDER BY partition_offset
                             LIMIT 1000",
                            topic_id, partition_id, fetch_offset
                        );

                        pgrx::Spi::connect(|client| {
                            let mut records = Vec::new();
                            let tup_table = client.select(&query, Some(1), &[])?;

                            for row in tup_table {
                                let offset: i64 = row[1].value()?.unwrap_or(0);
                                let key: Option<Vec<u8>> = row[2].value()?;
                                let value: Option<Vec<u8>> = row[3].value()?;
                                // For now, use current time as timestamp
                                // TODO: Properly convert PostgreSQL timestamp to milliseconds since epoch
                                let timestamp_ms = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis()
                                    as i64;

                                records.push((offset, key, value, timestamp_ms));
                            }

                            Ok::<
                                Vec<(i64, Option<Vec<u8>>, Option<Vec<u8>>, i64)>,
                                Box<dyn std::error::Error>,
                            >(records)
                        })
                    });

                    let db_records = match records_result {
                        Ok(records) => records,
                        Err(e) => {
                            pg_warning!("Failed to fetch records: {}", e);
                            let mut partition_data = PartitionData::default();
                            partition_data.partition_index = partition_id;
                            partition_data.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            partition_data.high_watermark = -1;
                            partition_responses.push(partition_data);
                            continue;
                        }
                    };

                    pg_log!("Fetched {} records", db_records.len());

                    // Get high watermark (next offset to be written)
                    let high_watermark_result = BackgroundWorker::transaction(|| {
                        pgrx::Spi::get_one::<i64>(&format!(
                            "SELECT COALESCE(MAX(partition_offset) + 1, 0)
                             FROM kafka.messages
                             WHERE topic_id = {} AND partition_id = {}",
                            topic_id, partition_id
                        ))
                    });

                    let high_watermark = high_watermark_result.unwrap_or(Some(0)).unwrap_or(0);

                    // Convert database records to Kafka RecordBatch format
                    let records_bytes = if !db_records.is_empty() {
                        let kafka_records: Vec<Record> = db_records
                            .into_iter()
                            .map(|(offset, key, value, timestamp_ms)| Record {
                                transactional: false,
                                control: false,
                                partition_leader_epoch: 0,
                                producer_id: -1,
                                producer_epoch: -1,
                                timestamp_type: TimestampType::Creation,
                                offset,
                                sequence: offset as i32,
                                timestamp: timestamp_ms,
                                key: key.map(bytes::Bytes::from),
                                value: value.map(bytes::Bytes::from),
                                headers: Default::default(),
                            })
                            .collect();

                        // Encode records as RecordBatch using the pattern from the example
                        let mut encoded = bytes::BytesMut::new();
                        if let Err(e) = RecordBatchEncoder::encode(
                            &mut encoded,
                            kafka_records.iter(),
                            &RecordEncodeOptions {
                                version: 2,
                                compression: Compression::None,
                            },
                        ) {
                            pg_warning!("Failed to encode RecordBatch: {}", e);
                            None
                        } else {
                            Some(encoded.freeze())
                        }
                    } else {
                        None
                    };

                    let mut partition_data = PartitionData::default();
                    partition_data.partition_index = partition_id;
                    partition_data.error_code = ERROR_NONE;
                    partition_data.high_watermark = high_watermark;
                    partition_data.last_stable_offset = high_watermark;
                    partition_data.log_start_offset = 0;
                    partition_data.records = records_bytes;
                    partition_responses.push(partition_data);
                }

                let mut topic_response = FetchableTopicResponse::default();
                topic_response.topic = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = partition_responses;
                responses.push(topic_response);
            }

            let mut kafka_response = FetchResponse::default();
            kafka_response.throttle_time_ms = 0;
            kafka_response.error_code = ERROR_NONE;
            kafka_response.session_id = 0;
            kafka_response.responses = responses;

            let response = KafkaResponse::Fetch {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send Fetch response: {}", e);
            } else {
                pg_log!("Fetch response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::OffsetCommit {
            correlation_id,
            client_id: _,
            api_version,
            group_id,
            topics,
            response_tx,
        } => {
            pg_log!(
                "Processing OffsetCommit request, correlation_id: {}, group_id: {}",
                correlation_id,
                group_id
            );

            use kafka_protocol::messages::offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            };
            use kafka_protocol::messages::TopicName;
            use kafka_protocol::protocol::StrBytes;

            let mut response_topics = Vec::new();

            // Process each topic
            for topic in topics {
                let topic_name = topic.name.clone();

                // Look up topic_id
                // TODO: Use prepared statements when pgrx supports them to prevent SQL injection
                let topic_name_escaped = topic_name.replace("'", "''");
                let topic_id_result = BackgroundWorker::transaction(|| {
                    pgrx::Spi::get_one::<i32>(&format!(
                        "SELECT id FROM kafka.topics WHERE name = '{}'",
                        topic_name_escaped
                    ))
                });

                let topic_id = match topic_id_result {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        pg_warning!("Topic not found for OffsetCommit: {}", topic_name);
                        // Return error for all partitions in this topic
                        let mut error_partitions = Vec::new();
                        for partition in topic.partitions {
                            let mut partition_response = OffsetCommitResponsePartition::default();
                            partition_response.partition_index = partition.partition_index;
                            partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                            error_partitions.push(partition_response);
                        }

                        let mut topic_response = OffsetCommitResponseTopic::default();
                        topic_response.name = TopicName(StrBytes::from_string(topic_name));
                        topic_response.partitions = error_partitions;
                        response_topics.push(topic_response);
                        continue;
                    }
                    Err(e) => {
                        pg_warning!("Error looking up topic {}: {:?}", topic_name, e);
                        let mut error_partitions = Vec::new();
                        for partition in topic.partitions {
                            let mut partition_response = OffsetCommitResponsePartition::default();
                            partition_response.partition_index = partition.partition_index;
                            partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            error_partitions.push(partition_response);
                        }

                        let mut topic_response = OffsetCommitResponseTopic::default();
                        topic_response.name = TopicName(StrBytes::from_string(topic_name));
                        topic_response.partitions = error_partitions;
                        response_topics.push(topic_response);
                        continue;
                    }
                };

                let mut partition_responses = Vec::new();

                // Process each partition offset commit
                for partition in topic.partitions {
                    let partition_id = partition.partition_index;
                    let committed_offset = partition.committed_offset;
                    let metadata = partition.metadata.unwrap_or_default();

                    // UPSERT committed offset
                    // TODO: Use prepared statements when pgrx supports them
                    let group_id_escaped = group_id.replace("'", "''");
                    let metadata_escaped = metadata.replace("'", "''");
                    let upsert_result = BackgroundWorker::transaction(|| {
                        let query = if metadata.is_empty() {
                            format!(
                                "INSERT INTO kafka.consumer_offsets
                                    (group_id, topic_id, partition_id, committed_offset, metadata)
                                VALUES ('{}', {}, {}, {}, NULL)
                                ON CONFLICT (group_id, topic_id, partition_id)
                                DO UPDATE SET
                                    committed_offset = EXCLUDED.committed_offset,
                                    metadata = EXCLUDED.metadata,
                                    commit_timestamp = NOW()",
                                group_id_escaped, topic_id, partition_id, committed_offset
                            )
                        } else {
                            format!(
                                "INSERT INTO kafka.consumer_offsets
                                    (group_id, topic_id, partition_id, committed_offset, metadata)
                                VALUES ('{}', {}, {}, {}, '{}')
                                ON CONFLICT (group_id, topic_id, partition_id)
                                DO UPDATE SET
                                    committed_offset = EXCLUDED.committed_offset,
                                    metadata = EXCLUDED.metadata,
                                    commit_timestamp = NOW()",
                                group_id_escaped,
                                topic_id,
                                partition_id,
                                committed_offset,
                                metadata_escaped
                            )
                        };
                        pgrx::Spi::run(&query)
                    });

                    let error_code = match upsert_result {
                        Ok(_) => {
                            pg_log!(
                                "Committed offset {} for group={}, topic={}, partition={}",
                                committed_offset,
                                group_id,
                                topic_name,
                                partition_id
                            );
                            ERROR_NONE
                        }
                        Err(e) => {
                            pg_warning!(
                                "Failed to commit offset for partition {}: {:?}",
                                partition_id,
                                e
                            );
                            ERROR_UNKNOWN_SERVER_ERROR
                        }
                    };

                    let mut partition_response = OffsetCommitResponsePartition::default();
                    partition_response.partition_index = partition_id;
                    partition_response.error_code = error_code;
                    partition_responses.push(partition_response);
                }

                let mut topic_response = OffsetCommitResponseTopic::default();
                topic_response.name = TopicName(StrBytes::from_string(topic_name));
                topic_response.partitions = partition_responses;
                response_topics.push(topic_response);
            }

            // Build response
            let mut kafka_response = OffsetCommitResponse::default();
            kafka_response.throttle_time_ms = 0;
            kafka_response.topics = response_topics;

            let response = KafkaResponse::OffsetCommit {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send OffsetCommit response: {}", e);
            } else {
                pg_log!("OffsetCommit response sent successfully");
            }
        }
        crate::kafka::KafkaRequest::OffsetFetch {
            correlation_id,
            client_id: _,
            api_version,
            group_id,
            topics,
            response_tx,
        } => {
            pg_log!(
                "Processing OffsetFetch request, correlation_id: {}, group_id: {}",
                correlation_id,
                group_id
            );

            use kafka_protocol::messages::offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
            };
            use kafka_protocol::messages::TopicName;
            use kafka_protocol::protocol::StrBytes;

            let mut response_topics = Vec::new();

            if let Some(topic_list) = topics {
                // Fetch specific topics and partitions
                for topic_request in topic_list {
                    let topic_name = topic_request.name.clone();

                    // Look up topic_id
                    // TODO: Use prepared statements when pgrx supports them
                    let topic_name_escaped = topic_name.replace("'", "''");
                    let topic_id_result = BackgroundWorker::transaction(|| {
                        pgrx::Spi::get_one::<i32>(&format!(
                            "SELECT id FROM kafka.topics WHERE name = '{}'",
                            topic_name_escaped
                        ))
                    });

                    let topic_id = match topic_id_result {
                        Ok(Some(id)) => id,
                        Ok(None) => {
                            pg_warning!("Topic not found for OffsetFetch: {}", topic_name);
                            // Return error for all requested partitions
                            let mut error_partitions = Vec::new();
                            for partition_index in topic_request.partition_indexes {
                                let mut partition_response =
                                    OffsetFetchResponsePartition::default();
                                partition_response.partition_index = partition_index;
                                partition_response.error_code = ERROR_UNKNOWN_TOPIC_OR_PARTITION;
                                partition_response.committed_offset = -1;
                                error_partitions.push(partition_response);
                            }

                            let mut topic_response = OffsetFetchResponseTopic::default();
                            topic_response.name = TopicName(StrBytes::from_string(topic_name));
                            topic_response.partitions = error_partitions;
                            response_topics.push(topic_response);
                            continue;
                        }
                        Err(e) => {
                            pg_warning!("Error looking up topic {}: {:?}", topic_name, e);
                            let mut error_partitions = Vec::new();
                            for partition_index in topic_request.partition_indexes {
                                let mut partition_response =
                                    OffsetFetchResponsePartition::default();
                                partition_response.partition_index = partition_index;
                                partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                                partition_response.committed_offset = -1;
                                error_partitions.push(partition_response);
                            }

                            let mut topic_response = OffsetFetchResponseTopic::default();
                            topic_response.name = TopicName(StrBytes::from_string(topic_name));
                            topic_response.partitions = error_partitions;
                            response_topics.push(topic_response);
                            continue;
                        }
                    };

                    let mut partition_responses = Vec::new();

                    // Fetch committed offset for each requested partition
                    for partition_index in topic_request.partition_indexes {
                        let group_id_escaped = group_id.replace("'", "''");
                        let offset_result = BackgroundWorker::transaction(|| {
                            let query = format!(
                                "SELECT committed_offset, metadata
                                 FROM kafka.consumer_offsets
                                 WHERE group_id = '{}' AND topic_id = {} AND partition_id = {}",
                                group_id_escaped, topic_id, partition_index
                            );

                            pgrx::Spi::connect(|client| {
                                let mut tup_table = client.select(&query, Some(1), &[])?;

                                let result = if let Some(row) = tup_table.next() {
                                    let committed_offset: i64 = row[1].value()?.unwrap_or(-1);
                                    let metadata: Option<String> = row[2].value()?;
                                    Some((committed_offset, metadata))
                                } else {
                                    None
                                };
                                Ok::<Option<(i64, Option<String>)>, Box<dyn std::error::Error>>(
                                    result,
                                )
                            })
                        });

                        let mut partition_response = OffsetFetchResponsePartition::default();
                        partition_response.partition_index = partition_index;

                        match offset_result {
                            Ok(Some((committed_offset, metadata))) => {
                                partition_response.committed_offset = committed_offset;
                                partition_response.metadata = metadata.map(StrBytes::from_string);
                                partition_response.error_code = ERROR_NONE;
                            }
                            Ok(None) => {
                                // No committed offset yet, return -1
                                partition_response.committed_offset = -1;
                                partition_response.error_code = ERROR_NONE;
                            }
                            Err(e) => {
                                pg_warning!("Failed to fetch offset: {:?}", e);
                                partition_response.committed_offset = -1;
                                partition_response.error_code = ERROR_UNKNOWN_SERVER_ERROR;
                            }
                        }

                        partition_responses.push(partition_response);
                    }

                    let mut topic_response = OffsetFetchResponseTopic::default();
                    topic_response.name = TopicName(StrBytes::from_string(topic_name));
                    topic_response.partitions = partition_responses;
                    response_topics.push(topic_response);
                }
            } else {
                // Fetch all topics for this consumer group
                let group_id_escaped = group_id.replace("'", "''");
                let all_offsets_result = BackgroundWorker::transaction(|| {
                    let query = format!(
                        "SELECT t.name, co.partition_id, co.committed_offset, co.metadata
                         FROM kafka.consumer_offsets co
                         JOIN kafka.topics t ON co.topic_id = t.id
                         WHERE co.group_id = '{}'
                         ORDER BY t.name, co.partition_id",
                        group_id_escaped
                    );

                    pgrx::Spi::connect(|client| {
                        let tup_table = client.select(&query, None, &[])?;

                        let mut results: Vec<(String, i32, i64, Option<String>)> = Vec::new();
                        for row in tup_table {
                            let topic_name: String = row[1].value()?.unwrap_or_default();
                            let partition_id: i32 = row[2].value()?.unwrap_or(0);
                            let committed_offset: i64 = row[3].value()?.unwrap_or(-1);
                            let metadata: Option<String> = row[4].value()?;
                            results.push((topic_name, partition_id, committed_offset, metadata));
                        }
                        Ok::<Vec<(String, i32, i64, Option<String>)>, Box<dyn std::error::Error>>(
                            results,
                        )
                    })
                });

                match all_offsets_result {
                    Ok(offsets) => {
                        // Group by topic
                        let mut topics_map: std::collections::HashMap<
                            String,
                            Vec<OffsetFetchResponsePartition>,
                        > = std::collections::HashMap::new();

                        for (topic_name, partition_id, committed_offset, metadata) in offsets {
                            let mut partition_response = OffsetFetchResponsePartition::default();
                            partition_response.partition_index = partition_id;
                            partition_response.committed_offset = committed_offset;
                            partition_response.metadata = metadata.map(StrBytes::from_string);
                            partition_response.error_code = ERROR_NONE;

                            topics_map
                                .entry(topic_name)
                                .or_default()
                                .push(partition_response);
                        }

                        for (topic_name, partitions) in topics_map {
                            let mut topic_response = OffsetFetchResponseTopic::default();
                            topic_response.name = TopicName(StrBytes::from_string(topic_name));
                            topic_response.partitions = partitions;
                            response_topics.push(topic_response);
                        }
                    }
                    Err(e) => {
                        pg_warning!(
                            "Failed to fetch all offsets for group {}: {:?}",
                            group_id,
                            e
                        );
                    }
                }
            }

            // Build response
            let mut kafka_response = OffsetFetchResponse::default();
            kafka_response.throttle_time_ms = 0;
            kafka_response.error_code = ERROR_NONE;
            kafka_response.topics = response_topics;

            let response = KafkaResponse::OffsetFetch {
                correlation_id,
                api_version,
                response: kafka_response,
            };

            if let Err(e) = response_tx.send(response) {
                pg_warning!("Failed to send OffsetFetch response: {}", e);
            } else {
                pg_log!("OffsetFetch response sent successfully");
            }
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::kafka::messages::KafkaResponse;
//     use crate::testing::helpers::*;

//     #[pgrx::pg_test]
//     fn test_api_versions_handler_success() {
//         // Test that ApiVersions request returns proper response
//         let (request, mut response_rx) =
//             mock_api_versions_request_with_client(42, "test-client".to_string());

//         // Process the request
//         process_request(request);

//         // Verify we got a response
//         let response = response_rx
//             .try_recv()
//             .expect("Should receive ApiVersions response");

//         // Verify it's an ApiVersions response
//         match response {
//             KafkaResponse::ApiVersions {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 42);
//                 assert!(!response.api_keys.is_empty(), "Should have API versions");
//             }
//             _ => panic!("Expected ApiVersions response"),
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_api_versions_handler_correlation_id() {
//         // Test that correlation_id is preserved
//         for test_correlation_id in [0, 1, 42, 999, i32::MAX] {
//             let (request, mut response_rx) = mock_api_versions_request(test_correlation_id);

//             process_request(request);

//             let response = response_rx.try_recv().expect("Should receive response");

//             match response {
//                 KafkaResponse::ApiVersions {
//                     correlation_id,
//                     api_version: _,
//                     response: _,
//                 } => {
//                     assert_eq!(
//                         correlation_id, test_correlation_id,
//                         "Correlation ID should be preserved"
//                     );
//                 }
//                 _ => panic!("Expected ApiVersions response"),
//             }
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_api_versions_handler_supported_versions() {
//         // Test that response includes correct API versions
//         let (request, mut response_rx) = mock_api_versions_request(1);

//         process_request(request);

//         let response = response_rx.try_recv().expect("Should receive response");

//         match response {
//             KafkaResponse::ApiVersions {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 assert_eq!(response.api_keys.len(), 3, "Should support 3 APIs");

//                 let api_versions_api = response
//                     .api_keys
//                     .iter()
//                     .find(|av| av.api_key == 18)
//                     .expect("Should support ApiVersions API");
//                 assert_eq!(api_versions_api.min_version, 0);
//                 assert_eq!(api_versions_api.max_version, 3);

//                 let metadata_api = response
//                     .api_keys
//                     .iter()
//                     .find(|av| av.api_key == 3)
//                     .expect("Should support Metadata API");
//                 assert_eq!(metadata_api.min_version, 0);
//                 assert_eq!(metadata_api.max_version, 9);
//             }
//             _ => panic!("Expected ApiVersions response"),
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_metadata_handler_all_topics() {
//         let (request, mut response_rx) =
//             mock_metadata_request_with_client(99, "test-client".to_string(), None);

//         process_request(request);

//         let response = response_rx.try_recv().expect("Should receive response");

//         match response {
//             KafkaResponse::Metadata {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 99);
//                 assert!(!response.brokers.is_empty());
//                 assert!(!response.topics.is_empty());

//                 let test_topic = response
//                     .topics
//                     .iter()
//                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//                     .expect("Should have test-topic");
//                 assert_eq!(test_topic.error_code, ERROR_NONE);
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_metadata_handler_broker_info() {
//         let (request, mut response_rx) = mock_metadata_request(101, None);

//         process_request(request);

//         let response = response_rx.try_recv().expect("Should receive response");

//         match response {
//             KafkaResponse::Metadata {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 assert_eq!(response.brokers.len(), 1);
//                 let broker = &response.brokers[0];
//                 assert_eq!(broker.node_id.0, DEFAULT_BROKER_ID);
//                 assert_eq!(broker.port, DEFAULT_KAFKA_PORT);
//                 assert!(!broker.host.is_empty());
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_metadata_handler_partition_info() {
//         let (request, mut response_rx) = mock_metadata_request(102, None);

//         process_request(request);

//         let response = response_rx.try_recv().expect("Should receive response");

//         match response {
//             KafkaResponse::Metadata {
//                 api_version: _,
//                 correlation_id: _,
//                 response,
//             } => {
//                 let test_topic = response
//                     .topics
//                     .iter()
//                     .find(|t| t.name.as_ref().map(|n| n.0.as_str()) == Some("test-topic"))
//                     .expect("Should have test-topic");

//                 assert_eq!(test_topic.partitions.len(), 1);

//                 let partition = &test_topic.partitions[0];
//                 assert_eq!(partition.error_code, ERROR_NONE);
//                 assert_eq!(partition.partition_index, 0);
//                 assert_eq!(partition.leader_id.0, DEFAULT_BROKER_ID);
//                 assert_eq!(
//                     partition.replica_nodes,
//                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 );
//                 assert_eq!(
//                     partition.isr_nodes,
//                     vec![kafka_protocol::messages::BrokerId(DEFAULT_BROKER_ID)]
//                 );
//             }
//             _ => panic!("Expected Metadata response"),
//         }
//     }

//     #[pgrx::pg_test]
//     fn test_multiple_sequential_requests() {
//         // Test sequence: ApiVersions → Metadata
//         let (request1, mut response_rx1) = mock_api_versions_request(1);
//         process_request(request1);
//         let response1 = response_rx1.try_recv().expect("Should receive response 1");
//         assert!(matches!(response1, KafkaResponse::ApiVersions { .. }));

//         let (request2, mut response_rx2) = mock_metadata_request(2, None);
//         process_request(request2);
//         let response2 = response_rx2.try_recv().expect("Should receive response 2");
//         assert!(matches!(response2, KafkaResponse::Metadata { .. }));
//     }

//     #[pgrx::pg_test]
//     fn test_produce_creates_topic() {
//         // Test that Produce request auto-creates a new topic
//         let test_topic = "auto-created-topic";
//         let records = vec![simple_record(None, "test message")];
//         let (request, mut response_rx) = mock_produce_request(200, test_topic, 0, records);

//         process_request(request);

//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 200);
//                 assert_eq!(response.responses.len(), 1);

//                 let topic_response = &response.responses[0];
//                 assert_eq!(topic_response.name.to_string(), test_topic);
//                 assert_eq!(topic_response.partition_responses.len(), 1);

//                 let partition_response = &topic_response.partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(
//                     partition_response.base_offset, 0,
//                     "First message should have offset 0"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         // Verify topic was created in database
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT COUNT(*) as count FROM kafka.topics WHERE name = $1",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             let count: i64 = result.first().get_by_name("count").unwrap().unwrap();
//             assert_eq!(count, 1, "Topic should be created in database");
//         });
//     }

//     #[pgrx::pg_test]
//     fn test_produce_single_message() {
//         // Test that a single message is inserted with correct offset
//         let test_topic = "single-message-topic";
//         let test_value = "Hello, Kafka!";
//         let records = vec![simple_record(Some("my-key"), test_value)];
//         let (request, mut response_rx) = mock_produce_request(201, test_topic, 0, records);

//         process_request(request);

//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 201);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(partition_response.base_offset, 0);
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         // Verify message is in database
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT partition_offset, value FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1 AND m.partition_id = 0
//                  ORDER BY partition_offset",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             assert_eq!(result.len(), 1, "Should have exactly 1 message");
//             let row = result.first();
//             let offset: i64 = row.get_by_name("partition_offset").unwrap().unwrap();
//             let value: Vec<u8> = row.get_by_name("value").unwrap().unwrap();

//             assert_eq!(offset, 0);
//             assert_eq!(value, test_value.as_bytes());
//         });
//     }

//     #[pgrx::pg_test]
//     fn test_produce_batch_messages() {
//         // Test that batch of messages gets consecutive offsets
//         let test_topic = "batch-topic";
//         let mut records = Vec::new();
//         for i in 0..10 {
//             records.push(simple_record(None, &format!("message-{}", i)));
//         }

//         let (request, mut response_rx) = mock_produce_request(202, test_topic, 0, records);

//         process_request(request);

//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 202);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//                 assert_eq!(
//                     partition_response.base_offset, 0,
//                     "Batch should start at offset 0"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         // Verify all 10 messages have consecutive offsets
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT COUNT(*) as count,
//                         MIN(partition_offset) as min_offset,
//                         MAX(partition_offset) as max_offset
//                  FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1 AND m.partition_id = 0",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             let row = result.first();
//             let count: i64 = row.get_by_name("count").unwrap().unwrap();
//             let min_offset: i64 = row.get_by_name("min_offset").unwrap().unwrap();
//             let max_offset: i64 = row.get_by_name("max_offset").unwrap().unwrap();

//             assert_eq!(count, 10, "Should have exactly 10 messages");
//             assert_eq!(min_offset, 0, "First offset should be 0");
//             assert_eq!(
//                 max_offset, 9,
//                 "Last offset should be 9 (consecutive from 0)"
//             );
//         });
//     }

//     #[pgrx::pg_test]
//     fn test_produce_with_headers() {
//         // Test that headers are correctly serialized to JSONB
//         let test_topic = "headers-topic";
//         let headers = vec![
//             ("correlation-id", b"12345".as_ref()),
//             ("source", b"test-client".as_ref()),
//         ];
//         let records = vec![record_with_headers(None, "message with headers", headers)];
//         let (request, mut response_rx) = mock_produce_request(203, test_topic, 0, records);

//         process_request(request);

//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 203);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(partition_response.error_code, ERROR_NONE);
//             }
//             _ => panic!("Expected Produce response"),
//         }

//         // Verify headers are in JSONB format
//         Spi::connect(|client| {
//             let result = client
//                 .select(
//                     "SELECT headers FROM kafka.messages m
//                  JOIN kafka.topics t ON m.topic_id = t.id
//                  WHERE t.name = $1",
//                     None,
//                     &[test_topic.into()],
//                 )
//                 .expect("Query should succeed");

//             assert_eq!(result.len(), 1);
//             let row = result.first();
//             let headers_json: pgrx::JsonB = row.get_by_name("headers").unwrap().unwrap();

//             // Headers should be a JSON array with 2 elements
//             let headers_value = headers_json.0;
//             assert!(headers_value.is_array(), "Headers should be JSONB array");
//             let headers_array = headers_value.as_array().unwrap();
//             assert_eq!(headers_array.len(), 2, "Should have 2 headers");
//         });
//     }

//     #[pgrx::pg_test]
//     fn test_produce_invalid_partition() {
//         // Test that partition > 0 returns an error (we only support single-partition topics)
//         let test_topic = "invalid-partition-topic";
//         let records = vec![simple_record(None, "test message")];
//         let (request, mut response_rx) = mock_produce_request(204, test_topic, 1, records); // partition 1 (invalid)

//         process_request(request);

//         let response = response_rx
//             .try_recv()
//             .expect("Should receive Produce response");

//         match response {
//             KafkaResponse::Produce {
//                 correlation_id,
//                 api_version: _,
//                 response,
//             } => {
//                 assert_eq!(correlation_id, 204);
//                 let partition_response = &response.responses[0].partition_responses[0];
//                 assert_eq!(
//                     partition_response.error_code, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
//                     "Should return error for invalid partition"
//                 );
//             }
//             _ => panic!("Expected Produce response"),
//         }
//     }
// }
