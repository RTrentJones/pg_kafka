// Copyright (c) 2026 Robert Trent Jones.
// This file is part of the "Shadow Mode" feature of pg_kafka.
//
// Use of this source code for production purposes is governed by the
// Commercial License found in the LICENSE file in this directory.
// Development and evaluation use is permitted.
//
// GitHub Sponsors: https://github.com/sponsors/RTrentJones

//! The shadow forwarder task: runs on the listener's tokio runtime (network
//! thread), receives `ForwardRequest`s from the DB thread and produces them to
//! the external Kafka cluster, answering each with a `ForwardAck`.
//!
//! Like the rest of the network thread it MUST NOT call pgrx (no SPI, no
//! pg_log!); it logs through `tracing`.

use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::{ForwardAck, ForwardRequest, ShadowConfig, ShadowProducer};

/// Shadow forwarder task for async forwarding
///
/// Receives ForwardRequest messages from the DB thread via crossbeam channel
/// and forwards them to external Kafka asynchronously (fire-and-forget).
///
/// This task uses RuntimeContext to check configuration dynamically, allowing
/// shadow mode to be enabled/disabled at runtime via ALTER SYSTEM + pg_reload_conf().
/// The producer is lazily created when shadow mode is first enabled and recreated
/// if the bootstrap servers change.
pub(crate) async fn run_shadow_forwarder(
    forward_rx: Receiver<ForwardRequest>,
    ack_tx: Sender<ForwardAck>,
    runtime_context: Arc<crate::kafka::RuntimeContext>,
) {
    info!("Shadow forwarder task started (dynamic config mode)");

    // Track current producer and config for change detection
    let mut producer: Option<Arc<ShadowProducer>> = None;
    let mut current_bootstrap_servers: String = String::new();

    loop {
        // Use spawn_blocking to receive from crossbeam channel in async context
        let rx = forward_rx.clone();
        let result =
            tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_millis(100))).await;

        match result {
            Ok(Ok(req)) => {
                // Check current config from RuntimeContext (dynamic reload support)
                let config = runtime_context.config();

                // Skip if shadow mode is not enabled or not configured
                if !config.shadow_mode_enabled || config.shadow_bootstrap_servers.is_empty() {
                    debug!(
                        "Shadow mode disabled or not configured, dropping forward request for {}[{}]",
                        req.topic_name, req.partition_id
                    );
                    continue;
                }

                // Check if config changed - recreate producer if bootstrap servers changed
                if config.shadow_bootstrap_servers != current_bootstrap_servers {
                    info!(
                        "Shadow config changed: bootstrap_servers '{}' -> '{}'",
                        current_bootstrap_servers, config.shadow_bootstrap_servers
                    );

                    let shadow_config = Arc::new(ShadowConfig::from_config(&config));
                    match ShadowProducer::new(shadow_config) {
                        Ok(p) => {
                            current_bootstrap_servers = config.shadow_bootstrap_servers.clone();
                            producer = Some(Arc::new(p));
                            info!(
                                "Shadow producer created/recreated for bootstrap_servers='{}'",
                                current_bootstrap_servers
                            );
                        }
                        Err(e) => {
                            warn!(
                                "Failed to create shadow producer for '{}': {:?}",
                                config.shadow_bootstrap_servers, e
                            );
                            // Clear producer so we retry on next config check
                            producer = None;
                            current_bootstrap_servers.clear();
                            continue;
                        }
                    }
                }

                // Get producer (should exist if we passed the config check)
                let p = match &producer {
                    Some(p) => p.clone(),
                    None => {
                        warn!("No producer available, dropping forward request");
                        continue;
                    }
                };

                // SH-15: forward with partition=None so the external cluster
                // re-partitions by key. The local partition count may differ
                // from the external topic's, so forcing the local partition id
                // would misroute.
                //
                // SH-9: this is the durable-outbox forward. Every attempt sends
                // a ForwardAck back to the DB thread, which finalizes the
                // kafka.shadow_tracking row (success → external_offset set;
                // failure → row stays pending and is retried). The send is no
                // longer fire-and-forget.
                let result = p
                    .send_async_offset(
                        &req.topic_name,
                        None,
                        req.key.as_deref(),
                        req.value.as_deref(),
                    )
                    .await;

                let ack_result = match result {
                    Ok(external_offset) => {
                        debug!(
                            "Forward OK {}[{}] local_offset {} -> external_offset {}",
                            req.topic_name, req.partition_id, req.local_offset, external_offset
                        );
                        Ok(external_offset)
                    }
                    Err(e) => {
                        warn!(
                            "Forward FAILED {}[{}] local_offset {}: {:?} (row stays pending)",
                            req.topic_name, req.partition_id, req.local_offset, e
                        );
                        Err(e.to_string())
                    }
                };

                let ack = ForwardAck {
                    topic_id: req.topic_id,
                    partition_id: req.partition_id,
                    local_offset: req.local_offset,
                    result: ack_result,
                };
                // Issue #93 test hook: hold the ack so it arrives later than
                // the outbox retry lease, letting the E2E suite prove the
                // in-flight gate prevents duplicate re-dispatch. 0 in
                // production (async sleep; only this forwarder task waits).
                let ack_delay_ms = config.test_forward_ack_delay_ms;
                if ack_delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(ack_delay_ms as u64)).await;
                }
                // RA-6: non-blocking send. A blocking `send` here could park this
                // tokio worker if the ack channel were full (the SEC-4
                // anti-pattern). On full/closed, drop the ack — the row stays
                // pending in the outbox and the poll re-forwards it, so the
                // external topic is at-least-once. The re-forward is a duplicate;
                // the idempotent producer suppresses it only if it happens within
                // the same producer session (stable PID) — a duplicate that spans
                // a producer restart still lands on the external topic.
                if ack_tx.try_send(ack).is_err() {
                    debug!("Forward ack channel full or closed, dropping ack (row stays pending)");
                }
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Timeout)) => {
                // Normal timeout, continue
            }
            Ok(Err(crossbeam_channel::RecvTimeoutError::Disconnected)) => {
                info!("Forward channel disconnected, stopping shadow forwarder");
                break;
            }
            Err(_) => {
                // spawn_blocking panicked
                error!("Shadow forwarder recv task panicked");
                break;
            }
        }
    }

    info!("Shadow forwarder task stopped");
}
