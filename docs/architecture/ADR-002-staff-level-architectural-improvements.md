# ADR-002: Staff-Level Architectural Improvements

## Status
Accepted

## Context

A staff-level architectural review identified several production risks in pg_kafka:

1. **Memory Unboundedness (P0)**: The request channel capacity of 10,000 items doesn't account for message size. A large message (e.g., 1MB) counts the same as a small message (100 bytes).

2. **Shadow Mode Durability (P0)**: Fire-and-forget async forwarding loses messages if the worker crashes between local write and external forward.

3. **Fetch Heuristics (P1)**: The `max_bytes / 100` heuristic assumes ~100 bytes per message, which can cause excessive roundtrips for tiny messages or OOM for large messages.

4. **Operational Visibility (P2)**: Lack of unified metrics view for monitoring system health.

## Decision

### 1. Byte-Weighted Backpressure

**Approach**: Track pending bytes in the request channel using an atomic counter. When the byte limit is exceeded, apply TCP-level backpressure by pausing reads from the socket.

**Implementation**:
- Add `RequestSize` trait to calculate approximate byte size of each request
- Track `pending_bytes` using `AtomicUsize` shared between sender and receiver
- Configure `MAX_PENDING_BYTES` (default: 100MB) as upper bound
- When limit exceeded, the listener delays accepting new requests

**Trade-offs**:
- (+) Prevents OOM from large message batches
- (+) TCP backpressure naturally propagates to clients
- (-) Adds overhead to track byte sizes
- (-) Size estimation is approximate (actual memory usage may vary)

### 2. Shadow Mode Outbox Pattern

**Approach**: Use the existing `kafka.shadow_tracking` table as a persistent outbox. Messages are written to the tracking table on insert, and the forwarder drains from it on startup.

**Implementation**:
- During `insert_records()`, also insert into `kafka.shadow_tracking` with `external_offset = NULL`
- On forwarder startup, query `kafka.shadow_tracking WHERE external_offset IS NULL`
- Forward pending messages before switching to real-time forwarding
- Update `external_offset` and `forwarded_at` on successful forward

**Trade-offs**:
- (+) Crash-safe: pending messages survive worker restart
- (+) Leverages existing table structure
- (-) Additional write per message (mitigated by batch inserts)
- (-) Requires periodic cleanup of forwarded records

### 3. Dynamic Fetch Sizing

**Approach**: Replace the fixed `/100` heuristic with adaptive sizing based on historical message sizes and running byte totals.

**Implementation**:
- Use `pg_column_size()` in the query to track actual bytes
- Stop fetching when cumulative bytes exceed `max_bytes`
- Add a configurable `fetch_initial_estimate_bytes` GUC (default: 500)

**Trade-offs**:
- (+) Prevents both starvation and memory exhaustion
- (+) Self-adapts to actual message sizes
- (-) Slightly more complex query
- (-) `pg_column_size()` has small overhead

### 4. Unified Metrics View

**Approach**: Create `kafka.metrics` view aggregating key operational metrics.

**Implementation**:
```sql
CREATE VIEW kafka.metrics AS
SELECT
    'topics' as category,
    count(*) as value,
    'count' as unit
FROM kafka.topics
UNION ALL
SELECT 'messages', count(*), 'count' FROM kafka.messages
-- ... etc
```

**Trade-offs**:
- (+) Single query for operational health check
- (+) Can be extended for Prometheus/OTEL integration
- (-) View queries may be expensive under load

## Consequences

1. **Performance**: Byte tracking adds ~1% overhead to produce path
2. **Durability**: Shadow mode achieves exactly-once semantics for external forwarding
3. **Observability**: Operators have single pane of glass for system health
4. **Memory Safety**: OOM risk from large messages is eliminated

## References

- [Staff-Level Architectural Review](../STAFF_REVIEW.md)
- [Phase 11: Shadow Mode](../PHASE_11_SHADOW_MODE.md)
- [Performance Guide](../PERFORMANCE.md)
