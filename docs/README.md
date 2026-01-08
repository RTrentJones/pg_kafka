# pg_kafka Documentation

**Status:** Phase 8 Complete (Compression Support)
**Last Updated:** 2026-01-08

## Quick Reference

| Document | Purpose |
|----------|---------|
| [KAFKA_PROTOCOL_COVERAGE.md](KAFKA_PROTOCOL_COVERAGE.md) | API coverage analysis (18 of ~50 APIs) |
| [PROTOCOL_DEVIATIONS.md](PROTOCOL_DEVIATIONS.md) | Intentional spec differences from Apache Kafka |
| [TEST_STRATEGY.md](TEST_STRATEGY.md) | Test architecture (184 unit + 74 E2E tests) |
| [REPOSITORY_PATTERN.md](REPOSITORY_PATTERN.md) | Storage abstraction design (KafkaStore trait) |
| [PERFORMANCE.md](PERFORMANCE.md) | Tuning, benchmarks, compression guide |
| [FULL_KAFKA_COMPLIANCE_PLAN.md](FULL_KAFKA_COMPLIANCE_PLAN.md) | Roadmap (Phases 9-12) |

## Architecture Decisions

| Document | Status |
|----------|--------|
| [ADR-001: Partitioning and Retention](architecture/ADR-001-partitioning-and-retention.md) | Proposed (Future Phase) |

## Document Purposes

### For Understanding the Implementation

- **KAFKA_PROTOCOL_COVERAGE.md** - What Kafka APIs are implemented and how they map to pg_kafka
- **PROTOCOL_DEVIATIONS.md** - Where pg_kafka intentionally differs from Apache Kafka

### For Development

- **TEST_STRATEGY.md** - How to run tests, test distribution, coverage
- **REPOSITORY_PATTERN.md** - Storage layer architecture and testing approach

### For Operations

- **PERFORMANCE.md** - PostgreSQL tuning, compression settings, monitoring queries

### For Planning

- **FULL_KAFKA_COMPLIANCE_PLAN.md** - Future phases and implementation roadmap

## Archive

Historical documents are preserved in [/.documents/archive/](/.documents/archive/):
- `PHASE_3B_COORDINATOR_DESIGN.md` - Original coordinator design (implemented)
- `TEST_COVERAGE_ANALYSIS.md` - Historical test analysis
- `E2E_TEST_COVERAGE_REVIEW.md` - Historical E2E review
- `DEVELOPMENT_HISTORY.md` - Development timeline

## Key Metrics

| Metric | Value |
|--------|-------|
| APIs Implemented | 18 of ~50 (36%) |
| Unit Tests | 184 |
| E2E Tests | 74 |
| Compression Codecs | gzip, snappy, lz4, zstd |
| Assignment Strategies | Range, RoundRobin, Sticky |
