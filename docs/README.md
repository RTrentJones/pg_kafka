# pg_kafka Documentation

**Status:** Phase 11 Complete (Shadow Mode)
**Last Updated:** 2026-10-29

## Quick Reference

| Document | Purpose |
|----------|---------|
| [KAFKA_PROTOCOL_COVERAGE.md](KAFKA_PROTOCOL_COVERAGE.md) | API coverage analysis (26 of ~50 APIs) |
| [PROTOCOL_DEVIATIONS.md](PROTOCOL_DEVIATIONS.md) | Intentional spec differences from Apache Kafka |
| [TEST_STRATEGY.md](TEST_STRATEGY.md) | Test architecture (~925 tests: unit + property + E2E) |
| [PGRX_TESTING_GUIDE.md](PGRX_TESTING_GUIDE.md) | Testing a pgrx extension that uses `PGC_POSTMASTER` GUCs and a background worker |
| [REPOSITORY_PATTERN.md](REPOSITORY_PATTERN.md) | Storage abstraction design (KafkaStore trait) |
| [PERFORMANCE.md](PERFORMANCE.md) | Tuning, benchmarks, compression guide |
| [SHADOW_MODE_TESTING.md](SHADOW_MODE_TESTING.md) | Shadow mode setup and testing |
| [SHADOW_MODE_FLOW_DIAGRAM.md](SHADOW_MODE_FLOW_DIAGRAM.md) | Shadow mode architecture |
| [SHADOW_EVIDENCE_PLAN.md](SHADOW_EVIDENCE_PLAN.md) | Plan for publishing shadow-forwarding evidence (`evidence/shadow/`) |
| [LICENSING.md](LICENSING.md) | Dual-licensing information |
| [FULL_KAFKA_COMPLIANCE_PLAN.md](FULL_KAFKA_COMPLIANCE_PLAN.md) | Roadmap (future phases) |

## Architecture Decisions

| Document | Status |
|----------|--------|
| [ADR-001: Partitioning and Retention](architecture/ADR-001-partitioning-and-retention.md) | Proposed. Table partitioning is not implemented; time-based retention ships as a bounded `DELETE` sweep instead. |
| [ADR-002: Staff-Level Architectural Improvements](architecture/ADR-002-staff-level-architectural-improvements.md) | Accepted. The byte-weighted backpressure item was never wired in and was removed; see the ADR's status note. |

## Reviews and Audits (dated records)

| Document | Purpose |
|----------|---------|
| [AUDIT-2026-06.md](AUDIT-2026-06.md) | June 2026 line-by-line code audit and its remediation tracking |
| [DEEP-REVIEW-2026-07.md](DEEP-REVIEW-2026-07.md) | July 2026 follow-up review (operational lifecycle, schema, CI, docs); source of the `DR-*` references |

These are point-in-time records. Their counts and line numbers describe the tree they reviewed, not the current code.

## Document Purposes

### For Understanding the Implementation

- **KAFKA_PROTOCOL_COVERAGE.md** - What Kafka APIs are implemented and how they map to pg_kafka
- **PROTOCOL_DEVIATIONS.md** - Where pg_kafka intentionally differs from Apache Kafka

### For Development

- **TEST_STRATEGY.md** - How to run tests, test distribution, coverage
- **PGRX_TESTING_GUIDE.md** - Why `cargo pgrx test` is not used and how the unit/E2E split works
- **REPOSITORY_PATTERN.md** - Storage layer architecture and testing approach
- **[../hooks/README.md](../hooks/README.md)** - Git hooks (pre-commit `cargo fmt`, pre-push `cargo clippy`)
- **[../.github/SETUP.md](../.github/SETUP.md)** - GitHub Actions, branch protection, and Codecov setup

### For Operations

- **PERFORMANCE.md** - PostgreSQL tuning, retention GUCs, compression settings, monitoring queries
- **SHADOW_MODE_TESTING.md** - Shadow mode configuration and external Kafka setup

### For Planning

- **FULL_KAFKA_COMPLIANCE_PLAN.md** - Future phases and implementation roadmap
- **SHADOW_EVIDENCE_PLAN.md** - Shadow-mode evidence and reporting plan

## Key Metrics

| Metric | Value |
|--------|-------|
| APIs Implemented | 26 of ~50 (52%) |
| Unit Tests (`src/`) | 713 (includes 4 in-crate proptest fuzz harnesses) |
| Property Tests (`tests/property_tests.rs`) | 10 |
| E2E Tests (`kafka_test/`) | 202 |
| Compression Codecs | gzip, snappy, lz4, zstd |
| Assignment Strategies | Range, RoundRobin, Sticky |
| Transaction Support | Full EOS |
| Shadow Mode | Complete |

Counts are refreshed manually and drift as tests land. The CI run is the source of truth.
