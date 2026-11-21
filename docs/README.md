# pg_kafka Documentation

## Reference

| Document | Purpose |
|----------|---------|
| [CONFIGURATION.md](CONFIGURATION.md) | Every `pg_kafka.*` setting (generated from the GUC definitions) |
| [KAFKA_PROTOCOL_COVERAGE.md](KAFKA_PROTOCOL_COVERAGE.md) | Which Kafka APIs are implemented (<!--gen:api-coverage-->28 of ~50 standard Kafka APIs (56%)<!--/gen-->) and how they map to pg_kafka |
| [PROTOCOL_DEVIATIONS.md](PROTOCOL_DEVIATIONS.md) | Where pg_kafka differs from Apache Kafka, and why |
| [AUTHENTICATION.md](AUTHENTICATION.md) | TLS and SASL listeners, Kafka users, what is and is not protected |
| [UPGRADING.md](UPGRADING.md) | Extension versions, upgrade scripts, roles after 0.1.0 |
| [PERFORMANCE.md](PERFORMANCE.md) | PostgreSQL tuning, retention, compression, benchmarks, monitoring queries |
| [LICENSING.md](LICENSING.md) | Dual-licensing model |

## Design

| Document | Purpose |
|----------|---------|
| [REPOSITORY_PATTERN.md](REPOSITORY_PATTERN.md) | Storage abstraction (the `KafkaStore` role traits) |
| [SHADOW_MODE_FLOW_DIAGRAM.md](SHADOW_MODE_FLOW_DIAGRAM.md) | Shadow mode architecture: outbox, forwarder, retries, replay |
| [ADR-001: Partitioning and Retention](architecture/ADR-001-partitioning-and-retention.md) | Accepted, implemented in 0.1.0: `kafka.messages` is time-partitioned and retention drops whole leaves |
| [ADR-002: Staff-Level Architectural Improvements](architecture/ADR-002-staff-level-architectural-improvements.md) | Accepted; see its status table for what was built |
| [FULL_KAFKA_COMPLIANCE_PLAN.md](FULL_KAFKA_COMPLIANCE_PLAN.md) | Roadmap for the APIs not yet implemented |

## Development

| Document | Purpose |
|----------|---------|
| [TEST_STRATEGY.md](TEST_STRATEGY.md) | Test architecture and the one place test counts are kept |
| [PGRX_TESTING_GUIDE.md](PGRX_TESTING_GUIDE.md) | Why `cargo pgrx test` is not used and how the unit/E2E split works |
| [SHADOW_MODE_TESTING.md](SHADOW_MODE_TESTING.md) | Running the shadow E2E tests against an external Kafka |
| [../hooks/README.md](../hooks/README.md) | Git hooks (pre-commit `cargo fmt`, pre-push `cargo clippy`) |
| [../.github/SETUP.md](../.github/SETUP.md) | GitHub Actions, branch protection and Codecov setup |

## Archive (dated records)

| Document | Purpose |
|----------|---------|
| [archive/AUDIT-2026-06.md](archive/AUDIT-2026-06.md) | June 2026 line-by-line code audit and its remediation tracking |
| [archive/DEEP-REVIEW-2026-07.md](archive/DEEP-REVIEW-2026-07.md) | July 2026 follow-up review; source of the `DR-*` references |
| [archive/SHADOW-EVIDENCE-PLAN-2026-10.md](archive/SHADOW-EVIDENCE-PLAN-2026-10.md) | October 2026 plan that added shadow forwarding to the published evidence |

These are point-in-time records: their counts and line numbers describe the tree they reviewed,
not the current code.
