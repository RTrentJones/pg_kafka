# pg_kafka Licensing Model

## Overview

pg_kafka uses a **dual-licensing model**:

| Component | License | Production Use |
|-----------|---------|----------------|
| Core Extension | MIT | Free |
| Shadow Mode | Commercial / Source-Available | Requires License |

## Core Extension (Open Source)

Everything EXCEPT Shadow Mode is fully open source under the MIT License:

- Kafka wire protocol implementation
- Producer/Consumer APIs
- Consumer groups and coordination
- Transaction support
- Idempotent producers
- Admin APIs
- Compression support

**No restrictions on production use.**

## Shadow Mode (Commercial)

### What is Shadow Mode?

Shadow Mode enables forwarding messages to an external Apache Kafka cluster while storing them locally in PostgreSQL. This supports:

- Zero-downtime migration from pg_kafka to Kafka
- Dual-write patterns for validation
- Hybrid architectures (local + centralized Kafka)

### Location

All code in:
- `src/kafka/shadow/` directory (11 Rust files, ~6,200 lines, plus its `LICENSE`)
- the "Phase 11: Shadow Mode" section of `sql/bootstrap.sql` (the `kafka.shadow_config`,
  `kafka.shadow_tracking` and `kafka.shadow_metrics` tables, the `kafka.shadow_status` view, and the
  `kafka.replay_shadow_messages()` function). There is no separate shadow SQL file.

### Source Available

The code is **publicly readable** for:
- Security auditing
- Understanding behavior before purchasing
- Development and testing
- Non-commercial use

### License Required For

Production use in **commercial environments** requires:

1. **GitHub Sponsorship** at "Pro" tier ($50/month): https://github.com/sponsors/RTrentJones
2. **Enterprise License** for companies requiring invoicing/POs

### Evaluation

You can evaluate Shadow Mode with:

```sql
-- In postgresql.conf (requires restart):
pg_kafka.shadow_license_key = 'eval'
```

`'evaluation'` is accepted as a synonym. This enables full functionality with periodic warning logs.

### Enforcement

Shadow Mode uses a "soft lock" approach:
- **No DRM or crippling** - full functionality available; forwarding is never blocked
- **Warning logs** at most once per hour while records are being forwarded, if the key is empty
  (unlicensed), `eval`/`evaluation`, or malformed. A registered key logs nothing.
- **Format check only** - a registered key is any `sponsor_id:token` pair with both halves non-empty.
  No signature is verified (see `src/kafka/shadow/license.rs`).
- **Trust-based model** - we trust users to comply with licensing

Large companies have compliance departments that enforce license terms. We don't need technical enforcement; legal teams do it for us.

## Configuration

### Production License

```sql
-- In postgresql.conf (shadow_license_key requires a restart; it is superuser-only and hidden from SHOW ALL):
pg_kafka.shadow_license_key = 'your_sponsor_id:token'
pg_kafka.shadow_mode_enabled = true
pg_kafka.shadow_bootstrap_servers = 'kafka1:9092,kafka2:9092'
```

### Evaluation Mode

```sql
-- In postgresql.conf (requires restart):
pg_kafka.shadow_license_key = 'eval'
pg_kafka.shadow_mode_enabled = true
pg_kafka.shadow_bootstrap_servers = 'localhost:9093'
```

## FAQ

**Q: Can I use pg_kafka in production without Shadow Mode?**
A: Yes! The core extension is fully open source with no restrictions.

**Q: Can I read the Shadow Mode code?**
A: Yes! It's source-available. Read, audit, and understand it before purchasing.

**Q: What if I use Shadow Mode without a license?**
A: It will run but emit a warning log at most once per hour while forwarding. You're trusting our work; we're trusting you to pay if it provides value.

**Q: Do I need a license per database/server?**
A: Licensing is per **organization**, not per instance. One sponsorship covers your company's use.

**Q: Can I modify Shadow Mode code?**
A: For internal use with a valid license, yes. Redistribution of modified versions is not permitted.

**Q: How do I get a license key?**
A: Sponsor at the "Pro" tier, then access the sponsor dashboard for your unique key.

## Contact

- GitHub Sponsors: https://github.com/sponsors/RTrentJones
- License Questions: File an issue with the `licensing` label
