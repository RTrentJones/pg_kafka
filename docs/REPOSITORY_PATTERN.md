# Repository Pattern

pg_kafka keeps Kafka protocol logic and storage apart: handlers speak Kafka and call a storage
interface; one implementation of that interface holds all the SQL. Handlers are unit-tested
against a mock store, the SQL is in one place to audit, and the shadow-mode store is a decorator
over the PostgreSQL one.

## Layers

### 1. Storage ([src/kafka/storage/](../src/kafka/storage/))

The storage interface is split into role traits by concern, in
[`storage/mod.rs`](../src/kafka/storage/mod.rs). `KafkaStore` is their sum
(`pub trait KafkaStore: TopicStore + LogStore + OffsetStore + ProducerStore + TransactionStore +
MaintenanceStore {}`, implemented for every type that has all six), so handlers hold a
`&dyn KafkaStore` and helpers can take the one role they need
(`resolve_topic_id<S: TopicStore + ?Sized>`).

| Role | Concern |
|------|---------|
| `TopicStore` | Topic catalog: create/look up/delete topics, partition counts, per-topic `retention.ms`, name collisions |
| `LogStore` | Records: insert, fetch (with isolation level), high watermark, log start and last stable offsets, timestamp lookup, DeleteRecords |
| `OffsetStore` | Committed group offsets, and which groups have any |
| `ProducerStore` | Producer ids, epochs, sequences and the last-5-batches duplicate cache |
| `TransactionStore` | Transactional producers, transaction state, the partitions and pending offsets of an open transaction, commit/abort |
| `MaintenanceStore` | Transaction timeouts, aborted-row cleanup |

The trait definitions are the reference for the individual methods. A wrapper forwards a role it
does not change with one macro line, `delegate_topic_store!(TopicStore, inner)` (one
`delegate_*_store!` per role, defined next to the traits). Code calling store methods on a
concrete type imports `crate::kafka::storage::roles::*`.

Implementations:

- `PostgresStore` ([`storage/postgres.rs`](../src/kafka/storage/postgres.rs)): the production
  implementation over pgrx SPI. Its SQL is in inherent methods, one submodule of `postgres/` per
  concern (`topics`, `log`, `offsets`, `producers`, `transactions`, `maintenance`), named like the
  role methods; [`postgres/roles.rs`](../src/kafka/storage/postgres/roles.rs)
  implements each role by forwarding to them (`delegate_*_store!(PostgresStore)`). Maintenance
  that is not a role, such as `run_retention_sweep`, is inherent too; the worker and the
  `pg_kafka_run_retention_sweep()` SQL function call it. Pure planning logic (retention windows,
  partition drop/trim decisions) is in [`storage/retention.rs`](../src/kafka/storage/retention.rs)
  so it is unit-tested.
- `ShadowStore<S: KafkaStore>` ([`shadow/store.rs`](../src/kafka/shadow/store.rs)): a decorator over
  an inner store. It forwards `TopicStore`, `OffsetStore` and `ProducerStore` wholesale and
  implements `LogStore` (outbox rows on write, external-primary read suppression),
  `TransactionStore` (forwarding on commit) and `MaintenanceStore`. The worker uses
  `ShadowStore<PostgresStore>`.
- `MockKafkaStore` ([`testing/mocks.rs`](../src/testing/mocks.rs)): the mockall-generated test
  double (all six roles).

### 2. Handlers ([src/kafka/handlers/](../src/kafka/handlers/))

One `pub fn handle_*` per API, grouped by API family. `handle_api_versions` needs no state; every
other handler takes a `HandlerContext` ([`handler_context.rs`](../src/kafka/handler_context.rs))
whose `store: &dyn KafkaStore` is the injected storage backend, next to the group coordinator,
broker metadata, default partition count and outbound compression:

```rust
pub fn handle_metadata(ctx: &HandlerContext, requested_topics: Option<Vec<String>>, allow_auto_topic_creation: bool) -> Result<MetadataResponse>
```

Handlers turn Kafka requests into storage calls and build Kafka responses, reporting per-item
errors where the response has no top-level error code. They know nothing of SQL, the schema, SPI
or transactions.

### 3. API registry and worker

Each API's `KafkaApi` impl in [`src/kafka/api/`](../src/kafka/api/) decodes its request, calls its
handler through `Call::run` (which maps a handler `Err` or panic to the API's error response) and
answers the client. For each request the worker builds a `HandlerContext` over its `ShadowStore`
and hands the request to `crate::kafka::api::dispatch`. Transaction boundaries are the worker's: a
batch of requests runs in one transaction, each request in its own subtransaction, and responses
are released only after the commit. Storage assumes it runs inside a transaction; handlers never
see one.

## Testing

Handlers are unit-tested with `MockKafkaStore`, no database needed (`src/kafka/handlers/tests.rs`
and per-file test modules):

```rust
#[test]
fn test_handle_produce_success() {
    let mut mock = MockKafkaStore::new();
    mock.expect_get_or_create_topic()
        .returning(|_, _| Ok((1, 1))); // (topic_id, partition_count)
    mock.expect_insert_records()
        .returning(|_, _, _| Ok(0)); // base offset

    let coordinator = GroupCoordinator::new();
    let broker = BrokerMetadata::new("localhost".to_string(), 9092);
    let ctx = HandlerContext::new(&mock, &coordinator, &broker, 1, Compression::None);

    let response = produce::handle_produce(&ctx, topic_data, None, None).unwrap();
    assert_eq!(response.responses.len(), 1);
}
```

`PostgresStore` is not tested with `#[pg_test]`: pg_kafka registers `PGC_POSTMASTER` GUCs, which
`cargo pgrx test` cannot load (see [PGRX_TESTING_GUIDE.md](PGRX_TESTING_GUIDE.md)). It is excluded
from unit coverage in `codecov.yml`, covered by the E2E suite in `kafka_test/`, and the
`spi-e2e-guard` CI job requires a `kafka_test/src/` change alongside any change to it. Test counts:
[TEST_STRATEGY.md](TEST_STRATEGY.md#test-summary).

## Errors

`KafkaError` (`src/kafka/error.rs`) has a variant per failure, and `From<pgrx::spi::SpiError>`
turns SPI errors into `KafkaError::Database`, so the storage layer propagates with `?`.
`PostgresStore` methods typically wrap other failures as `KafkaError::Internal("<method> failed:
...")`; both variants map to UNKNOWN_SERVER_ERROR on the wire.

## SQL safety

All SQL is in the storage layer, and user-supplied values (topic names, group ids, transactional
ids, payloads) are bound as SPI arguments to parameterized queries, never spliced into SQL text:

```rust
let table = client.select(
    "SELECT state, producer_id, producer_epoch FROM kafka.transactions WHERE transactional_id = $1",
    Some(1),
    &[transactional_id.into()],
)?;
```
