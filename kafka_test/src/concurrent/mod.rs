//! Concurrent E2E tests
//!
//! Tests for concurrent producer/consumer scenarios and race conditions.

mod batch_composition;
mod liveness_lane;
mod multi_consumer;
mod multi_producer;
mod pipelining;
mod producer_consumer;
mod race_conditions;

/// The `concurrent` tests, in run order.
pub fn tests() -> Vec<crate::registry::TestDef> {
    crate::test_defs!["concurrent";
        liveness_lane::test_heartbeat_bypasses_produce_backlog: sequential,
        multi_producer::test_concurrent_producers_same_topic: sequential,
        multi_producer::test_concurrent_producers_different_partitions: sequential,
        multi_consumer::test_multiple_consumer_groups: sequential,
        multi_consumer::test_consumer_group_two_members: sequential,
        multi_consumer::test_consumer_rejoin_after_leave: sequential,
        producer_consumer::test_produce_while_consuming: sequential,
        producer_consumer::test_consumer_catches_up: sequential,
        pipelining::test_request_pipelining, // Uses its own connection
        // Race condition tests
        race_conditions::test_produce_consume_race: sequential, // Tests concurrent access
        race_conditions::test_offset_commit_race: sequential, // Tests concurrent offset commits
        race_conditions::test_coordinator_state_race: sequential, // Tests coordinator state
        race_conditions::test_partition_assignment_race: sequential, // Tests partition assignment
        race_conditions::test_group_rejoin_race: sequential, // Tests group membership
        batch_composition::test_w3_heartbeats_do_not_split_batches: sequential,
    ]
}
