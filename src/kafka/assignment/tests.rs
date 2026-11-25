//! Integration tests for the assignment module
//!
//! Unit tests are in each submodule. This file contains cross-module integration tests.

#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;

    use crate::kafka::assignment::{
        create_strategy, AssignmentInput, AssignmentStrategy, MemberAssignment, MemberSubscription,
    };

    /// Helper to create assignment input
    fn make_input(members: Vec<(&str, Vec<&str>)>, topics: Vec<(&str, i32)>) -> AssignmentInput {
        let subscriptions: HashMap<String, MemberSubscription> = members
            .into_iter()
            .map(|(id, topic_list)| {
                (
                    id.to_string(),
                    MemberSubscription::new(topic_list.into_iter().map(String::from).collect()),
                )
            })
            .collect();

        let topic_partitions: HashMap<String, i32> = topics
            .into_iter()
            .map(|(name, count)| (name.to_string(), count))
            .collect();

        AssignmentInput::new(subscriptions, topic_partitions)
    }

    #[test]
    fn test_subscription_roundtrip_in_assignment() {
        // Test that subscription can be encoded, decoded, and used in assignment
        let original = MemberSubscription::new(vec!["topic-a".to_string(), "topic-b".to_string()]);
        let encoded = original.encode().unwrap();
        let decoded = MemberSubscription::parse(&encoded).unwrap();

        assert_eq!(original.topics, decoded.topics);
    }

    #[test]
    fn test_assignment_roundtrip() {
        // Test that assignment can be encoded, decoded
        let mut partitions = HashMap::new();
        partitions.insert("topic-a".to_string(), vec![0, 1, 2]);
        partitions.insert("topic-b".to_string(), vec![0]);

        let original = MemberAssignment::with_partitions(partitions);
        let encoded = original.encode().unwrap();
        let decoded = MemberAssignment::parse(&encoded).unwrap();

        assert_eq!(original.partition_count(), decoded.partition_count());
    }

    #[test]
    fn test_all_strategies_produce_valid_assignments() {
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a", "topic-b"]),
                ("member-2", vec!["topic-a", "topic-b"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 6), ("topic-b", 4)],
        );

        for strategy_name in &["range", "roundrobin", "sticky"] {
            let strategy = create_strategy(strategy_name).unwrap();
            let result = strategy.assign(&input);

            // All members have assignments
            assert_eq!(result.len(), 3, "Strategy {strategy_name} failed");

            // Count total partitions assigned
            let total_a: usize = result.values().map(|a| a.partitions("topic-a").len()).sum();
            let total_b: usize = result.values().map(|a| a.partitions("topic-b").len()).sum();

            assert_eq!(
                total_a, 6,
                "Strategy {strategy_name} didn't assign all topic-a partitions"
            );
            assert_eq!(
                total_b, 4,
                "Strategy {strategy_name} didn't assign all topic-b partitions"
            );

            // Verify member-3 only gets topic-a (not subscribed to topic-b)
            assert!(
                result["member-3"].partitions("topic-b").is_empty(),
                "Strategy {strategy_name} assigned topic-b to non-subscriber"
            );
        }
    }

    #[test]
    fn test_no_partition_overlap() {
        // Verify no two members get the same partition
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 10)],
        );

        for strategy_name in &["range", "roundrobin", "sticky"] {
            let strategy = create_strategy(strategy_name).unwrap();
            let result = strategy.assign(&input);

            let mut all_partitions: Vec<i32> = result
                .values()
                .flat_map(|a| a.partitions("topic-a"))
                .collect();

            let original_len = all_partitions.len();
            all_partitions.sort_unstable();
            all_partitions.dedup();

            assert_eq!(
                all_partitions.len(),
                original_len,
                "Strategy {strategy_name} produced overlapping assignments"
            );
        }
    }

    #[test]
    fn test_all_partitions_assigned() {
        // Verify all partitions are assigned (no gaps)
        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 7)],
        );

        for strategy_name in &["range", "roundrobin", "sticky"] {
            let strategy = create_strategy(strategy_name).unwrap();
            let result = strategy.assign(&input);

            let mut all_partitions: Vec<i32> = result
                .values()
                .flat_map(|a| a.partitions("topic-a"))
                .collect();
            all_partitions.sort_unstable();

            let expected: Vec<i32> = (0..7).collect();
            assert_eq!(
                all_partitions, expected,
                "Strategy {strategy_name} didn't assign all partitions"
            );
        }
    }

    // ----- scale: assignment is about linear in subscriptions plus partitions -----
    //
    // A leader's SyncGroup computes the assignment on the single DB thread. A member's
    // subscription comes from its JoinGroup metadata (up to 1 MiB, about half a million
    // names), so repeats and long lists must not multiply with the partition count. Each
    // input below took seconds (release) to minutes (debug) when every subscriber lookup
    // scanned the subscription list; the bound is generous for a debug build and far
    // below what the quadratic walk takes.

    const SCALE_BOUND: std::time::Duration = std::time::Duration::from_secs(5);

    /// One member subscribed to `padding` repeats of a name that is no topic, then `real`.
    fn padded_member(padding: usize, real: &[String]) -> HashMap<String, MemberSubscription> {
        let mut topics = vec!["pad".to_string(); padding];
        topics.extend(real.iter().cloned());
        HashMap::from([("m".to_string(), MemberSubscription::new(topics))])
    }

    fn timed<T>(what: &str, f: impl FnOnce() -> T) -> T {
        let started = std::time::Instant::now();
        let out = f();
        let took = started.elapsed();
        assert!(took < SCALE_BOUND, "{what} took {took:?}");
        out
    }

    #[test]
    fn test_roundrobin_padded_subscription_is_linear() {
        // 131 000 repeats of a padding name and one topic of 10 000 partitions (about
        // 1 MiB of metadata): 10 000 x 131 001 comparisons when each partition rescanned
        // the subscription.
        let input = AssignmentInput::new(
            padded_member(131_000, &["t".to_string()]),
            HashMap::from([("t".to_string(), 10_000)]),
        );
        let out = timed("roundrobin", || {
            create_strategy("roundrobin").unwrap().assign(&input)
        });
        assert_eq!(out["m"].partitions("t").len(), 10_000);
    }

    #[test]
    fn test_range_many_topics_is_linear() {
        // 100 000 one-partition topics (one Metadata auto-creates them) and one member
        // subscribed to all of them: each topic's subscriber lookup scanned the 100 000
        // names.
        let names: Vec<String> = (0..100_000).map(|i| format!("t{i}")).collect();
        let input = AssignmentInput::new(
            padded_member(0, &names),
            names.iter().map(|n| (n.clone(), 1)).collect(),
        );
        let out = timed("range", || create_strategy("range").unwrap().assign(&input));
        assert_eq!(out["m"].partition_count(), 100_000);
    }

    #[test]
    fn test_sticky_previous_assignment_is_linear() {
        // The leader-supplied previous assignment names 10 000 topics; the member's
        // subscription repeats a padding name 131 000 times: each previous topic's
        // subscription check scanned the 131 000 names.
        let names: Vec<String> = (0..10_000).map(|i| format!("t{i}")).collect();
        let input = AssignmentInput::new(
            padded_member(131_000, &names),
            names.iter().map(|n| (n.clone(), 1)).collect(),
        );
        let previous = HashMap::from([(
            "m".to_string(),
            MemberAssignment::with_partitions(names.iter().map(|n| (n.clone(), vec![0])).collect()),
        )]);
        let out = timed("sticky", || {
            crate::kafka::assignment::strategies::StickyStrategy::with_previous(previous)
                .assign(&input)
        });
        assert_eq!(out["m"].partition_count(), 10_000);
    }

    #[test]
    fn test_repeated_subscriptions_assign_as_one() {
        // A topic listed many times is subscribed to once, whatever the assignor.
        for strategy in ["range", "roundrobin", "sticky"] {
            let input = make_input(
                vec![
                    ("m1", vec!["a", "a", "b", "a"]),
                    ("m2", vec!["b", "b", "a"]),
                ],
                vec![("a", 4), ("b", 2)],
            );
            let out = create_strategy(strategy).unwrap().assign(&input);
            assert_eq!(
                out.values()
                    .map(MemberAssignment::partition_count)
                    .sum::<usize>(),
                6,
                "{strategy}"
            );
            assert_eq!(out["m1"].partition_count(), 3, "{strategy}");
            assert_eq!(out["m2"].partition_count(), 3, "{strategy}");
        }
    }
}
