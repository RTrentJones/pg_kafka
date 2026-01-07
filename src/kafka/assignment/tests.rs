//! Integration tests for the assignment module
//!
//! Unit tests are in each submodule. This file contains cross-module integration tests.

#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;

    use crate::kafka::assignment::{
        create_strategy, select_common_strategy, AssignmentInput, MemberAssignment,
        MemberSubscription,
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
        let encoded = original.encode();
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
        let encoded = original.encode();
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
            assert_eq!(result.len(), 3, "Strategy {} failed", strategy_name);

            // Count total partitions assigned
            let total_a: usize = result.values().map(|a| a.partitions("topic-a").len()).sum();
            let total_b: usize = result.values().map(|a| a.partitions("topic-b").len()).sum();

            assert_eq!(
                total_a, 6,
                "Strategy {} didn't assign all topic-a partitions",
                strategy_name
            );
            assert_eq!(
                total_b, 4,
                "Strategy {} didn't assign all topic-b partitions",
                strategy_name
            );

            // Verify member-3 only gets topic-a (not subscribed to topic-b)
            assert!(
                result["member-3"].partitions("topic-b").is_empty(),
                "Strategy {} assigned topic-b to non-subscriber",
                strategy_name
            );
        }
    }

    #[test]
    fn test_strategy_selection_integration() {
        // Simulate JoinGroup protocol negotiation
        let mut protocols = HashMap::new();
        protocols.insert(
            "member-1".to_string(),
            vec![
                (
                    "range".to_string(),
                    MemberSubscription::new(vec!["topic-a".to_string()]).encode(),
                ),
                (
                    "roundrobin".to_string(),
                    MemberSubscription::new(vec!["topic-a".to_string()]).encode(),
                ),
            ],
        );
        protocols.insert(
            "member-2".to_string(),
            vec![
                (
                    "roundrobin".to_string(),
                    MemberSubscription::new(vec!["topic-a".to_string()]).encode(),
                ),
                (
                    "range".to_string(),
                    MemberSubscription::new(vec!["topic-a".to_string()]).encode(),
                ),
            ],
        );

        let selected = select_common_strategy(&protocols).unwrap();

        // Both support roundrobin and range, roundrobin is preferred
        assert_eq!(selected, "roundrobin");

        // Create strategy and verify it works
        let strategy = create_strategy(&selected).unwrap();
        assert_eq!(strategy.name(), "roundrobin");
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
            all_partitions.sort();
            all_partitions.dedup();

            assert_eq!(
                all_partitions.len(),
                original_len,
                "Strategy {} produced overlapping assignments",
                strategy_name
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
            all_partitions.sort();

            let expected: Vec<i32> = (0..7).collect();
            assert_eq!(
                all_partitions, expected,
                "Strategy {} didn't assign all partitions",
                strategy_name
            );
        }
    }
}
