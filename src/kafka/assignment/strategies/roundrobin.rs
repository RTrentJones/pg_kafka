//! RoundRobin Assignment Strategy
//!
//! The RoundRobin strategy distributes partitions evenly across consumers
//! by cycling through them in round-robin order.
//!
//! # Algorithm
//!
//! Across ALL topics:
//! 1. Create sorted list of all (topic, partition) pairs
//! 2. Sort consumers by member_id (deterministic ordering)
//! 3. For each partition, assign to the next consumer that subscribes to its topic
//!
//! # Example
//!
//! With topics A (3 partitions), B (2 partitions), and 2 consumers both subscribed:
//! - All partitions: [A-0, A-1, A-2, B-0, B-1]
//! - Consumer 1: A-0, A-2, B-1
//! - Consumer 2: A-1, B-0
//!
//! # Characteristics
//!
//! - **Pros**: Better balance than Range when partition counts vary across topics
//! - **Cons**: Partitions scattered across topics (not co-located)

use std::collections::HashMap;

use super::{AssignmentInput, AssignmentOutput, AssignmentStrategy};
use crate::kafka::assignment::MemberAssignment;

/// RoundRobin partition assignment strategy
#[derive(Debug, Clone, Default)]
pub struct RoundRobinStrategy;

impl RoundRobinStrategy {
    /// Create a new RoundRobin strategy
    pub fn new() -> Self {
        Self
    }
}

impl AssignmentStrategy for RoundRobinStrategy {
    fn name(&self) -> &'static str {
        "roundrobin"
    }

    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput {
        let mut result: AssignmentOutput = HashMap::new();

        // Initialize empty assignments for all members
        for member_id in input.subscriptions.keys() {
            result.insert(member_id.clone(), MemberAssignment::new());
        }

        // Collect all (topic, partition) pairs from subscribed topics
        let mut all_partitions: Vec<(String, i32)> = Vec::new();
        for (topic, partition_count) in &input.topic_partitions {
            // Only include topics that have at least one subscriber
            let has_subscriber = input
                .subscriptions
                .values()
                .any(|sub| sub.topics.contains(topic));

            if has_subscriber && *partition_count > 0 {
                for partition in 0..*partition_count {
                    all_partitions.push((topic.clone(), partition));
                }
            }
        }

        // Sort for deterministic order (by topic name, then partition)
        all_partitions.sort();

        // Get sorted member IDs
        let mut member_ids: Vec<&String> = input.subscriptions.keys().collect();
        member_ids.sort();

        if member_ids.is_empty() {
            return result;
        }

        // CG-6: use a SINGLE global round-robin pointer across all (topic, partition) pairs — as
        // Kafka does and as this module's doc example describes — not a per-topic counter that
        // restarts at 0 for each topic (which imbalances heterogeneous multi-topic subscriptions).
        // The pointer walks the sorted members and skips any not subscribed to the current topic.
        let mut position = 0usize;
        let member_count = member_ids.len();

        for (topic, partition) in all_partitions {
            // Advance the global pointer to the next member subscribed to this topic.
            let mut assigned: Option<&String> = None;
            for _ in 0..member_count {
                let candidate = member_ids[position % member_count];
                position += 1;
                let subscribed = input
                    .subscriptions
                    .get(candidate)
                    .map(|sub| sub.topics.contains(&topic))
                    .unwrap_or(false);
                if subscribed {
                    assigned = Some(candidate);
                    break;
                }
            }

            if let Some(member_id) = assigned {
                if let Some(assignment) = result.get_mut(member_id) {
                    assignment
                        .topic_partitions
                        .entry(topic)
                        .or_default()
                        .push(partition);
                }
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::assignment::MemberSubscription;

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
    fn test_even_distribution() {
        // 6 partitions, 2 consumers -> 3 each (alternating)
        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 6)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // Round-robin: 0->m1, 1->m2, 2->m1, 3->m2, 4->m1, 5->m2
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 2, 4]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1, 3, 5]);
    }

    #[test]
    fn test_global_pointer_across_topics() {
        // CG-6: with topics A(3) and B(2) and two members subscribed to both, the round-robin
        // pointer must carry ACROSS topics: A0->m1, A1->m2, A2->m1, B0->m2, B1->m1. A per-topic
        // counter (the bug) restarts at B, giving m1 B0 and m2 B1 instead.
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a", "topic-b"]),
                ("member-2", vec!["topic-a", "topic-b"]),
            ],
            vec![("topic-a", 3), ("topic-b", 2)],
        );

        let result = RoundRobinStrategy::new().assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 2]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1]);
        // The pointer continues from topic-a into topic-b (the regression check):
        assert_eq!(result["member-1"].partitions("topic-b"), vec![1]);
        assert_eq!(result["member-2"].partitions("topic-b"), vec![0]);
    }

    #[test]
    fn test_odd_distribution() {
        // 5 partitions, 2 consumers -> 3 and 2
        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 5)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 2, 4]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1, 3]);
    }

    #[test]
    fn test_single_consumer() {
        let input = make_input(vec![("member-1", vec!["topic-a"])], vec![("topic-a", 5)]);

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(
            result["member-1"].partitions("topic-a"),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn test_more_consumers_than_partitions() {
        // 2 partitions, 5 consumers
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
                ("member-4", vec!["topic-a"]),
                ("member-5", vec!["topic-a"]),
            ],
            vec![("topic-a", 2)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // Only first 2 consumers get partitions
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1]);
        assert!(result["member-3"].partitions("topic-a").is_empty());
        assert!(result["member-4"].partitions("topic-a").is_empty());
        assert!(result["member-5"].partitions("topic-a").is_empty());
    }

    #[test]
    fn test_multiple_topics() {
        // Both consumers subscribe to both topics
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a", "topic-b"]),
                ("member-2", vec!["topic-a", "topic-b"]),
            ],
            vec![("topic-a", 3), ("topic-b", 2)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // topic-a: 0->m1, 1->m2, 2->m1
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 2]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1]);

        // topic-b: 0->m1, 1->m2
        assert_eq!(result["member-1"].partitions("topic-b"), vec![0]);
        assert_eq!(result["member-2"].partitions("topic-b"), vec![1]);
    }

    #[test]
    fn test_partial_subscription() {
        // member-1 subscribes only to topic-a, member-2 to both
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a", "topic-b"]),
            ],
            vec![("topic-a", 4), ("topic-b", 4)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // topic-a: both subscribe -> round-robin
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 2]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1, 3]);

        // topic-b: only member-2 subscribes -> all to member-2
        assert!(result["member-1"].partitions("topic-b").is_empty());
        assert_eq!(result["member-2"].partitions("topic-b"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_three_consumers() {
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 7)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // 7 partitions, 3 consumers: m1 gets 3, m2 gets 2, m3 gets 2
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 3, 6]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1, 4]);
        assert_eq!(result["member-3"].partitions("topic-a"), vec![2, 5]);
    }

    #[test]
    fn test_deterministic_ordering() {
        // Members should be sorted alphabetically
        let input = make_input(
            vec![("zebra", vec!["topic-a"]), ("alpha", vec!["topic-a"])],
            vec![("topic-a", 4)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        // Alphabetically: alpha, zebra
        assert_eq!(result["alpha"].partitions("topic-a"), vec![0, 2]);
        assert_eq!(result["zebra"].partitions("topic-a"), vec![1, 3]);
    }

    #[test]
    fn test_no_subscribers() {
        // Topic exists but no one subscribes
        let input = make_input(
            vec![("member-1", vec!["topic-a"])],
            vec![("topic-a", 4), ("topic-b", 4)],
        );

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1, 2, 3]);
        assert!(result["member-1"].partitions("topic-b").is_empty());
    }

    #[test]
    fn test_empty_input() {
        let input = AssignmentInput::new(HashMap::new(), HashMap::new());

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        assert!(result.is_empty());
    }

    #[test]
    fn test_zero_partitions() {
        let input = make_input(vec![("member-1", vec!["topic-a"])], vec![("topic-a", 0)]);

        let strategy = RoundRobinStrategy::new();
        let result = strategy.assign(&input);

        assert!(result["member-1"].partitions("topic-a").is_empty());
    }
}
