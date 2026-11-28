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

use super::{AssignmentInput, AssignmentOutput, AssignmentStrategy};

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

    /// Cost: one step per (topic, subscriber) plus a binary search over the topic's
    /// subscribers per partition, and a lookup of the topic name (at most 249 bytes) in the
    /// receiving member's assignment per partition; members are indexes, so no member id is
    /// hashed or copied besides once into the output.
    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput {
        let mut assignments = input.empty_assignments();
        let member_count = input.member_count();
        if member_count == 0 {
            return input.output(assignments);
        }

        // Per topic with at least one subscriber, its subscribers' indexes in ascending
        // order (index order is member id order).
        let mut topics: Vec<(&str, i32, &[usize])> = input
            .topic_partitions
            .iter()
            .filter(|(_, count)| **count > 0)
            .filter_map(|(topic, &count)| {
                let subscribers = input.members_for_topic(topic);
                (!subscribers.is_empty()).then_some((topic.as_str(), count, subscribers))
            })
            .collect();
        // All (topic, partition) pairs in order: by topic name, then partition.
        topics.sort_unstable_by(|a, b| a.0.cmp(b.0));

        // Use a SINGLE global round-robin pointer across all (topic, partition) pairs — as
        // Kafka does and as this module's doc example describes — not a per-topic counter that
        // restarts at 0 for each topic (which imbalances heterogeneous multi-topic subscriptions).
        // The pointer walks the sorted members and skips any not subscribed to the current
        // topic: the member it lands on is the topic's first subscriber at or after the
        // pointer (wrapping around), found by binary search instead of by walking.
        let mut position = 0usize;
        for (topic, partition_count, subscribers) in topics {
            for partition in 0..partition_count {
                let at = position % member_count;
                let next = subscribers.partition_point(|&m| m < at);
                let member = subscribers.get(next).copied().unwrap_or(subscribers[0]);
                position = member + 1;
                let assignment = &mut assignments[member];
                match assignment.topic_partitions.get_mut(topic) {
                    Some(partitions) => partitions.push(partition),
                    None => {
                        assignment
                            .topic_partitions
                            .insert(topic.to_string(), vec![partition]);
                    }
                }
            }
        }

        input.output(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::assignment::{MemberAssignment, MemberSubscription};
    use std::collections::HashMap;

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
        assert_eq!(result["member-3"].partitions("topic-a"), [] as [i32; 0]);
        assert_eq!(result["member-4"].partitions("topic-a"), [] as [i32; 0]);
        assert_eq!(result["member-5"].partitions("topic-a"), [] as [i32; 0]);
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

        // topic-b continues the global pointer from topic-a (B0->m2, B1->m1), as Kafka's
        // assignor does, rather than restarting per topic (m1 B0, m2 B1).
        assert_eq!(result["member-1"].partitions("topic-b"), vec![1]);
        assert_eq!(result["member-2"].partitions("topic-b"), vec![0]);
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
        assert_eq!(result["member-1"].partitions("topic-b"), [] as [i32; 0]);
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
        assert_eq!(result["member-1"].partitions("topic-b"), [] as [i32; 0]);
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

        assert_eq!(result["member-1"].partitions("topic-a"), [] as [i32; 0]);
    }

    /// The assignor as a walk: the global pointer steps member by member until it reaches
    /// a subscriber of the partition's topic.
    fn walk_reference(input: &AssignmentInput) -> AssignmentOutput {
        let mut result: AssignmentOutput = input
            .subscriptions()
            .map(|(m, _)| (m.to_string(), MemberAssignment::new()))
            .collect();
        let mut members: Vec<(&str, &MemberSubscription)> = input.subscriptions().collect();
        members.sort_by_key(|&(m, _)| m);
        let mut partitions: Vec<(String, i32)> = input
            .topic_partitions
            .iter()
            .filter(|(t, _)| !input.members_for_topic(t).is_empty())
            .flat_map(|(t, &n)| (0..n).map(move |p| (t.clone(), p)))
            .collect();
        partitions.sort();
        let mut position = 0;
        for (topic, partition) in partitions {
            loop {
                let (candidate, subscription) = members[position % members.len()];
                position += 1;
                if subscription.topics.contains(&topic) {
                    result
                        .get_mut(candidate)
                        .unwrap()
                        .topic_partitions
                        .entry(topic.clone())
                        .or_default()
                        .push(partition);
                    break;
                }
            }
        }
        result
    }

    #[test]
    fn test_matches_the_pointer_walk_on_mixed_subscriptions() {
        // Members subscribed to different topic sets, so the pointer skips members, wraps
        // and lands mid-list.
        let topics = ["a", "b", "c", "d", "e"];
        for seed in 0..50_usize {
            let members: Vec<(String, Vec<&str>)> = (0..=(seed % 7))
                .map(|m| {
                    let subs = topics
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| (seed + m * 3 + i * 5) % 3 != 0)
                        .map(|(_, t)| *t)
                        .collect();
                    (format!("m{m}"), subs)
                })
                .collect();
            let input = make_input(
                members
                    .iter()
                    .map(|(id, subs)| (id.as_str(), subs.clone()))
                    .collect(),
                topics
                    .iter()
                    .enumerate()
                    .map(|(i, t)| (*t, i32::try_from((seed + i * 2) % 9).unwrap()))
                    .collect(),
            );
            assert_eq!(
                RoundRobinStrategy::new().assign(&input),
                walk_reference(&input),
                "seed {seed}"
            );
        }
    }
}
