//! Range Assignment Strategy
//!
//! The Range strategy assigns partitions to consumers by range for each topic.
//!
//! # Algorithm
//!
//! For each topic independently:
//! 1. Sort consumers by member_id (deterministic ordering)
//! 2. Sort partitions by partition_id
//! 3. Calculate base range size: floor(num_partitions / num_consumers)
//! 4. Calculate extra partitions: num_partitions % num_consumers
//! 5. First `extra` consumers get one additional partition
//!
//! # Example
//!
//! With 7 partitions and 3 consumers:
//! - Base range = 7 / 3 = 2
//! - Extra = 7 % 3 = 1
//! - Consumer 0: partitions [0, 1, 2] (gets extra)
//! - Consumer 1: partitions [3, 4]
//! - Consumer 2: partitions [5, 6]
//!
//! # Characteristics
//!
//! - **Pros**: Simple, predictable, co-locates partitions (good for ordered processing)
//! - **Cons**: Can be unbalanced when number of partitions doesn't divide evenly

use std::collections::HashMap;

use super::{AssignmentInput, AssignmentOutput, AssignmentStrategy};
use crate::kafka::assignment::MemberAssignment;

/// Range partition assignment strategy
#[derive(Debug, Clone, Default)]
pub struct RangeStrategy;

impl RangeStrategy {
    /// Create a new Range strategy
    pub fn new() -> Self {
        Self
    }
}

impl AssignmentStrategy for RangeStrategy {
    fn name(&self) -> &'static str {
        "range"
    }

    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput {
        let mut result: AssignmentOutput = HashMap::new();

        // Initialize empty assignments for all members
        for member_id in input.subscriptions.keys() {
            result.insert(member_id.clone(), MemberAssignment::new());
        }

        // Process each topic independently
        for (topic, partition_count) in &input.topic_partitions {
            // Get members subscribed to this topic, sorted by ID
            let subscribed_members = input.members_for_topic(topic);

            if subscribed_members.is_empty() || *partition_count <= 0 {
                continue;
            }

            let num_members = subscribed_members.len() as i32;
            let num_partitions = *partition_count;

            // Calculate range distribution
            let base_range = num_partitions / num_members;
            let extra_partitions = num_partitions % num_members;

            let mut current_partition = 0;

            for (member_idx, member_id) in subscribed_members.iter().enumerate() {
                // First `extra_partitions` members get one additional partition
                let range_size = base_range
                    + if (member_idx as i32) < extra_partitions {
                        1
                    } else {
                        0
                    };

                if range_size > 0 {
                    let partitions: Vec<i32> =
                        (current_partition..current_partition + range_size).collect();
                    current_partition += range_size;

                    // Add to this member's assignment
                    if let Some(assignment) = result.get_mut(member_id) {
                        assignment
                            .topic_partitions
                            .insert(topic.clone(), partitions);
                    }
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
        // 6 partitions, 3 consumers -> 2 each
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 6)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![2, 3]);
        assert_eq!(result["member-3"].partitions("topic-a"), vec![4, 5]);
    }

    #[test]
    fn test_uneven_distribution() {
        // 7 partitions, 3 consumers -> 3, 2, 2
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 7)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1, 2]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![3, 4]);
        assert_eq!(result["member-3"].partitions("topic-a"), vec![5, 6]);
    }

    #[test]
    fn test_more_consumers_than_partitions() {
        // 2 partitions, 5 consumers -> 1, 1, 0, 0, 0
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

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![1]);
        assert!(result["member-3"].partitions("topic-a").is_empty());
        assert!(result["member-4"].partitions("topic-a").is_empty());
        assert!(result["member-5"].partitions("topic-a").is_empty());
    }

    #[test]
    fn test_single_consumer() {
        // 5 partitions, 1 consumer -> all to one
        let input = make_input(vec![("member-1", vec!["topic-a"])], vec![("topic-a", 5)]);

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(
            result["member-1"].partitions("topic-a"),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn test_single_partition() {
        // 1 partition, 3 consumers -> only first gets it
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]),
                ("member-3", vec!["topic-a"]),
            ],
            vec![("topic-a", 1)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert_eq!(result["member-1"].partitions("topic-a"), vec![0]);
        assert!(result["member-2"].partitions("topic-a").is_empty());
        assert!(result["member-3"].partitions("topic-a").is_empty());
    }

    #[test]
    fn test_multiple_topics() {
        // Two topics, each assigned independently
        let input = make_input(
            vec![
                ("member-1", vec!["topic-a", "topic-b"]),
                ("member-2", vec!["topic-a", "topic-b"]),
            ],
            vec![("topic-a", 4), ("topic-b", 6)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        // topic-a: 4 partitions, 2 consumers -> 2 each
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![2, 3]);

        // topic-b: 6 partitions, 2 consumers -> 3 each
        assert_eq!(result["member-1"].partitions("topic-b"), vec![0, 1, 2]);
        assert_eq!(result["member-2"].partitions("topic-b"), vec![3, 4, 5]);
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

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        // topic-a: both subscribe -> 2 each
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![2, 3]);

        // topic-b: only member-2 -> all 4
        assert!(result["member-1"].partitions("topic-b").is_empty());
        assert_eq!(result["member-2"].partitions("topic-b"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_no_subscribers() {
        // Topic exists but no one subscribes
        let input = make_input(
            vec![("member-1", vec!["topic-a"])],
            vec![("topic-a", 4), ("topic-b", 4)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        // member-1 gets topic-a, no one gets topic-b
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1, 2, 3]);
        assert!(result["member-1"].partitions("topic-b").is_empty());
    }

    #[test]
    fn test_zero_partitions() {
        let input = make_input(vec![("member-1", vec!["topic-a"])], vec![("topic-a", 0)]);

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        assert!(result["member-1"].partitions("topic-a").is_empty());
    }

    #[test]
    fn test_deterministic_ordering() {
        // Members should be sorted alphabetically for deterministic assignment
        let input = make_input(
            vec![
                ("zebra", vec!["topic-a"]),
                ("alpha", vec!["topic-a"]),
                ("middle", vec!["topic-a"]),
            ],
            vec![("topic-a", 3)],
        );

        let strategy = RangeStrategy::new();
        let result = strategy.assign(&input);

        // Alphabetically: alpha, middle, zebra
        assert_eq!(result["alpha"].partitions("topic-a"), vec![0]);
        assert_eq!(result["middle"].partitions("topic-a"), vec![1]);
        assert_eq!(result["zebra"].partitions("topic-a"), vec![2]);
    }
}
