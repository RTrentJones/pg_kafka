//! Assignment Strategy Module
//!
//! This module defines the `AssignmentStrategy` trait and provides three implementations:
//!
//! - **Range**: Assigns consecutive partition ranges to consumers (default)
//! - **RoundRobin**: Distributes partitions evenly across consumers
//! - **Sticky**: Minimizes partition movement during rebalancing
//!
//! # Strategy Selection
//!
//! Consumers advertise their supported strategies in JoinGroup, in preference
//! order. The coordinator (`ConsumerGroup::select_group_protocol`) picks one by
//! Kafka's member vote: among the strategies every member supports, each member
//! votes for its most-preferred one and the most votes win, ties going to the
//! leader's preference; `create_strategy()` maps the chosen name to an
//! implementation.

pub mod range;
pub mod roundrobin;
pub mod sticky;

use std::collections::HashMap;

use super::member_assignment::MemberAssignment;
use super::subscription::MemberSubscription;

pub use range::RangeStrategy;
pub use roundrobin::RoundRobinStrategy;
pub use sticky::StickyStrategy;

/// Input for partition assignment computation
#[derive(Debug, Clone)]
pub struct AssignmentInput {
    /// Map of member_id -> parsed subscription
    /// Contains each consumer's subscribed topics
    pub subscriptions: HashMap<String, MemberSubscription>,

    /// Available partitions per topic: topic_name -> partition_count
    pub topic_partitions: HashMap<String, i32>,
}

impl AssignmentInput {
    /// Create new assignment input
    pub fn new(
        subscriptions: HashMap<String, MemberSubscription>,
        topic_partitions: HashMap<String, i32>,
    ) -> Self {
        // The assignors iterate every partition, so a topic whose stored count
        // is corrupt (outside 1..=HARD_MAX_PARTITIONS) is left out instead of being expanded.
        let mut topic_partitions = topic_partitions;
        topic_partitions
            .retain(|_, count| crate::kafka::limits::sane_partition_count(*count).is_some());
        Self {
            subscriptions,
            topic_partitions,
        }
    }

    /// Get all unique topics that any member is subscribed to
    pub fn all_subscribed_topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self
            .subscriptions
            .values()
            .flat_map(|sub| sub.topics.iter().cloned())
            .collect();
        topics.sort();
        topics.dedup();
        topics
    }

    /// Get member IDs subscribed to a specific topic
    pub fn members_for_topic(&self, topic: &str) -> Vec<String> {
        let mut members: Vec<String> = self
            .subscriptions
            .iter()
            .filter(|(_, sub)| sub.topics.contains(&topic.to_string()))
            .map(|(id, _)| id.clone())
            .collect();
        members.sort(); // Deterministic order
        members
    }
}

/// Output of partition assignment computation
/// Map of member_id -> MemberAssignment
pub type AssignmentOutput = HashMap<String, MemberAssignment>;

/// Trait for partition assignment strategies
///
/// Implementations must be thread-safe (Send + Sync) as they may be called
/// from different threads in the coordinator.
pub trait AssignmentStrategy: Send + Sync {
    /// Strategy name (must match protocol name from JoinGroup, e.g., "range", "roundrobin")
    fn name(&self) -> &'static str;

    /// Compute partition assignments for all members
    ///
    /// # Arguments
    /// * `input` - Contains member subscriptions and available partitions
    ///
    /// # Returns
    /// Map of member_id -> MemberAssignment with their assigned partitions
    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput;
}

/// Create an assignment strategy by name
///
/// # Arguments
/// * `name` - Strategy name (case-insensitive): "range", "roundrobin", "sticky", "cooperative-sticky"
///
/// # Returns
/// Strategy instance or None if name is unrecognized
pub fn create_strategy(name: &str) -> Option<Box<dyn AssignmentStrategy>> {
    match name.to_lowercase().as_str() {
        "range" => Some(Box::new(RangeStrategy::new())),
        "roundrobin" => Some(Box::new(RoundRobinStrategy::new())),
        "sticky" | "cooperative-sticky" => Some(Box::new(StickyStrategy::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assignment_input_drops_corrupt_partition_counts() {
        // A corrupt stored count must not be expanded into billions of partitions.
        let input = AssignmentInput::new(
            HashMap::new(),
            HashMap::from([
                ("ok".to_string(), 4),
                ("zero".to_string(), 0),
                ("huge".to_string(), 2_000_000_000),
            ]),
        );
        assert_eq!(input.topic_partitions.len(), 1);
        assert_eq!(input.topic_partitions.get("ok"), Some(&4));
        let out = RangeStrategy::new().assign(&input);
        assert!(out.is_empty() || out.values().all(|a| a.topic_partitions.len() <= 1));
    }

    #[test]
    fn test_create_strategy_range() {
        let strategy = create_strategy("range").unwrap();
        assert_eq!(strategy.name(), "range");
    }

    #[test]
    fn test_create_strategy_roundrobin() {
        let strategy = create_strategy("roundrobin").unwrap();
        assert_eq!(strategy.name(), "roundrobin");
    }

    #[test]
    fn test_create_strategy_sticky() {
        // Both aliases resolve to the eager sticky assignor, which reports the honest "sticky"
        // name (not "cooperative-sticky" — it does not implement the cooperative rebalance protocol).
        let strategy = create_strategy("sticky").unwrap();
        assert_eq!(strategy.name(), "sticky");

        let strategy = create_strategy("cooperative-sticky").unwrap();
        assert_eq!(strategy.name(), "sticky");
    }

    #[test]
    fn test_create_strategy_unknown() {
        assert!(create_strategy("unknown").is_none());
    }

    #[test]
    fn test_create_strategy_case_insensitive() {
        assert!(create_strategy("RANGE").is_some());
        assert!(create_strategy("RoundRobin").is_some());
        assert!(create_strategy("STICKY").is_some());
    }

    #[test]
    fn test_assignment_input_all_subscribed_topics() {
        let mut subscriptions = HashMap::new();
        subscriptions.insert(
            "member-1".to_string(),
            MemberSubscription::new(vec!["topic-a".to_string(), "topic-b".to_string()]),
        );
        subscriptions.insert(
            "member-2".to_string(),
            MemberSubscription::new(vec!["topic-b".to_string(), "topic-c".to_string()]),
        );

        let input = AssignmentInput::new(subscriptions, HashMap::new());
        let topics = input.all_subscribed_topics();

        assert_eq!(topics, vec!["topic-a", "topic-b", "topic-c"]);
    }

    #[test]
    fn test_assignment_input_members_for_topic() {
        let mut subscriptions = HashMap::new();
        subscriptions.insert(
            "member-1".to_string(),
            MemberSubscription::new(vec!["topic-a".to_string()]),
        );
        subscriptions.insert(
            "member-2".to_string(),
            MemberSubscription::new(vec!["topic-a".to_string(), "topic-b".to_string()]),
        );
        subscriptions.insert(
            "member-3".to_string(),
            MemberSubscription::new(vec!["topic-b".to_string()]),
        );

        let input = AssignmentInput::new(subscriptions, HashMap::new());

        assert_eq!(
            input.members_for_topic("topic-a"),
            vec!["member-1", "member-2"]
        );
        assert_eq!(
            input.members_for_topic("topic-b"),
            vec!["member-2", "member-3"]
        );
        assert_eq!(input.members_for_topic("topic-c"), [] as [String; 0]);
    }
}
