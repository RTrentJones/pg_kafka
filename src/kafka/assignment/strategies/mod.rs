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
//! Consumers advertise their supported strategies in JoinGroup. The coordinator
//! selects a common strategy using `select_common_strategy()` with priority:
//! sticky > roundrobin > range

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

/// Strategy priority for selection (higher = preferred)
fn strategy_priority(name: &str) -> i32 {
    match name.to_lowercase().as_str() {
        "sticky" | "cooperative-sticky" => 3,
        "roundrobin" => 2,
        "range" => 1,
        _ => 0,
    }
}

/// Select a common strategy supported by all members
///
/// # Algorithm
/// 1. Collect all strategies from all members' protocol lists
/// 2. Find strategies supported by ALL members (intersection)
/// 3. Return highest priority strategy from intersection
///
/// # Arguments
/// * `member_protocols` - Map of member_id -> Vec<(strategy_name, metadata_bytes)>
///
/// # Returns
/// Selected strategy name, or None if no common strategy exists
pub fn select_common_strategy(
    member_protocols: &HashMap<String, Vec<(String, Vec<u8>)>>,
) -> Option<String> {
    if member_protocols.is_empty() {
        return None;
    }

    // Collect strategy sets for each member
    let member_strategies: Vec<std::collections::HashSet<String>> = member_protocols
        .values()
        .map(|protocols| {
            protocols
                .iter()
                .map(|(name, _)| name.to_lowercase())
                .collect()
        })
        .collect();

    if member_strategies.is_empty() {
        return None;
    }

    // Find intersection (strategies supported by ALL members)
    let mut common = member_strategies[0].clone();
    for strategies in &member_strategies[1..] {
        common = common.intersection(strategies).cloned().collect();
    }

    if common.is_empty() {
        return None;
    }

    // Select highest priority strategy
    common
        .into_iter()
        .max_by_key(|name| strategy_priority(name))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let strategy = create_strategy("sticky").unwrap();
        assert_eq!(strategy.name(), "cooperative-sticky");

        let strategy = create_strategy("cooperative-sticky").unwrap();
        assert_eq!(strategy.name(), "cooperative-sticky");
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
    fn test_select_common_strategy_all_support_range() {
        let mut protocols = HashMap::new();
        protocols.insert("member-1".to_string(), vec![("range".to_string(), vec![])]);
        protocols.insert("member-2".to_string(), vec![("range".to_string(), vec![])]);

        let selected = select_common_strategy(&protocols);
        assert_eq!(selected, Some("range".to_string()));
    }

    #[test]
    fn test_select_common_strategy_prioritizes_sticky() {
        let mut protocols = HashMap::new();
        protocols.insert(
            "member-1".to_string(),
            vec![
                ("range".to_string(), vec![]),
                ("roundrobin".to_string(), vec![]),
                ("sticky".to_string(), vec![]),
            ],
        );
        protocols.insert(
            "member-2".to_string(),
            vec![
                ("range".to_string(), vec![]),
                ("sticky".to_string(), vec![]),
            ],
        );

        let selected = select_common_strategy(&protocols);
        assert_eq!(selected, Some("sticky".to_string()));
    }

    #[test]
    fn test_select_common_strategy_roundrobin_over_range() {
        let mut protocols = HashMap::new();
        protocols.insert(
            "member-1".to_string(),
            vec![
                ("range".to_string(), vec![]),
                ("roundrobin".to_string(), vec![]),
            ],
        );
        protocols.insert(
            "member-2".to_string(),
            vec![
                ("range".to_string(), vec![]),
                ("roundrobin".to_string(), vec![]),
            ],
        );

        let selected = select_common_strategy(&protocols);
        assert_eq!(selected, Some("roundrobin".to_string()));
    }

    #[test]
    fn test_select_common_strategy_no_common() {
        let mut protocols = HashMap::new();
        protocols.insert("member-1".to_string(), vec![("range".to_string(), vec![])]);
        protocols.insert(
            "member-2".to_string(),
            vec![("roundrobin".to_string(), vec![])],
        );

        let selected = select_common_strategy(&protocols);
        assert!(selected.is_none());
    }

    #[test]
    fn test_select_common_strategy_empty() {
        let protocols: HashMap<String, Vec<(String, Vec<u8>)>> = HashMap::new();
        let selected = select_common_strategy(&protocols);
        assert!(selected.is_none());
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
        assert!(input.members_for_topic("topic-c").is_empty());
    }
}
