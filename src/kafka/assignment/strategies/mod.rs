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
///
/// The subscriptions come from the members' JoinGroup metadata, so a member may list a
/// topic many times: [`AssignmentInput::new`] drops the repeats and indexes the members
/// of every subscribed topic once, and the assignors look subscribers up in that index.
/// Range and round-robin then cost about the subscription entries plus the partitions
/// assigned, never their product (assignment runs on the single DB thread, where a slow
/// assignment stalls every other request). Sticky also does in an ordinary rebalance; how
/// much its balancing takes depends on the input's shape, so the coordinator runs it with a
/// step budget ([`StickyStrategy::assign_within`]).
///
/// A member is its index in ascending member id order ([`Self::member_id`]). Member ids are
/// `{client_id}-{uuid}` and the client id is the client's, up to 32 KB, so the assignors
/// keep their per-member state in vectors by index and never copy, hash or compare an id
/// per subscribed topic, per partition or per move: past the input itself an assignment
/// copies each id once (into [`AssignmentOutput`]), and the only work proportional to the
/// ids' lengths is sorting them, about `m log m` comparisons of up to the ids' length for
/// `m` members, once per assignment.
#[derive(Debug, Clone)]
pub struct AssignmentInput {
    /// The members in ascending id order, each with its subscription (topics sorted and
    /// without repeats).
    members: Vec<(String, MemberSubscription)>,

    /// Available partitions per topic: topic_name -> partition_count
    pub topic_partitions: HashMap<String, i32>,

    /// Per subscribed topic, its subscribers' indexes in ascending order.
    topic_members: HashMap<String, Vec<usize>>,
}

impl AssignmentInput {
    /// Create new assignment input.
    ///
    /// Cost: sorting the `m` member ids (`m log m` comparisons, each up to the ids' length),
    /// sorting each member's topics, and one map lookup per subscription entry (hashing the
    /// topic name); memory: one index per (member, distinct topic) plus one copy of each
    /// distinct topic name.
    pub fn new(
        subscriptions: HashMap<String, MemberSubscription>,
        topic_partitions: HashMap<String, i32>,
    ) -> Self {
        // The assignors iterate every partition, so a topic whose stored count
        // is corrupt (outside 1..=HARD_MAX_PARTITIONS) is left out instead of being expanded.
        let mut topic_partitions = topic_partitions;
        topic_partitions
            .retain(|_, count| crate::kafka::limits::sane_partition_count(*count).is_some());
        let mut members: Vec<(String, MemberSubscription)> = subscriptions.into_iter().collect();
        members.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut topic_members: HashMap<String, Vec<usize>> = HashMap::new();
        // Members are visited in index order, so every topic's list comes out ascending.
        for (member, (_, subscription)) in members.iter_mut().enumerate() {
            subscription.topics.sort_unstable();
            subscription.topics.dedup();
            for topic in &subscription.topics {
                match topic_members.get_mut(topic.as_str()) {
                    Some(subscribers) => subscribers.push(member),
                    None => {
                        topic_members.insert(topic.clone(), vec![member]);
                    }
                }
            }
        }
        Self {
            members,
            topic_partitions,
            topic_members,
        }
    }

    /// The number of members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// The id of member `member` (an index below [`Self::member_count`]).
    pub fn member_id(&self, member: usize) -> &str {
        &self.members[member].0
    }

    /// The subscription of member `member`: its topics sorted and without repeats.
    pub fn subscription(&self, member: usize) -> &MemberSubscription {
        &self.members[member].1
    }

    /// Every member's id and subscription, in ascending id order.
    pub fn subscriptions(&self) -> impl Iterator<Item = (&str, &MemberSubscription)> {
        self.members.iter().map(|(id, s)| (id.as_str(), s))
    }

    /// The index of the member `member_id`, if it is a member (a binary search: about
    /// `log m` comparisons of the id).
    pub fn member_index(&self, member_id: &str) -> Option<usize> {
        self.members
            .binary_search_by(|(id, _)| id.as_str().cmp(member_id))
            .ok()
    }

    /// Get all unique topics that any member is subscribed to
    pub fn all_subscribed_topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self.topic_members.keys().cloned().collect();
        topics.sort_unstable();
        topics
    }

    /// The indexes of the members subscribed to `topic`, in ascending order.
    pub fn members_for_topic(&self, topic: &str) -> &[usize] {
        self.topic_members.get(topic).map_or(&[], Vec::as_slice)
    }

    /// Whether member `member` subscribes to `topic`.
    pub fn is_subscribed(&self, member: usize, topic: &str) -> bool {
        self.members_for_topic(topic).binary_search(&member).is_ok()
    }

    /// An empty assignment per member, by index.
    pub(super) fn empty_assignments(&self) -> Vec<MemberAssignment> {
        vec![MemberAssignment::new(); self.members.len()]
    }

    /// The output for `assignments`, one per member by index: each member id is copied once.
    pub(super) fn output(&self, assignments: Vec<MemberAssignment>) -> AssignmentOutput {
        self.members
            .iter()
            .map(|(id, _)| id.clone())
            .zip(assignments)
            .collect()
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

        let ids = |topic: &str| -> Vec<&str> {
            input
                .members_for_topic(topic)
                .iter()
                .map(|&m| input.member_id(m))
                .collect()
        };
        assert_eq!(ids("topic-a"), vec!["member-1", "member-2"]);
        assert_eq!(ids("topic-b"), vec!["member-2", "member-3"]);
        assert_eq!(ids("topic-c"), [] as [&str; 0]);
        assert!(input.is_subscribed(input.member_index("member-2").unwrap(), "topic-b"));
        assert!(!input.is_subscribed(input.member_index("member-1").unwrap(), "topic-b"));
        assert_eq!(input.member_index("member-4"), None);
    }
}
