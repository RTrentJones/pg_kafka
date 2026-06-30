//! Sticky Assignment Strategy (KIP-54)
//!
//! The Sticky strategy minimizes partition movement during rebalancing while
//! maintaining balance across consumers.
//!
//! # Goals (in priority order)
//!
//! 1. **Balance**: Partitions should be evenly distributed across consumers
//! 2. **Stickiness**: Minimize partition movement from previous assignment
//!
//! # Algorithm
//!
//! 1. Start with previous assignments (if available)
//! 2. Remove assignments for:
//!    - Departed members (no longer in group)
//!    - Topics the member no longer subscribes to
//!    - Partitions that no longer exist
//! 3. Collect orphaned partitions (from departed members or removed assignments)
//! 4. Distribute orphaned partitions to members with fewest assignments
//!
//! # Example
//!
//! Previous: Consumer A has [0,1,2], Consumer B has [3,4,5]
//! Consumer B leaves -> partitions [3,4,5] become orphaned
//! Consumer A gets [3,4,5] added -> Consumer A now has [0,1,2,3,4,5]
//!
//! # Characteristics
//!
//! - **Pros**: Minimizes rebalance overhead, reduces consumer disruption
//! - **Cons**: More complex, requires tracking previous state

use std::collections::{HashMap, HashSet};

use super::{AssignmentInput, AssignmentOutput, AssignmentStrategy};
use crate::kafka::assignment::MemberAssignment;

/// Sticky partition assignment strategy
#[derive(Debug, Clone, Default)]
pub struct StickyStrategy {
    /// Previous assignments (member_id -> MemberAssignment)
    /// Used to preserve assignments during rebalancing
    previous_assignments: HashMap<String, MemberAssignment>,
}

impl StickyStrategy {
    /// Create a new Sticky strategy with no previous state
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a Sticky strategy with previous assignments
    ///
    /// # Arguments
    /// * `previous` - Map of member_id -> MemberAssignment from last rebalance
    pub fn with_previous(previous: HashMap<String, MemberAssignment>) -> Self {
        Self {
            previous_assignments: previous,
        }
    }
}

impl AssignmentStrategy for StickyStrategy {
    fn name(&self) -> &'static str {
        // CG-2: this is the *eager* sticky assignor — it computes a full assignment server-side and
        // does not implement the cooperative (incremental, KIP-429) rebalance protocol. Advertising
        // "cooperative-sticky" would tell clients to skip revoking moved partitions, briefly
        // double-owning them. Report the honest eager name.
        "sticky"
    }

    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput {
        let mut result: AssignmentOutput = HashMap::new();

        // Initialize empty assignments for all current members
        for member_id in input.subscriptions.keys() {
            result.insert(member_id.clone(), MemberAssignment::new());
        }

        // Calculate total partitions for balancing
        let total_partitions: usize = input
            .topic_partitions
            .values()
            .map(|&count| count.max(0) as usize)
            .sum();

        let num_members = input.subscriptions.len();

        // If no members or partitions, return empty
        if num_members == 0 || total_partitions == 0 {
            return result;
        }

        // Collect all available (topic, partition) pairs
        let mut all_partitions: HashSet<(String, i32)> = HashSet::new();
        for (topic, count) in &input.topic_partitions {
            for partition in 0..*count {
                all_partitions.insert((topic.clone(), partition));
            }
        }

        // Track which partitions have been assigned
        let mut assigned: HashSet<(String, i32)> = HashSet::new();

        // Step 1: Preserve valid assignments from previous state
        for (member_id, prev_assignment) in &self.previous_assignments {
            // Skip departed members
            if !input.subscriptions.contains_key(member_id) {
                continue;
            }

            let subscription = &input.subscriptions[member_id];

            for (topic, prev_partitions) in &prev_assignment.topic_partitions {
                // Skip topics the member no longer subscribes to
                if !subscription.topics.contains(topic) {
                    continue;
                }

                // Keep valid partitions (still exist and not yet assigned)
                for &partition in prev_partitions {
                    let key = (topic.clone(), partition);
                    if all_partitions.contains(&key) && !assigned.contains(&key) {
                        result
                            .get_mut(member_id)
                            .expect("every member is initialised in `result` before this loop")
                            .topic_partitions
                            .entry(topic.clone())
                            .or_default()
                            .push(partition);
                        assigned.insert(key);
                    }
                }
            }
        }

        // Step 2: Collect orphaned partitions (not yet assigned)
        let mut orphaned: Vec<(String, i32)> =
            all_partitions.difference(&assigned).cloned().collect();

        // Sort for deterministic assignment order
        orphaned.sort();

        // Step 3: Distribute orphaned partitions to members
        // Assign to members with fewest partitions who subscribe to the topic
        for (topic, partition) in orphaned {
            // Find eligible members (subscribed to this topic)
            let mut eligible: Vec<&String> = input
                .subscriptions
                .iter()
                .filter(|(_, sub)| sub.topics.contains(&topic))
                .map(|(id, _)| id)
                .collect();

            if eligible.is_empty() {
                continue;
            }

            // Sort by current assignment count (ascending), then by member_id (for determinism)
            eligible.sort_by(|a, b| {
                let count_a = result[*a].partition_count();
                let count_b = result[*b].partition_count();
                count_a.cmp(&count_b).then_with(|| a.cmp(b))
            });

            // Assign to member with fewest partitions
            let best_member = eligible[0];
            result
                .get_mut(best_member)
                .expect("`best_member` came from `result`'s own keys")
                .topic_partitions
                .entry(topic)
                .or_default()
                .push(partition);
        }

        // Step 4: Balance — move partitions from overloaded members to underloaded ones. CG-2:
        // sticky preservation (Step 1) can leave a member holding far more partitions than a peer
        // (e.g. when a new member joins an assignment one member already fully owns), and this was
        // previously never corrected ("skip for MVP").
        balance_assignments(&mut result, input, total_partitions);

        // Sort partition lists for consistent output
        for assignment in result.values_mut() {
            for partitions in assignment.topic_partitions.values_mut() {
                partitions.sort();
            }
        }

        result
    }
}

/// Move partitions from overloaded members to subscribed, less-loaded members until the load spread
/// is at most one (CG-2). Each move takes a partition from a member and hands it to a member that
/// subscribes to its topic and is at least two partitions lighter, which strictly reduces the
/// most-loaded member's count — so the loop terminates within `total_partitions` iterations.
fn balance_assignments(
    result: &mut AssignmentOutput,
    input: &AssignmentInput,
    total_partitions: usize,
) {
    for _ in 0..total_partitions {
        // Snapshot member ids in deterministic order.
        let mut members: Vec<String> = result.keys().cloned().collect();
        members.sort();

        // Find a beneficial move: a partition of some `from` member that a subscribed, strictly
        // lighter `to` member (at least two below `from`) can take.
        let mut chosen: Option<(String, String, String, i32)> = None;
        'search: for from in &members {
            let from_count = result[from].partition_count();
            for (topic, partitions) in &result[from].topic_partitions {
                for &partition in partitions {
                    let mut best: Option<String> = None;
                    let mut best_count = usize::MAX;
                    for to in &members {
                        if to == from || !input.subscriptions[to].topics.contains(topic) {
                            continue;
                        }
                        let to_count = result[to].partition_count();
                        if to_count + 1 < from_count && to_count < best_count {
                            best_count = to_count;
                            best = Some(to.clone());
                        }
                    }
                    if let Some(to) = best {
                        chosen = Some((from.clone(), to, topic.clone(), partition));
                        break 'search;
                    }
                }
            }
        }

        let (from, to, topic, partition) = match chosen {
            Some(mv) => mv,
            None => break,
        };

        if let Some(parts) = result
            .get_mut(&from)
            .expect("`from` is a member of `result` (chosen from its keys)")
            .topic_partitions
            .get_mut(&topic)
        {
            parts.retain(|&p| p != partition);
        }
        result
            .get_mut(&to)
            .expect("`to` is a member of `result` (chosen from its keys)")
            .topic_partitions
            .entry(topic)
            .or_default()
            .push(partition);
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

    fn make_assignment(partitions: Vec<(&str, Vec<i32>)>) -> MemberAssignment {
        let topic_partitions: HashMap<String, Vec<i32>> = partitions
            .into_iter()
            .map(|(topic, parts)| (topic.to_string(), parts))
            .collect();

        MemberAssignment::with_partitions(topic_partitions)
    }

    #[test]
    fn test_new_assignment_like_roundrobin() {
        // Without previous state, should behave like round-robin
        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 4)],
        );

        let strategy = StickyStrategy::new();
        let result = strategy.assign(&input);

        // Should be balanced: 2 each
        assert_eq!(result["member-1"].partition_count(), 2);
        assert_eq!(result["member-2"].partition_count(), 2);

        // All partitions assigned
        let all_partitions: HashSet<i32> = result
            .values()
            .flat_map(|a| a.partitions("topic-a"))
            .collect();
        assert_eq!(all_partitions, [0, 1, 2, 3].iter().cloned().collect());
    }

    #[test]
    fn test_preserves_existing_assignment() {
        // Previous: member-1 has [0,1], member-2 has [2,3]
        let mut previous = HashMap::new();
        previous.insert(
            "member-1".to_string(),
            make_assignment(vec![("topic-a", vec![0, 1])]),
        );
        previous.insert(
            "member-2".to_string(),
            make_assignment(vec![("topic-a", vec![2, 3])]),
        );

        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 4)],
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // Same assignment preserved
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![2, 3]);
    }

    #[test]
    fn test_member_leaves() {
        // Previous: member-1 has [0,1], member-2 has [2,3]
        // member-2 leaves
        let mut previous = HashMap::new();
        previous.insert(
            "member-1".to_string(),
            make_assignment(vec![("topic-a", vec![0, 1])]),
        );
        previous.insert(
            "member-2".to_string(),
            make_assignment(vec![("topic-a", vec![2, 3])]),
        );

        let input = make_input(
            vec![("member-1", vec!["topic-a"])], // Only member-1 remains
            vec![("topic-a", 4)],
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // member-1 keeps [0,1] and gets [2,3] from departed member-2
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_member_joins() {
        // Previous: member-1 has all [0,1,2,3]
        // member-2 joins
        let mut previous = HashMap::new();
        previous.insert(
            "member-1".to_string(),
            make_assignment(vec![("topic-a", vec![0, 1, 2, 3])]),
        );

        let input = make_input(
            vec![
                ("member-1", vec!["topic-a"]),
                ("member-2", vec!["topic-a"]), // New member
            ],
            vec![("topic-a", 4)],
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // CG-2: a new member joining an assignment member-1 fully owns must trigger rebalancing —
        // the load is split evenly (2/2), with every partition still assigned exactly once.
        assert_eq!(result["member-1"].partition_count(), 2);
        assert_eq!(result["member-2"].partition_count(), 2);
        let all: Vec<i32> = result
            .values()
            .flat_map(|a| a.partitions("topic-a"))
            .collect();
        let unique: HashSet<i32> = all.iter().copied().collect();
        assert_eq!(unique, [0, 1, 2, 3].iter().copied().collect());
        assert_eq!(all.len(), 4, "no partition assigned to two members");
    }

    #[test]
    fn test_new_partitions_distributed() {
        // Previous: member-1 has [0,1], member-2 has [2,3]
        // Topic grows from 4 to 6 partitions
        let mut previous = HashMap::new();
        previous.insert(
            "member-1".to_string(),
            make_assignment(vec![("topic-a", vec![0, 1])]),
        );
        previous.insert(
            "member-2".to_string(),
            make_assignment(vec![("topic-a", vec![2, 3])]),
        );

        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 6)], // Grew from 4 to 6
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // Previous assignments preserved
        assert!(result["member-1"].partitions("topic-a").contains(&0));
        assert!(result["member-1"].partitions("topic-a").contains(&1));
        assert!(result["member-2"].partitions("topic-a").contains(&2));
        assert!(result["member-2"].partitions("topic-a").contains(&3));

        // New partitions 4,5 distributed (to whoever has fewer)
        let total_partitions: Vec<i32> = result
            .values()
            .flat_map(|a| a.partitions("topic-a"))
            .collect();
        assert!(total_partitions.contains(&4));
        assert!(total_partitions.contains(&5));
    }

    #[test]
    fn test_unsubscribed_topic_removed() {
        // Previous: member-1 has topic-a[0,1] and topic-b[0,1]
        // member-1 unsubscribes from topic-b
        let mut previous = HashMap::new();
        let mut assignment = make_assignment(vec![("topic-a", vec![0, 1])]);
        assignment
            .topic_partitions
            .insert("topic-b".to_string(), vec![0, 1]);
        previous.insert("member-1".to_string(), assignment);

        let input = make_input(
            vec![("member-1", vec!["topic-a"])], // No longer subscribed to topic-b
            vec![("topic-a", 2), ("topic-b", 2)],
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // topic-a preserved, topic-b removed
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert!(result["member-1"].partitions("topic-b").is_empty());
    }

    #[test]
    fn test_partition_removed() {
        // Previous: member-1 has [0,1,2,3] but topic shrinks to 2 partitions
        let mut previous = HashMap::new();
        previous.insert(
            "member-1".to_string(),
            make_assignment(vec![("topic-a", vec![0, 1, 2, 3])]),
        );

        let input = make_input(
            vec![("member-1", vec!["topic-a"])],
            vec![("topic-a", 2)], // Shrunk from 4 to 2
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // Only valid partitions [0,1] kept
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
    }

    #[test]
    fn test_multiple_topics() {
        // Previous state for multiple topics
        let mut previous = HashMap::new();
        let mut assign1 = MemberAssignment::new();
        assign1
            .topic_partitions
            .insert("topic-a".to_string(), vec![0, 1]);
        assign1
            .topic_partitions
            .insert("topic-b".to_string(), vec![0]);
        previous.insert("member-1".to_string(), assign1);

        let mut assign2 = MemberAssignment::new();
        assign2
            .topic_partitions
            .insert("topic-a".to_string(), vec![2, 3]);
        assign2
            .topic_partitions
            .insert("topic-b".to_string(), vec![1]);
        previous.insert("member-2".to_string(), assign2);

        let input = make_input(
            vec![
                ("member-1", vec!["topic-a", "topic-b"]),
                ("member-2", vec!["topic-a", "topic-b"]),
            ],
            vec![("topic-a", 4), ("topic-b", 2)],
        );

        let strategy = StickyStrategy::with_previous(previous);
        let result = strategy.assign(&input);

        // All assignments preserved
        assert_eq!(result["member-1"].partitions("topic-a"), vec![0, 1]);
        assert_eq!(result["member-1"].partitions("topic-b"), vec![0]);
        assert_eq!(result["member-2"].partitions("topic-a"), vec![2, 3]);
        assert_eq!(result["member-2"].partitions("topic-b"), vec![1]);
    }

    #[test]
    fn test_empty_previous() {
        let strategy = StickyStrategy::with_previous(HashMap::new());
        let input = make_input(
            vec![("member-1", vec!["topic-a"]), ("member-2", vec!["topic-a"])],
            vec![("topic-a", 4)],
        );

        let result = strategy.assign(&input);

        // Should be balanced
        assert_eq!(result["member-1"].partition_count(), 2);
        assert_eq!(result["member-2"].partition_count(), 2);
    }

    #[test]
    fn test_empty_input() {
        let strategy = StickyStrategy::new();
        let input = AssignmentInput::new(HashMap::new(), HashMap::new());
        let result = strategy.assign(&input);
        assert!(result.is_empty());
    }

    #[test]
    fn test_no_partitions() {
        let strategy = StickyStrategy::new();
        let input = make_input(vec![("member-1", vec!["topic-a"])], vec![("topic-a", 0)]);
        let result = strategy.assign(&input);
        assert!(result["member-1"].is_empty());
    }

    #[test]
    fn test_deterministic_orphan_assignment() {
        // With 3 members and 3 partitions, each should get 1
        // Verify deterministic assignment based on member ID ordering
        let input = make_input(
            vec![
                ("zebra", vec!["topic-a"]),
                ("alpha", vec!["topic-a"]),
                ("middle", vec!["topic-a"]),
            ],
            vec![("topic-a", 3)],
        );

        let strategy = StickyStrategy::new();
        let result = strategy.assign(&input);

        // Each gets exactly 1 partition
        assert_eq!(result["alpha"].partition_count(), 1);
        assert_eq!(result["middle"].partition_count(), 1);
        assert_eq!(result["zebra"].partition_count(), 1);

        // Verify deterministic: run again, same result
        let result2 = strategy.assign(&input);
        assert_eq!(
            result["alpha"].partitions("topic-a"),
            result2["alpha"].partitions("topic-a")
        );
    }
}
