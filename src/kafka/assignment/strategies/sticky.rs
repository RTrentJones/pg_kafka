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
//! ```text
//! Previous: Consumer A has [0,1,2], Consumer B has [3,4,5]
//! Consumer B leaves -> partitions [3,4,5] become orphaned
//! Consumer A gets [3,4,5] added -> Consumer A has [0,1,2,3,4,5]
//! ```
//!
//! # Characteristics
//!
//! - **Pros**: Minimizes rebalance overhead, reduces consumer disruption
//! - **Cons**: More complex, requires tracking previous state

use std::collections::{BTreeSet, HashMap, HashSet};

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

    /// Keep each remaining member's previous partitions that still exist, that it still
    /// subscribes to and that no other member kept first; returns the kept partitions.
    fn keep_previous_assignments(
        &self,
        input: &AssignmentInput,
        all_partitions: &HashSet<(String, i32)>,
        result: &mut AssignmentOutput,
    ) -> HashSet<(String, i32)> {
        let mut assigned = HashSet::new();
        for (member_id, prev_assignment) in &self.previous_assignments {
            // Departed members keep nothing.
            let Some(subscription) = input.subscriptions.get(member_id) else {
                continue;
            };
            for (topic, prev_partitions) in &prev_assignment.topic_partitions {
                if !subscription.topics.contains(topic) {
                    continue;
                }
                for &partition in prev_partitions {
                    let key = (topic.clone(), partition);
                    if all_partitions.contains(&key) && !assigned.contains(&key) {
                        result
                            .get_mut(member_id)
                            .expect("every member is initialised in `result` before this step")
                            .topic_partitions
                            .entry(topic.clone())
                            .or_default()
                            .push(partition);
                        assigned.insert(key);
                    }
                }
            }
        }
        assigned
    }
}

impl AssignmentStrategy for StickyStrategy {
    fn name(&self) -> &'static str {
        // This is the *eager* sticky assignor — it computes a full assignment server-side and
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
            .map(|&count| usize::try_from(count).unwrap_or(0))
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

        // Step 1: Preserve valid assignments from previous state
        let assigned = self.keep_previous_assignments(input, &all_partitions, &mut result);

        // Step 2: Collect orphaned partitions (not yet assigned), sorted for a deterministic
        // assignment order
        let mut orphaned: Vec<(String, i32)> =
            all_partitions.difference(&assigned).cloned().collect();
        orphaned.sort();

        // Step 3: Distribute orphaned partitions to members
        distribute_orphans(input, orphaned, &mut result);

        // Step 4: Balance: move partitions from overloaded members to underloaded ones. Sticky
        // preservation (Step 1) can leave a member holding far more partitions than a peer
        // (e.g. when a new member joins an assignment one member already fully owns).
        balance_assignments(&mut result, input);

        // Sort partition lists for consistent output
        for assignment in result.values_mut() {
            for partitions in assignment.topic_partitions.values_mut() {
                partitions.sort_unstable();
            }
        }

        result
    }
}

/// Partition count per member, counted once so that choosing a member never recounts its
/// assignment.
type Loads = HashMap<String, usize>;

fn loads_of(result: &AssignmentOutput) -> Loads {
    result
        .iter()
        .map(|(id, assignment)| (id.clone(), assignment.partition_count()))
        .collect()
}

/// Per topic, the subscribed members ordered by (load, member id): the first entry is the
/// lightest subscriber, ties broken by member id.
type TopicQueues = HashMap<String, BTreeSet<(usize, String)>>;

fn topic_queues(input: &AssignmentInput, loads: &Loads) -> TopicQueues {
    let mut queues = TopicQueues::new();
    for (member_id, subscription) in &input.subscriptions {
        for topic in &subscription.topics {
            queues
                .entry(topic.clone())
                .or_default()
                .insert((loads[member_id], member_id.clone()));
        }
    }
    queues
}

/// Change `member_id`'s load from `old` to `new` in the queue of every topic it subscribes to.
fn set_load(
    input: &AssignmentInput,
    queues: &mut TopicQueues,
    member_id: &str,
    old: usize,
    new: usize,
) {
    for topic in &input.subscriptions[member_id].topics {
        if let Some(queue) = queues.get_mut(topic) {
            queue.remove(&(old, member_id.to_string()));
            queue.insert((new, member_id.to_string()));
        }
    }
}

/// Give each orphaned partition (in order) to the subscribed member with the fewest
/// partitions, ties broken by member id; a partition no member subscribes to stays
/// unassigned.
fn distribute_orphans(
    input: &AssignmentInput,
    orphaned: Vec<(String, i32)>,
    result: &mut AssignmentOutput,
) {
    let mut queues = topic_queues(input, &loads_of(result));
    for (topic, partition) in orphaned {
        let Some((load, member_id)) = queues.get(&topic).and_then(|q| q.first()).cloned() else {
            continue;
        };
        result
            .get_mut(&member_id)
            .expect("the topic queues hold only members of `result`")
            .topic_partitions
            .entry(topic)
            .or_default()
            .push(partition);
        set_load(input, &mut queues, &member_id, load, load + 1);
    }
}

/// Move partitions from overloaded members to subscribed, less-loaded members until no
/// partition sits on a member that is at least two heavier than the lightest member
/// subscribed to its topic.
///
/// Each move goes from the heaviest member that has such a partition to the lightest
/// subscriber of one of its topics, so every partition is
/// moved at most once per unit of imbalance and the members keep the rest of their previous
/// partitions. Loads are tracked in ordered sets, so a move costs O(T log M) plus the scan
/// for a donor, instead of a rescan of every member and partition: the rebalance runs on the
/// single DB thread, where a slow assignment stalls every other request.
fn balance_assignments(result: &mut AssignmentOutput, input: &AssignmentInput) {
    let loads = loads_of(result);
    let mut queues = topic_queues(input, &loads);
    // Members by (load, id); donors are tried from the heaviest down.
    let mut by_load: BTreeSet<(usize, String)> =
        loads.iter().map(|(id, &load)| (load, id.clone())).collect();

    loop {
        let mut chosen: Option<(String, String, String, usize, usize)> = None;
        'search: for (from_load, from) in by_load.iter().rev() {
            // The lightest possible receiver is at least the overall minimum, so no donor
            // this light (or lighter) can give.
            let min_load = by_load.first().map_or(0, |(load, _)| *load);
            if *from_load < min_load + 2 {
                break;
            }
            // Give to the lightest subscriber of any topic the donor holds, ties broken by
            // member id then topic name, so receivers fill evenly across topics.
            let best = result[from]
                .topic_partitions
                .iter()
                .filter(|(_, parts)| !parts.is_empty())
                .filter_map(|(topic, _)| {
                    let (to_load, to) = queues.get(topic)?.first()?;
                    Some((*to_load, to, topic))
                })
                .min();
            if let Some((to_load, to, topic)) = best {
                if to_load + 1 < *from_load {
                    chosen = Some((from.clone(), to.clone(), topic.clone(), *from_load, to_load));
                    break 'search;
                }
            }
        }

        let Some((from, to, topic, from_load, to_load)) = chosen else {
            break;
        };

        let partition = result
            .get_mut(&from)
            .expect("`from` is a member of `result` (chosen from its keys)")
            .topic_partitions
            .get_mut(&topic)
            .and_then(Vec::pop)
            .expect("the donor topic was chosen for holding a partition");
        result
            .get_mut(&to)
            .expect("`to` is a member of `result` (chosen from its keys)")
            .topic_partitions
            .entry(topic)
            .or_default()
            .push(partition);

        by_load.remove(&(from_load, from.clone()));
        by_load.insert((from_load - 1, from.clone()));
        by_load.remove(&(to_load, to.clone()));
        by_load.insert((to_load + 1, to.clone()));
        set_load(input, &mut queues, &from, from_load, from_load - 1);
        set_load(input, &mut queues, &to, to_load, to_load + 1);
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
        assert_eq!(all_partitions, [0, 1, 2, 3].iter().copied().collect());
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

        // A new member joining an assignment member-1 fully owns must trigger rebalancing —
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
        assert_eq!(result["member-1"].partitions("topic-b"), [] as [i32; 0]);
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

    #[test]
    fn test_balance_from_one_owner_is_fast() {
        // One previous owner of every partition and many new members: the rebalance runs on
        // the single DB thread, so it must stay far below the session timeouts it would
        // otherwise stall.
        const PARTITIONS: i32 = 10_000;
        const MEMBERS: usize = 100;
        let ids: Vec<String> = (0..MEMBERS).map(|i| format!("m{i:05}")).collect();
        let owner = ids[MEMBERS - 1].clone();
        let mut previous = HashMap::new();
        previous.insert(
            owner.clone(),
            make_assignment(vec![("topic-a", (0..PARTITIONS).collect())]),
        );
        let input = make_input(
            ids.iter()
                .map(|id| (id.as_str(), vec!["topic-a"]))
                .collect(),
            vec![("topic-a", PARTITIONS)],
        );

        let start = std::time::Instant::now();
        let result = StickyStrategy::with_previous(previous).assign(&input);
        let elapsed = start.elapsed();

        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "sticky assignment took {elapsed:?}"
        );
        for id in &ids {
            assert_eq!(result[id].partition_count(), 100, "member {id}");
        }
        // The owner keeps its share of its previous partitions; nothing else moved twice.
        let kept = result[&owner].partitions("topic-a");
        assert_eq!(kept, (0..100).collect::<Vec<_>>());
        let mut all: Vec<i32> = result
            .values()
            .flat_map(|a| a.partitions("topic-a"))
            .collect();
        all.sort_unstable();
        assert_eq!(all, (0..PARTITIONS).collect::<Vec<_>>());
    }

    #[test]
    fn test_balance_respects_subscriptions() {
        // m1 owns all of topic-a and topic-b; m2 subscribes only to topic-b, m3 only to
        // topic-a. Every member ends within one of the lightest subscriber of each of its
        // topics, and only subscribed topics move.
        let mut previous = HashMap::new();
        previous.insert(
            "m1".to_string(),
            make_assignment(vec![
                ("topic-a", (0..6).collect()),
                ("topic-b", (0..6).collect()),
            ]),
        );
        let input = make_input(
            vec![
                ("m1", vec!["topic-a", "topic-b"]),
                ("m2", vec!["topic-b"]),
                ("m3", vec!["topic-a"]),
            ],
            vec![("topic-a", 6), ("topic-b", 6)],
        );
        let result = StickyStrategy::with_previous(previous).assign(&input);
        assert_eq!(result["m1"].partition_count(), 4);
        assert_eq!(result["m2"].partition_count(), 4);
        assert_eq!(result["m3"].partition_count(), 4);
        assert_eq!(result["m2"].partitions("topic-a"), [] as [i32; 0]);
        assert_eq!(result["m3"].partitions("topic-b"), [] as [i32; 0]);
    }
}
