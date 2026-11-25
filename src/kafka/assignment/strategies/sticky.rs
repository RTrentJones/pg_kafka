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
//! 5. Move partitions from members at least two heavier than the lightest subscriber of
//!    their topic to that subscriber
//!
//! This is not the general assign algorithm of Kafka's StickyAssignor (which places the
//! partitions with the fewest possible consumers first), so for the same input the two can
//! choose different, equally balanced assignments (docs/PROTOCOL_DEVIATIONS.md). Every step
//! is counted against a budget ([`StickyStrategy::assign_within`]).
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

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};

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
    ///
    /// Cost: a binary search of each previous member's id among the members (about
    /// `log m` id comparisons), a map lookup of each previous topic name, and one set
    /// insertion per previous partition; the member is then its index, so no id is hashed
    /// or compared per topic or partition.
    fn keep_previous_assignments<'a>(
        &self,
        input: &'a AssignmentInput,
        result: &mut [MemberAssignment],
    ) -> HashSet<(&'a str, i32)> {
        let mut assigned = HashSet::new();
        for (member_id, prev_assignment) in &self.previous_assignments {
            // Departed members keep nothing.
            let Some(member) = input.member_index(member_id) else {
                continue;
            };
            for (topic, prev_partitions) in &prev_assignment.topic_partitions {
                // The previous assignment is the leader's, whatever names it carries: the
                // topic is looked up once, and only a topic that exists is keyed per
                // partition, so a long name over many partitions is not copied or hashed
                // per partition.
                let Some((topic, &count)) = input.topic_partitions.get_key_value(topic) else {
                    continue;
                };
                if !input.is_subscribed(member, topic) {
                    continue;
                }
                for &partition in prev_partitions {
                    if (0..count).contains(&partition)
                        && assigned.insert((topic.as_str(), partition))
                    {
                        let partitions = result[member]
                            .topic_partitions
                            .entry(topic.clone())
                            .or_default();
                        partitions.push(partition);
                    }
                }
            }
        }
        assigned
    }
}

impl StickyStrategy {
    /// Compute the assignment in at most `budget` steps, or return [`StepBudgetExceeded`]
    /// once they are spent.
    ///
    /// The assignment runs on the single DB thread, where a slow one stalls every other
    /// request. Its size is bounded before it starts (`limits::MAX_ASSIGNMENT_WORK`), but how
    /// much work the balancing takes also depends on the shape of the input (how the members'
    /// subscriptions overlap and how far the previous assignment is from balanced), so every
    /// step of distributing and balancing is counted here: one step is about one ordered-set
    /// or map operation (a move counts [`MOVE_STEPS`]), or [`ARRAY_STEPS`] array accesses or
    /// ordered entries looked at in order.
    pub fn assign_within(
        &self,
        input: &AssignmentInput,
        budget: usize,
    ) -> Result<AssignmentOutput, StepBudgetExceeded> {
        let mut steps = Steps::new(budget);
        // Every member's assignment, by member index.
        let mut result = input.empty_assignments();

        // Calculate total partitions for balancing
        let total_partitions: usize = input
            .topic_partitions
            .values()
            .map(|&count| usize::try_from(count).unwrap_or(0))
            .sum();

        let num_members = input.member_count();

        // If no members or partitions, return empty
        if num_members == 0 || total_partitions == 0 {
            return Ok(input.output(result));
        }

        // Step 1: Preserve valid assignments from previous state
        let assigned = self.keep_previous_assignments(input, &mut result);

        // Step 2: Collect orphaned partitions (not yet assigned), by topic name and then
        // partition for a deterministic assignment order
        let mut topics: Vec<(&String, i32)> = input
            .topic_partitions
            .iter()
            .map(|(topic, &count)| (topic, count))
            .collect();
        topics.sort_unstable();
        let orphaned: Vec<(&String, Vec<i32>)> = topics
            .into_iter()
            .map(|(topic, count)| {
                let partitions = (0..count)
                    .filter(|&partition| !assigned.contains(&(topic.as_str(), partition)))
                    .collect();
                (topic, partitions)
            })
            .collect();

        // Step 3: Distribute orphaned partitions to members
        let mut members = Members::new(input, &result);
        distribute_orphans(&mut members, orphaned, &mut result, &mut steps)?;

        // Step 4: Balance: move partitions from overloaded members to underloaded ones. Sticky
        // preservation (Step 1) can leave a member holding far more partitions than a peer
        // (e.g. when a new member joins an assignment one member already fully owns).
        balance_assignments(&mut members, &mut result, &mut steps)?;

        // Sort partition lists for consistent output
        for assignment in &mut result {
            for partitions in assignment.topic_partitions.values_mut() {
                partitions.sort_unstable();
            }
        }

        Ok(input.output(result))
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

    /// The assignment without a step budget; the coordinator runs
    /// [`StickyStrategy::assign_within`] instead.
    fn assign(&self, input: &AssignmentInput) -> AssignmentOutput {
        match self.assign_within(input, usize::MAX) {
            Ok(result) => result,
            Err(exceeded) => unreachable!("an assignment spent {} steps", exceeded.budget),
        }
    }
}

/// A sticky assignment spent its step budget before it finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepBudgetExceeded {
    /// The budget that was spent.
    pub budget: usize,
}

/// The steps one move takes: about as many ordered-set and map operations update the two
/// members' assignments and loads.
const MOVE_STEPS: usize = 16;

/// The steps left of an assignment's budget.
struct Steps {
    left: usize,
    budget: usize,
}

impl Steps {
    fn new(budget: usize) -> Self {
        Self {
            left: budget,
            budget,
        }
    }

    /// Spend `n` steps, or fail once the budget is spent.
    fn spend(&mut self, n: usize) -> Result<(), StepBudgetExceeded> {
        match self.left.checked_sub(n) {
            Some(left) => {
                self.left = left;
                Ok(())
            }
            None => Err(StepBudgetExceeded {
                budget: self.budget,
            }),
        }
    }
}

/// The members grouped into subscription classes (members subscribed to the same set of
/// topics), with each member's partition count.
///
/// The lightest subscriber of a topic is the lightest of the lightest members of the classes
/// subscribed to it, so a load change updates one ordered set (and the order of the classes'
/// lightest members) instead of one per subscribed topic: with wide subscriptions (hundreds of
/// topics per member) that is the difference between microseconds and seconds per rebalance,
/// and the rebalance runs on the single DB thread, where a slow assignment stalls every other
/// request. Topics subscribed to by the same classes form a topic group, which shares its
/// lightest subscriber.
///
/// A member is its index in the input ([`AssignmentInput::member_id`]), so index order is
/// id order and ties broken by index are broken by member id, without comparing ids.
struct Members<'a> {
    /// Partitions held, per member.
    loads: Vec<usize>,
    /// Subscription class, per member.
    class_of: Vec<usize>,
    /// Per class, its members by (load, index): the first is the class's lightest member.
    classes: Vec<BTreeSet<(usize, usize)>>,
    /// Every class's lightest member, as (load, index, class): the first entry whose class
    /// subscribes to a topic is that topic's lightest subscriber.
    firsts: BTreeSet<(usize, usize, usize)>,
    /// The topic group of every subscribed topic.
    topic_group: HashMap<&'a str, usize>,
    /// Per topic group, the classes subscribed to its topics, in ascending order.
    group_classes: Vec<Vec<usize>>,
}

impl<'a> Members<'a> {
    /// Cost: hashing each member's topic list once (about its subscription's bytes), plus
    /// one ordered-set insertion per member.
    fn new(input: &'a AssignmentInput, result: &[MemberAssignment]) -> Self {
        let count = input.member_count();
        let mut class_index: HashMap<Vec<&str>, usize> = HashMap::new();
        let mut topic_classes: HashMap<&str, Vec<usize>> = HashMap::new();
        let mut classes = Vec::new();
        let mut class_of = Vec::with_capacity(count);
        let mut loads = Vec::with_capacity(count);
        for (index, assignment) in result.iter().enumerate() {
            // Sorted and without repeats (`AssignmentInput::new`).
            let topics: Vec<&str> = input
                .subscription(index)
                .topics
                .iter()
                .map(String::as_str)
                .collect();
            let class = match class_index.entry(topics) {
                Entry::Occupied(entry) => *entry.get(),
                Entry::Vacant(entry) => {
                    let class = classes.len();
                    for topic in entry.key() {
                        topic_classes.entry(topic).or_default().push(class);
                    }
                    classes.push(BTreeSet::new());
                    *entry.insert(class)
                }
            };
            let load = assignment.partition_count();
            classes[class].insert((load, index));
            class_of.push(class);
            loads.push(load);
        }
        let firsts = classes
            .iter()
            .enumerate()
            .filter_map(|(class, members)| {
                let &(load, member) = members.first()?;
                Some((load, member, class))
            })
            .collect();

        let mut group_index: HashMap<Vec<usize>, usize> = HashMap::new();
        let mut group_classes = Vec::new();
        let topic_group = topic_classes
            .into_iter()
            .map(|(topic, classes)| {
                let group = *group_index.entry(classes).or_insert_with_key(|classes| {
                    group_classes.push(classes.clone());
                    group_classes.len() - 1
                });
                (topic, group)
            })
            .collect();
        Members {
            loads,
            class_of,
            classes,
            firsts,
            topic_group,
            group_classes,
        }
    }

    fn set_load(&mut self, member: usize, load: usize) {
        let class_id = self.class_of[member];
        let class = &mut self.classes[class_id];
        let before = class.first().copied();
        class.remove(&(self.loads[member], member));
        class.insert((load, member));
        self.loads[member] = load;
        let after = class.first().copied();
        if before != after {
            if let Some((load, member)) = before {
                self.firsts.remove(&(load, member, class_id));
            }
            if let Some((load, member)) = after {
                self.firsts.insert((load, member, class_id));
            }
        }
    }
}

/// Give each orphaned partition (in order) to the subscribed member with the fewest
/// partitions, ties broken by member id; a partition no member subscribes to stays
/// unassigned.
///
/// The lightest members of the classes subscribed to a topic are put in a heap once per
/// topic: while its partitions are given out only the receiver's class changes, so a
/// partition costs a heap operation instead of a look at every subscribed class, and the
/// whole distribution about the subscription entries plus the partitions.
fn distribute_orphans(
    members: &mut Members<'_>,
    orphaned: Vec<(&String, Vec<i32>)>,
    result: &mut [MemberAssignment],
    steps: &mut Steps,
) -> Result<(), StepBudgetExceeded> {
    for (topic, partitions) in orphaned {
        steps.spend(1)?;
        if partitions.is_empty() {
            continue;
        }
        let Some(&group) = members.topic_group.get(topic.as_str()) else {
            continue;
        };
        let classes = &members.group_classes[group];
        steps.spend(classes.len())?;
        let mut lightest: BinaryHeap<Reverse<(usize, usize, usize)>> = classes
            .iter()
            .filter_map(|&class| {
                let &(load, member) = members.classes[class].first()?;
                Some(Reverse((load, member, class)))
            })
            .collect();
        for partition in partitions {
            steps.spend(1)?;
            let Some(Reverse((load, member, class))) = lightest.pop() else {
                break;
            };
            result[member]
                .topic_partitions
                .entry(topic.clone())
                .or_default()
                .push(partition);
            members.set_load(member, load + 1);
            if let Some(&(load, member)) = members.classes[class].first() {
                lightest.push(Reverse((load, member, class)));
            }
        }
    }
    Ok(())
}

/// Per member, the subscribed topics it holds partitions of, by (topic group, topic): the
/// first entry of a group is the member's alphabetically first topic of that group.
type Held<'a> = BTreeSet<(usize, &'a str)>;

/// What a member's held topics share with each class: how many of its held topic groups
/// each class subscribes to, and, for the classes it has looked up a topic for, those groups
/// by (the member's first held topic of the group, group). The first entry of a class is the
/// alphabetically first topic the member holds and the class subscribes to.
///
/// The counts are an array per member, so building them costs about an increment per
/// (held group, subscribed class) pair; the ordered topics are built for a class only once
/// the member gives to it.
struct Shares<'a> {
    groups: Vec<u32>,
    topics: HashMap<usize, BTreeSet<(&'a str, usize)>>,
}

/// Array increments, or ordered-set entries looked at in order, that count as one step:
/// each costs about an eighth of an ordered-set insertion or removal.
const ARRAY_STEPS: usize = 8;

impl<'a> Shares<'a> {
    /// The shares of a member holding `held`.
    fn build(
        members: &Members<'a>,
        held: &Held<'a>,
        steps: &mut Steps,
    ) -> Result<Self, StepBudgetExceeded> {
        let class_count = members.classes.len();
        steps.spend(class_count.div_ceil(ARRAY_STEPS) + 1)?;
        let mut groups = vec![0_u32; class_count];
        for (group, _) in group_firsts(held) {
            let classes = &members.group_classes[group];
            steps.spend(classes.len().div_ceil(ARRAY_STEPS) + 1)?;
            for &class in classes {
                groups[class] += 1;
            }
        }
        Ok(Shares {
            groups,
            topics: HashMap::new(),
        })
    }

    /// Whether `class` subscribes to a topic group the member holds.
    fn shares(&self, class: usize) -> bool {
        self.groups[class] > 0
    }

    /// The member's alphabetically first held topic that `class` subscribes to, with its
    /// group.
    fn first_topic(
        &mut self,
        members: &Members<'a>,
        held: &Held<'a>,
        class: usize,
        steps: &mut Steps,
    ) -> Result<Option<(&'a str, usize)>, StepBudgetExceeded> {
        if let Entry::Vacant(entry) = self.topics.entry(class) {
            let mut topics = BTreeSet::new();
            for (group, topic) in group_firsts(held) {
                steps.spend(1)?;
                if members.group_classes[group].binary_search(&class).is_ok() {
                    topics.insert((topic, group));
                }
            }
            entry.insert(topics);
        }
        Ok(self.topics.get(&class).and_then(|t| t.first().copied()))
    }

    /// Record that the member's first held topic of `group` changed from `before` to
    /// `after` (`None`: it holds none of the group).
    fn change(
        &mut self,
        members: &Members<'a>,
        group: usize,
        (before, after): (Option<&'a str>, Option<&'a str>),
        steps: &mut Steps,
    ) -> Result<(), StepBudgetExceeded> {
        let classes = &members.group_classes[group];
        if before.is_none() || after.is_none() {
            steps.spend(classes.len().div_ceil(ARRAY_STEPS))?;
            for &class in classes {
                if before.is_none() {
                    self.groups[class] += 1;
                } else {
                    self.groups[class] -= 1;
                }
            }
        }
        let update = |topics: &mut BTreeSet<(&'a str, usize)>| {
            if let Some(before) = before {
                topics.remove(&(before, group));
            }
            if let Some(after) = after {
                topics.insert((after, group));
            }
        };
        if self.topics.len() < classes.len() {
            steps.spend(self.topics.len())?;
            for (class, topics) in &mut self.topics {
                if classes.binary_search(class).is_ok() {
                    update(topics);
                }
            }
        } else {
            steps.spend(classes.len())?;
            for class in classes {
                if let Some(topics) = self.topics.get_mut(class) {
                    update(topics);
                }
            }
        }
        Ok(())
    }
}

/// The first held topic of each topic group in `held`, with its group.
fn group_firsts<'s, 'a>(held: &'s Held<'a>) -> impl Iterator<Item = (usize, &'a str)> + 's {
    std::iter::successors(held.first().copied(), |&(group, _)| {
        held.range((group + 1, "")..).next().copied()
    })
}

/// The first topic of `group` in `held`.
fn first_of_group<'a>(held: &Held<'a>, group: usize) -> Option<&'a str> {
    held.range((group, "")..)
        .next()
        .filter(|&&(g, _)| g == group)
        .map(|&(_, topic)| topic)
}

/// Add (`add`) or remove `topic` of `group` in a member's `held` topics, and update its
/// `shares` (when they were built) if the member's first topic of the group changes.
fn change_held<'a>(
    members: &Members<'a>,
    held: &mut Held<'a>,
    shares: Option<&mut Shares<'a>>,
    (group, topic): (usize, &'a str),
    add: bool,
    steps: &mut Steps,
) -> Result<(), StepBudgetExceeded> {
    steps.spend(1)?;
    let before = first_of_group(held, group);
    let changed = if add {
        held.insert((group, topic))
    } else {
        held.remove(&(group, topic))
    };
    let Some(shares) = shares.filter(|_| changed) else {
        return Ok(());
    };
    let after = first_of_group(held, group);
    if before == after {
        return Ok(());
    }
    shares.change(members, group, (before, after), steps)
}

/// The lightest member subscribed to a topic in `shares` (the shares of a donor holding
/// `held`, `from_load` partitions), with the donor's alphabetically first topic it subscribes
/// to and that topic's group, if that member holds at least two partitions fewer than the
/// donor.
fn receiver<'a>(
    members: &Members<'a>,
    held: &Held<'a>,
    shares: &mut Shares<'a>,
    from_load: usize,
    steps: &mut Steps,
) -> Result<Option<(usize, &'a str, usize)>, StepBudgetExceeded> {
    let mut found = None;
    for (scanned, &(load, member, class)) in members.firsts.iter().enumerate() {
        if scanned % ARRAY_STEPS == 0 {
            steps.spend(1)?;
        }
        if load + 1 >= from_load {
            break;
        }
        if shares.shares(class) {
            found = Some((member, class));
            break;
        }
    }
    let Some((member, class)) = found else {
        return Ok(None);
    };
    Ok(shares
        .first_topic(members, held, class, steps)?
        .map(|(topic, group)| (member, topic, group)))
}

/// Move partitions from overloaded members to subscribed, less-loaded members until no
/// partition sits on a member that is at least two heavier than the lightest member
/// subscribed to its topic.
///
/// Each move goes from the heaviest member that has such a partition to the lightest
/// subscriber of one of its topics (ties broken by member id, then topic name), so every
/// partition is moved at most once per unit of imbalance and the members keep the rest of
/// their previous partitions.
///
/// A donor's receiver is found from its shares (which classes subscribe to the topics it
/// holds) and the classes' lightest members in load order, so finding one costs the lighter
/// classes skipped, not the donor's topic groups times their classes; the shares change only
/// when a member's first held topic of a group does. A move then costs a few ordered-set
/// operations plus at most a look at each class, so with few distinct subscriptions the
/// balancing is about linear in the moves; with many, the step budget bounds it.
///
/// A member that cannot give is set aside until it can again, so the search for a donor does
/// not rescan every heavy member on every move. Only two things can let it give: it receives
/// or gives a partition itself, or the lightest member of a class sharing its topics gets
/// lighter, to at least two below it. Loads only drop for a donor, so after each move only
/// the members set aside at least two above the donor's new load are looked at, and only if
/// the donor is now its class's lightest member. A receiver getting heavier never opens a
/// move.
fn balance_assignments(
    members: &mut Members<'_>,
    result: &mut [MemberAssignment],
    steps: &mut Steps,
) -> Result<(), StepBudgetExceeded> {
    let count = members.loads.len();
    let mut held: Vec<Held<'_>> = Vec::with_capacity(count);
    for assignment in result.iter() {
        let topics = &assignment.topic_partitions;
        steps.spend(topics.len() + 1)?;
        held.push(
            topics
                .iter()
                .filter(|(_, parts)| !parts.is_empty())
                .filter_map(|(topic, _)| {
                    let (&topic, &group) = members.topic_group.get_key_value(topic.as_str())?;
                    Some((group, topic))
                })
                .collect(),
        );
    }
    // Built when a member is first tried as a donor.
    let mut shares: Vec<Option<Shares<'_>>> = (0..count).map(|_| None).collect();
    let mut by_load: BTreeSet<(usize, usize)> = members.loads.iter().copied().zip(0..).collect();
    // The members that are not set aside, by (load, member); donors are tried from the
    // heaviest down.
    let mut candidates = by_load.clone();
    // The members set aside, by (load, member).
    let mut set_aside: BTreeSet<(usize, usize)> = BTreeSet::new();

    while let Some(&(min_load, _)) = by_load.first() {
        let mut cannot_give = Vec::new();
        let mut chosen = None;
        for &(from_load, from) in candidates.iter().rev() {
            steps.spend(1)?;
            // The lightest possible receiver is at least the overall minimum, so no donor
            // this light (or lighter) can give.
            if from_load < min_load + 2 {
                break;
            }
            if shares[from].is_none() {
                shares[from] = Some(Shares::build(members, &held[from], steps)?);
            }
            let Some(donor_shares) = &mut shares[from] else {
                continue;
            };
            match receiver(members, &held[from], donor_shares, from_load, steps)? {
                Some((to, topic, group)) => {
                    chosen = Some((from, to, topic, group));
                    break;
                }
                None => cannot_give.push(from),
            }
        }

        steps.spend(cannot_give.len())?;
        for member in cannot_give {
            let entry = (members.loads[member], member);
            candidates.remove(&entry);
            set_aside.insert(entry);
        }

        let Some((from, to, topic, group)) = chosen else {
            break;
        };

        steps.spend(MOVE_STEPS)?;
        let donor = result[from]
            .topic_partitions
            .get_mut(topic)
            .expect("the donor topic was chosen for holding a partition");
        let partition = donor
            .pop()
            .expect("the donor topic was chosen for holding a partition");
        if donor.is_empty() {
            change_held(
                members,
                &mut held[from],
                shares[from].as_mut(),
                (group, topic),
                false,
                steps,
            )?;
        }
        result[to]
            .topic_partitions
            .entry(topic.to_string())
            .or_default()
            .push(partition);
        change_held(
            members,
            &mut held[to],
            shares[to].as_mut(),
            (group, topic),
            true,
            steps,
        )?;

        let (from_load, to_load) = (members.loads[from], members.loads[to]);
        let class = members.class_of[from];
        let lightest_before = members.classes[class].first().map(|&(load, _)| load);
        for (member, load) in [(from, from_load - 1), (to, to_load + 1)] {
            let old = (members.loads[member], member);
            by_load.remove(&old);
            by_load.insert((load, member));
            set_aside.remove(&old);
            candidates.remove(&old);
            candidates.insert((load, member));
            members.set_load(member, load);
        }
        // Only the donor got lighter: a member set aside may give again if the lightest
        // member of the donor's class got lighter, below it by two, and it holds a topic
        // that class subscribes to.
        let lightest = from_load - 1;
        if lightest_before.is_some_and(|before| lightest < before) {
            let mut woken = Vec::new();
            for (scanned, &(load, member)) in set_aside.range((lightest + 2, 0)..).enumerate() {
                if scanned % ARRAY_STEPS == 0 {
                    steps.spend(1)?;
                }
                if shares[member]
                    .as_ref()
                    .is_some_and(|shares| shares.shares(class))
                {
                    woken.push((load, member));
                }
            }
            for entry in woken {
                set_aside.remove(&entry);
                candidates.insert(entry);
            }
        }
    }
    Ok(())
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

        let (result, elapsed) =
            crate::testing::cpu_time(|| StickyStrategy::with_previous(previous).assign(&input));

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

    /// A previous assignment is the leader's SyncGroup, whatever names it carries: a name
    /// that is no topic, subscribed to and over many partitions, costs nothing per
    /// partition. Each partition copied and hashed the name, so a 32 KB name over 100 000
    /// partitions took some 100 000 name-sized copies and hashes more than a 1-byte name.
    /// Both assignments also keep 100 000 partitions of a real topic, the same work for
    /// either name; the inputs are built outside the measured runs, which time only the
    /// assignment.
    #[test]
    fn test_previous_partitions_of_a_long_name_cost_nothing_per_partition() {
        const PARTITIONS: i32 = 100_000;
        let cost = |len: usize| {
            let name = "n".repeat(len);
            let previous: HashMap<String, MemberAssignment> = HashMap::from([(
                "m".to_string(),
                make_assignment(vec![
                    (name.as_str(), (0..PARTITIONS).collect()),
                    ("t", (0..PARTITIONS).collect()),
                ]),
            )]);
            let input = make_input(
                vec![("m", vec![name.as_str(), "t"])],
                vec![("t", PARTITIONS)],
            );
            // The least CPU time of three runs (noise only ever adds to it).
            (0..3)
                .map(|_| {
                    let strategy = StickyStrategy::with_previous(previous.clone());
                    let (result, spent) = crate::testing::cpu_time(|| strategy.assign(&input));
                    assert_eq!(result["m"].partition_count(), 100_000);
                    spent
                })
                .min()
                .unwrap()
        };
        let short = cost(1);
        let long = cost(32 * 1024);
        assert!(
            long < short * 3,
            "a 32 KB name took {long:?}, a 1-byte name {short:?}"
        );
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

    /// The assignor as it was before subscription classes: one ordered set per topic, every
    /// heavy member rescanned per move. Slow for wide subscriptions, but straightforward, so
    /// the tests check the fast assignor makes exactly its choices.
    mod reference {
        use std::collections::{BTreeSet, HashMap};

        use crate::kafka::assignment::strategies::{AssignmentInput, AssignmentOutput};

        pub(super) fn assign(
            previous: &super::StickyStrategy,
            input: &AssignmentInput,
        ) -> AssignmentOutput {
            let mut kept = input.empty_assignments();
            let mut all_partitions = std::collections::HashSet::new();
            for (topic, count) in &input.topic_partitions {
                for partition in 0..*count {
                    all_partitions.insert((topic.clone(), partition));
                }
            }
            if all_partitions.is_empty() {
                return input.output(kept);
            }
            let assigned = previous.keep_previous_assignments(input, &mut kept);
            let mut result = input.output(kept);
            let mut orphaned: Vec<(String, i32)> = all_partitions
                .into_iter()
                .filter(|(topic, partition)| !assigned.contains(&(topic.as_str(), *partition)))
                .collect();
            orphaned.sort();
            distribute_orphans(input, orphaned, &mut result);
            balance_assignments(&mut result, input);
            for assignment in result.values_mut() {
                for partitions in assignment.topic_partitions.values_mut() {
                    partitions.sort_unstable();
                }
            }
            result
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
            for (member_id, subscription) in input.subscriptions() {
                for topic in &subscription.topics {
                    queues
                        .entry(topic.clone())
                        .or_default()
                        .insert((loads[member_id], member_id.to_string()));
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
            let member = input.member_index(member_id).expect("a member");
            for topic in &input.subscription(member).topics {
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
                let Some((load, member_id)) = queues.get(&topic).and_then(|q| q.first()).cloned()
                else {
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
                            chosen = Some((
                                from.clone(),
                                to.clone(),
                                topic.clone(),
                                *from_load,
                                to_load,
                            ));
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
    }

    /// A small xorshift generator, so the shapes are reproducible without a dependency.
    struct Shapes(u64);

    impl Shapes {
        fn below(&mut self, bound: usize) -> usize {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            usize::try_from(self.0 % u64::try_from(bound).expect("small bound"))
                .expect("below a usize bound")
        }

        /// Members with random (possibly empty, possibly repeating) subscriptions, topics of
        /// 0-7 partitions, and a previous assignment that includes departed members, unknown
        /// topics, removed partitions and partitions claimed by two members.
        fn next(&mut self) -> (StickyStrategy, AssignmentInput) {
            let topics: Vec<(String, i32)> = (0..=self.below(5))
                .map(|t| {
                    let count = i32::try_from(self.below(8)).expect("below 8");
                    (format!("t{t}"), count)
                })
                .collect();
            let members: Vec<(String, Vec<String>)> = (0..=self.below(7))
                .map(|m| {
                    let subscribed = (0..self.below(5))
                        .map(|_| topics[self.below(topics.len())].0.clone())
                        .collect();
                    (format!("m{m}"), subscribed)
                })
                .collect();
            let mut previous = HashMap::new();
            for _ in 0..self.below(6) {
                let member = format!("m{}", self.below(10));
                let topic = format!("t{}", self.below(7));
                let partitions: Vec<i32> = (0..self.below(10))
                    .map(|_| i32::try_from(self.below(9)).expect("below 9"))
                    .collect();
                previous
                    .entry(member)
                    .or_insert_with(MemberAssignment::new)
                    .topic_partitions
                    .insert(topic, partitions);
            }
            (
                StickyStrategy::with_previous(previous),
                input_of(&members, &topics),
            )
        }
    }

    #[test]
    fn test_matches_reference_assignor() {
        let mut shapes = Shapes(0x9E37_79B9_7F4A_7C15);
        for case in 0..20_000 {
            let (strategy, input) = shapes.next();
            let expected = reference::assign(&strategy, &input);
            let actual = strategy.assign(&input);
            assert_eq!(actual.len(), expected.len(), "case {case}: {input:?}");
            for (member, assignment) in &expected {
                let mut want: Vec<_> = assignment
                    .topic_partitions
                    .iter()
                    .filter(|(_, p)| !p.is_empty())
                    .collect();
                let mut got: Vec<_> = actual[member]
                    .topic_partitions
                    .iter()
                    .filter(|(_, p)| !p.is_empty())
                    .collect();
                want.sort();
                got.sort();
                assert_eq!(got, want, "case {case}, member {member}: {input:?}");
            }
        }
    }

    #[test]
    fn test_matches_reference_assignor_on_wide_shapes() {
        // Scaled-down versions of the slow shapes below, where the reference still runs
        // quickly: the set-aside donors and subscription classes must not change a choice.
        let x_topics = topic_names("x", 40);
        let mut all = members("a", 8, &x_topics);
        all.push(("owner".to_string(), vec!["y".to_string()]));
        all.extend(members("n", 12, &["y".to_string()]));
        all.extend(members(
            "b",
            3,
            &[x_topics.clone(), vec!["y".to_string()]].concat(),
        ));
        let mut topics: Vec<(String, i32)> = x_topics.iter().map(|t| (t.clone(), 7)).collect();
        topics.push(("y".to_string(), 97));
        let mut previous = HashMap::new();
        for (i, topic) in x_topics.iter().enumerate() {
            previous
                .entry(format!("a{:05}", i % 5))
                .or_insert_with(MemberAssignment::new)
                .topic_partitions
                .insert(topic.clone(), (0..7).collect());
        }
        previous.insert(
            "owner".to_string(),
            make_assignment(vec![("y", (0..97).collect())]),
        );
        let wide = StickyStrategy::with_previous(previous);
        let input = input_of(&all, &topics);
        assert_eq!(wide.assign(&input), reference::assign(&wide, &input));

        let fresh = StickyStrategy::new();
        assert_eq!(fresh.assign(&input), reference::assign(&fresh, &input));
    }

    /// Members `prefix00000..` subscribed to `topics`.
    fn members(prefix: &str, count: usize, topics: &[String]) -> Vec<(String, Vec<String>)> {
        (0..count)
            .map(|i| (format!("{prefix}{i:05}"), topics.to_vec()))
            .collect()
    }

    fn input_of(members: &[(String, Vec<String>)], topics: &[(String, i32)]) -> AssignmentInput {
        AssignmentInput::new(
            members
                .iter()
                .map(|(id, t)| (id.clone(), MemberSubscription::new(t.clone())))
                .collect(),
            topics.iter().cloned().collect(),
        )
    }

    fn topic_names(prefix: &str, count: usize) -> Vec<String> {
        (0..count).map(|i| format!("{prefix}{i:05}")).collect()
    }

    /// Assert every existing partition is assigned exactly once, to a subscriber.
    fn assert_complete(input: &AssignmentInput, result: &AssignmentOutput) {
        let mut seen = HashSet::new();
        for (member, assignment) in result {
            for (topic, parts) in &assignment.topic_partitions {
                assert!(
                    input.is_subscribed(input.member_index(member).expect("a member"), topic),
                    "{member} holds unsubscribed {topic}"
                );
                for &p in parts {
                    assert!(
                        seen.insert((topic.clone(), p)),
                        "{topic}-{p} assigned twice"
                    );
                }
            }
        }
        let expected: usize = input
            .topic_partitions
            .values()
            .map(|&c| usize::try_from(c).unwrap_or(0))
            .sum();
        assert_eq!(seen.len(), expected);
    }

    fn timed(input: &AssignmentInput, strategy: &StickyStrategy) -> AssignmentOutput {
        let (result, elapsed) = crate::testing::cpu_time(|| strategy.assign(input));
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "sticky assignment took {elapsed:?}"
        );
        assert_complete(input, &result);
        result
    }

    #[test]
    fn test_wide_subscription_is_fast() {
        // 100 members, each subscribed to the same 1000 topics of 10 partitions.
        let topics = topic_names("t", 1000);
        let input = input_of(
            &members("m", 100, &topics),
            &topics.iter().map(|t| (t.clone(), 10)).collect::<Vec<_>>(),
        );
        let result = timed(&input, &StickyStrategy::new());
        for assignment in result.values() {
            assert_eq!(assignment.partition_count(), 100);
        }
    }

    #[test]
    fn test_many_topics_few_partitions_is_fast() {
        // 100 members, each subscribed to the same 2000 topics of 5 partitions.
        let topics = topic_names("t", 2000);
        let input = input_of(
            &members("m", 100, &topics),
            &topics.iter().map(|t| (t.clone(), 5)).collect::<Vec<_>>(),
        );
        let result = timed(&input, &StickyStrategy::new());
        for assignment in result.values() {
            assert_eq!(assignment.partition_count(), 100);
        }
    }

    #[test]
    fn test_balance_past_wide_donors_that_cannot_give_is_fast() {
        // 20 members hold a balanced assignment of 1000 x-topics of 20 partitions (one
        // partition of every topic each); one member owns all 999 partitions of topic y,
        // which 100 newcomers also subscribe to. The x-members are the heaviest members on
        // every move but cannot give anything.
        let x_topics = topic_names("x", 1000);
        let mut all = members("a", 20, &x_topics);
        all.push(("owner".to_string(), vec!["y".to_string()]));
        all.extend(members("n", 100, &["y".to_string()]));
        let mut topics: Vec<(String, i32)> = x_topics.iter().map(|t| (t.clone(), 20)).collect();
        topics.push(("y".to_string(), 999));
        let input = input_of(&all, &topics);

        let mut previous = HashMap::new();
        for topic in &x_topics {
            for p in 0..20 {
                previous
                    .entry(format!("a{p:05}"))
                    .or_insert_with(MemberAssignment::new)
                    .topic_partitions
                    .entry(topic.clone())
                    .or_insert_with(Vec::new)
                    .push(p);
            }
        }
        previous.insert(
            "owner".to_string(),
            make_assignment(vec![("y", (0..999).collect())]),
        );

        let result = timed(&input, &StickyStrategy::with_previous(previous));
        for i in 0..20 {
            assert_eq!(result[&format!("a{i:05}")].partition_count(), 1000);
        }
        // 999 y-partitions over 101 subscribers: 90 hold 10, 11 hold 9.
        let mut y_loads: Vec<usize> = std::iter::once("owner".to_string())
            .chain((0..100).map(|i| format!("n{i:05}")))
            .map(|m| result[&m].partition_count())
            .collect();
        y_loads.sort_unstable();
        assert_eq!(y_loads.first(), Some(&9));
        assert_eq!(y_loads.last(), Some(&10));
        // The owner keeps its share of its previous partitions.
        assert!(result["owner"].partitions("y").iter().all(|&p| p < 10));
    }

    #[test]
    fn test_wide_owner_rebalance_is_fast() {
        // One member owns every partition of 500 topics; 99 newcomers subscribe to all of
        // them.
        let topics = topic_names("t", 500);
        let input = input_of(
            &members("m", 100, &topics),
            &topics.iter().map(|t| (t.clone(), 20)).collect::<Vec<_>>(),
        );
        let mut owned = MemberAssignment::new();
        for topic in &topics {
            owned
                .topic_partitions
                .insert(topic.clone(), (0..20).collect());
        }
        let previous = HashMap::from([("m00000".to_string(), owned)]);
        let result = timed(&input, &StickyStrategy::with_previous(previous));
        for assignment in result.values() {
            assert_eq!(assignment.partition_count(), 100);
        }
    }

    #[test]
    fn test_mixed_subscriptions_are_fast() {
        // 200 members in 20 subscription classes: class k subscribes to a shared 500-topic
        // block plus its own 50 topics; half the members start from a previous assignment
        // of everything, held by one member per class.
        let shared = topic_names("s", 500);
        let mut all = Vec::new();
        let mut topics: Vec<(String, i32)> = shared.iter().map(|t| (t.clone(), 4)).collect();
        let mut previous = HashMap::new();
        for k in 0..20 {
            let own = topic_names(&format!("c{k:02}-"), 50);
            topics.extend(own.iter().map(|t| (t.clone(), 4)));
            let subscribed: Vec<String> = shared.iter().chain(&own).cloned().collect();
            let class = members(&format!("k{k:02}-"), 10, &subscribed);
            let mut owned = MemberAssignment::new();
            for topic in &own {
                owned
                    .topic_partitions
                    .insert(topic.clone(), (0..4).collect());
            }
            previous.insert(class[0].0.clone(), owned);
            all.extend(class);
        }
        let input = input_of(&all, &topics);
        let result = timed(&input, &StickyStrategy::with_previous(previous));
        assert_balanced(&input, &result);
    }

    /// Assert no partition sits on a member two heavier than the lightest subscriber of its
    /// topic.
    fn assert_balanced(input: &AssignmentInput, result: &AssignmentOutput) {
        let loads: HashMap<&str, usize> = result
            .iter()
            .map(|(m, a)| (m.as_str(), a.partition_count()))
            .collect();
        for (member, assignment) in result {
            for (topic, parts) in &assignment.topic_partitions {
                if parts.is_empty() {
                    continue;
                }
                let lightest = input
                    .members_for_topic(topic)
                    .iter()
                    .map(|&m| loads[input.member_id(m)])
                    .min()
                    .unwrap_or(0);
                assert!(
                    loads[member.as_str()] <= lightest + 1,
                    "{member} holds {topic} at {} over a subscriber at {lightest}",
                    loads[member.as_str()]
                );
            }
        }
    }

    /// Topics t00000.. of `parts` partitions each, all owned by member 0 before; member 0
    /// subscribes to every topic and member i (for i in 1..topics) to the topics from t{i}
    /// on. Every topic is then a topic group of its own, subscribed to by a different number
    /// of classes, and the donor of every move holds all of them. With `shuffled`, member ids
    /// do not follow the subscriptions (as with the random ids clients get), which changes
    /// the assignor's tie-breaks and the members it finds lighter.
    fn nested_over_one_owner(
        topics: usize,
        parts: i32,
        shuffled: bool,
    ) -> (StickyStrategy, AssignmentInput) {
        let names = topic_names("t", topics);
        // 389 is a prime above every count used, so i * 389 mod topics is a permutation.
        let id = |i: usize| {
            if shuffled {
                format!("m{:05}", (i + 1) * 389 % topics)
            } else {
                format!("m{i:05}")
            }
        };
        let all: Vec<(String, Vec<String>)> =
            (0..topics).map(|i| (id(i), names[i..].to_vec())).collect();
        let mut owned = MemberAssignment::new();
        for topic in &names {
            owned
                .topic_partitions
                .insert(topic.clone(), (0..parts).collect());
        }
        let input = input_of(
            &all,
            &names.iter().map(|t| (t.clone(), parts)).collect::<Vec<_>>(),
        );
        (
            StickyStrategy::with_previous(HashMap::from([(id(0), owned)])),
            input,
        )
    }

    #[test]
    fn test_nested_subscriptions_over_one_owner_are_fast() {
        // Finding a receiver must not cost the donor's topic groups times the classes of
        // each: here that is about 125,000 per move.
        for shuffled in [false, true] {
            let (strategy, input) = nested_over_one_owner(500, 4, shuffled);
            let result = timed(&input, &strategy);
            assert_balanced(&input, &result);
            if !shuffled {
                // Every member ends with 4 partitions.
                assert!(result.values().all(|a| a.partition_count() == 4));
            }
        }
    }

    #[test]
    fn test_nested_subscriptions_without_previous_are_fast() {
        let (_, input) = nested_over_one_owner(700, 8, false);
        let result = timed(&input, &StickyStrategy::new());
        assert_balanced(&input, &result);
        assert!(result.values().all(|a| a.partition_count() == 8));
    }

    #[test]
    fn test_matches_reference_assignor_on_nested_subscriptions() {
        for (topics, parts) in [(30, 3), (17, 1), (40, 7)] {
            for shuffled in [false, true] {
                let (strategy, input) = nested_over_one_owner(topics, parts, shuffled);
                assert_eq!(
                    strategy.assign(&input),
                    reference::assign(&strategy, &input)
                );
                let fresh = StickyStrategy::new();
                assert_eq!(fresh.assign(&input), reference::assign(&fresh, &input));
            }
        }
    }

    #[test]
    fn test_assignment_over_its_step_budget_is_refused() {
        let (strategy, input) = nested_over_one_owner(60, 3, true);
        let unbounded = strategy.assign(&input);
        // The smallest budget the assignment fits in.
        let (mut lo, mut hi) = (0, 1 << 30);
        while lo + 1 < hi {
            let mid = usize::midpoint(lo, hi);
            if strategy.assign_within(&input, mid).is_ok() {
                hi = mid;
            } else {
                lo = mid;
            }
        }
        assert_eq!(strategy.assign_within(&input, hi), Ok(unbounded));
        assert_eq!(
            strategy.assign_within(&input, hi - 1),
            Err(StepBudgetExceeded { budget: hi - 1 })
        );
        // The steps grow with the work: 60 topics of 3 partitions, about 180 moves and
        // 1,800 subscription entries.
        assert!(hi > 180 * MOVE_STEPS, "{hi} steps");
        assert!(hi < 20_000, "{hi} steps");
    }

    #[test]
    fn test_largest_nested_shape_fits_the_step_budget() {
        // 999 members (just under the default group_max_size) over 999 topics: of 64
        // partitions (about 630,000 entries and 64,000 moves), and of 8 with shuffled ids.
        for (parts, shuffled) in [(64, false), (8, true)] {
            let (strategy, input) = nested_over_one_owner(999, parts, shuffled);
            let result = strategy
                .assign_within(&input, crate::kafka::limits::MAX_ASSIGNMENT_STEPS / 2)
                .expect("within half the budget");
            assert_balanced(&input, &result);
        }
    }
}
