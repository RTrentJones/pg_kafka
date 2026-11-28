//! The coordinator's groups and the running totals its admission checks read.
//!
//! Every JoinGroup and leader SyncGroup asks what all groups hold in memory, how many are in
//! use and which Empty group was least recently active. Computed by walking every group, that
//! made each such request cost O(groups) on the single DB thread (27 ms per JoinGroup at
//! 85 000 groups, also for one answered UNKNOWN_MEMBER_ID). [`GroupMap`] keeps the answers up
//! to date instead: every change to a group goes through a [`GroupMut`], which on drop moves
//! the totals by the difference between the group's state before and after, so a request's
//! cost depends only on the groups it names (and the Empty groups it forgets).

use std::collections::{BTreeSet, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use super::group::EMPTY_GROUP_TTL_SECS;
use super::{ConsumerGroup, GroupState};

/// What the totals count of one group.
#[derive(Debug, Clone, Copy)]
struct Snapshot {
    held: usize,
    empty: bool,
    last_activity: Instant,
}

impl Snapshot {
    fn of(group: &ConsumerGroup) -> Self {
        Self {
            held: group.held_bytes,
            empty: group.state == GroupState::Empty,
            last_activity: group.last_activity,
        }
    }
}

/// Sums over all groups, as each group last counted itself ([`ConsumerGroup::recount`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Totals {
    /// What all groups hold.
    pub(super) held: usize,
    /// What the groups that are not Empty hold.
    pub(super) non_empty_held: usize,
    /// How many groups are not Empty.
    pub(super) non_empty: usize,
}

/// The Empty groups by last activity, oldest first: `(last_activity, id)`. The id is the map's
/// own key (an `Arc`), so the index copies no group id.
type EmptyIndex = BTreeSet<(Instant, Arc<str>)>;

/// Move `totals` and `empty` from a group's `before` state to its `after` state (`None`: the
/// group is not in the map). O(log groups).
fn account(
    totals: &mut Totals,
    empty: &mut EmptyIndex,
    key: &Arc<str>,
    before: Option<Snapshot>,
    after: Option<Snapshot>,
) {
    if let Some(b) = before {
        totals.held = totals.held.saturating_sub(b.held);
        if b.empty {
            empty.remove(&(b.last_activity, Arc::clone(key)));
        } else {
            totals.non_empty_held = totals.non_empty_held.saturating_sub(b.held);
            totals.non_empty = totals.non_empty.saturating_sub(1);
        }
    }
    if let Some(a) = after {
        totals.held = totals.held.saturating_add(a.held);
        if a.empty {
            empty.insert((a.last_activity, Arc::clone(key)));
        } else {
            totals.non_empty_held = totals.non_empty_held.saturating_add(a.held);
            totals.non_empty += 1;
        }
    }
}

/// The coordinator's groups by id, with the running [`Totals`], the Empty groups ordered by
/// last activity and every group id in byte order. Reads go through `Deref` to the map; every
/// change goes through [`Self::get_mut`] (or the other methods here), which keeps the totals
/// and both indexes exact.
///
/// Memory: one `Arc` and an `Instant` per Empty group in the activity index, and one `Arc` per
/// group in the id index (both bounded by `pg_kafka.max_consumer_groups`), no copy of any id.
#[derive(Debug, Default)]
pub struct GroupMap {
    map: HashMap<Arc<str>, ConsumerGroup>,
    totals: Totals,
    empty: EmptyIndex,
    /// Every group id in byte order (`str`'s `Ord`), for ListGroups. A group's id never
    /// changes, so only [`Self::insert`] and [`Self::remove`] touch it.
    ids: BTreeSet<Arc<str>>,
}

/// A group borrowed for a change: on drop, the totals move by what the change did. O(log
/// groups) per drop, O(1) when the group's held bytes, state and last activity are unchanged
/// (a Heartbeat).
pub struct GroupMut<'a> {
    key: Arc<str>,
    group: &'a mut ConsumerGroup,
    totals: &'a mut Totals,
    empty: &'a mut EmptyIndex,
    before: Snapshot,
}

impl Deref for GroupMut<'_> {
    type Target = ConsumerGroup;
    fn deref(&self) -> &ConsumerGroup {
        self.group
    }
}

impl DerefMut for GroupMut<'_> {
    fn deref_mut(&mut self) -> &mut ConsumerGroup {
        self.group
    }
}

impl Drop for GroupMut<'_> {
    fn drop(&mut self) {
        let after = Snapshot::of(self.group);
        let b = self.before;
        if b.held == after.held && b.empty == after.empty && b.last_activity == after.last_activity
        {
            return;
        }
        account(self.totals, self.empty, &self.key, Some(b), Some(after));
    }
}

impl Deref for GroupMap {
    type Target = HashMap<Arc<str>, ConsumerGroup>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl GroupMap {
    /// The group `group_id`, for a change. Two hash lookups (one to share the key).
    pub fn get_mut(&mut self, group_id: &str) -> Option<GroupMut<'_>> {
        let key = Arc::clone(self.map.get_key_value(group_id)?.0);
        let group = self.map.get_mut(group_id)?;
        let before = Snapshot::of(group);
        Some(GroupMut {
            key,
            group,
            totals: &mut self.totals,
            empty: &mut self.empty,
            before,
        })
    }

    /// The group `group_id`, created Empty if the map does not have it.
    pub(super) fn get_or_create(&mut self, group_id: &str) -> GroupMut<'_> {
        if !self.map.contains_key(group_id) {
            self.insert(
                group_id.to_string(),
                ConsumerGroup::new(group_id.to_string()),
            );
        }
        self.get_mut(group_id)
            .expect("the group was inserted just above")
    }

    /// Insert `group` under `group_id`, replacing any group of that id.
    pub fn insert(&mut self, group_id: String, group: ConsumerGroup) {
        self.remove(&group_id);
        let key: Arc<str> = Arc::from(group_id);
        account(
            &mut self.totals,
            &mut self.empty,
            &key,
            None,
            Some(Snapshot::of(&group)),
        );
        self.ids.insert(Arc::clone(&key));
        self.map.insert(key, group);
    }

    /// Remove the group `group_id`.
    pub fn remove(&mut self, group_id: &str) -> Option<ConsumerGroup> {
        let (key, group) = self.map.remove_entry(group_id)?;
        self.ids.remove(&key);
        account(
            &mut self.totals,
            &mut self.empty,
            &key,
            Some(Snapshot::of(&group)),
            None,
        );
        Some(group)
    }

    /// Run `f` on every group (the periodic sweep), keeping the totals. O(groups) plus what
    /// `f` does.
    pub(super) fn for_each_mut(&mut self, mut f: impl FnMut(&str, &mut ConsumerGroup)) {
        for (key, group) in &mut self.map {
            let before = Snapshot::of(group);
            f(key, group);
            let after = Snapshot::of(group);
            if before.held != after.held
                || before.empty != after.empty
                || before.last_activity != after.last_activity
            {
                account(
                    &mut self.totals,
                    &mut self.empty,
                    key,
                    Some(before),
                    Some(after),
                );
            }
        }
    }

    /// Forget the Empty groups idle for `EMPTY_GROUP_TTL_SECS` or more: the oldest entries of
    /// the index. O(expired groups x log groups).
    pub(super) fn remove_expired_empty(&mut self) {
        while let Some((at, key)) = self.empty.first().cloned() {
            if at.elapsed().as_secs() < EMPTY_GROUP_TTL_SECS {
                break;
            }
            self.remove(&key);
        }
    }

    /// The Empty groups, least recently active first.
    pub(super) fn empty_by_age(&self) -> impl Iterator<Item = &Arc<str>> {
        self.empty.iter().map(|(_, key)| key)
    }

    /// Every group with its id, in byte order of the ids. O(1) to start, then O(1) amortized
    /// per group walked plus its hash lookup: a caller that stops early pays only for what it
    /// walked.
    pub fn in_id_order(&self) -> impl Iterator<Item = (&Arc<str>, &ConsumerGroup)> + '_ {
        self.ids
            .iter()
            .filter_map(|key| self.map.get(&**key).map(|group| (key, group)))
    }

    /// What all groups hold.
    pub(super) fn total_held(&self) -> usize {
        self.totals.held
    }

    /// What the groups that are not Empty hold.
    pub(super) fn non_empty_held(&self) -> usize {
        self.totals.non_empty_held
    }

    /// What the Empty groups hold: what forgetting all of them would free.
    pub(super) fn empty_held(&self) -> usize {
        self.totals.held.saturating_sub(self.totals.non_empty_held)
    }

    /// How many groups are not Empty.
    pub(super) fn non_empty_count(&self) -> usize {
        self.totals.non_empty
    }

    /// Whether the id index holds exactly the map's ids, for tests.
    #[cfg(test)]
    pub(super) fn ids_exact(&self) -> bool {
        self.ids.len() == self.map.len() && self.ids.iter().all(|k| self.map.contains_key(k))
    }

    /// The totals and Empty index recomputed from every group, for tests that check the
    /// running ones.
    #[cfg(test)]
    pub(super) fn recounted(&self) -> (Totals, Vec<(Instant, String)>) {
        let mut totals = Totals::default();
        let mut empty = EmptyIndex::new();
        for (key, group) in &self.map {
            account(
                &mut totals,
                &mut empty,
                key,
                None,
                Some(Snapshot::of(group)),
            );
        }
        let list = |index: &EmptyIndex| -> Vec<(Instant, String)> {
            index.iter().map(|(at, k)| (*at, k.to_string())).collect()
        };
        (totals, list(&empty))
    }

    /// The running totals and Empty index, for tests.
    #[cfg(test)]
    pub(super) fn running(&self) -> (Totals, Vec<(Instant, String)>) {
        (
            self.totals,
            self.empty
                .iter()
                .map(|(at, k)| (*at, k.to_string()))
                .collect(),
        )
    }
}
