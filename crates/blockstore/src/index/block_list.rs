use super::{
    BTreeMap, BTreeSet, BlockEntry, BlockLevel, BlockListRepr, BlockMeta, Deserialize, Serialize,
    SeriesFingerprint, fingerprint_set_digest,
};

/// A tenant's blocks, ordered by time, with the block-to-series pairs inverted.
///
/// Two things about the previous shape made pruning cost what it did. Blocks
/// sat in an unordered `Vec`, so every prune walked all of them however narrow
/// the window; and each block carried a `BTreeSet` of every fingerprint in it,
/// so the per-block test was a membership probe against the query's resolved
/// set, and the index grew as blocks × cardinality.
///
/// Both are addressed by the same rearrangement. `order` holds the live
/// ordinals sorted by `(min_ts, max_ts, object_key)` and `max_end` the running
/// maximum of `max_ts` along it, which together turn the overlap filter into
/// two binary searches and a walk over the blocks that can overlap. `postings`
/// holds each fingerprint once against the ordinals of the blocks that carry
/// it, so a prune reads the blocks a series is actually in rather than asking
/// every block whether it holds the series.
///
/// Removals tombstone rather than rewrite: dropping an ordinal from every
/// posting list costs a pass over all the pairs, and compaction removes blocks
/// often enough that paying it per swap is worse than paying it once when the
/// dead outnumber the live.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(from = "BlockListRepr", into = "BlockListRepr")]
pub(crate) struct BlockList {
    /// Blocks by ordinal. Never reordered while an ordinal is referenced.
    entries: Vec<BlockEntry>,
    /// Whether the ordinal is still part of the index.
    live: Vec<bool>,
    by_key: BTreeMap<String, u32>,
    /// `fingerprint -> ordinals`, ascending. The only copy of the pairs.
    postings: BTreeMap<SeriesFingerprint, Vec<u32>>,
    /// Live ordinals, sorted by `(min_ts, max_ts, object_key)`.
    order: Vec<u32>,
    /// `ordinal -> position in order`, or `u32::MAX` for a dead ordinal.
    position: Vec<u32>,
    /// `max_end[i] = max(max_ts of order[0..=i])`. Non-decreasing, so it is
    /// searchable.
    max_end: Vec<i64>,
    dead: usize,
}

/// Sentinel for an ordinal that is no longer in `order`.
const NO_POSITION: u32 = u32::MAX;

impl From<BlockListRepr> for BlockList {
    fn from(repr: BlockListRepr) -> Self {
        let mut list = Self {
            live: vec![true; repr.entries.len()],
            position: vec![NO_POSITION; repr.entries.len()],
            entries: repr.entries,
            by_key: BTreeMap::new(),
            postings: repr.postings,
            order: Vec::new(),
            max_end: Vec::new(),
            dead: 0,
        };
        list.rebuild_derived();
        list
    }
}

impl From<BlockList> for BlockListRepr {
    fn from(list: BlockList) -> Self {
        let compact = list.compacted();
        Self {
            entries: compact.entries,
            postings: compact.postings,
        }
    }
}

impl BlockList {
    /// Number of live blocks.
    pub(crate) fn len(&self) -> usize {
        self.order.len()
    }

    /// Records `meta`, replacing whatever the same object key held before.
    ///
    /// Restating a block with the same series set updates its bounds in place.
    /// Restating it with a different one retires the old ordinal and appends a
    /// new one, because the posting lists cannot be edited in place without a
    /// pass over every pair.
    pub(crate) fn insert(&mut self, meta: &BlockMeta) {
        let fingerprints = meta.fingerprints.iter().copied().collect::<BTreeSet<_>>();
        let digest = fingerprint_set_digest(fingerprints.iter().copied());

        if let Some(ordinal) = self.by_key.get(&meta.object_key).copied() {
            let entry = &self.entries[ordinal as usize];
            if entry.fingerprint_count == fingerprints.len() && entry.fingerprint_digest == digest {
                let moved = entry.min_ts != meta.min_ts || entry.max_ts != meta.max_ts;
                let entry = &mut self.entries[ordinal as usize];
                entry.min_ts = meta.min_ts;
                entry.max_ts = meta.max_ts;
                entry.row_count = meta.row_count;
                entry.level = meta.level;
                if moved {
                    self.detach(ordinal);
                    self.attach(ordinal);
                }
                return;
            }
            self.retire(ordinal);
        }

        let ordinal = u32::try_from(self.entries.len())
            .expect("a tenant holds fewer than 4 billion block registrations");
        self.entries.push(BlockEntry {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            row_count: meta.row_count,
            fingerprint_count: fingerprints.len(),
            fingerprint_digest: digest,
            level: meta.level,
        });
        self.live.push(true);
        self.position.push(NO_POSITION);
        self.by_key.insert(meta.object_key.clone(), ordinal);
        for fingerprint in fingerprints {
            self.postings.entry(fingerprint).or_default().push(ordinal);
        }
        self.attach(ordinal);
        self.compact_if_mostly_dead();
    }

    /// Drops every block whose object key is in `keys`, and returns how many
    /// live blocks it dropped.
    pub(crate) fn remove(&mut self, keys: &BTreeSet<&String>) -> usize {
        let mut removed = 0;
        for key in keys {
            let ordinal = self.by_key.get(*key).copied();
            if let Some(ordinal) = ordinal
                && self.retire(ordinal)
            {
                removed += 1;
            }
        }
        self.compact_if_mostly_dead();
        removed
    }

    /// Object keys of the blocks that overlap `[min_ts, max_ts]` and carry at
    /// least one fingerprint in `fps`, in time order.
    pub(crate) fn candidate_blocks(
        &self,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let (lo, hi) = self.window(min_ts, max_ts);
        if lo >= hi || fps.is_empty() {
            return Vec::new();
        }

        let mut hit = vec![false; self.entries.len()];
        let mut found = 0_usize;
        let window = hi - lo;
        'fingerprints: for fingerprint in fps {
            let Some(ordinals) = self.postings.get(fingerprint) else {
                continue;
            };
            for ordinal in ordinals {
                let position = self.position[*ordinal as usize];
                if position == NO_POSITION {
                    continue;
                }
                let position = position as usize;
                if position < lo || position >= hi || hit[*ordinal as usize] {
                    continue;
                }
                hit[*ordinal as usize] = true;
                found += 1;
                // Every block the window can offer is already a candidate, so
                // the fingerprints still unread cannot add one. This is what
                // keeps a query whose selector resolves to a large share of the
                // tenant off a walk over all of it.
                if found == window {
                    break 'fingerprints;
                }
            }
        }

        self.keys_in_window(lo, hi, min_ts, max_ts, |ordinal| hit[ordinal as usize])
    }

    /// Object keys of the blocks that overlap `[min_ts, max_ts]`, in time order.
    pub(crate) fn blocks_in_range(&self, min_ts: i64, max_ts: i64) -> Vec<String> {
        let (lo, hi) = self.window(min_ts, max_ts);
        self.keys_in_window(lo, hi, min_ts, max_ts, |_| true)
    }

    /// Tightest `(min, max)` across the blocks that overlap `[min_ts, max_ts]`.
    pub(crate) fn time_bounds(&self, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        let (lo, hi) = self.window(min_ts, max_ts);
        self.order[lo..hi]
            .iter()
            .map(|ordinal| &self.entries[*ordinal as usize])
            .filter(|entry| entry.overlaps(min_ts, max_ts))
            .fold(None, |bounds, entry| match bounds {
                Some((min, max)) => Some((min.min(entry.min_ts), max.max(entry.max_ts))),
                None => Some((entry.min_ts, entry.max_ts)),
            })
    }

    /// The fingerprints of each live block, keyed by ordinal.
    ///
    /// One pass over the postings rebuilds every block's set, so a caller that
    /// needs them all pays for the inversion once rather than once per block.
    /// A merge and a full block listing are both such callers.
    pub(crate) fn fingerprints_by_ordinal(&self) -> BTreeMap<u32, Vec<SeriesFingerprint>> {
        let mut by_ordinal: BTreeMap<u32, Vec<SeriesFingerprint>> = BTreeMap::new();
        for (fingerprint, ordinals) in &self.postings {
            for ordinal in ordinals {
                if self.live[*ordinal as usize] {
                    by_ordinal.entry(*ordinal).or_default().push(*fingerprint);
                }
            }
        }
        by_ordinal
    }

    /// Every live block as a [`BlockMeta`], in time order.
    pub(crate) fn metas(&self, tenant: &str) -> Vec<BlockMeta> {
        let mut fingerprints = self.fingerprints_by_ordinal();
        self.order
            .iter()
            .map(|ordinal| {
                let entry = &self.entries[*ordinal as usize];
                BlockMeta {
                    tenant: tenant.to_string(),
                    object_key: entry.object_key.clone(),
                    min_ts: entry.min_ts,
                    max_ts: entry.max_ts,
                    row_count: entry.row_count,
                    fingerprints: fingerprints.remove(ordinal).unwrap_or_default(),
                    level: entry.level,
                }
            })
            .collect()
    }

    /// The live ordinals whose blocks overlap `[min_ts, max_ts]`, in time order.
    pub(crate) fn ordinals_overlapping(&self, min_ts: i64, max_ts: i64) -> Vec<u32> {
        let (lo, hi) = self.window(min_ts, max_ts);
        self.order[lo..hi]
            .iter()
            .copied()
            .filter(|ordinal| self.entries[*ordinal as usize].overlaps(min_ts, max_ts))
            .collect()
    }

    /// How many rounds of compaction produced the live block at `object_key`.
    pub(crate) fn level_of(&self, object_key: &str) -> Option<BlockLevel> {
        self.by_key
            .get(object_key)
            .map(|ordinal| self.entries[*ordinal as usize].level)
    }

    /// The entry behind an ordinal, live or not.
    pub(crate) fn entry(&self, ordinal: u32) -> &BlockEntry {
        &self.entries[ordinal as usize]
    }

    /// The postings restricted to `ordinals`, as `fingerprint -> ordinals`.
    pub(crate) fn postings_for(
        &self,
        ordinals: &BTreeSet<u32>,
    ) -> BTreeMap<SeriesFingerprint, Vec<u32>> {
        let mut selected: BTreeMap<SeriesFingerprint, Vec<u32>> = BTreeMap::new();
        for (fingerprint, block_ordinals) in &self.postings {
            let kept = block_ordinals
                .iter()
                .copied()
                .filter(|ordinal| ordinals.contains(ordinal))
                .collect::<Vec<_>>();
            if !kept.is_empty() {
                selected.insert(*fingerprint, kept);
            }
        }
        selected
    }

    /// Every fingerprint the list holds a posting for, live blocks only.
    pub(crate) fn live_fingerprints(&self) -> BTreeSet<SeriesFingerprint> {
        self.postings
            .iter()
            .filter(|(_, ordinals)| ordinals.iter().any(|o| self.live[*o as usize]))
            .map(|(fingerprint, _)| *fingerprint)
            .collect()
    }

    /// The half-open span of `order` that can hold a block overlapping
    /// `[min_ts, max_ts]`.
    ///
    /// `hi` is exact: `order` is sorted by `min_ts`, so a block past it starts
    /// after the window ends. `lo` is conservative: `max_end` is the running
    /// maximum of `max_ts`, so everything before it provably ends before the
    /// window starts, while a block inside the span still has to be tested.
    /// Blocks are cut on time and barely overlap, so in the usual case the
    /// span is the answer and the test rejects nothing.
    fn window(&self, min_ts: i64, max_ts: i64) -> (usize, usize) {
        let entries = &self.entries;
        let hi = self
            .order
            .partition_point(|ordinal| entries[*ordinal as usize].min_ts <= max_ts);
        let lo = self.max_end[..hi].partition_point(|end| *end < min_ts);
        (lo, hi)
    }

    fn keys_in_window(
        &self,
        lo: usize,
        hi: usize,
        min_ts: i64,
        max_ts: i64,
        keep: impl Fn(u32) -> bool,
    ) -> Vec<String> {
        self.order[lo..hi]
            .iter()
            .filter(|ordinal| keep(**ordinal))
            .map(|ordinal| &self.entries[*ordinal as usize])
            .filter(|entry| entry.overlaps(min_ts, max_ts))
            .map(|entry| entry.object_key.clone())
            .collect()
    }

    /// Takes `ordinal` out of the time order and returns the position it held,
    /// or the end of the order when it held none.
    fn detach(&mut self, ordinal: u32) -> usize {
        let position = self.position[ordinal as usize];
        if position == NO_POSITION {
            return self.order.len();
        }
        let position = position as usize;
        self.order.remove(position);
        self.position[ordinal as usize] = NO_POSITION;
        self.refresh_from(position);
        position
    }

    /// Puts a live `ordinal` back into the time order.
    fn attach(&mut self, ordinal: u32) {
        let entries = &self.entries;
        let entry = &entries[ordinal as usize];
        let key = (entry.min_ts, entry.max_ts, entry.object_key.as_str());
        let position = self.order.partition_point(|other| {
            let other = &entries[*other as usize];
            (other.min_ts, other.max_ts, other.object_key.as_str()) < key
        });
        self.order.insert(position, ordinal);
        self.refresh_from(position);
    }

    /// Recomputes the position map and the running maximum from `from` on.
    ///
    /// Both are prefix-dependent only, so an edit at `from` leaves everything
    /// before it correct and nothing before it is touched.
    fn refresh_from(&mut self, from: usize) {
        self.max_end.resize(self.order.len(), i64::MIN);
        for index in from..self.order.len() {
            let ordinal = self.order[index];
            self.position[ordinal as usize] =
                u32::try_from(index).expect("a position indexes a Vec that a u32 ordinal indexes");
            let previous = if index == 0 {
                i64::MIN
            } else {
                self.max_end[index - 1]
            };
            self.max_end[index] = previous.max(self.entries[ordinal as usize].max_ts);
        }
    }

    /// Takes `ordinal` out of the live set, and says whether it was live.
    fn retire(&mut self, ordinal: u32) -> bool {
        if !self.live[ordinal as usize] {
            return false;
        }
        self.live[ordinal as usize] = false;
        self.dead += 1;
        self.detach(ordinal);
        let key = self.entries[ordinal as usize].object_key.clone();
        if self.by_key.get(&key) == Some(&ordinal) {
            self.by_key.remove(&key);
        }
        true
    }

    fn compact_if_mostly_dead(&mut self) {
        if self.dead > self.order.len() {
            *self = self.compacted();
        }
    }

    /// A copy holding only the live blocks, renumbered from zero.
    fn compacted(&self) -> Self {
        let mut remap = BTreeMap::new();
        let mut entries = Vec::with_capacity(self.order.len());
        // Ordinal order, not time order: an ordinal is an identity, and
        // renumbering along `order` would make a re-save reshuffle every
        // posting list for no gain.
        for (ordinal, entry) in self.entries.iter().enumerate() {
            if !self.live[ordinal] {
                continue;
            }
            let ordinal = u32::try_from(ordinal).expect("an ordinal came from a u32");
            remap.insert(
                ordinal,
                u32::try_from(entries.len()).expect("a live count cannot exceed the ordinals"),
            );
            entries.push(entry.clone());
        }

        let mut postings: BTreeMap<SeriesFingerprint, Vec<u32>> = BTreeMap::new();
        for (fingerprint, ordinals) in &self.postings {
            let kept = ordinals
                .iter()
                .filter_map(|ordinal| remap.get(ordinal).copied())
                .collect::<Vec<_>>();
            if !kept.is_empty() {
                postings.insert(*fingerprint, kept);
            }
        }

        let mut compacted = Self {
            live: vec![true; entries.len()],
            position: vec![NO_POSITION; entries.len()],
            entries,
            by_key: BTreeMap::new(),
            postings,
            order: Vec::new(),
            max_end: Vec::new(),
            dead: 0,
        };
        compacted.rebuild_derived();
        compacted
    }

    /// Rebuilds `by_key`, `order`, `position` and `max_end` from the entries.
    fn rebuild_derived(&mut self) {
        self.by_key.clear();
        self.order.clear();
        // A later registration of an object key supersedes an earlier one, so
        // resolve the key map first and then take the winners as the live set.
        // Decoded bytes are the only way a duplicate reaches here, and a
        // duplicate left live would put one block in the index twice.
        for (ordinal, entry) in self.entries.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).expect("an entry count fits a u32");
            if self.live[ordinal as usize] {
                self.by_key.insert(entry.object_key.clone(), ordinal);
            }
        }
        self.dead = 0;
        for (ordinal, entry) in self.entries.iter().enumerate() {
            let ordinal = u32::try_from(ordinal).expect("an entry count fits a u32");
            if self.by_key.get(&entry.object_key) == Some(&ordinal) {
                self.order.push(ordinal);
            } else {
                self.live[ordinal as usize] = false;
                self.dead += 1;
            }
        }
        let entries = &self.entries;
        self.order.sort_by(|left, right| {
            let left = &entries[*left as usize];
            let right = &entries[*right as usize];
            (left.min_ts, left.max_ts, left.object_key.as_str()).cmp(&(
                right.min_ts,
                right.max_ts,
                right.object_key.as_str(),
            ))
        });
        self.position.resize(self.entries.len(), NO_POSITION);
        self.position.fill(NO_POSITION);
        self.max_end.clear();
        self.refresh_from(0);
    }
}
