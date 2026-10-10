use super::{
    BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, CompactionCandidate,
    HashMap, IndexShardRange, PendingBlockAdditions, PendingBlockRemovals, PendingRemoval, Result,
    ShardedTraceBloom, TRACE_INDEX_SHARD_WIDTH, TenantShardMerge, TenantTraceIndex,
    TraceBlockStats, decode_trace_shard, encode_trace_shard, level_above, shard_ranges_for_span,
    touched_shard_ranges, trace_block_fingerprint,
};

/// How an oversized or unreadable trace-index snapshot names itself in errors.
const SNAPSHOT_LABEL: &str = "trace index snapshot";

/// Trace block index.
///
/// # On disk
///
/// A published trace index is not one object. Each tenant's block records are
/// cut onto a time grid a day wide, and each slot is a
/// payload object of its own carrying the records of the blocks that cross it:
/// their trace-id blooms, their tag sets, their bounds and their level. A
/// payload is named by the hash of its own bytes, so it is immutable, and the
/// object a generation swaps is a small manifest listing
/// `(tenant, span, content hash)`.
///
/// Two things follow, and they are the point. A flush writes the payloads of
/// the shards its own blocks fall in, plus the manifest, and names every other
/// shard by the key the previous generation already used; it does not
/// republish the tenant, let alone the fleet. And a reader that knows its time
/// range fetches the payloads that meet it and no others: see
/// [`TraceIndex::load_latest_snapshot_for_range_with_max_bytes`].
///
/// The price of the shape is that a block whose span crosses a slot boundary
/// is written into both slots, bloom and all. That duplication is the
/// mechanism, not an oversight: cutting shards on block boundaries instead is
/// what would make an append rewrite its neighbours.
#[derive(Default)]
pub struct TraceIndex {
    pub(crate) tenants: HashMap<String, TenantTraceIndex>,
    /// Blocks this writer dropped and has yet to make durable. Not persisted:
    /// see [`PendingBlockRemovals`].
    pending_removals: PendingBlockRemovals,
    /// Blocks this writer registered and has yet to make durable. Not
    /// persisted: see [`PendingBlockAdditions`].
    pending_additions: PendingBlockAdditions,
}

impl TraceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a block, whose record says how many rows it holds and how
    /// many rounds of compaction produced it.
    ///
    /// A block builder writes [`BlockLevel::INGESTED`] records; only
    /// [`Self::replace_trace_blocks`] promotes one, because only it is told
    /// what the block replaced.
    pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats) {
        self.pending_removals.forget(tenant, &stats.object_key);
        self.pending_additions.record(&stats.object_key);
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        tenant_index
            .blocks
            .retain(|block| block.object_key != stats.object_key);
        tenant_index.blocks.push(stats);
    }

    /// How many rounds of compaction produced `object_key`, or
    /// [`BlockLevel::INGESTED`] for a block the index does not hold.
    #[must_use]
    pub fn block_level(&self, object_key: &str) -> BlockLevel {
        self.block_record(object_key)
            .map_or(BlockLevel::INGESTED, |block| block.level)
    }

    /// The record of `object_key`, whichever tenant holds it. Object keys are
    /// unique across tenants.
    fn block_record(&self, object_key: &str) -> Option<&TraceBlockStats> {
        self.tenants.values().find_map(|tenant_index| {
            tenant_index
                .blocks
                .iter()
                .find(|block| block.object_key == object_key)
        })
    }

    /// Every block in the index, as the compaction planner sees it.
    #[must_use]
    pub fn compaction_candidates(&self) -> Vec<CompactionCandidate> {
        let mut candidates: Vec<CompactionCandidate> = self
            .tenants
            .iter()
            .flat_map(|(tenant, tenant_index)| {
                tenant_index
                    .blocks
                    .iter()
                    .map(move |block| CompactionCandidate {
                        tenant: tenant.clone(),
                        object_key: block.object_key.clone(),
                        min_ts: block.min_ts,
                        max_ts: block.max_ts,
                        row_count: block.row_count,
                        level: block.level,
                    })
            })
            .collect();
        // `tenants` is a hash map, so its iteration order is not stable across
        // runs. The planner's output has to be, or two compactor passes over
        // the same index would disagree about which blocks pair up.
        candidates.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.object_key.cmp(&right.object_key))
        });
        candidates
    }

    #[must_use]
    pub fn trace_blocks(&self, tenant: &str) -> &[TraceBlockStats] {
        self.tenants
            .get(tenant)
            .map_or(&[], |tenant_index| tenant_index.blocks.as_slice())
    }

    #[must_use]
    pub fn tenants(&self) -> Vec<String> {
        let mut tenants: Vec<String> = self.tenants.keys().cloned().collect();
        tenants.sort();
        tenants
    }

    /// Swaps the `old_keys` blocks for `replacement`, and returns the level
    /// the replacement was stamped with.
    ///
    /// The level is not a parameter: it is one rung above the highest of the
    /// blocks being retired, and those are named here already because naming
    /// them is what retires them. A compactor therefore cannot register its
    /// output at a level that contradicts its inputs, and cannot register it
    /// without saying what they were.
    pub fn replace_trace_blocks(
        &mut self,
        tenant: &str,
        old_keys: &[String],
        mut replacement: TraceBlockStats,
    ) -> BlockLevel {
        // Read before the inputs are dropped, and never below what the
        // replacement key already sits at: a snapshot merge re-registers
        // blocks whose inputs are long gone, and re-deriving from nothing
        // would demote a compacted block back to level zero every save.
        replacement.level = level_above(old_keys.iter().map(|key| self.block_level(key)))
            .max(self.block_level(&replacement.object_key));
        let level = replacement.level;
        let old_keys: BTreeSet<&str> = old_keys.iter().map(String::as_str).collect();
        self.retire_trace_blocks(tenant, &old_keys);
        // A compaction may reuse the key of a block it replaces. That block is
        // live again, so it must not be replayed as a removal.
        self.pending_removals
            .forget(tenant, &replacement.object_key);
        self.pending_additions.record(&replacement.object_key);
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();

        let mut carried_tag_names = BTreeSet::new();
        let mut carried_tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        tenant_index.blocks.retain(|block| {
            if block.object_key == replacement.object_key {
                return false;
            }
            if !old_keys.contains(block.object_key.as_str()) {
                return true;
            }
            carried_tag_names.extend(block.tag_names.iter().cloned());
            for (tag, values) in &block.tag_values {
                carried_tag_values
                    .entry(tag.clone())
                    .or_default()
                    .extend(values.iter().cloned());
            }
            false
        });

        replacement.tag_names.extend(carried_tag_names);
        for (tag, values) in carried_tag_values {
            replacement
                .tag_values
                .entry(tag)
                .or_default()
                .extend(values);
        }
        tenant_index.blocks.push(replacement);
        level
    }

    /// Drops the `keys` blocks of `tenant`, and returns how many it dropped.
    ///
    /// This is retention's swap, where [`Self::replace_trace_blocks`] is
    /// compaction's: the blocks leave the index and nothing takes their place,
    /// so no level is derived and nothing is promoted. A key the index does
    /// not hold is skipped, so a repeated sweep returns zero rather than
    /// failing.
    ///
    /// Each dropped block is pinned to the record it dropped, so the removal
    /// survives the merge that publishes the next generation. Without the pin
    /// the merge would union the block straight back in, and every expired
    /// block would come back on the next save. See
    /// [`Self::save_latest_snapshot`].
    pub fn remove_trace_blocks(&mut self, tenant: &str, keys: &[String]) -> usize {
        let dropped: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
        // Pinned before anything is dropped: the records are the pins.
        self.retire_trace_blocks(tenant, &dropped);
        let Some(tenant_index) = self.tenants.get_mut(tenant) else {
            return 0;
        };
        let before = tenant_index.blocks.len();
        tenant_index
            .blocks
            .retain(|block| !dropped.contains(block.object_key.as_str()));
        before - tenant_index.blocks.len()
    }

    /// Records a pending removal for every `dropped` block this index holds
    /// for `tenant`, and forgets the additions those blocks were waiting on.
    ///
    /// The removals are pinned to the records being dropped, and so are read
    /// before they are. A removal that named only the object key would also
    /// drop a block another writer has since written under that key.
    fn retire_trace_blocks(&self, tenant: &str, dropped: &BTreeSet<&str>) {
        let retired: Vec<(&str, PendingRemoval)> = self
            .tenants
            .get(tenant)
            .into_iter()
            .flat_map(|tenant_index| tenant_index.blocks.iter())
            .filter(|block| dropped.contains(block.object_key.as_str()))
            .map(|block| {
                (
                    block.object_key.as_str(),
                    PendingRemoval {
                        fingerprint: trace_block_fingerprint(block),
                        min_ts: block.min_ts,
                        max_ts: block.max_ts,
                    },
                )
            })
            .collect();
        self.pending_removals.record(tenant, retired);
        for key in dropped.iter().copied() {
            self.pending_additions.forget(key);
        }
    }

    #[must_use]
    pub fn candidate_blocks_for_trace(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.blocks_overlapping(tenant, IndexShardRange::new(min_ts, max_ts))
            .filter(|b| b.bloom.maybe_contains(trace_id))
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn prune_blocks_by_tag(
        &self,
        tenant: &str,
        tag: &str,
        value: Option<&str>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.blocks_overlapping(tenant, IndexShardRange::new(min_ts, max_ts))
            .filter(|b| {
                if !b.tag_names.contains(tag) {
                    return false;
                }
                match value {
                    None => true,
                    Some(v) => b
                        .tag_values
                        .get(tag)
                        .is_some_and(|values| values.contains(v)),
                }
            })
            .map(|b| b.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn tag_names(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.blocks_overlapping(tenant, IndexShardRange::new(min_ts, max_ts))
            .flat_map(|block| block.tag_names.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn tag_values(&self, tenant: &str, tag: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.blocks_overlapping(tenant, IndexShardRange::new(min_ts, max_ts))
            .filter_map(|block| block.tag_values.get(tag))
            .flat_map(|values| values.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// The tenant's blocks whose `[min_ts, max_ts]` meets `window`, in index
    /// order. An unknown tenant has none.
    fn blocks_overlapping(
        &self,
        tenant: &str,
        window: IndexShardRange,
    ) -> impl Iterator<Item = &TraceBlockStats> {
        self.tenants
            .get(tenant)
            .into_iter()
            .flat_map(|t| t.blocks.iter())
            .filter(move |b| window.overlaps(b.min_ts, b.max_ts))
    }

    crate::index_snapshot::snapshot_persistence_methods!(SNAPSHOT_LABEL);

    /// One tenant's part of `merged_manifest`.
    ///
    /// Only the shards a contribution or a removal falls in are fetched and
    /// re-encoded. Every other shard the base names is carried into the new
    /// manifest by the object key it already has, so the generation costs one
    /// manifest plus the payloads of the shards that actually changed.
    async fn merge_tenant_shards(&self, merge: &mut TenantShardMerge<'_>) -> Result<()> {
        let tenant = merge.tenant;
        let contributed: Vec<&TraceBlockStats> = self
            .tenants
            .get(tenant)
            .into_iter()
            .flat_map(|tenant_index| tenant_index.blocks.iter())
            .filter(|block| merge.contributes(&block.object_key))
            .collect();
        let removed = merge.removed;

        let touched = touched_shard_ranges(
            contributed
                .iter()
                .map(|block| IndexShardRange::new(block.min_ts, block.max_ts)),
            removed,
            TRACE_INDEX_SHARD_WIDTH,
        );

        let mut records: BTreeMap<IndexShardRange, Vec<TraceBlockStats>> = BTreeMap::new();
        let carried = merge
            .read_touched(&touched, |range, object_key, bytes| {
                let (_, blocks) = decode_trace_shard(object_key, bytes)?;
                records.entry(range).or_default().extend(blocks);
                Ok(())
            })
            .await?;

        // Read before anything is applied: a removal whose pinned record
        // is not the one the base carries retires a block that is no
        // longer there, and publishing over it would either hide a live
        // block or double-count a retired one.
        for (object_key, removal) in removed.into_iter().flatten() {
            let unchanged = records
                .values()
                .flatten()
                .find(|block| block.object_key == *object_key)
                .is_some_and(|block| trace_block_fingerprint(block) == removal.fingerprint);
            if !unchanged {
                return Err(BlockStoreError::InvalidBlock(format!(
                    "trace compaction input `{object_key}` changed before its replacement was published"
                )));
            }
        }

        for (object_key, removal) in removed.into_iter().flatten() {
            // Only the record the removal pinned itself to is dropped. A
            // block written under the same key since is a different block,
            // and dropping it would hide an object nothing else names.
            for blocks in records.values_mut() {
                blocks.retain(|block| {
                    block.object_key != *object_key
                        || trace_block_fingerprint(block) != removal.fingerprint
                });
            }
        }

        for block in contributed {
            for blocks in records.values_mut() {
                blocks.retain(|held| held.object_key != block.object_key);
            }
            for range in shard_ranges_for_span(block.min_ts, block.max_ts, TRACE_INDEX_SHARD_WIDTH)
            {
                records.entry(range).or_default().push(block.clone());
            }
        }

        merge.carry(carried);
        for (range, mut blocks) in records {
            if blocks.is_empty() {
                // Nothing left in the slot, so the manifest stops naming
                // it and the sweep reclaims what it named.
                continue;
            }
            blocks.sort_by(|left, right| {
                left.min_ts
                    .cmp(&right.min_ts)
                    .then_with(|| left.max_ts.cmp(&right.max_ts))
                    .then_with(|| left.object_key.cmp(&right.object_key))
            });
            let bytes = encode_trace_shard(tenant, &blocks);
            merge.publish(range, bytes).await?;
        }
        Ok(())
    }

    fn snapshot_tenants(&self) -> impl Iterator<Item = &str> {
        self.tenants.keys().map(String::as_str)
    }

    /// Folds one shard payload of `tenant` into a load.
    ///
    /// A block whose span crosses a slot boundary arrives once per slot; the
    /// copies are folded together by [`Self::finish_load`].
    fn load_shard(&mut self, tenant: &str, object_key: &str, bytes: &[u8]) -> Result<()> {
        let (_, blocks) = decode_trace_shard(object_key, bytes)?;
        if !blocks.is_empty() {
            self.tenants
                .entry(tenant.to_string())
                .or_default()
                .blocks
                .extend(blocks);
        }
        Ok(())
    }

    /// Keeps one record per object key and puts each tenant's blocks in time
    /// order.
    fn finish_load(&mut self) {
        for tenant_index in self.tenants.values_mut() {
            let mut by_key: BTreeMap<String, TraceBlockStats> = BTreeMap::new();
            for block in std::mem::take(&mut tenant_index.blocks) {
                merge_record(&mut by_key, block);
            }
            tenant_index.blocks = by_key.into_values().collect();
            tenant_index.blocks.sort_by(|left, right| {
                left.min_ts
                    .cmp(&right.min_ts)
                    .then_with(|| left.max_ts.cmp(&right.max_ts))
                    .then_with(|| left.object_key.cmp(&right.object_key))
            });
        }
    }
}

/// Folds one decoded record into the records a load has already read.
///
/// A block whose span crosses a slot boundary is written into both slots, so
/// the same record arrives twice and the second copy is a no-op. Two records
/// that share an object key but say different things are a different matter:
/// the key was minted twice, which the object keys the writers derive allow.
/// Keeping both would make a query read one block twice, so exactly one
/// survives, and which one is a function of the records rather than of the
/// order the shards happened to be read in.
fn merge_record(records: &mut BTreeMap<String, TraceBlockStats>, incoming: TraceBlockStats) {
    match records.get(&incoming.object_key) {
        Some(held)
            if (held.max_ts, held.min_ts, trace_block_fingerprint(held))
                >= (
                    incoming.max_ts,
                    incoming.min_ts,
                    trace_block_fingerprint(&incoming),
                ) => {}
        _ => {
            records.insert(incoming.object_key.clone(), incoming);
        }
    }
}

impl BlockIndex for TraceIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        self.add_trace_block(
            &meta.tenant,
            TraceBlockStats {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                bloom: ShardedTraceBloom::match_all_with_tempo_defaults(),
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
                row_count: meta.row_count,
                level: meta.level,
            },
        );
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
            .map(|b| b.object_key.clone())
            .collect()
    }

    fn block_count(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.blocks.len())
    }
}
