use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, ByteSize,
    CompactionCandidate, DEFAULT_INDEX_SNAPSHOT_MAX, HashMap, IndexShardRange, IndexSnapshotRetain,
    ObjectStore, PendingBlockAdditions, PendingBlockRemovals, PendingRemoval, Result,
    ShardedTraceBloom, SnapshotManifest, TRACE_INDEX_SHARD_WIDTH, TenantTraceIndex,
    TraceBlockStats, decode_trace_shard, encode_trace_shard, instrument, level_above,
    put_manifest_snapshot, put_shard_payload, read_latest_snapshot_manifest, read_shard_payload,
    shard_payload_content_hash, shard_payload_object_key, shard_ranges_for_span,
    trace_block_fingerprint,
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
        // Pinned to the records being dropped, and so read before they are.
        // A removal that named only the key would also drop a block another
        // writer has since written under that key.
        let retired: Vec<(&str, PendingRemoval)> = self
            .tenants
            .get(tenant)
            .into_iter()
            .flat_map(|tenant_index| tenant_index.blocks.iter())
            .filter(|block| old_keys.contains(block.object_key.as_str()))
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
        for key in old_keys.iter().copied() {
            self.pending_additions.forget(key);
        }
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

    #[must_use]
    pub fn candidate_blocks_for_trace(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
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
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        t.blocks
            .iter()
            .filter(|b| b.min_ts <= max_ts && b.max_ts >= min_ts)
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
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for block in &t.blocks {
            if block.min_ts <= max_ts && block.max_ts >= min_ts {
                out.extend(block.tag_names.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    #[must_use]
    pub fn tag_values(&self, tenant: &str, tag: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(t) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for block in &t.blocks {
            if block.min_ts <= max_ts
                && block.max_ts >= min_ts
                && let Some(values) = block.tag_values.get(tag)
            {
                out.extend(values.iter().cloned());
            }
        }
        out.into_iter().collect()
    }

    /// Publishes this writer's contribution as the next generation.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save_latest_snapshot(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
    ) -> Result<String> {
        self.save_latest_snapshot_with_retain(store, key, IndexSnapshotRetain::default())
            .await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save_latest_snapshot_with_retain(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
        retain: IndexSnapshotRetain,
    ) -> Result<String> {
        self.save_latest_snapshot_with_retain_and_max_bytes(
            store,
            key,
            retain,
            DEFAULT_INDEX_SNAPSHOT_MAX,
        )
        .await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save_latest_snapshot_with_retain_and_max_bytes(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
        retain: IndexSnapshotRetain,
        max_bytes: ByteSize,
    ) -> Result<String> {
        let removals = self.pending_removals.pending();
        let additions = self.pending_additions.pending();
        let snapshot_key = put_manifest_snapshot(
            store,
            key,
            retain,
            max_bytes,
            SNAPSHOT_LABEL,
            |base| async {
                self.merged_manifest(store, key, base, &removals, &additions, max_bytes)
                    .await
            },
        )
        .await?;
        self.pending_removals.commit(&removals);
        self.pending_additions.commit(&additions);
        Ok(snapshot_key)
    }

    /// Folds this index into the manifest `base` and returns the manifest that
    /// replaces it.
    ///
    /// The base is the state of the whole system; this writer contributes what
    /// only it knows. That is `additions`, the blocks it has registered since
    /// its last successful write, plus a replay of `removals`.
    ///
    /// Contributing every block this index names instead would be the wider
    /// bug. A writer that read a block from a snapshot and has held it in
    /// memory ever since has no removal to replay when a *concurrent*
    /// compactor retires that block, so a full union would put the compaction's
    /// input back beside its output and the querier would read both. Anything
    /// this writer has already published is in the chain the base descends
    /// from, so leaving it out loses nothing.
    ///
    /// A `base` of `None` is the exception: there is no chain, so there is no
    /// concurrent writer whose removal could be undone, and everything this
    /// index names is contributed.
    ///
    /// Only the shards a contribution or a removal falls in are fetched and
    /// re-encoded. Every other shard the base names is carried into the new
    /// manifest by the object key it already has, so the generation costs one
    /// manifest plus the payloads of the shards that actually changed.
    #[instrument(
        level = "debug",
        skip_all,
        fields(key = %key, fetched = tracing::field::Empty, written = tracing::field::Empty),
        err
    )]
    async fn merged_manifest(
        &self,
        store: &Arc<dyn ObjectStore>,
        key: &str,
        base: Option<SnapshotManifest>,
        removals: &BTreeMap<String, BTreeMap<String, PendingRemoval>>,
        additions: &BTreeSet<String>,
        max_bytes: ByteSize,
    ) -> Result<SnapshotManifest> {
        let contribute_all = base.is_none();
        let base = base.unwrap_or_default();
        let mut fetched_shards = 0_usize;
        let mut written_shards = 0_usize;

        let mut tenants: BTreeSet<&str> = base.tenants().map(String::as_str).collect();
        tenants.extend(self.tenants.keys().map(String::as_str));
        tenants.extend(removals.keys().map(String::as_str));

        let mut manifest = SnapshotManifest::new();
        for tenant in tenants {
            let contributed: Vec<&TraceBlockStats> = self
                .tenants
                .get(tenant)
                .into_iter()
                .flat_map(|tenant_index| tenant_index.blocks.iter())
                .filter(|block| contribute_all || additions.contains(&block.object_key))
                .collect();
            let removed = removals.get(tenant);

            // The spans a contribution or a removal reaches into. Every shard
            // that meets one of them has to be read, because the record it
            // replaces or retires can only be in one of those.
            let mut touched: Vec<IndexShardRange> = Vec::new();
            for block in &contributed {
                touched.extend(shard_ranges_for_span(
                    block.min_ts,
                    block.max_ts,
                    TRACE_INDEX_SHARD_WIDTH,
                ));
            }
            for removal in removed.into_iter().flat_map(BTreeMap::values) {
                touched.extend(shard_ranges_for_span(
                    removal.min_ts,
                    removal.max_ts,
                    TRACE_INDEX_SHARD_WIDTH,
                ));
            }

            let mut records: BTreeMap<IndexShardRange, Vec<TraceBlockStats>> = BTreeMap::new();
            let mut carried: BTreeMap<IndexShardRange, String> = BTreeMap::new();
            for shard in base.shards_of(tenant) {
                let range = shard.range();
                if touched
                    .iter()
                    .any(|touched| range.overlaps(touched.start, touched.end))
                {
                    let object_key = shard_payload_object_key(key, tenant, range, &shard.content);
                    let bytes = read_shard_payload(store, &object_key, max_bytes).await?;
                    fetched_shards += 1;
                    let (_, blocks) = decode_trace_shard(&object_key, &bytes)?;
                    records.entry(range).or_default().extend(blocks);
                } else {
                    carried.insert(range, shard.content.clone());
                }
            }

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
                for range in
                    shard_ranges_for_span(block.min_ts, block.max_ts, TRACE_INDEX_SHARD_WIDTH)
                {
                    records.entry(range).or_default().push(block.clone());
                }
            }

            for (range, content) in carried {
                manifest.insert(tenant, range, content);
            }
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
                let content = shard_payload_content_hash(&bytes);
                // A shard a merge read and put back unchanged encodes to the
                // same bytes and so to the same key, and the object is already
                // there. Not writing it is the difference between a flush that
                // rewrites what it touched and one that rewrites what it read.
                if base
                    .shards_of(tenant)
                    .iter()
                    .any(|shard| shard.range() == range && shard.content == content)
                {
                    manifest.insert(tenant, range, content);
                    continue;
                }
                put_shard_payload(store, key, tenant, range, &content, bytes).await?;
                written_shards += 1;
                manifest.insert(tenant, range, content);
            }
        }

        manifest.sort();
        tracing::Span::current().record("fetched", fetched_shards);
        tracing::Span::current().record("written", written_shards);
        Ok(manifest)
    }

    /// Loads the whole published index, across every tenant and every shard.
    ///
    /// This is the whole-fleet load, and it is the one a query should not be
    /// doing: see [`Self::load_latest_snapshot_for_range_with_max_bytes`].
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_latest_snapshot(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        Self::load_latest_snapshot_with_max_bytes(store, key, DEFAULT_INDEX_SNAPSHOT_MAX).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_latest_snapshot_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let Some(manifest) =
            read_latest_snapshot_manifest(store, key, max_bytes, SNAPSHOT_LABEL).await?
        else {
            return Err(BlockStoreError::ObjectStore(format!(
                "no {SNAPSHOT_LABEL} published under `{key}`"
            )));
        };
        Self::from_manifest(store, key, &manifest, None, max_bytes).await
    }

    /// Loads only the shards of one tenant that meet `[min_ts, max_ts]`.
    ///
    /// A shard's span is in the manifest, so the loader decides from the
    /// manifest alone which payloads it has to fetch and never touches the
    /// rest. This is the load a query wants: what it holds is proportional to
    /// the range it asked about, not to the retention.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_latest_snapshot_for_range_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        tenant: &str,
        min_ts: i64,
        max_ts: i64,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let Some(manifest) =
            read_latest_snapshot_manifest(store, key, max_bytes, SNAPSHOT_LABEL).await?
        else {
            return Ok(Self::new());
        };
        Self::from_manifest(
            store,
            key,
            &manifest,
            Some((tenant, min_ts, max_ts)),
            max_bytes,
        )
        .await
    }

    /// Loads the newest snapshot, returning an empty index when the key has no
    /// generation yet.
    ///
    /// # Errors
    /// Returns an error when listing or reading object storage fails, or when
    /// persisted metadata is malformed.
    pub async fn load_latest_snapshot_or_empty_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let Some(manifest) =
            read_latest_snapshot_manifest(store, key, max_bytes, SNAPSHOT_LABEL).await?
        else {
            return Ok(Self::new());
        };
        Self::from_manifest(store, key, &manifest, None, max_bytes).await
    }

    /// Reads the payloads `manifest` names and folds them into one index.
    ///
    /// `window`, when given, keeps one tenant and the shards whose span meets
    /// the range; everything else is listed in the manifest and then not read.
    #[instrument(
        level = "debug",
        skip_all,
        fields(key = %key, listed = tracing::field::Empty, read = tracing::field::Empty),
        err
    )]
    async fn from_manifest(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        manifest: &SnapshotManifest,
        window: Option<(&str, i64, i64)>,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let mut listed = 0_usize;
        let mut read = 0_usize;
        let mut index = Self::new();
        for tenant in manifest.tenants() {
            if window.is_some_and(|(wanted, _, _)| wanted != tenant) {
                continue;
            }
            let mut by_key: BTreeMap<String, TraceBlockStats> = BTreeMap::new();
            for shard in manifest.shards_of(tenant) {
                listed += 1;
                let range = shard.range();
                if window.is_some_and(|(_, min_ts, max_ts)| !range.overlaps(min_ts, max_ts)) {
                    continue;
                }
                let object_key = shard_payload_object_key(key, tenant, range, &shard.content);
                let bytes = read_shard_payload(store, &object_key, max_bytes).await?;
                read += 1;
                let (_, blocks) = decode_trace_shard(&object_key, &bytes)?;
                for block in blocks {
                    merge_record(&mut by_key, block);
                }
            }
            if by_key.is_empty() {
                continue;
            }
            let tenant_index = index.tenants.entry(tenant.clone()).or_default();
            tenant_index.blocks = by_key.into_values().collect();
            tenant_index.blocks.sort_by(|left, right| {
                left.min_ts
                    .cmp(&right.min_ts)
                    .then_with(|| left.max_ts.cmp(&right.max_ts))
                    .then_with(|| left.object_key.cmp(&right.object_key))
            });
        }
        tracing::Span::current().record("listed", listed);
        tracing::Span::current().record("read", read);
        Ok(index)
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
