use super::{
    BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, CompactionCandidate,
    Index, IndexShardRange, LABEL_PROFILE_TYPE, LabelMatcher, Labels, PROFILE_INDEX_SHARD_WIDTH,
    PendingBlockAdditions, PendingBlockRemovals, PendingRemoval, ProfileShard, Result,
    SeriesFingerprint, TenantProfileExtras, TenantShardMerge, UNBOUNDED_SHARD_RANGE,
    decode_profile_shard, encode_profile_shard, level_above, profile_block_fingerprint,
    render_series_labels, shard_ranges_for_span, touched_shard_ranges,
};

/// How an oversized or unreadable profile-index snapshot names itself in errors.
const SNAPSHOT_LABEL: &str = "profile index snapshot";

/// Profile-specific index state over the reusable series postings index.
///
/// # On disk
///
/// A published profile index is not one object. Each tenant's blocks are cut
/// onto a time grid a day wide, and each slot is a
/// payload object of its own: the shared index's own shard encoding for the
/// blocks, the series their postings reach and the postings themselves, plus
/// the stacktrace partitions of those blocks. A payload is named by the hash
/// of its own bytes, so it is immutable, and the object a generation swaps is
/// a small manifest listing `(tenant, span, content hash)`.
///
/// A flush therefore writes the payloads of the shards its own blocks fall in,
/// plus the manifest, and names every other shard by the key the previous
/// generation already used. A reader that knows its time range fetches only
/// the payloads that meet it: see
/// [`ProfileIndex::load_latest_snapshot_for_range_with_max_bytes`].
#[derive(Default)]
pub struct ProfileIndex {
    pub(crate) series: Index,
    pub(crate) extras: BTreeMap<String, TenantProfileExtras>,
    pub(crate) block_partitions: BTreeMap<String, Vec<u64>>,
    /// Blocks this writer dropped and has yet to make durable. Not persisted:
    /// see [`PendingBlockRemovals`].
    pending_removals: PendingBlockRemovals,
    /// Blocks this writer registered and has yet to make durable. Not
    /// persisted: see [`PendingBlockAdditions`].
    pending_additions: PendingBlockAdditions,
}

impl ProfileIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `labels` as a series of `tenant`, under the profile type its
    /// [`LABEL_PROFILE_TYPE`] label names.
    ///
    /// The label is required, and a series without it is refused rather than
    /// half-registered. Every query the profile API serves selects on a
    /// profile type first — a Pyroscope `query` is a type plus a matcher set —
    /// and that selection is resolved through the postings this call builds.
    /// The postings are not published either: a load replays them from the
    /// same label (see [`Self::load_latest_snapshot`]), so a series admitted
    /// without it has no type to recover, not merely none registered.
    ///
    /// Nothing legitimate arrives without the label. Every profile reaches the
    /// WAL through the ingest split, which stamps `__profile_type__` on each
    /// series it emits after relabelling has run, so no relabel rule can strip
    /// it. Accepting such a series would store a profile that queries empty
    /// and reports nothing at either end, which is the one outcome worth
    /// refusing.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::MissingProfileTypeLabel`], naming the label
    /// and the series, when `labels` does not carry [`LABEL_PROFILE_TYPE`].
    pub fn add_series(
        &mut self,
        tenant: &str,
        fp: SeriesFingerprint,
        labels: &Labels,
    ) -> Result<()> {
        let Some(profile_type) = labels.get(LABEL_PROFILE_TYPE) else {
            return Err(BlockStoreError::MissingProfileTypeLabel {
                label: LABEL_PROFILE_TYPE,
                tenant: tenant.to_string(),
                fingerprint: fp,
                labels: render_series_labels(labels),
            });
        };
        let profile_type = profile_type.to_string();
        self.series.add_series(tenant, fp, labels);
        self.extras
            .entry(tenant.to_string())
            .or_default()
            .profile_types
            .entry(profile_type)
            .or_default()
            .insert(fp);
        Ok(())
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.resolve(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        self.series.matching_fingerprints(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn select_fingerprints(
        &self,
        tenant: &str,
        profile_type: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let profile_fps = self.fingerprints_for_profile_type(tenant, profile_type);
        if matchers.is_empty() {
            return Ok(profile_fps);
        }
        let label_fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(profile_fps.intersection(&label_fps).copied().collect())
    }

    #[must_use]
    pub fn candidate_blocks_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.series
            .candidate_blocks_for_series(tenant, fps, min_ts, max_ts)
    }

    #[must_use]
    pub fn candidate_block_metas_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<BlockMeta> {
        let keys: BTreeSet<_> = self
            .candidate_blocks_for_series(tenant, fps, min_ts, max_ts)
            .into_iter()
            .collect();
        self.series
            .all_blocks(tenant)
            .into_iter()
            .filter(|block| keys.contains(&block.object_key))
            .collect()
    }

    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        self.series.block_time_bounds(tenant, min_ts, max_ts)
    }

    #[must_use]
    pub fn profile_types(&self, tenant: &str) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| extras.profile_types.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn profile_types_for_time(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| {
                extras
                    .profile_types
                    .iter()
                    .filter(|(_, fps)| {
                        !self
                            .candidate_blocks_for_series(tenant, fps, min_ts, max_ts)
                            .is_empty()
                    })
                    .map(|(profile_type, _)| profile_type.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_values_for_time(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self
            .series
            .label_values_for_fingerprints(tenant, name, &active))
    }

    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.series.label_values_for_fingerprints(tenant, name, fps)
    }

    #[must_use]
    pub fn profile_types_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.extras
            .get(tenant)
            .map(|extras| {
                extras
                    .profile_types
                    .iter()
                    .filter(|(_, type_fps)| !type_fps.is_disjoint(fps))
                    .map(|(profile_type, _)| profile_type.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_names_for_time(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self.series.label_names_for_fingerprints(tenant, &active))
    }

    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        self.series.label_names_for_fingerprints(tenant, fps)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series_for_time(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Vec<Vec<(String, String)>>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        let active = self.active_fingerprints_for_time(tenant, &fps, min_ts, max_ts);
        Ok(self
            .series
            .series_for_fingerprints(tenant, &active, label_names))
    }

    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        self.series
            .series_for_fingerprints(tenant, fps, label_names)
    }

    #[must_use]
    pub fn fingerprints_for_profile_type(
        &self,
        tenant: &str,
        profile_type: &str,
    ) -> BTreeSet<SeriesFingerprint> {
        self.extras
            .get(tenant)
            .and_then(|extras| extras.profile_types.get(profile_type))
            .cloned()
            .unwrap_or_default()
    }

    pub(crate) fn active_fingerprints_for_time(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> BTreeSet<SeriesFingerprint> {
        fps.iter()
            .copied()
            .filter(|fp| {
                !self
                    .candidate_blocks_for_series(tenant, &BTreeSet::from([*fp]), min_ts, max_ts)
                    .is_empty()
            })
            .collect()
    }

    pub fn add_profile_block(&mut self, _tenant: &str, object_key: &str, partitions: Vec<u64>) {
        self.pending_additions.record(object_key);
        self.block_partitions
            .insert(object_key.to_string(), partitions);
    }

    /// How many rounds of compaction produced `object_key`, or
    /// [`BlockLevel::INGESTED`] for a block the index does not hold.
    #[must_use]
    pub fn block_level(&self, object_key: &str) -> BlockLevel {
        self.series
            .block_level(object_key)
            .unwrap_or(BlockLevel::INGESTED)
    }

    /// Every block in the index, as the compaction planner sees it.
    ///
    /// Time bounds, row count and level all come from the block records the
    /// series postings hold; nothing about a block is kept anywhere else.
    #[must_use]
    pub fn compaction_candidates(&self) -> Vec<CompactionCandidate> {
        let mut candidates: Vec<CompactionCandidate> = self
            .all_blocks()
            .into_iter()
            .map(|meta| CompactionCandidate {
                level: meta.level,
                tenant: meta.tenant,
                object_key: meta.object_key,
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                row_count: meta.row_count,
            })
            .collect();
        candidates.sort_by(|left, right| {
            left.tenant
                .cmp(&right.tenant)
                .then_with(|| left.object_key.cmp(&right.object_key))
        });
        candidates
    }

    /// Swaps the `remove_keys` blocks for `add`, and returns the level the
    /// added blocks were stamped with.
    ///
    /// The level is not a parameter: it is one rung above the highest of the
    /// blocks being retired, and those are named here already because naming
    /// them is what retires them. A compactor therefore cannot register its
    /// output at a level that contradicts its inputs, and cannot register it
    /// without saying what they were.
    pub fn replace_profile_blocks(
        &mut self,
        tenant: &str,
        remove_keys: &[String],
        add: &[(BlockMeta, Vec<u64>)],
    ) -> BlockLevel {
        let dropped: BTreeSet<&str> = remove_keys.iter().map(String::as_str).collect();
        self.retire_profile_blocks(tenant, &dropped);
        // A compaction may reuse the key of a block it replaces. That block is
        // live again, so it must not be replayed as a removal.
        for (meta, _) in add {
            self.pending_removals.forget(tenant, &meta.object_key);
        }
        for key in remove_keys {
            self.block_partitions.remove(key);
        }
        // Read before the inputs are dropped, and never below what an added
        // key already sits at: a snapshot merge re-registers blocks whose
        // inputs are long gone, and re-deriving from nothing would demote a
        // compacted block back to level zero every save.
        let mut level = level_above(remove_keys.iter().map(|key| self.block_level(key)));
        for (meta, _) in add {
            level = level.max(self.block_level(&meta.object_key));
        }
        let metas = add
            .iter()
            .map(|(meta, _)| BlockMeta {
                level,
                ..meta.clone()
            })
            .collect::<Vec<_>>();
        self.series.replace_blocks(tenant, remove_keys, &metas);
        for (meta, partitions) in add {
            self.add_profile_block(tenant, &meta.object_key, partitions.clone());
        }
        level
    }

    /// Drops the `keys` blocks of `tenant`, and returns how many it dropped.
    ///
    /// This is retention's swap, where [`Self::replace_profile_blocks`] is
    /// compaction's: the blocks leave the index and nothing takes their place,
    /// so no level is derived and nothing is promoted. The stacktrace
    /// partitions of each dropped block go with it, because nothing names them
    /// once the block record is gone.
    ///
    /// Each dropped block is pinned to the record it dropped, so the removal
    /// survives the merge that publishes the next generation. Without the pin
    /// the merge would union the block straight back in, and every expired
    /// block would come back on the next save. See
    /// [`Self::save_latest_snapshot`].
    pub fn remove_profile_blocks(&mut self, tenant: &str, keys: &[String]) -> usize {
        let dropped: BTreeSet<&str> = keys.iter().map(String::as_str).collect();
        // Pinned before anything is dropped: the records, and the partitions
        // that are part of them, are the pins.
        self.retire_profile_blocks(tenant, &dropped);
        for key in keys {
            self.block_partitions.remove(key);
        }
        self.series.remove_blocks(tenant, keys)
    }

    /// Records a pending removal for every `dropped` block this index holds
    /// for `tenant`, and forgets the additions those blocks were waiting on.
    ///
    /// The removals are pinned to the records being dropped, and so are read
    /// before they are. A removal that named only the object key would also
    /// drop a block another writer has since written under that key.
    fn retire_profile_blocks(&self, tenant: &str, dropped: &BTreeSet<&str>) {
        let retired: Vec<(String, PendingRemoval)> = self
            .all_blocks()
            .into_iter()
            .filter(|meta| meta.tenant == tenant && dropped.contains(meta.object_key.as_str()))
            .map(|meta| {
                let partitions = self.stacktrace_partitions(&meta.object_key);
                let removal = PendingRemoval {
                    fingerprint: profile_block_fingerprint(&meta, &partitions),
                    min_ts: meta.min_ts,
                    max_ts: meta.max_ts,
                };
                (meta.object_key, removal)
            })
            .collect();
        self.pending_removals.record(
            tenant,
            retired
                .iter()
                .map(|(key, removal)| (key.as_str(), *removal)),
        );
        for key in dropped {
            self.pending_additions.forget(key);
        }
    }

    #[must_use]
    pub fn stacktrace_partitions(&self, object_key: &str) -> Vec<u64> {
        self.block_partitions
            .get(object_key)
            .cloned()
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.series.label_names(tenant)
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.series.label_values(tenant, name)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        self.series.label_names_for(tenant, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        self.series.label_values_for(tenant, name, matchers)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        self.series.series_projected(tenant, matchers, label_names)
    }

    #[must_use]
    pub fn all_blocks(&self) -> Vec<BlockMeta> {
        self.series.all_blocks_unscoped()
    }
    crate::index_snapshot::snapshot_persistence_methods!(SNAPSHOT_LABEL);

    /// One tenant's part of `merged_manifest`.
    ///
    /// Series travel with the blocks that carry them, so a shard the merge did
    /// not touch keeps its own series and this writer contributes none of its
    /// own beyond the ones its contributed blocks reach. A series no block
    /// carries yet has no span, so it goes into the tenant's unbounded shard.
    ///
    /// Profile types are not published at all: they are a function of the
    /// `__profile_type__` label of the series, so a load replays them rather
    /// than reading them, exactly as the shared index does for its label
    /// postings.
    async fn merge_tenant_shards(&self, merge: &mut TenantShardMerge<'_>) -> Result<()> {
        let tenant = merge.tenant;
        let contributed: Vec<BlockMeta> = self
            .series
            .all_blocks(tenant)
            .into_iter()
            .filter(|meta| merge.contributes(&meta.object_key))
            .collect();
        let removed = merge.removed;
        let unbound = self.series.unbound_series(tenant);

        let mut touched = touched_shard_ranges(
            contributed
                .iter()
                .map(|meta| IndexShardRange::new(meta.min_ts, meta.max_ts)),
            removed,
            PROFILE_INDEX_SHARD_WIDTH,
        );
        if !unbound.is_empty() {
            touched.push(UNBOUNDED_SHARD_RANGE);
        }

        let mut shards: BTreeMap<IndexShardRange, ProfileShard> = BTreeMap::new();
        let carried = merge
            .read_touched(&touched, |range, object_key, bytes| {
                let (_, decoded) = decode_profile_shard(object_key, bytes)?;
                let held = shards.entry(range).or_default();
                held.index.merge_from(&decoded.index);
                held.partitions.extend(decoded.partitions);
                Ok(())
            })
            .await?;

        // Read before anything is applied: a removal whose pinned record
        // is not the one the base carries retires a block that is no
        // longer there, and publishing over it would either hide a live
        // block or double-count a retired one.
        for (object_key, removal) in removed.into_iter().flatten() {
            let unchanged = shards.values().any(|shard| {
                shard
                    .index
                    .all_blocks(tenant)
                    .into_iter()
                    .find(|meta| meta.object_key == *object_key)
                    .is_some_and(|meta| {
                        let partitions = shard
                            .partitions
                            .get(object_key)
                            .cloned()
                            .unwrap_or_default();
                        profile_block_fingerprint(&meta, &partitions) == removal.fingerprint
                    })
            });
            if !unchanged {
                return Err(BlockStoreError::InvalidBlock(format!(
                    "profile compaction input `{object_key}` changed before its replacement was published"
                )));
            }
        }

        for (object_key, removal) in removed.into_iter().flatten() {
            // Only the record the removal pinned itself to is dropped. A
            // block written under the same key since is a different block,
            // and dropping it would hide an object nothing else names.
            for shard in shards.values_mut() {
                let matches = shard
                    .index
                    .all_blocks(tenant)
                    .into_iter()
                    .find(|meta| meta.object_key == *object_key)
                    .is_some_and(|meta| {
                        let partitions = shard
                            .partitions
                            .get(object_key)
                            .cloned()
                            .unwrap_or_default();
                        profile_block_fingerprint(&meta, &partitions) == removal.fingerprint
                    });
                if matches {
                    shard
                        .index
                        .replace_blocks(tenant, std::slice::from_ref(object_key), &[]);
                    shard.partitions.remove(object_key);
                }
            }
        }

        for meta in contributed {
            let partitions = self.stacktrace_partitions(&meta.object_key);
            for shard in shards.values_mut() {
                shard
                    .index
                    .replace_blocks(tenant, std::slice::from_ref(&meta.object_key), &[]);
                shard.partitions.remove(&meta.object_key);
            }
            for range in shard_ranges_for_span(meta.min_ts, meta.max_ts, PROFILE_INDEX_SHARD_WIDTH)
            {
                let shard = shards.entry(range).or_default();
                for fingerprint in &meta.fingerprints {
                    if let Some(labels) = self.series.series_labels(tenant, *fingerprint) {
                        shard.index.add_series(tenant, *fingerprint, labels);
                    }
                }
                shard.index.add_block(&meta);
                shard
                    .partitions
                    .insert(meta.object_key.clone(), partitions.clone());
            }
        }

        if !unbound.is_empty() {
            let shard = shards.entry(UNBOUNDED_SHARD_RANGE).or_default();
            for (fingerprint, labels) in &unbound {
                shard.index.add_series(tenant, *fingerprint, labels);
            }
        }

        merge.carry(carried);
        for (range, shard) in shards {
            if shard.index.block_count(tenant) == 0 && shard.index.series_count(tenant) == 0 {
                // Nothing left in the slot, so the manifest stops naming
                // it and the sweep reclaims what it named.
                continue;
            }
            let bytes = encode_profile_shard(tenant, &shard);
            merge.publish(range, bytes).await?;
        }
        Ok(())
    }

    fn snapshot_tenants(&self) -> impl Iterator<Item = &str> {
        self.series.tenant_names().map(String::as_str)
    }

    /// Folds one shard payload into a load.
    fn load_shard(&mut self, _tenant: &str, object_key: &str, bytes: &[u8]) -> Result<()> {
        let (_, decoded) = decode_profile_shard(object_key, bytes)?;
        self.series.merge_from(&decoded.index);
        self.block_partitions.extend(decoded.partitions);
        Ok(())
    }

    fn finish_load(&mut self) {
        self.rebuild_profile_types();
        // A load publishes nothing: everything it read is already durable, so
        // the next merge owes the base none of it.
        self.pending_additions = PendingBlockAdditions::default();
    }

    /// Replays the `__profile_type__` postings from the series.
    ///
    /// The map is wholly derived from the series labels, which is why it is
    /// not published: storing it would be a second copy to keep in step, and
    /// the shared index does not store its label postings either.
    fn rebuild_profile_types(&mut self) {
        self.extras.clear();
        for tenant in self.series.tenant_names().cloned().collect::<Vec<_>>() {
            for (fingerprint, labels) in self.series.series_pairs(&tenant) {
                let Some(profile_type) = labels.get(LABEL_PROFILE_TYPE) else {
                    // `add_series` refuses a series without the label, so a
                    // published shard should hold none. If one does, it came
                    // from bytes this build did not write, and the series is
                    // about to become unqueryable by type: say so rather than
                    // drop it quietly.
                    tracing::warn!(
                        %tenant,
                        %fingerprint,
                        label = LABEL_PROFILE_TYPE,
                        series = %render_series_labels(&labels),
                        "profile series in a loaded shard has no profile-type label; \
                         no profile-type selector will reach it"
                    );
                    continue;
                };
                self.extras
                    .entry(tenant.clone())
                    .or_default()
                    .profile_types
                    .entry(profile_type.to_string())
                    .or_default()
                    .insert(fingerprint);
            }
        }
    }
}

impl BlockIndex for ProfileIndex {
    fn add_block(&mut self, meta: &BlockMeta) {
        self.pending_additions.record(&meta.object_key);
        BlockIndex::add_block(&mut self.series, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        BlockIndex::candidate_blocks(&self.series, tenant, min_ts, max_ts)
    }

    fn block_count(&self, tenant: &str) -> usize {
        BlockIndex::block_count(&self.series, tenant)
    }
}
