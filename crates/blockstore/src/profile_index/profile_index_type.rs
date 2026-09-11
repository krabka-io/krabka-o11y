use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, ByteSize,
    CompactionCandidate, DEFAULT_INDEX_SNAPSHOT_MAX, Index, IndexShardRange, IndexSnapshotRetain,
    LABEL_PROFILE_TYPE, LabelMatcher, Labels, ObjectStore, PROFILE_INDEX_SHARD_WIDTH,
    PendingBlockAdditions, PendingBlockRemovals, PendingRemoval, ProfileShard, Result,
    SeriesFingerprint, SnapshotManifest, TenantProfileExtras, UNBOUNDED_SHARD_RANGE,
    decode_profile_shard, encode_profile_shard, instrument, level_above, profile_block_fingerprint,
    put_manifest_snapshot, put_shard_payload, read_latest_snapshot_manifest, read_shard_payload,
    render_series_labels, shard_payload_content_hash, shard_payload_object_key,
    shard_ranges_for_span,
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
        // Pinned to the records being dropped, and so read before they are.
        // A removal that named only the key would also drop a block another
        // writer has since written under that key.
        let dropped: BTreeSet<&str> = remove_keys.iter().map(String::as_str).collect();
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
        for key in &dropped {
            self.pending_additions.forget(key);
        }
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
    /// Series travel with the blocks that carry them, so a shard the merge did
    /// not touch keeps its own series and this writer contributes none of its
    /// own beyond the ones its contributed blocks reach. A series no block
    /// carries yet has no span, so it goes into the tenant's unbounded shard.
    ///
    /// Profile types are not published at all: they are a function of the
    /// `__profile_type__` label of the series, so a load replays them rather
    /// than reading them, exactly as the shared index does for its label
    /// postings.
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
        tenants.extend(self.series.tenant_names().map(String::as_str));
        tenants.extend(removals.keys().map(String::as_str));

        let mut manifest = SnapshotManifest::new();
        for tenant in tenants {
            let contributed: Vec<BlockMeta> = self
                .series
                .all_blocks(tenant)
                .into_iter()
                .filter(|meta| contribute_all || additions.contains(&meta.object_key))
                .collect();
            let removed = removals.get(tenant);
            let unbound = self.series.unbound_series(tenant);

            // The spans a contribution or a removal reaches into. Every shard
            // that meets one of them has to be read, because the record it
            // replaces or retires can only be in one of those.
            let mut touched: Vec<IndexShardRange> = Vec::new();
            for meta in &contributed {
                touched.extend(shard_ranges_for_span(
                    meta.min_ts,
                    meta.max_ts,
                    PROFILE_INDEX_SHARD_WIDTH,
                ));
            }
            for removal in removed.into_iter().flat_map(BTreeMap::values) {
                touched.extend(shard_ranges_for_span(
                    removal.min_ts,
                    removal.max_ts,
                    PROFILE_INDEX_SHARD_WIDTH,
                ));
            }
            if !unbound.is_empty() {
                touched.push(UNBOUNDED_SHARD_RANGE);
            }

            let mut shards: BTreeMap<IndexShardRange, ProfileShard> = BTreeMap::new();
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
                    let (_, decoded) = decode_profile_shard(&object_key, &bytes)?;
                    let held = shards.entry(range).or_default();
                    held.index.merge_from(&decoded.index);
                    held.partitions.extend(decoded.partitions);
                } else {
                    carried.insert(range, shard.content.clone());
                }
            }

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
                for range in
                    shard_ranges_for_span(meta.min_ts, meta.max_ts, PROFILE_INDEX_SHARD_WIDTH)
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

            for (range, content) in carried {
                manifest.insert(tenant, range, content);
            }
            for (range, shard) in shards {
                if shard.index.block_count(tenant) == 0 && shard.index.series_count(tenant) == 0 {
                    // Nothing left in the slot, so the manifest stops naming
                    // it and the sweep reclaims what it named.
                    continue;
                }
                let bytes = encode_profile_shard(tenant, &shard);
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
            for shard in manifest.shards_of(tenant) {
                listed += 1;
                let range = shard.range();
                if window.is_some_and(|(_, min_ts, max_ts)| !range.overlaps(min_ts, max_ts)) {
                    continue;
                }
                let object_key = shard_payload_object_key(key, tenant, range, &shard.content);
                let bytes = read_shard_payload(store, &object_key, max_bytes).await?;
                read += 1;
                let (_, decoded) = decode_profile_shard(&object_key, &bytes)?;
                index.series.merge_from(&decoded.index);
                index.block_partitions.extend(decoded.partitions);
            }
        }
        index.rebuild_profile_types();
        // A load publishes nothing: everything it read is already durable, so
        // the next merge owes the base none of it.
        index.pending_additions = PendingBlockAdditions::default();
        tracing::Span::current().record("listed", listed);
        tracing::Span::current().record("read", read);
        Ok(index)
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
