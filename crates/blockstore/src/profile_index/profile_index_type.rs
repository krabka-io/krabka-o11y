use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, ByteSize, CompactionCandidate,
    DEFAULT_INDEX_SNAPSHOT_MAX, Deserialize, Index, IndexSnapshotBytes, IndexSnapshotRetain,
    LABEL_PROFILE_TYPE, LabelMatcher, Labels, ObjectStore, ObjectStoreExt, Path,
    PendingBlockAdditions, PendingBlockRemovals, PutPayload, Result, Serialize, SeriesFingerprint,
    TenantProfileExtras, instrument, latest_index_snapshot_path, level_above,
    profile_block_fingerprint, put_index_snapshot, read_index_snapshot_bytes,
};

/// How an oversized or unreadable profile-index snapshot names itself in errors.
const SNAPSHOT_LABEL: &str = "profile index snapshot";

/// Profile-specific index state over the reusable series postings index.
#[derive(Default, Serialize, Deserialize)]
pub struct ProfileIndex {
    pub(crate) series: Index,
    pub(crate) extras: BTreeMap<String, TenantProfileExtras>,
    pub(crate) block_partitions: BTreeMap<String, Vec<u64>>,
    /// Blocks this writer dropped and has yet to make durable. Not persisted:
    /// see [`PendingBlockRemovals`].
    #[serde(skip)]
    pending_removals: PendingBlockRemovals,
    /// Blocks this writer registered and has yet to make durable. Not
    /// persisted: see [`PendingBlockAdditions`].
    #[serde(skip)]
    pending_additions: PendingBlockAdditions,
}

impl ProfileIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        self.series.add_series(tenant, fp, labels);
        if let Some(profile_type) = labels.get(LABEL_PROFILE_TYPE) {
            self.extras
                .entry(tenant.to_string())
                .or_default()
                .profile_types
                .entry(profile_type.to_string())
                .or_default()
                .insert(fp);
        }
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
        let retired: Vec<(String, u64)> = self
            .all_blocks()
            .into_iter()
            .filter(|meta| meta.tenant == tenant && dropped.contains(meta.object_key.as_str()))
            .map(|meta| {
                let partitions = self.stacktrace_partitions(&meta.object_key);
                let fingerprint = profile_block_fingerprint(&meta, &partitions);
                (meta.object_key, fingerprint)
            })
            .collect();
        self.pending_removals.record(
            tenant,
            retired
                .iter()
                .map(|(key, fingerprint)| (key.as_str(), *fingerprint)),
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

    /// Every block's record fingerprint, grouped by tenant, as a snapshot
    /// merge needs it to tell a retired block from one written under the same
    /// key since.
    fn block_fingerprints_by_tenant(&self) -> BTreeMap<String, BTreeMap<String, u64>> {
        let mut by_tenant: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
        for meta in self.all_blocks() {
            let partitions = self.stacktrace_partitions(&meta.object_key);
            let fingerprint = profile_block_fingerprint(&meta, &partitions);
            by_tenant
                .entry(meta.tenant.clone())
                .or_default()
                .insert(meta.object_key, fingerprint);
        }
        by_tenant
    }

    #[instrument(skip_all, fields(key = %key, len = tracing::field::Empty), err)]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        tracing::Span::current().record("len", bytes.len());
        store.put(&Path::from(key), PutPayload::from(bytes)).await?;
        Ok(())
    }

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
        let removals = self.pending_removals.pending();
        let additions = self.pending_additions.pending();
        let snapshot_key = put_index_snapshot(
            store,
            key,
            retain,
            DEFAULT_INDEX_SNAPSHOT_MAX,
            SNAPSHOT_LABEL,
            |base| self.merged_snapshot_bytes(base, &removals, &additions),
        )
        .await?;
        self.pending_removals.commit(&removals);
        self.pending_additions.commit(&additions);
        Ok(snapshot_key)
    }

    /// Folds this index into the snapshot `base` and serialises the result.
    ///
    /// Series, postings and profile types are grow-only, so their union is
    /// their join. Blocks and their stacktrace partitions are another matter:
    /// the base is the state of the whole system, and this writer contributes
    /// only what it alone knows. That is `additions`, the blocks it has
    /// registered since its last successful write, plus a replay of
    /// `removals`.
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
    fn merged_snapshot_bytes(
        &self,
        base: Option<&[u8]>,
        removals: &BTreeMap<String, BTreeMap<String, u64>>,
        additions: &BTreeSet<String>,
    ) -> Result<Vec<u8>> {
        let (mut merged, contribute_all) = match base {
            Some(bytes) => (Self::from_snapshot_bytes(bytes)?, false),
            None => (Self::new(), true),
        };
        // Read before the union folds this writer's blocks in: a block this
        // writer has published that the base no longer names was retired by
        // somebody else, and must not come back.
        let base_blocks = merged
            .all_blocks()
            .into_iter()
            .map(|meta| {
                let partitions = merged.stacktrace_partitions(&meta.object_key);
                (meta.object_key.clone(), (meta, partitions))
            })
            .collect::<BTreeMap<_, _>>();
        let stale: BTreeSet<String> = if contribute_all {
            BTreeSet::new()
        } else {
            self.all_blocks()
                .into_iter()
                .filter(|meta| {
                    if additions.contains(&meta.object_key) {
                        return false;
                    }
                    let partitions = self.stacktrace_partitions(&meta.object_key);
                    let fingerprint = profile_block_fingerprint(meta, &partitions);
                    base_blocks
                        .get(&meta.object_key)
                        .map(|(base_meta, base_partitions)| {
                            profile_block_fingerprint(base_meta, base_partitions) != fingerprint
                        })
                        .unwrap_or(true)
                })
                .map(|meta| meta.object_key)
                .collect()
        };
        merged.series.merge_from(&self.series);
        for (tenant, extras) in &self.extras {
            let target = merged.extras.entry(tenant.clone()).or_default();
            for (profile_type, fingerprints) in &extras.profile_types {
                target
                    .profile_types
                    .entry(profile_type.clone())
                    .or_default()
                    .extend(fingerprints.iter().copied());
            }
        }
        for (object_key, partitions) in &self.block_partitions {
            if stale.contains(object_key) {
                continue;
            }
            merged
                .block_partitions
                .insert(object_key.clone(), partitions.clone());
        }
        // `Index::merge_from` takes the other side's record for an equal key,
        // so restore the base's newer record when the key was reused. A key
        // absent from the base was retired and stays absent. Only block
        // records change: the series postings behind them are grow-only.
        if !stale.is_empty() {
            let mut stale_by_tenant: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for meta in self.all_blocks() {
                if stale.contains(&meta.object_key) {
                    stale_by_tenant
                        .entry(meta.tenant)
                        .or_default()
                        .push(meta.object_key);
                }
            }
            for (tenant, keys) in stale_by_tenant {
                let replacements = keys
                    .iter()
                    .filter_map(|key| base_blocks.get(key).map(|(meta, _)| meta.clone()))
                    .collect::<Vec<_>>();
                merged.series.replace_blocks(&tenant, &keys, &replacements);
                for object_key in &keys {
                    if let Some((_, partitions)) = base_blocks.get(object_key) {
                        merged
                            .block_partitions
                            .insert(object_key.clone(), partitions.clone());
                    } else {
                        merged.block_partitions.remove(object_key);
                    }
                }
            }
        }
        let live_fingerprints = merged.block_fingerprints_by_tenant();
        for (tenant, removed) in removals {
            // Only the record the removal pinned itself to is dropped. A block
            // written under the same key since is a different block, and
            // dropping it would hide an object nothing else names.
            let live = live_fingerprints.get(tenant);
            let removed_keys: Vec<String> = removed
                .iter()
                .filter(|(object_key, fingerprint)| {
                    live.and_then(|live| live.get(*object_key)) == Some(*fingerprint)
                })
                .map(|(object_key, _)| object_key.clone())
                .collect();
            merged.series.replace_blocks(tenant, &removed_keys, &[]);
            for object_key in &removed_keys {
                merged.block_partitions.remove(object_key);
            }
        }
        Ok(serde_json::to_vec(&merged)?)
    }

    /// Parses a stored snapshot.
    fn from_snapshot_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes)?)
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load(store: &Arc<dyn ObjectStore>, key: &str) -> Result<Self> {
        Self::load_with_max_bytes(store, key, DEFAULT_INDEX_SNAPSHOT_MAX).await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        Self::load_path_with_max_bytes(store, &Path::from(key), max_bytes).await
    }

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
        if let Some(path) = latest_index_snapshot_path(store, key).await? {
            return Self::load_path_with_max_bytes(store, &path, max_bytes).await;
        }
        Self::load_with_max_bytes(store, key, max_bytes).await
    }

    /// Loads the newest snapshot, returning an empty index only when neither a
    /// versioned snapshot nor the legacy index object exists.
    ///
    /// # Errors
    /// Returns an error when listing or reading object storage fails, or when
    /// persisted metadata is malformed.
    pub async fn load_latest_snapshot_or_empty_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        if let Some(path) = latest_index_snapshot_path(store, key).await? {
            return Self::load_path_with_max_bytes(store, &path, max_bytes).await;
        }
        let path = Path::from(key);
        match store.head(&path).await {
            Ok(_) => Self::load_path_with_max_bytes(store, &path, max_bytes).await,
            Err(object_store::Error::NotFound { .. }) => Ok(Self::new()),
            Err(error) => Err(error.into()),
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path),
        err
    )]
    pub(crate) async fn load_path_with_max_bytes(
        store: &Arc<dyn ObjectStore>,
        path: &Path,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        match read_index_snapshot_bytes(store, path, max_bytes, SNAPSHOT_LABEL).await? {
            IndexSnapshotBytes::Present(bytes) => Self::from_snapshot_bytes(&bytes),
            IndexSnapshotBytes::Absent(missing) => Err(missing),
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
