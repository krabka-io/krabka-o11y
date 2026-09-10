use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, ByteSize,
    CompactionCandidate, DEFAULT_INDEX_SNAPSHOT_MAX, Deserialize, HashMap, IndexSnapshotBytes,
    IndexSnapshotRetain, ObjectStore, ObjectStoreExt, Path, PendingBlockAdditions,
    PendingBlockRemovals, PutPayload, Result, Serialize, ShardedTraceBloom, TenantTraceIndex,
    TraceBlockStats, instrument, latest_index_snapshot_path, level_above, put_index_snapshot,
    read_index_snapshot_bytes, trace_block_fingerprint,
};

/// How an oversized or unreadable trace-index snapshot names itself in errors.
const SNAPSHOT_LABEL: &str = "trace index snapshot";

/// Trace block index.
#[derive(Default, Serialize, Deserialize)]
pub struct TraceIndex {
    pub(crate) tenants: HashMap<String, TenantTraceIndex>,
    /// Blocks this writer dropped and has yet to make durable. Not persisted:
    /// see [`PendingBlockRemovals`].
    #[serde(skip)]
    pending_removals: PendingBlockRemovals,
    /// Blocks this writer registered and has yet to make durable. Not
    /// persisted: see [`PendingBlockAdditions`].
    #[serde(skip)]
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
        let retired: Vec<(&str, u64)> = self
            .tenants
            .get(tenant)
            .into_iter()
            .flat_map(|tenant_index| tenant_index.blocks.iter())
            .filter(|block| old_keys.contains(block.object_key.as_str()))
            .map(|block| (block.object_key.as_str(), trace_block_fingerprint(block)))
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
        for (tenant, removed) in removals {
            let live = merged.tenants.get(tenant);
            for (object_key, fingerprint) in removed {
                let unchanged = live
                    .and_then(|tenant_index| {
                        tenant_index
                            .blocks
                            .iter()
                            .find(|block| block.object_key == *object_key)
                    })
                    .is_some_and(|block| trace_block_fingerprint(block) == *fingerprint);
                if !unchanged {
                    return Err(BlockStoreError::InvalidBlock(format!(
                        "trace compaction input `{object_key}` changed before its replacement was published"
                    )));
                }
            }
        }
        for (tenant, tenant_index) in &self.tenants {
            for block in &tenant_index.blocks {
                if contribute_all || additions.contains(&block.object_key) {
                    merged.add_trace_block(tenant, block.clone());
                }
            }
        }
        for (tenant, removed) in removals {
            if let Some(tenant_index) = merged.tenants.get_mut(tenant) {
                // Only the record the removal pinned itself to is dropped. A
                // block written under the same key since is a different block,
                // and dropping it would hide an object nothing else names.
                tenant_index.blocks.retain(|block| {
                    removed
                        .get(&block.object_key)
                        .is_none_or(|fingerprint| *fingerprint != trace_block_fingerprint(block))
                });
            }
        }
        Ok(serde_json::to_vec(&merged)?)
    }

    /// Parses and validates a stored snapshot.
    fn from_snapshot_bytes(bytes: &[u8]) -> Result<Self> {
        let index: Self = serde_json::from_slice(bytes)?;
        // `Deserialize` bypasses the bloom constructors' invariant checks, so a
        // structurally-valid-but-corrupt snapshot would panic on the first
        // lookup. Validate here so it surfaces as an error instead.
        index.validate()?;
        Ok(index)
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

    #[instrument(level = "debug", skip_all, fields(path = %path), err)]
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

    pub(crate) fn validate(&self) -> Result<()> {
        for tenant_index in self.tenants.values() {
            for block in &tenant_index.blocks {
                block.bloom.validate().map_err(|e| {
                    BlockStoreError::Serde(format!(
                        "corrupt trace bloom for block `{}`: {e}",
                        block.object_key
                    ))
                })?;
            }
        }
        Ok(())
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
