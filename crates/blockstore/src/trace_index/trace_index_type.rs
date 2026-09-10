use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockLineage, BlockLineageIndex, BlockMeta,
    BlockStoreError, ByteSize, CompactionCandidate, DEFAULT_INDEX_SNAPSHOT_MAX, Deserialize,
    HashMap, IndexSnapshotBytes, IndexSnapshotRetain, ObjectStore, ObjectStoreExt, Path,
    PendingBlockRemovals, PutPayload, Result, Serialize, ShardedTraceBloom, TenantTraceIndex,
    TraceBlockStats, instrument, latest_index_snapshot_path, put_index_snapshot,
    read_index_snapshot_bytes,
};

/// How an oversized or unreadable trace-index snapshot names itself in errors.
const SNAPSHOT_LABEL: &str = "trace index snapshot";

/// Trace block index.
#[derive(Default, Serialize, Deserialize)]
pub struct TraceIndex {
    pub(crate) tenants: HashMap<String, TenantTraceIndex>,
    /// Compaction level and lineage per block. Kept beside the block records
    /// rather than inside [`TraceBlockStats`], which describes what a block
    /// contains rather than where it came from.
    pub(crate) lineage: BlockLineageIndex,
    /// Blocks this writer dropped and has yet to make durable. Not persisted:
    /// see [`PendingBlockRemovals`].
    #[serde(skip)]
    pending_removals: PendingBlockRemovals,
}

impl TraceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats) {
        self.add_trace_block_with_rows(tenant, stats, 0);
    }

    /// Registers a freshly built block along with its row count.
    ///
    /// The row count is what lets the compaction planner tell a block that has
    /// reached its size target, and so needs no further merging, from one that
    /// has not. A caller that does not have it uses [`Self::add_trace_block`].
    pub fn add_trace_block_with_rows(
        &mut self,
        tenant: &str,
        stats: TraceBlockStats,
        row_count: usize,
    ) {
        self.pending_removals.forget(tenant, &stats.object_key);
        self.lineage.record_ingested(&stats.object_key, row_count);
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        tenant_index
            .blocks
            .retain(|block| block.object_key != stats.object_key);
        tenant_index.blocks.push(stats);
    }

    /// How many rounds of compaction produced `object_key`.
    #[must_use]
    pub fn block_level(&self, object_key: &str) -> BlockLevel {
        self.lineage.level(object_key)
    }

    /// The level, row count and immediate inputs recorded for `object_key`.
    #[must_use]
    pub fn block_lineage(&self, object_key: &str) -> Option<&BlockLineage> {
        self.lineage.lineage(object_key)
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
                        row_count: self.lineage.row_count(&block.object_key),
                        level: self.lineage.level(&block.object_key),
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

    pub fn replace_trace_blocks(
        &mut self,
        tenant: &str,
        old_keys: &[String],
        mut replacement: TraceBlockStats,
        row_count: usize,
    ) {
        // Recorded before the inputs are forgotten, because the replacement's
        // level is one above the highest of theirs.
        self.lineage
            .record_compacted(&replacement.object_key, old_keys, row_count);
        let old_keys: BTreeSet<&str> = old_keys.iter().map(String::as_str).collect();
        self.lineage.forget(
            old_keys
                .iter()
                .copied()
                .filter(|key| *key != replacement.object_key.as_str()),
        );
        self.pending_removals
            .record(tenant, old_keys.iter().copied());
        // A compaction may reuse the key of a block it replaces. That block is
        // live again, so it must not be replayed as a removal.
        self.pending_removals
            .forget(tenant, &replacement.object_key);
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
        let snapshot_key = put_index_snapshot(
            store,
            key,
            retain,
            DEFAULT_INDEX_SNAPSHOT_MAX,
            SNAPSHOT_LABEL,
            |base| self.merged_snapshot_bytes(base, &removals),
        )
        .await?;
        self.pending_removals.commit(&removals);
        Ok(snapshot_key)
    }

    /// Folds this index into the snapshot `base` and serialises the result.
    ///
    /// The merge is a union keyed by object key, with this index winning, plus
    /// a replay of `removals`. Union alone would lose nothing this writer owns
    /// but would resurrect every block it compacted away, because the base
    /// still names them.
    fn merged_snapshot_bytes(
        &self,
        base: Option<&[u8]>,
        removals: &BTreeMap<String, BTreeSet<String>>,
    ) -> Result<Vec<u8>> {
        let mut merged = match base {
            Some(bytes) => Self::from_snapshot_bytes(bytes)?,
            None => Self::new(),
        };
        for (tenant, tenant_index) in &self.tenants {
            for block in &tenant_index.blocks {
                merged.add_trace_block(tenant, block.clone());
            }
        }
        // This writer's view of a block's level wins over the base's, and the
        // union above must not leave a compacted block reading as level zero.
        merged.lineage.merge_from(&self.lineage);
        for (tenant, removed) in removals {
            if let Some(tenant_index) = merged.tenants.get_mut(tenant) {
                tenant_index
                    .blocks
                    .retain(|block| !removed.contains(&block.object_key));
            }
        }
        // Lineage outlives nothing: a record for a block no longer in the
        // index would make the snapshot grow once per block ever written.
        let live: BTreeSet<String> = merged
            .tenants
            .values()
            .flat_map(|tenant_index| tenant_index.blocks.iter())
            .map(|block| block.object_key.clone())
            .collect();
        merged.lineage.retain_keys(&live);
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
