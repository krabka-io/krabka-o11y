use super::{
    Arc, BTreeMap, BTreeSet, BlockIndex, BlockLevel, BlockMeta, BlockStoreError, ByteSize,
    DEFAULT_INDEX_SHARD_WIDTH, Deserialize, LabelMatcher, Labels, MAX_INDEX_SNAPSHOT_BYTES,
    ObjectStore, QUERY_SHARD_LABEL, Result, Serialize, SeriesFingerprint, TenantIndex,
    load_index_shards, matcher_matches_empty, save_index_shards,
};

/// Multi-tenant in-memory index for label resolution and block pruning.
///
/// This is the metrics and logs series index. The profiles index and the
/// traces index embed it for shared label posting and matcher resolution.
///
/// # On disk
///
/// An index is not one object. Each tenant's blocks are cut onto a time grid
/// and each grid slot is a shard object of its own, carrying the blocks that
/// cross it, the series those blocks hold, and the block-to-series pairs. It
/// holds nothing about any other slot. A query over an hour therefore lists a
/// tenant's prefix, keeps the one or two shards whose span meets the hour, and
/// reads only those; it never holds the rest of the tenant, let alone the rest
/// of the fleet. This is the layout the logs path already uses, in
/// [`crate::log_blockstore`], reached for here for the same reason and kept
/// deliberately close to it.
///
/// The price of the shape is that a series is written into every shard whose
/// blocks carry it. That duplication is the mechanism, not an oversight: a
/// single shared series dictionary would be one object every querier has to
/// load whatever it asked for, which is the resident-memory problem this
/// layout exists to solve.
///
/// The bytes are a compact binary encoding rather than JSON: a per-shard
/// dictionary for label names and values, varints for counts and ordinals,
/// deltas for timestamps, and no storage at all for the label postings and
/// label-value sets, which are functions of the series and are rebuilt on
/// load.
///
/// [`Index::save`] republishes every shard and sweeps the ones the new layout
/// does not name. It is not a compare-and-swap: the generation-numbered
/// publication in [`crate::index_snapshot`] swaps a single object, and a
/// many-object index needs a manifest for it to swap instead.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Index {
    pub(crate) tenants: BTreeMap<String, TenantIndex>,
}

impl Index {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_series(&mut self, tenant: &str, fp: SeriesFingerprint, labels: &Labels) {
        self.tenants
            .entry(tenant.to_string())
            .or_default()
            .add_series(fp, labels);
    }

    pub fn add_block(&mut self, meta: &BlockMeta) {
        self.tenants
            .entry(meta.tenant.clone())
            .or_default()
            .blocks
            .insert(meta);
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn resolve(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        if matchers.is_empty() {
            return Err(BlockStoreError::InvalidBlock(
                "at least one label matcher is required".into(),
            ));
        }

        // Prometheus rejects vector selectors in which every matcher matches the
        // empty string (e.g. `{foo!="bar"}`): such a selector restricts nothing
        // and forces an O(total-series) full tenant scan. Require at least one
        // matcher that cannot match the empty string. The synthetic
        // `__query_shard__` matcher is internal-only and never restricts the
        // candidate set to a posting, so it does not satisfy this requirement.
        let mut has_non_empty_matcher = false;
        for matcher in matchers {
            if matcher.name != QUERY_SHARD_LABEL && !matcher_matches_empty(matcher)? {
                has_non_empty_matcher = true;
                break;
            }
        }
        if !has_non_empty_matcher {
            return Err(BlockStoreError::InvalidBlock(
                "vector selector must contain at least one non-empty matcher".to_string(),
            ));
        }

        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };

        let mut resolved = tenant_index.resolve_one(&matchers[0])?;
        for matcher in &matchers[1..] {
            let matched = tenant_index.resolve_one(matcher)?;
            resolved = resolved.intersection(&matched).copied().collect();
            if resolved.is_empty() {
                break;
            }
        }

        Ok(resolved)
    }

    #[must_use]
    pub fn candidate_blocks(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };

        tenant_index.blocks.candidate_blocks(fps, min_ts, max_ts)
    }

    #[must_use]
    pub fn all_blocks(&self, tenant: &str) -> Vec<BlockMeta> {
        self.tenants
            .get(tenant)
            .map(|tenant_index| tenant_index.blocks.metas(tenant))
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_names(&self, tenant: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .map(|tenant_index| tenant_index.values.keys().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn label_values(&self, tenant: &str, name: &str) -> Vec<String> {
        self.tenants
            .get(tenant)
            .and_then(|tenant_index| tenant_index.values.get(name))
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Full label sets for the series that match `matchers`. An empty
    /// `matchers` selects every series.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<Labels>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(Vec::new());
        };

        let fingerprints = if matchers.is_empty() {
            tenant_index.all_fingerprints()
        } else {
            self.resolve(tenant, matchers)?
        };
        Ok(fingerprints
            .into_iter()
            .filter_map(|fp| tenant_index.series.get(&fp).cloned())
            .collect())
    }

    /// Resolves matchers to fingerprints. An empty matcher set means "all
    /// fingerprints in the tenant". [`Index::resolve`] differs here and
    /// rejects empty matchers.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn matching_fingerprints(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
    ) -> Result<BTreeSet<SeriesFingerprint>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Ok(BTreeSet::new());
        };
        if matchers.is_empty() {
            return Ok(tenant_index.all_fingerprints());
        }
        self.resolve(tenant, matchers)
    }

    /// Distinct label names carried by the series that match `matchers`.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_names_for(&self, tenant: &str, matchers: &[LabelMatcher]) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_names_for_fingerprints(tenant, &fps))
    }

    /// Distinct label names carried by the given fingerprints.
    #[must_use]
    pub fn label_names_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut names = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = tenant_index.series.get(fp) {
                names.extend(labels.iter().map(|(name, _)| name.clone()));
            }
        }
        names.into_iter().collect()
    }

    /// Distinct values for `name` across the series that match `matchers`.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn label_values_for(
        &self,
        tenant: &str,
        name: &str,
        matchers: &[LabelMatcher],
    ) -> Result<Vec<String>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.label_values_for_fingerprints(tenant, name, &fps))
    }

    /// Distinct values for `name` across the given fingerprints.
    #[must_use]
    pub fn label_values_for_fingerprints(
        &self,
        tenant: &str,
        name: &str,
        fps: &BTreeSet<SeriesFingerprint>,
    ) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut values = BTreeSet::new();
        for fp in fps {
            if let Some(labels) = tenant_index.series.get(fp)
                && let Some(value) = labels.get(name)
            {
                values.insert(value.to_string());
            }
        }
        values.into_iter().collect()
    }

    /// Projects the series that match `matchers` onto `label_names`.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn series_projected(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        label_names: &[String],
    ) -> Result<Vec<Vec<(String, String)>>> {
        let fps = self.matching_fingerprints(tenant, matchers)?;
        Ok(self.series_for_fingerprints(tenant, &fps, label_names))
    }

    /// Projects the given fingerprints onto `label_names`.
    #[must_use]
    pub fn series_for_fingerprints(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        label_names: &[String],
    ) -> Vec<Vec<(String, String)>> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        let mut out = BTreeSet::new();
        for fp in fps {
            let Some(labels) = tenant_index.series.get(fp) else {
                continue;
            };
            // An empty `label_names` means "return the full label set" (the
            // Prometheus/Loki/Pyroscope `/series` convention). Projecting onto an
            // empty name list previously yielded one empty label set (`[{}]`),
            // which broke Grafana's Pyroscope label autocomplete.
            let mut projected = if label_names.is_empty() {
                labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect::<Vec<_>>()
            } else {
                label_names
                    .iter()
                    .filter_map(|name| {
                        labels
                            .get(name)
                            .map(|value| (name.clone(), value.to_string()))
                    })
                    .collect::<Vec<_>>()
            };
            // Pyroscope's `/series` emits each set's labels SORTED by name. The
            // full-label-set form already iterates the `BTreeMap` in key order, but
            // the projected form follows the request's `label_names` order, so sort
            // unconditionally to keep the wire order identical to Pyroscope's.
            projected.sort();
            if !projected.is_empty() {
                out.insert(projected);
            }
        }
        out.into_iter().collect()
    }

    /// Candidate block keys pruned by time and fingerprint. This is an alias
    /// of [`Self::candidate_blocks`], named for the profile index's call
    /// sites.
    #[must_use]
    pub fn candidate_blocks_for_series(
        &self,
        tenant: &str,
        fps: &BTreeSet<SeriesFingerprint>,
        min_ts: i64,
        max_ts: i64,
    ) -> Vec<String> {
        self.candidate_blocks(tenant, fps, min_ts, max_ts)
    }

    /// Tightest `(min, max)` time bounds across the blocks that overlap the
    /// range.
    #[must_use]
    pub fn block_time_bounds(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Option<(i64, i64)> {
        self.tenants.get(tenant)?.blocks.time_bounds(min_ts, max_ts)
    }

    /// Folds every series and block of `other` into this index.
    ///
    /// Series postings are grow-only, so the union is their join: replaying a
    /// series that is already here is a no-op, and a block that both sides name
    /// takes `other`'s bounds. Removals are not expressed here; a caller that
    /// has any replays them with [`Index::replace_blocks`] after merging.
    pub(crate) fn merge_from(&mut self, other: &Self) {
        for (tenant, tenant_index) in &other.tenants {
            for (fingerprint, labels) in &tenant_index.series {
                self.add_series(tenant, *fingerprint, labels);
            }
            // One pass over the other side's postings rebuilds every block's
            // series set, so a merge inverts the map once rather than once per
            // block.
            for meta in tenant_index.blocks.metas(tenant) {
                self.add_block(&meta);
            }
        }
    }

    /// Replaces the `remove_keys` blocks with `add`. This is the compaction
    /// swap.
    pub fn replace_blocks(&mut self, tenant: &str, remove_keys: &[String], add: &[BlockMeta]) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        tenant_index
            .blocks
            .remove(&remove_keys.iter().collect::<BTreeSet<_>>());
        for meta in add {
            tenant_index.blocks.insert(meta);
        }
    }

    /// How many rounds of compaction produced `object_key`, whichever tenant
    /// holds it. Object keys are unique across tenants.
    #[must_use]
    pub fn block_level(&self, object_key: &str) -> Option<BlockLevel> {
        self.tenants
            .values()
            .find_map(|tenant_index| tenant_index.blocks.level_of(object_key))
    }

    /// Every block across every tenant, as [`BlockMeta`]. Use
    /// [`Index::all_blocks`] when a tenant is known.
    #[must_use]
    pub fn all_blocks_unscoped(&self) -> Vec<BlockMeta> {
        self.tenants
            .iter()
            .flat_map(|(tenant, tenant_index)| tenant_index.blocks.metas(tenant))
            .collect()
    }

    /// Number of blocks recorded for a tenant.
    #[must_use]
    pub fn block_count(&self, tenant: &str) -> usize {
        self.tenants
            .get(tenant)
            .map_or(0, |tenant_index| tenant_index.blocks.len())
    }

    /// Object keys of the blocks that overlap `[min_ts, max_ts]`. The
    /// fingerprints do not matter.
    #[must_use]
    pub fn blocks_in_range(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        let Some(tenant_index) = self.tenants.get(tenant) else {
            return Vec::new();
        };
        tenant_index.blocks.blocks_in_range(min_ts, max_ts)
    }

    /// Persists the index as time-sharded objects under the shard prefix of
    /// `object_key`, at the default shard width.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<()> {
        self.save_with_shard_width(store, object_key, DEFAULT_INDEX_SHARD_WIDTH)
            .await
    }

    /// Persists the index with `shard_width` ticks to a shard.
    ///
    /// The index carries no unit of its own. The metrics path counts
    /// milliseconds through it and the profiles path counts nanoseconds, so
    /// [`DEFAULT_INDEX_SHARD_WIDTH`] can be right for only one of them. A
    /// caller that knows its unit says so here. A width that is wrong by
    /// orders of magnitude is widened until the tenant fits
    /// [`super::MAX_INDEX_SHARDS_PER_TENANT`] shards. The cost of not saying is
    /// therefore coarse shards, not a million objects.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails.
    pub async fn save_with_shard_width(
        &self,
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        shard_width: i64,
    ) -> Result<()> {
        save_index_shards(self, store, object_key, shard_width).await
    }

    /// Loads every shard of an index, across every tenant.
    ///
    /// This is the whole-fleet load, and it is the one a query should not be
    /// doing: see [`Index::load_for_range`]. An index with no shards loads as
    /// an empty index rather than an error, because a first save has not
    /// happened yet and that is not a failure.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails or a shard is malformed.
    pub async fn load(store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<Self> {
        Self::load_with_cap(store, object_key, MAX_INDEX_SNAPSHOT_BYTES).await
    }

    /// Loads one tenant's shards that overlap `[min_ts, max_ts]`.
    ///
    /// A shard carries its span in its key, so the loader lists the shards
    /// outside the window and then does not read them. This is the load a
    /// query wants. What it holds is proportional to the range it asked about,
    /// not to the fleet.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails or a shard is malformed.
    pub async fn load_for_range(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        tenant: &str,
        min_ts: i64,
        max_ts: i64,
    ) -> Result<Self> {
        Self::load_for_range_with_cap(
            store,
            object_key,
            tenant,
            min_ts,
            max_ts,
            MAX_INDEX_SNAPSHOT_BYTES,
        )
        .await
    }

    pub(crate) async fn load_with_cap(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        load_index_shards(store, object_key, None, None, max_bytes).await
    }

    /// [`Index::load_for_range`] with an explicit per-shard byte cap.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails or a shard is malformed.
    pub async fn load_for_range_with_cap(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        tenant: &str,
        min_ts: i64,
        max_ts: i64,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        load_index_shards(
            store,
            object_key,
            Some(tenant),
            Some((min_ts, max_ts)),
            max_bytes,
        )
        .await
    }
}

impl BlockIndex for Index {
    fn add_block(&mut self, meta: &BlockMeta) {
        Self::add_block(self, meta);
    }

    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String> {
        self.blocks_in_range(tenant, min_ts, max_ts)
    }

    fn block_count(&self, tenant: &str) -> usize {
        Self::block_count(self, tenant)
    }
}
