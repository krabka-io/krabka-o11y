use super::{
    Arc, BTreeMap, BTreeSet, BlockEntry, BlockIndex, BlockMeta, BlockStoreError, ByteSize,
    ByteSizeExt, Deserialize, LabelMatcher, Labels, MAX_INDEX_SNAPSHOT_BYTES, ObjectStore,
    ObjectStoreExt, Path, PutPayload, QUERY_SHARD_LABEL, Result, Serialize, SeriesFingerprint,
    TenantIndex, instrument, matcher_matches_empty,
};

/// Multi-tenant in-memory index for label resolution and block pruning.
///
/// This is the metrics and logs series index. The profiles index and the
/// traces index embed it for shared label posting and matcher resolution.
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
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        if tenant_index.series.contains_key(&fp) {
            return;
        }
        tenant_index.series.insert(fp, labels.clone());

        for (name, value) in labels.iter() {
            tenant_index
                .postings
                .entry(name.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(fp);
            tenant_index
                .values
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    pub fn add_block(&mut self, meta: &BlockMeta) {
        let tenant_index = self.tenants.entry(meta.tenant.clone()).or_default();
        if let Some(entry) = tenant_index
            .blocks
            .iter_mut()
            .find(|entry| entry.object_key == meta.object_key)
        {
            entry.min_ts = meta.min_ts;
            entry.max_ts = meta.max_ts;
            entry.row_count = meta.row_count;
            entry.fingerprints = meta.fingerprints.iter().copied().collect();
            return;
        }

        tenant_index.blocks.push(BlockEntry {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            row_count: meta.row_count,
            fingerprints: meta.fingerprints.iter().copied().collect(),
        });
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

        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .filter(|block| block.fingerprints.iter().any(|fp| fps.contains(fp)))
            .map(|block| block.object_key.clone())
            .collect()
    }

    #[must_use]
    pub fn all_blocks(&self, tenant: &str) -> Vec<BlockMeta> {
        self.tenants
            .get(tenant)
            .map(|tenant_index| {
                tenant_index
                    .blocks
                    .iter()
                    .map(|block| BlockMeta {
                        tenant: tenant.to_string(),
                        object_key: block.object_key.clone(),
                        min_ts: block.min_ts,
                        max_ts: block.max_ts,
                        row_count: block.row_count,
                        fingerprints: block.fingerprints.iter().copied().collect(),
                    })
                    .collect()
            })
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
        let tenant_index = self.tenants.get(tenant)?;
        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .fold(None, |acc, block| match acc {
                Some((min, max)) => Some((min.min(block.min_ts), max.max(block.max_ts))),
                None => Some((block.min_ts, block.max_ts)),
            })
    }

    /// Replaces the `remove_keys` blocks with `add`. This is the compaction
    /// swap.
    pub fn replace_blocks(&mut self, tenant: &str, remove_keys: &[String], add: &[BlockMeta]) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        let remove_keys = remove_keys.iter().collect::<BTreeSet<_>>();
        tenant_index
            .blocks
            .retain(|block| !remove_keys.contains(&block.object_key));
        for meta in add {
            tenant_index.blocks.push(BlockEntry {
                object_key: meta.object_key.clone(),
                min_ts: meta.min_ts,
                max_ts: meta.max_ts,
                row_count: meta.row_count,
                fingerprints: meta.fingerprints.iter().copied().collect(),
            });
        }
    }

    /// Every block across every tenant, as [`BlockMeta`]. Use
    /// [`Index::all_blocks`] when a tenant is known.
    #[must_use]
    pub fn all_blocks_unscoped(&self) -> Vec<BlockMeta> {
        self.tenants
            .iter()
            .flat_map(|(tenant, tenant_index)| {
                tenant_index.blocks.iter().map(move |block| BlockMeta {
                    tenant: tenant.clone(),
                    object_key: block.object_key.clone(),
                    min_ts: block.min_ts,
                    max_ts: block.max_ts,
                    row_count: block.row_count,
                    fingerprints: block.fingerprints.iter().copied().collect(),
                })
            })
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
        tenant_index
            .blocks
            .iter()
            .filter(|block| block.min_ts <= max_ts && block.max_ts >= min_ts)
            .map(|block| block.object_key.clone())
            .collect()
    }

    /// Persists the index as a JSON snapshot to object storage.
    #[instrument(
        skip_all,
        fields(object_key = %object_key, len = tracing::field::Empty),
        err
    )]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn save(&self, store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<()> {
        let bytes = serde_json::to_vec(self)?;
        tracing::Span::current().record("len", bytes.len());
        let path = Path::from(object_key);
        store.put(&path, PutPayload::from(bytes)).await?;
        Ok(())
    }

    /// Loads an index JSON snapshot from object storage.
    ///
    /// The loader `head()`s the object first and rejects it when it is larger
    /// than [`MAX_INDEX_SNAPSHOT_BYTES`], so a corrupt or oversized snapshot
    /// from shared storage cannot OOM the process during the buffered read.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn load(store: &Arc<dyn ObjectStore>, object_key: &str) -> Result<Self> {
        Self::load_with_cap(store, object_key, MAX_INDEX_SNAPSHOT_BYTES).await
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(object_key = %object_key),
        err
    )]
    pub(crate) async fn load_with_cap(
        store: &Arc<dyn ObjectStore>,
        object_key: &str,
        max_bytes: ByteSize,
    ) -> Result<Self> {
        let path = Path::from(object_key);
        let bytes = match krabka_object_store::read_capped(store, &path, max_bytes.bytes_u64())
            .await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(match error {
                    krabka_object_store::ObjectStoreError::TooLarge {
                        size, max_bytes, ..
                    } => BlockStoreError::InvalidBlock(format!(
                        "index snapshot `{object_key}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    )),
                    krabka_object_store::ObjectStoreError::Backend(message)
                    | krabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                        BlockStoreError::ObjectStore(message)
                    }
                    krabka_object_store::ObjectStoreError::Io(error) => {
                        BlockStoreError::ObjectStore(error.to_string())
                    }
                    not_found @ krabka_object_store::ObjectStoreError::NotFound(_) => {
                        match store.head(&path).await {
                            Ok(_) => BlockStoreError::ObjectStore(not_found.to_string()),
                            Err(missing) => BlockStoreError::ObjectStore(missing.to_string()),
                        }
                    }
                    // Write-side variants: `read_capped` cannot raise them, but
                    // they are part of the enum, so surface them like any other
                    // backend failure rather than widening the read path.
                    conflict @ (krabka_object_store::ObjectStoreError::AlreadyExists(_)
                    | krabka_object_store::ObjectStoreError::Precondition { .. }) => {
                        BlockStoreError::ObjectStore(conflict.to_string())
                    }
                });
            }
        };
        Ok(serde_json::from_slice(&bytes)?)
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
