use super::*;

/// Trace block index.
#[derive(Default, Serialize, Deserialize)]
pub struct TraceIndex {
    pub(crate) tenants: HashMap<String, TenantTraceIndex>,
}

impl TraceIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_trace_block(&mut self, tenant: &str, stats: TraceBlockStats) {
        let tenant_index = self.tenants.entry(tenant.to_string()).or_default();
        tenant_index
            .blocks
            .retain(|block| block.object_key != stats.object_key);
        tenant_index.blocks.push(stats);
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
    ) {
        let old_keys: BTreeSet<&str> = old_keys.iter().map(String::as_str).collect();
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
        put_index_snapshot(store, key, serde_json::to_vec(self)?, retain).await
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
        let bytes = match krabka_object_store::read_capped(store, path, max_bytes.bytes_u64()).await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                return Err(match error {
                    krabka_object_store::ObjectStoreError::TooLarge {
                        size, max_bytes, ..
                    } => BlockStoreError::InvalidBlock(format!(
                        "trace index snapshot `{path}` is {size} bytes, exceeds cap of {max_bytes} bytes"
                    )),
                    krabka_object_store::ObjectStoreError::Backend(message)
                    | krabka_object_store::ObjectStoreError::InvalidConfig(message) => {
                        BlockStoreError::ObjectStore(message)
                    }
                    krabka_object_store::ObjectStoreError::Io(error) => {
                        BlockStoreError::ObjectStore(error.to_string())
                    }
                    not_found @ krabka_object_store::ObjectStoreError::NotFound(_) => {
                        match store.head(path).await {
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
        let index: Self = serde_json::from_slice(&bytes)?;
        // `Deserialize` bypasses the bloom constructors' invariant checks, so a
        // structurally-valid-but-corrupt snapshot would panic on the first
        // lookup. Validate here so it surfaces as an error instead.
        index.validate()?;
        Ok(index)
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
