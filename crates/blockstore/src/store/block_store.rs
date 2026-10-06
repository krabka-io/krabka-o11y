use super::{
    Arc, BlockMetadataCache, BlockScan, BlockStoreError, BlockWriter, ByteSize, ByteSizeExt,
    DEFAULT_BLOCK_METADATA_CACHE_MAX, DEFAULT_BLOCK_READ_MAX, Index, LabelMatcher, MemTable,
    ObjectStore, ObjectStoreUrl, ParquetReadOptions, ProbedBlock, ProbedBlockTable, Result,
    RowGroupMeta, RuntimeEnv, RuntimeEnvBuilder, ScanReport, ScanTableRequest, SchemaRef,
    SeriesFingerprint, SessionConfig, SessionContext, TABLE_NAME, Url, instrument, probe_blocks,
    read_block_row_groups_cached, require_blocks, row_group_metadata, scan_filter,
};

/// Owns the object store, its `DataFusion` URL prefix, the in-memory index,
/// and the read caches every scan through this store shares.
#[derive(Clone)]
pub struct BlockStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) base: Url,
    pub(crate) index: Arc<Index>,
    pub(crate) block_read_max: ByteSize,
    /// Shared by every clone and every scan, so the footer a query reads is
    /// still there for the next one. A per-scan `SessionContext::new()` builds
    /// a `RuntimeEnv` of its own, whose caches are born empty and die with the
    /// query.
    pub(crate) runtime: Arc<RuntimeEnv>,
}

impl BlockStore {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, base: Url) -> Self {
        Self::new_with_block_read_max(store, base, DEFAULT_BLOCK_READ_MAX)
    }

    #[must_use]
    pub fn new_with_block_read_max(
        store: Arc<dyn ObjectStore>,
        base: Url,
        block_read_max: ByteSize,
    ) -> Self {
        Self {
            store,
            base,
            index: Arc::new(Index::new()),
            block_read_max,
            runtime: block_store_runtime(DEFAULT_BLOCK_METADATA_CACHE_MAX),
        }
    }

    /// Builds a `BlockStore` whose object store comes from `cfg` through the
    /// shared `krabka-object-store` substrate. `base` stays the caller's
    /// `DataFusion` registration URL, which is a query-engine concern that the
    /// caller owns.
    ///
    /// # Errors
    ///
    /// Returns [`BlockStoreError::ObjectStore`] if the backend builder rejects `cfg`.
    pub fn from_config(cfg: &krabka_object_store::ObjectStoreConfig, base: Url) -> Result<Self> {
        let store = krabka_object_store::v013::build_object_store(cfg)
            .map_err(|e| BlockStoreError::ObjectStore(e.to_string()))?;
        Ok(Self::new(store, base))
    }

    /// Sets the memory budget of this store's Parquet footer cache, replacing
    /// the shared caches with fresh ones.
    ///
    /// Call it while building the store, before any clone of it is handed out:
    /// clones made earlier keep the caches they were built with.
    #[must_use]
    pub fn with_metadata_cache_max(mut self, max: ByteSize) -> Self {
        self.runtime = block_store_runtime(max);
        self
    }

    #[must_use]
    pub fn writer(&self) -> BlockWriter {
        BlockWriter::new(self.store.clone())
    }

    #[must_use]
    pub fn index(&self) -> &Index {
        &self.index
    }

    pub fn index_mut(&mut self) -> &mut Index {
        Arc::make_mut(&mut self.index)
    }

    #[must_use]
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.store.clone()
    }

    #[must_use]
    pub fn empty_like(&self) -> Self {
        Self {
            store: self.store.clone(),
            base: self.base.clone(),
            index: Arc::new(Index::new()),
            block_read_max: self.block_read_max,
            runtime: Arc::clone(&self.runtime),
        }
    }

    /// This store's Parquet footer cache, shared with every scan it registers.
    #[must_use]
    pub fn metadata_cache(&self) -> BlockMetadataCache {
        BlockMetadataCache::new(self.runtime.cache_manager.get_file_metadata_cache())
    }

    /// A `DataFusion` session that reads through this store's caches.
    ///
    /// A caller that builds its own tables — `krabka-promql` registers two —
    /// should start from this rather than from `SessionContext::new()`, which
    /// gets a private `RuntimeEnv` and therefore a cold cache for every query.
    #[must_use]
    pub fn session_context(&self) -> SessionContext {
        SessionContext::new_with_config_rt(SessionConfig::new(), Arc::clone(&self.runtime))
    }

    /// Reads Parquet row-group metadata with this store's configured cap,
    /// through this store's footer cache.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, the block exceeds the
    /// configured cap, or persisted metadata is malformed.
    pub async fn read_row_group_metadata(&self, object_key: &str) -> Result<Vec<RowGroupMeta>> {
        row_group_metadata(
            &self.store,
            object_key,
            self.block_read_max,
            Some(&self.metadata_cache()),
        )
        .await
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn scan_context(
        &self,
        tenant: &str,
        matchers: &[LabelMatcher],
        min_ts: i64,
        max_ts: i64,
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = self.session_context();
        self.register_scan_table(
            &ctx,
            ScanTableRequest {
                table_name: TABLE_NAME,
                tenant,
                matchers,
                min_ts,
                max_ts,
                schema,
            },
        )
        .await?;
        Ok((ctx, TABLE_NAME.to_string()))
    }

    /// The blocks the index offers for `request`, and the series they were
    /// resolved from.
    fn scan_candidates(
        &self,
        request: &ScanTableRequest<'_>,
    ) -> Result<(std::collections::BTreeSet<SeriesFingerprint>, Vec<String>)> {
        let fingerprints = self.index.resolve(request.tenant, request.matchers)?;
        let candidates = self.index.candidate_blocks(
            request.tenant,
            &fingerprints,
            request.min_ts,
            request.max_ts,
        );
        tracing::Span::current().record("candidates", candidates.len());
        Ok((fingerprints, candidates))
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            table = %request.table_name,
            tenant = %request.tenant,
            matchers = request.matchers.len(),
            min_ts = request.min_ts,
            max_ts = request.max_ts,
            candidates = tracing::field::Empty,
        ),
        err
    )]
    /// Registers every block the index offers, and fails if any one of them
    /// cannot be read.
    ///
    /// Failing is the point. Every candidate is checked before the table is
    /// registered, so a caller that cannot handle a partial answer learns that
    /// it did not get a whole one. The check reads each footer, and the table
    /// reads the blocks with the footers and the `ObjectMeta` that the check
    /// returns.
    ///
    /// Use [`Self::register_scan_table_skipping_unreadable`] where a partial
    /// answer with a warning beats no answer at all.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn register_scan_table(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
    ) -> Result<bool> {
        let (fingerprints, candidates) = self.scan_candidates(&request)?;
        let blocks = require_blocks(
            &self.store,
            &candidates,
            self.block_read_max,
            &self.metadata_cache(),
        )
        .await?;
        self.register_blocks(ctx, &request, &fingerprints, blocks)
    }

    /// Registers the blocks the index offers, leaving out the ones that cannot
    /// be read and reporting them.
    ///
    /// The index and the objects it names are written in separate steps, and
    /// compaction swaps its outputs in without deleting its inputs, so an
    /// index snapshot restored from an older generation can name keys a later
    /// compactor replaced. Left to `DataFusion` that key is either fatal — a
    /// file that fails to decode fails the whole plan — or invisible, since a
    /// path that lists to nothing is quietly dropped. Neither is an answer a
    /// caller can act on.
    ///
    /// The caller is expected to surface [`ScanReport::skipped`] — see
    /// [`ScanReport`]. A skip nobody reports is the failure mode this exists to
    /// avoid, not the one it creates.
    ///
    /// # Errors
    /// Returns an error when the object store fails for a reason that is not
    /// one block's fault, when a block exceeds the read cap, or when the
    /// readable blocks cannot be registered.
    #[instrument(
        level = "debug",
        skip_all,
        fields(
            table = %request.table_name,
            tenant = %request.tenant,
            matchers = request.matchers.len(),
            min_ts = request.min_ts,
            max_ts = request.max_ts,
            candidates = tracing::field::Empty,
        ),
        err
    )]
    pub async fn register_scan_table_skipping_unreadable(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
    ) -> Result<ScanReport> {
        self.register_selected_scan_table(ctx, request, None).await
    }

    /// Registers only the requested series, choosing candidate blocks before
    /// probing them. Matchers, tenant and time bounds still apply.
    ///
    /// # Errors
    /// Returns the same object-store and registration errors as
    /// [`Self::register_scan_table_skipping_unreadable`].
    pub async fn register_scan_table_for_fingerprints_skipping_unreadable(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
        fingerprints: &std::collections::BTreeSet<SeriesFingerprint>,
    ) -> Result<ScanReport> {
        self.register_selected_scan_table(ctx, request, Some(fingerprints))
            .await
    }

    async fn register_selected_scan_table(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
        selected: Option<&std::collections::BTreeSet<SeriesFingerprint>>,
    ) -> Result<ScanReport> {
        let (mut fingerprints, mut candidates) = if request.matchers.is_empty() {
            if let Some(selected) = selected {
                (
                    selected.clone(),
                    self.index.candidate_blocks(
                        request.tenant,
                        selected,
                        request.min_ts,
                        request.max_ts,
                    ),
                )
            } else {
                self.scan_candidates(&request)?
            }
        } else {
            self.scan_candidates(&request)?
        };
        if let Some(selected) = selected {
            fingerprints.retain(|fingerprint| selected.contains(fingerprint));
            candidates = self.index.candidate_blocks(
                request.tenant,
                &fingerprints,
                request.min_ts,
                request.max_ts,
            );
        }
        let (readable, skipped) = probe_blocks(
            &self.store,
            &candidates,
            self.block_read_max,
            &self.metadata_cache(),
        )
        .await?;
        let registered = self.register_blocks(ctx, &request, &fingerprints, readable)?;
        Ok(ScanReport {
            registered,
            skipped,
        })
    }

    /// Registers `blocks` as `request.table_name`, bounded by the scan predicate.
    ///
    /// The table reads the blocks with the `ObjectMeta` and the footer that the
    /// probe already read, so planning the scan makes no request. See
    /// [`ProbedBlockTable`].
    fn register_blocks(
        &self,
        ctx: &SessionContext,
        request: &ScanTableRequest<'_>,
        fingerprints: &std::collections::BTreeSet<SeriesFingerprint>,
        blocks: Vec<ProbedBlock>,
    ) -> Result<bool> {
        ctx.register_object_store(&self.base, self.store.clone());
        if blocks.is_empty() {
            let table = MemTable::try_new(request.schema.clone(), vec![Vec::new()])?;
            ctx.register_table(request.table_name, Arc::new(table))?;
            return Ok(false);
        }

        // Late materialization. With filter pushdown on, the Parquet reader
        // evaluates the scan predicate against the fingerprint and timestamp
        // columns alone and decodes the payload columns only for the rows that
        // survive it. Without it the predicate is a `FilterExec` above a scan
        // that has already decoded everything, so only row-group pruning helps.
        // The flag lives on the session config, and the table copies
        // `execution.parquet` from it when it plans the scan.
        {
            let state = ctx.state_ref();
            let mut state = state.write();
            let parquet = &mut state.config_mut().options_mut().execution.parquet;
            parquet.pushdown_filters = true;
            parquet.reorder_filters = true;
        }

        let table = ProbedBlockTable {
            schema: request.schema.clone(),
            object_store_url: ObjectStoreUrl::parse(&self.base[..url::Position::BeforePath])?,
            store: Arc::clone(&self.store),
            metadata_cache: self.runtime.cache_manager.get_file_metadata_cache(),
            blocks,
        };
        let dataframe = ctx.read_table(Arc::new(table))?;
        // Bound the scan to the requested window and to the matcher-resolved
        // series. The table reports these as inexact, so `DataFusion` keeps
        // the `FilterExec` for exactness *and* hands the predicate to the
        // Parquet source, which prunes row groups and data pages by their
        // statistics. Block-level pruning alone would still decode every row of
        // every candidate block.
        let dataframe = match scan_filter(request.min_ts, request.max_ts, fingerprints) {
            Some(filter) => dataframe.filter(filter)?,
            None => dataframe,
        };
        ctx.register_table(request.table_name, dataframe.into_view())?;

        Ok(true)
    }

    /// Resolves complete object-store keys from the scheme and authority
    /// registered with `DataFusion`. The base path is a namespace prefix
    /// already present in each key, so joining against it duplicates the prefix.
    fn block_urls(&self, keys: &[String]) -> Result<Vec<String>> {
        let mut base = self.base.clone();
        base.set_path("/");
        keys.iter()
            .map(|key| {
                base.join(key.trim_start_matches('/'))
                    .map(|url| url.to_string())
                    .map_err(|error| {
                        BlockStoreError::InvalidBlock(format!(
                            "invalid block object key `{key}`: {error}"
                        ))
                    })
            })
            .collect()
    }

    #[instrument(level = "debug", skip_all, fields(keys = keys.len()), err)]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn scan_block_keys(
        &self,
        keys: &[String],
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        require_blocks(
            &self.store,
            keys,
            self.block_read_max,
            &self.metadata_cache(),
        )
        .await?;
        let ctx = self.session_context();
        self.register_block_keys(&ctx, keys, schema).await?;
        Ok((ctx, TABLE_NAME.to_string()))
    }

    /// Scans the named blocks, leaving out the ones that cannot be read and
    /// reporting them.
    ///
    /// This is the trace path's version of
    /// [`Self::register_scan_table_skipping_unreadable`], and the same
    /// obligation applies: report [`ScanReport::skipped`] to the caller.
    ///
    /// # Errors
    /// Returns an error when the object store fails for a reason that is not
    /// one block's fault, when a block exceeds the read cap, or when the
    /// readable blocks cannot be registered.
    #[instrument(level = "debug", skip_all, fields(keys = keys.len(), skipped = tracing::field::Empty), err)]
    pub async fn scan_block_keys_skipping_unreadable(
        &self,
        keys: &[String],
        schema: SchemaRef,
    ) -> Result<BlockScan> {
        let (readable, skipped) = probe_blocks(
            &self.store,
            keys,
            self.block_read_max,
            &self.metadata_cache(),
        )
        .await?;
        tracing::Span::current().record("skipped", skipped.len());
        let readable = readable
            .into_iter()
            .map(|block| block.object_key)
            .collect::<Vec<_>>();
        let ctx = self.session_context();
        let registered = self.register_block_keys(&ctx, &readable, schema).await?;
        Ok(BlockScan {
            ctx,
            table: TABLE_NAME.to_string(),
            report: ScanReport {
                registered,
                skipped,
            },
        })
    }

    async fn register_block_keys(
        &self,
        ctx: &SessionContext,
        keys: &[String],
        schema: SchemaRef,
    ) -> Result<bool> {
        ctx.register_object_store(&self.base, self.store.clone());
        if keys.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok(false);
        }

        let paths = self.block_urls(keys)?;
        let df = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await?;
        ctx.register_table(TABLE_NAME, df.into_view())?;
        Ok(true)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(object_key = %object_key, row_groups = row_groups.len()),
        err
    )]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn scan_block_row_groups(
        &self,
        object_key: &str,
        row_groups: &[usize],
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = self.session_context();
        ctx.register_object_store(&self.base, self.store.clone());

        if row_groups.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        let batches = read_block_row_groups_cached(
            &self.store,
            object_key,
            row_groups,
            self.block_read_max,
            Some(&self.metadata_cache()),
        )
        .await?;
        let partitions = if batches.is_empty() {
            vec![vec![]]
        } else {
            vec![batches]
        };
        let table = MemTable::try_new(schema, partitions)?;
        ctx.register_table(TABLE_NAME, Arc::new(table))?;
        Ok((ctx, TABLE_NAME.to_string()))
    }
}

/// Builds the `RuntimeEnv` a `BlockStore`'s scans share, with the footer cache
/// bounded at `metadata_cache_max`.
fn block_store_runtime(metadata_cache_max: ByteSize) -> Arc<RuntimeEnv> {
    RuntimeEnvBuilder::new()
        .with_metadata_cache_limit(metadata_cache_max.bytes_usize())
        .build_arc()
        .expect("a RuntimeEnv that sets only a cache limit has nothing left to reject")
}
