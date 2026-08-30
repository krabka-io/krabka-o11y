use super::{
    Arc, BlockStoreError, BlockWriter, ByteSize, DEFAULT_BLOCK_READ_MAX, Index, LabelMatcher,
    MemTable, ObjectStore, ParquetReadOptions, Result, RowGroupMeta, ScanTableRequest, SchemaRef,
    SessionContext, TABLE_NAME, Url, instrument, read_block_row_groups_with_max_bytes,
    read_row_group_metadata_with_max_bytes,
};

/// Owns the object store, its `DataFusion` URL prefix, and the in-memory index.
#[derive(Clone)]
pub struct BlockStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) base: Url,
    pub(crate) index: Arc<Index>,
    pub(crate) block_read_max: ByteSize,
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
        let store = krabka_object_store::build_object_store(cfg)
            .map_err(|e| BlockStoreError::ObjectStore(e.to_string()))?;
        Ok(Self::new(store, base))
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
        Self::new_with_block_read_max(self.store.clone(), self.base.clone(), self.block_read_max)
    }

    /// Reads Parquet row-group metadata with this store's configured cap.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, the block exceeds the
    /// configured cap, or persisted metadata is malformed.
    pub async fn read_row_group_metadata(&self, object_key: &str) -> Result<Vec<RowGroupMeta>> {
        read_row_group_metadata_with_max_bytes(self.store.clone(), object_key, self.block_read_max)
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
        let ctx = SessionContext::new();
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
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn register_scan_table(
        &self,
        ctx: &SessionContext,
        request: ScanTableRequest<'_>,
    ) -> Result<bool> {
        let fingerprints = self.index.resolve(request.tenant, request.matchers)?;
        let candidates = self.index.candidate_blocks(
            request.tenant,
            &fingerprints,
            request.min_ts,
            request.max_ts,
        );
        tracing::Span::current().record("candidates", candidates.len());
        ctx.register_object_store(&self.base, self.store.clone());
        if candidates.is_empty() {
            let table = MemTable::try_new(request.schema, vec![Vec::new()])?;
            ctx.register_table(request.table_name, Arc::new(table))?;
            return Ok(false);
        }

        let paths = candidates
            .iter()
            .map(|object_key| {
                self.base
                    .join(object_key)
                    .map(|url| url.to_string())
                    .map_err(|error| {
                        BlockStoreError::InvalidBlock(format!(
                            "invalid block object key `{object_key}`: {error}"
                        ))
                    })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let options = ParquetReadOptions::default().schema(request.schema.as_ref());
        let dataframe = ctx.read_parquet(paths, options).await?;
        ctx.register_table(request.table_name, dataframe.into_view())?;

        Ok(true)
    }

    #[instrument(level = "debug", skip_all, fields(keys = keys.len()), err)]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn scan_block_keys(
        &self,
        keys: &[String],
        schema: SchemaRef,
    ) -> Result<(SessionContext, String)> {
        let ctx = SessionContext::new();
        ctx.register_object_store(&self.base, self.store.clone());

        if keys.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        // Compose each block's location with `Url::join` (the same way
        // `register_scan_table` does) — a raw `format!("{base}{key}")` concat
        // omits the path separator, so a base like `s3://krabka-traces` + key
        // `traces/…` becomes `s3://krabka-tracestraces/…` (the prefix merges
        // into the bucket authority) and DataFusion can't resolve the store.
        let paths = keys
            .iter()
            .map(|key| {
                self.base
                    .join(key.trim_start_matches('/'))
                    .map(|url| url.to_string())
                    .map_err(|error| {
                        BlockStoreError::InvalidBlock(format!(
                            "invalid block object key `{key}`: {error}"
                        ))
                    })
            })
            .collect::<std::result::Result<Vec<String>, _>>()?;
        let df = ctx
            .read_parquet(paths, ParquetReadOptions::default())
            .await?;
        ctx.register_table(TABLE_NAME, df.into_view())?;
        Ok((ctx, TABLE_NAME.to_string()))
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
        let ctx = SessionContext::new();
        ctx.register_object_store(&self.base, self.store.clone());

        if row_groups.is_empty() {
            let empty = MemTable::try_new(schema, vec![vec![]])?;
            ctx.register_table(TABLE_NAME, Arc::new(empty))?;
            return Ok((ctx, TABLE_NAME.to_string()));
        }

        let batches = read_block_row_groups_with_max_bytes(
            self.store.clone(),
            object_key,
            row_groups,
            self.block_read_max,
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
