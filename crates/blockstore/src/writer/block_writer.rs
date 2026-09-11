use super::{
    AbortOnPartFailureStore, Arc, AsyncArrowWriter, BlockMeta, BlockSchema, BlockStreamWriter,
    BufWriter, ObjectStore, ObjectStoreMetrics, ObjectStoreOperation, ObjectStoreRetryPolicy, Path,
    RecordBatch, Result, SchemaRef, SortKeyCheck, SummaryColumns, block_writer_properties, debug,
    instrument, is_sorted_by_key, retry_object_store, series_block_schema, sort_batches_by_key,
    validate_against, validate_batch_schemas,
};

/// Writes Parquet blocks to an object store.
///
/// A block write that fails transiently is retried here, as a whole write,
/// because that is the only unit a block can be retried in: the bytes leave
/// through a [`BufWriter`], and past its buffer that is a multipart upload
/// whose parts cannot be replayed in place. Re-running the write from the
/// record batches starts a fresh upload to the same -- caller-supplied, so
/// unchanged -- key, and overwrites rather than duplicating. See
/// [`ObjectStoreRetryPolicy`] for the budget and
/// [`RetryingObjectStore`](crate::RetryingObjectStore) for why the store a
/// writer holds must not be wrapped as well.
pub struct BlockWriter {
    pub(crate) store: Arc<dyn ObjectStore>,
    retry: ObjectStoreRetryPolicy,
    metrics: ObjectStoreMetrics,
}

impl BlockWriter {
    /// A writer that retries transient block writes under
    /// [`ObjectStoreRetryPolicy::DEFAULT`].
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_retry_policy(store, ObjectStoreRetryPolicy::DEFAULT)
    }

    /// [`Self::new`] with the retry budget chosen by the caller.
    #[must_use]
    pub fn with_retry_policy(store: Arc<dyn ObjectStore>, retry: ObjectStoreRetryPolicy) -> Self {
        Self {
            store,
            retry,
            metrics: ObjectStoreMetrics::unregistered(),
        }
    }

    /// The same writer, with its block-write retries counted in `metrics`.
    ///
    /// A writer built without this counts nothing, so a service must call it.
    /// The puts underneath are counted anyway when the store is wrapped in
    /// [`MeteredObjectStore`](crate::MeteredObjectStore), but the block-write
    /// retry is the writer's own and only this reaches it.
    #[must_use]
    pub fn with_metrics(mut self, metrics: ObjectStoreMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Writes `batches` as a single Parquet block at `object_key`.
    ///
    /// Returns [`BlockMeta`] computed from the mandatory block columns.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn write_block(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<BlockMeta> {
        self.write_block_with_decl(
            tenant,
            object_key,
            schema,
            batches,
            &series_block_schema(),
            SummaryColumns::series(),
        )
        .await
    }

    /// Writes a block validated against a signal-specific schema declaration.
    ///
    /// The declaration decides how the block is physically written as well as
    /// what it must contain: the rows leave in the declared sort order, the
    /// Parquet file records that order as its `sorting_columns`, the declared
    /// identity columns get bloom filters, and the whole file is zstd
    /// compressed and cut into [`super::BLOCK_ROW_GROUP_ROWS`]-row groups.
    ///
    /// Enforcing the order here rather than trusting the caller is what makes
    /// the recorded `sorting_columns`, the block's min/max identity
    /// statistics, and the bloom filters describe the file that was actually
    /// written. A caller that hands over rows already in the declared order
    /// pays one comparison pass for the check and nothing else; a caller that
    /// cannot be in order has its rows sorted here, at the cost of holding
    /// the whole block in memory to do it. A caller that can produce the rows
    /// in order incrementally -- a merge of blocks that are each already
    /// sorted -- should use [`Self::open_block`] instead and pay neither.
    ///
    /// Returns [`BlockMeta`] computed from the declared summary columns.
    #[instrument(
        skip_all,
        fields(tenant = %tenant, object_key = %object_key, batches = batches.len()),
        err
    )]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn write_block_with_decl(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
        decl: &BlockSchema,
        summary: SummaryColumns,
    ) -> Result<BlockMeta> {
        validate_against(&schema, decl)?;
        validate_batch_schemas(&schema, batches)?;

        let sorted = if decl.sort_key.is_empty() || is_sorted_by_key(batches, &decl.sort_key)? {
            None
        } else {
            debug!(
                tenant,
                object_key,
                sort_key = ?decl.sort_key,
                "block rows were not in the declared sort order; sorting before write"
            );
            Some(sort_batches_by_key(&schema, batches, &decl.sort_key)?)
        };
        let batches = sorted.as_ref().map_or(batches, std::slice::from_ref);

        // Only the write is retried. The validation and the sort above are
        // deterministic and can only fail permanently, so repeating them would
        // burn the budget on work whose answer cannot change.
        retry_object_store(
            self.retry,
            ObjectStoreOperation::WriteBlock,
            &self.metrics,
            || async {
                // The order is settled above, either by the caller or by the sort,
                // so the stream writer has nothing left to check.
                let mut block = self.open(
                    tenant,
                    object_key,
                    schema.clone(),
                    decl,
                    summary.clone(),
                    None,
                )?;
                for batch in batches {
                    block.write_batch(batch).await?;
                }
                block.finish().await
            },
        )
        .await
    }

    /// Opens a block to be written one batch at a time.
    ///
    /// The block is physically identical to one [`Self::write_block_with_decl`]
    /// writes and its [`BlockMeta`] is derived from the same columns, but
    /// nothing is held resident: the summary accumulates as the batches pass
    /// and the encoded bytes leave a row group at a time. That makes the
    /// memory a compaction needs a function of its output rather than of the
    /// blocks it reads.
    ///
    /// The caller owes the writer rows in the declared sort order, across
    /// batch boundaries as well as within a batch, and
    /// [`BlockStreamWriter::write_batch`] rejects a batch that breaks it. See
    /// [`BlockStreamWriter`] for why that is an error rather than a silent
    /// sort.
    ///
    /// # Errors
    /// Returns an error when `schema` does not satisfy `decl`, when the
    /// declaration names a column the schema lacks, or when the Parquet writer
    /// cannot be created.
    pub fn open_block(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        decl: &BlockSchema,
        summary: SummaryColumns,
    ) -> Result<BlockStreamWriter> {
        let order = SortKeyCheck::new(&decl.sort_key);
        self.open(tenant, object_key, schema, decl, summary, Some(order))
    }

    fn open(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        decl: &BlockSchema,
        summary: SummaryColumns,
        order: Option<SortKeyCheck>,
    ) -> Result<BlockStreamWriter> {
        validate_against(&schema, decl)?;
        let properties = block_writer_properties(&schema, decl)?;
        let store = Arc::new(AbortOnPartFailureStore::new(self.store.clone()));
        let object_writer = BufWriter::new(store, Path::from(object_key));
        let writer = AsyncArrowWriter::try_new(object_writer, schema.clone(), Some(properties))?;
        Ok(BlockStreamWriter::new(
            writer,
            tenant,
            object_key,
            schema,
            summary,
            order,
            &decl.sort_key,
        ))
    }
}
