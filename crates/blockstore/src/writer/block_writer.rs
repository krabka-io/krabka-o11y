use super::{
    Arc, AsyncArrowWriter, BlockMeta, BlockSchema, BufWriter, ObjectStore, Path, RecordBatch,
    Result, SchemaRef, SummaryColumns, block_writer_properties, debug, instrument,
    is_sorted_by_key, series_block_schema, sort_batches_by_key, summarize, validate_against,
    validate_batch_schemas,
};

/// Writes Parquet blocks to an object store.
pub struct BlockWriter {
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl BlockWriter {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
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
    /// merges blocks, and so cannot be in order, has its rows sorted here.
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
        let properties = block_writer_properties(&schema, decl)?;

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

        let (min_ts, max_ts, row_count, fingerprints) = summarize(batches, &summary)?;

        let path = Path::from(object_key);
        let object_writer = BufWriter::new(self.store.clone(), path);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, Some(properties))?;
        for batch in batches {
            writer.write(batch).await?;
        }
        writer.close().await?;

        Ok(BlockMeta {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            fingerprints,
        })
    }
}
