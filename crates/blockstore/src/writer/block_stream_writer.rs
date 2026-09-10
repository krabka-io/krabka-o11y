use super::{
    AsyncArrowWriter, BlockLevel, BlockMeta, BlockStoreError, BlockSummary, BufWriter, RecordBatch,
    Result, SchemaRef, SortKeyCheck, SummaryColumns, validate_batch_schema,
};

/// A block being written one batch at a time.
///
/// [`BlockWriter::write_block_with_decl`](super::BlockWriter::write_block_with_decl)
/// takes the whole block up front, which costs a caller that merges several
/// blocks the memory of every input at once. This is the same block, the same
/// declaration and the same [`BlockMeta`], built as the rows go past: the
/// summary is a running fold, the sort order is verified across batch
/// boundaries rather than fixed up at the end, and the encoded bytes leave for
/// the object store a row group at a time.
///
/// What the caller gives up in exchange is the writer's willingness to sort:
/// rows must arrive in the declared order, because by the time a later batch
/// contradicts an earlier one the earlier one is already encoded. A batch out
/// of order is an error, and it poisons the writer -- the block is never
/// closed, so no Parquet footer is written and no readable object appears at
/// the key. Recording a `sorting_columns` the file does not honour would be
/// worse than failing: row-group pruning trusts it, and would skip groups that
/// hold matching rows.
pub struct BlockStreamWriter {
    writer: AsyncArrowWriter<BufWriter>,
    tenant: String,
    object_key: String,
    schema: SchemaRef,
    columns: SummaryColumns,
    summary: BlockSummary,
    /// `None` once the block's order is the writer's own doing rather than the
    /// caller's -- the buffered path sorts before it streams, and re-checking
    /// its own output would buy nothing for a second pass over every key.
    order: Option<SortKeyCheck>,
    sort_key: Vec<String>,
    batches: usize,
    failed: bool,
}

impl BlockStreamWriter {
    pub(crate) fn new(
        writer: AsyncArrowWriter<BufWriter>,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        columns: SummaryColumns,
        order: Option<SortKeyCheck>,
        sort_key: &[String],
    ) -> Self {
        Self {
            writer,
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            schema,
            columns,
            summary: BlockSummary::new(),
            order,
            sort_key: sort_key.to_vec(),
            batches: 0,
            failed: false,
        }
    }

    /// Appends `batch` to the block.
    ///
    /// Empty batches are dropped rather than encoded: they carry no rows to
    /// summarize and no key to order against, and a merge that runs dry on one
    /// of its inputs produces them routinely.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when `batch` does not carry
    /// the block's schema, when its rows do not continue the declared sort
    /// order, or when an earlier call already failed; and an object-store or
    /// Parquet error when the encoded bytes cannot be written.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.failed {
            return Err(self.poisoned());
        }
        if batch.num_rows() == 0 {
            return Ok(());
        }

        validate_batch_schema(&self.schema, batch, self.batches).inspect_err(|_| {
            self.failed = true;
        })?;

        if let Some(order) = &mut self.order {
            let sorted = order.accept(batch).inspect_err(|_| {
                self.failed = true;
            })?;
            if !sorted {
                self.failed = true;
                return Err(BlockStoreError::InvalidBlock(format!(
                    "batch {} of block `{}` is out of the declared sort order {:?}; a streaming \
                     writer cannot reorder rows it has already encoded",
                    self.batches, self.object_key, self.sort_key
                )));
            }
        }

        self.summary
            .push(batch, &self.columns)
            .inspect_err(|_| self.failed = true)?;
        self.writer.write(batch).await.inspect_err(|_| {
            self.failed = true;
        })?;
        self.batches += 1;
        Ok(())
    }

    /// Closes the block and returns the metadata of what was written.
    ///
    /// # Errors
    /// Returns [`BlockStoreError::InvalidBlock`] when an earlier call failed or
    /// no row was ever written, and an object-store or Parquet error when the
    /// footer cannot be written.
    pub async fn finish(self) -> Result<BlockMeta> {
        if self.failed {
            return Err(self.poisoned());
        }
        // Ask the summary first: it is what knows whether the block has any
        // rows, and closing an empty one would leave a queryless object at the
        // key instead of leaving the upload unfinished.
        let (min_ts, max_ts, row_count, fingerprints) = self.summary.finish()?;
        self.writer.close().await?;

        Ok(BlockMeta {
            tenant: self.tenant,
            object_key: self.object_key,
            min_ts,
            max_ts,
            row_count,
            fingerprints,
            // A writer encodes rows; it never sees where they came from. An
            // index promotes the record when it registers a block against the
            // blocks it replaces, which is the only place the inputs -- and so
            // the level -- are known.
            level: BlockLevel::INGESTED,
        })
    }

    fn poisoned(&self) -> BlockStoreError {
        BlockStoreError::InvalidBlock(format!(
            "block `{}` was abandoned by an earlier failed write",
            self.object_key
        ))
    }
}
