use super::*;

/// A `DataFusion` table over a planned set of log blocks.
///
/// The plan -- which blocks a query has to read -- is settled by the label and
/// block indexes before the provider is built. What is left is the scan, and
/// both sources do it the same way: a `ListingTable` over the planned Parquet
/// files. `DataFusion` then owns projection, predicate and limit pushdown, and
/// the parallelism across files, for the object store exactly as it already
/// did for the local filesystem.
#[derive(Debug)]
pub struct LogBlockTableProvider {
    pub(crate) schema: Arc<Schema>,
    pub(crate) planned_blocks: Vec<BlockDescriptor>,
    pub(crate) source: LogBlockTableSource,
    /// Largest on-disk block this provider will open. Only the object-store
    /// source enforces it: a local block is not shared storage and cannot be
    /// written by anyone the querier does not already trust.
    pub(crate) block_read_max: ByteSize,
}

impl LogBlockTableProvider {
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new(
        root: impl AsRef<Path>,
        blocks: &[BlockDescriptor],
    ) -> Result<Self, BlockStoreError> {
        let schema = log_block_schema();
        let listing_table = planned_log_listing_table(root, blocks, Arc::clone(&schema))?;
        Ok(Self {
            schema,
            planned_blocks: blocks.to_vec(),
            source: LogBlockTableSource::Local(Box::new(listing_table)),
            block_read_max: DEFAULT_BLOCK_READ_MAX,
        })
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new_object_store(
        store: Arc<dyn ObjectStore>,
        prefix: &ObjectPath,
        blocks: &[BlockDescriptor],
    ) -> Result<Self, BlockStoreError> {
        Self::try_new_object_store_with_block_read_max(
            store,
            prefix,
            blocks,
            DEFAULT_BLOCK_READ_MAX,
        )
    }

    /// As [`Self::try_new_object_store`], with the cap on a single block's
    /// on-disk size given rather than defaulted.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new_object_store_with_block_read_max(
        store: Arc<dyn ObjectStore>,
        prefix: &ObjectPath,
        blocks: &[BlockDescriptor],
        block_read_max: ByteSize,
    ) -> Result<Self, BlockStoreError> {
        validate_planned_blocks(blocks)?;

        let schema = log_block_schema();
        let block_paths = blocks
            .iter()
            .map(|block| log_block_object_path(prefix, &block.key))
            .collect::<Vec<_>>();
        let object_store_url = next_log_block_object_store_url()?;
        let listing_table =
            object_store_log_listing_table(&object_store_url, &block_paths, Arc::clone(&schema))?;
        Ok(Self {
            schema,
            planned_blocks: blocks.to_vec(),
            source: LogBlockTableSource::ObjectStore {
                store,
                object_store_url,
                block_paths,
                listing_table: Box::new(listing_table),
            },
            block_read_max,
        })
    }

    #[must_use]
    pub fn planned_blocks(&self) -> &[BlockDescriptor] {
        &self.planned_blocks
    }
}

#[async_trait]
impl TableProvider for LogBlockTableProvider {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        match &self.source {
            LogBlockTableSource::Local(listing_table) => {
                listing_table.scan(state, projection, filters, limit).await
            }
            LogBlockTableSource::ObjectStore {
                store,
                object_store_url,
                block_paths,
                listing_table,
            } => {
                // The provider holds the store, so the session cannot resolve
                // the listing table's paths until the provider says which
                // authority they belong to. Registering here rather than at
                // construction is what lets the provider be built without a
                // session at all.
                let url: &Url = object_store_url.as_ref();
                state
                    .runtime_env()
                    .register_object_store(url, Arc::clone(store));
                head_log_blocks_within_cap(store.as_ref(), block_paths, self.block_read_max)
                    .await
                    .map_err(|error| DataFusionError::External(Box::new(error)))?;
                listing_table.scan(state, projection, filters, limit).await
            }
        }
    }

    /// Every filter over a scalar block column is `Inexact`, and nothing is
    /// `Exact`.
    ///
    /// `Inexact` is a claim that the scan *may* drop rows the filter rejects,
    /// and that the caller must still apply it. That is exactly what a Parquet
    /// scan does: the predicate prunes row groups and pages by their
    /// statistics, and, where `pushdown_filters` applies it as a `RowFilter`,
    /// rejects rows during decode -- but neither is a promise, and `DataFusion`
    /// decides which of them it can use only when it builds the physical plan.
    /// `Exact` would let it drop the `FilterExec` on the strength of this
    /// answer alone, and any predicate the scan then declined to apply would
    /// silently return rows that do not match. Under-claiming costs a filter
    /// pass over rows the scan already rejected; over-claiming returns wrong
    /// answers, so the claim stays `Inexact` for all three of
    /// `series_fingerprint`, `timestamp_ns` and `line`.
    ///
    /// `structured_metadata` is `Unsupported`, not `Inexact`: it is a
    /// `Map<Utf8, Utf8>`, Parquet keeps no useful statistics for it, and no
    /// predicate over it can prune anything. Handing it down would only ask
    /// the scan to carry a predicate it cannot act on. A filter that
    /// references no column at all -- a bare literal -- is `Unsupported` for
    /// the same reason: there is nothing in the files for it to prune by.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if filter_references_only_pushdown_columns(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}
