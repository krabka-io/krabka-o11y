use super::*;

#[derive(Debug)]
pub struct LogBlockTableProvider {
    pub(crate) schema: Arc<Schema>,
    pub(crate) planned_blocks: Vec<BlockDescriptor>,
    pub(crate) source: LogBlockTableSource,
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
        })
    }

    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub fn try_new_object_store(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        blocks: &[BlockDescriptor],
    ) -> Result<Self, BlockStoreError> {
        validate_planned_blocks(blocks)?;
        Ok(Self {
            schema: log_block_schema(),
            planned_blocks: blocks.to_vec(),
            source: LogBlockTableSource::ObjectStore { store, prefix },
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
            LogBlockTableSource::ObjectStore { store, prefix } => {
                let mut partitions = Vec::with_capacity(self.planned_blocks.len());
                for block in &self.planned_blocks {
                    let rows = read_log_block_from_object_store(store.as_ref(), prefix, &block.key)
                        .await
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    partitions.push(vec![
                        rows_to_batch(&rows, Arc::clone(&self.schema))
                            .map_err(|error| DataFusionError::External(Box::new(error)))?,
                    ]);
                }

                let table = MemTable::try_new(Arc::clone(&self.schema), partitions)?;
                table.scan(state, projection, filters, limit).await
            }
        }
    }

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
