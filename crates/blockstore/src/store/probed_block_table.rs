use super::{
    Arc, CachedParquetFileReaderFactory, DFParquetMetadata, DataFusionResult, DataSourceExec,
    ExecutionPlan, Expr, FileGroup, FileMetadataCache, FileScanConfigBuilder, LexOrdering,
    ObjectStore, ObjectStoreUrl, ParquetSource, PartitionedFile, ProbedBlock, SchemaRef, Session,
    Statistics, TableProvider, TableProviderFilterPushDown, TableType, async_trait,
    ordering_from_parquet_metadata,
};

/// A `DataFusion` table over blocks that a probe has already read.
///
/// `ctx.read_parquet(paths)` builds a `ListingTable`. That table resolves
/// each path with a `head` request when the query is planned, and it reads
/// each footer again through the runtime of the session. The probe has
/// already done both, so this table takes the probe's `ObjectMeta` and footer
/// instead. It reads the footer through the footer cache of the
/// [`BlockStore`](crate::BlockStore), and it makes no request until the plan
/// reads column chunks.
///
/// The physical plan is the plan that `ListingTable` builds for the same
/// files: one Parquet `DataSourceExec` with the table options of the session,
/// with the same file groups, file statistics and output ordering. Every
/// filter is `Inexact`, as `ListingTable` reports a filter on a table without
/// partition columns. `DataFusion` therefore keeps the `FilterExec` and hands
/// the predicate to the Parquet source, which prunes and filters rows.
#[derive(Debug)]
pub(crate) struct ProbedBlockTable {
    pub(crate) schema: SchemaRef,
    pub(crate) object_store_url: ObjectStoreUrl,
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) metadata_cache: Arc<FileMetadataCache>,
    pub(crate) blocks: Vec<ProbedBlock>,
}

impl ProbedBlockTable {
    /// One file per block, with the statistics and the ordering that
    /// `ListingTable` reads from the same footer when the session collects
    /// statistics.
    fn files(&self, collect_statistics: bool) -> DataFusionResult<Vec<PartitionedFile>> {
        self.blocks
            .iter()
            .map(|block| {
                let file = PartitionedFile::from(block.meta.clone());
                if !collect_statistics {
                    return Ok(
                        file.with_statistics(Arc::new(Statistics::new_unknown(&self.schema)))
                    );
                }
                let statistics = DFParquetMetadata::statistics_from_parquet_metadata(
                    &block.metadata,
                    &self.schema,
                )?;
                let ordering = ordering_from_parquet_metadata(&block.metadata, &self.schema)?;
                Ok(file
                    .with_statistics(Arc::new(statistics))
                    .with_ordering(ordering))
            })
            .collect()
    }
}

#[async_trait]
impl TableProvider for ProbedBlockTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let collect_statistics = state.config().collect_statistics();
        let groups = FileGroup::new(self.files(collect_statistics)?)
            .split_files(state.config().target_partitions())
            .into_iter()
            .map(|group| group_with_statistics(group, &self.schema, collect_statistics))
            .collect::<DataFusionResult<Vec<_>>>()?;
        let statistics = Statistics::try_merge_iter(
            groups
                .iter()
                .filter_map(|group| group.file_statistics(None)),
            &self.schema,
        )?;
        let ordering = common_ordering(&groups);

        let options = state.default_table_options().parquet;
        let mut source = ParquetSource::new(Arc::clone(&self.schema))
            .with_table_parquet_options(options.clone())
            .with_parquet_file_reader_factory(Arc::new(CachedParquetFileReaderFactory::new(
                Arc::clone(&self.store),
                Arc::clone(&self.metadata_cache),
            )));
        if let Some(hint) = options.global.metadata_size_hint {
            source = source.with_metadata_size_hint(hint);
        }

        let config = FileScanConfigBuilder::new(self.object_store_url.clone(), Arc::new(source))
            .with_file_groups(groups)
            .with_statistics(statistics)
            .with_projection_indices(projection.map(<[usize]>::to_vec))?
            .with_limit(limit)
            .with_output_ordering(ordering.into_iter().collect())
            .build();
        Ok(DataSourceExec::from_data_source(config))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// `group` with the merge of its file statistics attached, as `ListingTable`
/// attaches it.
fn group_with_statistics(
    group: FileGroup,
    schema: &SchemaRef,
    collect_statistics: bool,
) -> DataFusionResult<FileGroup> {
    if !collect_statistics {
        return Ok(group);
    }
    let statistics = Statistics::try_merge_iter(
        group.iter().filter_map(|file| file.statistics.as_deref()),
        schema,
    )?;
    Ok(group.with_statistics(Arc::new(statistics)))
}

/// The longest ordering prefix that every file declares, or `None` when one
/// file declares none or two files share no prefix.
///
/// This is the rule that `ListingTable` applies to the orderings it reads
/// from the footers.
fn common_ordering(groups: &[FileGroup]) -> Option<LexOrdering> {
    let mut files = groups.iter().flat_map(FileGroup::iter);
    let mut common = files.next()?.ordering.clone()?;
    for file in files {
        let ordering = file.ordering.as_ref()?;
        let prefix = common
            .iter()
            .zip(ordering.iter())
            .take_while(|(left, right)| left == right)
            .count();
        common = LexOrdering::new(common.iter().take(prefix).cloned())?;
    }
    Some(common)
}
