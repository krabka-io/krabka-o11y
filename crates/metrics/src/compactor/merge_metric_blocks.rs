use super::{
    Arc, BTreeMap, BlockStoreError, BlockWriter, ByteSize, COL_FINGERPRINT, COL_TIMESTAMP,
    CompactionIndexManifest, CompactionIndexSink, CompactionObjectPlan, CompactionSeriesLabels,
    Labels, MERGE_BATCH_ROWS, MERGE_READ_BATCH_ROWS, MetricCompactionError, MetricCompactionJob,
    ObjectStore, SortedMerge, SummaryColumns, compacted_metric_object_key, compaction_index_key,
    deduplicate_series_timestamp_runs, open_block_stream, series_block_schema,
    versioned_compaction_key,
};

/// Merges the blocks one job names into a single block, and publishes its
/// manifest.
///
/// The inputs are each already in the declared `(fingerprint, timestamp)`
/// order, so they are merged rather than concatenated and sorted: the merge
/// holds one batch of each input, and the memory the pass needs follows the
/// output batch size rather than the bytes read.
///
/// Rows that repeat a `(fingerprint, timestamp)` are dropped. See
/// [`deduplicate_series_timestamp_runs`] for why a merge must do that.
///
/// The output manifest carries the level the job asked for, the offset window
/// its inputs cover, and the union of their series label sets. The label sets
/// are what the read path rebuilds its series index from, so an output that
/// dropped them would leave the merged block unreachable by any matcher.
///
/// The inputs are left in object storage. The caller deletes them once no
/// manifest names them, and not before.
///
/// # Errors
/// Returns an error when an input has no manifest, when the inputs disagree on
/// their schema, when an input exceeds `block_read_max` or cannot be read, or
/// when the output block or its manifest cannot be written.
pub(crate) async fn merge_metric_blocks<S>(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    index_sink: &S,
    manifests: &BTreeMap<&str, &CompactionIndexManifest>,
    planned: &MetricCompactionJob,
    block_read_max: ByteSize,
) -> Result<CompactionIndexManifest, MetricCompactionError>
where
    S: CompactionIndexSink + ?Sized,
{
    let inputs = planned
        .job
        .input_keys
        .iter()
        .map(|key| {
            manifests.get(key.as_str()).copied().ok_or_else(|| {
                MetricCompactionError::UnknownInput {
                    object_key: key.clone(),
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Every input is opened before any of it is read, so the output key can
    // name the exact object versions it was built from and the schemas can be
    // compared before a single row is decoded.
    let mut versions = Vec::with_capacity(inputs.len());
    let mut runs = Vec::with_capacity(inputs.len());
    let mut schema = None;
    for key in &planned.job.input_keys {
        let (meta, input_schema, batches) =
            open_block_stream(store.clone(), key, block_read_max, MERGE_READ_BATCH_ROWS).await?;
        match &schema {
            None => schema = Some(input_schema),
            Some(first) if *first == input_schema => {}
            Some(_) => {
                return Err(MetricCompactionError::SchemaMismatch {
                    first: planned.job.input_keys[0].clone(),
                    second: key.clone(),
                });
            }
        }
        versions.push(meta);
        runs.push(batches);
    }
    let schema = schema.ok_or_else(|| {
        BlockStoreError::InvalidBlock("cannot merge an empty set of blocks".to_string())
    })?;

    let block_key = versioned_compaction_key(
        &compacted_metric_object_key(&planned.job, planned.kind),
        &versions,
    );
    let decl = series_block_schema();
    let mut merge = SortedMerge::new(schema.clone(), &decl.sort_key, runs, MERGE_BATCH_ROWS)?;
    let mut block = block_writer.open_block(
        &planned.job.tenant,
        &block_key,
        schema,
        &decl,
        SummaryColumns::new(COL_FINGERPRINT, COL_TIMESTAMP),
    )?;

    let mut carried = None;
    let drained: Result<(), MetricCompactionError> = async {
        while let Some(merged) = merge.next_batch().await? {
            let deduplicated = deduplicate_series_timestamp_runs(&merged, &mut carried)?;
            block.write_batch(&deduplicated).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = drained {
        // No footer is written, so no readable object appears at the key. The
        // orphan sweep reclaims whatever bytes the aborted upload left.
        block.abort().await?;
        return Err(error);
    }

    let mut meta = block.finish().await?;
    // The writer stamps every block it closes as ingested, because it sees rows
    // and not their history. The job is what knows the level.
    meta.level = planned.job.output_level;
    let plan = CompactionObjectPlan {
        index_key: compaction_index_key(&block_key),
        block_key,
        // The offset window of the merged block is the window its inputs
        // covered together. A narrower one would claim the block holds less of
        // the WAL than it does.
        first_offset: inputs
            .iter()
            .map(|input| input.first_offset)
            .min()
            .unwrap_or_default(),
        last_offset: inputs
            .iter()
            .map(|input| input.last_offset)
            .max()
            .unwrap_or_default(),
        row_count: meta.row_count,
    };
    let series: BTreeMap<u64, Labels> = inputs
        .iter()
        .flat_map(|input| input.series.iter())
        .map(|series| (series.fingerprint, series.labels.clone()))
        .collect();
    let manifest = CompactionIndexManifest::from_block_meta(
        planned.kind,
        &plan,
        &meta,
        series
            .into_iter()
            .map(|(fingerprint, labels)| CompactionSeriesLabels {
                fingerprint,
                labels,
            })
            .collect(),
    );
    index_sink.write_manifest(&manifest).await?;
    Ok(manifest)
}
