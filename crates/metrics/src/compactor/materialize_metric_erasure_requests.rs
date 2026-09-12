use super::{
    Arc, BTreeSet, BlockDeletion, BlockWriter, ByteSize, COL_FINGERPRINT, COL_TIMESTAMP,
    CompactionIndexManifest, CompactionIndexSink, CompactionObjectPlan, CompactionSeriesLabels,
    ERASURE_REQUEST_PREFIX, ErasureRequest, Index, MetricBlockKind, MetricCompactionError,
    MetricCompactionPass, ObjectStore, StreamExt, SummaryColumns, UInt64Array, delete_blocks,
    delete_erasure_request, filter_record_batch, list_compaction_manifests, list_erasure_requests,
    open_block_stream, series_block_schema, versioned_compaction_key,
};
use crate::clock_reading_decl;

pub(super) async fn materialize_metric_erasure_requests<S>(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    index_sink: &S,
    block_read_max: ByteSize,
) -> Result<(MetricCompactionPass, Vec<String>), MetricCompactionError>
where
    S: CompactionIndexSink + ?Sized,
{
    let requests = list_erasure_requests(store, ERASURE_REQUEST_PREFIX).await?;
    if requests.is_empty() {
        return Ok((MetricCompactionPass::default(), Vec::new()));
    }

    let manifests = list_compaction_manifests(store).await?;
    let mut pass = MetricCompactionPass::default();
    let mut retired = Vec::new();
    for manifest in manifests {
        let mut index = Index::new();
        for series in &manifest.series {
            index.add_series(&manifest.tenant, series.fingerprint, &series.labels);
        }
        let mut applicable = Vec::new();
        for request in &requests {
            if let Some(fingerprints) = request_fingerprints(request, &manifest, &index)? {
                applicable.push((request, fingerprints));
            }
        }
        if applicable.is_empty() {
            continue;
        }
        let Some(rewrite) =
            rewrite_metric_block(store, block_writer, &manifest, &applicable, block_read_max)
                .await?
        else {
            continue;
        };
        if let Some(output) = rewrite {
            index_sink.write_manifest(&output).await?;
            pass.outputs.push(output);
        }
        retired.push((manifest.block_key, manifest.index_key));
    }
    let deletions = retired
        .iter()
        .map(|(_, index_key)| BlockDeletion {
            object_key: index_key.clone(),
            sidecars: Vec::new(),
        })
        .collect::<Vec<_>>();
    let rewrote_blocks = !retired.is_empty();
    let report = delete_blocks(store, &deletions).await;
    let failed = report
        .failures
        .iter()
        .map(|failure| failure.failed_key.as_str())
        .collect::<BTreeSet<_>>();
    let retired_blocks = retired
        .into_iter()
        .filter(|(_, index_key)| !failed.contains(index_key.as_str()))
        .map(|(block_key, _)| block_key)
        .collect();
    pass.manifests_retired = report.into();
    if !rewrote_blocks {
        for request in requests.iter().filter(|request| request.clean_requested) {
            delete_erasure_request(store, ERASURE_REQUEST_PREFIX, request).await?;
        }
    }
    Ok((pass, retired_blocks))
}

fn request_fingerprints(
    request: &ErasureRequest,
    manifest: &CompactionIndexManifest,
    index: &Index,
) -> Result<Option<BTreeSet<u64>>, MetricCompactionError> {
    if request.tenant != manifest.tenant {
        return Ok(None);
    }
    let min_ns = manifest.min_ts.saturating_mul(1_000_000);
    let max_ns = manifest.max_ts.saturating_mul(1_000_000);
    if !request.overlaps(min_ns, max_ns) {
        return Ok(None);
    }
    let mut fingerprints = BTreeSet::new();
    for matchers in &request.matcher_sets {
        fingerprints.extend(index.matching_fingerprints(&manifest.tenant, matchers)?);
    }
    Ok((!fingerprints.is_empty()).then_some(fingerprints))
}

async fn rewrite_metric_block(
    store: &Arc<dyn ObjectStore>,
    block_writer: &BlockWriter,
    manifest: &CompactionIndexManifest,
    requests: &[(&ErasureRequest, BTreeSet<u64>)],
    block_read_max: ByteSize,
) -> Result<Option<Option<CompactionIndexManifest>>, MetricCompactionError> {
    let (source_meta, schema, mut batches) = open_block_stream(
        store.clone(),
        &manifest.block_key,
        block_read_max,
        super::MERGE_READ_BATCH_ROWS,
    )
    .await?;
    let base = manifest.block_key.strip_suffix(".parquet").map_or_else(
        || format!("{}-erased", manifest.block_key),
        |stem| format!("{stem}-erased.parquet"),
    );
    let output_key = versioned_compaction_key(&base, &[source_meta]);
    let declaration = match manifest.kind {
        MetricBlockKind::ClockReadings => clock_reading_decl(),
        _ => series_block_schema(),
    };
    let mut output = block_writer.open_block(
        &manifest.tenant,
        &output_key,
        schema,
        &declaration,
        SummaryColumns::new(COL_FINGERPRINT, COL_TIMESTAMP),
    )?;
    let mut dropped = 0;
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        let fingerprints = batch
            .column_by_name(COL_FINGERPRINT)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                super::BlockStoreError::InvalidBlock(format!("`{COL_FINGERPRINT}` must be UInt64"))
            })?;
        let timestamps = batch
            .column_by_name(COL_TIMESTAMP)
            .and_then(|column| column.as_any().downcast_ref::<super::Int64Array>())
            .ok_or_else(|| {
                super::BlockStoreError::InvalidBlock(format!("`{COL_TIMESTAMP}` must be Int64"))
            })?;
        let keep = (0..batch.num_rows())
            .map(|row| {
                let fingerprint = fingerprints.value(row);
                let timestamp_ns = timestamps.value(row).saturating_mul(1_000_000);
                !requests.iter().any(|(request, targeted)| {
                    request.start_ns <= timestamp_ns
                        && request.end_ns >= timestamp_ns
                        && targeted.contains(&fingerprint)
                })
            })
            .collect::<Vec<_>>();
        dropped += keep.iter().filter(|keep| !**keep).count();
        output
            .write_batch(&filter_record_batch(
                &batch,
                &super::BooleanArray::from(keep),
            )?)
            .await?;
    }
    if dropped == 0 {
        output.abort().await?;
        return Ok(None);
    }
    let meta = match output.finish().await {
        Ok(meta) => meta,
        Err(super::BlockStoreError::InvalidBlock(message)) if message == "empty block" => {
            return Ok(Some(None));
        }
        Err(error) => return Err(error.into()),
    };
    let fingerprints = meta.fingerprints.iter().copied().collect::<BTreeSet<_>>();
    let series = manifest
        .series
        .iter()
        .filter(|series| fingerprints.contains(&series.fingerprint))
        .cloned()
        .collect::<Vec<CompactionSeriesLabels>>();
    let plan = CompactionObjectPlan {
        index_key: super::compaction_index_key(&output_key),
        block_key: output_key,
        first_offset: manifest.first_offset,
        last_offset: manifest.last_offset,
        row_count: meta.row_count,
    };
    let mut replacement =
        CompactionIndexManifest::from_block_meta(manifest.kind, &plan, &meta, series);
    replacement.level = manifest.level;
    Ok(Some(Some(replacement)))
}
