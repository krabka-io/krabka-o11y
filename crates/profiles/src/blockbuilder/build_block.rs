use super::{
    Arc, BlockMeta, BlockWriter, BuiltSample, ObjectStore, ObjectStoreExt, ObjectStoreMetrics,
    ObjectStoreOperation, ObjectStoreRetryPolicy, Path, ProfileRecord, ProfilesError, PutPayload,
    STACKTRACE_PARTITION, SummaryColumns, SymbolDb, intern_record, object_key,
    profile_samples_decl, profile_timestamp_ms, retry_object_store, samples_batch,
};

/// Interns one WAL window's records into a symbol DB and writes the samples as
/// one profile block.
///
/// The block itself goes through [`BlockWriter`], so it is validated against
/// [`profile_samples_decl`], left in the declared sort order, zstd compressed,
/// cut into row groups, and streamed to object storage rather than buffered
/// whole in memory. Its [`BlockMeta`] is what the writer derives from the
/// block's own columns rather than a second, hand-kept tally.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn build_block(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
    partition: i32,
    records: &[ProfileRecord],
    offset_range: (i64, i64),
    metrics: &ObjectStoreMetrics,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut symdb = SymbolDb::new();
    let mut rows = Vec::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    for rec in records {
        let stack_ids = intern_record(&mut symdb, rec)?;
        let fp = rec.series_fingerprint();
        let total_value = rec.samples.iter().map(|sample| sample.value).sum();
        for (sample, stack_id) in rec.samples.iter().zip(stack_ids) {
            let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
            min_ts = min_ts.min(timestamp_ms);
            max_ts = max_ts.max(timestamp_ms);
            rows.push(BuiltSample {
                series_fingerprint: fp,
                timestamp_ns: timestamp_ms,
                profile_type: rec.profile_type.clone(),
                stacktrace_id: u64::from(stack_id),
                value: sample.value,
                stacktrace_partition: STACKTRACE_PARTITION,
                total_value,
                span_id: sample.span_id,
                trace_id: sample.trace_id.clone(),
            });
        }
    }

    // Into the order `profile_samples_decl` declares. `BlockWriter` enforces
    // it either way, but a WAL window arrives in arrival order, and sorting
    // the built rows is cheaper than the Arrow lexsort-and-take the writer
    // would otherwise have to do over every column of the block.
    rows.sort_by(|left, right| {
        (
            left.series_fingerprint,
            &left.profile_type,
            left.timestamp_ns,
        )
            .cmp(&(
                right.series_fingerprint,
                &right.profile_type,
                right.timestamp_ns,
            ))
    });

    // The object key carries the window's time bounds, so it is needed before
    // the block is written and the writer's own summary is available.
    let key = object_key(
        tenant,
        partition,
        offset_range.0,
        offset_range.1,
        min_ts,
        max_ts,
    );
    let batch = samples_batch(&rows)?;
    let meta = BlockWriter::new(Arc::clone(store))
        .write_block_with_decl(
            tenant,
            &key,
            batch.schema(),
            std::slice::from_ref(&batch),
            &profile_samples_decl(),
            SummaryColumns::series(),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    // The symbol side-car is a plain single-shot put, so unlike the block it
    // can be retried where it stands. Its key is derived from the block's, so
    // a retry overwrites rather than orphaning a half-written side-car.
    let symdb_key = Path::from(format!("{key}.symdb"));
    let symdb_payload = PutPayload::from(symdb.encode());
    retry_object_store(
        ObjectStoreRetryPolicy::DEFAULT,
        ObjectStoreOperation::Put,
        metrics,
        || store.put(&symdb_key, symdb_payload.clone()),
    )
    .await
    .map_err(|err| ProfilesError::Block(err.to_string()))?;

    Ok(vec![meta])
}
