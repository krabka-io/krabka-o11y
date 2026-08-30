use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn build_block(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
    partition: i32,
    records: &[ProfileRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, ProfilesError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    let mut symdb = SymbolDb::new();
    let mut rows = Vec::new();
    let mut fingerprints = BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    for rec in records {
        let stack_ids = intern_record(&mut symdb, rec)?;
        let fp = rec.series_fingerprint();
        fingerprints.insert(fp);
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

    let key = object_key(
        tenant,
        partition,
        offset_range.0,
        offset_range.1,
        min_ts,
        max_ts,
    );
    let batch = samples_batch(&rows)?;
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, batch.schema(), None)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        writer
            .write(&batch)
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        writer
            .close()
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
    }

    store
        .put(
            &Path::from(key.clone()),
            PutPayload::from(bytes.into_inner()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    store
        .put(
            &Path::from(format!("{key}.symdb")),
            PutPayload::from(symdb.encode()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    Ok(vec![BlockMeta {
        tenant: tenant.to_string(),
        object_key: key,
        min_ts,
        max_ts,
        row_count: rows.len(),
        fingerprints: fingerprints.into_iter().collect(),
    }])
}
