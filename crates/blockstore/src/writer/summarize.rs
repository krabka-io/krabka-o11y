use super::{
    Array, BTreeSet, BlockStoreError, FixedSizeBinaryArray, Int64Array, RecordBatch, Result,
    SeriesFingerprint, SummaryColumns, UInt64Array,
};

pub(crate) fn summarize(
    batches: &[RecordBatch],
    summary: &SummaryColumns,
) -> Result<(i64, i64, usize, Vec<SeriesFingerprint>)> {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut row_count = 0_usize;
    let mut fps: BTreeSet<SeriesFingerprint> = BTreeSet::new();

    for batch in batches {
        row_count += batch.num_rows();

        let ts = batch
            .column_by_name(&summary.ts_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing timestamp column".into()))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlockStoreError::InvalidBlock("timestamp not Int64".into()))?;
        let id = batch
            .column_by_name(&summary.id_col)
            .ok_or_else(|| BlockStoreError::InvalidBlock("missing identity column".into()))?;
        // Only the series/logs/metrics (`UInt64` fingerprint) path populates
        // `BlockMeta.fingerprints`; trace (span) blocks key the identity column
        // as `FixedSizeBinary` and never read `fingerprints`, so we skip the
        // per-row FNV pass entirely for them. `FixedSizeBinary` is still an
        // accepted id-column type — we just don't fingerprint it.
        let id_u64 = id.as_any().downcast_ref::<UInt64Array>();
        if id_u64.is_none() && id.as_any().downcast_ref::<FixedSizeBinaryArray>().is_none() {
            return Err(BlockStoreError::InvalidBlock(format!(
                "`{}` must be UInt64 or FixedSizeBinary",
                summary.id_col
            )));
        }

        for i in 0..batch.num_rows() {
            if !ts.is_null(i) {
                let v = ts.value(i);
                min_ts = min_ts.min(v);
                max_ts = max_ts.max(v);
            }
            if let Some(fp) = id_u64
                && !fp.is_null(i)
            {
                fps.insert(fp.value(i));
            }
        }
    }

    if row_count == 0 {
        return Err(BlockStoreError::InvalidBlock("empty block".into()));
    }

    Ok((min_ts, max_ts, row_count, fps.into_iter().collect()))
}
