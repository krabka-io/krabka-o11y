use super::*;

pub(crate) fn collect_meta(
    batch: &RecordBatch,
    fingerprints: &mut BTreeSet<u64>,
    min_ts: &mut i64,
    max_ts: &mut i64,
) {
    let fp_idx = batch.schema().column_with_name(COL_FINGERPRINT).unwrap().0;
    let ts_idx = batch.schema().column_with_name(COL_TIMESTAMP).unwrap().0;
    let fps = batch.column(fp_idx).as_primitive::<UInt64Type>();
    let timestamps = batch.column(ts_idx).as_primitive::<Int64Type>();
    for row in 0..batch.num_rows() {
        fingerprints.insert(fps.value(row));
        *min_ts = (*min_ts).min(timestamps.value(row));
        *max_ts = (*max_ts).max(timestamps.value(row));
    }
}
