use super::{
    BooleanArray, COL_FINGERPRINT, COL_TIMESTAMP, Int64Array, MetricCompactionError, RecordBatch,
    UInt64Array, filter_record_batch, typed_column,
};

/// Drops the rows of `batch` that repeat the `(fingerprint, timestamp)` of the
/// row before them.
///
/// `carried` is the key of the last row kept, so a duplicate pair that straddles
/// two batches is dropped as well. The caller passes the same value to every
/// batch of one merged block, in order.
///
/// The merged rows arrive in the declared `(fingerprint, timestamp)` order, so
/// rows that share a key are next to each other and one pass over the batch
/// finds them all.
///
/// # Why a merge deduplicates at all
///
/// A block builder that crashed before it committed its WAL offsets re-emits
/// the same records under a different key, so two blocks can hold the same
/// `(fingerprint, timestamp)` rows. Today the `PromQL` engine tolerates that:
/// it keeps one row per key at query time. A merge that concatenated the
/// duplicates would bake them into one block and inflate its `row_count`, which
/// is the value [`plan_compactions`](super::plan_compactions) reads to decide a
/// block is full and the value a user sees as `TsdbBlock::num_samples`.
///
/// # Errors
/// Returns [`MetricCompactionError::Codec`] when `batch` carries no
/// non-null `series_fingerprint` and `timestamp` column of the declared type,
/// and [`MetricCompactionError::Arrow`] when the filtered batch cannot be
/// built.
pub(crate) fn deduplicate_series_timestamp_runs(
    batch: &RecordBatch,
    carried: &mut Option<(u64, i64)>,
) -> Result<RecordBatch, MetricCompactionError> {
    let fingerprints = typed_column::<UInt64Array>(batch, COL_FINGERPRINT)?;
    let timestamps = typed_column::<Int64Array>(batch, COL_TIMESTAMP)?;
    let mut keep = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let key = (fingerprints.value(row), timestamps.value(row));
        keep.push(*carried != Some(key));
        *carried = Some(key);
    }
    Ok(filter_record_batch(batch, &BooleanArray::from(keep))?)
}
