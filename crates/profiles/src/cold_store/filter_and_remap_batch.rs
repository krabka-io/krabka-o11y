use super::{
    Arc, ArrayRef, AsArray, BTreeMap, Int64Type, ProfileError, RecordBatch, SeriesFingerprint,
    UInt64Array, UInt64Type, profile_samples_schema,
};

pub(crate) fn filter_and_remap_batch(
    batch: &RecordBatch,
    partition_map: &BTreeMap<u64, u64>,
    fps: &std::collections::BTreeSet<SeriesFingerprint>,
    profile_type: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<RecordBatch, ProfileError> {
    let fingerprints = batch.column(0).as_primitive::<UInt64Type>();
    let timestamps = batch.column(1).as_primitive::<Int64Type>();
    let profile_types = batch
        .column(2)
        .as_dictionary::<arrow::datatypes::Int32Type>();
    let profile_values = profile_types.values().as_string::<i32>();
    let partitions = batch.column(5).as_primitive::<UInt64Type>();
    // Single pass: collect the indices of all surviving rows once.
    let mut indices = Vec::new();
    for row in 0..batch.num_rows() {
        let profile_key = profile_types.keys().value(row);
        let profile_idx = usize::try_from(profile_key)
            .map_err(|err| ProfileError::Store(format!("profile type key invalid: {err}")))?;
        let row_profile_type = profile_values.value(profile_idx);
        let ts = timestamps.value(row);
        if fps.contains(&fingerprints.value(row))
            && row_profile_type == profile_type
            && ts >= start_ms
            && ts <= end_ms
        {
            let row = u64::try_from(row)
                .map_err(|err| ProfileError::Store(format!("row index does not fit u64: {err}")))?;
            indices.push(row);
        }
    }

    if indices.is_empty() {
        return Ok(RecordBatch::new_empty(profile_samples_schema()));
    }

    // Remap the partition column once over the whole batch (cheap, O(N)) and
    // build the input batch a single time, then `take` all surviving rows in one
    // call instead of rebuilding the full batch per surviving row (O(R*N)).
    // Look up each stored partition in the dense per-block map so already-compacted
    // high-bit partitions are re-based without OR-folding/aliasing.
    let remapped = UInt64Array::from_iter_values((0..batch.num_rows()).map(|idx| {
        let stored = partitions.value(idx);
        partition_map.get(&stored).copied().unwrap_or(stored)
    }));
    let mut cols = batch.columns().to_vec();
    cols[5] = Arc::new(remapped) as ArrayRef;
    let remapped_batch = RecordBatch::try_new(profile_samples_schema(), cols)
        .map_err(|err| ProfileError::Store(err.to_string()))?;

    arrow::compute::take_record_batch(&remapped_batch, &UInt64Array::from(indices))
        .map_err(|err| ProfileError::Store(err.to_string()))
}
