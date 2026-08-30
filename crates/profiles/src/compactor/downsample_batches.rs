use super::*;

pub(crate) fn downsample_batches(
    batches: &[RecordBatch],
    policy: DownsamplePolicy,
) -> Result<Vec<RecordBatch>, ProfilesError> {
    if policy.resolution_ns <= 0 {
        return Err(ProfilesError::Block(
            "downsample resolution must be positive".to_string(),
        ));
    }

    let mut values: BTreeMap<DownsampleKey, (i64, i64)> = BTreeMap::new();
    for batch in batches {
        let fp_idx = batch.schema().column_with_name(COL_FINGERPRINT).unwrap().0;
        let ts_idx = batch.schema().column_with_name(COL_TIMESTAMP).unwrap().0;
        let profile_idx = batch
            .schema()
            .column_with_name(PCOL_PROFILE_TYPE)
            .unwrap()
            .0;
        let stack_idx = batch
            .schema()
            .column_with_name(PCOL_STACKTRACE_ID)
            .unwrap()
            .0;
        let value_idx = batch.schema().column_with_name(PCOL_VALUE).unwrap().0;
        let partition_idx = batch
            .schema()
            .column_with_name(PCOL_STACKTRACE_PARTITION)
            .unwrap()
            .0;
        let total_idx = batch.schema().column_with_name(PCOL_TOTAL_VALUE).unwrap().0;
        let span_idx = batch.schema().column_with_name(PCOL_SPAN_ID).unwrap().0;
        let trace_idx = batch.schema().column_with_name(PCOL_TRACE_ID).unwrap().0;

        let fingerprints = batch.column(fp_idx).as_primitive::<UInt64Type>();
        let timestamps = batch.column(ts_idx).as_primitive::<Int64Type>();
        let profile_types = batch.column(profile_idx).as_dictionary::<Int32Type>();
        let profile_values = profile_types.values().as_string::<i32>();
        let stacktrace_ids = batch.column(stack_idx).as_primitive::<UInt64Type>();
        let sample_values = batch.column(value_idx).as_primitive::<Int64Type>();
        let partitions = batch.column(partition_idx).as_primitive::<UInt64Type>();
        let total_values = batch.column(total_idx).as_primitive::<Int64Type>();
        let span_identifiers = batch.column(span_idx).as_primitive::<UInt64Type>();
        let trace_identifiers = batch
            .column(trace_idx)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| ProfilesError::Block(format!("`{PCOL_TRACE_ID}` must be Binary")))?;

        for row in 0..batch.num_rows() {
            let profile_key = profile_types.keys().value(row);
            let profile_pos = usize::try_from(profile_key).map_err(|err| {
                ProfilesError::Block(format!("profile type key invalid during downsample: {err}"))
            })?;
            let timestamp = timestamps
                .value(row)
                .div_euclid(policy.resolution_ns)
                .saturating_mul(policy.resolution_ns);
            let key = DownsampleKey {
                series_fingerprint: fingerprints.value(row),
                timestamp,
                profile_type: profile_values.value(profile_pos).to_string(),
                stacktrace_id: stacktrace_ids.value(row),
                stacktrace_partition: partitions.value(row),
                span_id: (!span_identifiers.is_null(row)).then(|| span_identifiers.value(row)),
                trace_id: (!trace_identifiers.is_null(row))
                    .then(|| trace_identifiers.value(row).to_vec()),
            };
            let entry = values.entry(key).or_insert((0, 0));
            entry.0 += sample_values.value(row);
            entry.1 += total_values.value(row);
        }
    }

    let rows = values
        .into_iter()
        .map(|(key, (value, total_value))| ProfileSampleRow {
            series_fingerprint: key.series_fingerprint,
            timestamp: key.timestamp,
            profile_type: key.profile_type,
            stacktrace_id: key.stacktrace_id,
            value,
            stacktrace_partition: key.stacktrace_partition,
            total_value,
            span_id: key.span_id,
            trace_id: key.trace_id,
        })
        .collect::<Vec<_>>();
    encode_profile_samples(&rows)
        .map(|batch| vec![batch])
        .map_err(|err| ProfilesError::Block(err.to_string()))
}
