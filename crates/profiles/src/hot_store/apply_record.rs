use super::{
    InMemoryProfileStore, ProfileRecord, ProfilesError, intern_record, profile_timestamp_ms,
};

/// Intern and push every sample of `record` into `store`.
pub(crate) fn apply_record(
    store: &mut InMemoryProfileStore,
    record: &ProfileRecord,
    position: Option<(i32, i64)>,
) -> Result<(), ProfilesError> {
    let stack_ids = intern_record(store.symbols_mut(), record)?;
    let total_value = record.samples.iter().map(|sample| sample.value).sum();
    for (ordinal, (sample, stack_id)) in record.samples.iter().zip(stack_ids).enumerate() {
        let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
        store.push_sample_with_provenance(
            (&record.tenant, &record.profile_type),
            record.labels.clone(),
            (crate::blockbuilder::STACKTRACE_PARTITION, stack_id),
            (sample.value, total_value),
            timestamp_ms,
            (
                sample.span_id,
                sample.trace_id.clone(),
                position
                    .map(|(partition, offset)| {
                        u64::try_from(ordinal)
                            .map(|ordinal| crate::wal::sample_identity(partition, offset, ordinal))
                    })
                    .transpose()
                    .map_err(|error| ProfilesError::Decode(error.to_string()))?
                    .into_iter()
                    .collect(),
            ),
        );
    }
    Ok(())
}
