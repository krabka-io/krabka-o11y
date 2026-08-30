use super::{
    InMemoryProfileStore, ProfileRecord, ProfilesError, intern_record, profile_timestamp_ms,
};

/// Intern and push every sample of `record` into `store`.
pub(crate) fn apply_record(
    store: &mut InMemoryProfileStore,
    record: &ProfileRecord,
) -> Result<(), ProfilesError> {
    let stack_ids = intern_record(store.symbols_mut(), record)?;
    let total_value = record.samples.iter().map(|sample| sample.value).sum();
    for (sample, stack_id) in record.samples.iter().zip(stack_ids) {
        let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
        store.push_sample_with_total_and_associations(
            (&record.tenant, &record.profile_type),
            record.labels.clone(),
            (crate::blockbuilder::STACKTRACE_PARTITION, stack_id),
            (sample.value, total_value),
            timestamp_ms,
            (sample.span_id, sample.trace_id.clone()),
        );
    }
    Ok(())
}
