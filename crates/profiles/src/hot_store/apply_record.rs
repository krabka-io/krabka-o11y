use krabka_pprof::{ProfileIdentity, ProfileLabel, SampleValues, StacktraceIdentity};

use super::{
    InMemoryProfileStore, ProfileRecord, ProfilesError, intern_record, profile_timestamp_ms,
};

/// Intern and push every sample of `record` into `store`.
pub(crate) fn apply_record(
    store: &mut InMemoryProfileStore,
    record: &ProfileRecord,
    position: Option<crate::wal::WalPosition>,
) -> Result<(), ProfilesError> {
    let stack_ids = intern_record(store.symbols_mut(), record)?;
    let total_value = record.samples.iter().map(|sample| sample.value).sum();
    let mut stack_ids = stack_ids.into_iter();
    let mut ordinal = 0_usize;
    for sample in &record.samples {
        let stack_id = stack_ids
            .next()
            .expect("intern_record returns one stack ID per sample");
        let timestamp_ms = profile_timestamp_ms(sample.timestamp_ns);
        store.push_sample_with_provenance(
            ProfileIdentity::new(&record.tenant, &record.profile_type),
            record
                .labels
                .iter()
                .map(|label| ProfileLabel::new(label.name.clone(), label.value.clone()))
                .collect(),
            StacktraceIdentity::new(crate::blockbuilder::STACKTRACE_PARTITION, stack_id),
            SampleValues::new(sample.value, total_value),
            timestamp_ms,
            krabka_pprof::SampleProvenance::new(
                sample.span_id,
                sample.trace_id.clone(),
                crate::blockbuilder::wal_sample_ids(position, ordinal)?,
            ),
        );
        ordinal = ordinal.saturating_add(1);
    }
    Ok(())
}
