use super::{BlockDescriptor, CompactorRunError, LabelIndex};

pub(crate) fn insert_descriptor_labels(
    target: &mut LabelIndex,
    source: &LabelIndex,
    tenant: &str,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    for fingerprint in &descriptor.fingerprints {
        let labels = source.labels_for(tenant, *fingerprint).ok_or_else(|| {
            CompactorRunError::MissingSeriesLabels {
                tenant: tenant.to_string(),
                fingerprint: *fingerprint,
            }
        })?;
        target.insert_series(tenant.to_string(), labels.clone());
    }
    Ok(())
}
