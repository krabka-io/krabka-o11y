use super::{ByteSize, ByteSizeExt, DistributorError, Labels, Limits, loki_stale_sample_label_set};

pub(crate) fn validate_structured_metadata_limits(
    metadata: &Labels,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    let observed_count = metadata.len();
    if limits.max_structured_metadata_entries_count > 0
        && u64::try_from(observed_count).unwrap_or(u64::MAX)
            > limits.max_structured_metadata_entries_count
    {
        return Err(DistributorError::TooManyStructuredMetadataLabels {
            stream: loki_stale_sample_label_set(stream_labels),
            observed: observed_count,
            limit: limits.max_structured_metadata_entries_count,
        });
    }
    let observed_size = metadata
        .iter()
        .map(|(name, value)| name.len() + value.len())
        .sum();
    if limits.max_structured_metadata_size > ByteSize::ZERO
        && observed_size > limits.max_structured_metadata_size.bytes_usize()
    {
        return Err(DistributorError::StructuredMetadataTooLarge {
            stream: loki_stale_sample_label_set(stream_labels),
            observed: observed_size,
            limit: limits.max_structured_metadata_size.bytes_usize(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_count_and_byte_caps_are_enforced() {
        let labels = Labels::from([("app".into(), "api".into())]);
        let metadata = Labels::from([("a".into(), "bc".into()), ("d".into(), "ef".into())]);
        let mut limits = Limits::unenforced();
        limits.max_structured_metadata_entries_count = 1;
        assert!(matches!(
            validate_structured_metadata_limits(&metadata, &labels, &limits),
            Err(DistributorError::TooManyStructuredMetadataLabels { .. })
        ));
        limits.max_structured_metadata_entries_count = 0;
        limits.max_structured_metadata_size = krabka_units::bytes(5);
        assert!(matches!(
            validate_structured_metadata_limits(&metadata, &labels, &limits),
            Err(DistributorError::StructuredMetadataTooLarge { .. })
        ));
    }
}
