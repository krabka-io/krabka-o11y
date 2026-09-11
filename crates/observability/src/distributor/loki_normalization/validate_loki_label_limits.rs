use super::{ByteSize, ByteSizeExt, DistributorError, Labels, Limits, loki_stale_sample_label_set};

/// Applies `Loki`'s three per-stream label caps.
///
/// `Loki` checks the label count, then each name's length, then each value's
/// length, and reports the first that fails. The messages are `Loki`'s own, so
/// a client that matches on them keeps working.
pub(crate) fn validate_loki_label_limits(
    labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if limits.max_label_names_per_series > 0 {
        let observed = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        if observed > limits.max_label_names_per_series {
            return Err(DistributorError::TooManyLabelNames {
                stream: loki_stale_sample_label_set(labels),
                observed,
                limit: limits.max_label_names_per_series,
            });
        }
    }
    for (name, value) in labels {
        if exceeds(name.len(), limits.max_label_name_length) {
            return Err(DistributorError::LabelNameTooLong {
                stream: loki_stale_sample_label_set(labels),
                name: name.clone(),
            });
        }
        if exceeds(value.len(), limits.max_label_value_length) {
            return Err(DistributorError::LabelValueTooLong {
                stream: loki_stale_sample_label_set(labels),
                value: value.clone(),
            });
        }
    }
    Ok(())
}

fn exceeds(observed: usize, limit: ByteSize) -> bool {
    limit > ByteSize::ZERO && observed > limit.bytes_usize()
}
