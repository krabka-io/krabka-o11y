use super::{Value, metric_binary_sample_timestamp_ns_candidates};

pub(crate) fn metric_binary_sample_timestamps_match(
    left_sample: &Value,
    right_sample: &Value,
) -> bool {
    match (
        metric_binary_sample_timestamp_ns_candidates(left_sample),
        metric_binary_sample_timestamp_ns_candidates(right_sample),
    ) {
        (Some(left), Some(right)) => left
            .iter()
            .any(|left_timestamp| right.contains(left_timestamp)),
        (None, None) => {
            left_sample.as_array().and_then(|sample| sample.first())
                == right_sample.as_array().and_then(|sample| sample.first())
        }
        _ => false,
    }
}
