use super::{Value, metric_binary_sample_timestamp_ns_candidates};

pub(crate) fn metric_binary_sample_timestamps_match(
    left_sample: &Value,
    right_sample: &Value,
) -> bool {
    let left_candidates = metric_binary_sample_timestamp_ns_candidates(left_sample);
    let right_candidates = metric_binary_sample_timestamp_ns_candidates(right_sample);
    if let Some(left) = left_candidates {
        if let Some(right) = right_candidates {
            left.iter()
                .any(|left_timestamp| right.contains(left_timestamp))
        } else {
            false
        }
    } else if right_candidates.is_none() {
        left_sample.as_array().and_then(|sample| sample.first())
            == right_sample.as_array().and_then(|sample| sample.first())
    } else {
        false
    }
}
