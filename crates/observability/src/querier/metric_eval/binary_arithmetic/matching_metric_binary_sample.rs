use super::*;

pub(crate) fn matching_metric_binary_sample<'a>(
    left_sample: &Value,
    right_values: &'a [Value],
) -> Option<&'a Value> {
    right_values
        .iter()
        .find(|right_sample| metric_binary_sample_timestamps_match(left_sample, right_sample))
}
