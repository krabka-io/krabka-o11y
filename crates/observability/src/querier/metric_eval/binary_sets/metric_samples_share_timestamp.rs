use super::{Value, metric_binary_sample_timestamps_match};

pub(crate) fn metric_samples_share_timestamp(left_sample: &Value, right_sample: &Value) -> bool {
    metric_binary_sample_timestamps_match(left_sample, right_sample)
}
