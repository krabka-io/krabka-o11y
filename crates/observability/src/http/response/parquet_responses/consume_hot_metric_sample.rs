use super::*;

pub(crate) fn consume_hot_metric_sample(
    hot_counts: &mut BTreeMap<(Labels, String), u64>,
    labels: &Labels,
    sample: &Value,
) -> bool {
    let Some(timestamp_key) = loki_metric_sample_timestamp_key(sample) else {
        return false;
    };
    let key = (labels.clone(), timestamp_key);
    let Some(count) = hot_counts.get_mut(&key) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}
