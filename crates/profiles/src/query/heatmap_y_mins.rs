use super::*;

pub(crate) fn heatmap_y_mins(
    min_value: MinValue,
    max_value: MaxValue,
    value_buckets: usize,
) -> Vec<f64> {
    if value_buckets == 0 {
        return Vec::new();
    }
    let span = max_value
        .0
        .checked_sub(min_value.0)
        .unwrap_or(i64::MAX)
        .max(0)
        .to_f64()
        .unwrap_or(f64::MAX);
    let min_value = min_value.0.to_f64().unwrap_or_else(|| {
        if min_value.0.is_negative() {
            f64::MIN
        } else {
            f64::MAX
        }
    });
    let bucket_count = value_buckets.to_f64().unwrap_or(f64::MAX);
    (0..value_buckets)
        .map(|bucket| min_value + span * bucket.to_f64().unwrap_or(f64::MAX) / bucket_count)
        .collect()
}
