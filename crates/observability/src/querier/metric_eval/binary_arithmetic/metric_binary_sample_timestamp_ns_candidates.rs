use super::*;

pub(crate) fn metric_binary_sample_timestamp_ns_candidates(sample: &Value) -> Option<Vec<i64>> {
    let timestamp = sample.as_array()?.first()?;
    if let Some(timestamp) = timestamp.as_i64() {
        return Some(metric_binary_integer_timestamp_ns_candidates(timestamp));
    }
    if let Some(timestamp) = timestamp.as_u64() {
        return i64::try_from(timestamp)
            .ok()
            .map(metric_binary_integer_timestamp_ns_candidates);
    }
    if let Some(timestamp) = timestamp.as_f64() {
        let timestamp = timestamp * 1_000_000_000.0;
        return i64::from_f64(timestamp.round()).map(|timestamp| vec![timestamp]);
    }
    if let Some(timestamp) = timestamp.as_str() {
        let mut candidates = Vec::new();
        if let Some(timestamp) = parse_decimal_seconds_timestamp(timestamp) {
            candidates.push(timestamp);
        }
        if let Ok(timestamp) = timestamp.parse::<i64>() {
            candidates.extend(metric_binary_integer_timestamp_ns_candidates(timestamp));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if !candidates.is_empty() {
            return Some(candidates);
        }
    }
    None
}
