use super::{Time, RangeFn, ToPrimitive, TimeExt};

pub(crate) fn extrapolate_histogram_delta(
    timestamps: &[i64],
    mut result: f64,
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let n = timestamps.len();
    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    if sampled_interval <= 0.0 {
        return None;
    }

    let average_duration_between_samples = sampled_interval / (n - 1).to_f64()?;
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms).to_f64()? / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts).to_f64()? / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = average_duration_between_samples / 2.0;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    let extrapolated_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolated_interval / sampled_interval;
    if kind == RangeFn::Rate {
        result /= range.secs_f64();
    }
    Some(result)
}
