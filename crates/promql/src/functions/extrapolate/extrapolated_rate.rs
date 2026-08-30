use super::{RangeKind, Time, TimeExt, ToPrimitive};

/// Prometheus' extrapolated range estimator, shared by `rate`/`increase`/`delta`.
///
/// This is a direct port of the engine's `extrapolated_rate`.
#[must_use]
pub fn extrapolated_rate(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeKind,
) -> Option<f64> {
    let n = timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }

    let is_counter = kind.is_counter();

    let mut result = values[n - 1] - values[0];
    if is_counter {
        for window in values.windows(2) {
            if window[1] < window[0] {
                result += window[0];
            }
        }
    }

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

    if is_counter
        && matches!(result.partial_cmp(&0.0), Some(std::cmp::Ordering::Greater))
        && values[0] >= 0.0
    {
        let duration_to_zero = sampled_interval * (values[0] / result);
        duration_to_start = duration_to_start.min(duration_to_zero);
    }

    let extrapolate_to_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolate_to_interval / sampled_interval;
    if kind == RangeKind::Rate {
        let range_seconds = range.secs_f64();
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}
