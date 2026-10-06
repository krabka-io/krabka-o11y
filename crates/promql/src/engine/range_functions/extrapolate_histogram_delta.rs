use super::{RangeFn, TimeExt, ToPrimitive};

pub(crate) fn extrapolate_histogram_delta(
    extrapolation: &super::HistogramExtrapolation<'_>,
    mut result: f64,
) -> Option<f64> {
    let super::HistogramExtrapolation {
        timestamps,
        range_start_ms,
        range_end_ms,
        range,
        kind,
        duration_to_zero,
        start_timestamp_ms,
        ..
    } = *extrapolation;
    let n = timestamps.len();
    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let mut sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    let average_duration_between_samples = if n > 1 {
        sampled_interval / (n - 1).to_f64()?
    } else {
        0.0
    };
    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut duration_to_start = (first_ts - range_start_ms).to_f64()? / 1000.0;
    let mut duration_to_end = (range_end_ms - last_ts).to_f64()? / 1000.0;

    if let Some(start) = start_timestamp_ms {
        duration_to_start = 0.0;
        sampled_interval = (last_ts - start).to_f64()? / 1000.0;
    } else {
        if n < 2 {
            return None;
        }
        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_duration_between_samples / 2.0;
        }
    }
    if sampled_interval <= 0.0 {
        return None;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
    }

    if start_timestamp_ms.is_none()
        && let Some(duration_to_zero) = duration_to_zero
        && duration_to_zero < duration_to_start
    {
        duration_to_start = duration_to_zero;
    }

    let extrapolated_interval = sampled_interval + duration_to_start + duration_to_end;
    result *= extrapolated_interval / sampled_interval;
    if kind == RangeFn::Rate {
        result /= range.secs_f64();
    }
    Some(result)
}
