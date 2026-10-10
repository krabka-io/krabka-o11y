use std::collections::BTreeMap;

use super::{
    RangeKind, RateWindow, TimeExt, ToPrimitive, WindowSpacing, start_timestamp_reset,
    window_spacing,
};

pub(crate) fn extrapolated_rate_with_starts(
    window: RateWindow<'_>,
    starts: &BTreeMap<i64, i64>,
    kind: RangeKind,
) -> Option<f64> {
    let RateWindow {
        timestamps,
        values,
        bounds,
        range,
    } = window;
    let n = timestamps.len();
    if n == 0 || values.len() != n {
        return None;
    }

    let is_counter = kind.is_counter();

    let mut result = values[n - 1] - values[0];
    if is_counter {
        for (index, pair) in values.windows(2).enumerate() {
            let previous_start = starts.get(&timestamps[index]).copied().unwrap_or(0);
            let start = starts.get(&timestamps[index + 1]).copied().unwrap_or(0);
            if pair[1] < pair[0]
                || start_timestamp_reset(
                    previous_start,
                    timestamps[index],
                    start,
                    timestamps[index + 1],
                )
            {
                result += pair[0];
            }
        }
    }

    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let WindowSpacing {
        mut sampled_interval,
        average_duration_between_samples,
        extrapolation_threshold,
        mut duration_to_start,
        mut duration_to_end,
    } = window_spacing(timestamps, bounds)?;

    let start = starts.get(&first_ts).copied().unwrap_or(0);
    if is_counter && start != 0 && start > bounds.range_start_ms && start < first_ts {
        duration_to_start = 0.0;
        sampled_interval = (last_ts - start).to_f64()? / 1000.0;
        result += values[0];
    } else {
        if n < 2 {
            return None;
        }
        if duration_to_start >= extrapolation_threshold {
            duration_to_start = average_duration_between_samples / 2.0;
        }
        if is_counter && result > 0.0 && values[0] >= 0.0 {
            let duration_to_zero = sampled_interval * (values[0] / result);
            duration_to_start = duration_to_start.min(duration_to_zero);
        }
    }
    if sampled_interval <= 0.0 {
        return None;
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = average_duration_between_samples / 2.0;
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
