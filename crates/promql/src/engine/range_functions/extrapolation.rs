use std::collections::BTreeMap;

use super::{RangeFn, Time, TimeExt, ToPrimitive};
use crate::functions::extrapolate::{
    RangeKind, RateWindow, WindowBounds, WindowSpacing, window_spacing,
};

pub(crate) fn extrapolated_rate(window: RateWindow<'_>, kind: RangeFn) -> Option<f64> {
    extrapolated_rate_with_starts(window, &BTreeMap::new(), kind)
}

pub(crate) fn extrapolated_rate_with_starts(
    window: RateWindow<'_>,
    starts: &BTreeMap<i64, i64>,
    kind: RangeFn,
) -> Option<f64> {
    let kind = match kind {
        RangeFn::Rate => RangeKind::Rate,
        RangeFn::Increase => RangeKind::Increase,
        RangeFn::Delta => RangeKind::Delta,
        _ => return None,
    };
    crate::functions::extrapolate::extrapolated_rate_with_starts(window, starts, kind)
}

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
    let last_ts = timestamps[n - 1];
    let WindowSpacing {
        mut sampled_interval,
        average_duration_between_samples,
        extrapolation_threshold,
        mut duration_to_start,
        mut duration_to_end,
    } = window_spacing(
        timestamps,
        WindowBounds {
            range_start_ms,
            range_end_ms,
        },
    )?;

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

pub(crate) fn extrapolated_histogram_component(
    extrapolation: &HistogramExtrapolation<'_>,
    values: &[f64],
) -> Option<f64> {
    if matches!(extrapolation.kind, RangeFn::Delta) {
        return extrapolated_rate(
            RateWindow {
                timestamps: extrapolation.timestamps,
                values,
                bounds: WindowBounds {
                    range_start_ms: extrapolation.range_start_ms,
                    range_end_ms: extrapolation.range_end_ms,
                },
                range: extrapolation.range,
            },
            extrapolation.kind,
        );
    }

    let n = extrapolation.timestamps.len();
    if n == 0 || values.len() != n {
        return None;
    }
    let mut result = values[n - 1] - values[0];
    for &reset_index in extrapolation.reset_indices {
        result += values.get(reset_index.checked_sub(1)?)?;
    }

    if extrapolation.start_timestamp_ms.is_some() {
        result += values[0];
    }
    extrapolate_histogram_delta(extrapolation, result)
}

pub(crate) struct HistogramExtrapolation<'a> {
    pub(crate) start_timestamp_ms: Option<i64>,
    pub(crate) timestamps: &'a [i64],
    pub(crate) reset_indices: &'a [usize],
    pub(crate) range_start_ms: i64,
    pub(crate) range_end_ms: i64,
    pub(crate) range: Time,
    pub(crate) kind: RangeFn,
    pub(crate) duration_to_zero: Option<f64>,
}
