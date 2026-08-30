use super::{HistogramExtrapolation, RangeFn, extrapolate_histogram_delta, extrapolated_rate};

pub(crate) fn extrapolated_histogram_component(
    extrapolation: &HistogramExtrapolation<'_>,
    values: &[f64],
) -> Option<f64> {
    if matches!(extrapolation.kind, RangeFn::Delta) {
        return extrapolated_rate(
            extrapolation.timestamps,
            values,
            extrapolation.range_start_ms,
            extrapolation.range_end_ms,
            extrapolation.range,
            extrapolation.kind,
        );
    }

    let n = extrapolation.timestamps.len();
    if n < 2 || values.len() != n {
        return None;
    }
    let mut result = values[n - 1] - values[0];
    for &reset_index in extrapolation.reset_indices {
        result += values.get(reset_index.checked_sub(1)?)?;
    }

    extrapolate_histogram_delta(
        extrapolation.timestamps,
        result,
        extrapolation.range_start_ms,
        extrapolation.range_end_ms,
        extrapolation.range,
        extrapolation.kind,
    )
}
