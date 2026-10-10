use super::ToPrimitive;

/// How a non-empty extrapolation window's samples sit in its range, in seconds.
pub(crate) struct WindowSpacing {
    /// From the first to the last sample.
    pub(crate) sampled_interval: f64,
    pub(crate) average_duration_between_samples: f64,
    /// The gap beyond which Prometheus extrapolates only half a sample spacing.
    pub(crate) extrapolation_threshold: f64,
    /// From the range start to the first sample.
    pub(crate) duration_to_start: f64,
    /// From the last sample to the range end.
    pub(crate) duration_to_end: f64,
}

/// The bounds of an extrapolation window, in epoch milliseconds.
#[derive(Clone, Copy)]
pub(crate) struct WindowBounds {
    pub(crate) range_start_ms: i64,
    pub(crate) range_end_ms: i64,
}

/// Measures the sample spacing of `timestamps` within `bounds`.
///
/// `timestamps` must be non-empty. `None` means a duration does not fit an `f64`.
pub(crate) fn window_spacing(timestamps: &[i64], bounds: WindowBounds) -> Option<WindowSpacing> {
    let WindowBounds {
        range_start_ms,
        range_end_ms,
    } = bounds;
    let n = timestamps.len();
    let first_ts = timestamps[0];
    let last_ts = timestamps[n - 1];
    let sampled_interval = (last_ts - first_ts).to_f64()? / 1000.0;
    let average_duration_between_samples = if n > 1 {
        sampled_interval / (n - 1).to_f64()?
    } else {
        0.0
    };
    Some(WindowSpacing {
        sampled_interval,
        average_duration_between_samples,
        extrapolation_threshold: average_duration_between_samples * 1.1,
        duration_to_start: (first_ts - range_start_ms).to_f64()? / 1000.0,
        duration_to_end: (range_end_ms - last_ts).to_f64()? / 1000.0,
    })
}
