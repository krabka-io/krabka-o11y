use std::collections::BTreeMap;

use super::{Time, WindowBounds, extrapolated_rate_with_starts};

/// The instant functions evaluated over only the last two samples of the
/// window: `irate` and `idelta`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InstantKind {
    /// Per-second instant rate from the last two samples, reset-clamped.
    Irate,
    /// Difference of the last two samples. This is a gauge function with no
    /// per-second division.
    Idelta,
}

/// The reset-correcting, windowed range functions evaluated over a full
/// `(t-range, t]` window: `rate`, `increase`, and `delta`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RangeKind {
    /// Per-second average rate of increase, counter-reset corrected.
    Rate,
    /// Total increase over the window, counter-reset corrected.
    Increase,
    /// Difference between the first and last sample. This is a gauge function
    /// with no reset correction.
    Delta,
}

impl RangeKind {
    /// Returns `true` when this function treats the series as a monotonic
    /// counter. A counter function applies counter-reset correction and the
    /// positive zero-anchor clamp.
    pub(crate) fn is_counter(self) -> bool {
        matches!(self, Self::Rate | Self::Increase)
    }
}

/// One series' float samples in a rate-family window, and the window itself.
#[derive(Clone, Copy)]
pub(crate) struct RateWindow<'a> {
    /// Sample timestamps in epoch milliseconds, ascending.
    pub(crate) timestamps: &'a [i64],
    /// Sample values, one per timestamp.
    pub(crate) values: &'a [f64],
    /// The window's bounds.
    pub(crate) bounds: WindowBounds,
    /// The selector range the rate is taken over.
    pub(crate) range: Time,
}

/// Prometheus' extrapolated range estimator, shared by `rate`/`increase`/`delta`.
///
/// This is a direct port of the engine's `extrapolated_rate`.
#[must_use]
pub fn extrapolated_rate(window: RateWindow<'_>, kind: RangeKind) -> Option<f64> {
    extrapolated_rate_with_starts(window, &BTreeMap::new(), kind)
}
