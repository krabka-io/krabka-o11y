use super::*;

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
