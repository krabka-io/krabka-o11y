use std::collections::BTreeMap;

use super::{RangeKind, RateWindow, extrapolated_rate_with_starts};

/// Prometheus' extrapolated range estimator, shared by `rate`/`increase`/`delta`.
///
/// This is a direct port of the engine's `extrapolated_rate`.
#[must_use]
pub fn extrapolated_rate(window: RateWindow<'_>, kind: RangeKind) -> Option<f64> {
    extrapolated_rate_with_starts(window, &BTreeMap::new(), kind)
}
