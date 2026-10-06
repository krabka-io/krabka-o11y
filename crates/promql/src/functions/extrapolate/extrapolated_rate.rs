use std::collections::BTreeMap;

use super::{RangeKind, Time, extrapolated_rate_with_starts};

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
    extrapolated_rate_with_starts(
        timestamps,
        values,
        &BTreeMap::new(),
        range_start_ms,
        range_end_ms,
        range,
        kind,
    )
}
