use super::*;

/// Query-range values available to Prometheus duration expressions.
///
/// `start` and `end` are epoch-millisecond instants. `step` is the grid
/// resolution, an extent. This type is not `Eq`, because [`Time`] stores `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DurationExprContext {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) step: Time,
}

impl DurationExprContext {
    #[must_use]
    pub fn instant(time_ms: i64) -> Self {
        Self {
            start_ms: time_ms,
            end_ms: time_ms,
            step: Time::ZERO,
        }
    }

    #[must_use]
    pub fn range(start_ms: i64, end_ms: i64, step: Time) -> Self {
        Self {
            start_ms,
            end_ms,
            step,
        }
    }
}
