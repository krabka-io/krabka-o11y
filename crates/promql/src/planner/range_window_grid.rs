use krabka_units::prelude::*;

use super::StepGrid;

/// The grid a range-selector fold evaluates at, and the window width it reads.
///
/// Each grid instant `t` reads the left-open window `(t - range, t]`.
#[derive(Clone, Copy, Debug)]
pub struct RangeWindowGrid {
    /// The evaluation instants.
    pub grid: StepGrid,
    /// The width of each instant's window.
    pub range: Time,
}

#[cfg(test)]
impl RangeWindowGrid {
    /// The one-point grid at `eval_time_ms` whose window reaches back to the
    /// epoch, `(0, eval_time_ms]`.
    pub(crate) fn since_epoch(eval_time_ms: i64) -> Self {
        Self {
            grid: StepGrid::instant(eval_time_ms, eval_time_ms),
            range: Time::from_millis(eval_time_ms),
        }
    }
}
