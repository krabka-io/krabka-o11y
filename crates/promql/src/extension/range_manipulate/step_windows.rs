use crate::RangeWindow;

/// The eval-step grid and input-row windows for a range-manipulation batch.
pub(crate) struct StepWindows {
    pub eval_timestamps_ms: Vec<i64>,
    pub ranges: Vec<RangeWindow>,
}
