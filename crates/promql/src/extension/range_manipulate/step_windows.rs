
/// The eval-step grid paired with the `(offset, len)` windows that index the
/// sorted input rows for each step.
pub(crate) type StepWindows = (Vec<i64>, Vec<(u32, u32)>);
