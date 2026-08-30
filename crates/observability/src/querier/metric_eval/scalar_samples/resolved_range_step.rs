use super::*;

/// Resolves a range query's step in nanoseconds, defaulting it from the range.
///
/// `Loki` refuses a non-positive step outright rather than dividing by it, and
/// every range-vector response resolves its step through here.
pub(crate) fn resolved_range_step(
    step: Option<i64>,
    time_range: TimeRange,
) -> Result<i64, HttpQueryError> {
    let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    Ok(step_ns)
}
