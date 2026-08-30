use super::*;

pub(crate) fn heatmap_time_buckets(
    start_ms: StartMs,
    end_ms: EndMs,
    step: Time,
    max_buckets: usize,
) -> Result<usize, ProfileError> {
    if start_ms.0 >= end_ms.0 {
        return Err(ProfileError::Plan(
            "heatmap start must be before end".to_string(),
        ));
    }
    // The bounds are instants and the step is an extent: only the step converts,
    // and the bucket walk stays exact integer arithmetic. `step_from_secs` has
    // already rejected a sub-millisecond step at the Connect boundary; this
    // guard keeps the division below safe for any other caller.
    if step < millis(1) {
        return Err(ProfileError::Plan("step must be >= 1ms".to_string()));
    }
    let step_ms = step.millis_i64();
    let span_ms = end_ms
        .0
        .checked_sub(start_ms.0)
        .ok_or_else(|| ProfileError::Plan("heatmap time range is too large".to_string()))?;
    let buckets = (span_ms / step_ms + i64::from(span_ms % step_ms != 0)).max(1);
    Ok(usize::try_from(buckets)
        .unwrap_or(max_buckets)
        .min(max_buckets))
}
