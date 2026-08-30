use super::*;

pub(crate) fn validate_loki_query_range_resolution(
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
) -> Result<(), HttpQueryError> {
    if !matches!(kind, QueryKind::Range) {
        return Ok(());
    }
    let step_ns = resolved_range_step(params.step, time_range)?;
    let query_range = time_range
        .end_ns
        .checked_sub(time_range.start_ns)
        .map(Time::from_nanos)
        .ok_or(HttpQueryError::QueryResolutionTooHigh)?;
    // Loki truncates the point count, so the division stays over whole
    // nanoseconds rather than fractional seconds.
    if query_range.nanos_i64() / step_ns > LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS {
        return Err(HttpQueryError::QueryResolutionTooHigh);
    }
    Ok(())
}
