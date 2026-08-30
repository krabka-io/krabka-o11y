use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn metric_plan_scan_sql(
    plan: &StreamPlan,
    query: &MetricQuery,
    eval_range: TimeRange,
) -> Result<String, QueryError> {
    let scan_range = metric_scan_range(query, eval_range)?;
    Ok(stream_plan_scan_sql_for_time_range(plan, scan_range))
}
