use super::{
    CompareSpec, HashSet, MetricsRange, RecordBatch, Result, TraceMetricsResponse, TraceqlError,
    accumulate_compare_counts, build_compare_series,
};

pub(crate) fn assemble_compare_response(
    batches: &[RecordBatch],
    compare: &CompareSpec,
    range: MetricsRange,
    max_values_per_attr: usize,
    selected_spans: Option<&HashSet<([u8; 16], [u8; 8])>>,
) -> Result<TraceMetricsResponse> {
    if range.step.0 <= 0 {
        return Err(TraceqlError::Plan("metrics step must be positive".into()));
    }
    if range.scan_end < range.scan_start {
        return Err(TraceqlError::Plan("metrics end must be >= start".into()));
    }
    let bucket_count = usize::try_from((range.scan_end.0 - range.scan_start.0) / range.step.0 + 1)
        .map_err(|e| TraceqlError::Plan(e.to_string()))?;

    let (counts, totals) = accumulate_compare_counts(
        batches,
        compare,
        range,
        bucket_count,
        max_values_per_attr,
        selected_spans,
    )?;
    let series = build_compare_series(counts, &totals, compare.top_n, range, bucket_count);
    Ok(TraceMetricsResponse { series })
}
