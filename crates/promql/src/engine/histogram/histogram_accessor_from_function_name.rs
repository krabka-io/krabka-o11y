use super::HistogramAccessor;

/// Maps a native-histogram accessor function name to its [`HistogramAccessor`] variant.
///
/// This function mirrors the accessor arms of `PromqlEngine::eval_instant_call`.
/// It returns `None` for any other function, so the planner dispatch falls
/// through.
pub(crate) fn histogram_accessor_from_function_name(name: &str) -> Option<HistogramAccessor> {
    Some(match name {
        "histogram_count" => HistogramAccessor::Count,
        "histogram_sum" => HistogramAccessor::Sum,
        "histogram_avg" => HistogramAccessor::Avg,
        "histogram_stddev" => HistogramAccessor::Stddev,
        "histogram_stdvar" => HistogramAccessor::Stdvar,
        _ => return None,
    })
}
