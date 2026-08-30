use super::{SearchPartial, SearchResponseJson, TraceJson, Metrics, merge_trace, apply_search_limits};

/// Merge search partials.
///
/// This reunions by `traceID`, accumulates metrics, then applies `limit`
/// newest-first and `spss` as a per-spanSet span cap. It preserves each
/// spanSet's `matched` count. It returns the merged `SearchResponseJson`, ready
/// to serialize.
#[must_use]
pub fn merge_search(partials: Vec<SearchPartial>, limit: usize, spss: usize) -> SearchResponseJson {
    let mut merged: Vec<TraceJson> = Vec::new();
    let mut metrics = Metrics::default();

    for p in partials {
        metrics.add(&p.metrics);
        for trace in p.traces {
            merge_trace(&mut merged, trace);
        }
    }

    apply_search_limits(&mut merged, limit, spss);
    SearchResponseJson {
        traces: merged,
        metrics,
    }
}
