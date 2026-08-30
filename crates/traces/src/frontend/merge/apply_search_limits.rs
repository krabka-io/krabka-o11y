use super::*;

/// Apply Tempo's post-merge `limit` and `spss` truncation.
///
/// This orders traces newest-first by `startTimeUnixNano` and keeps at most
/// `limit` of them. It then caps the `spans` of each kept trace's spanSets to
/// `spss`, and preserves each spanSet's `matched` count.
pub(crate) fn apply_search_limits(traces: &mut Vec<TraceJson>, limit: usize, spss: usize) {
    traces.sort_by(|a, b| {
        parse_nanos(&b.start_time_unix_nano).cmp(&parse_nanos(&a.start_time_unix_nano))
    });
    if limit > 0 {
        traces.truncate(limit);
    }
    if spss > 0 {
        for trace in traces.iter_mut() {
            for ss in &mut trace.span_sets {
                // `truncate` is already a no-op when the set is shorter.
                ss.spans.truncate(spss);
            }
        }
    }
}
