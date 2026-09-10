use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, Labels, LokiStreamEncoding,
    LokiStreamEntry, StreamPlan, Value, WalLogRecord, append_matching_hot_log_record, json,
    loki_stream_results, sort_loki_stream_values,
};

/// Builds one tail frame, in the encoding the tail request asked for.
///
/// A tail frame carries the same two encodings a query answer does, and Loki is
/// not consistent about the default one: the frame it backfills when a tail
/// opens folds an entry's structured metadata into the stream's labels, as
/// `/query_range` does, while the frames it streams afterwards drop the
/// metadata instead of folding it. Krabka folds throughout, which is the
/// backfill's answer and the one that loses nothing.
pub(crate) fn execute_tail_query_with_frontier_and_deletes(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
    encoding: LokiStreamEncoding,
) -> Value {
    let mut streams: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    json!({ "streams": loki_stream_results(streams, encoding) })
}
