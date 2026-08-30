use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, Labels, StreamPlan, Value, WalLogRecord,
    append_matching_hot_log_record, json, sort_loki_stream_values,
};

pub(crate) fn execute_tail_query_with_frontier_and_deletes(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Value {
    let mut streams: BTreeMap<Labels, Vec<[String; 2]>> = BTreeMap::new();
    for record in hot_tail {
        append_matching_hot_log_record(&mut streams, plan, record, frontier, delete_filters);
    }
    sort_loki_stream_values(&mut streams);

    json!({
        "streams": streams
            .into_iter()
            .map(|(stream, values)| json!({
                "stream": stream,
                "values": values,
            }))
            .collect::<Vec<_>>()
    })
}
