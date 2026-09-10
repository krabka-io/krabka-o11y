use super::{
    BTreeMap, Labels, LokiStreamEncoding, LokiStreamEntry, Value, categorized_loki_stream_results,
    json,
};

/// The `result` array of a `streams` response, in the requested encoding.
pub(crate) fn loki_stream_results(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    encoding: LokiStreamEncoding,
) -> Vec<Value> {
    match encoding {
        LokiStreamEncoding::Folded => streams
            .into_iter()
            .map(|(stream, values)| {
                json!({
                    "stream": stream,
                    "values": values,
                })
            })
            .collect(),
        LokiStreamEncoding::CategorizeLabels => categorized_loki_stream_results(streams),
    }
}
