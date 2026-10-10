use super::*;

pub(crate) fn loki_streams_response_with_warnings(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    warnings: &[String],
    encoding: LokiStreamEncoding,
) -> Value {
    let mut value = json!({
        "status": "success",
        "data": {"resultType": "streams"},
    });
    value["data"]["result"] = Value::Array(loki_stream_results(streams, encoding));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}
