use super::*;

pub(crate) fn loki_streams_response_with_warnings(
    streams: BTreeMap<Labels, Vec<[String; 2]>>,
    warnings: &[String],
) -> Value {
    let result = streams
        .into_iter()
        .map(|(stream, values)| {
            json!({
                "stream": stream,
                "values": values,
            })
        })
        .collect::<Vec<_>>();

    let mut value = loki_success_value(json!({
        "resultType": "streams",
        "result": result,
    }));
    if !warnings.is_empty() {
        value["warnings"] = json!(warnings);
    }
    value
}
