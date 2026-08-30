use super::*;

pub(crate) fn search_span_json(span: &SpanRef) -> Value {
    json!({
        "spanID": hex::encode(span.span_id),
        "startTimeUnixNano": span.start_time_unix_nano.to_string(),
        "durationNanos": span.duration.nanos_i64().to_string(),
        "attributes": attrs_json(&span.attributes),
    })
}
