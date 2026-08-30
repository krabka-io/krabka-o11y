use super::{Map, SpanRef, Value, attrs_json, base64, events_json, json, links_json, span_attributes, span_end_unix_nano, span_kind_json, span_status_json};

pub(crate) fn trace_span_json(trace_id: [u8; 16], span: &SpanRef) -> Value {
    let mut obj = Map::new();
    obj.insert("traceId".into(), json!(base64(trace_id)));
    obj.insert("spanId".into(), json!(base64(span.span_id)));
    if let Some(parent_span_id) = span.parent_span_id {
        obj.insert("parentSpanId".into(), json!(base64(parent_span_id)));
    }
    obj.insert("name".into(), json!(span.name));
    if let Some(kind) = span_kind_json(span.kind) {
        obj.insert("kind".into(), json!(kind));
    }
    obj.insert(
        "startTimeUnixNano".into(),
        json!(span.start_time_unix_nano.to_string()),
    );
    obj.insert(
        "endTimeUnixNano".into(),
        json!(span_end_unix_nano(span).to_string()),
    );
    obj.insert(
        "status".into(),
        span_status_json(span.status_code, &span.status_message),
    );
    obj.insert("attributes".into(), attrs_json(&span_attributes(span)));
    if !span.events.is_empty() {
        obj.insert("events".into(), events_json(span));
    }
    if !span.links.is_empty() {
        obj.insert("links".into(), links_json(&span.links));
    }
    Value::Object(obj)
}
