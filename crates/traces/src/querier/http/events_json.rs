use super::{SpanRef, Value, attrs_json, event_unix_nano, json};

pub(crate) fn events_json(span: &SpanRef) -> Value {
    Value::Array(
        span.events
            .iter()
            .map(|event| {
                json!({
                    "timeUnixNano": event_unix_nano(span, event).to_string(),
                    "name": event.name,
                    "attributes": attrs_json(&event.attributes),
                })
            })
            .collect(),
    )
}
