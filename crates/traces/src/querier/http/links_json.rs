use super::{json, Value, base64, attrs_json};

pub(crate) fn links_json(links: &[krabka_traceql::LinkRef]) -> Value {
    Value::Array(
        links
            .iter()
            .map(|link| {
                json!({
                    "traceId": base64(link.trace_id),
                    "spanId": base64(link.span_id),
                    "attributes": attrs_json(&link.attributes),
                })
            })
            .collect(),
    )
}
