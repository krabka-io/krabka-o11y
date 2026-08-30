use super::*;

impl From<&SpanJson> for SpanRef {
    fn from(s: &SpanJson) -> Self {
        SpanRef {
            span_id: parse_hex8(&s.span_id),
            parent_span_id: None,
            name: String::new(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: s.start_time_unix_nano.parse().unwrap_or(0),
            duration: Time::from_nanos(s.duration_nanos.parse().unwrap_or(0)),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: s
                .attributes
                .iter()
                .map(|kv| (kv.key.clone(), AttrValue::from(&kv.value)))
                .collect(),
            events: Vec::new(),
            links: Vec::new(),
        }
    }
}
