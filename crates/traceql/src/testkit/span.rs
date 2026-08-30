use super::*;

pub(crate) fn span(
    trace: u8,
    id: u8,
    parent: Option<u8>,
    name: &str,
    duration_nanos: i64,
    attrs: Vec<(&str, AttrValue)>,
) -> InputSpan {
    InputSpan {
        trace_id: [trace; 16],
        span_id: [id; 8],
        parent_span_id: parent.map(|p| [p; 8]),
        name: name.into(),
        kind: 0,
        start_unix_nano: 1_000 + i64::from(id),
        duration: Time::from_nanos(duration_nanos),
        status_code: 0,
        status_message: String::new(),
        instrumentation_name: String::new(),
        instrumentation_version: String::new(),
        attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        events: Vec::new(),
        links: Vec::new(),
    }
}
