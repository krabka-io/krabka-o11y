use super::*;

/// One flattened span row.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRow {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub nested_set: NestedSet,
    pub child_count: i32,
    pub root_service_name: Option<String>,
    pub root_span_name: Option<String>,
    pub trace_start_unix_nano: i64,
    pub trace_duration: Time,
    pub name: Option<String>,
    pub kind: SpanKind,
    pub start_unix_nano: i64,
    pub duration: Time,
    pub status_code: StatusCode,
    pub status_message: Option<String>,
    pub instrumentation_name: Option<String>,
    pub instrumentation_version: Option<String>,
    pub attrs: Vec<SpanAttr>,
    pub events: Vec<SpanEvent>,
    pub links: Vec<SpanLink>,
}
