use super::{Deserialize, EventRecord, KeyValue, LinkRecord, Serialize, SpanKind, StatusCode};

/// One internal span. The WAL carries one record per span.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: SpanKind,
    pub start_ns: i64,
    pub duration_ns: i64,
    pub status: StatusCode,
    pub status_message: String,
    pub resource_attrs: Vec<KeyValue>,
    pub span_attrs: Vec<KeyValue>,
    pub events: Vec<EventRecord>,
    pub links: Vec<LinkRecord>,
    pub instrumentation_scope: String,
    pub instrumentation_version: String,
}

impl Span {
    /// Root spans have no raw semantic parent span id.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent_span_id.is_none()
    }
}
