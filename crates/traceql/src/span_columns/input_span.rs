use super::{AttrValue, EventRef, LinkRef, Time};

#[derive(Clone, Debug, PartialEq)]
pub struct InputSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub start_unix_nano: i64,
    /// How long the span ran.
    pub duration: Time,
    pub status_code: i32,
    pub status_message: String,
    pub instrumentation_name: String,
    pub instrumentation_version: String,
    pub attrs: Vec<(String, AttrValue)>,
    pub events: Vec<EventRef>,
    pub links: Vec<LinkRef>,
}
