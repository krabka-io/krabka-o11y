use super::{AttrValue, EventRef, LinkRef, Time};

/// One matched span in a result span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanRef {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: String,
    pub kind: i32,
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub nested_set_parent: i32,
    pub start_time_unix_nano: u64,
    /// How long the span ran.
    pub duration: Time,
    pub status_code: i32,
    pub status_message: String,
    pub instrumentation_name: String,
    pub instrumentation_version: String,
    pub resource_attributes: Vec<(String, AttrValue)>,
    pub attributes: Vec<(String, AttrValue)>,
    pub events: Vec<EventRef>,
    pub links: Vec<LinkRef>,
}
